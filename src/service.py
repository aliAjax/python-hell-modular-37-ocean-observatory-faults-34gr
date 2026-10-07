from datetime import datetime, timezone
from uuid import uuid4

from .audit import AuditTrail
from .domain import ConflictError, NotFoundError, PermissionDenied, ValidationError
from .rules import RuleEngine


SOURCE_PRIORITY = {"shore": 100, "offline": 10}


def _source_priority(source):
    if not source:
        return 0
    text = str(source)
    if text in SOURCE_PRIORITY:
        return SOURCE_PRIORITY[text]
    for prefix, priority in SOURCE_PRIORITY.items():
        if text.startswith(prefix + ":"):
            return priority
    return 0


def _parse_timestamp(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


class DomainService:
    def __init__(self, repository, rules=None):
        self.repository = repository
        self.rules = rules or RuleEngine()
        self.audit = AuditTrail(repository)

    def _lookup(self, kind, field, value):
        return self.repository.find_entities(self.rules.normalize_kind(kind), field, value)

    def health(self):
        return {"status": "ok" if self.repository.ping() else "error"}

    def create(self, actor, kind, data, idempotency_key=None):
        kind = self.rules.normalize_kind(kind)
        payload = dict(data or {})
        if idempotency_key:
            existing = self.repository.get_idempotency(actor.user_id, idempotency_key)
            if existing:
                entity = self.repository.get_entity(existing)
                if entity:
                    return entity
        self.rules.validate_create(actor, kind, payload, self._lookup)
        if "source" not in payload:
            payload["source"] = "shore"
        entity_id = str(payload.pop("id", "") or uuid4())
        if self.repository.get_entity(entity_id):
            raise ConflictError("entity already exists: " + entity_id)
        status = self.rules.initial_status(kind, payload)
        entity = self.repository.create_entity(entity_id, kind, status, payload, actor.user_id)
        self.audit.record(entity_id, actor, "create", None, status, {"kind": kind})
        if idempotency_key:
            self.repository.save_idempotency(actor.user_id, idempotency_key, entity_id)
        return entity

    def transition(self, actor, entity_id, action, data=None, expected_version=None):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        expected = int(expected_version) if expected_version is not None else entity["version"]
        next_status, patch = self.rules.validate_transition(
            actor, entity, action, dict(data or {}), self._lookup
        )
        merged = dict(entity["data"])
        merged.update(patch)
        updated = self.repository.update_entity(entity_id, expected, next_status, merged)
        self.audit.record(
            entity_id,
            actor,
            action,
            entity["status"],
            updated["status"],
            {"patch": patch},
        )
        return updated

    # ---- offline reconciliation ----

    def merge_offline(self, actor, records):
        """Reconcile a batch of offline records; resumable from a checkpoint."""
        if not isinstance(records, list):
            raise ValidationError("records must be a list")
        batch_id = str(uuid4())
        self.repository.create_offline_batch(batch_id, actor.user_id, len(records))
        try:
            self._run_batch(actor, batch_id, records)
        except Exception:
            self.repository.fail_offline_batch(batch_id, "batch processing failed")
            raise
        return self.get_offline_batch(batch_id)

    def retry_offline_batch(self, actor, batch_id):
        batch = self.repository.get_offline_batch(batch_id)
        if not batch:
            raise NotFoundError("offline batch not found: " + batch_id)
        if batch["status"] == "completed":
            return self.get_offline_batch(batch_id)
        records = [row["payload"] for row in self.repository.list_offline_records(batch_id)]
        self.repository.reset_offline_batch(batch_id)
        try:
            self._run_batch(actor, batch_id, records)
        except Exception:
            self.repository.fail_offline_batch(batch_id, "batch processing failed")
            raise
        return self.get_offline_batch(batch_id)

    def list_offline_batches(self):
        return self.repository.list_offline_batches()

    def get_offline_batch(self, batch_id):
        batch = self.repository.get_offline_batch(batch_id)
        if not batch:
            raise NotFoundError("offline batch not found: " + batch_id)
        records = self.repository.list_offline_records(batch_id)
        counts = {}
        for row in records:
            counts[row["status"]] = counts.get(row["status"], 0) + 1
        return dict(batch, records=records, counts=counts)

    def _run_batch(self, actor, batch_id, records):
        batch = self.repository.get_offline_batch(batch_id)
        start = int(batch["processed"]) if batch else 0
        deferred = []
        for index, raw in enumerate(records):
            if index < start:
                continue
            status, result, kind = self._handle_record(actor, raw)
            source_id = str(raw.get("source_id", "")).strip() if isinstance(raw, dict) else ""
            record_id = str(raw.get("record_id", "")).strip() if isinstance(raw, dict) else ""
            self.repository.upsert_offline_record(
                batch_id, index, source_id, record_id, kind or "unknown", status,
                raw if isinstance(raw, dict) else {}, result,
            )
            self.repository.advance_offline_batch(batch_id, index + 1)
            if status == "deferred":
                deferred.append(index)
        for _pass in range(2):
            if not deferred:
                break
            still = []
            for index in deferred:
                raw = records[index]
                status, result, kind = self._handle_record(actor, raw)
                source_id = str(raw.get("source_id", "")).strip()
                record_id = str(raw.get("record_id", "")).strip()
                self.repository.upsert_offline_record(
                    batch_id, index, source_id, record_id, kind or "unknown", status, raw, result,
                )
                if status == "deferred":
                    still.append(index)
            deferred = still
        for index in deferred:
            raw = records[index]
            source_id = str(raw.get("source_id", "")).strip()
            record_id = str(raw.get("record_id", "")).strip()
            self.repository.upsert_offline_record(
                batch_id, index, source_id, record_id, self._infer_kind(raw) or "unknown",
                "rejected", raw, {"reason": "referenced entity could not be resolved"},
            )
        self.repository.complete_offline_batch(batch_id)

    def _handle_record(self, actor, raw):
        if not isinstance(raw, dict):
            return "rejected", {"reason": "each offline record must be an object"}, None
        source_id = str(raw.get("source_id", "")).strip()
        record_id = str(raw.get("record_id", "")).strip()
        if not source_id or not record_id:
            return "rejected", {"reason": "source_id and record_id are required"}, None
        existing = self.repository.get_offline_record(source_id, record_id)
        if existing and existing["status"] in ("accepted", "rejected", "conflict", "duplicate"):
            entity_id = existing["result"].get("entity_id") or existing["result"].get("conflict_id")
            return "duplicate", {"reason": "already ingested", "record_status": existing["status"], "entity_id": entity_id}, existing["kind"]
        kind = self._infer_kind(raw)
        if not kind:
            return "rejected", {"reason": "kind is required or could not be inferred"}, None
        try:
            status, result = self._process_record(actor, kind, raw)
        except PermissionDenied as exc:
            status, result = "rejected", {"reason": str(exc), "type": "PermissionDenied"}
        except ValidationError as exc:
            status, result = "rejected", {"reason": str(exc), "type": "ValidationError"}
        except ConflictError as exc:
            status, result = "rejected", {"reason": str(exc), "type": "ConflictError"}
        except NotFoundError as exc:
            status, result = "rejected", {"reason": str(exc), "type": "NotFoundError"}
        audit_entity = result.get("entity_id") or result.get("conflict_id") or ("offline-record:" + source_id + ":" + record_id)
        self.audit.record(
            audit_entity, actor, "merge_offline", None, status,
            {"source_id": source_id, "record_id": record_id, "kind": kind, "result": result},
        )
        return status, result, kind

    def _infer_kind(self, raw):
        kind = raw.get("kind")
        if kind:
            return self.rules.normalize_kind(str(kind))
        if "metric" in raw and ("value" in raw or "observed_at" in raw):
            return "telemetry"
        if "severity" in raw and "summary" in raw:
            return "incident"
        if "start_at" in raw and "end_at" in raw:
            return "gap"
        if "asset_type" in raw:
            return "asset"
        if "link_type" in raw:
            return "link"
        if "window_start" in raw:
            return "mission"
        return None

    def _process_record(self, actor, kind, raw):
        payload = {key: value for key, value in raw.items() if key not in ("source_id", "record_id", "kind")}
        if kind == "telemetry":
            return self._merge_telemetry(actor, payload)
        if kind == "incident":
            return self._merge_incident(actor, payload)
        if kind == "gap":
            return self._merge_gap(actor, payload)
        return self._merge_generic(actor, kind, payload)

    def _merge_generic(self, actor, kind, payload):
        self.rules.validate_create(actor, kind, payload, self._lookup)
        data = dict(payload)
        data["source"] = data.get("source") or "offline"
        entity_id = str(uuid4())
        entity = self.repository.create_entity(
            entity_id, kind, self.rules.initial_status(kind, data), data, actor.user_id,
        )
        return "accepted", {"entity_id": entity["id"]}

    def _merge_gap(self, actor, payload):
        try:
            self.rules.validate_create(actor, "gap", payload, self._lookup)
        except ValidationError as exc:
            if "requires incident" in str(exc):
                return "deferred", {"reason": str(exc)}
            raise
        data = dict(payload)
        data["source"] = data.get("source") or "offline"
        entity_id = str(uuid4())
        entity = self.repository.create_entity(entity_id, "gap", "open", data, actor.user_id)
        return "accepted", {"entity_id": entity["id"]}

    def _merge_incident(self, actor, payload):
        try:
            self.rules.validate_create(actor, "incident", payload, self._lookup)
        except ConflictError as exc:
            if "active incident" not in str(exc):
                raise
            existing = None
            for item in self._lookup("incident", "asset_id", payload.get("asset_id")):
                if item["status"] in ("open", "diagnosing", "recovery_planned", "recovering") and item["data"].get("kind") == payload.get("kind"):
                    existing = item
                    break
            conflict_data = {
                "kind": "incident",
                "asset_id": payload.get("asset_id"),
                "incident_kind": payload.get("kind"),
                "existing": existing["data"] if existing else None,
                "incoming": payload,
            }
            if existing:
                conflict_data["incident_id"] = existing["id"]
            conflict_id = str(uuid4())
            self.repository.create_entity(conflict_id, "conflict", "open", conflict_data, actor.user_id)
            return "conflict", {"conflict_id": conflict_id}
        data = dict(payload)
        data["source"] = data.get("source") or "offline"
        entity_id = str(uuid4())
        entity = self.repository.create_entity(
            entity_id, "incident", self.rules.initial_status("incident", data), data, actor.user_id,
        )
        return "accepted", {"entity_id": entity["id"]}

    def _find_telemetry(self, asset_id, metric):
        for item in self._lookup("telemetry", "asset_id", asset_id):
            if item["data"].get("metric") == metric:
                return item
        return None

    def _merge_telemetry(self, actor, payload):
        self.rules.validate_create(actor, "telemetry", payload, self._merge_telemetry_lookup)
        asset_id = payload["asset_id"]
        metric = payload["metric"]
        incoming = {
            "value": payload.get("value"),
            "observed_at": payload.get("observed_at"),
            "revision": int(payload.get("revision")),
            "source": payload.get("source") or "offline",
        }
        existing = self._find_telemetry(asset_id, metric)
        if not existing:
            data = dict(payload)
            data["source"] = incoming["source"]
            entity_id = str(uuid4())
            entity = self.repository.create_entity(entity_id, "telemetry", "current", data, actor.user_id)
            return "accepted", {"entity_id": entity["id"], "decision": "created"}
        for _attempt in range(5):
            current = self.repository.get_entity(existing["id"])
            decision, conflict_detail = self._reconcile_telemetry(current["data"], incoming)
            if conflict_detail:
                conflict_data = {
                    "kind": "telemetry",
                    "asset_id": asset_id,
                    "metric": metric,
                    "existing": conflict_detail["existing"],
                    "incoming": conflict_detail["incoming"],
                }
                open_incidents = [
                    item for item in self._lookup("incident", "asset_id", asset_id)
                    if item["status"] not in ("resolved", "closed")
                ]
                if open_incidents:
                    conflict_data["incident_id"] = open_incidents[0]["id"]
                conflict_id = str(uuid4())
                self.repository.create_entity(conflict_id, "conflict", "open", conflict_data, actor.user_id)
                return "conflict", {"conflict_id": conflict_id}
            if decision == "duplicate":
                return "duplicate", {"entity_id": current["id"], "decision": "duplicate"}
            if decision == "incoming":
                merged = dict(current["data"])
                merged["value"] = incoming["value"]
                merged["observed_at"] = incoming["observed_at"]
                merged["revision"] = max(int(current["data"].get("revision", 0)), incoming["revision"])
                merged["source"] = incoming["source"]
                try:
                    updated = self.repository.update_entity(
                        current["id"], current["version"], current["status"], merged,
                    )
                except ConflictError as exc:
                    if "version conflict" in str(exc):
                        continue
                    raise
                return "accepted", {"entity_id": updated["id"], "decision": "incoming"}
            return "accepted", {"entity_id": current["id"], "decision": "existing"}
        raise ConflictError("could not reconcile telemetry after concurrent updates")

    def _merge_telemetry_lookup(self, kind, field, value):
        if self.rules.normalize_kind(kind) == "telemetry":
            return []
        return self._lookup(kind, field, value)

    def _reconcile_telemetry(self, current, incoming):
        current_ts = _parse_timestamp(current.get("observed_at"))
        incoming_ts = _parse_timestamp(incoming.get("observed_at"))
        if current_ts is not None and incoming_ts is not None:
            if incoming_ts > current_ts:
                return "incoming", None
            if current_ts > incoming_ts:
                return "existing", None
        else:
            current_text = str(current.get("observed_at") or "")
            incoming_text = str(incoming.get("observed_at") or "")
            if incoming_text > current_text:
                return "incoming", None
            if current_text > incoming_text:
                return "existing", None
        current_priority = _source_priority(current.get("source"))
        incoming_priority = _source_priority(incoming.get("source"))
        if incoming_priority > current_priority:
            return "incoming", None
        if current_priority > incoming_priority:
            return "existing", None
        if current.get("value") == incoming.get("value"):
            return "duplicate", None
        return None, {"existing": current, "incoming": incoming}

    def get(self, entity_id):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        return entity

    def list(self, kind=None, status=None):
        if kind:
            kind = self.rules.normalize_kind(kind)
        return self.repository.list_entities(kind=kind, status=status)

    def audit_log(self, entity_id=None):
        return self.repository.list_audit(entity_id=entity_id)
