"""Offline reconciliation merge.

Watchkeepers log telemetry revisions, incidents and data gaps while the site
is disconnected and upload them in batches on return. Unlike the old
number-only dedup, this module:

* orders telemetry revisions by observation time, then source priority;
* parks undecidable disagreements as ``reconciliation_conflict`` records,
  which block the related incident from resolving/closing;
* ledgers every batch and record, so a retried batch resumes at the first
  unprocessed record without double ingesting anything;
* rejects records the uploader has no authority for, with a stated reason.
"""

import hashlib
from uuid import uuid4

from .domain import ConflictError, DomainError, NotFoundError, PermissionDenied, ValidationError
from .rules import parse_observed_at

RECORD_TYPES = ("telemetry_revision", "incident", "incident_action", "gap")
MERGE_ROLES = ("admin", "operator", "engineer", "field")


def uid_for(source_id, record_id):
    raw = (str(source_id) + "\0" + str(record_id)).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:32]


def deterministic_batch_id(actor, uids):
    raw = actor.user_id + "\0" + ",".join(sorted(uids))
    return "batch-" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def _same_value(left, right):
    try:
        return float(left) == float(right)
    except (TypeError, ValueError):
        return left == right


class OfflineMerger:
    def __init__(self, repository, rules, audit):
        self.repository = repository
        self.rules = rules
        self.audit = audit

    # ------------------------------------------------------------ public API
    def merge_batch(self, actor, records, batch_id=None):
        if actor.role not in MERGE_ROLES:
            raise PermissionDenied(
                "role %s is not allowed to upload offline batches" % actor.role
            )
        if not isinstance(records, list):
            raise ValidationError("records must be a list")

        envelopes = []
        uids = []
        for index, raw in enumerate(records):
            envelope = self._parse_envelope(raw, index)
            envelopes.append(envelope)
            uids.append(envelope["uid"])
        batch_id = batch_id or deterministic_batch_id(actor, uids)

        # Register the batch (or adopt an earlier failed one for retry).
        with self.repository.unit_of_work() as uow:
            existing = uow.get_batch(batch_id)
            if existing and existing["submitted_by"] != actor.user_id:
                raise ConflictError(
                    "batch %s is owned by %s; resubmit under your own batch id"
                    % (batch_id, existing["submitted_by"])
                )
            if not existing:
                uow.upsert_batch(batch_id, actor.user_id, len(envelopes), "running")
            uow.audit(batch_id, actor, "merge_batch_start", None, "running",
                      {"records": len(envelopes)})

        try:
            for envelope in envelopes:
                self._process_one(batch_id, actor, envelope)
        except Exception as exc:  # infrastructure failure: keep the checkpoint
            with self.repository.unit_of_work() as uow:
                uow.mark_batch(batch_id, "failed", str(exc))
                uow.audit(batch_id, actor, "merge_batch_failed", "running", "failed",
                          {"error": str(exc)})
            raise

        with self.repository.unit_of_work() as uow:
            uow.mark_batch(batch_id, "completed", None)
            uow.audit(batch_id, actor, "merge_batch_complete", "running", "completed", {})

        return self.repository.get_batch(batch_id)

    def get_batch(self, batch_id):
        batch = self.repository.get_batch(batch_id)
        if not batch:
            raise NotFoundError("batch not found: " + batch_id)
        return batch

    # ------------------------------------------------------------ envelopes
    def _parse_envelope(self, raw, index):
        if not isinstance(raw, dict):
            raise ValidationError("record at index %s must be an object" % index)
        source_id = str(raw.get("source_id", "")).strip()
        record_id = str(raw.get("record_id", "")).strip()
        if not source_id or not record_id:
            raise ValidationError(
                "record at index %s requires source_id and record_id" % index
            )
        record_type = raw.get("record_type", "telemetry_revision")
        if record_type not in RECORD_TYPES:
            raise ValidationError(
                "record %s/%s has unknown record_type: %s"
                % (source_id, record_id, record_type)
            )
        payload = raw.get("payload")
        if not isinstance(payload, dict):
            raise ValidationError(
                "record %s/%s requires a payload object" % (source_id, record_id)
            )
        observed_at = raw.get("observed_at", payload.get("observed_at"))
        return {
            "uid": uid_for(source_id, record_id),
            "source_id": source_id,
            "record_id": record_id,
            "record_type": record_type,
            "recorded_by": raw.get("recorded_by") or raw.get("created_by") or source_id,
            "observed_at": observed_at,
            "payload": payload,
        }

    # ------------------------------------------------------------ pipeline
    def _process_one(self, batch_id, actor, envelope):
        """Apply one record in its own transaction = the retry checkpoint."""
        uid = envelope["uid"]
        with self.repository.unit_of_work() as uow:
            # Same batch retry: already ledgered, never re-ingest.
            if uow.get_item(batch_id, uid):
                return
            prior = self._find_prior(uow, uid)
            if prior:
                # Same (source_id, record_id) already merged in another batch.
                target_id = prior["data"].get("target_id") or prior["id"]
                uow.put_item(
                    batch_id, uid, "duplicated",
                    envelope["source_id"], envelope["record_id"],
                    entity_id=target_id, reason="record already merged",
                )
                uow.audit(prior["id"], actor, "merge_offline_dedupe", None,
                          prior["status"], {"batch_id": batch_id})
                return

            try:
                outcome = self._apply(uow, actor, envelope)
            except PermissionDenied as exc:
                # Unauthorized record: bounce it back with a reason, keep going.
                uow.put_item(
                    batch_id, uid, "rejected",
                    envelope["source_id"], envelope["record_id"], reason=str(exc),
                )
                uow.audit("offline-" + uid, actor, "merge_offline_rejected", None,
                          "rejected", {"reason": str(exc), **self._audit_meta(envelope)})
                return
            except DomainError as exc:
                # Rule refusal (bad reference, active conflict, ...): bounce too.
                uow.put_item(
                    batch_id, uid, "rejected",
                    envelope["source_id"], envelope["record_id"], reason=str(exc),
                )
                uow.audit("offline-" + uid, actor, "merge_offline_rejected", None,
                          "rejected", {"reason": str(exc), **self._audit_meta(envelope)})
                return

            record = uow.insert(
                "offline-" + uid, "offline_record", outcome["status"],
                {
                    "source_id": envelope["source_id"],
                    "record_id": envelope["record_id"],
                    "record_type": envelope["record_type"],
                    "recorded_by": envelope["recorded_by"],
                    "observed_at": envelope["observed_at"],
                    "payload": envelope["payload"],
                    "target_kind": outcome.get("target_kind"),
                    "target_id": outcome.get("target_id"),
                    "conflict_id": outcome.get("conflict_id"),
                },
                actor.user_id,
            )
            uow.audit(record["id"], actor, "merge_offline", None, record["status"],
                      {"target_id": outcome.get("target_id"),
                       "conflict_id": outcome.get("conflict_id"),
                       **self._audit_meta(envelope)})
            uow.put_item(
                batch_id, uid, outcome["status"],
                envelope["source_id"], envelope["record_id"],
                entity_id=outcome.get("target_id") or record["id"],
                reason=outcome.get("reason"),
                payload=envelope,
            )

    def _find_prior(self, uow, uid):
        return uow.get("offline-" + uid)

    @staticmethod
    def _audit_meta(envelope):
        return {
            "source_id": envelope["source_id"],
            "record_id": envelope["record_id"],
            "record_type": envelope["record_type"],
        }

    # ------------------------------------------------------------ type hooks
    def _apply(self, uow, actor, envelope):
        hook = getattr(self, "_apply_" + envelope["record_type"])
        return hook(uow, actor, envelope)

    def _apply_telemetry_revision(self, uow, actor, envelope):
        self.rules.check_offline_role(actor, "telemetry_revision")
        payload = envelope["payload"]
        asset_id = payload.get("asset_id")
        metric = payload.get("metric")
        if not asset_id or not metric:
            raise ValidationError("telemetry revision requires asset_id and metric")
        observed = parse_observed_at(envelope["observed_at"])

        existing = next(
            (item for item in uow.list("telemetry")
             if item["data"].get("asset_id") == asset_id
             and item["data"].get("metric") == metric),
            None,
        )
        if not existing:
            # First sighting: create the telemetry series through normal rules.
            create_payload = {
                "asset_id": asset_id,
                "metric": metric,
                "value": payload.get("value"),
                "observed_at": envelope["observed_at"],
                "revision": int(payload.get("revision") or 1),
                "source_id": envelope["source_id"],
            }
            self.rules.validate_create(actor, "telemetry", create_payload, uow.find)
            entity = uow.insert(
                str(payload.get("id") or uuid4()), "telemetry", "current",
                create_payload, actor.user_id,
            )
            uow.audit(entity["id"], actor, "merge_offline_create", None, "current",
                      self._audit_meta(envelope))
            return {"status": "applied", "target_kind": "telemetry",
                    "target_id": entity["id"]}

        decision, reason = self._compare(existing, envelope, observed)
        if decision == "duplicate":
            return {"status": "duplicated", "target_kind": "telemetry",
                    "target_id": existing["id"], "reason": reason}
        if decision == "conflict":
            conflict = self._park_conflict(
                uow, actor, envelope,
                conflict_type="telemetry_disagreement",
                reason=reason,
                target_kind="telemetry",
                target_id=existing["id"],
                asset_id=asset_id,
                incident_id=payload.get("incident_id"),
                incoming={"value": payload.get("value"),
                          "observed_at": envelope["observed_at"],
                          "source_id": envelope["source_id"]},
                existing={"value": existing["data"].get("value"),
                          "observed_at": existing["data"].get("observed_at"),
                          "source_id": existing["data"].get("source_id")},
            )
            return {"status": "conflicted", "target_kind": "telemetry",
                    "target_id": existing["id"], "conflict_id": conflict["id"],
                    "reason": reason}

        # decision == "take_new": later observation or higher-priority source.
        merged = dict(existing["data"])
        merged["value"] = payload.get("value")
        merged["observed_at"] = envelope["observed_at"]
        merged["source_id"] = envelope["source_id"]
        merged["revision"] = max(
            int(merged.get("revision", 0)) + 1, int(payload.get("revision") or 0)
        )
        merged["revised_by"] = actor.user_id
        merged["late_revision"] = True
        updated = uow.update(existing["id"], existing["version"], "current", merged)
        uow.audit(updated["id"], actor, "merge_offline_revise", existing["status"],
                  "current", {"reason": reason, **self._audit_meta(envelope)})
        return {"status": "applied", "target_kind": "telemetry",
                "target_id": updated["id"], "reason": reason}

    def _compare(self, existing, envelope, observed):
        """Return (decision, reason); decision in take_new/duplicate/conflict."""
        payload = envelope["payload"]
        try:
            current_observed = parse_observed_at(existing["data"].get("observed_at"))
        except ValidationError:
            return "conflict", "stored telemetry has no usable observed_at"
        same = _same_value(payload.get("value"), existing["data"].get("value"))
        if same:
            return "duplicate", "same observation already recorded"
        if observed > current_observed:
            return "take_new", "incoming observation is newer"
        if observed < current_observed:
            return "conflict", (
                "older observation disagrees with stored value "
                "(incoming %s < stored %s)" % (envelope["observed_at"],
                                               existing["data"].get("observed_at"))
            )
        # Same observation time: source priority breaks the tie.
        incoming_rank, incoming_known = self.rules.source_rank(envelope["source_id"])
        existing_rank, existing_known = self.rules.source_rank(
            existing["data"].get("source_id")
        )
        if incoming_known and (not existing_known or incoming_rank < existing_rank):
            return "take_new", "incoming source has higher priority"
        if existing_known and (not incoming_known or existing_rank < incoming_rank):
            return "conflict", (
                "equal observation time but stored source %s outranks %s"
                % (existing["data"].get("source_id"), envelope["source_id"])
            )
        return "conflict", (
            "equal observation time and undistinguished sources "
            "(%s vs %s) disagree" % (envelope["source_id"],
                                     existing["data"].get("source_id"))
        )

    def _apply_incident(self, uow, actor, envelope):
        self.rules.check_offline_role(actor, "incident")
        payload = dict(envelope["payload"])
        payload["source_id"] = envelope["source_id"]
        try:
            self.rules.validate_create(actor, "incident", payload, uow.find)
        except ConflictError as exc:
            # Active incident for the same asset/kind: park, do not close over.
            asset_id = payload.get("asset_id")
            active = next(
                (item for item in uow.list("incident")
                 if item["status"] in ("open", "diagnosing", "recovery_planned",
                                       "recovering", "resolved")
                 and item["data"].get("asset_id") == asset_id
                 and item["data"].get("kind") == payload.get("kind")),
                None,
            )
            conflict = self._park_conflict(
                uow, actor, envelope,
                conflict_type="incident_duplicate",
                reason=str(exc),
                target_kind="incident",
                target_id=active["id"] if active else None,
                asset_id=asset_id,
                incident_id=active["id"] if active else payload.get("incident_id"),
                incoming=payload,
                existing={"incident_id": active["id"],
                          "status": active["status"]} if active else None,
            )
            return {"status": "conflicted",
                    "target_kind": "incident",
                    "target_id": active["id"] if active else None,
                    "conflict_id": conflict["id"], "reason": str(exc)}
        entity_id = str(payload.pop("id", "") or uuid4())
        entity = uow.insert(
            entity_id, "incident",
            self.rules.initial_status("incident", payload), payload, actor.user_id,
        )
        uow.audit(entity["id"], actor, "merge_offline_create", None,
                  entity["status"], self._audit_meta(envelope))
        return {"status": "applied", "target_kind": "incident",
                "target_id": entity["id"]}

    def _apply_incident_action(self, uow, actor, envelope):
        payload = envelope["payload"]
        incident_id = payload.get("incident_id")
        action = payload.get("action")
        if not incident_id or not action:
            raise ValidationError("incident_action requires incident_id and action")
        self.rules.check_offline_role(actor, "incident_action", action)
        entity = uow.get(incident_id)
        if not entity:
            raise ValidationError("incident not found: " + incident_id)
        if action in ("resolve", "close"):
            self.rules.check_no_pending_conflicts(entity, uow.find, action)
        data = dict(payload.get("data") or {})
        next_status, patch = self.rules.validate_transition(
            actor, entity, action, data, uow.find
        )
        merged = dict(entity["data"])
        merged.update(patch)
        updated = uow.update(entity["id"], entity["version"], next_status, merged)
        uow.audit(updated["id"], actor, "merge_offline_" + action, entity["status"],
                  next_status, {"patch": patch, **self._audit_meta(envelope)})
        return {"status": "applied", "target_kind": "incident",
                "target_id": updated["id"]}

    def _apply_gap(self, uow, actor, envelope):
        self.rules.check_offline_role(actor, "gap")
        payload = dict(envelope["payload"])
        payload["source_id"] = envelope["source_id"]
        duplicate = next(
            (item for item in uow.list("gap")
             if item["data"].get("incident_id") == payload.get("incident_id")
             and item["data"].get("start_at") == payload.get("start_at")
             and item["data"].get("end_at") == payload.get("end_at")),
            None,
        )
        if duplicate:
            return {"status": "duplicated", "target_kind": "gap",
                    "target_id": duplicate["id"],
                    "reason": "same gap window already recorded"}
        self.rules.validate_create(actor, "gap", payload, uow.find)
        entity = uow.insert(
            str(payload.pop("id", "") or uuid4()), "gap",
            self.rules.initial_status("gap", payload), payload, actor.user_id,
        )
        uow.audit(entity["id"], actor, "merge_offline_create", None,
                  entity["status"], self._audit_meta(envelope))
        return {"status": "applied", "target_kind": "gap", "target_id": entity["id"]}

    # ------------------------------------------------------------ conflicts
    def _park_conflict(self, uow, actor, envelope, conflict_type, reason,
                       target_kind, target_id, asset_id=None, incident_id=None,
                       incoming=None, existing=None):
        conflict_id = "conflict-" + envelope["uid"]
        data = {
            "type": conflict_type,
            "reason": reason,
            "source_id": envelope["source_id"],
            "record_id": envelope["record_id"],
            "record_type": envelope["record_type"],
            "recorded_by": envelope["recorded_by"],
            "target_kind": target_kind,
            "target_id": target_id,
            "asset_id": asset_id,
            "incident_id": incident_id,
            "incoming": incoming,
            "existing": existing,
        }
        conflict = uow.insert(conflict_id, "reconciliation_conflict", "pending",
                              data, actor.user_id)
        uow.audit(conflict["id"], actor, "conflict_parked", None, "pending",
                  {"reason": reason, **self._audit_meta(envelope)})
        return conflict
