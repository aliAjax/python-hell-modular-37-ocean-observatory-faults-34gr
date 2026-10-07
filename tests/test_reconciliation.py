import tempfile
import unittest
from pathlib import Path

from src.domain import Actor, ConflictError, PermissionDenied, ValidationError
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


def rev(source_id, record_id, asset_id, metric, value, observed_at,
        incident_id=None, revision=1):
    payload = {"asset_id": asset_id, "metric": metric, "value": value,
               "revision": revision}
    if incident_id:
        payload["incident_id"] = incident_id
    return {"source_id": source_id, "record_id": record_id,
            "record_type": "telemetry_revision", "observed_at": observed_at,
            "payload": payload}


def incident_rec(source_id, record_id, **fields):
    return {"source_id": source_id, "record_id": record_id,
            "record_type": "incident", "payload": fields}


def incident_action(source_id, record_id, incident_id, action, data=None):
    return {"source_id": source_id, "record_id": record_id,
            "record_type": "incident_action",
            "payload": {"incident_id": incident_id, "action": action, "data": data or {}}}


def gap_rec(source_id, record_id, incident_id, start_at, end_at):
    return {"source_id": source_id, "record_id": record_id, "record_type": "gap",
            "payload": {"incident_id": incident_id, "start_at": start_at,
                        "end_at": end_at}}


class ReconciliationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = DomainService(
            SQLiteRepository(Path(self.tmp.name) / "test.db"), RuleEngine()
        )
        self.admin = Actor("admin", "admin")
        self.op = Actor("operator-1", "operator")

    def tearDown(self):
        self.tmp.cleanup()

    def station_asset(self):
        station = self.service.create(self.admin, "station", {"name": "S", "region": "R"})
        asset = self.service.create(self.admin, "asset", {
            "station_id": station["id"], "asset_type": "sensor",
            "serial_no": "S-1", "last_seen": "2026-09-27T09:00:00Z",
        })
        return station, asset

    def telemetry(self, asset, metric="p", value=1, observed_at="2026-09-27T09:00:00Z"):
        return self.service.create(self.admin, "telemetry", {
            "asset_id": asset["id"], "metric": metric, "value": value,
            "observed_at": observed_at, "revision": 1,
        })

    # ------------------------------------------------- ordering & precedence
    def test_later_observation_wins_regardless_of_arrival_order(self):
        _, asset = self.station_asset()
        self.telemetry(asset, value=5, observed_at="2026-09-27T10:00:00Z")
        batch = self.service.merge_offline(self.op, [
            rev("field-a", "late-old", asset["id"], "p", 4, "2026-09-27T09:00:00Z"),
        ])
        item = batch["items"][0]
        self.assertEqual(item["status"], "conflicted")
        stored = self.service.get(item["entity_id"])
        self.assertEqual(stored["data"]["value"], 5)  # older revision not applied

        batch = self.service.merge_offline(self.op, [
            rev("field-a", "late-new", asset["id"], "p", 6, "2026-09-27T11:00:00Z"),
        ])
        self.assertEqual(batch["items"][0]["status"], "applied")
        stored = self.service.get(batch["items"][0]["entity_id"])
        self.assertEqual(stored["data"]["value"], 6)
        self.assertEqual(stored["data"]["source_id"], "field-a")

    def test_equal_observation_time_resolves_by_source_priority(self):
        _, asset = self.station_asset()
        self.telemetry(asset, value=5, observed_at="2026-09-27T10:00:00Z")
        # Shore outranks an unknown field device: conflict parked.
        batch = self.service.merge_offline(self.op, [
            rev("buoy-7", "r1", asset["id"], "p", 5.2, "2026-09-27T10:00:00Z"),
        ])
        self.assertEqual(batch["items"][0]["status"], "conflicted")

        # An explicitly higher-priority source wins on the same timestamp.
        service = DomainService(
            SQLiteRepository(Path(self.tmp.name) / "test.db"),
            RuleEngine(source_priority={"buoy-7": 1, "shore": 2}),
        )
        batch = service.merge_offline(self.op, [
            rev("buoy-7", "r2", asset["id"], "p", 5.3, "2026-09-27T10:00:00Z"),
        ])
        self.assertEqual(batch["items"][0]["status"], "applied")
        stored = service.get(batch["items"][0]["entity_id"])
        self.assertEqual(stored["data"]["value"], 5.3)

    def test_same_value_is_duplicated_not_a_conflict(self):
        _, asset = self.station_asset()
        self.telemetry(asset, value=5, observed_at="2026-09-27T10:00:00Z")
        batch = self.service.merge_offline(self.op, [
            rev("shore", "dup", asset["id"], "p", 5, "2026-09-27T10:00:00Z"),
        ])
        self.assertEqual(batch["items"][0]["status"], "duplicated")
        conflicts = self.service.list("reconciliation_conflict")
        self.assertEqual(conflicts, [])

    # ------------------------------------------------- conflicts block close
    def test_pending_conflict_blocks_incident_resolve_and_close(self):
        _, asset = self.station_asset()
        self.telemetry(asset, value=5, observed_at="2026-09-27T10:00:00Z")
        incident = self.service.create(self.admin, "incident", {
            "asset_id": asset["id"], "kind": "drift",
            "severity": "high", "summary": "odd readings",
        })
        batch = self.service.merge_offline(self.op, [
            rev("buoy-7", "c1", asset["id"], "p", 9,
                "2026-09-27T10:00:00Z", incident_id=incident["id"]),
        ])
        self.assertEqual(batch["items"][0]["status"], "conflicted")
        incident = self.service.transition(self.admin, incident["id"], "diagnose", {})
        incident = self.service.transition(self.admin, incident["id"], "plan_recovery", {})
        incident = self.service.transition(self.admin, incident["id"], "start_recovery", {})
        # Offline resolve attempt is bounced by the pending conflict; the
        # online path is blocked too.
        blocked = self.service.merge_offline(self.admin, [
            incident_action("admin", "close-1", incident["id"], "resolve",
                            {"summary": "done"}),
        ])
        self.assertEqual(blocked["items"][0]["status"], "rejected")
        self.assertIn("reconciliation conflict", blocked["items"][0]["reason"])
        with self.assertRaises(ConflictError):
            self.service.transition(self.admin, incident["id"], "resolve",
                                    {"summary": "done"})

        # Resolve the conflict; incident can now proceed.
        conflict = self.service.list("reconciliation_conflict")[0]
        self.assertEqual(conflict["status"], "pending")
        self.service.transition(self.admin, conflict["id"], "resolve",
                                {"resolution": "shore value confirmed"})
        moved = self.service.transition(self.admin, incident["id"], "resolve",
                                        {"summary": "done"})
        moved = self.service.transition(self.admin, moved["id"], "close", {})
        self.assertEqual(moved["status"], "closed")

    def test_duplicate_offline_incident_parks_conflict(self):
        _, asset = self.station_asset()
        self.service.create(self.admin, "incident", {
            "asset_id": asset["id"], "kind": "loss",
            "severity": "high", "summary": "first",
        })
        batch = self.service.merge_offline(self.op, [
            incident_rec("field-a", "i1", asset_id=asset["id"], kind="loss",
                         severity="high", summary="second"),
        ])
        self.assertEqual(batch["items"][0]["status"], "conflicted")
        conflicts = self.service.list("reconciliation_conflict")
        self.assertEqual(len(conflicts), 1)
        self.assertEqual(conflicts[0]["data"]["type"], "incident_duplicate")

    # ------------------------------------------------- checkpoints / retries
    def test_batch_is_idempotent_on_retry(self):
        _, asset = self.station_asset()
        records = [
            rev("field-a", "t1", asset["id"], "p", 2, "2026-09-27T11:00:00Z"),
            rev("field-a", "t2", asset["id"], "p", 3, "2026-09-27T12:00:00Z"),
        ]
        first = self.service.merge_offline(self.op, records, batch_id="b-1")
        self.assertEqual(first["state"], "completed")
        before = len(self.service.list("telemetry"))
        again = self.service.merge_offline(self.op, records, batch_id="b-1")
        self.assertEqual(again["state"], "completed")
        # Retry replays no writes: checkpoint results are returned as stored.
        self.assertEqual([i["status"] for i in again["items"]],
                         ["applied", "applied"])
        self.assertEqual(len(self.service.list("telemetry")), before)
        self.assertEqual(len(self.service.list("offline_record")), 2)

    def test_failed_batch_resumes_at_checkpoint_without_double_insert(self):
        _, asset = self.station_asset()
        records = [
            rev("field-a", "t1", asset["id"], "p", 2, "2026-09-27T11:00:00Z"),
            rev("field-a", "t2", asset["id"], "p", 3, "2026-09-27T12:00:00Z"),
            rev("field-a", "t3", asset["id"], "p", 4, "2026-09-27T13:00:00Z"),
        ]
        calls = {"n": 0}
        original = self.service.offline._apply_telemetry_revision

        def flaky(uow, actor, envelope):
            calls["n"] += 1
            if calls["n"] == 2:
                raise RuntimeError("disk full")
            return original(uow, actor, envelope)

        self.service.offline._apply_telemetry_revision = flaky
        with self.assertRaises(RuntimeError):
            self.service.merge_offline(self.op, records, batch_id="resume-1")

        status = self.service.offline_batch("resume-1")
        self.assertEqual(status["state"], "failed")
        self.assertEqual(len(status["items"]), 1)  # checkpoint at record 1

        self.service.offline._apply_telemetry_revision = original
        resumed = self.service.merge_offline(self.op, records, batch_id="resume-1")
        self.assertEqual(resumed["state"], "completed")
        self.assertEqual([i["status"] for i in resumed["items"]],
                         ["applied", "applied", "applied"])
        self.assertEqual(len(self.service.list("telemetry")), 1)
        self.assertEqual(len(self.service.list("offline_record")), 3)

    def test_same_record_in_other_batch_is_ledgered_as_duplicated(self):
        _, asset = self.station_asset()
        record = rev("field-a", "only", asset["id"], "p", 2, "2026-09-27T11:00:00Z")
        self.service.merge_offline(self.op, [record], batch_id="x-1")
        other = self.service.merge_offline(Actor("operator-2", "operator"),
                                           [record], batch_id="x-2")
        self.assertEqual(other["items"][0]["status"], "duplicated")
        self.assertEqual(len(self.service.list("offline_record")), 1)

    # ------------------------------------------------- authority & migration
    def test_unauthorized_record_is_returned_with_reason(self):
        _, asset = self.station_asset()
        viewer = Actor("voyeur", "viewer")
        with self.assertRaises(PermissionDenied):
            self.service.merge_offline(viewer, [
                rev("field-a", "t1", asset["id"], "p", 2, "2026-09-27T11:00:00Z"),
            ])

        operator = Actor("op", "operator")
        batch = self.service.merge_offline(operator, [
            incident_action("op", "a1", "inc-x", "resolve", {"summary": "done"}),
        ])
        self.assertEqual(batch["items"][0]["status"], "rejected")
        self.assertIn("resolve", batch["items"][0]["reason"])
        # Rejected record creates no domain entity; it is only ledgered.
        self.assertEqual(self.service.list("offline_record"), [])
        self.assertEqual(self.service.list("incident"), [])

    def test_second_operator_sees_first_operator_result(self):
        _, asset = self.station_asset()
        record = rev("field-a", "shared", asset["id"], "p", 2,
                     "2026-09-27T11:00:00Z")
        self.service.merge_offline(Actor("op-a", "operator"), [record],
                                   batch_id="concurrent-a")
        late = self.service.merge_offline(Actor("op-b", "operator"), [record],
                                          batch_id="concurrent-b")
        self.assertEqual(late["items"][0]["status"], "duplicated")
        self.assertEqual(late["items"][0]["entity_id"],
                         self.service.list("telemetry")[0]["id"])

    def test_legacy_rows_get_source_from_original_creator_on_upgrade(self):
        path = Path(self.tmp.name) / "legacy.db"
        repo = SQLiteRepository(path)
        service = DomainService(repo, RuleEngine())
        station = service.create(self.admin, "station", {"name": "old", "region": "R"})
        keeper = Actor("keeper-lee", "engineer")
        asset = service.create(keeper, "asset", {
            "station_id": station["id"], "asset_type": "sensor",
            "serial_no": "legacy", "last_seen": "2026-01-01T00:00:00Z",
        })
        telemetry = service.create(keeper, "telemetry", {
            "asset_id": asset["id"], "metric": "p", "value": 1,
            "observed_at": "2026-01-01T00:00:00Z", "revision": 1,
        })
        self.assertEqual(telemetry["data"]["source_id"], "keeper-lee")

        # Simulate a pre-upgrade row written without source_id.
        with repo._connect() as connection:
            connection.execute(
                "UPDATE entities SET data = json_remove(data, '$.source_id') "
                "WHERE id = ?", (telemetry["id"],)
            )
        self.assertNotIn("source_id", repo.get_entity(telemetry["id"])["data"])

        # Reopening (schema migration) backfills source from the watchkeeper.
        migrated = SQLiteRepository(path).get_entity(telemetry["id"])
        self.assertEqual(migrated["data"]["source_id"], "keeper-lee")

    def test_validation_errors_are_ledgered_per_record(self):
        _, asset = self.station_asset()
        batch = self.service.merge_offline(self.op, [
            rev("field-a", "bad", "missing-asset", "p", 2,
                "2026-09-27T11:00:00Z"),
        ])
        self.assertEqual(batch["items"][0]["status"], "rejected")
        self.assertTrue(batch["items"][0]["reason"])
        self.assertEqual(batch["state"], "completed")

    def test_envelope_shape_errors_fail_the_request(self):
        with self.assertRaises(ValidationError):
            self.service.merge_offline(self.op, [{"record_id": "x"}])
        with self.assertRaises(ValidationError):
            self.service.merge_offline(self.op, "not-a-list")


if __name__ == "__main__":
    unittest.main()
