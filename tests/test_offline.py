import tempfile
import unittest
from pathlib import Path

from src.domain import Actor, ConflictError
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class OfflineReconciliationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = DomainService(SQLiteRepository(Path(self.tmp.name) / "test.db"), RuleEngine())
        self.admin = Actor("admin", "admin")
        self.viewer = Actor("view", "viewer")
        station = self.service.create(self.admin, "station", {"name": "S", "region": "R"})
        self.asset = self.service.create(self.admin, "asset", {
            "station_id": station["id"], "asset_type": "sensor",
            "serial_no": "X", "last_seen": "2026-09-27T09:00:00Z",
        })

    def tearDown(self):
        self.tmp.cleanup()

    def telemetry(self, metric, value, observed_at, revision, source=None):
        data = {
            "asset_id": self.asset["id"], "metric": metric, "value": value,
            "observed_at": observed_at, "revision": revision,
        }
        if source:
            data["source"] = source
        return self.service.create(self.admin, "telemetry", data)

    def offline(self, *records):
        return self.service.merge_offline(self.admin, list(records))

    def tel_record(self, source_id, record_id, metric, value, observed_at, revision, source=None):
        record = {
            "source_id": source_id, "record_id": record_id, "kind": "telemetry",
            "asset_id": self.asset["id"], "metric": metric, "value": value,
            "observed_at": observed_at, "revision": revision,
        }
        if source:
            record["source"] = source
        return record

    def test_later_observation_wins(self):
        current = self.telemetry("pressure", 10, "2026-09-27T09:00:00Z", 1)
        self.offline(self.tel_record("ship-1", "r-1", "pressure", 11, "2026-09-27T10:00:00Z", 2))
        current = self.service.get(current["id"])
        self.assertEqual(current["data"]["value"], 11)
        self.assertEqual(current["data"]["observed_at"], "2026-09-27T10:00:00Z")
        self.assertEqual(current["data"]["source"], "offline")

    def test_shore_wins_tie_on_source_priority(self):
        current = self.telemetry("temp", 5, "2026-09-27T09:00:00Z", 1)
        self.offline(self.tel_record("ship-1", "r-2", "temp", 9, "2026-09-27T09:00:00Z", 2))
        current = self.service.get(current["id"])
        self.assertEqual(current["data"]["value"], 5)

    def test_same_priority_tie_with_different_value_becomes_conflict(self):
        current = self.telemetry("humidity", 50, "2026-09-27T09:00:00Z", 1, source="offline")
        batch = self.offline(self.tel_record("ship-1", "r-3", "humidity", 60, "2026-09-27T09:00:00Z", 2, source="offline"))
        self.assertEqual(batch["counts"].get("conflict"), 1)
        conflicts = self.service.list("conflict", status="open")
        self.assertEqual(len(conflicts), 1)
        self.assertEqual(conflicts[0]["data"]["existing"]["value"], 50)
        self.assertEqual(conflicts[0]["data"]["incoming"]["value"], 60)

    def test_pending_conflict_blocks_incident_closure(self):
        self.telemetry("humidity", 50, "2026-09-27T09:00:00Z", 1, source="offline")
        self.offline(self.tel_record("ship-1", "r-3", "humidity", 60, "2026-09-27T09:00:00Z", 2, source="offline"))
        incident = self.service.create(self.admin, "incident", {
            "station_id": self.asset["data"]["station_id"],
            "asset_id": self.asset["id"], "kind": "loss", "severity": "high", "summary": "x",
        })
        for action in ("diagnose", "plan_recovery", "start_recovery"):
            incident = self.service.transition(self.admin, incident["id"], action)
        with self.assertRaises(ConflictError):
            self.service.transition(self.admin, incident["id"], "resolve", {"summary": "done"})
        conflicts = self.service.list("conflict", status="open")
        self.service.transition(self.admin, conflicts[0]["id"], "resolve", {})
        incident = self.service.transition(self.admin, incident["id"], "resolve", {"summary": "done"})
        self.assertEqual(incident["status"], "resolved")
        # a new pending conflict after resolution must still block close
        self.offline(self.tel_record("ship-1", "r-3b", "humidity", 70, "2026-09-27T09:00:00Z", 3, source="offline"))
        with self.assertRaises(ConflictError):
            self.service.transition(self.admin, incident["id"], "close", {})
        conflicts = self.service.list("conflict", status="open")
        self.service.transition(self.admin, conflicts[0]["id"], "resolve", {})
        incident = self.service.transition(self.admin, incident["id"], "close", {})
        self.assertEqual(incident["status"], "closed")

    def test_unauthorized_record_rejected_with_reason(self):
        batch = self.service.merge_offline(self.viewer, [
            {"source_id": "ship-2", "record_id": "r-4", "kind": "station", "name": "S2", "region": "R2"},
        ])
        self.assertEqual(batch["counts"].get("rejected"), 1)
        record = batch["records"][0]
        self.assertEqual(record["status"], "rejected")
        self.assertIn("not allowed", record["result"]["reason"])

    def test_reupload_is_idempotent(self):
        self.telemetry("pressure", 10, "2026-09-27T09:00:00Z", 1)
        record = self.tel_record("ship-1", "r-1", "pressure", 11, "2026-09-27T10:00:00Z", 2)
        self.offline(record)
        before = len(self.service.list("telemetry"))
        batch = self.offline(record)
        after = len(self.service.list("telemetry"))
        self.assertEqual(before, after)
        self.assertEqual(batch["records"][0]["status"], "duplicate")

    def test_retry_from_checkpoint_does_not_duplicate(self):
        batch = self.offline(
            {"source_id": "ship-3", "record_id": "r-5", "kind": "station", "name": "S3", "region": "R3"},
            {"source_id": "ship-3", "record_id": "r-6", "kind": "station", "name": "S4", "region": "R4"},
        )
        self.assertEqual(batch["counts"].get("accepted"), 2)
        self.service.repository.fail_offline_batch(batch["id"], "simulated failure")
        self.service.repository.advance_offline_batch(batch["id"], 1)
        retried = self.service.retry_offline_batch(self.admin, batch["id"])
        self.assertEqual(retried["status"], "completed")
        self.assertEqual(retried["processed"], 2)
        stations = self.service.list("station")
        self.assertEqual(len(stations), 3)

    def test_later_concurrent_submission_sees_latest_result(self):
        asset2 = self.service.create(self.admin, "asset", {
            "station_id": self.asset["data"]["station_id"], "asset_type": "sensor",
            "serial_no": "Y", "last_seen": "2026-09-27T09:00:00Z",
        })
        self.service.merge_offline(self.admin, [
            {"source_id": "ship-a", "record_id": "c-1", "kind": "telemetry",
             "asset_id": asset2["id"], "metric": "voltage", "value": 1,
             "observed_at": "2026-09-27T09:00:00Z", "revision": 1},
        ])
        self.service.merge_offline(self.admin, [
            {"source_id": "ship-b", "record_id": "c-2", "kind": "telemetry",
             "asset_id": asset2["id"], "metric": "voltage", "value": 2,
             "observed_at": "2026-09-27T11:00:00Z", "revision": 2},
        ])
        current = self.service._find_telemetry(asset2["id"], "voltage")
        self.assertEqual(current["data"]["value"], 2)
        self.assertEqual(current["data"]["observed_at"], "2026-09-27T11:00:00Z")

    def test_legacy_source_backfilled_to_creator(self):
        self.service.repository.create_entity(
            "legacy-1", "station", "online",
            {"name": "Legacy", "region": "R"}, "oldkeeper",
        )
        self.service.repository.backfill_sources()
        legacy = self.service.get("legacy-1")
        self.assertEqual(legacy["data"]["source"], "oldkeeper")

    def test_batch_summary_shape(self):
        batch = self.offline(
            {"source_id": "ship-1", "record_id": "r-1", "kind": "station", "name": "S2", "region": "R2"},
        )
        summary = self.service.get_offline_batch(batch["id"])
        self.assertIn("counts", summary)
        self.assertIn("records", summary)
        self.assertEqual(summary["status"], "completed")


if __name__ == "__main__":
    unittest.main()
