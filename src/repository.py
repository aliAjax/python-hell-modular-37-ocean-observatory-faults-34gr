import hashlib
import json
import sqlite3
from datetime import datetime, timezone

from .domain import ConflictError, NotFoundError


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class SQLiteRepository:
    def __init__(self, path):
        self.path = str(path)
        self._initialize()

    def _connect(self):
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self):
        with self._connect() as connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS entities (
                    id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    data TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_entities_kind_status
                    ON entities(kind, status);
                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_id TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    actor_role TEXT NOT NULL,
                    action TEXT NOT NULL,
                    from_status TEXT,
                    to_status TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_audit_entity
                    ON audit_log(entity_id, id);
                CREATE TABLE IF NOT EXISTS idempotency (
                    actor_id TEXT NOT NULL,
                    idem_key TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(actor_id, idem_key)
                );
                CREATE TABLE IF NOT EXISTS offline_batches (
                    id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    total INTEGER NOT NULL,
                    processed INTEGER NOT NULL,
                    error TEXT,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS offline_records (
                    id TEXT PRIMARY KEY,
                    batch_id TEXT NOT NULL,
                    seq INTEGER NOT NULL,
                    source_id TEXT NOT NULL,
                    record_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    result TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(source_id, record_id)
                );
                CREATE INDEX IF NOT EXISTS idx_offline_records_batch
                    ON offline_records(batch_id, seq);
            """)
        self.backfill_sources()

    def backfill_sources(self):
        """Legacy records without a source field are attributed to their creator."""
        with self._connect() as connection:
            rows = connection.execute("SELECT id, data, created_by FROM entities").fetchall()
            for row in rows:
                data = json.loads(row["data"])
                if data.get("source") is None:
                    data["source"] = row["created_by"]
                    connection.execute(
                        "UPDATE entities SET data = ?, updated_at = ? WHERE id = ?",
                        (json.dumps(data, ensure_ascii=False, sort_keys=True), utcnow(), row["id"]),
                    )

    @staticmethod
    def _entity_from_row(row):
        return {
            "id": row["id"],
            "kind": row["kind"],
            "status": row["status"],
            "version": int(row["version"]),
            "data": json.loads(row["data"]),
            "created_by": row["created_by"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def create_entity(self, entity_id, kind, status, data, actor_id):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
                "VALUES (?, ?, ?, 1, ?, ?, ?, ?)",
                (entity_id, kind, status, payload, actor_id, now, now),
            )
        return self.get_entity(entity_id)

    def get_entity(self, entity_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM entities WHERE id = ?", (entity_id,)
            ).fetchone()
        return self._entity_from_row(row) if row else None

    def list_entities(self, kind=None, status=None):
        clauses = []
        params = []
        if kind:
            clauses.append("kind = ?")
            params.append(kind)
        if status:
            clauses.append("status = ?")
            params.append(status)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM entities" + where + " ORDER BY created_at, id", params
            ).fetchall()
        return [self._entity_from_row(row) for row in rows]

    def find_entities(self, kind, field, value):
        entities = self.list_entities(kind=kind)
        if field == "*":
            return entities
        return [
            entity
            for entity in entities
            if (entity["id"] == value if field == "id" else entity["data"].get(field) == value)
        ]

    def update_entity(self, entity_id, expected_version, status, data):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT version FROM entities WHERE id = ?", (entity_id,)
            ).fetchone()
            if not row:
                raise NotFoundError("entity not found: " + entity_id)
            current_version = int(row["version"])
            if expected_version is not None and current_version != int(expected_version):
                raise ConflictError(
                    "version conflict: expected %s, found %s"
                    % (expected_version, current_version)
                )
            connection.execute(
                "UPDATE entities SET status = ?, version = version + 1, data = ?, updated_at = ? "
                "WHERE id = ? AND version = ?",
                (status, payload, now, entity_id, current_version),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return self.get_entity(entity_id)

    def append_audit(self, entity_id, actor_id, actor_role, action, from_status, to_status, detail):
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO audit_log(entity_id, actor_id, actor_role, action, from_status, to_status, detail, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    entity_id,
                    actor_id,
                    actor_role,
                    action,
                    from_status,
                    to_status,
                    json.dumps(detail, ensure_ascii=False, sort_keys=True),
                    utcnow(),
                ),
            )

    def list_audit(self, entity_id=None):
        with self._connect() as connection:
            if entity_id:
                rows = connection.execute(
                    "SELECT * FROM audit_log WHERE entity_id = ? ORDER BY id", (entity_id,)
                ).fetchall()
            else:
                rows = connection.execute("SELECT * FROM audit_log ORDER BY id").fetchall()
        return [
            {
                "id": row["id"],
                "entity_id": row["entity_id"],
                "actor_id": row["actor_id"],
                "actor_role": row["actor_role"],
                "action": row["action"],
                "from_status": row["from_status"],
                "to_status": row["to_status"],
                "detail": json.loads(row["detail"]),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def get_idempotency(self, actor_id, idem_key):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT entity_id FROM idempotency WHERE actor_id = ? AND idem_key = ?",
                (actor_id, idem_key),
            ).fetchone()
        return row["entity_id"] if row else None

    def save_idempotency(self, actor_id, idem_key, entity_id):
        with self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO idempotency(actor_id, idem_key, entity_id, created_at) "
                "VALUES (?, ?, ?, ?)",
                (actor_id, idem_key, entity_id, utcnow()),
            )

    # ---- offline batches ----

    def create_offline_batch(self, batch_id, created_by, total):
        now = utcnow()
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO offline_batches(id, status, total, processed, error, created_by, created_at, updated_at) "
                "VALUES (?, 'processing', ?, 0, NULL, ?, ?, ?)",
                (batch_id, total, created_by, now, now),
            )

    def get_offline_batch(self, batch_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM offline_batches WHERE id = ?", (batch_id,)
            ).fetchone()
        return self._batch_from_row(row) if row else None

    def list_offline_batches(self):
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM offline_batches ORDER BY created_at, id"
            ).fetchall()
        return [self._batch_from_row(row) for row in rows]

    def advance_offline_batch(self, batch_id, processed):
        with self._connect() as connection:
            connection.execute(
                "UPDATE offline_batches SET processed = ?, updated_at = ? WHERE id = ?",
                (processed, utcnow(), batch_id),
            )

    def complete_offline_batch(self, batch_id):
        with self._connect() as connection:
            connection.execute(
                "UPDATE offline_batches SET status = 'completed', error = NULL, updated_at = ? WHERE id = ?",
                (utcnow(), batch_id),
            )

    def fail_offline_batch(self, batch_id, error):
        with self._connect() as connection:
            connection.execute(
                "UPDATE offline_batches SET status = 'failed', error = ?, updated_at = ? WHERE id = ?",
                (str(error), utcnow(), batch_id),
            )

    def reset_offline_batch(self, batch_id):
        with self._connect() as connection:
            connection.execute(
                "UPDATE offline_batches SET status = 'processing', error = NULL, updated_at = ? WHERE id = ?",
                (utcnow(), batch_id),
            )

    @staticmethod
    def _batch_from_row(row):
        return {
            "id": row["id"],
            "status": row["status"],
            "total": int(row["total"]),
            "processed": int(row["processed"]),
            "error": row["error"],
            "created_by": row["created_by"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    # ---- offline records ----

    def upsert_offline_record(self, batch_id, seq, source_id, record_id, kind, status, payload, result):
        digest = hashlib.sha256((source_id + "\0" + record_id).encode("utf-8")).hexdigest()[:32]
        now = utcnow()
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO offline_records(id, batch_id, seq, source_id, record_id, kind, status, payload, result, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(source_id, record_id) DO UPDATE SET "
                "batch_id = excluded.batch_id, seq = excluded.seq, kind = excluded.kind, status = excluded.status, "
                "payload = excluded.payload, result = excluded.result, updated_at = excluded.updated_at",
                (
                    digest,
                    batch_id,
                    seq,
                    source_id,
                    record_id,
                    kind,
                    status,
                    json.dumps(payload, ensure_ascii=False, sort_keys=True),
                    json.dumps(result, ensure_ascii=False, sort_keys=True),
                    now,
                    now,
                ),
            )

    def get_offline_record(self, source_id, record_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM offline_records WHERE source_id = ? AND record_id = ?",
                (source_id, record_id),
            ).fetchone()
        return self._record_from_row(row) if row else None

    def list_offline_records(self, batch_id):
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM offline_records WHERE batch_id = ? ORDER BY seq", (batch_id,)
            ).fetchall()
        return [self._record_from_row(row) for row in rows]

    @staticmethod
    def _record_from_row(row):
        return {
            "id": row["id"],
            "batch_id": row["batch_id"],
            "seq": int(row["seq"]),
            "source_id": row["source_id"],
            "record_id": row["record_id"],
            "kind": row["kind"],
            "status": row["status"],
            "payload": json.loads(row["payload"]),
            "result": json.loads(row["result"]),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def ping(self):
        with self._connect() as connection:
            connection.execute("SELECT 1").fetchone()
        return True
