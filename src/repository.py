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
                CREATE TABLE IF NOT EXISTS merge_batches (
                    batch_id TEXT PRIMARY KEY,
                    submitted_by TEXT NOT NULL,
                    total INTEGER NOT NULL,
                    state TEXT NOT NULL,
                    error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS merge_items (
                    batch_id TEXT NOT NULL,
                    uid TEXT NOT NULL,
                    status TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    record_id TEXT NOT NULL,
                    entity_id TEXT,
                    reason TEXT,
                    payload TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(batch_id, uid)
                );
                CREATE INDEX IF NOT EXISTS idx_merge_items_uid
                    ON merge_items(uid);
            """)
            # Migration: legacy rows predate the source field. Backfill it from
            # the original watchkeeper so later reconciliations have an origin.
            rows = connection.execute(
                "SELECT id, data, created_by FROM entities "
                "WHERE data NOT LIKE '%" + '"source_id"' + "%'"
            ).fetchall()
            for row in rows:
                data = json.loads(row["data"])
                data.setdefault("source_id", row["created_by"])
                connection.execute(
                    "UPDATE entities SET data = ? WHERE id = ?",
                    (json.dumps(data, ensure_ascii=False, sort_keys=True), row["id"]),
                )

    # ------------------------------------------------------------------ helpers
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

    def _row_to_entity(self, connection, entity_id):
        row = connection.execute(
            "SELECT * FROM entities WHERE id = ?", (entity_id,)
        ).fetchone()
        return self._entity_from_row(row) if row else None

    def _insert_entity(self, connection, entity_id, kind, status, data, actor_id):
        now = utcnow()
        connection.execute(
            "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
            "VALUES (?, ?, ?, 1, ?, ?, ?, ?)",
            (entity_id, kind, status, json.dumps(data, ensure_ascii=False, sort_keys=True), actor_id, now, now),
        )
        return self._row_to_entity(connection, entity_id)

    def _write_entity(self, connection, entity_id, expected_version, status, data):
        now = utcnow()
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
        cur = connection.execute(
            "UPDATE entities SET status = ?, version = version + 1, data = ?, updated_at = ? "
            "WHERE id = ? AND version = ?",
            (status, json.dumps(data, ensure_ascii=False, sort_keys=True), now, entity_id, current_version),
        )
        if cur.rowcount == 0:
            raise ConflictError("entity was concurrently updated: " + entity_id)
        return self._row_to_entity(connection, entity_id)

    def _append_audit(self, connection, entity_id, actor_id, actor_role, action,
                      from_status, to_status, detail):
        connection.execute(
            "INSERT INTO audit_log(entity_id, actor_id, actor_role, action, from_status, to_status, detail, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                entity_id, actor_id, actor_role, action, from_status, to_status,
                json.dumps(detail or {}, ensure_ascii=False, sort_keys=True), utcnow(),
            ),
        )

    # ------------------------------------------------------------- public reads
    def create_entity(self, entity_id, kind, status, data, actor_id):
        with self._connect() as connection:
            try:
                return self._insert_entity(connection, entity_id, kind, status, data, actor_id)
            except sqlite3.IntegrityError:
                raise ConflictError("entity already exists: " + entity_id)

    def get_entity(self, entity_id):
        with self._connect() as connection:
            return self._row_to_entity(connection, entity_id)

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
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            updated = self._write_entity(connection, entity_id, expected_version, status, data)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return updated

    def append_audit(self, entity_id, actor_id, actor_role, action, from_status, to_status, detail):
        with self._connect() as connection:
            self._append_audit(connection, entity_id, actor_id, actor_role, action,
                               from_status, to_status, detail)

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

    # ------------------------------------------------------------- merge ledger
    def unit_of_work(self):
        """One writer transaction: serializes concurrent batch submissions."""
        connection = self._connect()
        connection.execute("BEGIN IMMEDIATE")
        return _UnitOfWork(self, connection)

    def get_batch(self, batch_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM merge_batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            if not row:
                return None
            items = connection.execute(
                "SELECT * FROM merge_items WHERE batch_id = ? ORDER BY rowid", (batch_id,)
            ).fetchall()
        return {
            "batch_id": row["batch_id"],
            "submitted_by": row["submitted_by"],
            "total": row["total"],
            "state": row["state"],
            "error": row["error"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "items": [
                {
                    "uid": item["uid"],
                    "status": item["status"],
                    "source_id": item["source_id"],
                    "record_id": item["record_id"],
                    "entity_id": item["entity_id"],
                    "reason": item["reason"],
                }
                for item in items
            ],
        }

    def ping(self):
        with self._connect() as connection:
            connection.execute("SELECT 1").fetchone()
        return True


class _UnitOfWork:
    """Transactional gateway used by the reconciliation merge so a record is
    ledgered together with every entity/audit write it performs."""

    def __init__(self, repository, connection):
        self.repository = repository
        self.connection = connection

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.connection.commit()
        else:
            self.connection.rollback()
        self.connection.close()
        return False

    def get(self, entity_id):
        return self.repository._row_to_entity(self.connection, entity_id)

    def list(self, kind):
        rows = self.connection.execute(
            "SELECT * FROM entities WHERE kind = ? ORDER BY created_at, id", (kind,)
        ).fetchall()
        return [self.repository._entity_from_row(row) for row in rows]

    def find(self, kind, field, value):
        entities = self.list(kind)
        if field == "*":
            return entities
        return [
            entity
            for entity in entities
            if (entity["id"] == value if field == "id" else entity["data"].get(field) == value)
        ]

    def insert(self, entity_id, kind, status, data, actor_id):
        try:
            return self.repository._insert_entity(
                self.connection, entity_id, kind, status, data, actor_id
            )
        except sqlite3.IntegrityError:
            raise ConflictError("entity already exists: " + entity_id)

    def update(self, entity_id, expected_version, status, data):
        return self.repository._write_entity(
            self.connection, entity_id, expected_version, status, data
        )

    def audit(self, entity_id, actor, action, from_status, to_status, detail=None):
        self.repository._append_audit(
            self.connection, entity_id, actor.user_id, actor.role, action,
            from_status, to_status, detail or {},
        )

    def get_batch(self, batch_id):
        row = self.connection.execute(
            "SELECT * FROM merge_batches WHERE batch_id = ?", (batch_id,)
        ).fetchone()
        if not row:
            return None
        return {
            "batch_id": row["batch_id"],
            "submitted_by": row["submitted_by"],
            "total": row["total"],
            "state": row["state"],
            "error": row["error"],
        }

    def upsert_batch(self, batch_id, submitted_by, total, state, error=None):
        now = utcnow()
        self.connection.execute(
            "INSERT INTO merge_batches(batch_id, submitted_by, total, state, error, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(batch_id) DO UPDATE SET total = excluded.total, state = excluded.state, "
            "error = excluded.error, updated_at = excluded.updated_at",
            (batch_id, submitted_by, total, state, error, now, now),
        )

    def mark_batch(self, batch_id, state, error=None):
        self.connection.execute(
            "UPDATE merge_batches SET state = ?, error = ?, updated_at = ? WHERE batch_id = ?",
            (state, error, utcnow(), batch_id),
        )

    def put_item(self, batch_id, uid, status, source_id, record_id,
                 entity_id=None, reason=None, payload=None):
        now = utcnow()
        self.connection.execute(
            "INSERT INTO merge_items(batch_id, uid, status, source_id, record_id, entity_id, reason, payload, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(batch_id, uid) DO UPDATE SET status = excluded.status, "
            "entity_id = excluded.entity_id, reason = excluded.reason, "
            "payload = excluded.payload, updated_at = excluded.updated_at",
            (
                batch_id, uid, status, source_id, record_id, entity_id, reason,
                json.dumps(payload, ensure_ascii=False, sort_keys=True) if payload is not None else None,
                now, now,
            ),
        )

    def get_item(self, batch_id, uid):
        row = self.connection.execute(
            "SELECT * FROM merge_items WHERE batch_id = ? AND uid = ?", (batch_id, uid)
        ).fetchone()
        if not row:
            return None
        return {
            "uid": row["uid"],
            "status": row["status"],
            "source_id": row["source_id"],
            "record_id": row["record_id"],
            "entity_id": row["entity_id"],
            "reason": row["reason"],
            "payload": json.loads(row["payload"]) if row["payload"] else None,
        }

    def count_items(self, batch_id):
        row = self.connection.execute(
            "SELECT COUNT(*) AS n FROM merge_items WHERE batch_id = ?", (batch_id,)
        ).fetchone()
        return int(row["n"])
