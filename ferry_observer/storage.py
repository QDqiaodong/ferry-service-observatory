"""Immutable revision ledger, knowledge sequencing and explicit legacy upgrade.

Corrections and retractions never overwrite or delete history. Every accepted
revision is stored in ``revisions`` keyed by the immutable identity
``(event_id, revision)``. A batch that introduces at least one new revision is
assigned one monotonically increasing ``knowledge_seq`` inside the same
transaction as the revisions, the current event projection and the source
registration. Empty or fully duplicate files register the source but consume no
sequence number.
"""
import hashlib
import sqlite3
from collections import defaultdict
from pathlib import Path

from .contracts import events as parse_v1
from .contracts import revisions as parse_v2
from .contracts import schedule, upsert_canonical
from .reconcile import lifecycle

SCHEMA_VERSION = 2
LOCK_WAIT_SECONDS = 5

SCHEMA = [
    """CREATE TABLE IF NOT EXISTS trips (
 trip_id TEXT PRIMARY KEY, route TEXT NOT NULL, planned_departure TEXT NOT NULL,
 planned_arrival TEXT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS revisions (
 event_id TEXT NOT NULL,
 revision INTEGER NOT NULL CHECK (revision >= 1),
 action TEXT NOT NULL CHECK (action IN ('upsert','void')),
 trip_id TEXT,
 kind TEXT,
 occurred_at TEXT,
 canonical_json TEXT NOT NULL,
 source_sha256 TEXT,
 knowledge_seq INTEGER NOT NULL REFERENCES knowledge_batches(knowledge_seq),
 PRIMARY KEY (event_id, revision))""",
    """CREATE TABLE IF NOT EXISTS event_projection (
 event_id TEXT PRIMARY KEY,
 trip_id TEXT NOT NULL REFERENCES trips(trip_id),
 kind TEXT NOT NULL,
 occurred_at TEXT NOT NULL,
 revision INTEGER NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS knowledge_batches (
 knowledge_seq INTEGER PRIMARY KEY CHECK (knowledge_seq >= 1),
 note TEXT)""",
    """CREATE TABLE IF NOT EXISTS sources (sha256 TEXT PRIMARY KEY, row_count INTEGER NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)""",
]

LEGACY_NOTE = (
    "legacy_unattributed: revision 1 holds the complete set of facts known when "
    "the pre-revision database was migrated; per-event source mapping and the "
    "fine-grained ingestion history before migration cannot be recovered, so "
    "migrated revisions have no source_sha256 or received timestamp"
)


def _connect(database):
    db = sqlite3.connect(database, timeout=LOCK_WAIT_SECONDS)
    db.execute("PRAGMA foreign_keys=ON")
    db.execute(f"PRAGMA busy_timeout={LOCK_WAIT_SECONDS * 1000}")
    return db


def database_kind(db):
    """Classify an open database: ``v2``, ``legacy`` or ``empty``."""
    tables = {
        r[0]
        for r in db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }
    if "revisions" in tables:
        return "v2"
    if "events" in tables:
        return "legacy"
    if tables:
        raise ValueError("unrecognized database layout; expected a ferry observatory database")
    return "empty"


def _create_v2(db):
    for statement in SCHEMA:
        db.execute(statement)
    db.execute("INSERT INTO meta(key,value) VALUES ('schema_version',?)", (str(SCHEMA_VERSION),))


def _load_ledger(db):
    ledger = defaultdict(dict)
    for (
        event_id, revision, action, trip_id, kind, occurred_at,
        canonical_json, source_sha256, knowledge_seq,
    ) in db.execute(
        "SELECT event_id,revision,action,trip_id,kind,occurred_at,"
        "canonical_json,source_sha256,knowledge_seq FROM revisions"
    ):
        ledger[event_id][revision] = {
            "event_id": event_id,
            "revision": revision,
            "action": action,
            "trip_id": trip_id,
            "kind": kind,
            "occurred_at": occurred_at,
            "canonical": canonical_json,
            "source_sha256": source_sha256,
            "knowledge_seq": knowledge_seq,
        }
    return ledger


def _current_seq(db):
    row = db.execute("SELECT max(knowledge_seq) FROM knowledge_batches").fetchone()
    return row[0] or 0


def _validate_versions(ledger, new_by_event, trips):
    """Continuity, first-upsert, fixed trip identity and known-trip checks.

    The check looks at the union of the accepted ledger and the whole candidate
    batch, so out-of-order files carrying several consecutive revisions are
    judged on the combined result rather than on a misleading row order.
    """
    for event_id in sorted(set(ledger) | set(new_by_event)):
        versions = dict(ledger.get(event_id, {}))
        for rec in new_by_event.get(event_id, ()):
            versions[rec["revision"]] = rec
        ordered = sorted(versions)
        if ordered[0] != 1 or ordered != list(range(1, len(ordered) + 1)):
            raise ValueError(
                f"event {event_id}: revisions must start at 1 and be "
                f"continuous; got {ordered}"
            )
        trip_id = None
        for revision in ordered:
            version = versions[revision]
            if version["action"] == "upsert":
                if trip_id is None:
                    if revision != 1:
                        raise ValueError(
                            f"event {event_id}: revision 1 must be an upsert"
                        )
                    trip_id = version["trip_id"]
                elif version["trip_id"] != trip_id:
                    raise ValueError(
                        f"event {event_id} revision {revision}: trip_id cannot "
                        f"change from {trip_id} to {version['trip_id']}"
                    )
            elif trip_id is None:
                raise ValueError(
                    f"event {event_id} revision {revision}: cannot void an "
                    "event that has never been upserted"
                )
        if trip_id not in trips:
            raise ValueError(
                f"event {event_id} references unknown trip {trip_id}"
            )


def _project_final(ledger, new_by_event):
    """Highest revision per logical event over ledger + batch, voids removed."""
    projection = {}
    for event_id in set(ledger) | set(new_by_event):
        versions = dict(ledger.get(event_id, {}))
        for rec in new_by_event.get(event_id, ()):
            versions[rec["revision"]] = rec
        winner = versions[max(versions)]
        if winner["action"] == "upsert":
            projection[event_id] = winner
    return projection

def ingest(schedule_path, events_path, database, event_format="v1"):
    """Acquire one export file inside a single serializable transaction."""
    if event_format not in {"v1", "v2"}:
        raise ValueError("event_format must be 'v1' or 'v2'")
    trips = schedule(schedule_path)
    parsed = parse_v1(events_path) if event_format == "v1" else parse_v2(events_path)
    digest = hashlib.sha256(Path(events_path).read_bytes()).hexdigest()
    Path(database).parent.mkdir(parents=True, exist_ok=True)

    db = _connect(database)
    try:
        try:
            # Serializes concurrent writers so two batches can never share a
            # knowledge_seq; busy_timeout bounds the lock wait. The schema is
            # inspected only after the lock is held, otherwise a writer that
            # waited on a brand-new database would observe stale emptiness and
            # re-run table creation against the first writer's committed schema.
            db.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError as exc:
            raise ValueError(
                f"database lock not acquired within {LOCK_WAIT_SECONDS}s: {exc}"
            ) from exc
        try:
            kind = database_kind(db)
            if kind == "legacy":
                raise ValueError(
                    "database uses the legacy pre-revision schema; run "
                    "--upgrade-db before ingestion"
                )
            if kind == "empty":
                _create_v2(db)
            existing = {r[0]: r for r in db.execute("SELECT * FROM trips")}
            if existing and existing != trips:
                raise ValueError("schedule is immutable for this database")
            if not existing:
                db.executemany("INSERT INTO trips VALUES (?,?,?,?)", trips.values())

            ledger = _load_ledger(db)

            # Pass 1: detect conflicting records for one revision identity
            # inside this very file (identical repeats are allowed).
            candidates = {}
            for rec in parsed:
                key = (rec["event_id"], rec["revision"])
                present = candidates.get(key)
                if present is not None:
                    if present["action"] != rec["action"] or present["canonical"] != rec["canonical"]:
                        raise ValueError(
                            f"event line {rec['line']}: conflicting records for "
                            f"{rec['event_id']} revision {rec['revision']} in the same batch"
                        )
                    continue
                candidates[key] = rec

            # Pass 2: ledger conflict check and duplicate accounting. Every
            # input row is classified; a superseded older revision replayed
            # later still only counts as a duplicate and can never re-activate.
            duplicates = 0
            new_records = []
            seen_new = set()
            for rec in parsed:
                key = (rec["event_id"], rec["revision"])
                prior = ledger.get(rec["event_id"], {}).get(rec["revision"])
                if prior is not None:
                    if prior["action"] != rec["action"] or prior["canonical"] != rec["canonical"]:
                        raise ValueError(
                            f"event line {rec['line']}: revision conflict for "
                            f"{rec['event_id']} revision {rec['revision']}; that "
                            "revision identity is already accepted with a "
                            "different normalized payload"
                        )
                    duplicates += 1
                    continue
                if key in seen_new:
                    duplicates += 1
                    continue
                seen_new.add(key)
                new_records.append(rec)

            new_by_event = defaultdict(list)
            for rec in new_records:
                new_by_event[rec["event_id"]].append(rec)

            _validate_versions(ledger, new_by_event, trips)
            projection = _project_final(ledger, new_by_event)

            # Lifecycle validation runs on the final merged state only, so a
            # batch that temporarily looks invalid row-by-row (for example
            # moving both departure and arrival later together) still applies.
            grouped = defaultdict(list)
            for rec in projection.values():
                grouped[rec["trip_id"]].append(
                    {
                        "event_id": rec["event_id"],
                        "trip_id": rec["trip_id"],
                        "kind": rec["kind"],
                        "occurred_at": rec["occurred_at"],
                    }
                )
            for trip_id in sorted(grouped):
                try:
                    lifecycle(grouped[trip_id])
                except ValueError as exc:
                    raise ValueError(f"trip {trip_id}: {exc}") from exc

            next_seq = _current_seq(db) + 1 if new_records else None
            if new_records:
                db.execute(
                    "INSERT INTO knowledge_batches(knowledge_seq,note) VALUES (?,?)",
                    (next_seq, None),
                )
                for rec in new_records:
                    db.execute(
                        "INSERT INTO revisions(event_id,revision,action,trip_id,"
                        "kind,occurred_at,canonical_json,source_sha256,knowledge_seq) "
                        "VALUES (?,?,?,?,?,?,?,?,?)",
                        (
                            rec["event_id"], rec["revision"], rec["action"],
                            rec["trip_id"], rec["kind"], rec["occurred_at"],
                            rec["canonical"], digest, next_seq,
                        ),
                    )

            touched = {rec["event_id"] for rec in new_records}
            if touched:
                db.executemany(
                    "DELETE FROM event_projection WHERE event_id=?",
                    [(event_id,) for event_id in touched],
                )
                db.executemany(
                    "INSERT INTO event_projection(event_id,trip_id,kind,"
                    "occurred_at,revision) VALUES (?,?,?,?,?)",
                    [
                        (
                            event_id, rec["trip_id"], rec["kind"],
                            rec["occurred_at"], rec["revision"],
                        )
                        for event_id, rec in projection.items()
                        if event_id in touched
                    ],
                )

            # Source registration survives empty and fully duplicate files but
            # never drives knowledge_seq by itself.
            db.execute(
                "INSERT OR IGNORE INTO sources(sha256,row_count) VALUES (?,?)",
                (digest, len(parsed)),
            )
            total_events = db.execute(
                "SELECT count(*) FROM event_projection"
            ).fetchone()[0]
            knowledge_seq = _current_seq(db)
            db.commit()
        except Exception:
            db.rollback()
            raise
    finally:
        db.close()

    return {
        "input_rows": len(parsed),
        "inserted": len(new_records),
        "duplicates": duplicates,
        "total_events": total_events,
        "source_sha256": digest,
        "knowledge_seq": knowledge_seq,
        "new_revisions": len(new_records),
        "active_events": total_events,
    }


def upgrade_database(database):
    """Explicit transactional upgrade of a legacy database to the v2 ledger.

    Legacy ``trips``, ``events`` and ``sources`` facts are preserved untouched.
    Legacy events become revision-1 upserts in synthetic knowledge version 1; a
    legacy database without events stays at knowledge version 0. Re-running on an
    already upgraded database is an idempotent no-op.
    """
    path = Path(database)
    if not path.exists():
        raise ValueError("upgrade requires an existing database file")
    db = _connect(path)
    try:
        kind = database_kind(db)
        if kind == "v2":
            seq = _current_seq(db)
            return {
                "upgraded": False,
                "reason": "database already at schema version 2",
                "knowledge_seq": seq,
                "migrated_events": 0,
            }
        if kind == "empty":
            raise ValueError("database contains no ferry tables; nothing to upgrade")
        for required in ("trips", "events", "sources"):
            present = db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (required,),
            ).fetchone()
            if not present:
                raise ValueError(f"legacy database is missing table {required}")
        db.execute("BEGIN IMMEDIATE")
        try:
            _create_v2(db)
            db.execute(
                "INSERT INTO meta(key,value) VALUES ('legacy_note',?)",
                (LEGACY_NOTE,),
            )
            migrated = 0
            legacy_events = db.execute(
                "SELECT event_id,trip_id,kind,occurred_at FROM events"
            ).fetchall()
            if legacy_events:
                db.execute(
                    "INSERT INTO knowledge_batches(knowledge_seq,note) VALUES (?,?)",
                    (1, "legacy_migration"),
                )
            for event_id, trip_id, kind_name, occurred_at in legacy_events:
                canonical = upsert_canonical(trip_id, kind_name, occurred_at)
                db.execute(
                    "INSERT INTO revisions(event_id,revision,action,trip_id,"
                    "kind,occurred_at,canonical_json,source_sha256,knowledge_seq) "
                    "VALUES (?,?,?,?,?,?,?,?,?)",
                    (
                        event_id, 1, "upsert", trip_id, kind_name, occurred_at,
                        canonical, None, 1,
                    ),
                )
                db.execute(
                    "INSERT INTO event_projection(event_id,trip_id,kind,"
                    "occurred_at,revision) VALUES (?,?,?,?,?)",
                    (event_id, trip_id, kind_name, occurred_at, 1),
                )
                migrated += 1
            db.commit()
        except Exception:
            db.rollback()
            raise
    finally:
        db.close()
    return {
        "upgraded": True,
        "knowledge_seq": 1 if migrated else 0,
        "migrated_events": migrated,
        "note": LEGACY_NOTE,
    }
