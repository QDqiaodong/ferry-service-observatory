"""Immutable schedules and idempotent event ingestion in SQLite transactions."""
import hashlib
import json
import sqlite3
from collections import defaultdict
from pathlib import Path
from .contracts import schedule, events
from .reconcile import lifecycle

SCHEMA = """
CREATE TABLE IF NOT EXISTS trips (
 trip_id TEXT PRIMARY KEY, route TEXT NOT NULL, planned_departure TEXT NOT NULL,
 planned_arrival TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS events (
 event_id TEXT PRIMARY KEY, trip_id TEXT NOT NULL REFERENCES trips(trip_id),
 kind TEXT NOT NULL, occurred_at TEXT NOT NULL, canonical_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS sources (sha256 TEXT PRIMARY KEY, row_count INTEGER NOT NULL);
"""


def read_events(db):
    groups = defaultdict(list)
    for event_id, trip_id, kind, occurred_at in db.execute("SELECT event_id,trip_id,kind,occurred_at FROM events"):
        groups[trip_id].append(dict(event_id=event_id,trip_id=trip_id,kind=kind,occurred_at=occurred_at))
    return groups


def ingest(schedule_path, events_path, database):
    trips, incoming = schedule(schedule_path), events(events_path)
    Path(database).parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(database) as db:
        db.execute("PRAGMA foreign_keys=ON")
        db.executescript(SCHEMA)
        existing = {r[0]:r for r in db.execute("SELECT * FROM trips")}
        if existing and existing != trips:
            raise ValueError("schedule is immutable for this database")
        if not existing:
            db.executemany("INSERT INTO trips VALUES (?,?,?,?)", trips.values())
        inserted = 0
        for event in incoming:
            if event["trip_id"] not in trips:
                raise ValueError("event references unknown trip")
            canonical = json.dumps(event, sort_keys=True, separators=(",", ":"))
            previous = db.execute("SELECT canonical_json FROM events WHERE event_id=?", (event["event_id"],)).fetchone()
            if previous:
                if previous[0] != canonical:
                    raise ValueError("conflicting event identity")
                continue
            db.execute("INSERT INTO events VALUES (?,?,?,?,?)", (event["event_id"],event["trip_id"],event["kind"],event["occurred_at"],canonical))
            inserted += 1
        for records in read_events(db).values():
            lifecycle(records)
        digest = hashlib.sha256(Path(events_path).read_bytes()).hexdigest()
        db.execute("INSERT OR IGNORE INTO sources VALUES (?,?)", (digest,len(incoming)))
        total = db.execute("SELECT count(*) FROM events").fetchone()[0]
    return {"input_rows": len(incoming), "inserted": inserted, "duplicates": len(incoming)-inserted, "total_events":total, "source_sha256":digest}
