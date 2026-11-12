"""As-of service performance with a dual cutoff and revision evidence.

Two independent cutoffs decide which events contribute:

1. Knowledge cutoff ``K``: only revisions whose first accepting batch has
   ``knowledge_seq <= K`` are visible. For each logical event the highest
   revision visible at ``K`` wins and voids remove the event entirely.
2. Event-time cutoff ``T`` (``as_of``): among the surviving projected events,
   only those with ``occurred_at <= T`` enter the lifecycle calculation.

The order matters. Filtering by event time before choosing the revision would
silently resurrect an old, corrected value whenever the correction moved the
event after ``T``; selecting revisions first makes that case correctly look
like the event is unknown at the earlier cutoff.
"""
import csv
import json
import sqlite3
from collections import defaultdict
from pathlib import Path

from .contracts import instant
from .reconcile import lifecycle
from .storage import LEGACY_NOTE, SCHEMA_VERSION, database_kind


def _read_only_connect(database):
    path = Path(database)
    if not path.exists():
        raise ValueError(f"database does not exist: {database}")
    uri = f"file:{path.resolve()}?mode=ro"
    db = sqlite3.connect(uri, uri=True)
    db.execute("PRAGMA query_only=ON")
    kind = database_kind(db)
    if kind == "legacy":
        db.close()
        raise ValueError("database uses the legacy schema; run --upgrade-db before reporting")
    if kind == "empty":
        db.close()
        raise ValueError("database contains no ferry tables; ingest data before reporting")
    version = db.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
    if version is None or int(version[0]) != SCHEMA_VERSION:
        db.close()
        raise ValueError("unsupported database schema version")
    return db


def _revisions_at(db, known_at):
    """All revisions accepted up to and including knowledge sequence K."""
    rows = db.execute(
        "SELECT event_id,revision,action,trip_id,kind,occurred_at,"
        "canonical_json,source_sha256,knowledge_seq "
        "FROM revisions WHERE knowledge_seq <= ? "
        "ORDER BY event_id,revision",
        (known_at,),
    ).fetchall()
    revisions = []
    for event_id, revision, action, trip_id, kind, occurred_at, canonical_json, source_sha256, seq in rows:
        revisions.append(
            {
                "event_id": event_id,
                "revision": revision,
                "action": action,
                "trip_id": trip_id,
                "kind": kind,
                "occurred_at": occurred_at,
                "canonical_json": canonical_json,
                "source_sha256": source_sha256,
                "knowledge_seq": seq,
            }
        )
    return revisions


def _projection(revisions):
    """Highest revision per event over an already knowledge-limited set."""
    winners = {}
    for rec in revisions:
        current = winners.get(rec["event_id"])
        if current is None or rec["revision"] > current["revision"]:
            winners[rec["event_id"]] = rec
    return winners


def performance(database, as_of, known_at=None):
    cutoff = instant(as_of)
    db = _read_only_connect(database)
    try:
        # One read transaction holds a single snapshot across plan, revisions,
        # projection basis and source evidence.
        db.execute("BEGIN")
        latest_seq = db.execute(
            "SELECT max(knowledge_seq) FROM knowledge_batches"
        ).fetchone()[0] or 0
        if known_at is None:
            known_at = latest_seq
        if isinstance(known_at, bool) or not isinstance(known_at, int) or known_at < 0 or known_at > latest_seq:
            raise ValueError(
                f"known_at must be an integer between 0 and {latest_seq}; got {known_at!r}"
            )

        revisions = _revisions_at(db, known_at)
        winners = _projection(revisions)

        # Resolve the trip of void-only revisions from the event's own history.
        event_trip = {}
        for rec in revisions:
            if rec["action"] == "upsert":
                event_trip.setdefault(rec["event_id"], rec["trip_id"])

        # Evidence per trip: every revision known at K, with an explicit status
        # explaining whether it drove the result and, if not, why not.
        evidence_by_trip = defaultdict(list)
        for rec in revisions:
            trip_id = event_trip.get(rec["event_id"])
            winner = winners[rec["event_id"]]
            if rec["revision"] != winner["revision"]:
                # A replaced revision, including one replaced by a later void.
                status = "superseded"
            elif winner["action"] == "void":
                status = "voided"
            elif instant(winner["occurred_at"]) > cutoff:
                status = "after_cutoff"
            else:
                status = "used"
            entry = {
                "event_id": rec["event_id"],
                "revision": rec["revision"],
                "action": rec["action"],
                "knowledge_seq": rec["knowledge_seq"],
                "canonical_payload": json.loads(rec["canonical_json"]),
                "first_source_sha256": rec["source_sha256"],
                "status": status,
            }
            if rec["source_sha256"] is None:
                entry["source_note"] = LEGACY_NOTE
            evidence_by_trip[trip_id].append(entry)
            # Event still untrippable only when malformed data existed, which
            # the ingestion contract rejects; guard anyway.
            if trip_id is None:
                raise ValueError(
                    f"event {rec['event_id']} has no upsert revision to establish its trip"
                )

        projected_events = defaultdict(list)
        for rec in winners.values():
            if rec["action"] != "upsert":
                continue
            if instant(rec["occurred_at"]) > cutoff:
                continue
            projected_events[rec["trip_id"]].append(
                {
                    "event_id": rec["event_id"],
                    "trip_id": rec["trip_id"],
                    "kind": rec["kind"],
                    "occurred_at": rec["occurred_at"],
                }
            )

        details = []
        for trip, route, planned_departure, planned_arrival in db.execute(
            "SELECT * FROM trips ORDER BY planned_departure,trip_id"
        ):
            planned = instant(planned_departure)
            if planned > cutoff:
                continue
            visible = projected_events.get(trip, [])
            state, departure, arrival = lifecycle(visible)
            delay = (departure - planned).total_seconds() if departure is not None else None
            details.append(
                {
                    "trip_id": trip,
                    "route": route,
                    "state": state,
                    "departure_delay_seconds": delay,
                    "on_time": None if delay is None else 0 <= delay <= 300,
                    "travel_seconds": (arrival - departure).total_seconds() if arrival else None,
                    "knowledge_seq": known_at,
                    "event_revisions": evidence_by_trip.get(trip, []),
                }
            )
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

    routes = []
    for route in sorted({d["route"] for d in details}):
        rows = [d for d in details if d["route"] == route]
        operated = [r for r in rows if r["departure_delay_seconds"] is not None]
        travelled = [r["travel_seconds"] for r in rows if r["travel_seconds"] is not None]
        cancelled = sum(r["state"] == "cancelled" for r in rows)
        ontime = sum(r["on_time"] is True for r in rows)
        routes.append(
            {
                "route": route,
                "due_trips": len(rows),
                "departed_trips": len(operated),
                "cancelled_trips": cancelled,
                "pending_trips": sum(r["state"] == "pending" for r in rows),
                "arrived_trips": len(travelled),
                "on_time_departures": ontime,
                "on_time_rate": ontime / len(operated) if operated else None,
                "cancellation_rate": cancelled / len(rows),
                "mean_departure_delay_seconds": (
                    sum(r["departure_delay_seconds"] for r in operated) / len(operated)
                    if operated else None
                ),
                "mean_travel_seconds": sum(travelled) / len(travelled) if travelled else None,
            }
        )
    return {
        "schema_version": 2,
        "as_of": cutoff.isoformat(),
        "knowledge_seq": known_at,
        "routes": routes,
        "trips": details,
    }


def write_report(report, output):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    (output / "performance.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    with (output / "trips.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=[
                "trip_id", "route", "state", "departure_delay_seconds",
                "on_time", "travel_seconds", "knowledge_seq",
            ],
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(report["trips"])
