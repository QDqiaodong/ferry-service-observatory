"""Synthetic schedule CSV and shore-gateway event JSONL contracts.

Two event file formats are supported:

* v1 (default): one logical event per line with keys
  ``event_id, trip_id, kind, occurred_at``. Every record is interpreted as
  ``revision=1`` / ``action=upsert`` of that logical event.
* v2 (``--event-format v2``): explicit revision records. Each line is either an
  ``upsert`` (carrying the business payload) or a ``void`` (logical retraction
  carrying only the logical/revision identity).

All timestamps carry a timezone and canonicalize to UTC. Canonical payloads are
deterministic JSON strings, so equivalent records (different UTC offsets or
different key ordering) normalize to the same identity.
"""
import csv
import json
from datetime import datetime, timezone

KINDS = {"departed", "arrived", "cancelled"}
UPLOAD_KEYS = {"event_id", "revision", "action", "trip_id", "kind", "occurred_at"}
VOID_KEYS = {"event_id", "revision", "action"}


def instant(value):
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("timezone is required")
    return result.astimezone(timezone.utc)


def schedule(path):
    trips = {}
    with open(path, newline="", encoding="utf-8") as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames != ["trip_id", "route", "planned_departure", "planned_arrival"]:
            raise ValueError("invalid schedule headers")
        for row in reader:
            if None in row or any(not v for v in row.values()):
                raise ValueError("malformed schedule row")
            trip, route = row["trip_id"], row["route"]
            departure, arrival = instant(row["planned_departure"]), instant(row["planned_arrival"])
            if trip in trips or arrival <= departure:
                raise ValueError("duplicate trip or nonpositive planned duration")
            trips[trip] = (trip, route, departure.isoformat(), arrival.isoformat())
    if not trips:
        raise ValueError("schedule is empty")
    return trips


def _revision_number(value):
    """Revision must be a positive integer; booleans are explicitly rejected."""
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError("revision must be a positive integer (booleans rejected)")
    return value


def _identifier(value, label):
    if not isinstance(value, str) or not value:
        raise ValueError(f"invalid {label}")
    return value


def upsert_canonical(trip_id, kind, occurred_at):
    """Deterministic normalized payload of an upsert revision."""
    return json.dumps(
        {"kind": kind, "occurred_at": occurred_at, "trip_id": trip_id},
        sort_keys=True, separators=(",", ":"),
    )


def void_canonical():
    """Deterministic normalized payload of a void revision."""
    return json.dumps({"action": "void"}, sort_keys=True, separators=(",", ":"))


def events(path):
    """Parse a v1 JSONL export into normalized revision-1 upsert records."""
    records = []
    with open(path, encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, 1):
            try:
                row = json.loads(line)
                if not isinstance(row, dict) or set(row) != {"event_id", "trip_id", "kind", "occurred_at"}:
                    raise ValueError("event keys differ from v1")
                if any(not isinstance(v, str) or not v for v in row.values()) or row["kind"] not in KINDS:
                    raise ValueError("invalid event identifiers or kind")
                occurred_at = instant(row["occurred_at"]).isoformat()
                event_id, trip_id, kind = row["event_id"], row["trip_id"], row["kind"]
                records.append({
                    "line": line_number,
                    "event_id": event_id,
                    "revision": 1,
                    "action": "upsert",
                    "trip_id": trip_id,
                    "kind": kind,
                    "occurred_at": occurred_at,
                    "canonical": upsert_canonical(trip_id, kind, occurred_at),
                })
            except (ValueError, TypeError, AttributeError) as exc:
                raise ValueError(f"event line {line_number}: {exc}") from exc
    return records


def revisions(path):
    """Parse a v2 JSONL revision export.

    Every line passes its own contract check here, even when a later line in
    the same batch supersedes it: malformed or otherwise invalid revisions are
    never silently accepted just because they will not win selection.
    """
    records = []
    with open(path, encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, 1):
            try:
                row = json.loads(line)
                if not isinstance(row, dict):
                    raise ValueError("revision must be a JSON object")
                action = row.get("action")
                if action not in {"upsert", "void"}:
                    raise ValueError("action must be upsert or void")
                event_id = _identifier(row.get("event_id"), "event_id")
                revision = _revision_number(row.get("revision"))
                if action == "void":
                    if set(row) != VOID_KEYS:
                        raise ValueError("void revision only accepts event_id, revision, action")
                    records.append({
                        "line": line_number,
                        "event_id": event_id,
                        "revision": revision,
                        "action": "void",
                        "trip_id": None,
                        "kind": None,
                        "occurred_at": None,
                        "canonical": void_canonical(),
                    })
                    continue
                if set(row) != UPLOAD_KEYS:
                    raise ValueError("upsert revision requires event_id, revision, action, trip_id, kind, occurred_at")
                trip_id = _identifier(row.get("trip_id"), "trip_id")
                kind = row.get("kind")
                if not isinstance(kind, str) or kind not in KINDS:
                    raise ValueError("invalid kind")
                occurred_at = instant(row["occurred_at"]).isoformat()
                records.append({
                    "line": line_number,
                    "event_id": event_id,
                    "revision": revision,
                    "action": "upsert",
                    "trip_id": trip_id,
                    "kind": kind,
                    "occurred_at": occurred_at,
                    "canonical": upsert_canonical(trip_id, kind, occurred_at),
                })
            except (ValueError, TypeError, AttributeError) as exc:
                raise ValueError(f"event line {line_number}: {exc}") from exc
    return records
