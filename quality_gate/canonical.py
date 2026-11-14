"""Independent timestamp, canonical-payload and JSONL/CSV primitives.

Deliberately written from the contract instead of reusing
``ferry_observer.contracts``: if these helpers merely called the production
parser the gate would lose its independence. Only the Python standard
library is used.
"""
import csv
import json
from datetime import datetime, timezone

KINDS = {"departed", "arrived", "cancelled"}


class ContractError(ValueError):
    """Raised when one rendered input line violates the event contract."""


def parse_instant(value):
    """Parse a timezone-aware ISO 8601 string and normalize it to UTC."""
    if not isinstance(value, str) or not value:
        raise ContractError("timestamp must be a nonempty string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ContractError(f"unparseable timestamp {value!r}") from exc
    if parsed.tzinfo is None:
        raise ContractError("timestamp is missing its timezone")
    return parsed.astimezone(timezone.utc)


def instant_iso(value):
    """Normalized UTC representation used inside stored revision records.

    The rendering (``...+00:00``) deliberately matches the documented
    canonicalization of the production parser so that full-table byte
    comparison is meaningful; the instant itself is timezone-normalized.
    """
    return parse_instant(value).isoformat()


def upsert_canonical(trip_id, kind, occurred_at):
    """Deterministic compact JSON payload for an upsert revision."""
    return json.dumps(
        {"kind": kind, "occurred_at": occurred_at, "trip_id": trip_id},
        sort_keys=True,
        separators=(",", ":"),
    )


def void_canonical():
    """Deterministic compact JSON payload for a void revision."""
    return json.dumps({"action": "void"}, sort_keys=True, separators=(",", ":"))


def _check_identifier(value, label):
    if not isinstance(value, str) or not value:
        raise ContractError(f"invalid {label}")
    return value


def _check_revision(value):
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ContractError("revision must be a positive integer (booleans rejected)")
    return value


def parse_revision_line(line, line_number):
    """Parse one v2 JSONL line into a normalized revision record.

    This is the oracle's own contract check. It mirrors the documented v2
    shape (exact key sets, positive integer revisions, UTC normalization)
    without importing the production parser.
    """
    try:
        row = json.loads(line)
    except ValueError as exc:
        raise ContractError(f"line {line_number}: not valid JSON: {exc}") from exc
    if not isinstance(row, dict):
        raise ContractError(f"line {line_number}: revision must be a JSON object")
    action = row.get("action")
    if action not in {"upsert", "void"}:
        raise ContractError(f"line {line_number}: action must be upsert or void")
    event_id = _check_identifier(row.get("event_id"), "event_id")
    revision = _check_revision(row.get("revision"))
    if action == "void":
        if set(row) != {"event_id", "revision", "action"}:
            raise ContractError(
                f"line {line_number}: void revision only accepts "
                "event_id, revision, action"
            )
        return {
            "event_id": event_id,
            "revision": revision,
            "action": "void",
            "trip_id": None,
            "kind": None,
            "occurred_at": None,
            "canonical": void_canonical(),
        }
    if set(row) != {
        "event_id", "revision", "action", "trip_id", "kind", "occurred_at",
    }:
        raise ContractError(
            f"line {line_number}: upsert revision requires the six v2 keys"
        )
    trip_id = _check_identifier(row.get("trip_id"), "trip_id")
    kind = row.get("kind")
    if not isinstance(kind, str) or kind not in KINDS:
        raise ContractError(f"line {line_number}: invalid kind")
    occurred_at = instant_iso(row["occurred_at"])
    return {
        "event_id": event_id,
        "revision": revision,
        "action": "upsert",
        "trip_id": trip_id,
        "kind": kind,
        "occurred_at": occurred_at,
        "canonical": upsert_canonical(trip_id, kind, occurred_at),
    }


def parse_revision_file(path):
    """Parse a whole JSONL file into normalized records (oracle's parser)."""
    records = []
    with open(path, encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, 1):
            if not line.strip():
                raise ContractError(f"line {line_number}: blank lines are not allowed")
            records.append(parse_revision_line(line, line_number))
    return records


def parse_schedule(path):
    """Parse the fixed schedule CSV into an ordered trip mapping."""
    trips = {}
    with open(path, newline="", encoding="utf-8") as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames != [
            "trip_id", "route", "planned_departure", "planned_arrival",
        ]:
            raise ContractError("invalid schedule headers")
        for row in reader:
            if None in row or any(not v for v in row.values()):
                raise ContractError("malformed schedule row")
            departure = parse_instant(row["planned_departure"])
            arrival = parse_instant(row["planned_arrival"])
            if row["trip_id"] in trips or arrival <= departure:
                raise ContractError("duplicate trip or nonpositive planned duration")
            trips[row["trip_id"]] = {
                "trip_id": row["trip_id"],
                "route": row["route"],
                "planned_departure": instant_iso(row["planned_departure"]),
                "planned_arrival": instant_iso(row["planned_arrival"]),
            }
    if not trips:
        raise ContractError("schedule is empty")
    return trips
