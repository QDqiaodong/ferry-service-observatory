"""Synthetic schedule CSV and shore-gateway event JSONL contracts."""
import csv
import json
from datetime import datetime, timezone


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


def events(path):
    records = []
    with open(path, encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, 1):
            try:
                row = json.loads(line)
                if set(row) != {"event_id", "trip_id", "kind", "occurred_at"}:
                    raise ValueError("event keys differ from v1")
                if any(not isinstance(v,str) or not v for v in row.values()) or row["kind"] not in {"departed", "arrived", "cancelled"}:
                    raise ValueError("invalid event identifiers or kind")
                row["occurred_at"] = instant(row["occurred_at"]).isoformat()
                records.append(row)
            except (ValueError, TypeError, AttributeError) as exc:
                raise ValueError(f"event line {line_number}: {exc}") from exc
    return records
