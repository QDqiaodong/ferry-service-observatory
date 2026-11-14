"""Offline connection (transfer) analysis over the committed ledger.

A passenger asks: starting at ``origin`` at ``ready_at``, which journeys to
``destination`` are worth considering? The answer is the Pareto frontier over
three metrics -- arrival time, total fare, transfer count -- computed from
*actual* departure/arrival times of trips that had arrived by the cutoff.

Selection of facts follows the existing dual-cutoff rule exactly: revisions
with ``knowledge_seq <= K`` are visible, the highest revision per event wins
and voids retract, and only then are events with ``occurred_at <= T`` kept.
Only trips whose projected lifecycle is ``arrived`` become candidates, with
their actual times; planned times never substitute for unknown actuals.

Journey rules: the first leg departs at or after ``ready_at``; every later
leg departs from the port the previous leg arrived at, at or after
``previous arrival + transfer_seconds[port]``.

The frontier is computed with a multi-criteria label scan in departure
order, keeping a Pareto-optimal label set per port. It neither collapses
each port to its earliest label (which would lose later-but-cheaper
options) nor enumerates all paths (dominated labels are pruned as labels
are created). Among journeys with identical metrics only the
lexicographically smallest trip_id sequence is kept.

The report is written by deterministic atomic replacement: identical
inputs yield byte-identical JSON, and a bad request or a failed write
leaves any previous report untouched.
"""
import json
import os
import tempfile
from collections import defaultdict
from datetime import timedelta
from pathlib import Path

from .analysis import _projection, _read_only_connect, _revisions_at
from .contracts import instant
from .reconcile import lifecycle
from .storage import LEGACY_NOTE

CONNECTIONS_SCHEMA_VERSION = 1
REQUEST_KEYS = {
    "schema_version", "routes", "transfer_seconds",
    "origin", "destination", "ready_at",
}
ROUTE_KEYS = {"origin", "destination", "fare_cents"}

NO_CANDIDATES = "no_candidate_trips"
ORIGIN_NOT_SERVED = "origin_not_served"
DESTINATION_NOT_SERVED = "destination_not_served"
NO_CONNECTION = "no_connection"


# --------------------------------------------------------------------- request
def _reject_constant(value):
    raise ValueError(f"invalid JSON constant {value}")


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate key {key!r}")
        result[key] = value
    return result


def load_request(path):
    """Read a request file as strict JSON (no duplicate keys, no NaN)."""
    try:
        text = Path(path).read_text(encoding="utf-8")
    except OSError as exc:
        raise ValueError(f"request file unreadable: {exc}") from exc
    try:
        return json.loads(
            text, object_pairs_hook=_unique_object, parse_constant=_reject_constant
            )
    except ValueError as exc:
        raise ValueError(f"request is not valid strict JSON: {exc}") from exc


def _non_negative_int(value, label):
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{label} must be a non-negative integer (booleans rejected)")
    return value


def _port_name(value, label):
    if not isinstance(value, str) or not value:
        raise ValueError(f"invalid {label}")
    return value


def validate_request(data):
    """Validate the raw request mapping and return its normalized form.

    Normalization canonicalizes ``ready_at`` to UTC. Port references must be
    complete inside the request itself: every route endpoint and the journey
    origin/destination must have a ``transfer_seconds`` entry. Coverage of the
    database routes is checked separately by :func:`connections`, which owns
    the snapshot.
    """
    if not isinstance(data, dict) or set(data) != REQUEST_KEYS:
        raise ValueError(
            "request must be a JSON object with exactly "
            f"{sorted(REQUEST_KEYS)}"
        )
    version = data["schema_version"]
    if isinstance(version, bool) or not isinstance(version, int) or version != CONNECTIONS_SCHEMA_VERSION:
        raise ValueError(f"schema_version must be {CONNECTIONS_SCHEMA_VERSION}")

    routes = data["routes"]
    if not isinstance(routes, dict) or not routes:
        raise ValueError("routes must be a non-empty object")
    normalized_routes = {}
    for route, spec in routes.items():
        _port_name(route, "route name")
        if not isinstance(spec, dict) or set(spec) != ROUTE_KEYS:
            raise ValueError(
                f"route {route}: mapping must have exactly {sorted(ROUTE_KEYS)}"
            )
        origin = _port_name(spec["origin"], f"route {route} origin")
        destination = _port_name(spec["destination"], f"route {route} destination")
        if origin == destination:
            raise ValueError(f"route {route}: origin and destination must differ")
        fare = _non_negative_int(spec["fare_cents"], f"route {route} fare_cents")
        normalized_routes[route] = {
            "origin": origin, "destination": destination, "fare_cents": fare,
        }

    transfer = data["transfer_seconds"]
    if not isinstance(transfer, dict) or not transfer:
        raise ValueError("transfer_seconds must be a non-empty object")
    normalized_transfer = {}
    for port, seconds in transfer.items():
        _port_name(port, "port name")
        normalized_transfer[port] = _non_negative_int(
            seconds, f"transfer_seconds[{port!r}]"
        )

    origin = _port_name(data["origin"], "origin")
    destination = _port_name(data["destination"], "destination")
    if origin == destination:
        raise ValueError("origin and destination must differ")

    referenced = {origin, destination}
    for spec in normalized_routes.values():
        referenced.add(spec["origin"])
        referenced.add(spec["destination"])
    missing = sorted(referenced - set(normalized_transfer))
    if missing:
        raise ValueError(f"ports missing from transfer_seconds: {missing}")

    ready_at = data["ready_at"]
    if not isinstance(ready_at, str):
        raise ValueError("ready_at must be a timezone-aware timestamp string")
    ready_at = instant(ready_at).isoformat()

    return {
        "schema_version": CONNECTIONS_SCHEMA_VERSION,
        "routes": normalized_routes,
        "transfer_seconds": normalized_transfer,
        "origin": origin,
        "destination": destination,
        "ready_at": ready_at,
    }


# -------------------------------------------------------------------- analysis
def _evidence(rec):
    entry = {
        "event_id": rec["event_id"],
        "revision": rec["revision"],
        "action": rec["action"],
        "knowledge_seq": rec["knowledge_seq"],
        "kind": rec["kind"],
        "occurred_at": rec["occurred_at"],
        "first_source_sha256": rec["source_sha256"],
    }
    if rec["source_sha256"] is None:
        entry["source_note"] = LEGACY_NOTE
    return entry


def _leq_metrics(a, b):
    return a[0] <= b[0] and a[1] <= b[1] and a[2] <= b[2]


def _covers_intermediate(a, b):
    """Pruning order at intermediate ports.

    An arrival-time advantage can evaporate in connection slack (both labels
    end up boarding the same next leg), turning into an exact metric tie that
    is then decided by the lexicographic trip_id sequence. So a label is only
    discarded when another label is no worse on every metric and is either
    strictly better on fare or transfers (its continuations are then strictly
    dominated) or carries a lexicographically no-larger trip_id path (its
    continuations then never lose a tie-break).
    """
    return _leq_metrics(a, b) and (
        a[1] < b[1] or a[2] < b[2] or a[3] <= b[3]
    )


def _covers_final(a, b):
    """Dominance at the destination: strict Pareto on the three metrics, with
    identical metric tuples keeping the lexicographically smallest path."""
    return _leq_metrics(a, b) and (
        a[0] < b[0] or a[1] < b[1] or a[2] < b[2] or a[3] <= b[3]
    )


def _insert(frontier, label, covers):
    """Insert ``label`` into a frontier, pruning labels it covers.

    Labels are ``(arrival, total_fare, transfers, trip_id_path)``. ``covers``
    is a transitive pruning order, so the surviving set is the unique set of
    minimal elements regardless of insertion order.
    """
    for existing in frontier:
        if covers(existing, label):
            return False
    frontier[:] = [e for e in frontier if not covers(label, e)]
    frontier.append(label)
    return True


def _frontier(candidates, request):
    """Pareto frontier at the destination plus the set of reachable ports."""
    ready = instant(request["ready_at"])
    origin = request["origin"]
    destination = request["destination"]
    transfer = request["transfer_seconds"]

    frontiers = defaultdict(list)
    ordered = sorted(
        candidates,
        key=lambda c: (c["departure"], c["arrival"], c["trip_id"]),
    )
    for leg in ordered:
        bases = []
        if leg["origin"] == origin and leg["departure"] >= ready:
            bases.append((None, 0, 0, ()))
        minimum = timedelta(seconds=transfer[leg["origin"]])
        for label in frontiers[leg["origin"]]:
            if label[0] + minimum <= leg["departure"]:
                bases.append(label)
        for base in bases:
            path = base[3] + (leg["trip_id"],)
            label = (
                leg["arrival"],
                base[1] + leg["fare_cents"],
                base[2] + (0 if base[3] == () else 1),
                path,
            )
            _insert(frontiers[leg["destination"]], label, _covers_intermediate)

    reachable = sorted(
        {origin} | {port for port, labels in frontiers.items() if labels}
    )
    # Port frontiers use the intermediate order so continuations stay
    # correct; the answer itself applies destination dominance.
    result = []
    for label in frontiers[destination]:
        _insert(result, label, _covers_final)
    return result, reachable


def connections(database, as_of, known_at=None, request=None):
    """Build the connection report from one read-only snapshot.

    ``request`` is the raw request mapping (see :func:`load_request`); it is
    validated and normalized here so API and CLI callers share one contract.
    """
    if request is None:
        raise ValueError("request is required")
    normalized = validate_request(request)
    cutoff = instant(as_of)
    ready = instant(normalized["ready_at"])
    if ready > cutoff:
        raise ValueError(
            f"ready_at {normalized['ready_at']} must be at or before the "
            f"as-of cutoff {cutoff.isoformat()}"
        )

    db = _read_only_connect(database)
    try:
        # One read transaction holds a single snapshot across the trip plan,
        # the revision ledger and the source evidence.
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

        trip_routes = {}
        for trip_id, route in db.execute("SELECT trip_id, route FROM trips"):
            trip_routes[trip_id] = route

        revisions = _revisions_at(db, known_at)
        winners = _projection(revisions)
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

    # Reference completeness: every route in this database must be mapped by
    # the request, otherwise candidate fares and ports would be unknown.
    unmapped = sorted(set(trip_routes.values()) - set(normalized["routes"]))
    if unmapped:
        raise ValueError(f"request does not map database routes: {unmapped}")

    candidates = []
    projected = defaultdict(list)
    for rec in winners.values():
        if rec["action"] != "upsert":
            continue
        if instant(rec["occurred_at"]) > cutoff:
            continue
        projected[rec["trip_id"]].append(rec)
    for trip_id in sorted(projected):
        records = projected[trip_id]
        state, departure, arrival = lifecycle(records)
        if state != "arrived":
            continue
        route = trip_routes[trip_id]
        spec = normalized["routes"][route]
        candidates.append({
            "trip_id": trip_id,
            "route": route,
            "origin": spec["origin"],
            "destination": spec["destination"],
            "fare_cents": spec["fare_cents"],
            "departure": departure,
            "arrival": arrival,
            "records": sorted(
                records, key=lambda r: (r["occurred_at"], r["event_id"])
            ),
        })

    frontier, reachable = _frontier(candidates, normalized)

    by_id = {c["trip_id"]: c for c in candidates}
    options = []
    for arrival, fare, transfers, path in frontier:
        legs = []
        for trip_id in path:
            leg = by_id[trip_id]
            legs.append({
                "trip_id": trip_id,
                "route": leg["route"],
                "origin": leg["origin"],
                "destination": leg["destination"],
                "departure": leg["departure"].isoformat(),
                "arrival": leg["arrival"].isoformat(),
                "fare_cents": leg["fare_cents"],
            })
        options.append({
            "trip_ids": list(path),
            "legs": legs,
            "arrival": arrival.isoformat(),
            "total_fare_cents": fare,
            "transfers": transfers,
        })
    options.sort(
        key=lambda o: (o["arrival"], o["total_fare_cents"], o["transfers"], o["trip_ids"])
    )

    served_origins = {c["origin"] for c in candidates}
    served_destinations = {c["destination"] for c in candidates}
    if options:
        status, reason = "ok", None
    elif not candidates:
        status, reason = "no_route", NO_CANDIDATES
    elif normalized["origin"] not in served_origins:
        status, reason = "no_route", ORIGIN_NOT_SERVED
    elif normalized["destination"] not in served_destinations:
        status, reason = "no_route", DESTINATION_NOT_SERVED
    else:
        status, reason = "no_route", NO_CONNECTION

    candidate_entries = []
    for leg in sorted(candidates, key=lambda c: (c["departure"], c["arrival"], c["trip_id"])):
        candidate_entries.append({
            "trip_id": leg["trip_id"],
            "route": leg["route"],
            "origin": leg["origin"],
            "destination": leg["destination"],
            "fare_cents": leg["fare_cents"],
            "departure": leg["departure"].isoformat(),
            "arrival": leg["arrival"].isoformat(),
            "event_revisions": [_evidence(rec) for rec in leg["records"]],
        })

    return {
        "schema_version": CONNECTIONS_SCHEMA_VERSION,
        "as_of": cutoff.isoformat(),
        "knowledge_seq": known_at,
        "request": normalized,
        "candidate_trips": candidate_entries,
        "options": options,
        "diagnostics": {
            "status": status,
            "reason": reason,
            "candidate_trip_count": len(candidates),
            "reachable_ports": reachable,
        },
    }


def write_connections(report, output):
    """Atomically replace ``connections.json`` inside ``output``.

    The payload is rendered deterministically (sorted keys, no volatile
    fields), written to a temporary file in the same directory and fsynced
    before ``os.replace``. A failure anywhere before the rename leaves the
    previous report byte-identical.
    """
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    payload = (json.dumps(report, indent=2, sort_keys=True) + "\n").encode("utf-8")
    fd, temporary = tempfile.mkstemp(
        dir=output, prefix=".connections-", suffix=".tmp"
    )
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, output / "connections.json")
    except BaseException:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise
