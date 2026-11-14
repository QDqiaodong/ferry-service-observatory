"""Independent in-memory model of the revision ledger and reports.

Everything here is derived from the written contract and re-implemented from
scratch: the module deliberately contains no ``ferry_observer`` import (the
gate has a structural test enforcing that). It is an *oracle*: the harness
feeds it exactly the same batch files the real system ingests and then
compares accept/reject decisions, counters, knowledge sequences, first-source
ownership, full table contents and dual-cutoff reports.
"""
import hashlib
import json
from collections import defaultdict
from pathlib import Path

from .canonical import (
    ContractError,
    parse_instant,
    parse_revision_file,
    parse_schedule,
)

SCHEMA_VERSION = 2


class OracleRejection(Exception):
    """The oracle predicts that the batch must be rejected."""

    def __init__(self, reason):
        super().__init__(reason)
        self.reason = reason


def lifecycle(events):
    """Contract lifecycle, independent of ferry_observer.reconcile."""
    state = "pending"
    departure = arrival = None
    for event in sorted(events, key=lambda e: (e["occurred_at"], e["event_id"])):
        kind = event["kind"]
        if kind == "cancelled" and state == "pending":
            state = "cancelled"
        elif kind == "departed" and state == "pending":
            state = "underway"
            departure = parse_instant(event["occurred_at"])
        elif kind == "arrived" and state == "underway":
            arrival = parse_instant(event["occurred_at"])
            if arrival <= departure:
                raise OracleRejection("arrival must be later than departure")
            state = "arrived"
        else:
            raise OracleRejection(f"invalid lifecycle: {state} -> {kind}")
    return state, departure, arrival


class RevisionOracle:
    """Pure-Python counterpart of the production SQLite ledger."""

    def __init__(self, schedule_path):
        self.trips = parse_schedule(schedule_path)
        # event_id -> {revision -> stored revision dict}
        self.ledger = defaultdict(dict)
        self.projection = {}
        self.sources = {}
        self.sequences = []

    # ------------------------------------------------------------- ingestion
    def ingest(self, batch_path):
        """Apply one batch file; return the predicted ingest summary.

        Raises :class:`OracleRejection` when the contract says the whole
        batch must roll back. The oracle state is only mutated on success.
        """
        path = Path(batch_path)
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        try:
            parsed = parse_revision_file(path)
            self._pass1_intra_batch(parsed)
            new_records, duplicates = self._pass2_ledger(parsed)
            new_by_event = defaultdict(list)
            for rec in new_records:
                new_by_event[rec["event_id"]].append(rec)
            self._validate_versions(new_by_event)
            projection = self._project_final(new_by_event)
            self._validate_lifecycle(projection)
        except ContractError as exc:
            raise OracleRejection(str(exc)) from exc

        next_seq = len(self.sequences) + 1 if new_records else None
        if new_records:
            self.sequences.append(next_seq)
            for rec in new_records:
                stored = dict(rec)
                stored["source_sha256"] = digest
                stored["knowledge_seq"] = next_seq
                self.ledger[rec["event_id"]][rec["revision"]] = stored

        touched = {rec["event_id"] for rec in new_records}
        if touched:
            for event_id in touched:
                self.projection.pop(event_id, None)
            for event_id, rec in projection.items():
                if event_id in touched:
                    self.projection[event_id] = rec

        # Source registration happens for every successful batch, including
        # empty and fully duplicate ones, and never consumes a sequence.
        self.sources.setdefault(digest, len(parsed))

        return {
            "input_rows": len(parsed),
            "inserted": len(new_records),
            "duplicates": duplicates,
            "total_events": len(self.projection),
            "source_sha256": digest,
            "knowledge_seq": len(self.sequences),
            "new_revisions": len(new_records),
            "active_events": len(self.projection),
        }

    def _pass1_intra_batch(self, parsed):
        candidates = {}
        for rec in parsed:
            key = (rec["event_id"], rec["revision"])
            present = candidates.get(key)
            if present is not None:
                if (
                    present["action"] != rec["action"]
                    or present["canonical"] != rec["canonical"]
                ):
                    raise OracleRejection(
                        f"conflicting records for {rec['event_id']} "
                        f"revision {rec['revision']} in the same batch"
                    )
                continue
            candidates[key] = rec
        return candidates

    def _pass2_ledger(self, parsed):
        duplicates = 0
        new_records = []
        seen_new = set()
        for rec in parsed:
            key = (rec["event_id"], rec["revision"])
            prior = self.ledger.get(rec["event_id"], {}).get(rec["revision"])
            if prior is not None:
                if (
                    prior["action"] != rec["action"]
                    or prior["canonical"] != rec["canonical"]
                ):
                    raise OracleRejection(
                        f"revision conflict for {rec['event_id']} "
                        f"revision {rec['revision']} against the accepted ledger"
                    )
                duplicates += 1
                continue
            if key in seen_new:
                duplicates += 1
                continue
            seen_new.add(key)
            new_records.append(rec)
        return new_records, duplicates

    def _validate_versions(self, new_by_event):
        for event_id in sorted(set(self.ledger) | set(new_by_event)):
            versions = dict(self.ledger.get(event_id, {}))
            for rec in new_by_event.get(event_id, ()):
                versions[rec["revision"]] = rec
            ordered = sorted(versions)
            if ordered[0] != 1 or ordered != list(range(1, len(ordered) + 1)):
                raise OracleRejection(
                    f"event {event_id}: revisions must start at 1 and be "
                    f"continuous; got {ordered}"
                )
            trip_id = None
            for revision in ordered:
                version = versions[revision]
                if version["action"] == "upsert":
                    if trip_id is None:
                        if revision != 1:
                            raise OracleRejection(
                                f"event {event_id}: revision 1 must be an upsert"
                            )
                        trip_id = version["trip_id"]
                    elif version["trip_id"] != trip_id:
                        raise OracleRejection(
                            f"event {event_id} revision {revision}: trip_id "
                            f"cannot change from {trip_id} to {version['trip_id']}"
                        )
                elif trip_id is None:
                    raise OracleRejection(
                        f"event {event_id} revision {revision}: cannot void an "
                        "event that has never been upserted"
                    )
            if trip_id not in self.trips:
                raise OracleRejection(
                    f"event {event_id} references unknown trip {trip_id}"
                )

    def _project_final(self, new_by_event):
        projection = {}
        for event_id in set(self.ledger) | set(new_by_event):
            versions = dict(self.ledger.get(event_id, {}))
            for rec in new_by_event.get(event_id, ()):
                versions[rec["revision"]] = rec
            winner = versions[max(versions)]
            if winner["action"] == "upsert":
                projection[event_id] = winner
        return projection

    def _validate_lifecycle(self, projection):
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
            except OracleRejection as exc:
                raise OracleRejection(f"trip {trip_id}: {exc.reason}") from exc

    # ------------------------------------------------------------- snapshots
    def snapshot(self):
        """Full logical table content, compared row-for-row with SQLite."""
        revisions = []
        for event_id in sorted(self.ledger):
            for revision in sorted(self.ledger[event_id]):
                rec = self.ledger[event_id][revision]
                revisions.append(
                    (
                        rec["event_id"],
                        rec["revision"],
                        rec["action"],
                        rec["trip_id"],
                        rec["kind"],
                        rec["occurred_at"],
                        rec["canonical"],
                        rec["source_sha256"],
                        rec["knowledge_seq"],
                    )
                )
        projection_rows = [
            (
                event_id,
                rec["trip_id"],
                rec["kind"],
                rec["occurred_at"],
                rec["revision"],
            )
            for event_id, rec in sorted(self.projection.items())
        ]
        return {
            "trips": sorted(
                (
                    t["trip_id"],
                    t["route"],
                    t["planned_departure"],
                    t["planned_arrival"],
                )
                for t in self.trips.values()
            ),
            "revisions": revisions,
            "event_projection": projection_rows,
            "knowledge_batches": [(seq, None) for seq in self.sequences],
            "sources": sorted(self.sources.items()),
            "meta": [("schema_version", str(SCHEMA_VERSION))],
        }

    # --------------------------------------------------------------- reports
    def performance(self, as_of, known_at=None):
        """Independent dual-cutoff report matching the documented semantics."""
        cutoff = parse_instant(as_of)
        latest_seq = len(self.sequences)
        if known_at is None:
            known_at = latest_seq
        if (
            isinstance(known_at, bool)
            or not isinstance(known_at, int)
            or known_at < 0
            or known_at > latest_seq
        ):
            raise OracleRejection(
                f"known_at must be an integer between 0 and {latest_seq}; "
                f"got {known_at!r}"
            )

        visible = []
        for event_id in sorted(self.ledger):
            for revision in sorted(self.ledger[event_id]):
                rec = self.ledger[event_id][revision]
                if rec["knowledge_seq"] <= known_at:
                    visible.append(rec)

        winners = {}
        for rec in visible:
            current = winners.get(rec["event_id"])
            if current is None or rec["revision"] > current["revision"]:
                winners[rec["event_id"]] = rec

        event_trip = {}
        for rec in visible:
            if rec["action"] == "upsert":
                event_trip.setdefault(rec["event_id"], rec["trip_id"])

        evidence_by_trip = defaultdict(list)
        for rec in visible:
            trip_id = event_trip.get(rec["event_id"])
            winner = winners[rec["event_id"]]
            if rec["revision"] != winner["revision"]:
                status = "superseded"
            elif winner["action"] == "void":
                status = "voided"
            elif parse_instant(winner["occurred_at"]) > cutoff:
                status = "after_cutoff"
            else:
                status = "used"
            entry = {
                "event_id": rec["event_id"],
                "revision": rec["revision"],
                "action": rec["action"],
                "knowledge_seq": rec["knowledge_seq"],
                "canonical_payload": json.loads(rec["canonical"]),
                "first_source_sha256": rec["source_sha256"],
                "status": status,
            }
            if rec["source_sha256"] is None:
                entry["source_note"] = "legacy_unattributed"
            evidence_by_trip[trip_id].append(entry)
            if trip_id is None:
                raise OracleRejection(
                    f"event {rec['event_id']} has no upsert revision"
                )

        projected_events = defaultdict(list)
        for rec in winners.values():
            if rec["action"] != "upsert":
                continue
            if parse_instant(rec["occurred_at"]) > cutoff:
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
        due_trips = [
            t
            for t in self.trips.values()
            if parse_instant(t["planned_departure"]) <= cutoff
        ]
        due_trips.sort(key=lambda t: (t["planned_departure"], t["trip_id"]))
        for trip in due_trips:
            planned = parse_instant(trip["planned_departure"])
            visible_events = projected_events.get(trip["trip_id"], [])
            state, departure, arrival = lifecycle(visible_events)
            delay = (
                (departure - planned).total_seconds()
                if departure is not None
                else None
            )
            details.append(
                {
                    "trip_id": trip["trip_id"],
                    "route": trip["route"],
                    "state": state,
                    "departure_delay_seconds": delay,
                    "on_time": None if delay is None else 0 <= delay <= 300,
                    "travel_seconds": (
                        (arrival - departure).total_seconds() if arrival else None
                    ),
                    "knowledge_seq": known_at,
                    "event_revisions": evidence_by_trip.get(trip["trip_id"], []),
                }
            )

        routes = []
        for route in sorted({d["route"] for d in details}):
            rows = [d for d in details if d["route"] == route]
            operated = [r for r in rows if r["departure_delay_seconds"] is not None]
            travelled = [
                r["travel_seconds"] for r in rows if r["travel_seconds"] is not None
            ]
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
                        sum(r["departure_delay_seconds"] for r in operated)
                        / len(operated)
                        if operated
                        else None
                    ),
                    "mean_travel_seconds": (
                        sum(travelled) / len(travelled) if travelled else None
                    ),
                }
            )

        return {
            "schema_version": 2,
            "as_of": cutoff.isoformat(),
            "knowledge_seq": known_at,
            "routes": routes,
            "trips": details,
        }
