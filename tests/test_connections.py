"""Connection analysis: frontier correctness, request contract, dual cutoff,
atomic replacement, exhaustive small-graph cross-checks and CLI end-to-end."""
import json
import os
import random
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from ferry_observer.connections import (
    connections, load_request, validate_request, write_connections,
)
from ferry_observer.contracts import instant
from ferry_observer.storage import ingest, upgrade_database

DATA = Path(__file__).resolve().parents[1] / "data" / "connections"
CUTOFF = "2026-01-01T12:00:00Z"
BASE = datetime(2026, 1, 1, 6, 0, tzinfo=timezone.utc)


def iso(dt):
    return dt.isoformat().replace("+00:00", "Z")


def write_jsonl(path, rows):
    path.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")


def upsert(event_id, revision, trip_id, kind, occurred_at):
    return {
        "event_id": event_id, "revision": revision, "action": "upsert",
        "trip_id": trip_id, "kind": kind, "occurred_at": occurred_at,
    }


def void(event_id, revision):
    return {"event_id": event_id, "revision": revision, "action": "void"}


def v1_event(event_id, trip_id, kind, occurred_at):
    return {"event_id": event_id, "trip_id": trip_id,
            "kind": kind, "occurred_at": occurred_at}


def option_keys(report):
    return sorted(
        (o["arrival"], o["total_fare_cents"], o["transfers"], tuple(o["trip_ids"]))
        for o in report["options"]
    )


class ConnectionsCase(unittest.TestCase):
    """API-level tests over the bundled demo network and hand-built datasets."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.schedule = self.root / "schedule.csv"
        self.events = self.root / "events.jsonl"
        self.db = self.root / "ferry.sqlite"
        self.request = json.loads((DATA / "request.json").read_text())

    def use_demo_data(self):
        self.schedule.write_bytes((DATA / "schedule.csv").read_bytes())
        self.events.write_bytes((DATA / "events.jsonl").read_bytes())

    def ingest(self, event_format="v1"):
        return ingest(self.schedule, self.events, self.db, event_format)

    def build_dataset(self, trips, events, routes, transfer,
                      origin="A", destination="C", ready="2026-01-01T06:00:00Z"):
        """Write a custom schedule/events/request dataset and ingest it."""
        lines = ["trip_id,route,planned_departure,planned_arrival"]
        for trip_id, route, dep, arr in trips:
            lines.append(f"{trip_id},{route},{dep},{arr}")
        self.schedule.write_text("\n".join(lines) + "\n", encoding="utf-8")
        write_jsonl(self.events, events)
        self.request = {
            "schema_version": 1,
            "routes": routes,
            "transfer_seconds": transfer,
            "origin": origin,
            "destination": destination,
            "ready_at": ready,
        }
        return self.ingest(event_format="v1")

    # ------------------------------------------------------------- frontier
    def test_demo_frontier_keeps_later_but_cheaper_option(self):
        self.use_demo_data()
        self.ingest()
        report = connections(self.db, CUTOFF, None, self.request)
        self.assertEqual(report["knowledge_seq"], 1)
        self.assertEqual(report["diagnostics"]["status"], "ok")
        self.assertEqual(
            option_keys(report),
            [
                ("2026-01-01T09:50:00+00:00", 900, 0, ("D1",)),
                ("2026-01-01T10:12:00+00:00", 800, 1, ("H1", "E1")),
            ],
        )
        # The later arrival is cheaper; neither dominates the other. The
        # dominated H1->E2 and H2->E2 journeys (same fare, later arrival)
        # and the infeasible H2->E1 transfer do not appear.
        self.assertEqual(report["diagnostics"]["candidate_trip_count"], 5)
        self.assertEqual(
            report["diagnostics"]["reachable_ports"], ["East", "Harbor", "North"]
        )
        # Candidates carry the used revisions and their first sources.
        h1 = next(c for c in report["candidate_trips"] if c["trip_id"] == "H1")
        self.assertEqual(
            [(e["event_id"], e["revision"], e["kind"]) for e in h1["event_revisions"]],
            [("h1d", 1, "departed"), ("h1a", 1, "arrived")],
        )
        self.assertTrue(all(e["first_source_sha256"] for e in h1["event_revisions"]))
        self.assertEqual(report["candidate_trips"][0]["departure"], "2026-01-01T08:05:00+00:00")

    def test_transfer_boundary_exact_and_one_second_short(self):
        trips = [
            ("X", "AB", "2026-01-01T09:00:00Z", "2026-01-01T10:00:00Z"),
            ("Y", "BC", "2026-01-01T10:05:00Z", "2026-01-01T11:00:00Z"),
        ]
        events = [
            v1_event("xd", "X", "departed", "2026-01-01T09:00:00Z"),
            v1_event("xa", "X", "arrived", "2026-01-01T10:00:00Z"),
            v1_event("yd", "Y", "departed", "2026-01-01T10:05:00Z"),
            v1_event("ya", "Y", "arrived", "2026-01-01T11:00:00Z"),
        ]
        routes = {
            "AB": {"origin": "A", "destination": "B", "fare_cents": 100},
            "BC": {"origin": "B", "destination": "C", "fare_cents": 100},
        }
        # departure == arrival + transfer_seconds exactly: feasible.
        self.build_dataset(trips, events, routes, {"A": 0, "B": 300, "C": 0})
        report = connections(self.db, CUTOFF, None, self.request)
        self.assertEqual(
            option_keys(report),
            [("2026-01-01T11:00:00+00:00", 200, 1, ("X", "Y"))],
        )
        # One second more of transfer time breaks the connection.
        self.request["transfer_seconds"]["B"] = 301
        report = connections(self.db, CUTOFF, None, self.request)
        self.assertEqual(report["options"], [])
        self.assertEqual(report["diagnostics"]["reason"], "no_connection")

    def test_first_leg_ready_boundary(self):
        trips = [("X", "AB", "2026-01-01T09:00:00Z", "2026-01-01T10:00:00Z")]
        events = [
            v1_event("xd", "X", "departed", "2026-01-01T09:00:00Z"),
            v1_event("xa", "X", "arrived", "2026-01-01T10:00:00Z"),
        ]
        routes = {"AB": {"origin": "A", "destination": "B", "fare_cents": 50}}
        # ready exactly at departure: feasible.
        self.build_dataset(trips, events, routes, {"A": 0, "B": 0},
                           origin="A", destination="B",
                           ready="2026-01-01T09:00:00Z")
        report = connections(self.db, CUTOFF, None, self.request)
        self.assertEqual(len(report["options"]), 1)
        # One second later: the only leg is missed.
        self.request["ready_at"] = "2026-01-01T09:00:01Z"
        report = connections(self.db, CUTOFF, None, self.request)
        self.assertEqual(report["options"], [])
        self.assertEqual(report["diagnostics"]["reason"], "no_connection")

    def test_tie_break_survives_connection_slack(self):
        # T1 arrives at the transfer port earlier than T0, but both board T3
        # (the slack absorbs the difference), so the final metrics tie and the
        # lexicographically smaller sequence ("T0", "T3") must win even though
        # T0's intermediate label looked strictly worse on arrival time.
        trips = [
            ("T0", "R1", "2026-01-01T15:27:00Z", "2026-01-01T15:44:00Z"),
            ("T1", "R1", "2026-01-01T11:39:00Z", "2026-01-01T12:32:00Z"),
            ("T3", "R3", "2026-01-01T17:01:00Z", "2026-01-01T17:56:00Z"),
        ]
        events = [
            v1_event("t0d", "T0", "departed", "2026-01-01T15:27:00Z"),
            v1_event("t0a", "T0", "arrived", "2026-01-01T15:44:00Z"),
            v1_event("t1d", "T1", "departed", "2026-01-01T11:39:00Z"),
            v1_event("t1a", "T1", "arrived", "2026-01-01T12:32:00Z"),
            v1_event("t3d", "T3", "departed", "2026-01-01T17:01:00Z"),
            v1_event("t3a", "T3", "arrived", "2026-01-01T17:56:00Z"),
        ]
        routes = {
            "R1": {"origin": "A", "destination": "B", "fare_cents": 513},
            "R3": {"origin": "B", "destination": "C", "fare_cents": 325},
        }
        self.build_dataset(trips, events, routes, {"A": 0, "B": 866, "C": 0})
        report = connections(self.db, "2026-01-01T23:00:00Z", None, self.request)
        self.assertEqual(
            option_keys(report),
            [("2026-01-01T17:56:00+00:00", 838, 1, ("T0", "T3"))],
        )

    def test_tie_break_keeps_lexicographically_smallest_trip_sequence(self):
        trips = [
            ("U1", "Q", "2026-01-01T09:30:00Z", "2026-01-01T10:00:00Z"),
            ("U2", "Q", "2026-01-01T09:30:00Z", "2026-01-01T10:00:00Z"),
            ("T1", "R", "2026-01-01T10:00:00Z", "2026-01-01T10:30:00Z"),
            ("T2", "R", "2026-01-01T10:00:00Z", "2026-01-01T10:30:00Z"),
            ("T3", "S", "2026-01-01T11:00:00Z", "2026-01-01T11:30:00Z"),
        ]
        events = []
        for trip_id, dep, arr in [
            ("U1", "09:30", "10:00"), ("U2", "09:30", "10:00"),
            ("T1", "10:00", "10:30"), ("T2", "10:00", "10:30"),
            ("T3", "11:00", "11:30"),
        ]:
            events.append(v1_event(trip_id + "d", trip_id, "departed",
                                   f"2026-01-01T{dep}:00Z"))
            events.append(v1_event(trip_id + "a", trip_id, "arrived",
                                   f"2026-01-01T{arr}:00Z"))
        routes = {
            "Q": {"origin": "A", "destination": "C", "fare_cents": 500},
            "R": {"origin": "A", "destination": "B", "fare_cents": 100},
            "S": {"origin": "B", "destination": "C", "fare_cents": 100},
        }
        self.build_dataset(trips, events, routes, {"A": 0, "B": 0, "C": 0})
        report = connections(self.db, CUTOFF, None, self.request)
        # [U1] beats the identical [U2]; [T1,T3] beats the identical [T2,T3].
        self.assertEqual(
            option_keys(report),
            [
                ("2026-01-01T10:00:00+00:00", 500, 0, ("U1",)),
                ("2026-01-01T11:30:00+00:00", 200, 1, ("T1", "T3")),
            ],
        )

    def test_only_arrived_trips_are_candidates_never_planned_times(self):
        trips = [
            ("OK", "AB", "2026-01-01T09:00:00Z", "2026-01-01T10:00:00Z"),
            ("LATE", "AB", "2026-01-01T11:00:00Z", "2026-01-01T12:00:00Z"),
            ("GHOST", "AB", "2026-01-01T08:00:00Z", "2026-01-01T08:30:00Z"),
            ("STOP", "AB", "2026-01-01T12:00:00Z", "2026-01-01T12:30:00Z"),
        ]
        events = [
            v1_event("ok1", "OK", "departed", "2026-01-01T09:05:00Z"),
            v1_event("ok2", "OK", "arrived", "2026-01-01T10:05:00Z"),
            v1_event("late1", "LATE", "departed", "2026-01-01T11:05:00Z"),
            v1_event("stop1", "STOP", "cancelled", "2026-01-01T11:30:00Z"),
        ]
        routes = {"AB": {"origin": "A", "destination": "B", "fare_cents": 50}}
        self.build_dataset(trips, events, routes, {"A": 0, "B": 0},
                           origin="A", destination="B")
        report = connections(self.db, CUTOFF, None, self.request)
        # Underway LATE, event-less GHOST and cancelled STOP are excluded;
        # planned times never fill in unknown actuals.
        self.assertEqual(
            [c["trip_id"] for c in report["candidate_trips"]], ["OK"]
        )
        self.assertEqual(
            option_keys(report),
            [("2026-01-01T10:05:00+00:00", 50, 0, ("OK",))],
        )

    # -------------------------------------------------------- dual cutoff
    def test_correction_past_t_and_knowledge_replay(self):
        self.use_demo_data()
        self.ingest()
        self.events.write_bytes((DATA / "events-v2-e1-shift.jsonl").read_bytes())
        self.ingest("v2")
        self.events.write_bytes((DATA / "events-v2-void-d1.jsonl").read_bytes())
        self.ingest("v2")

        k1 = connections(self.db, CUTOFF, 1, self.request)
        k2 = connections(self.db, CUTOFF, 2, self.request)
        k3 = connections(self.db, CUTOFF, 3, self.request)
        # K=1: original E1 departure 09:40, D1 present.
        self.assertEqual(len(k1["options"]), 2)
        e1_k1 = next(c for c in k1["candidate_trips"] if c["trip_id"] == "E1")
        self.assertEqual(e1_k1["departure"], "2026-01-01T09:40:00+00:00")
        # K=2: corrected departure 08:45; H1 arrives 08:35 and 600s transfer
        # makes the connection exactly feasible (boundary).
        e1_k2 = next(c for c in k2["candidate_trips"] if c["trip_id"] == "E1")
        self.assertEqual(e1_k2["departure"], "2026-01-01T08:45:00+00:00")
        self.assertEqual(option_keys(k1), option_keys(k2))
        # K=3: D1's arrival voided, D1 is no longer a candidate.
        self.assertEqual(
            option_keys(k3),
            [("2026-01-01T10:12:00+00:00", 800, 1, ("H1", "E1"))],
        )
        self.assertNotIn(
            "D1", [c["trip_id"] for c in k3["candidate_trips"]]
        )
        # Evidence shows the correction and its source at K=2: the departure
        # event carries revision 2, the untouched arrival stays revision 1.
        by_kind = {e["kind"]: e for e in e1_k2["event_revisions"]}
        self.assertEqual(by_kind["departed"]["revision"], 2)
        self.assertEqual(by_kind["departed"]["knowledge_seq"], 2)
        self.assertEqual(by_kind["arrived"]["revision"], 1)
        self.assertEqual(by_kind["arrived"]["knowledge_seq"], 1)

    def test_correction_breaks_connection_and_void_restores_nothing(self):
        trips = [
            ("X", "AB", "2026-01-01T09:00:00Z", "2026-01-01T10:00:00Z"),
            ("Y", "BC", "2026-01-01T10:10:00Z", "2026-01-01T11:00:00Z"),
        ]
        events = [
            v1_event("xd", "X", "departed", "2026-01-01T09:00:00Z"),
            v1_event("xa", "X", "arrived", "2026-01-01T10:00:00Z"),
            v1_event("yd", "Y", "departed", "2026-01-01T10:10:00Z"),
            v1_event("ya", "Y", "arrived", "2026-01-01T11:00:00Z"),
        ]
        routes = {
            "AB": {"origin": "A", "destination": "B", "fare_cents": 100},
            "BC": {"origin": "B", "destination": "C", "fare_cents": 100},
        }
        self.build_dataset(trips, events, routes, {"A": 0, "B": 0, "C": 0})
        # Correction moves X's arrival past Y's departure.
        write_jsonl(self.events, [upsert("xa", 2, "X", "arrived", "2026-01-01T10:15:00Z")])
        self.ingest("v2")
        # Void Y's arrival afterwards.
        write_jsonl(self.events, [void("ya", 2)])
        self.ingest("v2")

        k1 = connections(self.db, CUTOFF, 1, self.request)
        self.assertEqual(
            option_keys(k1),
            [("2026-01-01T11:00:00+00:00", 200, 1, ("X", "Y"))],
        )
        k2 = connections(self.db, CUTOFF, 2, self.request)
        self.assertEqual(k2["options"], [])
        self.assertEqual(k2["diagnostics"]["reason"], "no_connection")
        k3 = connections(self.db, CUTOFF, 3, self.request)
        self.assertEqual(k3["options"], [])
        self.assertNotIn("Y", [c["trip_id"] for c in k3["candidate_trips"]])

    # ------------------------------------------------------------ diagnostics
    def test_unreachable_and_empty_diagnoses(self):
        self.use_demo_data()
        self.ingest()
        # K=0: nothing known, no candidates at all.
        report = connections(self.db, CUTOFF, 0, self.request)
        self.assertEqual(report["diagnostics"]["status"], "no_route")
        self.assertEqual(report["diagnostics"]["reason"], "no_candidate_trips")
        self.assertEqual(report["options"], [])
        # Origin never departed.
        self.request["origin"], self.request["destination"] = "North", "Harbor"
        report = connections(self.db, CUTOFF, None, self.request)
        self.assertEqual(report["diagnostics"]["reason"], "origin_not_served")
        self.assertEqual(report["diagnostics"]["reachable_ports"], ["North"])
        # Destination never arrived.
        self.request["origin"], self.request["destination"] = "Harbor", "South"
        self.request["transfer_seconds"]["South"] = 0
        report = connections(self.db, CUTOFF, None, self.request)
        self.assertEqual(report["diagnostics"]["reason"], "destination_not_served")
        # Both served but no feasible chain (transfer too long).
        self.request["destination"] = "North"
        del self.request["transfer_seconds"]["South"]
        self.request["transfer_seconds"]["East"] = 86400
        self.request["routes"]["Harbor-North"]["fare_cents"] = 0
        report = connections(self.db, CUTOFF, None, self.request)
        paths = {tuple(o["trip_ids"]) for o in report["options"]}
        self.assertEqual(paths, {("D1",)})  # direct route still works

    # ------------------------------------------------------- request contract
    def test_request_validation_rejects_bad_input(self):
        self.use_demo_data()
        self.ingest()
        good = json.loads((DATA / "request.json").read_text())

        def bad(**patch):
            request = json.loads(json.dumps(good))
            for key, value in patch.items():
                if value is ...:
                    del request[key]
                else:
                    request[key] = value
            with self.assertRaises(ValueError, msg=patch):
                connections(self.db, CUTOFF, None, request)

        bad(schema_version=2)
        bad(schema_version="1")
        bad(schema_version=True)
        bad(schema_version=1.0)
        bad(routes={})
        bad(transfer_seconds={})
        bad(origin="Harbor", destination="Harbor")
        bad(ready_at="2026-01-01T08:00:00")       # naive timestamp
        bad(ready_at="2026-01-01T13:00:00Z")      # after the cutoff
        bad(unknown_key=1)                         # extra key
        bad(origin=...)                            # missing key
        bad(routes={"Harbor-East": {"origin": "Harbor", "destination": "East",
                                    "fare_cents": -1}})
        bad(routes={"Harbor-East": {"origin": "Harbor", "destination": "East",
                                    "fare_cents": True}})
        bad(routes={"Harbor-East": {"origin": "Harbor", "destination": "East",
                                    "fare_cents": 1.5}})
        bad(routes={"Harbor-East": {"origin": "Harbor", "destination": "Harbor",
                                    "fare_cents": 1}})
        bad(transfer_seconds={"Harbor": 0, "East": -5, "North": 0})
        bad(transfer_seconds={"Harbor": 0, "East": True, "North": 0})
        # Route endpoint port without a transfer_seconds entry.
        bad(transfer_seconds={"Harbor": 0, "East": 600})
        # Origin/destination without a transfer_seconds entry.
        bad(origin="Nowhere")
        # Database route missing from the request mapping.
        bad(routes={"Harbor-East": {"origin": "Harbor", "destination": "East",
                                    "fare_cents": 500}})

    def test_request_allows_extra_routes_and_zero_fares(self):
        self.use_demo_data()
        self.ingest()
        request = json.loads(json.dumps(self.request))
        request["routes"]["East-Harbor"] = {
            "origin": "East", "destination": "Harbor", "fare_cents": 0,
        }
        report = connections(self.db, CUTOFF, None, request)
        self.assertEqual(report["diagnostics"]["status"], "ok")

    def test_validate_request_normalizes_ready_at_to_utc(self):
        request = json.loads((DATA / "request.json").read_text())
        request["ready_at"] = "2026-01-01T09:00:00+01:00"
        normalized = validate_request(request)
        self.assertEqual(normalized["ready_at"], "2026-01-01T08:00:00+00:00")
        self.assertEqual(normalized["schema_version"], 1)

    def test_load_request_strict_json(self):
        path = self.root / "request.json"
        path.write_text('{"a": 1, "a": 2}', encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "duplicate key"):
            load_request(path)
        path.write_text('{"a": NaN}', encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "constant"):
            load_request(path)
        path.write_text("not json", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "strict JSON"):
            load_request(path)
        with self.assertRaisesRegex(ValueError, "unreadable"):
            load_request(self.root / "missing.json")
        path.write_text((DATA / "request.json").read_text(), encoding="utf-8")
        self.assertEqual(load_request(path)["origin"], "Harbor")

    # --------------------------------------------- determinism and atomicity
    def test_deterministic_bytes_and_read_only_snapshot(self):
        self.use_demo_data()
        self.ingest()
        before_db = self.db.read_bytes()
        first = connections(self.db, CUTOFF, None, self.request)
        second = connections(self.db, CUTOFF, None, self.request)
        self.assertEqual(first, second)
        self.assertEqual(before_db, self.db.read_bytes())
        out1, out2 = self.root / "out1", self.root / "out2"
        write_connections(first, out1)
        write_connections(second, out2)
        payload = (out1 / "connections.json").read_bytes()
        self.assertEqual(payload, (out2 / "connections.json").read_bytes())
        self.assertNotIn(b"/workspace", payload)
        self.assertNotIn(str(self.root).encode(), payload)

    def test_byte_stable_historical_report_after_later_commits(self):
        self.use_demo_data()
        self.ingest()
        out1 = self.root / "k1-first"
        write_connections(connections(self.db, CUTOFF, 1, self.request), out1)
        self.events.write_bytes((DATA / "events-v2-e1-shift.jsonl").read_bytes())
        self.ingest("v2")
        out2 = self.root / "k1-second"
        write_connections(connections(self.db, CUTOFF, 1, self.request), out2)
        self.assertEqual(
            (out1 / "connections.json").read_bytes(),
            (out2 / "connections.json").read_bytes(),
        )

    def test_bad_request_preserves_previous_report(self):
        self.use_demo_data()
        self.ingest()
        out = self.root / "out"
        write_connections(connections(self.db, CUTOFF, None, self.request), out)
        good = (out / "connections.json").read_bytes()
        broken = json.loads(json.dumps(self.request))
        broken["transfer_seconds"] = {"Harbor": 0}  # incomplete port mapping
        with self.assertRaises(ValueError):
            connections(self.db, CUTOFF, None, broken)
        self.assertEqual((out / "connections.json").read_bytes(), good)

    def test_write_failure_preserves_previous_report(self):
        self.use_demo_data()
        self.ingest()
        out = self.root / "out"
        report = connections(self.db, CUTOFF, None, self.request)
        write_connections(report, out)
        good = (out / "connections.json").read_bytes()
        if hasattr(os, "geteuid") and os.geteuid() == 0:
            self.skipTest("permission bits do not stop root")
        os.chmod(out, 0o500)
        try:
            with self.assertRaises(OSError):
                write_connections(report, out)
        finally:
            os.chmod(out, 0o700)
        self.assertEqual((out / "connections.json").read_bytes(), good)
        self.assertEqual(list(out.glob(".connections-*.tmp")), [])

    def test_known_at_guardrails_and_legacy_database(self):
        self.use_demo_data()
        self.ingest()
        with self.assertRaisesRegex(ValueError, "known_at"):
            connections(self.db, CUTOFF, 99, self.request)
        with self.assertRaisesRegex(ValueError, "known_at"):
            connections(self.db, CUTOFF, -1, self.request)
        with self.assertRaisesRegex(ValueError, "does not exist"):
            connections(self.root / "missing.sqlite", CUTOFF, None, self.request)
        self.assertFalse((self.root / "missing.sqlite").exists())
        # A legacy pre-revision database is rejected with the upgrade hint,
        # never implicitly migrated.
        legacy = self.root / "legacy.sqlite"
        with sqlite3.connect(legacy) as db:
            db.executescript(
                """
                CREATE TABLE trips (trip_id TEXT PRIMARY KEY, route TEXT NOT NULL,
                 planned_departure TEXT NOT NULL, planned_arrival TEXT NOT NULL);
                CREATE TABLE events (event_id TEXT PRIMARY KEY, trip_id TEXT NOT NULL,
                 kind TEXT NOT NULL, occurred_at TEXT NOT NULL, canonical_json TEXT NOT NULL);
                CREATE TABLE sources (sha256 TEXT PRIMARY KEY, row_count INTEGER NOT NULL);
                """
            )
        with self.assertRaisesRegex(ValueError, "upgrade-db"):
            connections(legacy, CUTOFF, None, self.request)


    def test_connections_on_upgraded_legacy_database(self):
        # Legacy databases upgraded via --upgrade-db carry revision-1 facts
        # with no source mapping; the analysis works and says so honestly.
        legacy = self.root / "legacy.sqlite"
        with sqlite3.connect(legacy) as db:
            db.executescript(
                """
                CREATE TABLE trips (trip_id TEXT PRIMARY KEY, route TEXT NOT NULL,
                 planned_departure TEXT NOT NULL, planned_arrival TEXT NOT NULL);
                CREATE TABLE events (event_id TEXT PRIMARY KEY, trip_id TEXT NOT NULL,
                 kind TEXT NOT NULL, occurred_at TEXT NOT NULL, canonical_json TEXT NOT NULL);
                CREATE TABLE sources (sha256 TEXT PRIMARY KEY, row_count INTEGER NOT NULL);
                """
            )
            for line in (DATA / "schedule.csv").read_text().splitlines()[1:]:
                db.execute("INSERT INTO trips VALUES (?,?,?,?)", line.split(","))
            for line in (DATA / "events.jsonl").read_text().splitlines():
                row = json.loads(line)
                db.execute(
                    "INSERT INTO events VALUES (?,?,?,?,?)",
                    (row["event_id"], row["trip_id"], row["kind"],
                     row["occurred_at"], json.dumps(row, sort_keys=True)),
                )
        result = upgrade_database(legacy)
        self.assertEqual(result["knowledge_seq"], 1)
        report = connections(legacy, CUTOFF, None, self.request)
        self.assertEqual(len(report["options"]), 2)
        evidence = report["candidate_trips"][0]["event_revisions"][0]
        self.assertIsNone(evidence["first_source_sha256"])
        self.assertIn("legacy_unattributed", evidence["source_note"])


class ExhaustiveFrontierCase(unittest.TestCase):
    """Cross-check the label-scan frontier against brute-force enumeration
    of every valid journey on deterministic random small networks."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def brute_force(self, candidates, request):
        """Enumerate all valid journeys, then take the Pareto frontier with
        the lexicographic tie-break -- deliberately naive and independent."""
        ready = instant(request["ready_at"])
        transfer = request["transfer_seconds"]
        journeys = []

        def extend(path, port, earliest):
            for leg in candidates:
                if leg["origin"] != port or leg["departure"] < earliest:
                    continue
                new_path = path + (leg,)
                if leg["destination"] == request["destination"]:
                    journeys.append(new_path)
                extend(new_path, leg["destination"],
                       leg["arrival"] + timedelta(seconds=transfer[leg["destination"]]))

        for leg in candidates:
            if leg["origin"] == request["origin"] and leg["departure"] >= ready:
                path = (leg,)
                if leg["destination"] == request["destination"]:
                    journeys.append(path)
                extend(path, leg["destination"],
                       leg["arrival"] + timedelta(seconds=transfer[leg["destination"]]))

        entries = []
        for path in journeys:
            entries.append((
                path[-1]["arrival"],
                sum(leg["fare_cents"] for leg in path),
                len(path) - 1,
                tuple(leg["trip_id"] for leg in path),
            ))
        frontier = {}
        for entry in entries:
            dominated = any(
                other is not entry
                and other[0] <= entry[0] and other[1] <= entry[1] and other[2] <= entry[2]
                and (other[0] < entry[0] or other[1] < entry[1] or other[2] < entry[2])
                for other in entries
            )
            if dominated:
                continue
            key = entry[:3]
            if key not in frontier or entry[3] < frontier[key]:
                frontier[key] = entry[3]
        return sorted(
            (iso(arr), fare, transfers, ids)
            for (arr, fare, transfers), ids in frontier.items()
        )

    def random_network(self, rng):
        ports = ["P1", "P2", "P3", "P4"][: rng.randint(3, 4)]
        pairs = [(a, b) for a in ports for b in ports if a != b]
        rng.shuffle(pairs)
        routes = {}
        for index, (origin, destination) in enumerate(pairs[: rng.randint(3, 5)]):
            routes[f"R{index}"] = {
                "origin": origin, "destination": destination,
                "fare_cents": rng.randint(0, 1000),
            }
        transfer = {port: rng.randint(0, 900) for port in ports}
        trips, events, arrived = [], [], {}
        for index in range(rng.randint(4, 9)):
            route = rng.choice(sorted(routes))
            planned_dep = BASE + timedelta(minutes=rng.randint(0, 720))
            planned_arr = planned_dep + timedelta(minutes=rng.randint(15, 60))
            trip_id = f"T{index}"
            trips.append((trip_id, route, iso(planned_dep), iso(planned_arr)))
            fate = rng.random()
            if fate < 0.65:  # arrived with jittered actual times
                dep = planned_dep + timedelta(seconds=rng.randint(0, 600))
                arr = planned_arr + timedelta(seconds=rng.randint(0, 600))
                events.append(v1_event(f"{trip_id}d", trip_id, "departed", iso(dep)))
                events.append(v1_event(f"{trip_id}a", trip_id, "arrived", iso(arr)))
                arrived[trip_id] = {
                    "trip_id": trip_id, "route": route,
                    "origin": routes[route]["origin"],
                    "destination": routes[route]["destination"],
                    "fare_cents": routes[route]["fare_cents"],
                    "departure": dep, "arrival": arr,
                }
            elif fate < 0.8:  # underway only
                dep = planned_dep + timedelta(seconds=rng.randint(0, 600))
                events.append(v1_event(f"{trip_id}d", trip_id, "departed", iso(dep)))
            elif fate < 0.9:  # cancelled
                events.append(v1_event(f"{trip_id}c", trip_id, "cancelled", iso(planned_dep)))
            # else: no events at all
        origin, destination = rng.sample(ports, 2)
        request = {
            "schema_version": 1,
            "routes": routes,
            "transfer_seconds": transfer,
            "origin": origin,
            "destination": destination,
            "ready_at": iso(BASE + timedelta(minutes=rng.randint(0, 300))),
        }
        return trips, events, arrived, request

    def mutate(self, rng, arrived, events_v2):
        """One correction or void batch; returns the K=2 candidate mapping."""
        arrived = {k: dict(v) for k, v in arrived.items()}
        if not arrived:
            return arrived
        trip_id = rng.choice(sorted(arrived))
        if rng.random() < 0.5:
            events_v2.append(void(f"{trip_id}d", 2))
            events_v2.append(void(f"{trip_id}a", 2))
            del arrived[trip_id]
        else:
            leg = arrived[trip_id]
            if rng.random() < 0.5:
                leg["arrival"] = leg["arrival"] + timedelta(seconds=rng.randint(1, 900))
                events_v2.append(upsert(f"{trip_id}a", 2, trip_id, "arrived", iso(leg["arrival"])))
            else:
                leg["departure"] = leg["departure"] - timedelta(seconds=rng.randint(0, 300))
                events_v2.append(upsert(f"{trip_id}d", 2, trip_id, "departed", iso(leg["departure"])))
        return arrived

    def check_frontier(self, arrived, request, report, cutoff):
        # The brute force sees the same event-time cutoff: trips whose actual
        # arrival is after T are underway, not candidates.
        visible = [
            leg for leg in arrived.values() if leg["arrival"] <= instant(cutoff)
        ]
        expected = self.brute_force(visible, request)
        actual = sorted(
            (o["arrival"].replace("+00:00", "Z"), o["total_fare_cents"],
             o["transfers"], tuple(o["trip_ids"]))
            for o in report["options"]
        )
        self.assertEqual(actual, expected)
        self.assertEqual(
            report["diagnostics"]["candidate_trip_count"], len(visible)
        )

    def test_frontier_matches_brute_force_on_random_networks(self):
        for seed in range(60):
            rng = random.Random(seed)
            with self.subTest(seed=seed):
                cutoff = iso(BASE + timedelta(
                    hours=rng.randint(10, 20), minutes=rng.randint(0, 59)
                ))
                root = self.root / f"seed{seed}"
                root.mkdir()
                trips, events, arrived, request = self.random_network(rng)
                schedule = root / "schedule.csv"
                schedule.write_text(
                    "trip_id,route,planned_departure,planned_arrival\n"
                    + "".join(f"{t},{r},{d},{a}\n" for t, r, d, a in trips),
                    encoding="utf-8",
                )
                events_path = root / "events.jsonl"
                write_jsonl(events_path, events)
                db = root / "ferry.sqlite"
                ingest(schedule, events_path, db, "v1")
                report = connections(db, cutoff, None, request)
                self.check_frontier(arrived, request, report, cutoff)

                # A correction/void batch: re-check both knowledge levels.
                events_v2 = []
                arrived_v2 = self.mutate(rng, arrived, events_v2)
                if events_v2:
                    write_jsonl(events_path, events_v2)
                    ingest(schedule, events_path, db, "v2")
                    self.check_frontier(
                        arrived, request, connections(db, cutoff, 1, request), cutoff
                    )
                    self.check_frontier(
                        arrived_v2, request, connections(db, cutoff, 2, request), cutoff
                    )


class ConnectionsCliCase(unittest.TestCase):
    """Real CLI subprocess runs: ingest, corrections, analysis, failures."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        for name in ("schedule.csv", "events.jsonl", "request.json"):
            (self.root / name).write_bytes((DATA / name).read_bytes())
        self.env = dict(os.environ)
        self.env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])

    def run_cli(self, *args):
        return subprocess.run(
            [sys.executable, "-m", "ferry_observer", *args],
            cwd=self.root, capture_output=True, text=True, env=self.env,
        )

    def ingest_cli(self, events="events.jsonl", fmt="v1"):
        proc = self.run_cli(
            "--schedule", "schedule.csv", "--events", events,
            "--event-format", fmt, "--db", "ferry.sqlite",
            "--output", "report", "--as-of", CUTOFF,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        return proc

    def connections_cli(self, *extra, expect_ok=True):
        proc = self.run_cli(
            "--connections", "request.json", "--db", "ferry.sqlite",
            "--output", "conn", "--as-of", CUTOFF, *extra,
        )
        if expect_ok:
            self.assertEqual(proc.returncode, 0, proc.stderr)
        return proc

    def test_cli_ingest_then_connections_and_knowledge_replay(self):
        self.ingest_cli()
        proc = self.connections_cli()
        summary = json.loads(proc.stdout.strip().splitlines()[-1])
        self.assertEqual(
            summary,
            {"connections": {"candidate_trips": 5, "knowledge_seq": 1,
                             "options": 2, "status": "ok"}},
        )
        report = json.loads((self.root / "conn" / "connections.json").read_text())
        self.assertEqual(report["schema_version"], 1)
        self.assertEqual(
            [o["trip_ids"] for o in report["options"]], [["D1"], ["H1", "E1"]]
        )

        # Correction then void through the real CLI; K replay stays stable.
        (self.root / "events.jsonl").write_bytes(
            (DATA / "events-v2-e1-shift.jsonl").read_bytes()
        )
        self.ingest_cli(fmt="v2")
        (self.root / "events.jsonl").write_bytes(
            (DATA / "events-v2-void-d1.jsonl").read_bytes()
        )
        self.ingest_cli(fmt="v2")
        proc = self.connections_cli("--known-at", "1")
        self.assertEqual(json.loads(proc.stdout)["connections"]["options"], 2)
        k1 = (self.root / "conn" / "connections.json").read_bytes()
        self.assertEqual(k1, (self.root / "conn" / "connections.json").read_bytes())
        proc = self.connections_cli("--known-at", "3")
        summary = json.loads(proc.stdout)["connections"]
        self.assertEqual((summary["options"], summary["candidate_trips"]), (1, 4))
        latest = json.loads((self.root / "conn" / "connections.json").read_text())
        self.assertEqual([o["trip_ids"] for o in latest["options"]], [["H1", "E1"]])
        # Rebuilding K=1 after later commits is byte-identical.
        self.connections_cli("--known-at", "1")
        self.assertEqual((self.root / "conn" / "connections.json").read_bytes(), k1)

    def test_cli_unreachable_is_a_normal_result(self):
        self.ingest_cli()
        request = json.loads((self.root / "request.json").read_text())
        request["origin"], request["destination"] = "North", "Harbor"
        (self.root / "request.json").write_text(json.dumps(request), encoding="utf-8")
        proc = self.connections_cli()
        summary = json.loads(proc.stdout)["connections"]
        self.assertEqual((summary["status"], summary["options"]), ("no_route", 0))
        report = json.loads((self.root / "conn" / "connections.json").read_text())
        self.assertEqual(report["diagnostics"]["reason"], "origin_not_served")

    def test_cli_bad_request_keeps_previous_report(self):
        self.ingest_cli()
        self.connections_cli()
        good = (self.root / "conn" / "connections.json").read_bytes()
        (self.root / "request.json").write_text('{"schema_version": 1}', encoding="utf-8")
        proc = self.connections_cli(expect_ok=False)
        self.assertEqual(proc.returncode, 2)
        self.assertIn("observation failed", proc.stderr)
        self.assertEqual((self.root / "conn" / "connections.json").read_bytes(), good)
        # Missing request file and missing database also fail without writes.
        proc = self.run_cli(
            "--connections", "missing.json", "--db", "ferry.sqlite",
            "--output", "conn", "--as-of", CUTOFF,
        )
        self.assertEqual(proc.returncode, 2)
        proc = self.run_cli(
            "--connections", "../data-request.json", "--db", "missing.sqlite",
            "--output", "conn2", "--as-of", CUTOFF,
        )
        self.assertEqual(proc.returncode, 2)
        self.assertFalse((self.root / "missing.sqlite").exists())
        self.assertEqual((self.root / "conn" / "connections.json").read_bytes(), good)

    def test_cli_mode_exclusivity(self):
        self.ingest_cli()
        proc = self.run_cli(
            "--connections", "request.json", "--report-only",
            "--db", "ferry.sqlite", "--output", "conn", "--as-of", CUTOFF,
        )
        self.assertEqual(proc.returncode, 2)
        proc = self.run_cli(
            "--upgrade-db", "--connections", "request.json", "--db", "ferry.sqlite",
        )
        self.assertEqual(proc.returncode, 2)
        proc = self.run_cli(
            "--connections", "request.json", "--db", "ferry.sqlite",
            "--output", "conn",
        )
        self.assertEqual(proc.returncode, 2)  # --as-of is required
        # The old entry points are untouched: default ingest still works.
        proc = self.run_cli(
            "--report-only", "--db", "ferry.sqlite", "--output", "perf",
            "--as-of", CUTOFF,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)


if __name__ == "__main__":
    unittest.main()
