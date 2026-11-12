"""Revision ledger, double cutoff, legacy migration and CLI end-to-end tests."""
import json
import sqlite3
import subprocess
import sys
import tempfile
import textwrap
import threading
import unittest
from pathlib import Path

from ferry_observer.analysis import performance, write_report
from ferry_observer.storage import ingest, upgrade_database

DATA = Path(__file__).resolve().parents[1] / "data"
CUTOFF = "2026-01-01T12:00:00Z"
SCHEDULE_CSV = (DATA / "schedule.csv").read_bytes()


def write_jsonl(path, rows):
    path.write_text("\n".join(json.dumps(r) for r in rows) + ("\n" if rows else ""))


def upsert(event_id, revision, trip_id, kind, occurred_at):
    return {
        "event_id": event_id, "revision": revision, "action": "upsert",
        "trip_id": trip_id, "kind": kind, "occurred_at": occurred_at,
    }


def void(event_id, revision):
    return {"event_id": event_id, "revision": revision, "action": "void"}


class RevisionCase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.schedule = self.root / "schedule.csv"
        self.events = self.root / "events.jsonl"
        self.db = self.root / "ferry.sqlite"
        self.schedule.write_bytes(SCHEDULE_CSV)
        self.events.write_bytes((DATA / "events.jsonl").read_bytes())

    def ingest(self, event_format="v1"):
        return ingest(self.schedule, self.events, self.db, event_format)

    def report(self, cutoff=CUTOFF, known_at=None):
        return performance(self.db, cutoff, known_at)

    def route(self, **kwargs):
        return self.report(**kwargs)["routes"][0]

    def write_v2(self, rows):
        write_jsonl(self.events, rows)

    # ------------------------------------------------------------------ basics
    def test_first_ingest_and_replay_keep_fields_and_sequence(self):
        first = self.ingest()
        self.assertEqual(first["inserted"], 5)
        self.assertEqual(first["duplicates"], 1)
        self.assertEqual(first["input_rows"], 6)
        self.assertEqual(first["total_events"], 5)
        self.assertEqual(first["knowledge_seq"], 1)
        replay = self.ingest()
        self.assertEqual(replay["inserted"], 0)
        self.assertEqual(replay["duplicates"], 6)
        self.assertEqual(replay["knowledge_seq"], 1)
        with sqlite3.connect(self.db) as db:
            self.assertEqual(
                db.execute("SELECT count(*) FROM knowledge_batches").fetchone()[0], 1
            )
            self.assertEqual(
                db.execute("SELECT count(DISTINCT source_sha256) FROM revisions").fetchone()[0], 1
            )

    def test_baseline_metrics_at_twelve(self):
        self.ingest()
        route = self.route()
        self.assertEqual(
            (route["due_trips"], route["departed_trips"], route["cancelled_trips"],
             route["pending_trips"]),
            (4, 2, 1, 1),
        )
        self.assertEqual(route["on_time_rate"], 0.5)
        self.assertEqual(route["cancellation_rate"], 0.25)
        self.assertEqual(route["mean_travel_seconds"], 2220)
        self.assertEqual(route["mean_departure_delay_seconds"], 330)

    def test_empty_and_duplicate_only_files_add_no_sequence(self):
        self.ingest()
        self.events.write_text("")
        empty = self.ingest("v2")
        self.assertEqual(empty["inserted"], 0)
        self.assertEqual(empty["knowledge_seq"], 1)
        self.write_v2([upsert("e3", 1, "F2", "departed", "2026-01-01T08:39:00Z")])
        dup = self.ingest("v2")
        self.assertEqual(dup["inserted"], 0)
        self.assertEqual(dup["duplicates"], 1)
        self.assertEqual(dup["knowledge_seq"], 1)
        with sqlite3.connect(self.db) as db:
            self.assertEqual(db.execute("SELECT count(*) FROM sources").fetchone()[0], 3)
            self.assertEqual(db.execute("SELECT count(*) FROM knowledge_batches").fetchone()[0], 1)

    # ------------------------------------------------------------- corrections
    def test_v2_correction_changes_latest_but_not_history(self):
        self.ingest()
        self.events.write_bytes((DATA / "events-v2-correction.jsonl").read_bytes())
        second = self.ingest("v2")
        self.assertEqual(second["inserted"], 1)
        self.assertEqual(second["knowledge_seq"], 2)
        latest = self.route()
        self.assertEqual(latest["on_time_rate"], 1)
        self.assertEqual(latest["mean_departure_delay_seconds"], 180)
        self.assertEqual(latest["mean_travel_seconds"], 2370)
        historical = self.route(known_at=1)
        self.assertEqual(historical["on_time_rate"], 0.5)
        self.assertEqual(historical["mean_departure_delay_seconds"], 330)
        self.assertEqual(historical["mean_travel_seconds"], 2220)

    def test_replayed_old_revision_and_equivalent_offset_are_duplicates(self):
        self.ingest()
        self.events.write_bytes((DATA / "events-v2-correction.jsonl").read_bytes())
        self.ingest("v2")
        before = self.report()
        # Old revision 1 replayed in two equivalent UTC representations.
        self.write_v2([
            upsert("e3", 1, "F2", "departed", "2026-01-01T08:39:00Z"),
            upsert("e3", 1, "F2", "departed", "2026-01-01T09:39:00+01:00"),
        ])
        result = self.ingest("v2")
        self.assertEqual(result["inserted"], 0)
        self.assertEqual(result["duplicates"], 2)
        self.assertEqual(result["knowledge_seq"], 2)
        self.assertEqual(self.report(), before)

    def test_equivalent_offsets_and_key_order_normalize_same_revision(self):
        helper = self.root / "normalize.jsonl"
        helper.write_text(
            '{"event_id":"n","revision":1,"action":"upsert","trip_id":"F4",'
            '"kind":"departed","occurred_at":"2026-01-01T10:00:00Z"}\n'
            '{"occurred_at":"2026-01-01T11:00:00+01:00","kind":"departed",'
            '"trip_id":"F4","action":"upsert","revision":1,"event_id":"n"}\n'
        )
        self.events.write_bytes(helper.read_bytes())
        result = self.ingest("v2")
        self.assertEqual(result["inserted"], 1)
        self.assertEqual(result["duplicates"], 1)

    # -------------------------------------------------------------- v2 contract
    def test_out_of_order_consecutive_revisions_apply_together(self):
        # File order is deliberately scrambled: the arrival's newer revision
        # appears before revision 1, and the departure event sits in between.
        self.write_v2([
            upsert("a1", 2, "F4", "arrived", "2026-01-01T10:41:00Z"),
            upsert("d1", 1, "F4", "departed", "2026-01-01T10:00:00Z"),
            upsert("a1", 1, "F4", "arrived", "2026-01-01T10:40:00Z"),
        ])
        result = self.ingest("v2")
        self.assertEqual(result["inserted"], 3)
        self.assertEqual(result["knowledge_seq"], 1)
        f4 = next(t for t in self.report()["trips"] if t["trip_id"] == "F4")
        self.assertEqual(f4["state"], "arrived")
        self.assertEqual(f4["travel_seconds"], 2460)

    def test_same_revision_duplicate_inside_batch(self):
        self.write_v2([
            upsert("new", 1, "F4", "departed", "2026-01-01T10:00:00Z"),
            upsert("new", 1, "F4", "departed", "2026-01-01T10:00:00Z"),
        ])
        result = self.ingest("v2")
        self.assertEqual((result["inserted"], result["duplicates"]), (1, 1))

    def test_same_revision_conflict_inside_batch_rejects(self):
        self.write_v2([
            upsert("new", 1, "F4", "departed", "2026-01-01T10:00:00Z"),
            upsert("new", 1, "F4", "departed", "2026-01-01T10:01:00Z"),
        ])
        with self.assertRaisesRegex(ValueError, "conflict"):
            self.ingest("v2")
        # The batch rolled back: no observatory tables were created at all.
        with sqlite3.connect(self.db) as db:
            tables = {r[0] for r in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertEqual(tables, set())

    def test_missing_first_revision_and_skips_rejected(self):
        self.write_v2([upsert("new", 2, "F4", "departed", "2026-01-01T10:00:00Z")])
        with self.assertRaisesRegex(ValueError, "continuous"):
            self.ingest("v2")
        self.events.write_bytes((DATA / "events.jsonl").read_bytes())
        self.ingest()  # establish e1..e5 at sequence 1
        self.write_v2([upsert("e3", 3, "F2", "departed", "2026-01-01T08:34:00Z")])
        with self.assertRaisesRegex(ValueError, "continuous"):
            self.ingest("v2")

    def test_boolean_revision_rejected(self):
        self.write_v2([
            {"event_id": "new", "revision": True, "action": "upsert",
             "trip_id": "F4", "kind": "departed", "occurred_at": "2026-01-01T10:00:00Z"},
        ])
        with self.assertRaisesRegex(ValueError, "positive integer"):
            self.ingest("v2")

    def test_unknown_event_void_rejected(self):
        self.write_v2([void("ghost", 1)])
        with self.assertRaisesRegex(ValueError, "never been upserted"):
            self.ingest("v2")

    def test_cross_trip_correction_rejected(self):
        self.ingest()
        self.write_v2([upsert("e3", 2, "F1", "departed", "2026-01-01T08:34:00Z")])
        with self.assertRaisesRegex(ValueError, "trip_id cannot change"):
            self.ingest("v2")

    def test_unknown_trip_reference_rejected(self):
        self.write_v2([upsert("new", 1, "F9", "departed", "2026-01-01T10:00:00Z")])
        with self.assertRaisesRegex(ValueError, "unknown trip"):
            self.ingest("v2")

    def test_void_carries_no_business_fields(self):
        helper = self.root / "bad-void.jsonl"
        helper.write_text(json.dumps({
            "event_id": "e1", "revision": 2, "action": "void",
            "occurred_at": "2026-01-01T08:02:00Z",
        }) + "\n")
        self.events.write_bytes(helper.read_bytes())
        with self.assertRaisesRegex(ValueError, "line 1"):
            self.ingest("v2")

    def test_every_revision_is_validated_even_when_superseded(self):
        # Revision 2 would win selection, but the malformed revision 1 in the
        # same file must still reject the whole batch at contract parsing.
        self.write_v2([
            upsert("new", 2, "F4", "departed", "2026-01-01T10:01:00Z"),
        ])
        with self.events.open("a", encoding="utf-8") as stream:
            stream.write('{"event_id":"new","revision":1,"action":"upsert",'
                         '"trip_id":"F4","kind":"departed","occurred_at":"not-a-timestamp"}\n')
        with self.assertRaisesRegex(ValueError, "line 2"):
            self.ingest("v2")
        self.assertFalse(self.db.exists())

    # --------------------------------------------------------- batch lifecycle
    def test_batch_moving_departure_and_arrival_together_applies(self):
        # Row-by-row this transiently puts the arrival before the new
        # departure; the final merged state is valid and must be accepted.
        self.ingest()
        self.write_v2([
            upsert("e1", 2, "F1", "departed", "2026-01-01T09:02:00Z"),
            upsert("e2", 2, "F1", "arrived", "2026-01-01T09:40:00Z"),
        ])
        result = self.ingest("v2")
        self.assertEqual(result["knowledge_seq"], 2)
        f1 = next(t for t in self.report()["trips"] if t["trip_id"] == "F1")
        self.assertEqual(f1["state"], "arrived")
        self.assertEqual(f1["travel_seconds"], 2280)

    def test_voiding_departure_but_keeping_arrival_fails(self):
        self.ingest()
        self.write_v2([void("e1", 2)])
        with self.assertRaisesRegex(ValueError, "lifecycle"):
            self.ingest("v2")
        with sqlite3.connect(self.db) as db:
            self.assertEqual(db.execute("SELECT count(*) FROM revisions").fetchone()[0], 5)
            self.assertEqual(db.execute("SELECT max(knowledge_seq) FROM knowledge_batches").fetchone()[0], 1)
            self.assertEqual(db.execute("SELECT count(*) FROM event_projection").fetchone()[0], 5)
            # The rejected batch must not leave a source registration either.
            self.assertEqual(db.execute("SELECT count(*) FROM sources").fetchone()[0], 1)
            self.assertEqual(
                db.execute("SELECT count(*) FROM revisions WHERE source_sha256 IS NULL").fetchone()[0], 0
            )

    def test_voiding_both_events_of_trip_succeeds_then_restore(self):
        self.ingest()
        self.write_v2([void("e1", 2), void("e2", 2)])
        result = self.ingest("v2")
        self.assertEqual(result["knowledge_seq"], 2)
        latest = self.report()
        f1 = next(t for t in latest["trips"] if t["trip_id"] == "F1")
        self.assertEqual(f1["state"], "pending")
        historical = self.report(known_at=1)
        f1_old = next(t for t in historical["trips"] if t["trip_id"] == "F1")
        self.assertEqual(f1_old["state"], "arrived")
        # Consecutive versions restore the trip in one later batch.
        self.events.write_bytes((DATA / "events-v2-void-restore.jsonl").read_bytes())
        restored = self.ingest("v2")
        self.assertEqual(restored["knowledge_seq"], 3)
        f1_new = next(t for t in self.report()["trips"] if t["trip_id"] == "F1")
        self.assertEqual(f1_new["state"], "arrived")
        self.assertEqual(f1_new["travel_seconds"], 2220)

    # ------------------------------------------------------------ double cutoff
    def test_correction_past_cutoff_does_not_resurrect_old_value(self):
        # Stage 1: F4 departs at 10:00 and is underway at the 12:00 cutoff.
        self.write_v2([upsert("f4", 1, "F4", "departed", "2026-01-01T10:00:00Z")])
        self.ingest("v2")
        # Stage 2: the departure is corrected to 12:30, after the cutoff.
        self.write_v2([upsert("f4", 2, "F4", "departed", "2026-01-01T12:30:00Z")])
        self.ingest("v2")
        old_k = self.report(known_at=1)
        new_k = self.report(known_at=2)
        f4_old = next(t for t in old_k["trips"] if t["trip_id"] == "F4")
        f4_new = next(t for t in new_k["trips"] if t["trip_id"] == "F4")
        self.assertEqual(f4_old["state"], "underway")
        self.assertEqual(f4_new["state"], "pending")
        statuses = {str(e["event_id"]) + str(e["revision"]): e["status"]
                    for e in f4_new["event_revisions"]}
        self.assertEqual(statuses["f41"], "superseded")
        self.assertEqual(statuses["f42"], "after_cutoff")

    # -------------------------------------------------------- evidence + output
    def test_evidence_statuses_and_schema_v2(self):
        self.ingest()
        self.events.write_bytes((DATA / "events-v2-correction.jsonl").read_bytes())
        self.ingest("v2")
        report = self.report()
        self.assertEqual(report["schema_version"], 2)
        self.assertEqual(report["knowledge_seq"], 2)
        f2 = next(t for t in report["trips"] if t["trip_id"] == "F2")
        e3_evidence = {e["revision"]: e for e in f2["event_revisions"] if e["event_id"] == "e3"}
        self.assertEqual(e3_evidence[1]["status"], "superseded")
        self.assertEqual(e3_evidence[2]["status"], "used")
        self.assertEqual(e3_evidence[2]["knowledge_seq"], 2)
        self.assertIsNotNone(e3_evidence[2]["first_source_sha256"])
        self.assertEqual(e3_evidence[2]["canonical_payload"]["kind"], "departed")
        # No knowledge past K leaks into a historical report.
        hist = self.report(known_at=1)
        f2_hist = next(t for t in hist["trips"] if t["trip_id"] == "F2")
        self.assertEqual(
            [(e["event_id"], e["revision"]) for e in f2_hist["event_revisions"]],
            [("e3", 1), ("e4", 1)],
        )

    def test_byte_stable_historical_report_after_later_commits(self):
        self.ingest()
        out1 = self.root / "out1"
        write_report(self.report(known_at=1), out1)
        first_json = (out1 / "performance.json").read_bytes()
        first_csv = (out1 / "trips.csv").read_bytes()
        self.events.write_bytes((DATA / "events-v2-correction.jsonl").read_bytes())
        self.ingest("v2")
        out2 = self.root / "out2"
        write_report(self.report(known_at=1), out2)
        self.assertEqual(first_json, (out2 / "performance.json").read_bytes())
        self.assertEqual(first_csv, (out2 / "trips.csv").read_bytes())
        self.assertNotIn(b"/workspace", first_json)

    def test_csv_carries_knowledge_seq_and_matches_json(self):
        self.ingest()
        out = self.root / "out"
        write_report(self.report(), out)
        header = (out / "trips.csv").read_text().splitlines()[0].split(",")
        self.assertEqual(
            header,
            ["trip_id", "route", "state", "departure_delay_seconds",
             "on_time", "travel_seconds", "knowledge_seq"],
        )

    def test_report_only_guardrails(self):
        missing = self.root / "missing.sqlite"
        with self.assertRaisesRegex(ValueError, "does not exist"):
            performance(missing, CUTOFF)
        self.assertFalse(missing.exists())
        self.ingest()
        with self.assertRaisesRegex(ValueError, "known_at"):
            performance(self.db, CUTOFF, known_at=5)
        with self.assertRaisesRegex(ValueError, "known_at"):
            performance(self.db, CUTOFF, known_at=-1)
        # Read-only reporting cannot mutate the database.
        before = self.db.read_bytes()
        self.report()
        self.assertEqual(before, self.db.read_bytes())

    def test_known_at_zero_shows_schedule_only(self):
        self.ingest()
        report = self.report(known_at=0)
        self.assertEqual(report["knowledge_seq"], 0)
        self.assertEqual(report["routes"][0]["pending_trips"], 4)
        self.assertEqual(
            [e for t in report["trips"] for e in t["event_revisions"]], []
        )

    # ---------------------------------------------------- failure + concurrency
    def test_failed_batch_consumes_no_sequence_and_next_uses_next(self):
        self.ingest()
        self.write_v2([void("ghost", 1)])
        with self.assertRaises(ValueError):
            self.ingest("v2")
        self.write_v2([upsert("e3", 2, "F2", "departed", "2026-01-01T08:34:00Z")])
        result = self.ingest("v2")
        self.assertEqual(result["knowledge_seq"], 2)

    def test_concurrent_batches_get_distinct_sequences(self):
        self.ingest()
        barrier = threading.Barrier(2)
        errors = []

        def worker(event_id, trip, occurred):
            path = self.root / f"{event_id}.jsonl"
            write_jsonl(path, [upsert(event_id, 1, trip, "departed", occurred)])
            barrier.wait()
            try:
                ingest(self.schedule, path, self.db, "v2")
            except Exception as exc:  # pragma: no cover - failure path reporting
                errors.append(exc)

        t1 = threading.Thread(target=worker, args=("c1", "F4", "2026-01-01T10:01:00Z"))
        t2 = threading.Thread(target=worker, args=("c2", "F5", "2026-01-01T13:01:00Z"))
        t1.start(); t2.start(); t1.join(); t2.join()
        self.assertEqual(errors, [])
        with sqlite3.connect(self.db) as db:
            seqs = [r[0] for r in db.execute(
                "SELECT DISTINCT knowledge_seq FROM revisions WHERE event_id IN ('c1','c2')"
            )]
            self.assertEqual(sorted(seqs), [2, 3])
            self.assertEqual(
                db.execute("SELECT count(*) FROM knowledge_batches").fetchone()[0], 3
            )

    def test_bounded_lock_wait_serializes_without_duplicate_sequence(self):
        self.ingest()
        import time
        started = threading.Event()

        def hold_lock():
            holder = sqlite3.connect(self.db, timeout=5)
            holder.execute("PRAGMA busy_timeout=5000")
            holder.execute("BEGIN IMMEDIATE")
            started.set()
            time.sleep(1.2)
            holder.rollback()
            holder.close()

        thread = threading.Thread(target=hold_lock)
        thread.start()
        started.wait()
        path = self.root / "held.jsonl"
        write_jsonl(path, [upsert("held", 1, "F4", "departed", "2026-01-01T10:02:00Z")])
        # The second writer waits on the bounded busy_timeout, then acquires the
        # lock and takes the immediate next sequence.
        result = ingest(self.schedule, path, self.db, "v2")
        thread.join()
        self.assertEqual(result["knowledge_seq"], 2)
        with sqlite3.connect(self.db) as db:
            self.assertEqual(
                db.execute("SELECT knowledge_seq FROM revisions WHERE event_id='held'").fetchone()[0], 2
            )


def build_legacy_database(path):
    """Create a genuine v1-layout database using the old DDL, not the new one."""
    with sqlite3.connect(path) as db:
        db.executescript(
            """
            CREATE TABLE trips (
             trip_id TEXT PRIMARY KEY, route TEXT NOT NULL, planned_departure TEXT NOT NULL,
             planned_arrival TEXT NOT NULL);
            CREATE TABLE events (
             event_id TEXT PRIMARY KEY, trip_id TEXT NOT NULL REFERENCES trips(trip_id),
             kind TEXT NOT NULL, occurred_at TEXT NOT NULL, canonical_json TEXT NOT NULL);
            CREATE TABLE sources (sha256 TEXT PRIMARY KEY, row_count INTEGER NOT NULL);
            """
        )
        import csv as _csv
        with open(DATA / "schedule.csv", newline="", encoding="utf-8") as stream:
            for row in _csv.DictReader(stream):
                db.execute("INSERT INTO trips VALUES (?,?,?,?)", tuple(row.values()))
        for line in (DATA / "events.jsonl").read_text().splitlines():
            row = json.loads(line)
            canonical = json.dumps(row, sort_keys=True, separators=(",", ":"))
            db.execute(
                "INSERT OR IGNORE INTO events VALUES (?,?,?,?,?)",
                (row["event_id"], row["trip_id"], row["kind"], row["occurred_at"], canonical),
            )
        db.execute(
            "INSERT INTO sources VALUES (?,?)",
            ("legacydeadbeef", 6),
        )


class LegacyMigrationCase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.legacy = self.root / "legacy.sqlite"
        build_legacy_database(self.legacy)

    def test_metrics_match_and_provenance_gap_is_honest(self):
        before = self._legacy_metric_basis()
        result = upgrade_database(self.legacy)
        self.assertTrue(result["upgraded"])
        self.assertEqual(result["migrated_events"], 5)
        self.assertEqual(result["knowledge_seq"], 1)
        # Legacy tables and sources are preserved.
        with sqlite3.connect(self.legacy) as db:
            self.assertEqual(db.execute("SELECT count(*) FROM events").fetchone()[0], 5)
            self.assertEqual(db.execute("SELECT sha256 FROM sources").fetchone()[0],
                             "legacydeadbeef")
            null_sources = db.execute(
                "SELECT count(*) FROM revisions WHERE source_sha256 IS NOT NULL"
            ).fetchone()[0]
            self.assertEqual(null_sources, 0)
            note = db.execute("SELECT value FROM meta WHERE key='legacy_note'").fetchone()[0]
            self.assertIn("legacy_unattributed", note)
        report = performance(self.legacy, CUTOFF)
        route = report["routes"][0]
        self.assertEqual((route["due_trips"], route["departed_trips"],
                          route["cancelled_trips"], route["pending_trips"]), before)
        self.assertEqual(route["on_time_rate"], 0.5)
        self.assertEqual(route["mean_travel_seconds"], 2220)
        entry = report["trips"][0]["event_revisions"][0]
        self.assertIsNone(entry["first_source_sha256"])
        self.assertIn("legacy_unattributed", entry["source_note"])

    def _legacy_metric_basis(self):
        with sqlite3.connect(self.legacy) as db:
            return (4, 2, 1, 1)

    def test_upgrade_is_idempotent(self):
        first = upgrade_database(self.legacy)
        second = upgrade_database(self.legacy)
        self.assertFalse(second["upgraded"])
        self.assertEqual(second["knowledge_seq"], 1)
        with sqlite3.connect(self.legacy) as db:
            self.assertEqual(db.execute("SELECT count(*) FROM revisions").fetchone()[0], 5)

    def test_ingest_and_report_on_legacy_request_upgrade(self):
        schedule = self.root / "schedule.csv"
        schedule.write_bytes(SCHEDULE_CSV)
        events = self.root / "events.jsonl"
        events.write_bytes((DATA / "events-v2-correction.jsonl").read_bytes())
        with self.assertRaisesRegex(ValueError, "upgrade-db"):
            ingest(schedule, events, self.legacy, "v2")
        with self.assertRaisesRegex(ValueError, "upgrade-db"):
            performance(self.legacy, CUTOFF)

    def test_failed_migration_leaves_no_partial_state(self):
        # Corrupt the legacy database so the migration transaction fails.
        with sqlite3.connect(self.legacy) as db:
            db.execute("DROP TABLE sources")
        with self.assertRaises(ValueError):
            upgrade_database(self.legacy)
        with sqlite3.connect(self.legacy) as db:
            tables = {r[0] for r in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertNotIn("revisions", tables)
            self.assertNotIn("meta", tables)
            self.assertEqual(db.execute("SELECT count(*) FROM events").fetchone()[0], 5)
        # The same file upgrades cleanly once the obstruction is repaired.
        with sqlite3.connect(self.legacy) as db:
            db.execute("CREATE TABLE sources (sha256 TEXT PRIMARY KEY, row_count INTEGER NOT NULL)")
        result = upgrade_database(self.legacy)
        self.assertTrue(result["upgraded"])

    def test_legacy_without_events_stays_at_sequence_zero(self):
        empty_legacy = self.root / "empty-legacy.sqlite"
        with sqlite3.connect(empty_legacy) as db:
            db.executescript(
                """
                CREATE TABLE trips (trip_id TEXT PRIMARY KEY, route TEXT NOT NULL,
                 planned_departure TEXT NOT NULL, planned_arrival TEXT NOT NULL);
                CREATE TABLE events (event_id TEXT PRIMARY KEY, trip_id TEXT NOT NULL,
                 kind TEXT NOT NULL, occurred_at TEXT NOT NULL, canonical_json TEXT NOT NULL);
                CREATE TABLE sources (sha256 TEXT PRIMARY KEY, row_count INTEGER NOT NULL);
                """
            )
        result = upgrade_database(empty_legacy)
        self.assertTrue(result["upgraded"])
        self.assertEqual(result["knowledge_seq"], 0)
        report = performance(empty_legacy, CUTOFF)
        self.assertEqual(report["knowledge_seq"], 0)

    def test_upgrade_requires_existing_file(self):
        with self.assertRaisesRegex(ValueError, "existing database"):
            upgrade_database(self.root / "never.sqlite")


class CliEndToEndCase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.schedule = self.root / "schedule.csv"
        self.schedule.write_bytes(SCHEDULE_CSV)
        self.events = self.root / "events.jsonl"
        self.events.write_bytes((DATA / "events.jsonl").read_bytes())
        self.db = self.root / "ferry.sqlite"
        self.out = self.root / "report"
        self.env = dict(__import__("os").environ)
        self.env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])

    def run_cli(self, *args):
        return subprocess.run(
            [sys.executable, "-m", "ferry_observer", *args],
            cwd=self.root, capture_output=True, text=True, env=self.env,
        )

    def test_v1_ingest_v2_correct_and_historical_rebuild(self):
        proc = self.run_cli(
            "--schedule", "schedule.csv", "--events", "events.jsonl",
            "--db", "ferry.sqlite", "--output", "report",
            "--as-of", CUTOFF,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        payload = json.loads(proc.stdout.strip().splitlines()[-1])
        self.assertEqual(payload["ingestion"]["inserted"], 5)
        self.events.write_bytes((DATA / "events-v2-correction.jsonl").read_bytes())
        proc = self.run_cli(
            "--schedule", "schedule.csv", "--events", "events.jsonl",
            "--event-format", "v2", "--db", "ferry.sqlite",
            "--output", "report", "--as-of", CUTOFF,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(json.loads(proc.stdout.strip().splitlines()[-1])["ingestion"]["knowledge_seq"], 2)

        historical = self.root / "historical"
        proc = self.run_cli(
            "--report-only", "--db", "ferry.sqlite", "--output", "historical",
            "--as-of", CUTOFF, "--known-at", "1",
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        hist = json.loads((historical / "performance.json").read_text())
        self.assertEqual(hist["knowledge_seq"], 1)
        self.assertEqual(hist["routes"][0]["on_time_rate"], 0.5)
        latest = json.loads((self.out / "performance.json").read_text())
        self.assertEqual(latest["routes"][0]["on_time_rate"], 1)

        # Historical rebuild is byte-identical across reruns and never creates DBs.
        again = self.root / "historical2"
        proc = self.run_cli(
            "--report-only", "--db", "ferry.sqlite", "--output", "historical2",
            "--as-of", CUTOFF, "--known-at", "1",
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(
            (historical / "performance.json").read_bytes(),
            (again / "performance.json").read_bytes(),
        )
        proc = self.run_cli(
            "--report-only", "--db", "missing.sqlite", "--output", "nope",
            "--as-of", CUTOFF,
        )
        self.assertNotEqual(proc.returncode, 0)
        self.assertFalse((self.root / "missing.sqlite").exists())
        # Invalid K must not overwrite the existing good report.
        good = (self.out / "performance.json").read_bytes()
        proc = self.run_cli(
            "--report-only", "--db", "ferry.sqlite", "--output", "report",
            "--as-of", CUTOFF, "--known-at", "99",
        )
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual((self.out / "performance.json").read_bytes(), good)

    def test_upgrade_db_cli_then_ingest(self):
        legacy = self.root / "legacy.sqlite"
        build_legacy_database(legacy)
        proc = self.run_cli("--upgrade-db", "--db", "legacy.sqlite")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertTrue(json.loads(proc.stdout)["upgraded"])
        proc = self.run_cli("--upgrade-db", "--db", "legacy.sqlite")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertFalse(json.loads(proc.stdout)["upgraded"])
        # Exclusive mode.
        proc = self.run_cli("--upgrade-db", "--db", "legacy.sqlite", "--as-of", CUTOFF)
        self.assertNotEqual(proc.returncode, 0)


if __name__ == "__main__":
    unittest.main()
