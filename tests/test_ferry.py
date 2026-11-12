import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from ferry_observer.storage import ingest
from ferry_observer.analysis import performance, write_report

DATA = Path(__file__).resolve().parents[1]/"data"
CUTOFF = "2026-01-01T12:00:00Z"


class FerryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.schedule = self.root/"schedule.csv"
        self.events = self.root/"events.jsonl"
        self.db = self.root/"ferry.sqlite"
        self.schedule.write_bytes((DATA/"schedule.csv").read_bytes())
        self.events.write_bytes((DATA/"events.jsonl").read_bytes())

    def ingest(self):
        return ingest(self.schedule,self.events,self.db)

    def test_out_of_order_duplicate_and_business_denominators(self):
        self.assertEqual(self.ingest()["inserted"], 5)
        report = performance(self.db,CUTOFF)
        route = report["routes"][0]
        self.assertEqual((route["due_trips"],route["departed_trips"],route["cancelled_trips"],route["pending_trips"]),(4,2,1,1))
        self.assertEqual(route["on_time_rate"], .5)
        self.assertEqual(route["cancellation_rate"], .25)
        self.assertEqual(route["mean_travel_seconds"], 2220)
        self.assertEqual(route["mean_departure_delay_seconds"], 330)
        self.assertNotIn("F5", [t["trip_id"] for t in report["trips"]])

    def test_replay_leaves_same_report(self):
        self.ingest()
        first = performance(self.db,CUTOFF)
        self.assertEqual(self.ingest()["inserted"],0)
        self.assertEqual(first,performance(self.db,CUTOFF))
        write_report(first,self.root/"out")
        before = (self.root/"out/performance.json").read_bytes()
        write_report(performance(self.db,CUTOFF),self.root/"out")
        self.assertEqual(before,(self.root/"out/performance.json").read_bytes())

    def test_as_of_excludes_future_events_and_no_denominator(self):
        self.ingest()
        early = performance(self.db,"2026-01-01T08:01:00Z")["routes"][0]
        self.assertEqual(early["departed_trips"],0)
        self.assertIsNone(early["on_time_rate"])
        self.assertEqual(early["pending_trips"],1)
        self.assertEqual(performance(self.db,"2026-01-01T07:00:00Z")["routes"],[])

    def test_conflicting_identity_rolls_back(self):
        self.ingest()
        self.events.write_text('{"event_id":"new","trip_id":"F4","kind":"departed","occurred_at":"2026-01-01T10:00:00Z"}\n'+self.events.read_text().replace('"08:02', '"08:03').replace('T08:02', 'T08:03'))
        with self.assertRaisesRegex(ValueError,"conflict"):
            self.ingest()
        with sqlite3.connect(self.db) as db:
            self.assertEqual(db.execute("SELECT count(*) FROM event_projection").fetchone()[0],5)
            self.assertEqual(db.execute("SELECT count(*) FROM revisions").fetchone()[0],5)
            self.assertEqual(db.execute("SELECT count(*) FROM sources").fetchone()[0],1)

    def test_unknown_trip(self):
        self.events.write_text(self.events.read_text().replace('"F2"','"unknown"'))
        with self.assertRaisesRegex(ValueError,"unknown"):
            self.ingest()

    def test_arrival_without_departure_and_cancel_after_departure(self):
        for kind in ["arrived","cancelled"]:
            rows=[{"event_id":"x","trip_id":"F4","kind":kind,"occurred_at":"2026-01-01T10:10:00Z"}]
            if kind=="cancelled":
                rows.insert(0,{"event_id":"y","trip_id":"F4","kind":"departed","occurred_at":"2026-01-01T10:00:00Z"})
            self.events.write_text("\n".join(json.dumps(r) for r in rows)+"\n")
            with self.assertRaisesRegex(ValueError,"lifecycle"):
                self.ingest()

    def test_immutable_schedule(self):
        self.ingest()
        self.schedule.write_text(self.schedule.read_text().replace("Harbor-East","Other"))
        with self.assertRaisesRegex(ValueError,"immutable"):
            self.ingest()

    def test_on_time_inclusive_and_early_is_not_on_time(self):
        for timestamp,expected in [("08:05:00",True),("08:05:01",False),("07:59:59",False)]:
            self.events.write_text(json.dumps({"event_id":"single","trip_id":"F1","kind":"departed","occurred_at":f"2026-01-01T{timestamp}Z"})+"\n")
            db = self.root/(timestamp.replace(":","")+".sqlite")
            ingest(self.schedule,self.events,db)
            self.assertIs(performance(db,CUTOFF)["trips"][0]["on_time"],expected)

    def test_empty_event_feed_keeps_pending_and_bad_json_fails(self):
        self.events.write_text("")
        self.assertEqual(self.ingest()["total_events"],0)
        self.assertEqual(performance(self.db,CUTOFF)["routes"][0]["pending_trips"],4)
        self.events.write_text("not-json\n")
        with self.assertRaisesRegex(ValueError,"line 1"):
            self.ingest()


if __name__ == "__main__":
    unittest.main()
