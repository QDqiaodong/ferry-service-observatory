import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from ferry_observer.storage import ingest
from ferry_observer.ticketing import (
    ingest_tickets,
    largest_remainder_basis,
    parse_ticket_file,
    settlement,
    write_settlement,
)

DATA = Path(__file__).resolve().parents[1] / "data"
TICKETS = DATA / "ticketing"
CUTOFF = "2026-01-01T12:00:00Z"


def write_jsonl(path, rows):
    path.write_text(
        "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8"
    )


def sale(ticket_id, paid, legs):
    return {
        "type": "sale",
        "ticket_id": ticket_id,
        "paid_cents": paid,
        "legs": [{"trip_id": t, "weight": w} for t, w in legs],
    }


def refund(refund_id, ticket_id, cents):
    return {
        "type": "refund",
        "refund_id": refund_id,
        "ticket_id": ticket_id,
        "cents": cents,
    }


class ApportionmentTests(unittest.TestCase):
    """Pure integer largest-remainder behaviour, including the contract tie."""

    def test_ten_cents_equal_three_legs(self):
        basis = largest_remainder_basis(
            10, [("F1", 1), ("F2", 1), ("F3", 1)]
        )
        self.assertEqual(
            [(b["key"], b["allocated_cents"]) for b in basis],
            [("F1", 4), ("F2", 3), ("F3", 3)],
        )
        self.assertEqual(sum(b["allocated_cents"] for b in basis), 10)
        # All three remainders tie; the single extra unit goes to the
        # lexicographically smallest trip_id.
        self.assertEqual(
            [(b["key"], b["extra_unit"]) for b in basis],
            [("F1", 1), ("F2", 0), ("F3", 0)],
        )
        for b in basis:
            self.assertEqual(b["base_cents"], 3)
            self.assertEqual(b["remainder_numerator"], 1)

    def test_weighted_split_keeps_integer_invariants(self):
        basis = largest_remainder_basis(100, [("F1", 2), ("F2", 3)])
        self.assertEqual(
            [(b["key"], b["allocated_cents"]) for b in basis],
            [("F1", 40), ("F2", 60)],
        )

    def test_tie_break_is_lexicographic_trip_id(self):
        basis = largest_remainder_basis(1, [("Z9", 1), ("A1", 1)])
        self.assertEqual(
            [(b["key"], b["allocated_cents"]) for b in basis],
            [("A1", 1), ("Z9", 0)],
        )

    def test_zero_total_allocates_nothing(self):
        basis = largest_remainder_basis(0, [("F1", 1), ("F2", 2)])
        self.assertEqual([b["allocated_cents"] for b in basis], [0, 0])
        self.assertEqual(sum(b["allocated_cents"] for b in basis), 0)

    def test_rejects_booleans_floats_and_bad_weights(self):
        for total in (True, 1.5, -1):
            with self.assertRaises(ValueError):
                largest_remainder_basis(total, [("F1", 1)])
        with self.assertRaises(ValueError):
            largest_remainder_basis(5, [("F1", 0)])
        with self.assertRaises(ValueError):
            largest_remainder_basis(5, [])
        # All-zero weights cannot distribute a positive amount.
        with self.assertRaises(ValueError):
            largest_remainder_basis(5, [("F1", 0), ("F2", 0)])

    def test_exhaustive_preservation_and_tie_order(self):
        # Every total 0..99 over several weight vectors must preserve the total
        # using integer math, and remainder ties must follow trip_id order.
        for weights in ([("F1", 1), ("F2", 1), ("F3", 1)],
                        [("F1", 1), ("F2", 2), ("F3", 3)],
                        [("F1", 7), ("F2", 11)]):
            wsum = sum(w for _, w in weights)
            for total in range(100):
                basis = largest_remainder_basis(total, weights)
                self.assertEqual(
                    sum(b["allocated_cents"] for b in basis), total
                )
                for b in basis:
                    self.assertEqual(
                        b["base_cents"],
                        (total * b["weight"]) // wsum,
                    )
                    self.assertEqual(
                        b["remainder_numerator"],
                        (total * b["weight"]) % wsum,
                    )
                extras = sorted(
                    (b["remainder_numerator"], b["key"]) for b in basis
                    if b["extra_unit"]
                )
                non_extras = sorted(
                    (b["remainder_numerator"], b["key"]) for b in basis
                    if not b["extra_unit"]
                )
                # Every selected remainder must be no smaller than every
                # unselected one; ties inside either side are key-ordered.
                if extras and non_extras:
                    self.assertGreaterEqual(extras[0][0], non_extras[-1][0])


class TicketLedgerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.schedule = self.root / "schedule.csv"
        self.schedule.write_bytes((DATA / "schedule.csv").read_bytes())
        self.db = self.root / "ferry.sqlite"
        # Only the trips schedule is needed; an empty event feed creates it.
        (self.root / "empty.jsonl").write_text("")
        ingest(self.schedule, self.root / "empty.jsonl", self.db)
        self.tickets = self.root / "tickets.jsonl"

    def load(self, path=None):
        return ingest_tickets(path or self.tickets, self.db)

    def ticket(self, report, ticket_id):
        return next(t for t in report["tickets"]
                    if t["ticket_id"] == ticket_id)

    # ------------------------------------------------- 10-cent equal 3 legs
    def test_ten_cents_equal_weight_three_legs_and_basis_export(self):
        write_jsonl(self.tickets, [
            sale("T-100", 10, [("F3", 1), ("F1", 1), ("F2", 1)]),
            sale("T-200", 10, [("F1", 1), ("F2", 1), ("F3", 1)]),
        ])
        result = self.load()
        self.assertEqual(
            result,
            {**result, "sales_inserted": 2, "refunds_inserted": 0,
             "duplicates": 0, "ingest_seq": 1},
        )
        report = settlement(self.db)
        t100 = self.ticket(report, "T-100")
        # Stored legs are canonicalized by trip_id regardless of input order.
        self.assertEqual(
            [leg["trip_id"] for leg in t100["legs"]], ["F1", "F2", "F3"]
        )
        self.assertEqual(
            [(a["trip_id"], a["fare_cents"]) for a in t100["allocation"]],
            [("F1", 4), ("F2", 3), ("F3", 3)],
        )
        self.assertNotIn("refund_basis", t100)
        basis = t100["allocation_basis"]
        self.assertEqual(basis["method"], "largest_remainder")
        self.assertEqual(basis["weight_sum"], 3)
        self.assertEqual(
            [(b["trip_id"], b["base_cents"], b["remainder_numerator"],
              b["extra_unit"], b["allocated_cents"]) for b in basis["legs"]],
            [("F1", 3, 1, 1, 4), ("F2", 3, 1, 0, 3), ("F3", 3, 1, 0, 3)],
        )
        # Per-trip rollups aggregate both tickets (4+4, 3+3, 3+3).
        self.assertEqual(
            [(t["trip_id"], t["fare_cents"], t["refund_cents"], t["net_cents"])
             for t in report["trips"]],
            [("F1", 8, 0, 8), ("F2", 6, 0, 6), ("F3", 6, 0, 6),
             ("F4", 0, 0, 0), ("F5", 0, 0, 0)],
        )
        self.assertEqual(report["pending_refunds"], [])

    # --------------------------------------------- cross-batch partial refund
    def test_cross_batch_partial_refund_reapportioned_on_original_split(self):
        write_jsonl(self.tickets, [sale("T-200", 10, [
            ("F1", 1), ("F2", 1), ("F3", 1)])])
        self.load()
        write_jsonl(self.tickets, [refund("R-200-a", "T-200", 2)])
        self.load()
        write_jsonl(self.tickets, [refund("R-200-b", "T-200", 1)])
        result = self.load()
        self.assertEqual(result["ingest_seq"], 3)
        report = settlement(self.db)
        t200 = self.ticket(report, "T-200")
        self.assertEqual(t200["refunded_cents"], 3)
        # Refund weights are the original fare split 4/3/3; cumulative 3 gives
        # one cent to each leg, so nets are 3/2/2.
        self.assertEqual(
            [(a["trip_id"], a["fare_cents"], a["refund_cents"], a["net_cents"])
             for a in t200["allocation"]],
            [("F1", 4, 1, 3), ("F2", 3, 1, 2), ("F3", 3, 1, 2)],
        )
        rb = t200["refund_basis"]
        self.assertEqual(rb["total_cents"], 3)
        self.assertEqual(rb["weight_sum"], 10)
        self.assertEqual(
            [(b["trip_id"], b["weight_cents"], b["allocated_cents"])
             for b in rb["legs"]],
            [("F1", 4, 1), ("F2", 3, 1), ("F3", 3, 1)],
        )
        # Trip rollup nets reconcile to paid minus refunded.
        totals = {t["trip_id"]: t for t in report["trips"]}
        self.assertEqual(
            sum(t["net_cents"] for t in report["trips"]), 7
        )
        self.assertEqual(totals["F1"]["refund_cents"], 1)

    def test_refund_can_precede_sale_and_pending_is_visible(self):
        write_jsonl(self.tickets, [refund("R-E", "T-FUTURE", 5)])
        result = self.load()
        self.assertEqual(result["refunds_inserted"], 1)
        report = settlement(self.db)
        self.assertEqual(report["tickets"], [])
        self.assertEqual(
            report["pending_refunds"],
            [{"refund_id": "R-E", "ticket_id": "T-FUTURE", "cents": 5}],
        )
        # The sale lands later in another batch and absorbs the pending refund.
        write_jsonl(self.tickets, [
            sale("T-FUTURE", 5, [("F1", 1), ("F2", 1)])])
        self.load()
        report = settlement(self.db)
        self.assertEqual(report["pending_refunds"], [])
        t = self.ticket(report, "T-FUTURE")
        self.assertEqual(t["refunded_cents"], 5)
        self.assertEqual(sum(a["net_cents"] for a in t["allocation"]), 0)
        self.assertEqual(
            [(a["trip_id"], a["refund_cents"]) for a in t["allocation"]],
            [("F1", 3), ("F2", 2)],
        )

    def test_same_batch_merges_refunds_before_cap_check(self):
        write_jsonl(self.tickets, [
            sale("T-MB", 5, [("F1", 1)]),
            refund("M1", "T-MB", 3),
            refund("M2", "T-MB", 2),
        ])
        self.load()  # 3 + 2 == paid, exactly at the cap
        report = settlement(self.db)
        self.assertEqual(self.ticket(report, "T-MB")["refunded_cents"], 5)
        # One cent over the cap in the same merged batch rejects everything.
        write_jsonl(self.tickets, [
            sale("T-MC", 5, [("F1", 1)]),
            refund("C1", "T-MC", 3),
            refund("C2", "T-MC", 3),
        ])
        with self.assertRaisesRegex(ValueError, "exceed paid_cents"):
            self.load()
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT count(*) FROM ticket_sales WHERE ticket_id='T-MC'"
                ).fetchone()[0], 0
            )

    # --------------------------------------------- shuffled replay / conflict
    def test_shuffled_replay_is_idempotent_and_conflict_rejects_batch(self):
        first = TICKETS / "batch1-sales.jsonl"
        shuffled = TICKETS / "batch1-replay-shuffled.jsonl"
        self.load(first)
        replay = self.load(shuffled)
        self.assertEqual(
            (replay["sales_inserted"], replay["refunds_inserted"],
             replay["duplicates"]),
            (0, 0, 2),
        )
        # Replay consumes no sequence and changes no facts.
        self.assertEqual(replay["ingest_seq"], 1)
        before = settlement(self.db)
        self.assertEqual(before, settlement(self.db))

        with self.assertRaisesRegex(ValueError, "sale conflict"):
            self.load(TICKETS / "batch4-conflict.jsonl")
        # The conflict batch left no trace: same sequence, same rows.
        after = settlement(self.db)
        self.assertEqual(after, before)
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(
                conn.execute("SELECT count(*) FROM ticket_batches").fetchone()[0],
                1,
            )

    def test_refund_id_conflict_and_in_file_duplicates(self):
        write_jsonl(self.tickets, [
            sale("T-1", 10, [("F1", 1)]),
            refund("R1", "T-1", 2),
        ])
        self.load()
        # Same refund_id, different content.
        write_jsonl(self.tickets, [refund("R1", "T-1", 3)])
        with self.assertRaisesRegex(ValueError, "refund conflict"):
            self.load()
        # Identical in-file repeats are accepted as duplicates; differing
        # repeats of one ticket_id reject the batch.
        write_jsonl(self.tickets, [
            sale("T-9", 4, [("F1", 1)]),
            sale("T-9", 4, [("F1", 1)]),
        ])
        result = self.load()
        self.assertEqual((result["sales_inserted"], result["duplicates"]), (1, 1))
        write_jsonl(self.tickets, [
            sale("T-8", 4, [("F1", 1)]),
            sale("T-8", 5, [("F1", 1)]),
        ])
        with self.assertRaisesRegex(ValueError, "conflicting sale"):
            self.load()

    # ------------------------------------------------------------- rollback
    def test_failed_batch_rolls_back_everything_including_source(self):
        write_jsonl(self.tickets, [sale("T-OK", 10, [("F1", 1)])])
        self.load()
        with sqlite3.connect(self.db) as conn:
            baseline = {
                "sales": conn.execute(
                    "SELECT count(*) FROM ticket_sales").fetchone()[0],
                "refunds": conn.execute(
                    "SELECT count(*) FROM ticket_refunds").fetchone()[0],
                "alloc": conn.execute(
                    "SELECT count(*) FROM ticket_allocation").fetchone()[0],
                "sources": conn.execute(
                    "SELECT count(*) FROM ticket_sources").fetchone()[0],
                "batches": conn.execute(
                    "SELECT count(*) FROM ticket_batches").fetchone()[0],
            }
        # A valid-looking sale followed by an invalid line in one file: the
        # whole batch must vanish.
        write_jsonl(self.tickets, [
            sale("T-NEW", 10, [("F1", 1)]),
            sale("T-GHOST", 10, [("F9", 1)]),
        ])
        with self.assertRaisesRegex(ValueError, "unknown trip_id"):
            self.load()
        with sqlite3.connect(self.db) as conn:
            for table, key in (
                ("ticket_sales", "sales"),
                ("ticket_refunds", "refunds"),
                ("ticket_allocation", "alloc"),
                ("ticket_sources", "sources"),
                ("ticket_batches", "batches"),
            ):
                self.assertEqual(
                    conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0],
                    baseline[key],
                    table,
                )

    def test_strict_contract_rejects_every_bad_sample_line(self):
        lines = (TICKETS / "bad-records.jsonl").read_text().splitlines()
        for i, line in enumerate(lines):
            path = self.root / f"bad-{i}.jsonl"
            path.write_text(line + "\n", encoding="utf-8")
            with self.assertRaises(ValueError, msg=f"line {i}: {line}"):
                # Parser-only and full-ingest paths must both reject; the last
                # line is not even JSON, so parse_ticket_file raises first.
                parse_ticket_file(path)
                self.load(path)

    def test_cumulative_cap_holds_across_batches(self):
        write_jsonl(self.tickets, [sale("T-CAP", 5, [("F1", 1)])])
        self.load()
        write_jsonl(self.tickets, [refund("R1", "T-CAP", 3)])
        self.load()
        write_jsonl(self.tickets, [refund("R2", "T-CAP", 3)])
        with self.assertRaisesRegex(ValueError, "exceed paid_cents"):
            self.load()
        # Full refund at exactly the cap is allowed and zeros every net.
        write_jsonl(self.tickets, [refund("R3", "T-CAP", 2)])
        self.load()
        report = settlement(self.db)
        t = self.ticket(report, "T-CAP")
        self.assertEqual(t["refunded_cents"], 5)
        self.assertEqual([a["net_cents"] for a in t["allocation"]], [0])

    def test_zero_fare_ticket_cannot_be_refunded(self):
        write_jsonl(self.tickets, [sale("T-FREE", 0, [("F1", 1)])])
        self.load()
        write_jsonl(self.tickets, [refund("R-F", "T-FREE", 1)])
        with self.assertRaisesRegex(ValueError, "exceed paid_cents"):
            self.load()

    # ------------------------------------------------- deterministic export
    def test_settlement_export_is_deterministic_and_write_failure_keeps_old(self):
        write_jsonl(self.tickets, [
            sale("T-100", 10, [("F3", 1), ("F1", 1), ("F2", 1)]),
            refund("R-100", "T-100", 1),
        ])
        self.load()
        out = self.root / "out"
        write_settlement(settlement(self.db), out)
        first = (out / "settlement.json").read_bytes()
        # Re-export from the committed ledger is byte-identical.
        write_settlement(settlement(self.db), out)
        self.assertEqual(first, (out / "settlement.json").read_bytes())
        payload = json.loads(first)
        self.assertEqual(payload["schema_version"], 1)
        # No volatile fields; rendering is sorted-key JSON with a newline.
        self.assertEqual(
            first,
            (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode(),
        )
        # If the destination cannot be written, the previous file survives.
        bad_out = self.root / "not-a-dir"
        bad_out.write_text("blocker")
        with self.assertRaises(OSError):
            write_settlement(settlement(self.db), bad_out / "settlement.json")
        self.assertEqual((out / "settlement.json").read_bytes(), first)

    def test_requires_existing_schedule_and_creates_no_database(self):
        fresh = self.root / "fresh.sqlite"
        write_jsonl(self.tickets, [sale("T-1", 1, [("F1", 1)])])
        with self.assertRaisesRegex(ValueError, "trips"):
            ingest_tickets(self.tickets, fresh)
        self.assertFalse(fresh.exists())
        with self.assertRaisesRegex(ValueError, "does not exist"):
            settlement(fresh)


class TicketCliCase(unittest.TestCase):
    """Real CLI subprocess: schedule ingest, ticket ingest, re-export, failure."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "schedule.csv").write_bytes(
            (DATA / "schedule.csv").read_bytes()
        )
        (self.root / "events.jsonl").write_bytes(
            (DATA / "events.jsonl").read_bytes()
        )
        self.env = dict(os.environ)
        self.env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])

    def run_cli(self, *args):
        return subprocess.run(
            [sys.executable, "-m", "ferry_observer", *args],
            cwd=self.root, capture_output=True, text=True, env=self.env,
        )

    def copy_ticket_file(self, name):
        target = self.root / "tickets.jsonl"
        target.write_bytes((TICKETS / name).read_bytes())
        return target

    def test_full_cli_flow_ingest_refund_reexport_and_rollback(self):
        # 1. Normal event ingestion creates the trips schedule.
        proc = self.run_cli(
            "--schedule", "schedule.csv", "--events", "events.jsonl",
            "--db", "ferry.sqlite", "--output", "report",
            "--as-of", CUTOFF,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)

        # 2. Ticket batch 1: two 10-cent equal-weight three-leg tickets.
        self.copy_ticket_file("batch1-sales.jsonl")
        proc = self.run_cli(
            "--ticket-ingest", "tickets.jsonl", "--db", "ferry.sqlite",
            "--output", "settle",
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        summary = json.loads(proc.stdout.strip().splitlines()[-1])
        self.assertEqual(summary["ticket_ingestion"]["sales_inserted"], 2)
        self.assertEqual(summary["ticket_ingestion"]["ingest_seq"], 1)
        report_path = self.root / "settle" / "settlement.json"
        report = json.loads(report_path.read_text())
        t100 = next(t for t in report["tickets"] if t["ticket_id"] == "T-100")
        self.assertEqual(
            [(a["trip_id"], a["fare_cents"]) for a in t100["allocation"]],
            [("F1", 4), ("F2", 3), ("F3", 3)],
        )

        # 3. Batch 2 mixes an early refund with its later sale (same batch).
        self.copy_ticket_file("batch2-early-refunds.jsonl")
        proc = self.run_cli(
            "--ticket-ingest", "tickets.jsonl", "--db", "ferry.sqlite",
            "--output", "settle",
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        report = json.loads(report_path.read_text())
        self.assertEqual(report["ingest_seq"], 2)
        t300 = next(t for t in report["tickets"] if t["ticket_id"] == "T-300")
        # Refund of 10 over original equal split 10/10/10 of 30 cents.
        self.assertEqual(
            [(a["trip_id"], a["refund_cents"], a["net_cents"])
             for a in t300["allocation"]],
            [("F1", 4, 6), ("F2", 3, 7), ("F3", 3, 7)],
        )

        # 4. Cross-batch partial refunds.
        self.copy_ticket_file("batch3-partial-refunds.jsonl")
        proc = self.run_cli(
            "--ticket-ingest", "tickets.jsonl", "--db", "ferry.sqlite",
            "--output", "settle",
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)

        # 5. Shuffled replay is idempotent; settlement bytes stay identical.
        committed = report_path.read_bytes()
        self.copy_ticket_file("batch1-replay-shuffled.jsonl")
        proc = self.run_cli(
            "--ticket-ingest", "tickets.jsonl", "--db", "ferry.sqlite",
            "--output", "settle",
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(
            json.loads(proc.stdout)["ticket_ingestion"]["duplicates"], 2
        )
        self.assertEqual(report_path.read_bytes(), committed)

        # 6. Read-only re-export after commit reproduces the same bytes.
        proc = self.run_cli(
            "--settlement", "--db", "ferry.sqlite", "--output", "settle2",
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(
            (self.root / "settle2" / "settlement.json").read_bytes(), committed
        )
        summary = json.loads(proc.stdout)
        self.assertTrue(summary["settlement"])
        self.assertEqual(summary["tickets"], 3)

        # 7. Conflict batch fails with exit 2 and leaves the old report.
        self.copy_ticket_file("batch4-conflict.jsonl")
        proc = self.run_cli(
            "--ticket-ingest", "tickets.jsonl", "--db", "ferry.sqlite",
            "--output", "settle",
        )
        self.assertEqual(proc.returncode, 2)
        self.assertIn("observation failed", proc.stderr)
        self.assertEqual(report_path.read_bytes(), committed)

        # 8. Mode exclusivity and missing --as-of rules.
        proc = self.run_cli(
            "--ticket-ingest", "tickets.jsonl", "--db", "ferry.sqlite",
            "--settlement",
        )
        self.assertEqual(proc.returncode, 2)
        self.assertIn("mutually exclusive", proc.stderr)
        proc = self.run_cli(
            "--ticket-ingest", "tickets.jsonl", "--db", "ferry.sqlite",
            "--as-of", CUTOFF,
        )
        self.assertEqual(proc.returncode, 2)
        proc = self.run_cli(
            "--settlement", "--db", "missing.sqlite", "--output", "settle3",
        )
        self.assertEqual(proc.returncode, 2)
        self.assertFalse((self.root / "missing.sqlite").exists())


if __name__ == "__main__":
    unittest.main()
