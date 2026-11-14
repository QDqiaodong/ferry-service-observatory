"""Passenger ticket clearing: strict JSONL ingestion, integer apportionment.

Ticket sales and refunds arrive as JSONL files with two record types:

* ``type="sale"``: ``ticket_id, paid_cents, legs`` where every leg names an
  existing ``trip_id`` with a positive integer ``weight``.
* ``type="refund"``: ``refund_id, ticket_id, cents`` with a strictly positive
  integer amount. Refunds may arrive before their ticket's sale; the whole
  candidate batch is merged with the committed ledger before the cumulative
  refund cap (never above the paid amount) is enforced.

Money is integer cents end to end. A ticket's fare is split across its legs by
the largest-remainder (Hamilton) method using the leg weights, with fractional
remainder ties resolved by lexicographic ``trip_id``. Cumulative refunds are
re-apportioned the same way using the *original* per-leg fare allocation as
weights; per-leg net revenue is the integer difference of the two. No floating
point arithmetic is used anywhere.

Sales and refunds keep independent identifier namespaces. Replaying a file with
the same identifier and identical normalized content is an idempotent no-op;
different content for one identifier rejects the whole batch. Tickets, legs,
refunds, apportionment facts and the source-file digest commit in one serializable
transaction, so any failure rolls the entire batch back.
"""
import hashlib
import json
import os
import sqlite3
import tempfile
from pathlib import Path

TICKETING_SCHEMA_VERSION = 1
LOCK_WAIT_SECONDS = 5

SALE_KEYS = {"type", "ticket_id", "paid_cents", "legs"}
REFUND_KEYS = {"type", "refund_id", "ticket_id", "cents"}
LEG_KEYS = {"trip_id", "weight"}

SCHEMA = [
    """CREATE TABLE IF NOT EXISTS ticket_sales (
 ticket_id TEXT PRIMARY KEY,
 paid_cents INTEGER NOT NULL CHECK (paid_cents >= 0),
 canonical_json TEXT NOT NULL,
 source_sha256 TEXT NOT NULL,
 ingest_seq INTEGER NOT NULL REFERENCES ticket_batches(ingest_seq))""",
    """CREATE TABLE IF NOT EXISTS ticket_sale_legs (
 ticket_id TEXT NOT NULL REFERENCES ticket_sales(ticket_id),
 leg_order INTEGER NOT NULL CHECK (leg_order >= 1),
 trip_id TEXT NOT NULL REFERENCES trips(trip_id),
 weight INTEGER NOT NULL CHECK (weight >= 1),
 PRIMARY KEY (ticket_id, leg_order),
 UNIQUE (ticket_id, trip_id))""",
    """CREATE TABLE IF NOT EXISTS ticket_refunds (
 refund_id TEXT PRIMARY KEY,
 ticket_id TEXT NOT NULL,
 cents INTEGER NOT NULL CHECK (cents > 0),
 canonical_json TEXT NOT NULL,
 source_sha256 TEXT NOT NULL,
 ingest_seq INTEGER NOT NULL REFERENCES ticket_batches(ingest_seq))""",
    """CREATE TABLE IF NOT EXISTS ticket_allocation (
 ticket_id TEXT NOT NULL REFERENCES ticket_sales(ticket_id),
 trip_id TEXT NOT NULL REFERENCES trips(trip_id),
 fare_cents INTEGER NOT NULL CHECK (fare_cents >= 0),
 refund_cents INTEGER NOT NULL CHECK (refund_cents >= 0),
 net_cents INTEGER NOT NULL,
 PRIMARY KEY (ticket_id, trip_id))""",
    """CREATE TABLE IF NOT EXISTS ticket_batches (
 ingest_seq INTEGER PRIMARY KEY CHECK (ingest_seq >= 1),
 source_sha256 TEXT NOT NULL,
 sales_inserted INTEGER NOT NULL,
 refunds_inserted INTEGER NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS ticket_sources (
 sha256 TEXT PRIMARY KEY, row_count INTEGER NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS ticket_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)""",
]


# --------------------------------------------------------------------- parsing
def _reject_constant(value):
    raise ValueError(f"invalid JSON constant {value}")


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate key {key!r}")
        result[key] = value
    return result


def _non_negative_int(value, label):
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{label} must be a non-negative integer (booleans rejected)")
    return value


def _positive_int(value, label):
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{label} must be a positive integer (booleans rejected)")
    return value


def _identifier(value, label):
    if not isinstance(value, str) or not value:
        raise ValueError(f"invalid {label}: must be a nonempty string")
    return value


def _sale_canonical(ticket_id, paid_cents, legs):
    """Normalized sale payload; legs sort by trip_id so leg order is irrelevant."""
    return json.dumps(
        {
            "type": "sale",
            "ticket_id": ticket_id,
            "paid_cents": paid_cents,
            "legs": [
                {"trip_id": trip_id, "weight": weight}
                for trip_id, weight in sorted(legs)
            ],
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def _refund_canonical(refund_id, ticket_id, cents):
    return json.dumps(
        {
            "type": "refund",
            "refund_id": refund_id,
            "ticket_id": ticket_id,
            "cents": cents,
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def parse_ticket_file(path):
    """Parse and strictly validate a ticket JSONL file.

    Returns a list of normalized records, each tagged with its source line.
    Malformed lines raise ``ValueError`` naming the line number.
    """
    records = []
    with open(path, encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, 1):
            try:
                row = json.loads(
                    line,
                    object_pairs_hook=_unique_object,
                    parse_constant=_reject_constant,
                )
                if not isinstance(row, dict):
                    raise ValueError("record must be a JSON object")
                record_type = row.get("type")
                if record_type == "sale":
                    if set(row) != SALE_KEYS:
                        raise ValueError(
                            f"sale record requires exactly {sorted(SALE_KEYS)}"
                        )
                    ticket_id = _identifier(row["ticket_id"], "ticket_id")
                    paid_cents = _non_negative_int(row["paid_cents"], "paid_cents")
                    legs = row["legs"]
                    if not isinstance(legs, list) or not legs:
                        raise ValueError("legs must be a non-empty list")
                    normalized_legs = []
                    seen_trips = set()
                    for leg in legs:
                        if not isinstance(leg, dict) or set(leg) != LEG_KEYS:
                            raise ValueError(
                                f"each leg requires exactly {sorted(LEG_KEYS)}"
                            )
                        trip_id = _identifier(leg["trip_id"], "leg trip_id")
                        weight = _positive_int(leg["weight"], "leg weight")
                        if trip_id in seen_trips:
                            raise ValueError(
                                f"duplicate leg trip_id {trip_id} on ticket "
                                f"{ticket_id}"
                            )
                        seen_trips.add(trip_id)
                        normalized_legs.append((trip_id, weight))
                    records.append(
                        {
                            "line": line_number,
                            "type": "sale",
                            "ticket_id": ticket_id,
                            "paid_cents": paid_cents,
                            "legs": normalized_legs,
                            "canonical": _sale_canonical(
                                ticket_id, paid_cents, normalized_legs
                            ),
                        }
                    )
                elif record_type == "refund":
                    if set(row) != REFUND_KEYS:
                        raise ValueError(
                            f"refund record requires exactly {sorted(REFUND_KEYS)}"
                        )
                    refund_id = _identifier(row["refund_id"], "refund_id")
                    ticket_id = _identifier(row["ticket_id"], "ticket_id")
                    cents = _positive_int(row["cents"], "cents")
                    records.append(
                        {
                            "line": line_number,
                            "type": "refund",
                            "refund_id": refund_id,
                            "ticket_id": ticket_id,
                            "cents": cents,
                            "canonical": _refund_canonical(
                                refund_id, ticket_id, cents
                            ),
                        }
                    )
                else:
                    raise ValueError("type must be 'sale' or 'refund'")
            except (ValueError, TypeError, AttributeError) as exc:
                raise ValueError(f"ticket line {line_number}: {exc}") from exc
    return records


# ------------------------------------------------------------- apportionment
def largest_remainder_basis(total_cents, items):
    """Largest-remainder apportionment of ``total_cents`` over weighted items.

    ``items`` is a sequence of ``(key, weight)`` pairs with non-negative integer
    weights; ``total_cents`` is a non-negative integer. Returns one basis dict
    per key with the integer floor share, the remainder numerator over the
    shared weight denominator, whether the key received one of the leftover
    units, and the final allocated amount. Remainder ties go to the
    lexicographically smallest key. Everything is integer arithmetic.
    """
    if isinstance(total_cents, bool) or not isinstance(total_cents, int) or total_cents < 0:
        raise ValueError("total_cents must be a non-negative integer (booleans rejected)")
    if not items:
        raise ValueError("at least one weighted item is required")
    weight_sum = 0
    for key, weight in items:
        if isinstance(weight, bool) or not isinstance(weight, int) or weight < 0:
            raise ValueError(f"weight for {key!r} must be a non-negative integer")
        weight_sum += weight
    if weight_sum == 0:
        raise ValueError("cannot apportion a positive total against all-zero weights")

    basis = []
    for key, weight in items:
        scaled = total_cents * weight
        base = scaled // weight_sum
        basis.append(
            {
                "key": key,
                "weight": weight,
                "base_cents": base,
                "remainder_numerator": scaled % weight_sum,
                "extra_unit": 0,
                "allocated_cents": base,
            }
        )
    allocated = sum(entry["base_cents"] for entry in basis)
    leftover = total_cents - allocated
    # Highest remainder first; exact ties resolve by lexicographic key. Zero
    # weight keys carry remainder 0 and only win when leftover still remains,
    # which cannot happen while a positive-weight key exists.
    ranked = sorted(
        basis,
        key=lambda entry: (-entry["remainder_numerator"], entry["key"]),
    )
    for entry in ranked[:leftover]:
        entry["extra_unit"] = 1
        entry["allocated_cents"] += 1
    assert sum(entry["allocated_cents"] for entry in basis) == total_cents
    return sorted(basis, key=lambda entry: entry["key"])


# ----------------------------------------------------------------- storage
def _connect(database):
    db = sqlite3.connect(database, timeout=LOCK_WAIT_SECONDS)
    db.execute("PRAGMA foreign_keys=ON")
    db.execute(f"PRAGMA busy_timeout={LOCK_WAIT_SECONDS * 1000}")
    return db


def _ensure_trips(db):
    present = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='trips'"
    ).fetchone()
    if not present:
        raise ValueError(
            "database has no trips table; ingest a schedule before ticketing"
        )
    trips = {row[0] for row in db.execute("SELECT trip_id FROM trips")}
    if not trips:
        raise ValueError("schedule has no trips; ticketing requires an existing schedule")
    return trips


def _recompute_allocation(db, ticket_id):
    """Recompute fare/refund/net facts for one ticket from the committed rows."""
    sale = db.execute(
        "SELECT paid_cents FROM ticket_sales WHERE ticket_id=?",
        (ticket_id,),
    ).fetchone()
    if sale is None:
        # Refund arrived before its sale: nothing apportionable yet.
        return
    paid_cents = sale[0]
    legs = db.execute(
        "SELECT trip_id, weight FROM ticket_sale_legs WHERE ticket_id=? "
        "ORDER BY leg_order",
        (ticket_id,),
    ).fetchall()
    fare_basis = largest_remainder_basis(
        paid_cents, [(trip_id, weight) for trip_id, weight in legs]
    )
    fare = {entry["key"]: entry["allocated_cents"] for entry in fare_basis}

    cumulative = db.execute(
        "SELECT coalesce(sum(cents),0) FROM ticket_refunds WHERE ticket_id=?",
        (ticket_id,),
    ).fetchone()[0]
    refunds = {trip_id: 0 for trip_id, _ in legs}
    if cumulative:
        refund_basis = largest_remainder_basis(
            cumulative, [(trip_id, fare[trip_id]) for trip_id, _ in legs]
        )
        refunds = {entry["key"]: entry["allocated_cents"] for entry in refund_basis}

    db.execute("DELETE FROM ticket_allocation WHERE ticket_id=?", (ticket_id,))
    db.executemany(
        "INSERT INTO ticket_allocation(ticket_id,trip_id,fare_cents,"
        "refund_cents,net_cents) VALUES (?,?,?,?,?)",
        [
            (
                ticket_id,
                trip_id,
                fare[trip_id],
                refunds[trip_id],
                fare[trip_id] - refunds[trip_id],
            )
            for trip_id, _ in legs
        ],
    )


def ingest_tickets(ticket_path, database):
    """Acquire one ticket JSONL file inside a single serializable transaction.

    The database must already hold the immutable ``trips`` schedule. All
    validation, including same-batch merges and the cumulative refund cap, runs
    before any commit; any failure rolls the full batch back and consumes no
    ingest sequence.
    """
    ticket_path = Path(ticket_path)
    records = parse_ticket_file(ticket_path)
    digest = hashlib.sha256(ticket_path.read_bytes()).hexdigest()
    db_path = Path(database)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    # sqlite3.connect creates an empty file; remember that so a rejected
    # database (no schedule yet) can leave no artifact behind on rollback.
    created_empty = not db_path.exists() or db_path.stat().st_size == 0

    db = _connect(database)
    try:
        try:
            db.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError as exc:
            raise ValueError(
                f"database lock not acquired within {LOCK_WAIT_SECONDS}s: {exc}"
            ) from exc
        try:
            trips = _ensure_trips(db)
            for statement in SCHEMA:
                db.execute(statement)
            db.execute(
                "INSERT OR IGNORE INTO ticket_meta(key,value) VALUES (?,?)",
                ("schema_version", str(TICKETING_SCHEMA_VERSION)),
            )

            existing_sales = {
                row[0]: {"paid_cents": row[1], "canonical": row[2]}
                for row in db.execute(
                    "SELECT ticket_id,paid_cents,canonical_json FROM ticket_sales"
                )
            }
            existing_refunds = {
                row[0]: {"ticket_id": row[1], "cents": row[2], "canonical": row[3]}
                for row in db.execute(
                    "SELECT refund_id,ticket_id,cents,canonical_json FROM ticket_refunds"
                )
            }

            # Pass 1: in-file identity conflicts (identical repeats are allowed).
            file_sales, file_refunds = {}, {}
            for rec in records:
                if rec["type"] == "sale":
                    present = file_sales.get(rec["ticket_id"])
                    if present is not None:
                        if present["canonical"] != rec["canonical"]:
                            raise ValueError(
                                f"ticket line {rec['line']}: conflicting sale "
                                f"records for ticket_id {rec['ticket_id']} in "
                                "the same batch"
                            )
                        continue
                    file_sales[rec["ticket_id"]] = rec
                else:
                    present = file_refunds.get(rec["refund_id"])
                    if present is not None:
                        if present["canonical"] != rec["canonical"]:
                            raise ValueError(
                                f"ticket line {rec['line']}: conflicting refund "
                                f"records for refund_id {rec['refund_id']} in "
                                "the same batch"
                            )
                        continue
                    file_refunds[rec["refund_id"]] = rec

            # Pass 2: ledger conflict check and duplicate accounting. Every
            # input row is classified; identical in-file repeats (pass 1) and
            # ledger replays both count as duplicates and never insert twice.
            duplicates = 0
            new_sales, new_refunds = [], []
            seen_sale_ids, seen_refund_ids = set(), set()
            for rec in records:
                if rec["type"] == "sale":
                    prior = existing_sales.get(rec["ticket_id"])
                    if prior is not None:
                        if prior["canonical"] != rec["canonical"]:
                            raise ValueError(
                                f"ticket line {rec['line']}: sale conflict for "
                                f"ticket_id {rec['ticket_id']}; that ticket is "
                                "already accepted with different normalized content"
                            )
                        duplicates += 1
                        continue
                    if rec["ticket_id"] in seen_sale_ids:
                        duplicates += 1
                        continue
                    seen_sale_ids.add(rec["ticket_id"])
                    new_sales.append(rec)
                else:
                    prior = existing_refunds.get(rec["refund_id"])
                    if prior is not None:
                        if prior["canonical"] != rec["canonical"]:
                            raise ValueError(
                                f"ticket line {rec['line']}: refund conflict for "
                                f"refund_id {rec['refund_id']}; that refund is "
                                "already accepted with different normalized content"
                            )
                        duplicates += 1
                        continue
                    if rec["refund_id"] in seen_refund_ids:
                        duplicates += 1
                        continue
                    seen_refund_ids.add(rec["refund_id"])
                    new_refunds.append(rec)

            # Referential and structural checks against the merged state.
            sale_by_ticket = dict(existing_sales)
            for rec in new_sales:
                unknown = sorted({trip_id for trip_id, _ in rec["legs"]} - trips)
                if unknown:
                    raise ValueError(
                        f"ticket line {rec['line']}: unknown trip_id {unknown}"
                    )
                sale_by_ticket[rec["ticket_id"]] = {
                    "paid_cents": rec["paid_cents"],
                    "canonical": rec["canonical"],
                }

            # Cumulative refunds: committed ledger plus every new refund in this
            # batch, so refunds may precede sales as long as the merged total
            # stays within the ticket's paid amount once that amount is known.
            refund_totals = {}
            for prior in existing_refunds.values():
                refund_totals[prior["ticket_id"]] = (
                    refund_totals.get(prior["ticket_id"], 0) + prior["cents"]
                )
            for rec in new_refunds:
                refund_totals[rec["ticket_id"]] = (
                    refund_totals.get(rec["ticket_id"], 0) + rec["cents"]
                )
            for ticket_id in sorted(refund_totals):
                sale = sale_by_ticket.get(ticket_id)
                if sale is None:
                    continue  # refund before sale; cap is checked when the sale lands
                if refund_totals[ticket_id] > sale["paid_cents"]:
                    raise ValueError(
                        f"cumulative refunds {refund_totals[ticket_id]} for ticket "
                        f"{ticket_id} exceed paid_cents {sale['paid_cents']}"
                    )

            next_seq_row = db.execute(
                "SELECT max(ingest_seq) FROM ticket_batches"
            ).fetchone()
            next_seq = (next_seq_row[0] or 0) + 1
            has_new = bool(new_sales or new_refunds)
            if has_new:
                db.execute(
                    "INSERT INTO ticket_batches(ingest_seq,source_sha256,"
                    "sales_inserted,refunds_inserted) VALUES (?,?,?,?)",
                    (next_seq, digest, len(new_sales), len(new_refunds)),
                )
            for rec in new_sales:
                db.execute(
                    "INSERT INTO ticket_sales(ticket_id,paid_cents,"
                    "canonical_json,source_sha256,ingest_seq) VALUES (?,?,?,?,?)",
                    (
                        rec["ticket_id"], rec["paid_cents"], rec["canonical"],
                        digest, next_seq,
                    ),
                )
                db.executemany(
                    "INSERT INTO ticket_sale_legs(ticket_id,leg_order,trip_id,"
                    "weight) VALUES (?,?,?,?)",
                    [
                        (rec["ticket_id"], order, trip_id, weight)
                        for order, (trip_id, weight) in enumerate(
                            sorted(rec["legs"]), start=1
                        )
                    ],
                )
            for rec in new_refunds:
                db.execute(
                    "INSERT INTO ticket_refunds(refund_id,ticket_id,cents,"
                    "canonical_json,source_sha256,ingest_seq) VALUES (?,?,?,?,?,?)",
                    (
                        rec["refund_id"], rec["ticket_id"], rec["cents"],
                        rec["canonical"], digest, next_seq,
                    ),
                )

            touched = {rec["ticket_id"] for rec in new_sales}
            touched.update(rec["ticket_id"] for rec in new_refunds)
            for ticket_id in sorted(touched):
                _recompute_allocation(db, ticket_id)

            db.execute(
                "INSERT OR IGNORE INTO ticket_sources(sha256,row_count) VALUES (?,?)",
                (digest, len(records)),
            )
            ingest_seq = db.execute(
                "SELECT max(ingest_seq) FROM ticket_batches"
            ).fetchone()[0] or 0
            db.commit()
        except Exception:
            db.rollback()
            raise
    finally:
        db.close()
        if created_empty:
            # No schedule existed: nothing was created on purpose, so remove
            # the zero-byte file sqlite made rather than leave a broken DB.
            try:
                if db_path.exists() and db_path.stat().st_size == 0:
                    db_path.unlink()
            except OSError:
                pass

    return {
        "input_rows": len(records),
        "sales_inserted": len(new_sales),
        "refunds_inserted": len(new_refunds),
        "duplicates": duplicates,
        "ingest_seq": ingest_seq,
        "source_sha256": digest,
    }


# ------------------------------------------------------------------ report
def _connect_read_only(database):
    path = Path(database)
    if not path.exists():
        raise ValueError(f"database does not exist: {database}")
    uri = f"file:{path.resolve()}?mode=ro"
    db = sqlite3.connect(uri, uri=True)
    db.execute("PRAGMA query_only=ON")
    if not db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='trips'"
    ).fetchone():
        db.close()
        raise ValueError("database has no trips table; ingest a schedule first")
    return db


def _table_exists(db, name):
    return db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone() is not None


def settlement(database):
    """Build the deterministic clearing report from one read-only snapshot.

    The report lists every accepted ticket with the exact largest-remainder
    basis of its fare and cumulative refund apportionment, plus per-trip
    revenue/refund/net rollups. Refunds whose sale has not arrived yet are
    reported as ``pending_refunds`` so no committed fact is hidden.
    """
    db = _connect_read_only(database)
    try:
        db.execute("BEGIN")
        if not _table_exists(db, "ticket_sales"):
            ingest_seq = 0
            tickets = []
            pending = []
            rollups = {
                row[0]: {"fare_cents": 0, "refund_cents": 0, "net_cents": 0}
                for row in db.execute("SELECT trip_id FROM trips ORDER BY trip_id")
            }
        else:
            ingest_seq = db.execute(
                "SELECT max(ingest_seq) FROM ticket_batches"
            ).fetchone()[0] or 0
            tickets = []
            rollups = {
                row[0]: {"fare_cents": 0, "refund_cents": 0, "net_cents": 0}
                for row in db.execute("SELECT trip_id FROM trips ORDER BY trip_id")
            }
            sale_rows = db.execute(
                "SELECT ticket_id,paid_cents FROM ticket_sales ORDER BY ticket_id"
            ).fetchall()
            for ticket_id, paid_cents in sale_rows:
                legs = db.execute(
                    "SELECT trip_id,weight FROM ticket_sale_legs "
                    "WHERE ticket_id=? ORDER BY leg_order",
                    (ticket_id,),
                ).fetchall()
                fare_basis = largest_remainder_basis(
                    paid_cents, list(legs)
                )
                allocation_rows = db.execute(
                    "SELECT trip_id,fare_cents,refund_cents,net_cents "
                    "FROM ticket_allocation WHERE ticket_id=? ORDER BY trip_id",
                    (ticket_id,),
                ).fetchall()
                refunded = sum(row[2] for row in allocation_rows)
                refunds = [
                    {"refund_id": refund_id, "cents": cents}
                    for refund_id, cents in db.execute(
                        "SELECT refund_id,cents FROM ticket_refunds "
                        "WHERE ticket_id=? ORDER BY refund_id",
                        (ticket_id,),
                    )
                ]
                entry = {
                    "ticket_id": ticket_id,
                    "paid_cents": paid_cents,
                    "refunded_cents": refunded,
                    "legs": [
                        {"trip_id": trip_id, "weight": weight}
                        for trip_id, weight in legs
                    ],
                    "allocation_basis": {
                        "method": "largest_remainder",
                        "total_cents": paid_cents,
                        "weight_sum": sum(weight for _, weight in legs),
                        "legs": [
                            {
                                "trip_id": item["key"],
                                "weight": item["weight"],
                                "base_cents": item["base_cents"],
                                "remainder_numerator": item["remainder_numerator"],
                                "extra_unit": item["extra_unit"],
                                "allocated_cents": item["allocated_cents"],
                            }
                            for item in fare_basis
                        ],
                    },
                    "allocation": [
                        {
                            "trip_id": trip_id,
                            "fare_cents": fare_cents,
                            "refund_cents": refund_cents,
                            "net_cents": net_cents,
                        }
                        for trip_id, fare_cents, refund_cents, net_cents
                        in allocation_rows
                    ],
                    "refunds": refunds,
                }
                if refunded:
                    fare_weights = {
                        trip_id: fare_cents
                        for trip_id, fare_cents, _, _ in allocation_rows
                    }
                    refund_basis = largest_remainder_basis(
                        refunded,
                        [(trip_id, fare_weights[trip_id]) for trip_id, _ in legs],
                    )
                    entry["refund_basis"] = {
                        "method": "largest_remainder",
                        "total_cents": refunded,
                        "weight_sum": paid_cents,
                        "legs": [
                            {
                                "trip_id": item["key"],
                                "weight_cents": item["weight"],
                                "base_cents": item["base_cents"],
                                "remainder_numerator": item["remainder_numerator"],
                                "extra_unit": item["extra_unit"],
                                "allocated_cents": item["allocated_cents"],
                            }
                            for item in refund_basis
                        ],
                    }
                tickets.append(entry)
                for trip_id, fare_cents, refund_cents, net_cents in allocation_rows:
                    rollup = rollups[trip_id]
                    rollup["fare_cents"] += fare_cents
                    rollup["refund_cents"] += refund_cents
                    rollup["net_cents"] += net_cents

            pending = [
                {
                    "refund_id": refund_id,
                    "ticket_id": ticket_id,
                    "cents": cents,
                }
                for refund_id, ticket_id, cents in db.execute(
                    "SELECT r.refund_id,r.ticket_id,r.cents FROM ticket_refunds r "
                    "LEFT JOIN ticket_sales s ON s.ticket_id=r.ticket_id "
                    "WHERE s.ticket_id IS NULL ORDER BY r.refund_id"
                )
            ]
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

    return {
        "schema_version": TICKETING_SCHEMA_VERSION,
        "ingest_seq": ingest_seq,
        "tickets": tickets,
        "pending_refunds": pending,
        "trips": [
            {
                "trip_id": trip_id,
                "fare_cents": totals["fare_cents"],
                "refund_cents": totals["refund_cents"],
                "net_cents": totals["net_cents"],
            }
            for trip_id, totals in sorted(rollups.items())
        ],
    }


def write_settlement(report, output):
    """Atomically replace ``settlement.json`` inside ``output``.

    Deterministic rendering (sorted keys, sorted lists, no volatile fields),
    temp file plus fsync and ``os.replace``. A failure before the rename leaves
    any previous settlement file byte-identical.
    """
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    payload = (json.dumps(report, indent=2, sort_keys=True) + "\n").encode("utf-8")
    fd, temporary = tempfile.mkstemp(
        dir=output, prefix=".settlement-", suffix=".tmp"
    )
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, output / "settlement.json")
    except BaseException:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise
