"""Batch-by-batch comparison harness between production and the oracle.

For every generated history the harness:

1. writes the fixed schedule and each rendered batch into an isolated
   throwaway directory;
2. feeds the same batch to the real ``storage.ingest`` and to the
   independent :class:`~quality_gate.oracle.RevisionOracle`;
3. compares accept/reject decisions, ingest counters, knowledge sequences and
   first-source ownership, then dumps *every* SQLite table and compares the
   full content against the oracle snapshot (a rejected batch must leave zero
   residue);
4. rebuilds reports over a grid of knowledge cutoffs ``K`` and event-time
   cutoffs ``T`` and compares the full report structures, and asserts that
   historical report bytes never change after later ingestion.

On failure it persists the seed, the complete input set and an
expected/actual diff for offline diagnosis and singleton replay.
"""
import hashlib
import json
import sqlite3
import tempfile
from pathlib import Path

from .generator import BATCH_COUNT
from .oracle import OracleRejection, RevisionOracle

# Event-time cutoffs used for the dual-cutoff grid (UTC).
TIME_CUTOFFS = [
    "2026-01-01T07:30:00Z",
    "2026-01-01T08:45:00Z",
    "2026-01-01T10:15:00Z",
    "2026-01-01T12:00:00Z",
    "2026-01-01T14:00:00Z",
]


class GateFailure(AssertionError):
    """A concrete expected/actual disagreement captured by the harness."""

    def __init__(self, context, expected, actual, detail=None):
        message = f"{context}: expected={expected!r} actual={actual!r}"
        if detail:
            message += f" | {detail}"
        super().__init__(message)
        self.context = context
        self.expected = expected
        self.actual = actual
        self.detail = detail


def _load_production():
    """Import the production package lazily so importing the gate stays free."""
    from ferry_observer.analysis import performance, write_report
    from ferry_observer.storage import ingest

    return ingest, performance, write_report


def dump_database(db_path):
    """Full ordered content of every observatory table (raw SQL only)."""
    snapshot = {}
    with sqlite3.connect(db_path) as db:
        names = [
            row[0]
            for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "ORDER BY name"
            )
        ]
        for name in names:
            cols = [
                row[1]
                for row in db.execute(f"PRAGMA table_info({name})")
            ]
            rows = db.execute(
                f"SELECT * FROM {name} ORDER BY {', '.join(cols)}"
            ).fetchall()
            snapshot[name] = rows
    return snapshot


def _write_bundle_files(workdir, history):
    (workdir / "schedule.csv").write_text(history["schedule_csv"])
    paths = []
    for index, batch in enumerate(history["batches"], 1):
        path = workdir / f"batch-{index:02d}.jsonl"
        payload = batch["payload"]
        if isinstance(payload, str):
            # Bundles loaded back from JSON carry text; re-encode exactly.
            payload = payload.encode("utf-8")
        path.write_bytes(payload)
        paths.append(path)
    return paths


def _check_snapshot(context, db_path, oracle):
    actual = dump_database(db_path)
    expected = oracle.snapshot()
    if set(actual) != set(expected):
        raise GateFailure(
            f"{context}: table set", sorted(expected), sorted(actual)
        )
    for name, expected_rows in expected.items():
        actual_rows = actual[name]
        if expected_rows != actual_rows:
            raise GateFailure(
                f"{context}: table {name}",
                expected_rows,
                actual_rows,
            )


def run_history(history, *, failures_dir=None, production=None):
    """Replay one full history and return a structured verification record.

    Raises :class:`GateFailure` on the first disagreement. When
    ``failures_dir`` is set, the seed, all inputs and the diff are persisted
    before the exception propagates.
    """
    if production is None:
        ingest, performance, write_report = _load_production()
    else:
        ingest, performance, write_report = production

    seed = history["seed"]
    record = {
        "seed": seed,
        "batches": [],
        "accepted_batches": 0,
        "rejected_batches": 0,
        "knowledge_sequences": [],
    }

    with tempfile.TemporaryDirectory(prefix=f"qg-{seed}-") as tmp:
        workdir = Path(tmp)
        schedule_path = workdir / "schedule.csv"
        db_path = workdir / "gate.sqlite"
        report_dir = workdir / "reports"
        batch_paths = _write_bundle_files(workdir, history)
        oracle = RevisionOracle(schedule_path)

        # Historical report bytes captured after each sequence; later
        # ingestion must never mutate them.
        frozen_bytes = {}
        sequence_seen = 0

        for index, (batch, batch_path) in enumerate(
            zip(history["batches"], batch_paths), 1
        ):
            context = f"seed {seed} batch {index} ({batch['label']})"
            before = dump_database(db_path) if db_path.exists() else None

            # ---- production side
            prod_error = None
            prod_result = None
            try:
                prod_result = ingest(
                    schedule_path, batch_path, db_path, event_format="v2"
                )
            except ValueError as exc:
                prod_error = str(exc)

            # ---- oracle side
            oracle_error = None
            oracle_result = None
            try:
                oracle_result = oracle.ingest(batch_path)
            except OracleRejection as exc:
                oracle_error = exc.reason

            accepted = oracle_error is None
            if (prod_error is None) != accepted:
                _persist_failure(
                    failures_dir, history, index,
                    GateFailure(
                        f"{context}: decision",
                        "reject" if not accepted else "accept",
                        "accept" if prod_error is None else "reject",
                        {"oracle": oracle_error, "production": prod_error},
                    ),
                )
                raise GateFailure(
                    f"{context}: decision",
                    "reject" if not accepted else "accept",
                    "accept" if prod_error is None else "reject",
                    {"oracle": oracle_error, "production": prod_error},
                )

            if accepted:
                for key in (
                    "input_rows", "inserted", "duplicates", "total_events",
                    "source_sha256", "knowledge_seq", "new_revisions",
                    "active_events",
                ):
                    if prod_result[key] != oracle_result[key]:
                        failure = GateFailure(
                            f"{context}: result field {key}",
                            oracle_result[key],
                            prod_result[key],
                        )
                        _persist_failure(failures_dir, history, index, failure)
                        raise failure
                record["accepted_batches"] += 1
                if oracle_result["inserted"]:
                    sequence_seen = oracle_result["knowledge_seq"]
                    record["knowledge_sequences"].append(sequence_seen)
            else:
                record["rejected_batches"] += 1
                # Rollback check: the database must be byte-identical to its
                # pre-batch state (or still absent entirely).
                after = dump_database(db_path) if db_path.exists() else None
                if before != after:
                    failure = GateFailure(
                        f"{context}: rejected batch left residue",
                        before,
                        after,
                    )
                    _persist_failure(failures_dir, history, index, failure)
                    raise failure

            # Full-table comparison after every single batch.
            try:
                _check_snapshot(context, db_path, oracle)
            except GateFailure as failure:
                _persist_failure(failures_dir, history, index, failure)
                raise

            # First-source ownership check at the revision-row level is part
            # of the snapshot comparison above; also verify no revision ever
            # changes ownership after redelivery.
            if accepted and oracle_result["inserted"]:
                expected_owner = {
                    (event_id, revision): rec["source_sha256"]
                    for event_id, versions in oracle.ledger.items()
                    for revision, rec in versions.items()
                }
                with sqlite3.connect(db_path) as db:
                    actual_owner = {
                        (event_id, revision): source
                        for event_id, revision, source in db.execute(
                            "SELECT event_id,revision,source_sha256 FROM revisions"
                        )
                    }
                if expected_owner != actual_owner:
                    failure = GateFailure(
                        f"{context}: first-source ownership",
                        expected_owner,
                        actual_owner,
                    )
                    _persist_failure(failures_dir, history, index, failure)
                    raise failure

            record["batches"].append(
                {
                    "index": index,
                    "label": batch["label"],
                    "expected": batch["expected"],
                    "decision": "accept" if accepted else "reject",
                    "label_matches": batch["expected"]
                    == ("accept" if accepted else "reject"),
                    "summary": oracle_result,
                }
            )

            # Dual-cutoff grid at every batch boundary.
            for cutoff in TIME_CUTOFFS:
                for known_at in range(0, sequence_seen + 1):
                    ctx = (
                        f"{context}: report K={known_at} T={cutoff}"
                    )
                    prod_report = performance(db_path, cutoff, known_at)
                    oracle_report = oracle.performance(cutoff, known_at)
                    if prod_report != oracle_report:
                        failure = GateFailure(
                            ctx,
                            oracle_report,
                            prod_report,
                        )
                        _persist_failure(failures_dir, history, index, failure)
                        raise failure

            # Freeze historical report bytes; later batches must not change
            # them. We render the same (K, T) pairs production emits.
            for cutoff in TIME_CUTOFFS:
                for known_at in range(0, sequence_seen + 1):
                    key = (known_at, cutoff)
                    if key in frozen_bytes:
                        continue
                    out = report_dir / f"k{known_at}-t{cutoff.replace(':', '')}"
                    write_report(
                        performance(db_path, cutoff, known_at), out
                    )
                    frozen_bytes[key] = {
                        "json": (out / "performance.json").read_bytes(),
                        "csv": (out / "trips.csv").read_bytes(),
                    }

        # Final byte-stability sweep: every frozen historical report must
        # rebuild identically after all subsequent ingestion.
        for (known_at, cutoff), original in frozen_bytes.items():
            out = workdir / f"rebuild-k{known_at}-t{known_at}"
            write_report(
                performance(db_path, cutoff, known_at), out
            )
            if (out / "performance.json").read_bytes() != original["json"]:
                raise GateFailure(
                    f"seed {seed}: historical JSON changed for K={known_at} T={cutoff}",
                    original["json"],
                    (out / "performance.json").read_bytes(),
                )
            if (out / "trips.csv").read_bytes() != original["csv"]:
                raise GateFailure(
                    f"seed {seed}: historical CSV changed for K={known_at} T={cutoff}",
                    original["csv"],
                    (out / "trips.csv").read_bytes(),
                )

    if record["accepted_batches"] + record["rejected_batches"] != BATCH_COUNT:
        raise GateFailure(
            f"seed {seed}: batch count",
            BATCH_COUNT,
            record["accepted_batches"] + record["rejected_batches"],
        )
    record["frozen_report_pairs"] = len(frozen_bytes)
    return record


def _persist_failure(failures_dir, history, batch_index, failure):
    """Save seed, full inputs and the expected/actual diff on failure."""
    if failures_dir is None:
        return
    root = Path(failures_dir) / f"seed-{history['seed']}-batch-{batch_index:02d}"
    root.mkdir(parents=True, exist_ok=True)
    (root / "seed.txt").write_text(str(history["seed"]))
    (root / "schedule.csv").write_text(history["schedule_csv"])
    for index, batch in enumerate(history["batches"], 1):
        path = root / f"batch-{index:02d}.jsonl"
        path.write_bytes(batch["payload"])
    diff = {
        "schema": "ferry-qg-failure/1",
        "seed": history["seed"],
        "failed_batch_index": batch_index,
        "failed_batch_label": history["batches"][batch_index - 1]["label"],
        "context": failure.context,
        "expected": _jsonable(failure.expected),
        "actual": _jsonable(failure.actual),
        "detail": _jsonable(failure.detail),
        "replay_hint": (
            "python3 -m quality_gate.replay --bundle <saved-bundle.json> "
            "--failures-dir <dir>"
        ),
    }
    (root / "diff.json").write_text(
        json.dumps(diff, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    )
    bundle_path = root / "history-bundle.json"
    serializable_history = {
        key: (
            [
                {
                    inner: (
                        value.decode("utf-8")
                        if isinstance(value, bytes)
                        else value
                    )
                    for inner, value in batch.items()
                }
                for batch in history["batches"]
            ]
            if key == "batches"
            else value
        )
        for key, value in history.items()
    }
    bundle_path.write_text(
        json.dumps(
            serializable_history, indent=2, sort_keys=True, ensure_ascii=False
        )
        + "\n"
    )


def _jsonable(value):
    """Best-effort conversion of SQLite/JSON values for a saved diff."""
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, (bytes, bytearray)):
        return hashlib.sha256(bytes(value)).hexdigest()
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(item) for item in value]
    if isinstance(value, Exception):
        return repr(value)
    return repr(value)
