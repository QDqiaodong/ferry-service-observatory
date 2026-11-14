"""Offline singleton replay CLI, independent of the random generator.

A history bundle is plain JSON (schedule + every rendered batch + expected
labels), so replaying it requires no RNG and no generated state:

    python3 -m quality_gate.replay --bundle path/to/history-bundle.json

Exit code is zero only when the production package and the independent oracle
agree on every batch, every table and every dual-cutoff report.
"""
import argparse
import json
import sys
from pathlib import Path

from .harness import GateFailure, run_history


def load_bundle(path):
    with open(path, encoding="utf-8") as stream:
        bundle = json.load(stream)
    required = {"schema", "seed", "schedule_csv", "batches"}
    if not isinstance(bundle, dict) or not required <= set(bundle):
        raise ValueError("not a ferry-qg history bundle")
    return bundle


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Replay one saved revision-history bundle without RNG"
    )
    parser.add_argument("--bundle", required=True, type=Path)
    parser.add_argument(
        "--failures-dir",
        type=Path,
        default=None,
        help="directory for seed/input/diff artifacts on mismatch",
    )
    args = parser.parse_args(argv)

    bundle = load_bundle(args.bundle)
    try:
        record = run_history(bundle, failures_dir=args.failures_dir)
    except GateFailure as exc:
        print(json.dumps({"status": "fail", "failure": str(exc)}, ensure_ascii=False))
        return 1
    print(json.dumps({"status": "pass", **record}, sort_keys=True, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
