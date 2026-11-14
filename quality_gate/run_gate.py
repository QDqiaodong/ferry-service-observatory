"""Full quality-gate runner CLI.

Generates the fixed-seed histories (standard library ``random.Random``),
replays each one through production and the independent oracle, and writes a
machine-readable summary plus, on any disagreement, full failure artifacts
(seed, every input, expected/actual diff, replay bundle).

    python3 -m quality_gate.run_gate --seeds 40 --out run/quality-gate
    python3 -m quality_gate.run_gate --save-bundle run/singleton.json --seed 20260101
"""
import argparse
import json
import sys
from pathlib import Path

from .generator import DEFAULT_SEEDS, generate_history
from .harness import GateFailure, run_history


def _write_bundle(history, path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    serializable = {
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
    path.write_text(
        json.dumps(serializable, indent=2, sort_keys=True, ensure_ascii=False)
        + "\n",
        encoding="utf-8",
    )


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Run the reproducible revision state-machine quality gate"
    )
    parser.add_argument(
        "--seeds",
        type=int,
        default=len(DEFAULT_SEEDS),
        help="number of fixed-seed histories to replay (default 40)",
    )
    parser.add_argument("--out", type=Path, default=Path("run/quality-gate"))
    parser.add_argument(
        "--save-bundle",
        type=Path,
        default=None,
        help="also save the first history as a self-contained replay bundle",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="explicit seed for --save-bundle (defaults to the first seed)",
    )
    args = parser.parse_args(argv)

    failures_dir = args.out / "failures"
    failures_dir.mkdir(parents=True, exist_ok=True)
    results = []
    status = "pass"

    if args.save_bundle is not None:
        seed = args.seed if args.seed is not None else DEFAULT_SEEDS[0]
        _write_bundle(generate_history(seed), args.save_bundle)

    for index in range(args.seeds):
        seed = DEFAULT_SEEDS[index % len(DEFAULT_SEEDS)]
        history = generate_history(seed)
        try:
            record = run_history(history, failures_dir=failures_dir)
            results.append({"seed": seed, "status": "pass", **record})
            print(f"[gate] seed {seed}: PASS")
        except GateFailure as exc:
            status = "fail"
            results.append(
                {"seed": seed, "status": "fail", "failure": str(exc)}
            )
            print(f"[gate] seed {seed}: FAIL -> {exc}", file=sys.stderr)

    summary = {
        "schema": "ferry-qg-summary/1",
        "status": status,
        "histories": len(results),
        "passed": sum(r["status"] == "pass" for r in results),
        "failed": sum(r["status"] == "fail" for r in results),
        "results": results,
    }
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({k: v for k, v in summary.items() if k != "results"}, sort_keys=True))
    return 0 if status == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())
