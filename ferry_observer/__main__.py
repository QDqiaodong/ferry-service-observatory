import argparse
import json
from pathlib import Path
from .analysis import performance, write_report
from .contracts import instant
from .storage import ingest


def main():
    parser = argparse.ArgumentParser(description="Observe synthetic ferry service from event exports")
    parser.add_argument("--schedule", type=Path, default=Path("data/schedule.csv"))
    parser.add_argument("--events", type=Path, default=Path("data/events.jsonl"))
    parser.add_argument("--db", type=Path, default=Path("run/ferry.sqlite"))
    parser.add_argument("--output", type=Path, default=Path("run/report"))
    parser.add_argument("--as-of", required=True, help="timezone-aware observation cutoff")
    args = parser.parse_args()
    try:
        instant(args.as_of)
        acquisition = ingest(args.schedule, args.events, args.db)
        report = performance(args.db, args.as_of)
        write_report(report, args.output)
    except (OSError, ValueError) as exc:
        parser.exit(2, f"observation failed: {exc}\n")
    print(json.dumps({"ingestion":acquisition,"routes":report["routes"]},sort_keys=True))


if __name__ == "__main__":
    main()
