import argparse
import json
from pathlib import Path

from .analysis import performance, write_report
from .connections import connections, load_request, write_connections
from .contracts import instant
from .storage import ingest, upgrade_database
from .ticketing import ingest_tickets, settlement, write_settlement


def _build_parser():
    parser = argparse.ArgumentParser(
        description="Observe synthetic ferry service from event exports"
    )
    parser.add_argument("--schedule", type=Path, default=Path("data/schedule.csv"))
    parser.add_argument("--events", type=Path, default=Path("data/events.jsonl"))
    parser.add_argument("--db", type=Path, default=Path("run/ferry.sqlite"))
    parser.add_argument("--output", type=Path, default=Path("run/report"))
    parser.add_argument("--as-of", help="timezone-aware observation cutoff")
    parser.add_argument(
        "--event-format", choices=["v1", "v2"], default="v1",
        help="revision format of the events file (v1 export or v2 revisions)",
    )
    parser.add_argument(
        "--report-only", action="store_true",
        help="rebuild a report from the committed ledger without ingesting",
    )
    parser.add_argument(
        "--known-at", type=int, default=None,
        help="knowledge sequence cutoff; report-only and connections only, 0..current",
    )
    parser.add_argument(
        "--upgrade-db", action="store_true",
        help="explicitly migrate a legacy database and exit",
    )
    parser.add_argument(
        "--connections", type=Path, default=None, metavar="REQUEST.json",
        help="offline connection analysis with the given request JSON "
             "(read-only, mutually exclusive with --report-only)",
    )
    parser.add_argument(
        "--ticket-ingest", type=Path, default=None, metavar="TICKETS.jsonl",
        help="ingest a strict ticket sale/refund JSONL file and refresh the "
             "settlement report; mutually exclusive with all event modes",
    )
    parser.add_argument(
        "--settlement", action="store_true",
        help="re-export the deterministic settlement report from committed "
             "ticket facts without ingesting (read-only re-export)",
    )
    return parser


def main(argv=None):
    parser = _build_parser()
    args = parser.parse_args(argv)
    report = None
    acquisition = None
    try:
        if args.upgrade_db:
            if (args.report_only or args.known_at is not None or args.as_of is not None
                    or args.connections is not None
                    or args.ticket_ingest is not None or args.settlement):
                raise ValueError("--upgrade-db is mutually exclusive with --report-only, --known-at, --as-of, --connections, --ticket-ingest and --settlement")
            result = upgrade_database(args.db)
            print(json.dumps(result, sort_keys=True))
            return

        # Ticket clearing operates on the existing trips schedule and never
        # touches the event revision ledger. It needs no as-of cutoff.
        if args.ticket_ingest is not None or args.settlement:
            if (args.report_only or args.known_at is not None
                    or args.as_of is not None or args.connections is not None):
                raise ValueError(
                    "ticket modes are mutually exclusive with --report-only, "
                    "--known-at, --as-of and --connections"
                )
            if args.ticket_ingest is not None and args.settlement:
                raise ValueError(
                    "--ticket-ingest and --settlement are mutually exclusive"
                )
            if args.ticket_ingest is not None:
                # The ledger commits before report writing. A failed write keeps
                # the committed facts intact and the old settlement file in
                # place; --settlement re-exports from the committed database.
                result = ingest_tickets(args.ticket_ingest, args.db)
                report = settlement(args.db)
                write_settlement(report, args.output)
                print(json.dumps(
                    {"ticket_ingestion": result,
                     "tickets": len(report["tickets"]),
                     "pending_refunds": len(report["pending_refunds"])},
                    sort_keys=True,
                ))
            else:
                report = settlement(args.db)
                write_settlement(report, args.output)
                print(json.dumps(
                    {"settlement": True, "ingest_seq": report["ingest_seq"],
                     "tickets": len(report["tickets"]),
                     "pending_refunds": len(report["pending_refunds"])},
                    sort_keys=True,
                ))
            return

        if args.as_of is None:
            raise ValueError("--as-of is required")
        instant(args.as_of)

        if args.connections is not None:
            if args.report_only:
                raise ValueError("--connections is mutually exclusive with --report-only")
            if args.known_at is not None and not (
                not isinstance(args.known_at, bool) and args.known_at >= 0
            ):
                raise ValueError("--known-at must be an integer >= 0")
            # Read-only path: the database is never created or upgraded here,
            # and a bad request or failed write keeps the previous report.
            request = load_request(args.connections)
            report = connections(args.db, args.as_of, args.known_at, request)
            write_connections(report, args.output)
            print(
                json.dumps(
                    {"connections": {
                        "status": report["diagnostics"]["status"],
                        "options": len(report["options"]),
                        "candidate_trips": len(report["candidate_trips"]),
                        "knowledge_seq": report["knowledge_seq"],
                    }},
                    sort_keys=True,
                )
            )
            return

        if args.report_only:
            if args.known_at is not None and not (
                not isinstance(args.known_at, bool) and args.known_at >= 0
            ):
                raise ValueError("--known-at must be an integer >= 0")
            # Read-only path: the database is never created or upgraded here.
            report = performance(args.db, args.as_of, args.known_at)
            write_report(report, args.output)
            print(
                json.dumps(
                    {"report_only": True, "knowledge_seq": report["knowledge_seq"],
                     "routes": report["routes"]},
                    sort_keys=True,
                )
            )
            return

        if args.known_at is not None:
            raise ValueError("--known-at is only valid together with --report-only or --connections")
        # The ledger commits before report writing. If writing the output fails
        # afterwards, the committed data stays valid and is recoverable through
        # --report-only; ingestion itself is not rolled back.
        acquisition = ingest(args.schedule, args.events, args.db, args.event_format)
        report = performance(args.db, args.as_of)
        write_report(report, args.output)
    except (OSError, ValueError) as exc:
        parser.exit(2, f"observation failed: {exc}\n")
    print(json.dumps({"ingestion": acquisition, "routes": report["routes"]}, sort_keys=True))


if __name__ == "__main__":
    main()
