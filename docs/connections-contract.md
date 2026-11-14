# Offline connection analysis: request, frontier and report contract

The connections mode answers one passenger question against the committed
ledger: starting at `origin` at `ready_at`, which journeys to `destination`
are worth considering, given the trips that had *actually arrived* by the
event-time cutoff `T` under knowledge cutoff `K`?

It is a read-only mode: `--connections REQUEST.json --db DB --output DIR
--as-of T [--known-at K]`. The database is opened read-only inside one
snapshot transaction and is never created, migrated or mutated. The old
entry points (ingest, `--report-only`, `--upgrade-db`) are unchanged;
`--connections` is mutually exclusive with `--report-only` and
`--upgrade-db`, and `--as-of` is required.

## Request JSON (strict)

A single JSON object with exactly these keys; duplicate keys, `NaN` and
`Infinity` are rejected:

```json
{
  "schema_version": 1,
  "routes": {
    "Harbor-East": {"origin": "Harbor", "destination": "East", "fare_cents": 500}
  },
  "transfer_seconds": {"Harbor": 0, "East": 600},
  "origin": "Harbor",
  "destination": "North",
  "ready_at": "2026-01-01T08:00:00Z"
}
```

- `routes` maps each route name to exactly `origin`, `destination`,
  `fare_cents`: two distinct non-empty port names and a non-negative integer
  fare in cents (booleans and floats are rejected). The mapping is unique by
  construction (duplicate JSON keys are an error).
- `transfer_seconds` maps each port to a non-negative integer number of
  seconds (booleans rejected).
- `origin` and `destination` are non-empty, distinct port names.
- `ready_at` is a timezone-aware timestamp, canonicalized to UTC, and must be
  at or before `--as-of` (`T`).

References must be complete. Every port named as a route endpoint or as the
journey origin/destination must have a `transfer_seconds` entry, and every
route present in the database must appear in `routes` — otherwise candidate
ports or fares would be unknown. Extra routes not present in the database are
allowed so one request file can serve several schedule snapshots. Any
violation is a bad input: the command fails with exit code 2 and the previous
report is left untouched.

## Candidate trips (existing dual-cutoff rule)

Fact selection follows the reporting rule exactly, in the same order:

1. keep revisions whose first accepting `knowledge_seq <= K` (latest
   knowledge when `--known-at` is omitted);
2. per `event_id` pick the highest revision and apply voids;
3. among surviving upserts keep only `occurred_at <= T`.

A trip is a candidate only when its projected lifecycle is `arrived`; its
actual departure and actual arrival are used. Pending, underway and cancelled
trips are excluded, and planned times never substitute for unknown actuals.

## Journey rules and the frontier

A journey is a sequence of one or more candidate legs where

- the first leg departs from `origin` at or after `ready_at`;
- every later leg departs from the port the previous leg arrived at, at or
  after `previous arrival + transfer_seconds[that port]`.

Each journey carries three metrics: final arrival time, total fare (sum of
leg fares), transfer count (legs minus one). Journey A dominates journey B
when A is no worse on all three metrics and strictly better on at least one.
The report lists exactly the non-dominated journeys; among journeys with
identical metrics only the lexicographically smallest `trip_id` sequence is
kept.

The frontier is computed by a multi-criteria label scan in departure order,
keeping a pruned label set per port — neither collapsing each port to its
earliest label (later-but-cheaper options would be lost) nor enumerating all
paths. Pruning at intermediate ports cannot use arrival-time dominance alone:
connection slack can absorb an arrival-time advantage, turning it into an
exact metric tie that the lexicographic tie-break then decides. An
intermediate label is therefore discarded only when another label is no worse
on every metric and is either strictly better on fare or transfers, or
carries a lexicographically no-larger `trip_id` path. The destination applies
full Pareto dominance plus the tie-break. Correctness is cross-checked in the
test suite against brute-force enumeration of every valid journey on
randomized small networks.

## Report JSON (`connections.json`, schema_version 1)

Deterministic, byte-stable per `(database, T, K, request)` and free of run
times and machine paths:

- `as_of`, `knowledge_seq`: the effective cutoffs.
- `request`: the normalized request (UTC `ready_at`).
- `candidate_trips`: every candidate trip with route, ports, fare, actual
  departure/arrival, and `event_revisions` — the used revisions with
  `event_id`, `revision`, `knowledge_seq`, `kind`, `occurred_at` and
  `first_source_sha256` (plus `source_note` for legacy-unattributed
  revisions). Together with `request` this is everything needed to recompute
  the frontier offline.
- `options`: the non-dominated journeys, sorted by
  `(arrival, total_fare_cents, transfers, trip_ids)`, each with the full leg
  list (trip, route, ports, actual times, fare) and the three metrics.
- `diagnostics`: `status` (`ok` / `no_route`), `reason`
  (`no_candidate_trips`, `origin_not_served`, `destination_not_served`,
  `no_connection`, or null), `candidate_trip_count` and the sorted
  `reachable_ports` from `origin`. An unreachable request is a normal result
  (exit code 0), not an error.

## Deterministic atomic replacement

`connections.json` is rendered with sorted keys, written to a temporary file
in the output directory, fsynced and atomically renamed over the previous
report. A bad request, an out-of-range `K`, an unreadable database or a
failed write all leave the previous report byte-identical; no partial output
is ever visible. Rebuilding the same `(K, T)` after later ingestions
reproduces the same bytes.
