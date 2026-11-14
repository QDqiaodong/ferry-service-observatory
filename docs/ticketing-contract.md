# Passenger ticket clearing: JSONL contract, apportionment and settlement

Ticket clearing adds a passenger-money path alongside the event ledger. It
reuses the immutable `trips` schedule already in the database and never
touches the event revision tables. Two modes are exposed:

- `--ticket-ingest TICKETS.jsonl --db DB --output DIR`: validate and acquire
  one strict JSONL file inside a single serializable transaction, then refresh
  the settlement report.
- `--settlement --db DB --output DIR`: read-only re-export of the
  deterministic `settlement.json` from committed facts, with no ingestion.

Both are mutually exclusive with `--report-only`, `--connections`,
`--upgrade-db`, `--known-at` and `--as-of`; ticketing has no event-time
cutoff. Ingestion requires an existing non-empty `trips` table, so a schedule
must be ingested first. A database without trips is rejected and, if sqlite
had to create an empty file for the attempt, that zero-byte file is removed.

## Strict JSONL input

Every line is one JSON object parsed as strict JSON: duplicate object keys,
`NaN`/`Infinity` constants and non-object lines are rejected. Two record types
exist with exactly the listed keys.

Sale:

```json
{"type":"sale","ticket_id":"T-100","paid_cents":10,
 "legs":[{"trip_id":"F3","weight":1},{"trip_id":"F1","weight":1}]}
```

- `ticket_id`: non-empty string.
- `paid_cents`: non-negative integer; booleans and floats are rejected. A
  zero-fare ticket is legal and can never carry a refund.
- `legs`: non-empty list, each with exactly `trip_id` (non-empty string
  naming an existing schedule trip) and `weight` (positive integer; booleans
  rejected). Leg trip_ids must be unique within the ticket. Leg order carries
  no meaning: the canonical payload sorts legs by `trip_id`, so a shuffled
  replay is the same identity.

Refund:

```json
{"type":"refund","refund_id":"R-100","ticket_id":"T-100","cents":1}
```

- `refund_id` and `ticket_id`: non-empty strings.
- `cents`: strictly positive integer; zero, negatives, booleans and floats
  are rejected.

Refunds may arrive before their ticket's sale. Such a refund is accepted and
surfaced under `pending_refunds` until the sale lands; it cannot be validated
against a fare that does not exist yet.

## Identifiers, idempotency and conflicts

Sales and refunds keep independent identifier namespaces (`ticket_id` and
`refund_id` never collide). Identical repeats inside one file, and replays of
already-accepted records, are idempotent duplicates: they must carry the same
canonical normalized content. A different payload for one already-used
identifier — inside the same file or against the committed ledger — is a
conflict and rejects the whole batch before anything commits. Replays
consume no ingest sequence.

## Integer apportionment (largest remainder)

All money is integer cents; no floating point arithmetic appears anywhere.

1. **Fare split.** A ticket's `paid_cents` is split over its legs by the
   Hamilton/largest-remainder method on the leg weights. Each leg receives
   `floor(total * weight / weight_sum)`; the leftover cents are distributed
   one each to the legs with the largest remainder
   `(total * weight) mod weight_sum`. Exact remainder ties go to the
   lexicographically smallest `trip_id`. The allocated amounts always sum
   exactly to `paid_cents`.
2. **Cumulative refund split.** Refunds are accumulated per ticket. The
   cumulative refund total is re-apportioned with the same largest-remainder
   method, using the *original per-leg fare allocation* (in cents) as the
   leg weights. Refund amounts therefore always reconcile to the cumulative
   refund total and never exceed the original fare on any leg.
3. **Net.** Per-leg net revenue is the integer difference
   `fare_cents - refund_cents`.

The cumulative refund for a ticket can never exceed its `paid_cents`. The cap
is evaluated against the union of the committed ledger and every refund in
the candidate batch, so a batch that crosses the cap only when its rows are
merged is still rejected. Refunds whose sale is unknown are held as pending
and checked when that sale is later accepted.

## Transactional storage

Tables live beside the event ledger in the same SQLite file, prefixed
`ticket_`: `ticket_sales`, `ticket_sale_legs`, `ticket_refunds`,
`ticket_allocation`, `ticket_batches`, `ticket_sources` and `ticket_meta`.

A successful batch that adds at least one sale or refund gets one
monotonically increasing `ingest_seq` (starting at 1) inside the same
`BEGIN IMMEDIATE` transaction as the new rows, the recomputed
`ticket_allocation` facts for every touched ticket and the source-file
SHA256. Fully duplicate or empty files register their digest in
`ticket_sources` but consume no sequence. Any validation failure rolls the
entire batch back — rows, allocations, batch sequence and source
registration — leaving the database byte-for-byte as it was.

## Deterministic settlement export

`settlement.json` is rendered with sorted keys and fixed list ordering and
contains no timestamps, host names or paths, so the same committed database
reproduces byte-identical output on every run, including after later
ingestions. It contains:

- `schema_version`, the current `ingest_seq`.
- `tickets`: one entry per accepted sale with its canonical legs and weights,
  the full fare `allocation_basis` (per-leg base floor, remainder numerator,
  `extra_unit` flag and final allocation), the fare/refund/net `allocation`,
  the ordered refunds, and — when any refund exists — the `refund_basis`
  showing the original-fare weights and the same remainder evidence.
- `pending_refunds`: refunds accepted before their sale, if any.
- `trips`: per-schedule-trip rollups of `fare_cents`, `refund_cents` and
  `net_cents`, including trips with no ticket money (all zero).

The file is written to a temporary file in the output directory, fsynced and
atomically `os.replace`d. A failed write leaves any previous
`settlement.json` untouched; because ingestion commits first, a post-commit
write failure still leaves every fact recoverable through `--settlement`.

## Worked example

A 10-cent ticket over three equal-weight legs `F1/F2/F3` splits 4/3/3: each
floor is 3, all three remainders tie at 1, and the single leftover cent goes
to the lexicographically smallest `F1`. If 3 cents are later refunded
cumulatively, the refund uses the original split 4/3/3 as weights: each leg
gets 1 cent, leaving nets 3/2/2. A second ticket of 30 cents over the same
three legs splits 10/10/10; a 10-cent refund re-apportions to 4/3/3 by the
same tie rule.
