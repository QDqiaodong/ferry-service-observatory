# Ferry events and performance: v1 exports and v2 revisions

Inputs are a synthetic shore-gateway JSONL export and an independent planned
schedule CSV. This is file-based acquisition, not an external API connector.
The schedule has unique `trip_id, route, planned_departure, planned_arrival`;
arrival must be later than departure. Trip identity is globally unique inside
one immutable schedule snapshot and never changes for a logical event.

All timestamps carry a timezone and canonicalize to UTC. Canonical payloads are
deterministic compact JSON strings (`sort_keys=True`), so two records that name
the same instant in different UTC offsets, or order JSON fields differently,
normalize to the same revision identity.

## v1 export (default)

Every line has exactly `event_id, trip_id, kind, occurred_at`. All values are
nonempty strings; kinds are `departed`, `arrived`, `cancelled`. Each record is
interpreted as `revision=1`, `action=upsert` of that logical event. A v1 record
and the equivalent v2 first revision are the same revision, not two facts.

## v2 revision export (`--event-format v2`)

Each line is one of two exact structures:

- upsert: `event_id, revision, action, trip_id, kind, occurred_at` with
  `action="upsert"`; the business fields follow the same validation and UTC
  normalization as v1.
- void: `event_id, revision, action` with `action="void"`. A void retracts the
  whole logical event and carries no event time or business fields.

`revision` must be a positive integer; booleans are rejected. `event_id` is the
logical event identity and `(event_id, revision)` is the immutable revision
identity. Per logical event the chain starts at a revision-1 upsert and later
revisions are consecutive integers. A void may be followed by another upsert to
restore the event. Upserts can correct `kind` and `occurred_at`, but every
revision of one event keeps the `trip_id` established by revision 1.

Two records with the same `(event_id, revision)` and identical normalized
content are duplicates (including offset and key-order differences). Different
normalized content for one revision identity is a conflict and rejects the
whole batch. Old revisions that were later corrected or voided still only match
as duplicates on replay; they can never take effect again.

File line order does not express revision or event order. A batch may present
several consecutive revisions out of order; continuity is evaluated against the
union of the already accepted ledger and the whole batch. Missing revision 1,
revision skips, voiding an unknown event, changing an event's trip, or
referencing an unknown trip all reject the batch. Every input revision passes
its own contract check, even one that a higher revision in the same file will
supersede.

## Immutable ledger and knowledge sequencing

Accepted revisions are never overwritten or deleted. A successful batch that
adds at least one new revision receives one monotonically increasing
`knowledge_seq` starting at 1, assigned inside the same transaction as the
revisions, the current event projection and the source registration. Empty
files and fully duplicate inputs register their source but consume no sequence;
sequence 0 means only the fixed schedule is known with no event facts. Failed
batches roll back entirely and do not consume a sequence.

For each event the batch first selects the highest revision over the ledger
plus the new candidates, removes events whose winner is a void, and only then
validates the resulting per-trip lifecycle. Validation therefore targets the
final merged state, never a row-by-row transient: moving a departure and its
arrival later together applies, while voiding a departure but keeping its
arrival is rejected.

Lifecycle rules are unchanged, by event time and independent of file order:
`pending -> departed/underway -> arrived`, or `pending -> cancelled`. Arrival
must be strictly later than departure. Repeated state transitions and
cancellation after departure are invalid. No row-level quarantine or partial
acceptance exists.

Every new revision stores the SHA256 of the first file that successfully
delivered it; repeated delivery never rewrites that ownership. `sources` keeps
the raw-byte digest and `row_count` of every successful file, including empty
and fully duplicate ones; source registration alone never advances
`knowledge_seq`.

## Dual cutoff reporting

Reports take an event-time cutoff `T` (`--as-of`) and a knowledge cutoff `K`
(`--known-at`, latest sequence when omitted). The order is fixed:

1. keep revisions whose first accepting `knowledge_seq <= K`;
2. for each `event_id` pick the highest revision and apply voids;
3. among surviving upserts keep only `occurred_at <= T`.

Filtering by event time before choosing a revision would resurrect a corrected
old value when the correction moved the event after `T`; selecting revisions
first prevents that. Trips whose planned departure is `<= T` remain in scope
and trips without a valid departure stay pending. Cancellation before
departure stays terminal.

On-time means actual departure delay in `[0, 300]` seconds inclusive; an early
departure is not on-time. The on-time denominator is observed departed trips
(including arrived trips); the cancellation denominator is all due trips. No
departed trips means a null on-time rate. Travel duration averages arrived
trips only. Report JSON is `schema_version=2` and lists, per included trip,
every revision known at `K` with its commit sequence, canonical payload, first
source and a status of `used`, `superseded`, `voided` or `after_cutoff`.
Revisions or sources accepted after `K` never appear.

The same `(K, T)` rebuilds byte-identical JSON and CSV regardless of later
ingestion; reports contain no run time, current-version counters or machine
paths. `--report-only` opens the database read-only, never creates or upgrades
it, and rejects an out-of-range `K`. Ingestion commits before report writing:
an output failure leaves the committed ledger valid and rebuildable via
`--report-only`.

## Legacy database upgrade

`--upgrade-db` is mutually exclusive with every other mode and migrates an
existing pre-revision database in one explicit transaction, preserving the old
`trips`, `events` and `sources` data. Legacy events become revision-1 upserts
in synthetic knowledge version 1; a legacy database without events stays at
sequence 0. The migration is idempotent and a failure leaves no partial
schema or version marker.

The legacy layout has no event-to-source mapping and no real ingestion order,
so migrated revisions have an empty `source_sha256` and a `legacy_unattributed`
note. Source digests are never guessed from the old `sources` table and no
received time is fabricated. Version 1 means the complete set of facts known
at migration; the fine-grained knowledge history before migration cannot be
recovered. Only revisions accepted by the new code carry a first-source link.
Ingestion and reporting against an unupgraded legacy database ask for the
upgrade instead of migrating implicitly.
