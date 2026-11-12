# Ferry events and performance v1

Inputs are a synthetic shore-gateway JSONL export and independent planned
schedule CSV. This is file-based acquisition, not an external API connector.
Every event has exactly event_id, trip_id, kind, occurred_at. All values are
nonempty strings; kinds: departed, arrived, cancelled. All timestamps carry a
timezone and canonicalize to UTC. Schedule has unique trip_id, route,
planned_departure, planned_arrival; arrival must be later than departure.
Trip identity is globally unique inside one immutable schedule snapshot.

Event ID is the delivery deduplication key. Identical canonical payload repeats
are ignored. An ID with a changed payload rejects the whole ingestion batch.
Unknown trips and invalid lifecycles also reject and roll back the batch,
including source registration. Source SHA256 fingerprints each accepted export.
No row-level quarantine or partial acceptance. Empty event exports are valid.

Lifecycle by event time, independent of file order:
pending -> departed/underway -> arrived, or pending -> cancelled.
Arrival must be strictly later than departure. Repeated transitions with distinct
IDs and cancellation after departure are invalid. Input must contain a complete
valid lifecycle prefix per trip; an arrival arriving in a separate batch before
its departure is not buffered by this baseline. There are no retractions.

As-of cutoff is required. Only trips with planned_departure <= cutoff enter the
report; events after cutoff are hidden. Missing departure events stay pending,
not automatically cancelled. Cancellation before departure is terminal.
On-time means actual departure delay in [0,300] seconds inclusive; early departure
is not on-time. On-time rate denominator is observed departed trips (including
arrived trips); cancellation rate denominator is all due trips. No departed trips
means null on-time rate, not 0% or 100%. Travel duration is averaged only over
arrived trips. Report details expose every included trip and the metric inputs.

SQLite ingestion commits before output generation. JSON and CSV can be regenerated
from the committed database. Multi-file report writes are single-process and do
not provide a concurrent snapshot publication protocol. CSV identifiers are raw
machine data; import as text if opening in spreadsheet software.
