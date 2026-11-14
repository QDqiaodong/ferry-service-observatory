"""Fixed-seed generator of interleaved revision histories.

The generator builds the histories purely with the Python standard library
(``random.Random`` seeded per history). It never calls production parsing,
projection or lifecycle functions: it only emits rendered JSONL text plus the
*intent* behind each row, which the independent oracle interprets.

Every generated history contains at least 12 batches and deliberately covers:

* corrections (upsert of a higher consecutive revision),
* void retractions followed by restore upserts,
* legacy duplicates (old revisions replayed, including offset/key-order
  variants that normalize to the same identity),
* in-batch duplicates and in-batch conflicts,
* same-batch out-of-order consecutive revisions,
* ledger-level conflicts, missing revisions and lifecycle rejections,
* empty and fully duplicate-only files.

The harness treats the generator labels as documentation; the oracle is the
real authority on accept/reject.
"""
import json
import random
from datetime import datetime, timedelta, timezone

# Eight trips across two routes, all on one deterministic service day.
SCHEDULE_ROWS = [
    ("G1", "Harbor-North", "2026-01-01T08:00:00Z", "2026-01-01T08:35:00Z"),
    ("G2", "Harbor-North", "2026-01-01T08:30:00Z", "2026-01-01T09:05:00Z"),
    ("G3", "Harbor-North", "2026-01-01T09:00:00Z", "2026-01-01T09:35:00Z"),
    ("G4", "Harbor-North", "2026-01-01T09:30:00Z", "2026-01-01T10:05:00Z"),
    ("G5", "Harbor-East", "2026-01-01T10:00:00Z", "2026-01-01T10:35:00Z"),
    ("G6", "Harbor-East", "2026-01-01T10:30:00Z", "2026-01-01T11:05:00Z"),
    ("G7", "Harbor-East", "2026-01-01T11:00:00Z", "2026-01-01T11:35:00Z"),
    ("G8", "Harbor-East", "2026-01-01T13:00:00Z", "2026-01-01T13:35:00Z"),
]

# Per trip the first delivery always uses these event times (minute resolution,
# all inside the 12:00 UTC reporting cutoff).
PATTERNS = {
    "arrived_pair": {
        "events": [
            ("departed", "2026-01-01T{h:02d}:02:00Z"),
            ("arrived", "2026-01-01T{h:02d}:40:00Z"),
        ],
    },
    "cancelled": {
        "events": [("cancelled", "2026-01-01T{h:02d}:45:00Z")],
    },
    "departed_only": {
        "events": [("departed", "2026-01-01T{h:02d}:00:00Z")],
    },
}
TRIP_PATTERN = {
    "G1": "arrived_pair",
    "G2": "arrived_pair",
    "G3": "cancelled",
    "G4": "departed_only",
    "G5": "departed_only",
    "G6": "arrived_pair",
    "G7": "departed_only",
    "G8": "departed_only",
}

BATCH_COUNT = 15  # >= 12 batches per history, as required by the gate.


def render_schedule():
    lines = ["trip_id,route,planned_departure,planned_arrival"]
    lines.extend(",".join(row) for row in SCHEDULE_ROWS)
    return ("\n".join(lines) + "\n").encode("utf-8")


def _iso(dt):
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _shift(timestamp, minutes):
    dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    return _iso(dt + timedelta(minutes=minutes))


def _offset_variant(timestamp):
    """Equivalent instant rendered with a +01:00 offset (normalizes equal)."""
    dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    local = dt.astimezone(timezone(timedelta(hours=1)))
    return local.strftime("%Y-%m-%dT%H:%M:%S+01:00")


def _upsert_row(event_id, revision, trip_id, kind, occurred_at):
    return {
        "event_id": event_id,
        "revision": revision,
        "action": "upsert",
        "trip_id": trip_id,
        "kind": kind,
        "occurred_at": occurred_at,
    }


def _void_row(event_id, revision):
    return {"event_id": event_id, "revision": revision, "action": "void"}


def _render(row, rng):
    """Render one intent row as JSON text.

    Some rows are deliberately rendered with shuffled key order or an
    equivalent +01:00 offset to exercise normalization-based dedup.
    """
    style = rng.randrange(10)
    if style == 3 and row.get("action") == "upsert":
        variant = dict(row)
        variant["occurred_at"] = _offset_variant(row["occurred_at"])
        keys = list(variant)
        rng.shuffle(keys)
        return json.dumps({k: variant[k] for k in keys})
    if style == 4:
        keys = list(row)
        rng.shuffle(keys)
        return json.dumps({k: row[k] for k in keys})
    return json.dumps(row)


class HistoryGenerator:
    """Builds one deterministic, self-contained interleaved history."""

    def __init__(self, seed):
        self.seed = seed
        self.rng = random.Random(seed)
        # event_id -> {"trip", "kind", "time", "next_rev", "alive", "rows": {rev: row}}
        self.state = {}
        self.batches = []
        self._build()

    # ------------------------------------------------------------ state ops
    def _add_event(self, event_id, trip_id, kind, occurred_at):
        row = _upsert_row(event_id, 1, trip_id, kind, occurred_at)
        self.state[event_id] = {
            "trip": trip_id,
            "kind": kind,
            "time": occurred_at,
            "next_rev": 2,
            "alive": True,
            "rows": {1: row},
        }
        return row

    def _next_rev(self, event_id):
        rev = self.state[event_id]["next_rev"]
        self.state[event_id]["next_rev"] = rev + 1
        return rev

    def _emit(self, label, rows, expected, note):
        rendered = [
            row if isinstance(row, str) else _render(row, self.rng)
            for row in rows
        ]
        self.rng.shuffle(rendered)
        payload = ("\n".join(rendered) + "\n").encode("utf-8") if rendered else b""
        self.batches.append(
            {
                "label": label,
                "expected": expected,  # "accept" or "reject"; oracle is authority
                "note": note,
                "payload": payload,
            }
        )

    # ------------------------------------------------------------- building
    def _initial_events(self):
        rows = []
        for trip_id, pattern_name in TRIP_PATTERN.items():
            hour = int(
                next(
                    row[2]
                    for row in SCHEDULE_ROWS
                    if row[0] == trip_id
                )[11:13]
            )
            for index, (kind, template) in enumerate(PATTERNS[pattern_name]["events"]):
                event_id = f"ev-{trip_id.lower()}-{index}"
                row = self._add_event(
                    event_id,
                    trip_id,
                    kind,
                    template.format(h=hour),
                )
                rows.append(row)
        # One exact in-batch duplicate (normalization must collapse it).
        dup_target = self.rng.choice(rows)
        rows.append(dict(dup_target))
        self._emit(
            "01_initial_shuffled_with_duplicate",
            rows,
            "accept",
            "revision-1 upserts, file order scrambled, one exact duplicate",
        )

    def _correction_batches(self):
        # A normal correction of an arrived trip: move departure and arrival
        # together so the merged lifecycle stays valid.
        dep = self.state["ev-g1-0"]
        arr = self.state["ev-g1-1"]
        dep_rev = self._next_rev("ev-g1-0")
        arr_rev = self._next_rev("ev-g1-1")
        new_dep = _shift(dep["time"], 3)
        new_arr = _shift(arr["time"], 2)
        dep_row = _upsert_row("ev-g1-0", dep_rev, "G1", "departed", new_dep)
        arr_row = _upsert_row("ev-g1-1", arr_rev, "G1", "arrived", new_arr)
        dep["time"], arr["time"] = new_dep, new_arr
        dep["rows"][dep_rev] = dep_row
        arr["rows"][arr_rev] = arr_row
        self._emit(
            "02_correction_move_pair_together",
            [dep_row, arr_row],
            "accept",
            "departure and arrival corrected together, lifecycle still valid",
        )

        # Same-batch out-of-order consecutive revisions for one event: the
        # higher revision is emitted before the lower one.
        ev = self.state["ev-g4-0"]
        rev_a = self._next_rev("ev-g4-0")
        rev_b = self._next_rev("ev-g4-0")
        low = _upsert_row(
            "ev-g4-0", rev_a, "G4", "departed", _shift(ev["time"], 1)
        )
        high = _upsert_row(
            "ev-g4-0", rev_b, "G4", "departed", _shift(ev["time"], 2)
        )
        ev["time"] = high["occurred_at"]
        ev["rows"][rev_a], ev["rows"][rev_b] = low, high
        self._emit(
            "03_out_of_order_consecutive_revisions",
            [high, low],
            "accept",
            "consecutive revisions presented in reverse order in one batch",
        )

        # Legacy duplicates: replay an old superseded revision in two
        # equivalent normalized forms.
        old_rev = 1
        original = ev["rows"][old_rev]
        offset = dict(original)
        offset["occurred_at"] = _offset_variant(original["occurred_at"])
        shuffled = dict(original)
        self._emit(
            "04_legacy_old_revision_replay",
            [original, offset, shuffled],
            "accept",
            "superseded revision 1 replayed with offset and key-order variants",
        )

        # Fully duplicate-only file: every row already accepted.
        some_row = self.rng.choice(list(self.state["ev-g2-0"]["rows"].values()))
        self._emit(
            "05_fully_duplicate_file",
            [dict(some_row)],
            "accept",
            "duplicate-only input registers a source but consumes no sequence",
        )

    def _void_restore(self):
        # Void both events of an arrived trip, then restore them with the
        # next consecutive revisions in one later batch.
        dep, arr = self.state["ev-g2-0"], self.state["ev-g2-1"]
        dep_void_rev = self._next_rev("ev-g2-0")
        arr_void_rev = self._next_rev("ev-g2-1")
        dep_void = _void_row("ev-g2-0", dep_void_rev)
        arr_void = _void_row("ev-g2-1", arr_void_rev)
        dep["alive"] = arr["alive"] = False
        dep["rows"][dep_void_rev] = dep_void
        arr["rows"][arr_void_rev] = arr_void
        self._emit(
            "06_void_whole_trip",
            [arr_void, dep_void],
            "accept",
            "voiding every event of trip G2 leaves it pending",
        )

        dep_rest_rev = self._next_rev("ev-g2-0")
        arr_rest_rev = self._next_rev("ev-g2-1")
        dep_rest = _upsert_row(
            "ev-g2-0", dep_rest_rev, "G2", "departed", _shift(dep["time"], 5)
        )
        arr_rest = _upsert_row(
            "ev-g2-1", arr_rest_rev, "G2", "arrived", _shift(arr["time"], 4)
        )
        dep["time"], arr["time"] = dep_rest["occurred_at"], arr_rest["occurred_at"]
        dep["alive"] = arr["alive"] = True
        dep["rows"][dep_rest_rev] = dep_rest
        arr["rows"][arr_rest_rev] = arr_rest
        self._emit(
            "07_restore_after_void",
            [arr_rest, dep_rest],
            "accept",
            "consecutive upsert revisions restore the voided trip",
        )

    def _empty_and_rejects(self):
        self._emit(
            "08_empty_file",
            [],
            "accept",
            "empty file registers its source without a knowledge sequence",
        )

        # Lifecycle rejection: void the departure of an arrived pair while
        # its arrival stays in place. The whole batch must roll back.
        arr_pair = self.state["ev-g6-1"]
        dep = self.state["ev-g6-0"]
        bad_rev = dep["next_rev"]
        dep["next_rev"] += 1
        bad_void = _void_row("ev-g6-0", bad_rev)
        self._emit(
            "09_reject_void_departure_keep_arrival",
            [bad_void],
            "reject",
            "final merged state has an arrival without a departure",
        )
        # The rejected revision must not become part of the logical chain.
        dep["next_rev"] = bad_rev

        # Unknown-event void rejects.
        self._emit(
            "10_reject_unknown_event_void",
            [_void_row("ev-ghost", 1)],
            "reject",
            "voiding an event that was never upserted",
        )

        # In-batch conflict: same revision identity, different payload.
        ev = self.state["ev-g3-0"]
        rev = self._next_rev("ev-g3-0")
        good = _upsert_row(
            "ev-g3-0", rev, "G3", "cancelled", _shift(ev["time"], 1)
        )
        bad = _upsert_row(
            "ev-g3-0", rev, "G3", "cancelled", _shift(ev["time"], 2)
        )
        self._emit(
            "11_reject_in_batch_conflict",
            [good, bad],
            "reject",
            "two different payloads for one revision identity in a batch",
        )
        ev["next_rev"] = rev  # rolled back: revision number is reusable

        # Ledger conflict: re-deliver an accepted revision identity with a
        # different normalized payload.
        accepted_rev = self.rng.choice(sorted(ev["rows"]))
        original = ev["rows"][accepted_rev]
        conflicting = dict(original)
        conflicting["occurred_at"] = _shift(original["occurred_at"], 7)
        self._emit(
            "12_reject_ledger_conflict",
            [conflicting],
            "reject",
            "accepted revision identity re-delivered with another payload",
        )

    def _cutoff_and_late_batches(self):
        # Correction that moves a 10:00 departure to 12:30, after the 12:00
        # reporting cutoff. Selecting revisions before the event-time filter
        # must NOT resurrect the old value.
        ev = self.state["ev-g5-0"]
        rev = self._next_rev("ev-g5-0")
        moved = _upsert_row(
            "ev-g5-0", rev, "G5", "departed", "2026-01-01T12:30:00Z"
        )
        ev["time"] = moved["occurred_at"]
        ev["rows"][rev] = moved
        self._emit(
            "13_correction_after_reporting_cutoff",
            [moved],
            "accept",
            "winner moves beyond T; old value must not be resurrected",
        )

        # A later correction brings it back before the cutoff (still valid).
        rev = self._next_rev("ev-g5-0")
        back = _upsert_row(
            "ev-g5-0", rev, "G5", "departed", "2026-01-01T09:58:00Z"
        )
        ev["time"] = back["occurred_at"]
        ev["rows"][rev] = back
        self._emit(
            "14_correction_back_before_cutoff",
            [back],
            "accept",
            "another consecutive revision moves the event before T again",
        )

        # A final duplicate replay touching several events, including an old
        # revision of the cutoff-moved event; no sequence may be consumed.
        replay = [
            dict(self.state["ev-g1-0"]["rows"][1]),
            dict(self.state["ev-g5-0"]["rows"][1]),
        ]
        self._emit(
            "15_final_duplicate_replay",
            replay,
            "accept",
            "mixed old-revision replay after many later revisions",
        )

    def _build(self):
        self._initial_events()
        self._correction_batches()
        self._void_restore()
        self._empty_and_rejects()
        self._cutoff_and_late_batches()
        assert len(self.batches) == BATCH_COUNT


def generate_history(seed):
    """Return a serializable history bundle for one fixed seed."""
    gen = HistoryGenerator(seed)
    return {
        "schema": "ferry-qg-history/1",
        "seed": seed,
        "schedule_csv": render_schedule().decode("utf-8"),
        "batches": gen.batches,
    }


DEFAULT_SEEDS = [20260101 + index for index in range(40)]
