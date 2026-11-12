"""A small explicit event lifecycle, independent of delivery order."""
from .contracts import instant


def lifecycle(events):
    state = "pending"
    departure = arrival = None
    for event in sorted(events, key=lambda e: (e["occurred_at"], e["event_id"])):
        kind = event["kind"]
        if kind == "cancelled" and state == "pending":
            state = "cancelled"
        elif kind == "departed" and state == "pending":
            state = "underway"
            departure = instant(event["occurred_at"])
        elif kind == "arrived" and state == "underway":
            arrival = instant(event["occurred_at"])
            if arrival <= departure:
                raise ValueError("arrival must be later than departure")
            state = "arrived"
        else:
            raise ValueError(f"invalid lifecycle: {state} -> {kind}")
    return state, departure, arrival
