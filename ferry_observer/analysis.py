"""As-of service performance with explicit denominators and missing states."""
import csv
import json
import sqlite3
from pathlib import Path
from .contracts import instant
from .reconcile import lifecycle
from .storage import read_events


def performance(database, as_of):
    cutoff = instant(as_of)
    details = []
    with sqlite3.connect(database) as db:
        groups = read_events(db)
        for trip, route, planned_departure, planned_arrival in db.execute("SELECT * FROM trips ORDER BY planned_departure,trip_id"):
            planned = instant(planned_departure)
            if planned > cutoff:
                continue
            visible = [e for e in groups[trip] if instant(e["occurred_at"]) <= cutoff]
            state, departure, arrival = lifecycle(visible)
            delay = (departure-planned).total_seconds() if departure is not None else None
            details.append({"trip_id":trip,"route":route,"state":state,"departure_delay_seconds":delay,
                            "on_time": None if delay is None else 0 <= delay <= 300,
                            "travel_seconds": (arrival-departure).total_seconds() if arrival else None})
    routes = []
    for route in sorted({d["route"] for d in details}):
        rows = [d for d in details if d["route"]==route]
        operated = [r for r in rows if r["departure_delay_seconds"] is not None]
        travelled = [r["travel_seconds"] for r in rows if r["travel_seconds"] is not None]
        cancelled = sum(r["state"]=="cancelled" for r in rows)
        ontime = sum(r["on_time"] is True for r in rows)
        routes.append({"route":route,"due_trips":len(rows),"departed_trips":len(operated),"cancelled_trips":cancelled,
                       "pending_trips":sum(r["state"]=="pending" for r in rows),"arrived_trips":len(travelled),
                       "on_time_departures":ontime,"on_time_rate":ontime/len(operated) if operated else None,
                       "cancellation_rate":cancelled/len(rows),
                       "mean_departure_delay_seconds":sum(r["departure_delay_seconds"] for r in operated)/len(operated) if operated else None,
                       "mean_travel_seconds":sum(travelled)/len(travelled) if travelled else None})
    return {"schema_version":1,"as_of":cutoff.isoformat(),"routes":routes,"trips":details}


def write_report(report, output):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    (output/"performance.json").write_text(json.dumps(report,indent=2,sort_keys=True)+"\n")
    with (output/"trips.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=["trip_id","route","state","departure_delay_seconds","on_time","travel_seconds"])
        writer.writeheader()
        writer.writerows(report["trips"])
