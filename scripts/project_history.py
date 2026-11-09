"""Project one caller-supplied historical window into arrival scenarios."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

from _common import encode_read, write_report

from dcr import Query, Read
from dcr.curves import ExistingProducer, ProgressCurve, project_window


def curve_for(read: Read, samples: list) -> ProgressCurve:
    curve = ProgressCurve(tuple(tuple(sample) for sample in samples))
    if curve.samples and curve.samples[-1][1] > read.size:
        raise ValueError("curve byte count exceeds its read span")
    return curve


def project(data: dict) -> dict:
    reads = {name: Read(**definition) for name, definition in data["reads"].items()}
    queries = tuple(Query(q["id"], tuple(reads[name] for name in q["atoms"]),
                          q["deadline"], q.get("weight", 1.0)) for q in data["queries"])
    by_id = {query.id: query for query in queries}
    if not queries or len(by_id) != len(queries):
        raise ValueError("query IDs must be unique and queries nonempty")
    now, dispatch_at = data["now"], data["dispatch_at"]
    if not math.isfinite(now) or not math.isfinite(dispatch_at) or dispatch_at < now:
        raise ValueError("finite observation and dispatch times must satisfy now <= dispatch_at")
    if any(query.id not in data["cpu"] for query in queries):
        raise ValueError("every query requires a CPU estimate")
    existing = []
    for item in data.get("existing", []):
        read = reads[item["read"]]
        dispatched_at = item["dispatched_at"]
        delivered = item.get("delivered_bytes", 0)
        consumers = frozenset(item["consumers"])
        if not math.isfinite(dispatched_at) or dispatched_at > now:
            raise ValueError("existing producer dispatch must be finite and no later than now")
        if not isinstance(delivered, int) or not 0 <= delivered <= read.size:
            raise ValueError("delivered bytes must be an integer within the read span")
        if not consumers.issubset(by_id):
            raise ValueError("existing producer has an unknown consumer")
        existing.append(ExistingProducer(read, dispatched_at, curve_for(read, item["curve"]),
                                         consumers, delivered))
    completed = {}
    for item in data.get("completed", []):
        query, atom, arrival = by_id[item["query"]], reads[item["atom"]], item["at"]
        if atom not in query.atoms or not math.isfinite(arrival) or arrival > now:
            raise ValueError("completed atoms require a matching demand and observed arrival")
        key = query.id, atom
        completed[key] = min(completed.get(key, math.inf), arrival)
    candidates = {reads[name]: curve_for(reads[name], samples)
                  for name, samples in data.get("candidates", {}).items()}
    scenario = project_window(queries, existing, candidates, data["cpu"],
                              now=now, dispatch_at=dispatch_at, completed=completed)

    def arrival_value(value: float) -> float | None:
        return value if math.isfinite(value) else None

    return {
        "kind": "HISTORICAL_WINDOW_PROJECTION",
        "window_id": data.get("window_id"),
        "time_unit": "seconds",
        "now": now,
        "dispatch_at": dispatch_at,
        "cpu": dict(scenario.cpu),
        "baseline": [{"query": qid, "atom": encode_read(atom), "arrival": arrival_value(at)}
                     for (qid, atom), at in sorted(scenario.baseline.items())],
        "rescue": [{"read": encode_read(read), "query": qid, "atom": encode_read(atom),
                    "arrival": arrival_value(at)}
                   for (read, qid, atom), at in sorted(scenario.rescue.items())],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="JSON description of a matched window")
    parser.add_argument("--output", type=Path, help="write JSON instead of stdout")
    args = parser.parse_args()
    try:
        data = json.loads(args.input.read_text(encoding="utf-8"))
        write_report(project(data), args.output)
    except (ValueError, TypeError, KeyError, AttributeError, OSError) as error:
        parser.exit(2, f"project_history: {error}\n")


if __name__ == "__main__":
    main()

