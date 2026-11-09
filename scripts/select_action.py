"""Inspect bundle selection on a fixed analytical dependency example."""

from __future__ import annotations

import argparse
from dataclasses import asdict
from pathlib import Path

from _common import encode_read, write_report

from dcr import Budget, Limits, Query, Read, Scenario, generate_bundles, select
from dcr.selector import MaskScorer


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gets", type=int, default=2, help="optional GET cap")
    parser.add_argument("--bytes", dest="requested_bytes", type=int, default=8)
    parser.add_argument("--slots", type=int, default=2)
    parser.add_argument("--beam-width", type=int, default=4)
    parser.add_argument("--output", type=Path, help="write JSON instead of stdout")
    args = parser.parse_args()
    try:
        budget = Budget(args.gets, args.requested_bytes, args.slots)
        limits = Limits(beam_width=args.beam_width)
        x, y, z = (Read(key, "analytical-v1", 0, 4) for key in ("x", "y", "z"))
        queries = (Query("q1", (x, y), .010), Query("q2", (y, z), .040),
                   Query("q3", (y,), .009))
        base = {x: .020, y: .025, z: .007}
        baseline = {(q.id, atom): base[atom] for q in queries for atom in q.atoms}
        cpu = {q.id: 0.0 for q in queries}
        generation = (Scenario(baseline, cpu),)
        bundles = generate_bundles(queries, generation, dispatch_at=0.0, limits=limits)
        menu = tuple(sorted({read for bundle in bundles for read in bundle}))
        arrivals = {(read, q.id, atom): .008 for read in menu
                    for q in queries for atom in q.atoms if read.covers(atom)}
        scoring = (Scenario(baseline, cpu, arrivals),)
        proposal = select(queries, bundles, scoring, budget, 0.0, limits)
        scorer = MaskScorer(queries, menu, scoring, 0.0)
        report = {
            "kind": "ANALYTICAL_SELECTION",
            "time_unit": "seconds",
            "dispatch_at": 0.0,
            "budget": asdict(budget),
            "beam_width": args.beam_width,
            "bundles": [[encode_read(read) for read in bundle] for bundle in bundles],
            "single_read_values": [
                {"read": encode_read(read), "value": scorer.value((read,))}
                for read in menu
            ],
            "selected_reads": [encode_read(read) for read in proposal.reads],
            "gets": len(proposal.reads),
            "requested_bytes": sum(read.size for read in proposal.reads),
            "waiting_value": proposal.waiting_value,
            "selected_value": proposal.value,
            "gain": proposal.gain,
        }
        write_report(report, args.output)
    except (ValueError, OSError) as error:
        parser.exit(2, f"select_action: {error}\n")


if __name__ == "__main__":
    main()

