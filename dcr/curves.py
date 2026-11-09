"""Conservative chunk-arrival projection from caller-supplied traffic windows."""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from typing import Iterable, Mapping

from .model import Query, Read, Scenario


@dataclass(frozen=True)
class ProgressCurve:
    """Observed (seconds since dispatch, cumulative bytes) samples for one range."""

    samples: tuple[tuple[float, int], ...]

    def __post_init__(self):
        object.__setattr__(self, "samples", tuple(tuple(point) for point in self.samples))
        previous_time, previous_bytes = -1.0, 0
        for elapsed, count in self.samples:
            if (not math.isfinite(elapsed) or elapsed < 0 or elapsed < previous_time
                    or not isinstance(count, int) or count <= previous_bytes):
                raise ValueError("curve times must be monotonic and byte counts must increase")
            previous_time, previous_bytes = elapsed, count

    def final_byte_at(self, offset: int) -> float:
        if offset < 1:
            raise ValueError("final-byte offset must be positive")
        return next((elapsed for elapsed, count in self.samples if count >= offset), math.inf)


@dataclass(frozen=True)
class ExistingProducer:
    read: Read
    dispatched_at: float
    curve: ProgressCurve
    consumers: frozenset[str]
    delivered_bytes: int = 0


def project_window(queries: Iterable[Query], existing: Iterable[ExistingProducer],
                   candidates: Mapping[Read, ProgressCurve], cpu: Mapping[str, float], *,
                   now: float, dispatch_at: float,
                   completed: Mapping[tuple[str, Read], float] | None = None) -> Scenario:
    """Project one matched traffic window without mixing independent reads.

    Existing curves must survive the observed request age for unfinished bytes.
    Prefixes already delivered are supplied through completed; they are not
    assumed replayable to consumers attaching later. Incompatible historical
    curves are omitted. All candidate reads are new requests at dispatch_at.
    """
    if dispatch_at < now:
        raise ValueError("candidate dispatch cannot precede the epoch")
    queries, existing = tuple(queries), tuple(existing)
    baseline = dict(completed or {})
    rescue = {}
    for query in queries:
        for atom in query.atoms:
            for producer in existing:
                if query.id not in producer.consumers or not producer.read.covers(atom):
                    continue
                offset = atom.end - producer.read.start
                arrival = producer.dispatched_at + producer.curve.final_byte_at(offset)
                # Reject a history that would already have delivered an observed missing byte.
                if offset > producer.delivered_bytes and arrival >= now:
                    key = query.id, atom
                    baseline[key] = min(baseline.get(key, math.inf), arrival)
            for read, curve in candidates.items():
                if read.covers(atom):
                    rescue[read, query.id, atom] = dispatch_at + curve.final_byte_at(
                        atom.end - read.start)
    return Scenario(baseline, cpu, rescue)


def split_windows(windows: Iterable[tuple[str, object]], generation_count: int,
                  scoring_count: int, *, seed: int = 0) -> tuple[tuple, tuple]:
    """Sample disjoint whole windows after the caller matches load/progress buckets.

    Return empty cohorts when calibration is sparse, so the controller can skip
    optional rescue. Duplicate window IDs are rejected rather than leaking a
    window into both cohorts.
    """
    windows = tuple(windows)
    if generation_count < 1 or scoring_count < 1:
        raise ValueError("both scenario counts must be positive")
    if len({window_id for window_id, _ in windows}) != len(windows):
        raise ValueError("historical window IDs must be unique")
    if len(windows) < generation_count + scoring_count:
        return (), ()
    sampled = random.Random(seed).sample(windows, generation_count + scoring_count)
    return tuple(sampled[:generation_count]), tuple(sampled[generation_count:])
