"""Immutable interval identities and frozen, consumer-specific arrival scenarios."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Mapping


@dataclass(frozen=True, order=True)
class Read:
    """One half-open interval on an immutable object version.

    Within one dispatch epoch this is also a physical GET key. Each issued
    attempt receives a separate producer ID in the executor.
    """

    key: str
    version: str
    start: int
    end: int

    def __post_init__(self):
        if not self.key or not self.version or self.version == "null":
            raise ValueError("an object key and an immutable, non-null version are required")
        if not isinstance(self.start, int) or not isinstance(self.end, int):
            raise ValueError("byte offsets must be integers")
        if self.start < 0 or self.end <= self.start:
            raise ValueError("expected 0 <= start < end")

    @property
    def size(self) -> int:
        return self.end - self.start

    def covers(self, atom: Read) -> bool:
        return (self.key, self.version) == (atom.key, atom.version) and (
            self.start <= atom.start and atom.end <= self.end
        )


@dataclass(frozen=True)
class Query:
    id: str
    atoms: tuple[Read, ...]
    deadline: float
    weight: float = 1.0

    def __post_init__(self):
        if not self.id or not self.atoms or len(set(self.atoms)) != len(self.atoms):
            raise ValueError("queries need an ID and distinct, nonempty required atoms")
        if not math.isfinite(self.deadline) or not math.isfinite(self.weight) or self.weight < 0:
            raise ValueError("deadline must be finite and weight finite and nonnegative")
        object.__setattr__(self, "atoms", tuple(sorted(self.atoms)))
        for left, right in zip(self.atoms, self.atoms[1:]):
            if (left.key, left.version) == (right.key, right.version) and left.end > right.start:
                raise ValueError("query atoms must be disjoint; partition demands with atomize")


def atomize(demands: Mapping[str, tuple[Read, ...]]) -> dict[str, tuple[Read, ...]]:
    """Split shared demands at all endpoints, preserving object-version identity."""
    endpoints: dict[tuple[str, str], set[int]] = {}
    for spans in demands.values():
        for span in spans:
            endpoints.setdefault((span.key, span.version), set()).update((span.start, span.end))
    partitions = {
        identity: tuple(zip(sorted(points), sorted(points)[1:]))
        for identity, points in endpoints.items()
    }
    result = {}
    for query_id, spans in demands.items():
        atoms = {
            Read(span.key, span.version, start, end)
            for span in spans
            for start, end in partitions[(span.key, span.version)]
            if span.start <= start and end <= span.end
        }
        result[query_id] = tuple(sorted(atoms))
    return result


@dataclass(frozen=True)
class Scenario:
    """Absolute monotonic seconds, frozen for all actions in an epoch.

    Missing arrivals mean infinity; CPU estimates are mandatory. Rescue keys
    include the consumer so attachment and retained-prefix eligibility can be
    represented by omission. Baselines include all eligible existing producers.
    """

    baseline: Mapping[tuple[str, Read], float]
    cpu: Mapping[str, float]
    rescue: Mapping[tuple[Read, str, Read], float] = field(default_factory=dict)

    def __post_init__(self):
        for value in (*self.baseline.values(), *self.rescue.values()):
            if math.isnan(value) or value == -math.inf:
                raise ValueError("arrival times must be finite or positive infinity")
        if any(not math.isfinite(value) or value < 0 for value in self.cpu.values()):
            raise ValueError("CPU estimates must be finite and nonnegative")
        object.__setattr__(self, "baseline", MappingProxyType(dict(self.baseline)))
        object.__setattr__(self, "cpu", MappingProxyType(dict(self.cpu)))
        object.__setattr__(self, "rescue", MappingProxyType(dict(self.rescue)))

    def deficit(self, query: Query) -> tuple[Read, ...]:
        cpu = self.cpu[query.id]
        return tuple(atom for atom in query.atoms
                     if self.baseline.get((query.id, atom), math.inf) + cpu > query.deadline)

    def eligible(self, query: Query, dispatch_at: float) -> bool:
        return dispatch_at + self.cpu[query.id] <= query.deadline

    def timely(self, read: Read, query: Query, atom: Read, dispatch_at: float) -> bool:
        arrival = self.rescue.get((read, query.id, atom), math.inf)
        return (read.covers(atom) and arrival >= dispatch_at
                and arrival + self.cpu[query.id] <= query.deadline)
