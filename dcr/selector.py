"""Late-signature interval covers and bounded joint completion-mask search."""

from __future__ import annotations

import math
import time
from collections import Counter
from dataclasses import dataclass
from typing import Callable, Iterable

from .budget import Budget
from .model import Query, Read, Scenario

Action = tuple[Read, ...]


@dataclass(frozen=True)
class Limits:
    max_gets: int = 4
    max_range_bytes: int = 8 * 1024 * 1024
    beam_width: int = 4
    candidate_queries: int = 8
    signatures_per_query: int = 2
    max_signature_atoms: int = 16

    def __post_init__(self):
        if any(not isinstance(v, int) or v < 1 for v in vars(self).values()):
            raise ValueError("all search limits must be positive integers")


def _rank_cover(action: Action):
    return sum(read.size for read in action), action


def _object_covers(atoms: tuple[Read, ...], limits: Limits) -> dict[int, Action]:
    """F[j,k]: minimum requested span covering the first j sorted atoms."""
    states: dict[tuple[int, int], Action] = {(0, 0): ()}
    for end in range(1, len(atoms) + 1):
        for count in range(1, min(end, limits.max_gets) + 1):
            choices = []
            for begin in range(end):
                prefix = states.get((begin, count - 1))
                if prefix is None:
                    continue
                read = Read(atoms[begin].key, atoms[begin].version,
                            atoms[begin].start, atoms[end - 1].end)
                if read.size <= limits.max_range_bytes:
                    choices.append(prefix + (read,))
            if choices:
                states[end, count] = min(choices, key=_rank_cover)
    return {count: states[len(atoms), count] for count in range(1, limits.max_gets + 1)
            if (len(atoms), count) in states}


def _signature_covers(atoms: tuple[Read, ...], limits: Limits) -> list[Action]:
    groups: dict[tuple[str, str], list[Read]] = {}
    for atom in sorted(atoms):
        groups.setdefault((atom.key, atom.version), []).append(atom)
    combined = {0: ()}
    for group in groups.values():
        choices = _object_covers(tuple(group), limits)
        updated: dict[int, Action] = {}
        for left_count, left in combined.items():
            for right_count, right in choices.items():
                count = left_count + right_count
                if count > limits.max_gets:
                    continue
                action = tuple(sorted(left + right))
                if count not in updated or _rank_cover(action) < _rank_cover(updated[count]):
                    updated[count] = action
        combined = updated
    return list(combined.values())


def generate_bundles(
    queries: Iterable[Query], scenarios: Iterable[Scenario], dispatch_at: float,
    limits: Limits = Limits(), *, stop_at: float = math.inf,
    clock: Callable[[], float] = time.monotonic,
) -> tuple[Action, ...]:
    """Keep frequent late signatures and one minimum-span cover per GET count.

    Generation and scoring scenario sets should come from disjoint historical
    windows. stop_at is a soft wall-clock cutoff, checked between signatures.
    """
    scenarios = tuple(scenarios)
    if not scenarios:
        return ()
    queries = tuple(queries)
    _validate_queries(queries, scenarios)
    eligible = [q for q in queries if any(s.eligible(q, dispatch_at) for s in scenarios)]
    bundles: set[Action] = set()
    for query in sorted(eligible, key=lambda q: (q.deadline, q.id))[:limits.candidate_queries]:
        counts = Counter(s.deficit(query) for s in scenarios if s.eligible(query, dispatch_at))
        counts.pop((), None)
        signatures = sorted(counts, key=lambda sig: (-counts[sig], len(sig), sig))
        for signature in signatures[:limits.signatures_per_query]:
            if clock() >= stop_at:
                return tuple(sorted(bundles))
            if len(signature) <= limits.max_signature_atoms:
                bundles.update(_signature_covers(signature, limits))
    return tuple(sorted(bundles))


def _validate_queries(queries: tuple[Query, ...], scenarios: tuple[Scenario, ...]):
    if len({q.id for q in queries}) != len(queries):
        raise ValueError("query IDs must be unique")
    for scenario in scenarios:
        if any(q.id not in scenario.cpu for q in queries):
            raise ValueError("every scenario must supply a CPU estimate for every query")


class MaskScorer:
    """Exact completion objective under frozen scenario curves."""

    def __init__(self, queries: Iterable[Query], reads: Iterable[Read],
                 scenarios: Iterable[Scenario], dispatch_at: float):
        queries, scenarios = tuple(queries), tuple(scenarios)
        if not scenarios:
            raise ValueError("at least one scoring scenario is required")
        _validate_queries(queries, scenarios)
        reads = tuple(sorted(set(reads)))
        self.count = len(scenarios)
        self.targets: list[tuple[int, float]] = []
        self.masks = {read: [] for read in reads}
        for scenario in scenarios:
            for query in queries:
                if not scenario.eligible(query, dispatch_at):
                    continue
                late = scenario.deficit(query)
                self.targets.append(((1 << len(late)) - 1, query.weight))
                for read in reads:
                    mask = sum(1 << i for i, atom in enumerate(late)
                               if scenario.timely(read, query, atom, dispatch_at))
                    self.masks[read].append(mask)

    def value(self, action: Iterable[Read]) -> float:
        union = [0] * len(self.targets)
        for read in set(action):
            for i, mask in enumerate(self.masks[read]):
                union[i] |= mask
        return math.fsum(weight for (target, weight), mask in zip(self.targets, union)
                         if mask & target == target) / self.count


@dataclass(frozen=True)
class Selection:
    reads: Action
    value: float
    waiting_value: float

    @property
    def gain(self) -> float:
        return self.value - self.waiting_value


def select(queries: Iterable[Query], bundles: Iterable[Action], scenarios: Iterable[Scenario],
           budget: Budget, dispatch_at: float, limits: Limits = Limits(), *,
           stop_at: float = math.inf,
           clock: Callable[[], float] = time.monotonic) -> Selection:
    """Search bundle unions, charging identical physical GETs exactly once.

    The incumbent includes waiting. A result is a proposal; dispatch must refresh
    scenarios and validate the full action against live state and budget.
    """
    bundles = tuple(sorted({tuple(sorted(set(b))) for b in bundles if b}))
    menu = tuple(sorted({read for bundle in bundles for read in bundle}))
    scorer = MaskScorer(queries, menu, scenarios, dispatch_at)
    waiting = scorer.value(())
    incumbent: Action = ()
    scores = {(): waiting}

    def rank(action):
        return (-scores[action], len(action), sum(r.size for r in action), action)

    frontier: list[Action] = [()]
    for _ in range(min(limits.max_gets, budget.gets, budget.slots)):
        expanded: set[Action] = set()
        for state in frontier:
            for bundle in bundles:
                if clock() >= stop_at:
                    return Selection(incumbent, scores[incumbent], waiting)
                action = tuple(sorted(set(state) | set(bundle)))
                if action == state or action in expanded:
                    continue
                if (len(action) > limits.max_gets or not budget.fits(action)
                        or any(r.size > limits.max_range_bytes for r in action)):
                    continue
                if action not in scores:
                    scores[action] = scorer.value(action)
                expanded.add(action)
                if rank(action) < rank(incumbent):
                    incumbent = action
        if not expanded:
            break
        frontier = sorted(expanded, key=rank)[:limits.beam_width]
    if scores[incumbent] <= waiting:
        incumbent = ()
    return Selection(incumbent, scores[incumbent], waiting)
