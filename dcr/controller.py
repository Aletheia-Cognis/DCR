"""One bounded control epoch with live whole-action validation before dispatch."""

from __future__ import annotations

import time
from typing import Callable, Protocol

from .executor import Executor
from .model import Query, Read, Scenario
from .selector import Limits, MaskScorer, Selection, generate_bundles, select


class ScenarioProvider(Protocol):
    def __call__(self, queries: tuple[Query, ...], reads: tuple[Read, ...], *,
                 now: float, dispatch_at: float, purpose: str) -> tuple[Scenario, ...]:
        """Supply matched historical windows, or () when calibration is insufficient.

        purpose is generation, scoring or validation. Validation must reuse the
        scoring cohort while refreshing dispatch/progress. Generation and scoring
        cohorts must be disjoint. Calls are synchronous and must remain bounded.
        """
        ...


class Controller:
    def __init__(self, limits: Limits = Limits(), *, allowance_seconds: float = .002,
                 max_queries: int = 64, clock: Callable[[], float] = time.monotonic):
        if allowance_seconds <= 0 or max_queries < 1:
            raise ValueError("controller allowance and query cap must be positive")
        self.limits, self.allowance = limits, allowance_seconds
        self.max_queries, self.clock = max_queries, clock
        self.last_selection: Selection | None = None

    def rescue(self, executor: Executor, provider: ScenarioProvider) -> tuple[str, ...]:
        """Run after progress or a coalesced timer tick; never alters primary plans."""
        self.last_selection = None
        now = self.clock()
        stop_at = now + self.allowance
        queries = tuple(assembly.query for assembly in executor.assemblies.values())
        if not queries or len(queries) > self.max_queries:
            return ()
        budget = executor.budget.available()
        if min(budget.gets, budget.slots, budget.requested_bytes) == 0:
            return ()
        generation = provider(queries, (), now=now, dispatch_at=stop_at, purpose="generation")
        if not generation or self.clock() >= stop_at:
            return ()
        bundles = generate_bundles(queries, generation, stop_at, self.limits,
                                   stop_at=stop_at, clock=self.clock)
        if not bundles or self.clock() >= stop_at:
            return ()
        menu = tuple(sorted({read for bundle in bundles for read in bundle}))
        scenarios = provider(queries, menu, now=self.clock(),
                             dispatch_at=stop_at, purpose="scoring")
        if not scenarios or self.clock() >= stop_at:
            return ()
        proposal = select(queries, bundles, scenarios, budget, stop_at, self.limits,
                          stop_at=stop_at, clock=self.clock)
        self.last_selection = proposal
        if not proposal.reads or self.clock() >= stop_at:
            return ()

        def validate(action, dispatched_at):
            if dispatched_at >= stop_at:
                return False
            current = tuple(assembly.query for assembly in executor.assemblies.values())
            if not current:
                return False
            refreshed = provider(current, action, now=dispatched_at,
                                 dispatch_at=dispatched_at, purpose="validation")
            if not refreshed:
                return False
            scorer = MaskScorer(current, action, refreshed, dispatched_at)
            improves = scorer.value(action) > scorer.value(())
            return improves and self.clock() < stop_at

        return executor.dispatch_rescue(proposal.reads, validate=validate)
