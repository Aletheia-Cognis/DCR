"""Atomic, expiring rescue credits; every wire attempt pays its full span."""

from __future__ import annotations

import math
import threading
import time
from dataclasses import dataclass
from fractions import Fraction
from typing import Callable, Iterable

from .model import Read


@dataclass(frozen=True)
class Budget:
    gets: int
    requested_bytes: int
    slots: int

    def __post_init__(self):
        if any(not isinstance(v, int) or v < 0 for v in vars(self).values()):
            raise ValueError("budget values must be nonnegative integers")

    def fits(self, reads: Iterable[Read]) -> bool:
        reads = set(reads)
        return (len(reads) <= min(self.gets, self.slots)
                and sum(r.size for r in reads) <= self.requested_bytes)


@dataclass
class _Attempt:
    read: Read
    window: int
    sent: bool = False


class BudgetLedger:
    """Original primaries create credits; retries and rescues never do.

    Window resets expire credits but keep outstanding slots occupied. Only an
    unsent reservation in the same window restores credits. A sent cancellation
    merely releases its slot. Admission IDs must identify original physical GETs.
    """

    def __init__(self, *, get_ratio: float = .25, byte_ratio: float = .15,
                 window_seconds: float = .1, max_outstanding: int = 4,
                 clock: Callable[[], float] = time.monotonic):
        if (not math.isfinite(window_seconds) or window_seconds <= 0
                or not isinstance(max_outstanding, int) or max_outstanding < 1):
            raise ValueError("window and outstanding limit must be positive")
        self.get_ratio, self.byte_ratio = Fraction(str(get_ratio)), Fraction(str(byte_ratio))
        if self.get_ratio < 0 or self.byte_ratio < 0:
            raise ValueError("credit ratios must be nonnegative")
        self.clock, self.window_seconds = clock, window_seconds
        self.max_outstanding = max_outstanding
        self._lock = threading.RLock()
        self._window: int | None = None
        self._get_credit = Fraction(0)
        self._byte_credit = Fraction(0)
        self._primaries: set[str] = set()
        self._attempts: dict[int, _Attempt] = {}
        self._next = 0

    def _advance(self):
        window = math.floor(self.clock() / self.window_seconds)
        if self._window is not None and window < self._window:
            raise ValueError("budget clock must be monotonic")
        if window != self._window:
            self._window = window
            self._get_credit = Fraction(0)
            self._byte_credit = Fraction(0)

    def admit_primary(self, admission_id: str, read: Read) -> bool:
        with self._lock:
            self._advance()
            if not admission_id:
                raise ValueError("a unique original-primary admission ID is required")
            if admission_id in self._primaries:
                return False
            self._primaries.add(admission_id)
            self._get_credit += self.get_ratio
            self._byte_credit += self.byte_ratio * read.size
            return True

    def available(self) -> Budget:
        with self._lock:
            self._advance()
            return Budget(int(self._get_credit), int(self._byte_credit),
                          self.max_outstanding - len(self._attempts))

    def reserve(self, reads: Iterable[Read]) -> tuple[int, ...]:
        """Reserve one complete action atomically or return no reservations."""
        reads = tuple(sorted(set(reads)))
        with self._lock:
            if not self.available().fits(reads):
                return ()
            tokens = []
            for read in reads:
                self._next += 1
                tokens.append(self._next)
                self._attempts[self._next] = _Attempt(read, self._window)
                self._get_credit -= 1
                self._byte_credit -= read.size
            return tuple(tokens)

    def mark_sent(self, token: int):
        with self._lock:
            attempt = self._attempts[token]
            if attempt.sent:
                raise ValueError("attempt already sent")
            attempt.sent = True

    def release(self, token: int):
        with self._lock:
            self._advance()
            attempt = self._attempts.pop(token)
            if not attempt.sent and attempt.window == self._window:
                self._get_credit += 1
                self._byte_credit += attempt.read.size
