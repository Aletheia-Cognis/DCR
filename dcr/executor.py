"""Progressive query-local assembly and separately identified physical producers."""

from __future__ import annotations

import asyncio
import itertools
import time
from dataclasses import dataclass, field
from typing import Callable, Iterable

from .budget import BudgetLedger
from .model import Query, Read
from .storage import Chunk, IntegrityError, Store


@dataclass
class _Buffer:
    data: bytearray
    coverage: list[tuple[int, int]] = field(default_factory=list)

    def add(self, start: int, data: bytes):
        end = start + len(data)
        for left, right in self.coverage:
            begin, finish = max(left, start), min(right, end)
            if begin < finish and self.data[begin:finish] != data[begin - start:finish - start]:
                raise IntegrityError("conflicting bytes from matching-version producers")
        self.data[start:end] = data
        merged = []
        for left, right in sorted(self.coverage + [(start, end)]):
            if merged and left <= merged[-1][1]:
                merged[-1] = (merged[-1][0], max(merged[-1][1], right))
            else:
                merged.append((left, right))
        self.coverage = merged

    @property
    def complete(self):
        return self.coverage == [(0, len(self.data))]


class Assembly:
    """Deliver exactly one complete byte string for every fixed-plan atom."""

    def __init__(self, query: Query):
        self.query = query
        self.buffers = {atom: _Buffer(bytearray(atom.size)) for atom in query.atoms}
        self.arrivals: dict[Read, float] = {}

    def feed(self, chunk: Chunk, now: float):
        read = chunk.read
        if (chunk.version != read.version or not chunk.data or chunk.offset < read.start
                or chunk.offset + len(chunk.data) > read.end):
            raise IntegrityError("invalid chunk version or bounds")
        for atom, buffer in self.buffers.items():
            if (atom.key, atom.version) != (read.key, read.version):
                continue
            left = max(atom.start, chunk.offset)
            right = min(atom.end, chunk.offset + len(chunk.data))
            if left < right:
                buffer.add(left - atom.start, chunk.data[left - chunk.offset:right - chunk.offset])
                if buffer.complete:
                    self.arrivals.setdefault(atom, now)

    @property
    def complete(self) -> bool:
        return len(self.arrivals) == len(self.buffers)

    def payload(self) -> dict[Read, bytes]:
        if not self.complete:
            raise RuntimeError("query still has missing bytes")
        return {atom: bytes(buffer.data) for atom, buffer in self.buffers.items()}


@dataclass(frozen=True)
class ProducerEvent:
    producer_id: str
    kind: str
    read: Read
    at: float
    offset: int
    size: int = 0
    error: str | None = None


class Executor:
    """Event-loop-owned executor; query release never cancels a wider primary.

    There is no response replay cache: a late consumer receives future chunks,
    but must retain its own primary path for an already-emitted prefix.
    """

    def __init__(self, store: Store, budget: BudgetLedger | None = None, *,
                 max_buffer_bytes: int = 64 * 1024 * 1024, chunk_size: int = 65536,
                 clock: Callable[[], float] = time.monotonic,
                 on_event: Callable[[ProducerEvent], None] | None = None):
        if max_buffer_bytes < 1 or chunk_size < 1:
            raise ValueError("buffer and chunk limits must be positive")
        self.store, self.clock = store, clock
        self.budget = budget if budget is not None else BudgetLedger(clock=clock)
        self.max_buffer_bytes, self.chunk_size = max_buffer_bytes, chunk_size
        self.on_event = on_event
        self.assemblies: dict[str, Assembly] = {}
        self.futures: dict[str, asyncio.Future] = {}
        self.tasks: dict[str, asyncio.Task] = {}
        self.failures: dict[str, Exception] = {}
        self._reservations: dict[str, int] = {}
        self._ids = itertools.count(1)
        self._buffer_bytes = 0

    def admit(self, query: Query) -> asyncio.Future:
        if query.id in self.futures:
            raise ValueError("duplicate query ID")
        size = sum(atom.size for atom in query.atoms)
        if self._buffer_bytes + size > self.max_buffer_bytes:
            raise BufferError("query assembly admission exceeds the buffer limit")
        future = asyncio.get_running_loop().create_future()
        self.assemblies[query.id] = Assembly(query)
        self.futures[query.id] = future
        self._buffer_bytes += size
        return future

    def _emit(self, producer_id, kind, read, offset, size=0, error=None):
        if self.on_event is not None:
            self.on_event(ProducerEvent(producer_id, kind, read, self.clock(), offset, size, error))

    def start_primary(self, read: Read, *, original: bool = True) -> str:
        """Use original=False for primary retries, which create no new credits."""
        asyncio.get_running_loop()
        producer_id = f"primary-{next(self._ids)}"
        if original:
            self.budget.admit_primary(producer_id, read)
        self.tasks[producer_id] = asyncio.create_task(self._run(producer_id, read))
        return producer_id

    def dispatch_rescue(self, reads: Iterable[Read], *,
                        validate: Callable[[tuple[Read, ...], float], bool]) -> tuple[str, ...]:
        """Revalidate the complete action synchronously, then reserve before send.

        validate must refresh arrival estimates, version/attachment eligibility,
        live consumers and positive gain at the actual dispatch time. It must
        not block or await. A retry calls this method again and pays again.
        """
        asyncio.get_running_loop()
        reads = tuple(sorted(set(reads)))
        if not reads or not validate(reads, self.clock()):
            return ()
        tokens = self.budget.reserve(reads)
        if len(tokens) != len(reads):
            return ()
        ids = []
        for read, token in zip(reads, tokens):
            producer_id = f"rescue-{next(self._ids)}"
            self.budget.mark_sent(token)
            self._reservations[producer_id] = token
            task = asyncio.create_task(self._run(producer_id, read))
            task.add_done_callback(
                lambda _task, producer_id=producer_id: self._release_reservation(producer_id))
            self.tasks[producer_id] = task
            ids.append(producer_id)
        return tuple(ids)

    def _release_reservation(self, producer_id: str):
        token = self._reservations.pop(producer_id, None)
        if token is not None:
            self.budget.release(token)

    async def _run(self, producer_id: str, read: Read):
        offset = read.start
        try:
            self._emit(producer_id, "dispatch", read, offset)
            async for chunk in self.store.stream(read, self.chunk_size):
                if (chunk.read != read or chunk.offset != offset or chunk.version != read.version
                        or not chunk.data or chunk.offset + len(chunk.data) > read.end):
                    raise IntegrityError("producer returned a mismatched or noncontiguous range")
                now = self.clock()
                for query_id, assembly in tuple(self.assemblies.items()):
                    if self.futures[query_id].cancelled():
                        self._drop_assembly(query_id)
                        continue
                    assembly.feed(chunk, now)
                    if assembly.complete:
                        self.futures[query_id].set_result(assembly.payload())
                        self._drop_assembly(query_id)
                offset += len(chunk.data)
                self._emit(producer_id, "chunk", read, chunk.offset, len(chunk.data))
            if offset != read.end:
                raise IntegrityError("producer ended before the range was complete")
            self._emit(producer_id, "complete", read, offset)
        except asyncio.CancelledError:
            self._emit(producer_id, "cancelled", read, offset)
            raise
        except Exception as error:
            self.failures[producer_id] = error
            self._emit(producer_id, "failed", read, offset, error=str(error))
        finally:
            self._release_reservation(producer_id)

    def _drop_assembly(self, query_id):
        assembly = self.assemblies.pop(query_id)
        self._buffer_bytes -= sum(atom.size for atom in assembly.query.atoms)

    async def wait(self, query_id: str, timeout: float | None = None) -> dict[Read, bytes]:
        """Wait for bytes. CPU scoring must run afterwards and is part of the deadline."""
        return await asyncio.wait_for(asyncio.shield(self.futures[query_id]), timeout)

    def forget(self, query_id: str):
        """Release a completed or abandoned query's references and payload future."""
        future = self.futures.pop(query_id)
        if not future.done():
            future.cancel()
        if query_id in self.assemblies:
            self._drop_assembly(query_id)

    async def drain(self):
        """Wait for producers, including primaries whose urgent consumers finished."""
        tasks = tuple(self.tasks.values())
        await asyncio.gather(*tasks, return_exceptions=True)
        for producer_id, task in self.tasks.items():
            if task.done():
                self._release_reservation(producer_id)
        self.tasks = {key: task for key, task in self.tasks.items() if not task.done()}

    async def close(self):
        """Explicit shutdown or caller timeout; sent reservations are never refunded."""
        for query_id in tuple(self.futures):
            self.forget(query_id)
        for task in self.tasks.values():
            task.cancel()
        await self.drain()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        await self.close()
