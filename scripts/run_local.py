"""Demonstrate one local primary read feeding several query assemblies."""

from __future__ import annotations

import argparse
import asyncio
import math
import tempfile
import time
from collections import Counter
from pathlib import Path

from _common import encode_read, write_report

from dcr import Query, Read
from dcr.executor import Executor
from dcr.storage import LocalStore


async def retrieve(chunk_size: int, timeout: float) -> dict:
    content = b"AAAABBBBCCCC"
    with tempfile.TemporaryDirectory(prefix="dcr-local-") as directory:
        store = LocalStore(directory)
        version = store.put("payload", content)
        atoms = tuple(Read("payload", version, start, start + 4) for start in (0, 4, 8))
        x, y, z = atoms
        deadline = time.monotonic() + timeout
        queries = (Query("q1", (x, y), deadline), Query("q2", (y, z), deadline),
                   Query("q3", (y,), deadline))
        primary = Read("payload", version, 0, len(content))
        events, outputs = [], []
        async with Executor(store, chunk_size=chunk_size, on_event=events.append) as executor:
            for query in queries:
                executor.admit(query)
            executor.start_primary(primary)
            for query in queries:
                payload = await executor.wait(query.id, timeout=timeout)
                for atom, data in payload.items():
                    if data != content[atom.start:atom.end]:
                        raise RuntimeError("assembled payload differs from the local object")
                outputs.append({
                    "query": query.id,
                    "atoms": [{"read": encode_read(atom), "text": data.decode("ascii")}
                              for atom, data in sorted(payload.items())],
                })
                executor.forget(query.id)
            await executor.drain()
            if executor.failures:
                failures = "; ".join(f"{pid}: {error}" for pid, error in executor.failures.items())
                raise RuntimeError(failures)
        return {
            "kind": "LOCAL_ASSEMBLY_DEMO",
            "primary_read": encode_read(primary),
            "chunk_size": chunk_size,
            "events": dict(Counter(event.kind for event in events)),
            "queries": outputs,
        }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--chunk-size", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=5.0, help="caller timeout in seconds")
    parser.add_argument("--output", type=Path, help="write JSON instead of stdout")
    args = parser.parse_args()
    if args.chunk_size < 1 or not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error("chunk size and timeout must be positive, with a finite timeout")
    try:
        write_report(asyncio.run(retrieve(args.chunk_size, args.timeout)), args.output)
    except (ValueError, OSError, RuntimeError, asyncio.TimeoutError) as error:
        parser.exit(2, f"run_local: {error}\n")


if __name__ == "__main__":
    main()

