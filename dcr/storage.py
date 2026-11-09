"""Version-pinned streaming range reads from local objects or S3."""

from __future__ import annotations

import asyncio
import hashlib
import os
import re
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import AsyncIterator, Protocol

from .model import Read


class IntegrityError(ValueError):
    """A response violates the immutable range contract."""


@dataclass(frozen=True)
class Chunk:
    read: Read
    version: str
    offset: int
    data: bytes


class Store(Protocol):
    def stream(self, read: Read, chunk_size: int = 65536) -> AsyncIterator[Chunk]: ...


class LocalStore:
    """Content-addressed immutable objects; no object keys become filesystem paths."""

    def __init__(self, root: str | Path):
        self.root = Path(root)

    def path(self, key: str, version: str) -> Path:
        digest = hashlib.sha256((key + "\0" + version).encode()).hexdigest()
        return self.root / "objects" / (digest + ".bin")

    def import_file(self, key: str, source: str | Path) -> str:
        if not key:
            raise ValueError("object key must be nonempty")
        directory = self.root / "objects"
        directory.mkdir(parents=True, exist_ok=True)
        handle, temp = tempfile.mkstemp(dir=directory, prefix=".import-")
        try:
            digest = hashlib.sha256()
            with os.fdopen(handle, "wb") as output, Path(source).open("rb") as stream:
                while chunk := stream.read(1024 * 1024):
                    digest.update(chunk)
                    output.write(chunk)
            version = digest.hexdigest()
            target = self.path(key, version)
            if not target.exists():
                os.replace(temp, target)
            return version
        finally:
            Path(temp).unlink(missing_ok=True)

    def put(self, key: str, data: bytes) -> str:
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "payload"
            source.write_bytes(data)
            return self.import_file(key, source)

    async def stream(self, read: Read, chunk_size: int = 65536) -> AsyncIterator[Chunk]:
        if chunk_size < 1:
            raise ValueError("chunk size must be positive")
        with self.path(read.key, read.version).open("rb") as stream:
            if read.end > os.fstat(stream.fileno()).st_size:
                raise IntegrityError("range exceeds object size")
            stream.seek(read.start)
            offset = read.start
            while offset < read.end:
                data = await asyncio.to_thread(stream.read, min(chunk_size, read.end - offset))
                if not data:
                    raise IntegrityError("truncated local range")
                yield Chunk(read, read.version, offset, data)
                offset += len(data)


class S3Store:
    """S3 adapter with one SDK attempt per accounted executor attempt.

    Authentication follows the standard SDK credential chain. Bucket versioning
    must be enabled. The caller supplies VersionId values, never a mutable alias.
    """

    def __init__(self, bucket: str, *, endpoint_url: str | None = None,
                 region_name: str | None = None):
        import boto3
        from botocore.config import Config

        self.bucket = bucket
        self.client = boto3.client(
            "s3", endpoint_url=endpoint_url, region_name=region_name,
            config=Config(retries={"total_max_attempts": 1, "mode": "standard"}),
        )

    @staticmethod
    def validate_response(read: Read, response: dict):
        if response.get("VersionId") != read.version:
            raise IntegrityError("S3 response version differs from the requested version")
        header = response.get("ContentRange", "")
        match = re.fullmatch(r"bytes (\d+)-(\d+)/(\d+)", header)
        if (not match or int(match[1]) != read.start or int(match[2]) != read.end - 1
                or int(match[3]) < read.end or response.get("ContentLength") != read.size
                or response.get("ResponseMetadata", {}).get("HTTPStatusCode") != 206):
            raise IntegrityError("invalid partial-content response")

    async def stream(self, read: Read, chunk_size: int = 65536) -> AsyncIterator[Chunk]:
        if chunk_size < 1:
            raise ValueError("chunk size must be positive")
        response = await asyncio.to_thread(
            self.client.get_object, Bucket=self.bucket, Key=read.key, VersionId=read.version,
            Range=f"bytes={read.start}-{read.end - 1}",
        )
        body = response["Body"]
        try:
            self.validate_response(read, response)
            offset = read.start
            while offset < read.end:
                data = await asyncio.to_thread(body.read, min(chunk_size, read.end - offset))
                if not data or len(data) > read.end - offset:
                    raise IntegrityError("truncated or oversized S3 range")
                yield Chunk(read, response["VersionId"], offset, data)
                offset += len(data)
        finally:
            await asyncio.to_thread(body.close)


def copy_object(store: LocalStore, read: Read, destination: str | Path):
    """Export an entire local object for a user-managed upload."""
    shutil.copyfile(store.path(read.key, read.version), destination)
