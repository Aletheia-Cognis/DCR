"""JSON reporting helpers for the source-release scripts."""

from __future__ import annotations

import json
from pathlib import Path

from dcr.model import Read


def encode_read(read: Read) -> dict:
    return {"key": read.key, "version": read.version,
            "start": read.start, "end": read.end}


def write_report(report: dict, output: Path | None) -> None:
    text = json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n"
    if output is None:
        print(text, end="")
    else:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(text, encoding="utf-8")

