# Script guide

Install the package with `python -m pip install -e .` and run the following
commands from the repository root. All three entry points use the standard
library and included core modules. `_common.py` supplies JSON reporting helpers.
The S3 extra is unnecessary for these scripts.

## Inspect a selection decision

```bash
python scripts/select_action.py
python scripts/select_action.py --gets 1 --bytes 4 --slots 1
python scripts/select_action.py --beam-width 4 --output outputs/selection.json
```

The script constructs three analytical query dependencies over reads x/y/z.
Their fixed baseline arrivals are 20/25/7 ms, and a new eligible read arrives at
8 ms. The first query requires x and y, so its completion depends on a compound
action. These specified arrivals illustrate selection; the script performs no
storage I/O and measures no storage latency.

The JSON report contains generated bundles, individual-read scores, the selected
physical reads, GET/byte costs, waiting value, selected value, and gain. Resource
flags change the budget; `--beam-width` changes the search frontier width.
Decision values count weighted timely queries in the supplied analytical model.

## Assemble local query payloads

```bash
python scripts/run_local.py
python scripts/run_local.py --chunk-size 2 --timeout 5 --output outputs/local.json
```

The script creates a temporary object containing `AAAABBBBCCCC`, admits three
queries with overlapping demands, and issues one primary read. It assembles the
queries' bytes through the executor, checks those bytes against the local object,
and reports the payloads and producer-event counts. Temporary objects are removed
on exit. This demonstrates local primary sharing and assembly; it does not issue
rescue reads or report deadline gains.

## Project a supplied history window

```bash
python scripts/project_history.py configs/history.json
python scripts/project_history.py configs/history.json --output outputs/projection.json
```

The configuration is an illustrative input, with named reads, query demands,
existing producer progress, candidate curves, completed atoms, and CPU estimates.
It contains no collected storage measurements. Replace it with a description of
one already matched historical window when integrating with real telemetry.

| Field | Meaning |
| --- | --- |
| `reads` | Named immutable read definitions: key, version, start, end |
| `queries` | Query IDs, atom names, absolute deadlines, and optional weights |
| `now`, `dispatch_at` | Observation time and proposed dispatch time on one monotonic clock |
| `cpu` | Nonnegative CPU estimate in seconds for every query |
| `existing` | Read name, dispatch time, delivered byte count, attached consumers, and historical curve |
| `completed` | Query ID, atom name, and observed final-byte arrival |
| `candidates` | New read names mapped to cumulative-byte curves |

Each curve is an array of `[seconds_since_dispatch, cumulative_bytes]` pairs.
Other timestamps are absolute values on the shared monotonic clock. The report
contains baseline and candidate rescue arrivals per query/atom. An arrival of
`null` represents unavailable coverage; absent entries are also unavailable.
This command projects one window and leaves historical matching, cohort splitting,
and the concrete online provider to the integrating application.

## Reports and scope

Without `--output`, each script writes JSON to stdout. With it, parent directories
are created and the selected output file is replaced. No generated reports are
bundled as measured results. These scripts illustrate the partial Python release;
the paper's performance experiments require the separate evaluation system.
