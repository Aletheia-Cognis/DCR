# DCR

**Completion-Closed Rescue for Concurrent Vector Retrieval on Object Storage**

This repository contains selected core Python code for DCR: late-signature bundle
generation, completion-aware joint selection, shared physical GET accounting,
online control, and progressive range-read execution. It provides the method's
main scheduling and execution components as a Python reference.

## Code structure

```text
.
├── dcr/
│   ├── __init__.py   # public scheduling interface
│   ├── model.py      # immutable ranges, query demands and arrival scenarios
│   ├── selector.py   # interval covers, completion masks and bounded beam search
│   ├── budget.py     # GET/byte credits and atomic resource reservations
│   ├── curves.py     # progress-curve projection and disjoint window sampling
│   ├── controller.py # bounded selection and live action validation
│   ├── executor.py   # asynchronous producers and progressive byte assembly
│   └── storage.py    # local and S3 streaming range-read adapters
├── scripts/
│   ├── select_action.py   # inspect analytical bundle selection
│   ├── run_local.py       # assemble overlapping demands from a local object
│   ├── project_history.py # project a supplied historical window
│   └── _common.py         # shared JSON reporting helpers
├── configs/
│   └── history.json       # illustrative progress-window input
├── docs/
│   ├── method.md     # method contracts and integration workflow
│   └── scripts.md    # script commands, inputs and report fields
├── pyproject.toml
├── MANIFEST.in
├── README.md
└── LICENSE
```

## Installation

Python 3.10 or newer is required. The scheduling components and local storage
adapter use the standard library. From the repository root:

```bash
python -m pip install -e .
```

For the optional S3 adapter:

```bash
python -m pip install -e ".[s3]"
```

## Scripts

After installation, from the repository root:

```bash
python scripts/select_action.py --output outputs/selection.json
python scripts/run_local.py --output outputs/local.json
python scripts/project_history.py configs/history.json --output outputs/projection.json
```

These entry points call the included selection, projection, and execution modules.
The selector uses specified analytical arrivals; the local script reads temporary
objects; the history configuration illustrates a caller-supplied window. Each
command writes a JSON report. See the [script guide](docs/scripts.md) for arguments
and input fields. These demonstrations do not reproduce storage performance tables.

## Scheduling interface

```python
from dcr import Budget, BudgetLedger, Limits, Query, Read, Scenario
from dcr import atomize, generate_bundles, select
```

The caller supplies fixed query demands and arrival scenarios on one monotonic
clock. `generate_bundles` builds candidate covers from generation scenarios;
`select` jointly scores their unions under GET, requested-byte and slot limits.
Generation and scoring scenarios should use disjoint historical windows. The
returned action is a proposal requiring live validation and budget reservation
before dispatch; `BudgetLedger` provides the reservation interface.

## Controller and execution

```python
from dcr.controller import Controller, ScenarioProvider
from dcr.curves import ExistingProducer, ProgressCurve, project_window, split_windows
from dcr.executor import Executor
from dcr.storage import LocalStore, S3Store
```

`Executor` admits query demands, starts primary reads, and assembles incoming
chunks. `Controller.rescue` invokes an application-supplied `ScenarioProvider`,
generates and scores candidate bundles, and refreshes the full action before
dispatch. The application calls it on progress events or timer ticks.

`curves.py` projects caller-supplied historical windows into arrival scenarios
and samples disjoint generation/scoring cohorts. `LocalStore` uses immutable
local objects; `S3Store` issues version-pinned range reads and validates response
metadata. Query deadlines, observations, and predictions use the same monotonic
clock. See [method and integration notes](docs/method.md) for the contracts and
module responsibilities.

## Release scope

This is a partial source release covering the core scheduling and byte-delivery
mechanisms. Workload-specific ANN plans, vector packing/scoring utilities,
historical-trace collection and matching, and full performance harnesses are outside
its scope.

## License

[MIT License](LICENSE).
