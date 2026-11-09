# Method and integration notes

## Modules and data flow

| Component | Main interface | Responsibility |
| --- | --- | --- |
| Query model | `Read`, `Query`, `Scenario`, `atomize` | Immutable object/version intervals, query dependencies, and consumer-specific arrival scenarios |
| Arrival projection | `ProgressCurve`, `ExistingProducer`, `project_window`, `split_windows` | Project supplied historical curves and form disjoint window cohorts |
| Candidate selection | `generate_bundles`, `MaskScorer`, `select` | Generate interval covers, evaluate query completion, and search bundle unions |
| Resource accounting | `Budget`, `BudgetLedger` | GET/byte credit windows and atomic action reservations |
| Online control | `Controller`, `ScenarioProvider` | Coordinate generation, scoring, and live validation within a soft allowance |
| Execution | `Executor`, `Assembly`, `ProducerEvent` | Start producers, assemble exact bytes, and expose progress events |
| Storage | `LocalStore`, `S3Store`, `Chunk` | Stream version-pinned local or S3 byte ranges |

The application supplies fixed query demands and primary ranges. Historical
windows feed the scenario provider; the controller uses those scenarios to
select additional reads. The executor delivers chunks to query assemblies,
which produce payloads once all required bytes are present.

## Fixed demands and completion objective

A `Read` uses a half-open interval `[start, end)` on one immutable object version.
`atomize` partitions demand endpoints independently for each object/version pair.
Queries retain the same required atoms throughout execution.

For each scenario, the completion model is:

```text
Cq(X) = max over required atoms x [min(baseline(q,x), selected arrivals(q,x))] + CPUq
V(X)  = average over scenarios [sum of weights of deadline-feasible queries]
```

All times are absolute values on one monotonic clock. Missing producer arrivals
mean unavailable coverage; each scenario must supply a CPU estimate for every
query. Consumer eligibility and final-byte arrival determine timely coverage.

## Bundle generation and selection

Generation scenarios identify late-atom signatures. An interval dynamic program
retains a minimum-span cover for each request count within an object; request
counts are combined across objects. Beam search then evaluates unions of these
bundles across consumers, including zero-gain intermediate states.

Identical physical read keys are charged once within an action. Unequal
overlapping ranges retain separate span charges. Ranking prefers greater
completion value, then fewer GETs, fewer requested bytes, and stable read keys.
A dispatched action must improve over waiting. Minimum-span generation and beam
pruning make selection approximate.

Default limits are four GETs, beam width four, eight candidate queries, two
signatures per query, and sixteen atoms per signature. The controller accepts
at most 64 active queries and uses a soft 2 ms allowance.

## Scenarios and online control

`ScenarioProvider` is an application-supplied callable. Its `purpose` is
`generation`, `scoring`, or `validation`. Generation and scoring cohorts must be
disjoint; validation reuses the scoring cohort while refreshing dispatch and
progress. Empty cohorts cause optional rescue to be skipped.

`project_window` converts supplied cumulative-byte curves into conservative
final-byte arrivals. Existing producers include their dispatch time, delivered
prefix, and attached consumers; already completed atoms are supplied separately.
`split_windows` samples disjoint whole windows after the application matches
them to the current conditions. Collection and matching are integration work.

Immediately before dispatch, the controller refreshes the whole action's gain
against current consumers. The executor then reserves credits atomically.
Provider calls are synchronous and must remain bounded. The allowance is soft;
it does not guarantee a real-time decision or storage completion deadline.

## Execution workflow

1. Construct fixed query atoms and primary reads using immutable versions.
2. Create a store, an executor, and a controller using one monotonic clock.
3. Admit queries with `Executor.admit` and issue their primary reads with
   `start_primary`.
4. Call `Controller.rescue` on application-owned progress events or timer ticks,
   supplying the scenario provider.
5. Consume complete payloads with `Executor.wait`, perform the fixed scoring
   computation, then release query references with `forget`.
6. Use `drain` to wait for remaining producers; `close` performs caller shutdown.

Every query keeps an eventual primary path. Successful rescue does not
automatically cancel a wider primary needed by other consumers. `wait` returns
bytes before local scoring; its caller-controlled timeout is separate from the
scheduling deadline. Producer failures are recorded in `executor.failures`;
applications decide retries and abandonment.

## Budget and byte-delivery contracts

Original primary admissions create GET and requested-byte credits, with default
ratios of 25% and 15% in 100 ms windows. Credits expire at window boundaries;
outstanding rescue slots persist. Every rescue attempt, including a retry,
reserves its full requested span. A sent attempt keeps its traffic charge after
failure or cancellation. Primary retries use `start_primary(read, original=False)`.

Assembly checks immutable versions and byte bounds. Identical overlaps are
idempotent and conflicting bytes are rejected. Active query assembly buffers
are admission-limited, with a default 64 MiB cap. Shared query demands have
separate query buffers. There is no response replay cache: a late consumer still
needs a producer for any prefix it missed. Completed payload futures and
caller-held results have separate memory ownership.

## Storage adapters

`LocalStore` imports files into immutable content-addressed objects. Treat those
files as immutable after import. `S3Store` requires versioned objects and their
actual non-null S3 `VersionId` values. A local SHA-256 identity is not an S3
VersionId; uploads and identity mapping belong to the application.

S3 credentials follow the SDK credential chain. The adapter validates response
version, partial-content status, range, and length. SDK retries are disabled so
each attempt can be accounted for explicitly. The optional `s3` dependency
installs the SDK; local execution uses the standard library.
