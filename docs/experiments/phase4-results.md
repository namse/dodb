# Phase 4 Results: Persistent Parallel Leaf Workers

## Scope

Phase 4 keeps logical admission and FIFO transaction ordering on one
coordinator, while eligible non-structural leaf work runs on persistent OS
workers. This diagnostic compares the `planned-blink` sparse serial executor
with `parallel-blink` using two workers on the 2-OCPU OCI target. The run uses
sync-disabled writes and is not a durability benchmark. No Phase 5 work was
started.

## Initial Scoped-Thread Attempt

The initial scoped-thread implementation measured effective parallelism
0.9824 and throughput ratio 0.954x. Per-group OS-thread creation and teardown
was identified as the execution problem. The worker implementation was changed
to a fixed pool whose threads live with `BlinkStore` and receive one command
per participating worker per group.

## Persistent Worker Pool

### Pool lifecycle

`BlinkStore` owns the configured worker count and pool. Enabling parallel
execution creates one persistent thread per worker, each with its own dedicated
`std::sync::mpsc` receiver. Group execution sends worker-bucket commands; pool
drop sends shutdown commands and joins all threads. The benchmark seeds the
store before enabling this pool.

### Deterministic job partition

Planner-ordered leaf jobs are assigned by `job_index % worker_count`. Each
participating worker receives one vector of jobs and processes that vector in
order. The coordinator collects every participating worker result before
continuing.

### Private leaf ownership

Each job owns a private clone of its existing target `BlinkPage`. Worker
threads receive only owned leaf jobs and do not access the store, sparse
working state, WAL, generation publisher, or batch metrics. Coordinator code
aggregates metrics and merges final leaf pages into the sparse working-state
overlay. No page latch or shared mutable page is used.

### FIFO commit and WAL assembly

The coordinator assigns final commit LSNs in transaction FIFO order before
dispatch. Workers apply `Revision::from(commit_lsn)` and encode a page image at
each transaction boundary. Worker completion order is discarded during
assembly: page results are indexed by FIFO position and the existing ordered
`ExecutedPlanTransaction` path builds WAL commits in logical FIFO order.

### Structural/overflow fallback

Single-leaf groups are skipped. Multi-leaf transactions, cross-leaf dependency
edges, overflow/allocator requirements, and leaf overflow requiring a split
discard speculative worker output and run the original plan through the sparse
serial executor. Structural split metrics are changed only by that serial
execution. This initial private-page design has no shared page latch and no
worker dependency waiting; cross-leaf dependencies use group fallback.

### WAL-before-install atomicity

Workers prepare private candidates only. The coordinator joins all workers,
validates and assembles the plan, appends and syncs WAL, then installs the
sparse `BlinkStateDelta` and publishes the generation. A worker never appends
WAL or changes committed state.

## Correctness

The implementation passed `cargo fmt --all -- --check`,
`cargo test -p dodb-storage`, `cargo test -p dodb-storage --bin phase0-bench`,
`cargo test --workspace`, and `git diff --check`. The Phase 4 tests remain in
place, including FIFO WAL assembly, fallback behavior, generation atomicity,
WAL failure atomicity, randomized differential behavior, and a focused test
that verifies persistent worker threads are reused across execute cycles.

## Local Smoke

The delay-zero one-repetition development smoke passed its gate:

| engine | mutation ops/s | relative |
| --- | ---: | ---: |
| planned | 17,769.77 | 1.000x |
| parallel | 17,093.47 | 0.962x |

Effective worker parallelism was 1.292. The parallel executor completed 1,733
groups and skipped 429 single-leaf groups, for coverage of approximately
80.2%. Multi-leaf, dependency, overflow, and structural fallback counts were
all zero. Errors and overloads were zero, and full-state clones and clone time
were zero. This is a development smoke, not an OCI performance conclusion.

## OCI Diagnostic

The OCI diagnostic used three repetitions per engine, 16 writers, width 1,
`different-leaf-heavy`, working set 100,000, cache 4,096, 16-byte keys,
64-byte values, group limit 64, 4 MiB group cap, queue 256, collection delay
0 us, sync disabled, two Tokio workers, 1 s warmup, 2 s duration, and seed
`0x3a042026`.

| engine | mut/s median | min..max | relative |
| --- | ---: | ---: | ---: |
| planned-blink | 7,818.20 | 7,678.95..7,870.21 | 1.000x |
| parallel-blink | 6,717.26 | 6,689.50..6,755.83 | 0.859x |

The parallel speedup was 0.859x.

## OCI Parallel Metrics

| metric | value |
| --- | ---: |
| effective parallelism, repetition 0 | 0.976 |
| effective parallelism, repetition 1 | 1.092 |
| effective parallelism, repetition 2 | 1.075 |
| effective parallelism, median | 1.075 |
| parallel group coverage | 3,531 / 4,361 = 81.0% |
| leaf jobs / parallel group | 11.20 |
| worker dispatches / parallel group | 2.00 |
| multi-leaf fallback | 0 |
| dependency fallback | 0 |
| overflow fallback | 0 |
| structural fallback | 0 |
| single-leaf skips | 830 |

## Interpretation

The OCI result is a **mechanism failure** under the overlap criterion because
median effective parallelism was below 1.20. It is also a **regression** in
throughput because parallel median throughput was 0.859x the serial planned
median, below 0.95. Coverage exceeded 80%, so low coverage does not explain
the result. No worker-pool redesign or additional benchmark matrix was started.

The local smoke and OCI result agree in throughput direction: parallel was
slower in both. Local smoke showed useful overlap (1.292), while OCI did not
meet the overlap threshold (1.075 median), so the overlap result does not
replicate across targets.

## Remaining Serial Costs

Median cumulative timings from the parallel OCI records:

| component | median |
| --- | ---: |
| planning | 178.65 ms |
| physical execution | 309.61 ms |
| WAL assembly | 26.37 ms |
| WAL append | 459.11 ms |
| dirty tracking | 14.46 ms |
| catalog construction | 412.78 ms |
| generation publication | 448.73 ms |
| parallel worker busy time, summed | 208.37 ms |
| dispatch-to-final-result wall time | 193.59 ms |

These are the existing cumulative benchmark attributions for each repetition;
they are not per-request timings or exclusive CPU profiles.

## Phase 5 Readiness

**NO.** Correctness, FIFO/WAL ordering, sparse-state behavior, and
WAL-before-install atomicity passed. However, OCI effective parallelism was
1.075, below 1.20, and throughput speedup was 0.859x, below 0.95. Review the
worker mechanism before Phase 5 design proceeds. Phase 5 implementation,
concurrent splits, SMO coordination, and page latches were not started.

## Artifacts

- Planned control:
  [`planned-pool-different-width1-nosync.jsonl`](results/oci-a1-2ocpu-12g-200g/phase4/planned-pool-different-width1-nosync.jsonl)
  SHA256 `6b6bdc8044ed4713e8c7b0cf9d4c8cd2b7510c4afc83dce0bc416c3580939919`
- Parallel run:
  [`parallel-pool-different-width1-nosync.jsonl`](results/oci-a1-2ocpu-12g-200g/phase4/parallel-pool-different-width1-nosync.jsonl)
  SHA256 `4bd3b0a9929de7c47df4d40491147f6430e010340300944b6ee4e630ca7bf50d`

## OCI Artifact Preservation

Before fast-forward, the three pre-existing Phase 3.1 artifacts matched their
expected SHA256 values. They were moved unchanged to
`/home/opc/dodb-oci-artifacts-pre-phase4-0dc5219/phase3/`. After fast-forward,
the tracked copies matched both the expected SHA256 values and the preserved
backup copies:

| artifact | SHA256 |
| --- | --- |
| `planned-sparse-nosync-w16-width16-ws100k.jsonl` | `24c0426b0768e74fb8e44791c82dc3414a95d52503720268dd1d494ba4a01d54` |
| `planned-sparse-nosync-w16-width16-ws4096.jsonl` | `fd0c57281c3090c1ba83887364238f6f8286d78b39dbddb36f84f76a7818a3b3` |
| `planned-sparse-real-w16-width16-ws100k.jsonl` | `ecd444dc25c5f409ba16f39b03c7efb2ce8bff3fc3f9570e0355cdece4a14317` |
