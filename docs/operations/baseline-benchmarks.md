# Reproducible performance baseline

The `dodb-bench` package measures the current implementation without changing
production scheduling, storage, WAL, protocol, or cache policy. It emits one
JSON artifact containing the machine description, resource limits, dataset
context, latency samples, and layer-specific counters.

The checked-in runner builds with four Cargo jobs and runs the release binary
inside a user systemd scope. On the current Ryzen 5 5600 host the default
scope uses logical CPUs `0,1,6,7`, which are both SMT siblings of physical
cores 0 and 1. It applies `MemoryHigh=6G`, `MemoryMax=8G`, and
`MemorySwapMax=0`. The runner places temporary database files under `target`
by default so it does not consume the shared `/tmp` tmpfs.

Run it from the repository root:

```bash
scripts/run-baseline-bench.sh
```

The warmup and measurement windows can be changed without changing source:

```bash
DODB_BENCH_WARMUP_MS=150 DODB_BENCH_MEASURE_MS=700 \
  scripts/run-baseline-bench.sh
```

The matrix contains durable direct `BTreeStore` cases, `AsyncShard` cases,
in-process `LocalTenantService` cases, and real loopback QUIC cases. Primary
datasets use 4,096 keys and 64 B, 512 B, or 4 KiB values. A separate 256-key
dataset uses 64 KiB values. Point reads, bounded writes, the three requested
transaction shapes, range reads, mixed read/write workloads, and a hot-key
conflict workload are included. Durable write results retain normal WAL
`sync_data` behavior.

The JSON contains p50/p95/p99/max latency, bounded raw samples, throughput,
WAL bytes and sync timing, coordinator group-size percentiles and queue wait,
storage validation/preparation/publication timing, QUIC request counters, and
process/cgroup memory and I/O snapshots. It is intentionally not part of
normal tests.

Existing `crates/dodb-storage/src/bin/phase1-bench.rs` and
`phase4-bench.rs` remain useful focused diagnostic programs. They are not
replaced; this package provides the reproducible cross-layer artifact used for
baseline comparison.
