# Phase 3 OCI Results

## Environment

- OCI A1, 2 OCPU, 12 GiB shape RAM, ARM64 Ampere; Linux reported two logical CPUs and Neoverse-N1. Guest `free` reported approximately 10 GiB.
- Oracle Linux Server 9.8; kernel `6.12.0-206.104.4.4.el9uek.aarch64`; Rust/Cargo 1.98.1.
- `/dev/sda` was reported as a 200 GB block device, but it was not mounted as a 200 GB filesystem; `/dev/sda3` was approximately 44.5 GB. Benchmark files were on `/home/opc/dodb`, the root XFS filesystem on the root LVM logical volume, with `rw,relatime`; that filesystem was approximately 30 GB. No format, resize, or storage tuning was performed.
- Benchmark DB/WAL temporary path: `/home/opc/dodb/target/oci-a1-2ocpu-12g-200g/phase3-tmp`. It was on the root XFS filesystem, not a separate 200 GB mounted data filesystem.
- HEAD recorded by the initial Phase 3 partial artifacts: `aa00444348d074e081e0dbe5d0979032271bf014`.

## Scope

- Deliberately partial diagnostic benchmark; full matrix intentionally stopped because per-repetition database seeding/setup cost dominated elapsed time. Existing partial artifacts are preserved. No engine adoption decision is made.
- Existing real-sync runs use 5 repetitions for `main-btree` and 3 for `planned-blink`; workload settings match, but derived per-scenario seeds differ because each engine's scenario enumeration differs. Treat ratios as diagnostic, not a controlled paired estimate.
- Two additional runs used release mode, Tokio workers 2, 100,000-row working set, cache 4,096, 16-byte keys, 64-byte values, 1 s warmup, 2 s measurement, 3 repetitions, 16 writers, width 16, uniform distribution, and seed base `0x3a032026`. Both used `--sync-mode injected --sync-delay 0us`.

## Existing Real-Sync Write Results

Throughput is logical transactions/s. Latency columns are planned-blink medians over its three repetitions.

| workload | main median | planned median | ratio | planned p50/p95/p99 |
|---|---:|---:|---:|---:|
| 16 writers, width 1, uniform | 4,301.5 | 456.8 | 0.106 | 26.106 / 51.841 / 54.316 ms |
| 16 writers, width 16, uniform | 614.1 | 340.3 | 0.554 | 43.408 / 84.456 / 87.627 ms |
| 64 writers, width 1, uniform | 6,242.5 | 1,854.0 | 0.297 | 32.586 / 56.121 / 64.085 ms |

At width 16, mutation throughput medians were 9,826 ops/s for main and 5,445 ops/s for planned. The preserved core artifacts also contain same-leaf-heavy and different-leaf-heavy points; no additional real-sync runs were made.

## Injected Sync=0 Diagnostic

| engine | tx/s median | mutation ops/s median | min..max mutation ops/s |
|---|---:|---:|---:|
| main-btree | 590.4 | 9,445.8 | 9,258.9..9,450.9 |
| planned-blink | 337.2 | 5,394.7 | 5,329.1..5,465.6 |

Planned/main mutation throughput ratio: **0.571**. Planned latency median was p50/p95/p99 **43.408 / 84.456 / 87.627 ms**, versus main **26.790 / 29.838 / 30.847 ms**.

This is not a true filesystem-sync-disabled control. In the current harness, `BenchFile::sync_data()` applies the configured delay and then always calls the underlying file's `sync_data()`; a zero injected delay removes only the extra sleep. Consequently these measurements do not isolate engine/tree/planner/publication cost from real filesystem durability cost. The configured-mode number meets the below-70% threshold numerically, but it cannot establish Case A's causal conclusion.

## True Sync-Disabled Diagnostic

### Harness correction

- The prior `injected, 0us` runs retained physical filesystem sync. The benchmark-only `disabled` mode now makes `BenchFile::sync_data()` and `sync_all()` return `Ok(())` without sleeping or invoking `ProductionFile` sync methods. Both data-file and WAL `BenchFile` adapters use this mode.
- Writes and WAL encoding/buffered write syscalls are still performed. These are no-fsync CPU/engine diagnostics, not production durability benchmarks.
- All four artifacts use harness commit `2deea52ccc43639b1824d93fda840eab87e05510`; main and planned repetition seeds match. Each run set has three records with zero errors and overloads.
- The measured files remained on the root XFS filesystem on the root LVM volume. `/dev/sda` was a 200 GB block device but no separate 200 GB filesystem was mounted. This result does not represent performance on the intended 200 GB production filesystem.

### 100k working set

Throughput and latency values are medians across three repetitions. Throughput spread is min..max.

| engine | mutation ops/s median | min..max | tx/s median | p50 | p95 | p99 |
|---|---:|---:|---:|---:|---:|---:|
| main-btree | 11,467.8 | 10,798.4..11,568.0 | 716.7 | 22.345 ms | 25.473 ms | 26.030 ms |
| planned-blink | 6,074.8 | 5,664.9..6,214.3 | 379.7 | 38.791 ms | 73.284 ms | 78.554 ms |

`R100K = planned/main = 0.530`.

### Timing attribution

Planned medians; timing values are milliseconds accumulated over each run. The residual is calculated per repetition as `max(processing - known, 0)` and then medianed. `btree_preparation_nanos_total` and `publication_nanos_total` are excluded because their scopes overlap.

| metric | planned median |
|---|---:|
| processing | 2,009.084 ms |
| logical admission | 11.694 ms |
| planning | 156.758 ms |
| physical execution | 225.409 ms |
| catalog construction | 87.375 ms |
| WAL append | 196.822 ms |
| WAL sync function timing | 0.015 ms |
| generation publication | 246.721 ms |
| unattributed residual | 1,084.291 ms |

The residual is not a clone-time measurement. It may include `self.state.clone()`, WAL commit/image-vector assembly, final image aggregation, state assignment, coordinator/mutex overhead, and other untimed work. `wal_syncs_delta` remains nonzero because it counts logical sync invocations; `wal_sync_nanos_total` was only 14,680–17,640 ns per planned 100k repetition, consistent with the disabled sync closure being a no-op.

### Working-set sensitivity

| working set | main mutation ops/s | planned mutation ops/s | planned/main |
|---:|---:|---:|---:|
| 100,000 | 11,467.8 | 6,074.8 | 0.530 |
| 4,096 | 23,441.4 | 15,730.2 | 0.671 |

`R4096 - R100K = 0.141`, above the diagnostic 0.10 threshold. The no-sync planned regression is sensitive to working-set size. This points to whole-state-size-dependent clone and/or immutable catalog construction costs as the next investigation priority; it does not prove either is the cause. At 4,096 rows the ratio is still below parity.

## Read Sanity

Median requests/s over three repetitions; Query and Scan use limit 16. Main's 16/32/64-reader observations are excluded here because planned has no corresponding completed runs.

| readers | main Get | planned Get | main Query | planned Query | main Scan | planned Scan |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 559,993 | 543,546 | 268,485 | 261,031 | 234,437 | 304,132 |
| 2 | 1,055,844 | 906,599 | 493,711 | 453,503 | 535,165 | 494,864 |
| 4 | 1,096,181 | 873,762 | 475,897 | 424,778 | 456,583 | 431,783 |
| 8 | 1,114,424 | 924,950 | 422,156 | 423,450 | 446,870 | 481,745 |

Main Get throughput plateaus around 1.1M requests/s by 2–4 readers; query/scan are also broadly flat after 2 readers on this 2-OCPU machine. Planned's direct read path does not use coordinator groups. These partial reads show no multi-fold read-path collapse.

## Planner Evidence

- The Phase 3 development smoke reported roughly 2.1–3.0 full-state clones/transaction on the serial path versus about 0.12 clones/transaction for planned. In the OCI 16-writer, width-16 injected-zero-delay repetitions, planned recorded 48–49 full-state clones per run, approximately 0.05 clones/transaction.
- OCI batching was present: average group size was 14.27 requests in the width-16 diagnostic, against a maximum of 64.
- On the preserved 16-writer, width-16 same-leaf-heavy runs, median counters were 14,600 route reuses, 13,158 coalesced mutations, 264 leaf groups, and 1,706 leaf loads per run.
- On the preserved 16-writer, width-1 different-leaf-heavy runs, median counters were 882 independent leaf groups out of 882 total leaf groups, with zero dependency edges. Independent work exists, but this does not show that parallel execution would repay current serial overhead.

## Bottleneck Interpretation

The true sync-disabled `R100K` is 0.530, so the large planned write regression remains after physical sync calls are disabled. This establishes that real fsync is not the sole cause. The configured disabled mode still performs buffered writes and WAL encoding, so it does not isolate every storage-path cost. The higher `R4096` ratio indicates meaningful working-set-size sensitivity, while the large unattributed planned timing residual prevents assigning the remaining cost to a specific operation.

## Phase 4 Readiness

- Parallelizable work exists: **yes**; independent leaf groups were observed.
- Current serial planned path understood enough to begin Phase 4: **no**; the no-sync regression remains, and most processing time is still unattributed.
- Recommendation for next engineering step: investigate state-size-dependent clone/catalog costs first, guided by the 0.141 ratio change. The timing residual is not proof of clone time. No Phase 4 or optimization work was performed here.

## Artifacts

- [`environment.txt`](results/oci-a1-2ocpu-12g-200g/phase3/environment.txt)
- [`main-write-core.jsonl`](results/oci-a1-2ocpu-12g-200g/phase3/main-write-core.jsonl)
- [`planned-write-core-partial.jsonl`](results/oci-a1-2ocpu-12g-200g/phase3/planned-write-core-partial.jsonl)
- [`main-read.jsonl`](results/oci-a1-2ocpu-12g-200g/phase3/main-read.jsonl)
- [`planned-read-partial.jsonl`](results/oci-a1-2ocpu-12g-200g/phase3/planned-read-partial.jsonl)
- [`main-injected0-w16-width16.jsonl`](results/oci-a1-2ocpu-12g-200g/phase3/main-injected0-w16-width16.jsonl)
- [`planned-injected0-w16-width16.jsonl`](results/oci-a1-2ocpu-12g-200g/phase3/planned-injected0-w16-width16.jsonl)
- [`main-nosync-w16-width16-ws100k.jsonl`](results/oci-a1-2ocpu-12g-200g/phase3/main-nosync-w16-width16-ws100k.jsonl)
- [`planned-nosync-w16-width16-ws100k.jsonl`](results/oci-a1-2ocpu-12g-200g/phase3/planned-nosync-w16-width16-ws100k.jsonl)
- [`main-nosync-w16-width16-ws4096.jsonl`](results/oci-a1-2ocpu-12g-200g/phase3/main-nosync-w16-width16-ws4096.jsonl)
- [`planned-nosync-w16-width16-ws4096.jsonl`](results/oci-a1-2ocpu-12g-200g/phase3/planned-nosync-w16-width16-ws4096.jsonl)
- Preserved smoke controls: [`smoke-main-btree.jsonl`](results/oci-a1-2ocpu-12g-200g/phase3/smoke-main-btree.jsonl) and [`smoke-planned-blink.jsonl`](results/oci-a1-2ocpu-12g-200g/phase3/smoke-planned-blink.jsonl).
