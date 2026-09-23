# Phase 3 OCI Results

## Environment

- OCI A1, 2 OCPU, 12 GiB shape RAM, ARM64 Ampere; Linux reported two logical CPUs and Neoverse-N1. Guest `free` reported approximately 10 GiB.
- Oracle Linux Server 9.8; kernel `6.12.0-206.104.4.4.el9uek.aarch64`; Rust/Cargo 1.98.1.
- `/dev/sda` was reported as a 200 GB block device, but it was not mounted as a 200 GB filesystem; `/dev/sda3` was approximately 44.5 GB. Benchmark files were on `/home/opc/dodb`, the root XFS filesystem on the root LVM logical volume, with `rw,relatime`; that filesystem was approximately 30 GB. No format, resize, or storage tuning was performed.
- Benchmark DB/WAL temporary path: `/home/opc/dodb/target/oci-a1-2ocpu-12g-200g/phase3-tmp`. It was on the root XFS filesystem, not a separate 200 GB mounted data filesystem.
- Final branch HEAD: `aa00444348d074e081e0dbe5d0979032271bf014`.

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

The observed `injected, 0us` planned/main ratio is below 0.70, but the harness still performs real `sync_data()`. Therefore none of Cases A/B/C can be selected as a valid fsync-isolated conclusion from this experiment. Existing real-sync measurements show large planned regressions, while the new configured-zero-delay comparison remains durability-contaminated. The sync-versus-engine question is unresolved.

## Phase 4 Readiness

- Parallelizable work exists: **yes**; independent leaf groups were observed.
- Current serial overhead acceptable: **no**; planned's observed write throughput is substantially below main, and this diagnostic does not isolate the source.
- Recommendation for next engineering step: **do not begin Phase 4 yet**. First obtain a valid sync-free comparison or otherwise isolate the existing serial planned-path overhead. No optimization or Phase 4 work was performed here.

## Artifacts

- [`environment.txt`](results/oci-a1-2ocpu-12g-200g/phase3/environment.txt)
- [`main-write-core.jsonl`](results/oci-a1-2ocpu-12g-200g/phase3/main-write-core.jsonl)
- [`planned-write-core-partial.jsonl`](results/oci-a1-2ocpu-12g-200g/phase3/planned-write-core-partial.jsonl)
- [`main-read.jsonl`](results/oci-a1-2ocpu-12g-200g/phase3/main-read.jsonl)
- [`planned-read-partial.jsonl`](results/oci-a1-2ocpu-12g-200g/phase3/planned-read-partial.jsonl)
- [`main-injected0-w16-width16.jsonl`](results/oci-a1-2ocpu-12g-200g/phase3/main-injected0-w16-width16.jsonl)
- [`planned-injected0-w16-width16.jsonl`](results/oci-a1-2ocpu-12g-200g/phase3/planned-injected0-w16-width16.jsonl)
- Preserved smoke controls: [`smoke-main-btree.jsonl`](results/oci-a1-2ocpu-12g-200g/phase3/smoke-main-btree.jsonl) and [`smoke-planned-blink.jsonl`](results/oci-a1-2ocpu-12g-200g/phase3/smoke-planned-blink.jsonl).
