# WAL v3 Compact Redo Experiment

Branch `experiment/wal-v3-compact-redo`. The planned-blink durable baseline this experiment starts from is in `planned-blink-durable-baseline-results.md` (planned-blink / ExactMain multiwriter GM 1.861×, / experiment main-btree 1.645×, / RocksDB 0.363×, 4,224 WAL bytes per width-1 transaction).

WAL v3 / PageDelta is not implemented yet. Phase A below is a prerequisite that fixes a memory problem in the current WAL v2 without changing its format.

## Phase A — runtime WAL replay-payload lifetime

Code commit: `16a5b207fc9d4076fd0a43d4adc539a5d6825904` (`wal: drop committed replay payloads after recovery`). Raw results: `results/wal-v3-phase-a/`.

### Root cause

`WalLog` had a field `committed: Vec<CommittedWalBatch>`. It was filled in two places:

1. `WalLog::open` stored every complete commit found by the WAL scan, with all its page images.
2. `append_group_inner` cloned every page image of every successful commit (`commit.pages.clone()`) and appended it after the WAL sync.

Nothing removed entries except `WalLog::reset` (checkpoint). The baseline sustained runs never checkpoint, so every committed page image stayed in memory for the life of the process. Each width-1 transaction added one 4,096-byte leaf image plus its page ID and batch record, close to the 4,224 bytes it added to the WAL file. That is the 1.04 RSS bytes per WAL byte measured in the baseline, and the reason the 64-writer run hit the memory guard at about 87 s.

### Ownership before the change

Every read of `WalLog::committed` / `committed_batches()`, classified:

| Place | What it read | Class |
|---|---|---|
| `BlinkStore::open_internal` (`recover_data_file(&mut file, wal.committed_batches(), checkpoint_hint)`) | page images of commits newer than the checkpoint | A. recovery only, at open |
| `BTreeStore::open_internal` (`recover_data_file(..., wal.committed_batches(), ...)`) | same | A. recovery only, at open |
| Blink and BTree open, `file.is_empty()? && wal.committed_batches().is_empty()` | whether any commit exists | A. recovery only (emptiness) |
| `BTreeStore::open_internal`, `max_commit_lsn` for `next_revision` | LSN of the last commit | B. metadata |
| `BlinkStore::checkpoint`, `BTreeStore::checkpoint`: `committed_batches().last().commit_lsn` | LSN of the last commit, the new checkpoint LSN | B. metadata |
| `BTreeStore::checkpoint`, `should_reset_wal`: `!committed_batches().is_empty()` | whether any commit exists since the last reset | B. metadata |
| `BTreeStore::check_invariants`: `latest_known_lsn` | LSN of the last commit | B. metadata |
| `WalLog::metrics().committed_batches` = `committed.len()` | commit count since open / reset | C. metrics (count only) |
| Page images kept after `append_group_inner` | nothing in production read them | D. historical payload, not needed |
| Unit tests in `wal.rs` and `blink/mod.rs` | page images of recent commits | test inspection only |

So production needs the page images only between the WAL scan and `recover_data_file` at open. After that it needs one LSN and some counters.

The Blink open flow is: validate the superblock identity and read the checkpoint hint → open the WAL (scan, truncate a torn tail) → if the data file is empty and there are no commits, initialize → otherwise `recover_data_file` writes the newer page images into the data file and syncs it → `load_file` → `resume_after(checkpoint_lsn)`. BTree is the same shape.

Checkpoint needs: the LSN of the last durable commit (or the current superblock checkpoint LSN when there is none), whether there is anything to reset, and the WAL history start.

### Ownership after the change

`WalLog` fields:

- `recovery_batches: Vec<CommittedWalBatch>` — the open-time scan result. Only `WalLog::open` fills it. `take_recovery_batches()` hands it to the caller with `std::mem::take`, which also frees the vector here. `reset` clears it.
- `last_commit_lsn: Option<Lsn>` — set from the last scanned commit at open, set after each successful group sync, cleared by `reset`.
- The commit count comes from the existing `scan_report.committed_batches` counter, which was already equal to `committed.len()` at every point.
- `append_group_inner` no longer clones page images.

Open flow: `let recovery_batches = wal.take_recovery_batches();` → emptiness check → `recover_data_file(&mut file, &recovery_batches, checkpoint_hint)` → `drop(recovery_batches)` → load. If recovery fails, the vector is dropped with the error.

Checkpoint and invariant checks read `wal.last_commit_lsn()`. `should_reset_wal` uses `last_commit_lsn().is_some()`. The checkpoint order is unchanged: flush dirty data pages, sync the data file, write and sync the new superblock, then truncate the WAL, sync, write INIT, sync.

New `WalMetrics` fields `retained_recovery_batches` and `retained_recovery_page_images` show what is still held. Tests read recent commits through a test-only `committed_batches_on_disk()`, which rescans the WAL file.

`phase0-bench` gained `--window-seconds N`. Without the flag the benchmark behaves as before. With it, the JSON row gets per-window tx/s and latency (the same code as the baseline's `dodb-sustained-window.patch`) and a set of samples taken after seeding, at measurement start, at each window end and at measurement end: process RSS, WAL bytes, committed batches, retained recovery payload, dirty pages.

Invariants now:

- open: replay payload is available;
- after recovery: it is dropped (retained counts are 0);
- runtime commit: no page image is kept after the sync.

### On-disk format unchanged

No encoder, decoder or frame layout was touched (`Init`, `PageImage`, `Commit` and the format version are the same). To check the bytes, `scripts/wal_byte_probe.rs` runs a fixed workload through the public API on real files: 2,000 planned-blink puts, an overflow value, a delete, 40 groups of 16 transactions mixing width 1 and 16, a checkpoint, 500 more puts, 100 single-key updates, and a BTree run with a checkpoint. It was run on old (`3ac7515`) and new code, twice each (`wal-byte-probe.txt`):

| File | SHA256 (old = new) |
|---|---|
| `blink.wal` (2,534,504 B) | `02c0da2ab5eecd4711fdfb6a3b1f1327dc82803a1ae34bb0b5ecf748f871247d` |
| `blink.db` | `fc9a84f357a0314d78bffb68e010ecff88c12ca4879efdbf48198f229da86e8d` |
| `btree.wal` (2,514,104 B) | `8f76b74ed48ffdb34a32baa6bfddd68b245ea5f3062db5c396bdc98a9da2f0dc` |
| `btree.db` | `af5cc20d2977f14103c9ed414b6b70964e0531f3cdd4ab64f9ff981daa9483ef` |

All four files match byte for byte, and both runs of each version gave the same hashes. The width-1 existing-key update wrote exactly 4,224 bytes in all 100 samples on both versions. Every benchmark row below also measured 4,224.0 WAL bytes per transaction.

### Correctness

`cargo test --workspace --release --no-fail-fast`: 197 passed, 0 failed. New tests:

- `wal::appended_commits_keep_only_metadata_and_reopen_hands_payload_once`: 3,000 commits in groups of 7 keep 0 retained batches/images; reopen finds 3,000 batches equal to what was written; after `take_recovery_batches` the retained counts are 0, a second take is empty, and `last_commit_lsn` survives; `reset` clears it.
- `blink::runtime_commits_do_not_retain_wal_page_images` (A): 4,000 planned-blink commits, retained payload checked as 0 every 1,000 commits; `last_commit_lsn` equals the last revision.
- `blink::reopen_without_checkpoint_replays_every_commit_and_drops_payload` (B): 3,000 commits, drop without flush or checkpoint, the reopened WAL holds all commits, the reopened store has every value, retained payload 0, invariants pass, later commits still work.
- `blink::checkpoint_uses_last_commit_lsn_without_retained_payload` (C): 1,500 commits → checkpoint LSN equals the last commit, WAL history start moves to it, WAL bytes reclaimed → an idle checkpoint keeps the same LSN and reclaims nothing → 1,000 more commits → reopen: superblock checkpoint LSN kept, every value present.
- `blink::torn_final_commit_is_discarded_on_reopen_without_retained_payload` (D): cut the last 10 bytes of the WAL (inside the final commit frame) → reopen drops that commit, keeps the previous value, truncates only the torn frame (the same rule as the existing `fast_group_torn_tail_keeps_only_complete_commits`), accepts new commits, and reopens again.

Existing WAL, recovery, checkpoint and fault-injection tests pass unchanged except for the test-only accessor swap.

One existing test is flaky on this Mac with and without the change: `wal::group_fast_path_is_byte_identical_to_fault_injectable_path` asserts `group_commit_direct_encode_nanos > 0` for four tiny commits, which can measure 0 with the ~41 ns clock tick. Isolated runs failed 10/200 on old code and 13/200 on new code. It was left as is.

### Short A/B throughput

OCI A1 2 OCPU, ZFS `/bench/zfs/db` (same host and dataset as the baseline). `old-v2` is the baseline core binary (SHA256 `79122436…`, built from `0d310da`; crates are identical to `3ac7515`). `retention-fixed-v2` is built from `16a5b20` (SHA256 `f9d5199f…`). Width 1, uniform, working set 100,000, 2 s warmup, 5 s measure, the same seeds as the baseline scenarios 0 and 6, old/new order swapped every repetition. Every row: `engine=planned-blink`, `sync_mode=real`, 0 errors.

| Writers | Variant | rep1 | rep2 | rep3 | mean tx/s | mean p99 µs | WAL B/tx | mean peak RSS MiB |
|---|---|---|---|---|---|---|---|---|
| 16 | old-v2 | 4,323 | 4,499 | 4,488 | 4,437 | 6,387 | 4,224.0 | 712 |
| 16 | retention-fixed-v2 | 4,586 | 4,568 | 4,297 | 4,484 | 6,424 | 4,224.0 | 155 |
| 64 | old-v2 | 11,777 | 11,216 | 11,436 | 11,476 | 10,769 | 4,224.0 | 918 |
| 64 | retention-fixed-v2 | 12,240 | 11,892 | 11,644 | 11,925 | 10,570 | 4,224.0 | 169 |

New / old: 1.011 at 16 writers, 1.039 at 64 writers. Both are inside the ±5% band, and the repetition spread overlaps, so there is no regression. The 64-writer gain may be real (one less 4 KiB copy and allocation per commit on the single coordinator thread) but this run is too short to claim it.

### 120-second sustained runs

Working set 1,000,000, width 1, uniform, 10 s warmup, 120 s measure, real sync, planned-blink, the baseline seeds, no checkpoint. Same once-per-second process monitor and 256 MiB `MemAvailable` guard as the baseline. The windows use the 12 full 10 s windows; RSS and WAL columns come from the external monitor.

**16 writers, retention-fixed-v2** — completed. 4,308 tx/s, p50 3,597 µs, p99 7,080 µs, 517,036 transactions, 0 errors.

| Window | tx/s | p99 µs | WAL growth MiB | RSS MiB | RSS growth MiB | dirty pages | retained batches / images |
|---|---|---|---|---|---|---|---|
| 0-10 s | 4,205 | 6,893 | 170 | 1,203 | 30 | 68,448 | 0 / 0 |
| 20-30 s | 4,313 | 7,309 | 517 | 1,236 | 64 | 68,448 | 0 / 0 |
| 50-60 s | 4,338 | 6,582 | 1,039 | 1,265 | 92 | 68,448 | 0 / 0 |
| 80-90 s | 4,320 | 7,272 | 1,558 | 1,285 | 112 | 68,448 | 0 / 0 |
| 110-120 s | 4,307 | 6,832 | 2,083 | 1,298 | 126 | 68,448 | 0 / 0 |

**64 writers, retention-fixed-v2** — completed all 120 s (the baseline was killed at about 87 s). 10,166 tx/s, p50 5,598 µs, p99 13,633 µs, 1,220,140 transactions, 0 errors. Peak RSS 1,547 MiB, lowest `MemAvailable` 3,044 MiB.

| Window | tx/s | p99 µs | WAL growth MiB | RSS MiB | RSS growth MiB | dirty pages | retained batches / images |
|---|---|---|---|---|---|---|---|
| 0-10 s | 10,172 | 13,599 | 417 | 1,270 | 37 | 68,448 | 0 / 0 |
| 20-30 s | 10,210 | 13,465 | 1,238 | 1,313 | 80 | 68,448 | 0 / 0 |
| 50-60 s | 10,117 | 14,285 | 2,464 | 1,447 | 214 | 68,448 | 0 / 0 |
| 80-90 s | 10,192 | 13,766 | 3,684 | 1,487 | 254 | 68,448 | 0 / 0 |
| 110-120 s | 10,303 | 13,029 | 4,915 | 1,547 | 314 | 68,448 | 0 / 0 |

Every window is in `results/wal-v3-phase-a/tables.md`.

**16 writers, old-v2 control, same session** — the baseline window binary (SHA256 `fba986d4…`) re-run right after, to get a "before" from the same host state. 4,355 tx/s, p99 7,345 µs. WAL grew 2,105 MiB and RSS grew 2,187 MiB (1.039 RSS bytes per WAL byte), from 5,871 to 8,058 MiB. This repeats the baseline (2,108 / 2,192 MiB, 1.04).

Summary:

| Run | Completed | tx/s | p99 µs | WAL growth MiB | RSS at start MiB | RSS growth MiB | RSS / WAL (0-120 s) | RSS / WAL (60-120 s) |
|---|---|---|---|---|---|---|---|---|
| 16w old-v2 (baseline) | yes | 4,369 | — | 2,108 | — | 2,192 | 1.04 | — |
| 16w old-v2 (control) | yes | 4,355 | 7,345 | 2,105 | 5,871 | 2,187 | 1.039 | 1.009 |
| 16w retention-fixed-v2 | yes | 4,308 | 7,080 | 2,083 | 1,173 | 126 | 0.060 | 0.032 |
| 64w old-v2 (baseline) | no, guard at ~87 s, RSS 9,645 MiB | ~9,600 (derived) | unknown | 3,093 in 80 s | — | 3,238 in 80 s | 1.05 | — |
| 64w retention-fixed-v2 | yes | 10,166 | 13,633 | 4,915 | 1,233 | 314 | 0.064 | 0.040 |

The 1:1 link between WAL history and RSS is gone. The process also starts the measurement much smaller: seeding 1,000,000 rows writes a 4.6 GiB WAL, which used to stay in memory (5.9 GiB RSS at measurement start), and now leaves 1.13 GiB RSS.

### What the remaining RSS is

- Retained WAL payload: 0 batches and 0 images in every sample.
- Dirty pages: 68,448 in every sample, from seeding to the end, in both runs. Uniform width-1 updates only rewrite leaves that are already dirty, so this map does not grow. At 4 KiB per image it is about 267 MiB, already inside the 1.13 GiB after seeding.
- Benchmark timeline: `--window-seconds` stores 16 bytes per successful transaction (finish time and latency). That is 7.9 MiB at 16 writers and 18.6 MiB at 64 writers, up to twice that with `Vec` spare capacity. The baseline window binary had the same cost.
- The rest, about 110 MiB at 16 writers and about 280 MiB at 64 writers over 120 s, is not attributed by this run. It does not follow the WAL: at 16 writers the growth per window falls from 30 MiB to 3–7 MiB, and at 64 writers it comes in steps (for example +60 MiB at 30-40 s and +71 MiB at 50-60 s) with flat windows in between, which looks like allocator arenas or retained state growing in chunks rather than a per-commit cost. In the second half of the run it is 0.03–0.04 bytes per WAL byte. Finding it would need a heap profile; that is outside this phase.

### Notes

- `MemAvailable` on the host was about 5 GiB lower at the start of the later runs than at the start of the session. It excludes the ZFS ARC, which grows while the benchmark writes the WAL. The runs never came close to the guard.
- The primary sustained runs do not checkpoint, as in the baseline. The WAL file still grows without limit; a checkpoint policy is separate work.

### Files

`results/wal-v3-phase-a/`: `environment.txt`, `run-order.txt`, `process-metrics.jsonl`, `short-progress.log`, `sustained-progress.log`, `sustained-control-progress.log`, `wal-byte-probe.txt`, `tables.md`, `raw/` (12 short rows and logs, 3 sustained rows, logs and once-per-second monitors), `scripts/run_phase_a.py`, `scripts/analyze_phase_a.py`, `scripts/wal_byte_probe.rs`, `SHA256SUMS`.

## Phase B — compact PageDelta redo

Question: if ordinary existing-page updates log a compact byte delta instead of a 4 KiB page image, does durable throughput go up clearly? Answer: yes. Planned Blink multiwriter GM is 1.853× Phase A on the 14-scenario matrix. That is the ">1.50×" band of the plan, so full-page WAL was a major bottleneck.

Code (branch `experiment/wal-v3-compact-redo`, not merged):

| Commit | Content |
|---|---|
| `4ac3a2e` wal: add versioned page-delta redo records | WAL format 3, PageDelta record, v3 commit digest, full-image-first page chain, streaming scan, Blink recovery from rebuilt pages |
| `6bcf8bb` blink: emit compact redo for existing pages | planned Blink producer, checkpoint fault points, store-level tests, B0 probe, bench metrics |
| `11639db` wal: keep redo counters across WAL reset | counters only; no format or recovery change |
| `def6f7e` bench: add WAL-size checkpoint control | `--checkpoint-wal-bytes` in `phase0-bench` |

`BTreeStore` still writes format 2 page images; its WAL and data-file bytes are identical to Phase A (`results/wal-v3-phase-b/byte-probe.txt`).

### Format and recovery rule

Full specification: `results/wal-v3-phase-b/format-specification.md`. Summary:

- PageDelta (record type 4, format 3 only) = page ID u64, base page LSN u64, span count u16 (1..=820), then spans of offset u16, length u16, bytes. Header 18 B, 4 B per span. It is always one redo record per dirty page, so `commit_lsn = first_record_lsn + record_count` is unchanged.
- Spans are the maximal changed-byte runs between the previous committed image and the new canonical `encode_blink_page` image, merged left to right when the unchanged gap is at most 4 bytes. Decode and apply reject anything that is not exactly that encoding.
- The v3 commit digest is CRC32C over each record's type, index, length and payload.
- **Full image first:** after WAL initialization or reset, the first committed record for a data page is always a PageImage. Later changes of that page in the same WAL history may be deltas against the previous committed image. The WAL tracks page ID → (page LSN, CRC32C) of the latest committed image, O(unique pages since reset), rebuilt on open and cleared on reset. A delta base that does not match that entry is an invariant error; a page not in the chain is written as a full image.
- **Recovery never uses data-file bytes as a delta base.** The scan keeps page ID → latest rebuilt image, applies a transaction's records only after its commit frame and digest check, and validates every rebuilt page with the full Blink page validator (checksum included) plus the commit LSN. `recover_data_file` then overwrites each page newer than the checkpoint with its rebuilt image. So a checkpoint that crashed after writing some pages, or tore a page, before the checkpoint superblock was durable, is repaired from the WAL.
- The planned Blink producer marks a transaction eligible only if it emits no superblock image (no split, allocation, page reuse, free-list or overflow allocator change). The WAL still decides per page: superblock, first touch since reset, missing base, or a delta not smaller than an image all stay full images.

### B0 — encoded size (deterministic probe, local)

`results/wal-v3-phase-b/b0-encoded-size-probe.jsonl`, 20,000 rows (1,279 leaves), 16-byte keys, 64-byte values:

| Case | tx | WAL B/tx mean (p50 / p95 / max) | delta frame B mean (p50 / p95 / max) | spans | changed B | fallback |
|---|---|---|---|---|---|---|
| existing-key width-1 update | 4,000 | 226.9 (227 / 228 / 228) | 158.9 (159 / 160 / 160) | 4.0 | 72.9 | 0 |
| first touch after checkpoint (1 in 40 keys) | 500 | 1,584.7 (225 / 4,224 / 4,224) | 157.1 | 4.0 | 71.1 | 34% first touch |
| delete existing key | 500 | 1,190.8 (1,165 / 2,056 / 3,586) | 1,122.8 | 15.2 | 992 | 0 |
| insert new key (no split) | 500 | 1,189.4 (1,165 / 2,056 / 3,511) | 1,121.4 | 15.2 | 990 | 0 |
| width-16 existing update | 500 | 2,596.1 (2,605 / 2,610 / 2,615) | 159.1 (max 309) | 4.0 | 73.1 | 0 |
| 16 tx on one leaf per group | 3,200 | 224.8 | 156.8 | 4.0 | 70.8 | 0 |
| 16 tx on different leaves per group | 3,200 | 226.4 | 158.4 | 4.0 | 72.4 | 0 |

Width-1 update: **4,224 → 227 B/tx** (PageImage frame 4,156 B → delta frame ~159 B; commit frame 68 B). The four spans are the page LSN, the page checksum, the entry revision and the value. Delete and insert are about 1.2 KB because the canonical leaf encoding moves every record after the changed slot. The gate (<512 B/tx, stop above 1 KiB) passed.

### Correctness

`cargo test --workspace --release --no-fail-fast`: 212 passed, 0 failed, 1 ignored (the B0 probe). New tests:

- codec: 3,000 random Blink leaf mutations (value same size, value resized, delete, insert, tombstone, revision only) and 2,000 random raw edits round-trip byte for byte, re-encode to the same delta, and the rebuilt Blink page validates; gap merge boundary; 18 malformed cases (bad base LSN, wrong page ID, zero-length, out-of-range, overlapping, unsorted, unmerged gap, truncated, trailing bytes, span count 0 / too high / too large, not starting on a changed byte, long unchanged gap, invalid rebuilt checksum).
- WAL: first redo after reset is an image then deltas, and again an image after the next reset; base mismatch (wrong bytes or stale LSN) is an invariant error and writes nothing; four transactions on two pages in one group chain in FIFO order (base LSNs checked in the frames); superblocks, ineligible commits and a Blink WAL still at version 2 get images; 11 malformed delta records written into an otherwise valid WAL (with the digest recomputed so only the delta check can fail) are rejected as corruption; bad digest and a delta in a version-2 WAL are rejected; omission, duplication, reordering, type substitution, payload corruption and a spliced commit are rejected.
- store: torn checkpoint (page set to the old checkpoint image, an intermediate committed image, the latest image, half old/half new, random bytes; another dirty page partially flushed) always recovers the latest committed page; delta-append fault matrix over 14 points (including during delta header, delta payload, after a delta record, before/after the commit record, before/during/after the WAL sync) at every occurrence, with reopen, atomicity check (a width-16 transaction is all-or-nothing and visible exactly when its commit frame was written), further writes and a second reopen; every byte cut of the WAL tail inside a width-16 delta transaction; checkpoint fault matrix over 18 points (page writes, data sync, superblock write and sync, WAL truncate, INIT rewrite, reset sync) with reopen, more writes, reopen, checkpoint, reopen; deltas after `flush()` and from the parallel executor.

The known timing-only flake `group_fast_path_is_byte_identical_to_fault_injectable_path` did not fire in these runs.

### First OCI gate

OCI A1 2 OCPU, ZFS `/bench/zfs/db`, same host as the baseline. Baseline `phase-a` = `12ab044` (SHA256 `f9d5199f…`, byte-identical to the Phase A build), candidate `page-delta` = `6bcf8bb` (SHA256 `0dc74cc6…`). `--engine planned-blink --sync-mode real`, working set 100,000, 2 s warmup, 5 s measure, baseline seeds, 3 repetitions with the variant order swapped every repetition. Every row was checked for `engine`, `sync_mode`, WAL syncs and planner counters; every page-delta row wrote deltas.

| Scenario | phase-a tx/s | page-delta tx/s | ratio | p99 µs | WAL B/tx | mean sync ms | tx/sync |
|---|---|---|---|---|---|---|---|
| 16w width 1 uniform | 4,597 | 8,298 | 1.805 | 6,077 → 3,343 | 4,224 → 227 | 1.42 → 0.74 | 7.9 → 8.3 |
| 16w width 16 uniform | 1,088 | 1,829 | 1.681 | 33,601 → 27,017 | 66,506 → 2,622 | 7.17 → 2.34 | 14.7 → 14.7 |
| 64w width 1 uniform | 12,655 | 24,111 | 1.905 | 9,752 → 5,154 | 4,224 → 227 | 3.03 → 0.89 | 59.8 → 58.8 |
| 64w width 16 uniform | 1,169 | 2,033 | 1.739 | 143,131 → 54,228 | 66,490 → 2,612 | 24.69 → 3.23 | 61.5 → 62.3 |

GM 1.781, far above the 1.10 stop line. Transactions per sync do not change; each sync gets much cheaper because it writes about 1/18 of the bytes, so the coordinator turns over faster. CPU per run went up (for example 22% → 32% at 16w width 1) because more transactions are processed. Peak RSS is about the same.

### Full matrix

14 scenarios × 3 repetitions × 2 variants, interleaved (84 rows). Per-scenario tables with p50/p95/p99, CPU, RSS, WAL bytes, image and delta records per transaction, span counts, fallback counts, tx/sync, sync count, sync latency and WAL MiB/s: `results/wal-v3-phase-b/tables.md`.

| # | Scenario | page-delta tx/s | phase-a tx/s | PD / phase-a | p99 µs phase-a → PD |
|---|---|---|---|---|---|
| 0 | 16w w1 uniform | 8,211 | 4,477 | 1.834 | 6,308 → 3,240 |
| 1 | 16w w1 compact | 9,474 | 4,900 | 1.933 | 6,315 → 2,827 |
| 2 | 16w w1 spread | 8,524 | 4,712 | 1.809 | 6,855 → 3,052 |
| 3 | 16w w16 uniform | 1,923 | 1,121 | 1.716 | 29,966 → 16,020 |
| 4 | 16w w16 compact | 6,016 | 3,318 | 1.813 | 7,897 → 4,059 |
| 5 | 16w w16 spread | 5,532 | 3,344 | 1.654 | 9,731 → 5,898 |
| 6 | 64w w1 uniform | 24,234 | 12,066 | 2.009 | 14,461 → 5,058 |
| 7 | 64w w1 compact | 32,897 | 11,426 | 2.879 | 9,673 → 3,772 |
| 8 | 64w w1 spread | 26,522 | 12,481 | 2.125 | 9,238 → 5,312 |
| 9 | 64w w16 uniform | 2,026 | 1,172 | 1.729 | 95,087 → 59,505 |
| 10 | 64w w16 compact | 10,768 | 5,779 | 1.863 | 21,861 → 9,797 |
| 11 | 64w w16 spread | 5,941 | 4,727 | 1.257 | 25,979 → 19,584 |
| 12 | 1w w1 uniform | 1,389 | 1,318 | 1.053 | 1,300 → 1,073 |
| 13 | 1w w16 uniform | 812 | 388 | 2.091 | 3,265 → 1,730 |

Multiwriter geometric means (12 scenarios):

| Category | PD / phase-a | PD / ExactMain | PD / RocksDB | phase-a / ExactMain | phase-a / RocksDB |
|---|---|---|---|---|---|
| overall | **1.853** | **3.605** | **0.704** | 1.945 | 0.380 |
| writers 16 | 1.791 | 2.843 | 0.713 | 1.587 | 0.398 |
| writers 64 | 1.918 | 4.572 | 0.695 | 2.384 | 0.362 |
| width 1 | 2.071 | 3.618 | 0.925 | 1.747 | 0.447 |
| width 16 | 1.659 | 3.593 | 0.536 | 2.166 | 0.323 |
| uniform | 1.818 | 3.854 | 0.481 | 2.120 | 0.265 |
| compact locality | 2.082 | 3.639 | 0.961 | 1.747 | 0.462 |
| spread locality | 1.681 | 3.342 | 0.753 | 1.988 | 0.448 |

ExactMain and RocksDB are the reused rows of the earlier runs (ExactMain: real-sync matrix; RocksDB v11.8.1: cross-DB matrix, 2026-09-25). They are **not interleaved** with this session. The phase-a row of this session gives 1.945× ExactMain against 1.861× in the baseline document, so this session ran about 4% faster for planned Blink than the day of the reused rows. Single-writer width 1 barely moves (1.053): with one writer every transaction waits for its own sync, and the sync itself is only a little cheaper for a 4 KiB write.

### RocksDB confirmation (same session)

Because the matrix GM gain is above 1.25×, RocksDB was re-run in this session on the four uniform scenarios, alternating order with page-delta, same binary (SHA256 `78f222f2…`), `write_options_sync=true`, WAL on, pipelined write off, verification passed on every row.

| Scenario | page-delta tx/s | RocksDB tx/s | RocksDB reused tx/s | PD / RocksDB | p99 µs PD / RocksDB |
|---|---|---|---|---|---|
| 16w w1 | 8,256 | 10,103 | 10,034 | 0.817 | 3,272 / 2,358 |
| 16w w16 | 1,923 | 6,147 | 5,723 | 0.313 | 16,147 / 5,135 |
| 64w w1 | 24,071 | 26,802 | 26,254 | 0.898 | 5,125 / 4,154 |
| 64w w16 | 2,054 | 9,852 | 9,573 | 0.208 | 50,106 / 14,375 |

The same-session RocksDB numbers are within 1–7% of the reused ones, so the reused 0.704× matrix GM stands. On these four scenarios PD / RocksDB is 0.468 (reused: 0.467). Page-delta closes most of the width-1 gap (0.82–0.90×) but width 16 uniform stays at 0.2–0.3×: 16 random leaves per transaction still cost 16 records, 16 leaf re-encodes and 16 page validations in the single coordinator, where RocksDB appends one small WriteBatch.

### 120-second sustained (no checkpoint)

Candidate `6bcf8bb`, working set 1,000,000, width 1, uniform, 10 s warmup, 120 s measure, real sync, same seeds and monitor as Phase A (`results/wal-v3-phase-b/sustained-tables.md`).

| Run | tx/s | p99 µs | WAL growth 120 s | RSS growth 120 s | peak RSS | Phase A tx/s / p99 / WAL growth |
|---|---|---|---|---|---|---|
| 16 writers | 8,704 | 3,380 | 227 MiB | 169 MiB | 1,374 MiB | 4,308 / 7,080 / 2,083 MiB |
| 64 writers | 20,440 | 5,993 | 533 MiB | 371 MiB | 1,694 MiB | 10,166 / 13,633 / 4,915 MiB |

About 2.0× Phase A throughput at both writer counts, with half the p99 and about 1/9 of the WAL growth. Every window wrote only deltas (delta ratio 1.000), dirty pages stayed at 68,448, mean WAL sync stayed at 0.75 ms (16w) and 0.91 ms (64w), and no window dropped. Seeding 1,000,000 rows now leaves a 3.6 GiB WAL instead of 4.6 GiB (seed transactions still write first-touch images). RSS grows by about 170 / 370 MiB, which is not tied to the WAL and has the same shape as in Phase A (the benchmark's 16-byte-per-transaction timeline is 17 / 39 MiB of it).

### Checkpoint control

Separate from the headline. Both variants get the same bench-only `--checkpoint-wal-bytes` patch: phase-a = `12ab044` + `scripts/checkpoint-control-on-12ab044.patch` (SHA256 of binary `fa8c8334…`), page-delta = `def6f7e` (`11953816…`). 16 writers, same sustained workload; a checkpoint right after seeding, then a synchronous checkpoint in the writer thread whenever the WAL reaches the threshold.

| Threshold | Variant | tx/s | p99 µs | checkpoints in 120 s | checkpoint duration | WAL reclaimed | WAL B/tx written |
|---|---|---|---|---|---|---|---|
| 1 GiB | phase-a | 4,091 | 6,936 | 2 | 2.43 / 2.69 s | 2,048 MiB | 4,224 |
| 1 GiB | page-delta | **8,707** (2.13×) | 3,636 | 0 | — | 0 | 355 |
| 256 MiB | phase-a | 3,835 | 7,781 | 7 | 1.40–1.59 s | 1,792 MiB | 4,224 |
| 256 MiB | page-delta | **5,600** (1.46×) | 6,713 | 5 | 2.17–2.36 s | 1,280 MiB | 1,777 |

Post-checkpoint full-image tax (page-delta, 1 GiB, after the post-seed checkpoint): image share of redo records per 10 s window 30.8% → 10.1% → 3.0% → 0.9% → 0.2% → 0.1% → 0; estimated WAL bytes per transaction 1,457 → 629 → 348 → 261 → 237 → 229 → 227. Throughput rises from 7,098 to about 9,300 tx/s as the image share falls. After about 40 s every page is covered again and the run is back at the no-checkpoint steady state.

With a 256 MiB threshold the WAL never reaches that steady state: covering 68k leaves once costs about 280 MiB of images, more than the threshold, so each checkpoint cycle is spent mostly on first-touch images (image share 18–77% per window, 1,777 B/tx overall). Page-delta still wins 1.46× because the delta part is cheap and it needs fewer checkpoints (5 vs 7), but each of its checkpoints is longer (about 2.2 s vs 1.5 s) because more transactions, and so more dirty pages, accumulate between them. Checkpoint windows show the stall in both variants (p99 up to about 8.9 ms). The PageDelta advantage survives periodic checkpoints, but how much of it survives depends on the WAL budget compared with the size of the dirty working set. A checkpoint policy should be sized to at least one full-image pass over the hot pages.

The window after a checkpoint can show its WAL drop one window early, because the resource sampler waits for the store lock that the checkpoint holds.

### Files

`results/wal-v3-phase-b/`: `format-specification.md`, `b0-encoded-size-probe.jsonl`, `byte-probe.txt`, `environment.txt`, `run-order.txt`, `process-metrics.jsonl`, progress logs, `tables.md`, `analysis.json`, `sustained-tables.md`, `raw/` (gate, matrix, confirmation, sustained and checkpoint rows, logs and monitors), `scripts/run_phase_b.py`, `scripts/analyze_phase_b.py`, `scripts/analyze_sustained.py`, `scripts/checkpoint-control-on-12ab044.patch`, `SHA256SUMS`.

## Phase C — width-16 bottleneck attribution

Question: why is width 1 close to RocksDB (0.92×) while width 16 uniform is still about 4.8× slower? Measurement only; no algorithm was changed. Instrumentation commit `cb7fb54` adds per-transaction locality histograms (mutations, dirty pages, dirty leaves, structural transactions) and a separate timer for WAL redo planning (page-delta diff and checks). Raw results, tables and perf data: `results/wal-v3-phase-c/`.

### Method

- OCI A1 2 OCPU, ZFS, same host. Binary `cb7fb54` (SHA256 `b6c74b1c…`), `--engine planned-blink`; every row was checked for `engine`, `sync_mode` and page deltas.
- Six scenarios: 16w/64w × width 1/16 uniform, plus 64w width 16 compact and spread. Working set 100,000, 2 s warmup, 5 s measure, 3 repetitions, baseline seeds.
- Each scenario ran with `--sync-mode real` and `--sync-mode disabled`, with the order swapped every repetition. **The `disabled` rows skip fsync. They separate CPU from storage and are not durable throughput.**
- Times below are the coordinator's `apply_transaction_group` time (`processing_nanos`), split by the existing Blink batch timers and WAL timers. "physical other" is physical execution minus mutation, restamp and page encode; "WAL frame encode" is WAL group encode minus redo planning; "unattributed" is what the timers do not cover.
- perf: a separate build of the same commit with frame pointers and line tables (`0db4bd76…`), `perf record -F 499 -g` on the running process for 15 s of the measurement, 64w width 1 and width 16 uniform (real sync).
- RocksDB v11.8.1 (same binary as before) on 64w width 1 and width 16 uniform, 3 repetitions, alternating with dodb.

### Throughput

| Scenario | real tx/s | no-sync tx/s | no-sync / real | real p99 µs | coordinator busy (real) | tx per sync |
|---|---|---|---|---|---|---|
| 16w w1 uniform | 8,156 | 33,413 | 4.10 | 3,249 | 97.6% | 8.1 |
| 16w w16 uniform | 1,913 | 2,606 | 1.36 | 16,251 | 92.9% | 14.5 |
| 64w w1 uniform | 23,545 | 37,350 | 1.59 | 5,564 | 93.5% | 60.6 |
| 64w w16 uniform | 2,063 | 2,270 | 1.10 | 58,977 | 92.3% | 61.5 |
| 64w w16 compact | 10,788 | 14,245 | 1.32 | 9,306 | 87.0% | 43.0 |
| 64w w16 spread | 5,860 | 7,904 | 1.35 | 20,250 | 84.1% | 61.3 |

The single coordinator is busy about 90% of the wall time in every case. At 64w width 16 uniform, removing fsync entirely gives only 1.10×: that scenario is CPU-bound, not storage-bound.

### Page locality per transaction

| Scenario | mutations | dirty pages mean / p50 / p95 / max | dirty leaves mean / p50 / p95 | mutations sharing a leaf | structural tx |
|---|---|---|---|---|---|
| 64w w16 uniform | 16 | 15.98 / 16 / 16 / 18 | 15.98 / 16 / 16 | 0.02 | 4 of ~31k |
| 16w w16 uniform | 16 | 15.98 / 16 / 16 / 18 | 15.98 / 16 / 16 | 0.02 | 6 |
| 64w w16 compact | 16 | 2.00 / 2 / 2 / 2 | 2.00 / 2 / 2 | 14 | 0 |
| 64w w16 spread | 16 | 2.00 / 2 / 2 / 2 | 2.00 / 2 / 2 | 14 | 0 |

A uniform width-16 transaction touches 16 different leaves: every mutation is on its own page. Compact and spread touch 2 leaves (8 mutations each). There are no overflow values; splits are rare (4–6 transactions per scenario).

### Coordinator time per transaction (real sync, ns)

| Component | 64w w1 | 64w w16 uniform | per mutation (w16) | w16 / w1 | share (w16) | 64w w16 compact |
|---|---|---|---|---|---|---|
| admission | 750 | 11,788 | 737 | 15.7× | 2.6% | 7,032 |
| planning | 3,259 | 82,974 | 5,186 | 25.5× | 18.6% | 19,941 |
| physical mutation | 3,388 | 57,976 | 3,623 | 17.1× | 13.0% | 12,081 |
| physical restamp | 150 | 2,963 | 185 | 19.7× | 0.7% | 919 |
| page encode | 2,281 | 38,067 | 2,379 | 16.7× | 8.5% | 2,894 |
| physical other | 326 | 1,345 | 84 | 4.1× | 0.3% | 333 |
| dirty union | 89 | 837 | 52 | 9.4× | 0.2% | 98 |
| catalog construction | 1,967 | 21,951 | 1,372 | 11.2× | 4.9% | 162 |
| WAL assembly | 1,331 | 27,629 | 1,727 | 20.8× | 6.2% | 1,157 |
| WAL redo plan (delta encode) | 3,773 | 56,341 | 3,521 | 14.9× | 12.6% | 5,798 |
| WAL frame encode | 890 | 9,972 | 623 | 11.2× | 2.2% | 1,376 |
| WAL write | 837 | 3,538 | 221 | 4.2× | 0.8% | 1,321 |
| WAL sync | 15,136 | 51,150 | 3,197 | 3.4× | 11.4% | 22,810 |
| state install | 1,218 | 17,718 | 1,107 | 14.5× | 4.0% | 98 |
| generation publication | 1,536 | 18,569 | 1,161 | 12.1× | 4.2% | 264 |
| dirty tracking | 839 | 16,535 | 1,033 | 19.7× | 3.7% | 83 |
| unattributed | 1,955 | 27,878 | 1,742 | 14.3× | 6.2% | 4,294 |
| **total** | **39,727** | **447,232** | **27,952** | **11.3×** | 100% | **80,661** |

Per-mutation, per-scenario and no-sync versions, plus detail timers (planner route, leaf clone, catalog scan/clone, retired-generation drop), are in `results/wal-v3-phase-c/tables.md`. Work counts per transaction at 64w width 16 uniform: 16 mutations, 15.98 page encodes (0.999 per mutation), 15.98 page deltas, 0.002 page images, 1,703 delta payload bytes (106 per mutation), 63.9 spans (4.0 per mutation), 2,612 WAL bytes.

What scales with what:

- Every per-page stage grows 15–21× from width 1 to width 16 (physical mutation 17×, page encode 17×, delta encode 15×, WAL assembly 21×, state install 15×, dirty tracking 20×, catalog 11×, publication 12×) — in line with 16× dirty leaves.
- Planning grows 25.5× at 64 writers (13.7× at 16): per mutation it costs 5.2 µs in 64-transaction groups against 3.4 µs in 16-transaction groups, so it grows faster than linearly with group size.
- Only WAL sync (3.4×) and WAL write (4.2×) stay mostly per group.
- **Cost follows touched leaves, not mutations.** With sync disabled, coordinator time per touched leaf is 23.9 µs at 64w width 1, 25.0 µs at 64w width 16 uniform and 28.9 µs at 64w width 16 compact (8 mutations per leaf). Compact width 16 is 5.2× faster than uniform width 16 because it touches 8× fewer leaves.

### Real sync against sync disabled

| Scenario | real ns/tx | no-sync ns/tx | WAL sync ns/tx | sync share |
|---|---|---|---|---|
| 16w w1 | 119,675 | 26,800 | 89,986 | 75.2% |
| 64w w1 | 39,727 | 23,928 | 15,136 | 38.1% |
| 16w w16 | 485,509 | 348,307 | 133,980 | 27.6% |
| 64w w16 uniform | 447,232 | 399,921 | 51,150 | 11.4% |
| 64w w16 compact | 80,661 | 57,782 | 22,810 | 28.3% |

Width 1 is mostly waiting for the sync (38–75%); width 16 uniform at 64 writers is 89% CPU.

### perf (64w width 16 uniform, real sync, frame-pointer build, 1,910 tx/s)

Self time, top entries: `memcpy` 14.1%, `plan_batch` 11.4%, `_int_malloc` 7.1%, Arc refcount atomics (`ldadd8`) 10.0%, `memcmp` 5.5%, `free` 4.7%, `malloc_consolidate` 3.7%, CRC32C 4.3%, `DocumentKey::validate_encoded` 3.5%, `encode_page_delta` 3.4%, `prepare_planned_serial_execution` 3.2%, `_int_free` 2.8%, `memmove` 2.6%. Memory copy, allocation and refcounting together are about 45% of samples.

Inclusive: `apply_transaction_group` 88%, `prepare_planned_serial_execution` 22.4%, `plan_batch` 18.4%, WAL `append_group_inner` 16.5%, `BTreeMap<PageId, [u8; 4096]>::insert` (dirty-page copies) 6.9%, `encode_blink_page` 6.5%, `leaf_body_layout` 5.1%, `ensure_sorted_leaf` 4.8%, `apply_cached_leaf_mutation` 4.7%, `prepare_delta` (catalog) 4.6%, `BlinkPage` / `Vec<LeafEntry>` clones about 4% (their Arc refcount increments are most of the `ldadd8_relax` samples), dropping the retired generation (`PublishedGeneration`/`PageCatalog`/`PageCell` drop) about 4%.

The 64w width 1 profile has the same functions, with a larger share for publication drop (6.7%) and WAL append (24.7%), and a smaller share for `plan_batch` (10.3%). Of the areas the plan asked about: leaf lookup (`memcmp`, `ensure_sorted_leaf`), allocation/free, page encoding, PageDelta diff, CRC32C, catalog/generation publication and `Arc`/`BTreeMap` cloning are all visible; none is above 15% on its own. About 2.6–4.4% of samples are `Vec<TransactionMutation>` growth in the benchmark coordinator closure, outside the timed `apply_transaction_group`.

Reports: `results/wal-v3-phase-c/perf/*.report-{top,inclusive,dso,hot-callers,callers,children,flat}.txt`, `*.inclusive-summary.md` (roughly demangled), and the `perf.data` files.

### RocksDB same session

| Scenario | dodb tx/s | RocksDB tx/s | dodb / RocksDB | p99 µs dodb / RocksDB | CPU % (one core) dodb / RocksDB |
|---|---|---|---|---|---|
| 64w w1 uniform | 23,986 | 26,208 | 0.915 | 5,148 / 3,927 | 68 / 64 |
| 64w w16 uniform | 2,060 | 9,805 | 0.210 | 57,860 / 14,893 | 92 / 85 |

RocksDB matches the Phase B session (26,802 / 9,852 tx/s), so storage did not drift. At width 16 RocksDB spends about 0.85 core for 9,805 tx/s (roughly 87 µs of CPU per transaction). dodb spends about 400 µs of coordinator CPU per transaction, all in one thread, which caps it near 2,300 tx/s even without fsync.

### Attribution of the remaining RocksDB gap (64w width 16 uniform)

| Plan category | Components | Share of coordinator time |
|---|---|---|
| A. physical execution per leaf | mutation, restamp, page encode, other | 22.5% |
| B. planner | planning + admission | 21.2% |
| D. WAL CPU | redo plan (delta encode) 12.6%, assembly 6.2%, frame encode 2.2%, write 0.8% | 21.8% |
| C. catalog / publication / install | catalog 4.9%, publication 4.2%, state install 4.0%, dirty tracking 3.7%, dirty union 0.2% | 17.0% |
| E. durability sync | WAL sync | 11.4% |
| — | unattributed | 6.2% |

There is no single dominant stage. The gap comes from about 25 µs of serial CPU per touched leaf, spread over the whole pipeline, times 16 leaves per transaction. Sync is 11%. Even infinitely fast storage would give only 1.10×.

### Recommendation

Primary direction: **A — transaction-internal parallel per-leaf execution.** Extend the existing parallel executor, which today falls back on multi-leaf transactions (`parallel_fallback_multi_leaf`), so that the independent per-leaf work of one non-structural transaction runs on workers: leaf mutation, restamp, page encode, and the page-delta diff for that leaf. That covers about 22.5% + 12.6% + part of 8.4% ≈ 40% of coordinator time, all of it independent across the 16 leaves. The coordinator then keeps planning, WAL framing and sync, and publication.

Order of what follows, from the same data: B planner (21%, growing faster than linearly with group size) second, C catalog/publication (17%) third, E sync (11%) last. D (delta encode, 12.6%) should move into the per-leaf workers as part of A rather than be tuned on its own. Two caveats for A on this host:
- Two OCPUs cap it near 2× on the parallel part.
- The profile shows about 45% of cycles in memcpy, malloc/free and Arc refcounting spread across all stages, so per-leaf copy and allocation cost is the other lever if parallel speedup falls short.

### Files

`results/wal-v3-phase-c/`: `environment.txt`, `run-order.txt`, `process-metrics.jsonl`, progress logs, `raw/` (36 attribution rows, 6 dodb and 6 RocksDB confirmation rows, 2 perf rows, logs), `tables.md`, `analysis.json`, `perf/`, `scripts/run_phase_c.py`, `scripts/analyze_phase_c.py`, `scripts/summarize_perf.py`, `SHA256SUMS`.
