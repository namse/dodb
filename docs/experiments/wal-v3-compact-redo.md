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

## Phase B — compact PageDelta redo (in progress)

Code: `4ac3a2e` (WAL format 3, PageDelta record, v3 commit digest, streaming scan and per-page recovery), `6bcf8bb` (planned Blink producer, checkpoint fault points, tests), `11639db` (redo counters survive WAL reset), `def6f7e` (bench checkpoint control). Format, full-image-first rule and recovery: `results/wal-v3-phase-b/format-specification.md`. Raw results: `results/wal-v3-phase-b/`.

Done so far:

- B0 encoded-size probe (`b0-encoded-size-probe.jsonl`): existing-key width-1 update 4,224 → 226.9 B/tx (delta frame ~159 B, 4 spans, ~73 changed bytes); width-16 update 2,596 B/tx; delete/insert ~1,190 B/tx (record shifts); first touch after checkpoint is a full image.
- BTree WAL and data file bytes unchanged (`byte-probe.txt`).
- `cargo test --workspace --release --no-fail-fast`: 212 passed, 0 failed, 1 ignored (the B0 probe).
- OCI first gate (4 scenarios × 3, interleaved): page-delta / Phase A GM 1.781.
- Full 14-scenario matrix (× 3, interleaved): multiwriter GM page-delta / Phase A 1.853, / ExactMain 3.605 (reused), / RocksDB 0.704 (reused, not interleaved). Details in `tables.md`.

Not done yet: same-session RocksDB confirmation, 120 s sustained runs, checkpoint control (runner phases `confirm`, `sustained`, `checkpoint` exist in `scripts/run_phase_b.py`; the checkpoint binaries still need to be built on OCI).
