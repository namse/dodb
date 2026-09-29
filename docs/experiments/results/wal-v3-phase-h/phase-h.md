# Phase H — CRC32C scan elimination

Base: `a78472377fa16ed491c18bd2d6770f7dba7e9311` (`experiment/wal-v3-compact-redo`). OCI host: A1, 2 OCPU, AArch64, `/bench/zfs/db` on the `dodbbench/db` ZFS dataset. G0 remains the Phase G mimalloc-only source and allocator selection.

## H0 hardware CRC verification

The baseline and H1 binaries were built on the OCI A1 host with rustc 1.98.1 for `aarch64-unknown-linux-gnu`. `RUSTFLAGS`, `CARGO_ENCODED_RUSTFLAGS`, and `CARGO_BUILD_RUSTFLAGS` were empty. The repository target config supplied `-C target-feature=+crc`; the `crc32c 0.6.8` rustc command also had `cfg armsimd`. `/proc/cpuinfo` reports `crc32`. `objdump` found `crc32cb` and `crc32cx` in both binaries; `crc32ch` and `crc32cw` were not present. The binaries therefore use the AArch64 hardware CRC backend rather than the software fallback.

The dependency stayed on `crc32c = 0.6` (locked to 0.6.8). Build commands, environment, rustc invocations, and instruction extracts are under `hardware/`. G0 binary SHA256 is `f63694a4b49f3485ac2b4af79aadb225800488a7a69a5a3e5a519d79bc112964`; H1 binary SHA256 is `1ae1b9240d6f2aeee74a2e62204f5dc7e918d0abd15f871fecc11cfc4390d5f8`.

## H0 CRC attribution

The baseline profile recorded 27,652,006,533 cycle events over 29,760 successful transactions: about 929,167 cycles/transaction. CRC32C instruction/backend samples summed to 10.72% of samples, about 99,607 estimated CRC cycles/transaction. The earlier Phase G profile estimate was about 11.2%; the H0 profile is consistent with that attribution within run-to-run sampling variation.

The following call counts and bytes are derived from the Phase G0 storage counters for 64 writers, width 16, uniform, sync-disabled. Full-page byte counts use 4,096 bytes per CRC scan. Timing columns are the closest existing stage timers; the timers around page encode/base/delta include surrounding work and are not represented as isolated CRC-only timers.

| Site | Calls per transaction | Bytes hashed per transaction | Observed timing |
|---|---:|---:|---:|
| A. `finalize_encoded_page()` canonical page checksum | 15.98 full-page calls | 65,464 | Included in worker encode; 29.22 µs/tx for the entire worker encode stage |
| B. Fresh encoded image fingerprint | 15.98 full-page calls | 65,464 | Included in worker delta preparation; 32.04 µs/tx for the entire worker delta stage |
| C. Existing base page chain validation | 15.13 full-page calls | 61,980 | 26.03 µs/tx in worker base stage, including fallback encoding where needed |
| D. WAL redo payload checksum | 15.98 redo calls; small PageDelta payloads dominate | 1,704 payload bytes plus rare image bytes | 1.315 µs/tx in WAL page-payload CRC timer |
| E. Commit digest | 15.98 record updates; 4 CRC operations per format-3 record (type, index, length, payload) | 1,848 including digest metadata | 1.719 µs/tx in commit-digest CRC timer |
| F. WAL frame/header and commit-payload CRC | 16.98 frame-header calls plus one commit-payload call | 815 header bytes plus 16 commit-payload bytes | 0.796 µs/tx header timers plus 0.040 µs/tx commit-payload timer |
| G. Open/recovery/checkpoint validation | Per-open work, outside the measured transaction denominator | Full page/frame validation remains enabled | Not included in hot-loop per-transaction timers |

A, B, and C share `run_leaf_chain_job` and the hardware CRC kernel in the profile. The counters establish call and byte counts; the sampling profile does not reliably split cycles among those three call sites. The H0 raw JSONL, perf report, and perf sample data are retained under `raw/g0/` and `perf/`.

## H1 cached trusted page fingerprint

H1 wraps each immutable base image with its page LSN and the raw full-image fingerprint already recorded in the WAL page chain. Before delta preparation it checks the wrapper metadata against the chain entry and checks the image page ID and LSN. A mismatch returns an invariant error before WAL append. The worker then carries this immutable image/fingerprint pair through the next update. If no cached image exists, the original encode-and-rehash path remains.

The fingerprint remains the existing CRC32C of the full 4 KiB image. The embedded canonical checksum cannot replace it in the PageDelta base field without changing the format-3 payload and WAL bytes. Public append validation, fault-injection validation, data-file decode, WAL open/recovery, torn-input handling, and persisted page checksums were left intact.

On the 64w width16 uniform sync-disabled workload, H1 removed about 15.13 full 4 KiB base scans per transaction: 61,980 CRC bytes/tx removed. The scan count went from about 47.10 to 31.97 full-page scans/tx, a 32.1% reduction. Across the paired profile, estimated CRC work fell from about 99,607 to 60,870 cycles/tx (10.72% to 6.78% of samples). The base-stage timer fell from 26.03 to 15.42 µs/tx in the same-session CPU gate.

| Paired 3-run CPU gate | G0 median tx/s | H1 median tx/s | H1/G0 | G0 cycles/tx | H1 cycles/tx |
|---|---:|---:|---:|---:|---:|
| 64w width16 uniform, sync-disabled | 5,974 | 6,254 | 1.047x | 926,527 | 865,193 |
| 64w width16 uniform, real-sync | 4,589 | 4,582 | 0.999x | 1,001,539 | 982,519 |
| 64w width1 uniform, real-sync | 22,884 | 23,104 | 1.010x | 112,717 | 112,257 |
| 64w width1 uniform, sync-disabled control | 55,841 | 62,399 | 1.117x | 83,902 | 75,115 |

H1 clears the CPU gate. The real-sync width16 result is sync-limited and has nearly unchanged throughput while its measured cycles/transaction are lower.

## H2 payload CRC reuse in commit digest

The equivalence test covers 4,096 randomized cases with multiple records, mixed PageImage/PageDelta records, 159-byte and large PageDelta payloads, 4 KiB PageImage payloads, multiple payload segments, legacy/current format versions, and zero/max/varied record indexes. It compares exact `u32` digests after every record.

The candidate computes the same metadata digest, then combines it with the already computed payload CRC using `crc32c_combine`. Equivalence passed, but the OCI timing rejected H2: for 50,000 records, direct versus combine was 72 ns versus 7,967 ns at 159 bytes, 544 ns versus 14,586 ns at 4,079 bytes, and 542 ns versus 16,155 ns at 4,104 bytes. The combine overhead outweighs the avoided second payload pass by a wide margin. No production digest code changed. WAL SHA256 identity remains covered by the G0-to-H1 public-API byte comparison.

## H3 remaining canonical page checksum

Skipped. H1 perf samples attribute about 2.14% of total cycles to canonical page encoding/checksum work, below the 5% CPU threshold. No incremental page checksum prototype was implemented. Persisted page checksums remain unchanged.

## Six-scenario durable comparison

The six required scenarios were run with G0/H1 interleaved on the same OCI host, real sync, three repetitions, and identical paired seeds. `same-leaf-heavy` is the compact-locality control; `different-leaf-heavy` is the spread-locality control.

| Scenario | G0 median tx/s | H1 median tx/s | H1/G0 | G0 cycles/tx | H1 cycles/tx |
|---|---:|---:|---:|---:|---:|
| 16w width1 uniform | 8,027 | 8,173 | 1.018x | 209,435 | 208,465 |
| 16w width16 uniform | 3,028 | 2,989 | 0.987x | 1,169,036 | 1,179,165 |
| 64w width1 uniform | 22,918 | 23,168 | 1.011x | 113,133 | 110,328 |
| 64w width16 uniform | 4,609 | 4,572 | 0.992x | 978,184 | 973,024 |
| 64w width16 compact | 14,506 | 14,408 | 0.993x | 236,417 | 239,151 |
| 64w width16 spread | 9,268 | 9,321 | 1.006x | 299,175 | 298,796 |

Both width1 durability checks exceed the 0.98 floor. The 64w width16 real-sync ratio is below the 1.04 full-matrix gate, so the 14-scenario matrix and same-session RocksDB rerun were not triggered. This follows the measured gate; no RocksDB result is claimed for H.

## Correctness and byte identity

The H1 trusted-image unit test covers fresh encoding, cached fingerprint equality with the old full rehash, and wrong cached LSN, fingerprint, and page association rejection. Existing corruption/recovery and fault-injection tests remain enabled. The final OCI `cargo test --workspace --release --no-fail-fast` log is `correctness/workspace-release.log`; the H2 randomized test log is `correctness/h2-equivalence.log`.

The deterministic public-API comparison exercises transaction outcomes, commit LSNs, reads/queries/scans, reopen scans, data SHA256, and WAL SHA256 across 12 configurations. H1 output matches the Phase G0 output byte-for-byte; `correctness/g0-h1-byte-identity.diff` is empty.

## Next measured bottleneck

The largest remaining single storage-owned profile category is `encode_page_delta()` at 10.38% combined samples across coordinator/worker threads. It exceeds the remaining CRC kernel share of 6.78%. This is the one next investigation selected from the H1 profile.

The Phase G production allocator note remains outstanding: `dodb-server` is library-only in this repository, so the eventual production executable must choose mimalloc at its own binary boundary. No allocator was added inside `dodb-storage`.
