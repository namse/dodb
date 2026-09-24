# Direct Blink Page Encoding Results

## Motivation

After allocation-free canonical key validation, actual Blink page encoding
accounted for 142.722 ms, or 29.11% of width-1 physical execution. This
experiment writes fixed-size Blink pages directly into their final 4096-byte
images to remove intermediate buffers and copies.

## Previous Encoding Path

Leaf encoding allocated a `BODY_SIZE` vector, one vector per leaf record, and
a slot vector, then copied the body into another vector. `encode_page()` then
allocated the final page and copied the body again. Page checksum encoding
copied all 4096 bytes into a scratch buffer before CRC32C.

Internal, overflow, and free page bodies each allocated a `BODY_SIZE` vector.
Internal encoding also allocated a slot vector. Leaf fit checks already use
`leaf_body_layout()` and do not serialize a body.

## Direct Fixed-Buffer Design

### Page body direct writes

`encode_blink_page()` now creates one zeroed `[u8; PAGE_SIZE]` and passes its
body slice to page-type-specific writers. Overflow and free bodies are written
into that slice directly.

### Leaf record direct writes

`encode_leaf_body_into()` reuses `leaf_body_layout()` for checked offsets,
sorted-entry validation, high-key validation, and fit checks. It writes the
leaf header and canonical high key directly into the page body.
`encode_leaf_record_into()` writes the existing record fields and payload
directly into each record's final range while retaining encoded-key validation.

### Slot direct writes

Leaf and internal encoders walk entries in reverse to place packed records at
the end of the body. Each slot is written directly at the original entry
index; no slot vector is created. Internal encoding reuses
`internal_body_layout()`.

### Page header finalization

`finalize_encoded_page()` validates the page format version and writes the
existing magic, version, type, ID, LSN, flags, and checksum fields. Public
`encode_page(header, body)` remains available and now copies the body into a
zeroed fixed page before calling the same finalizer.

### Checksum without encode-side scratch copy

The finalizer zeros checksum bytes, computes CRC32C directly over the 4096-byte
page, and stores the result. `decode_page_at()` and its checksum verification
path are unchanged.

Production Blink encoding no longer allocates a `BODY_SIZE` vector, leaf
record vectors, a slot vector, or `body.to_vec()`. It also removes the
body-to-page full copy and encode-side checksum scratch copy. The legacy
allocation-based encoder exists only under `cfg(test)` as the differential
reference.

## Byte Compatibility

The direct encoder produced byte-identical output to the retained reference
encoder for Leaf, Internal, Overflow, and Free pages. Shape coverage includes
empty and minimal pages, inline values, many entries and separators, a
tombstone, an overflow reference, high keys, right siblings, different
internal levels, and near-full leaf and internal pages.

An additional 800 fixed-seed randomized valid Blink pages matched byte for
byte. Each of the 810 differential cases also decoded to the same logical
`BlinkPage`. The public generic `encode_page()` has a separate byte comparison
against its previous encoding semantics.

## Correctness

Passed locally:

- `cargo fmt --all -- --check`
- `cargo test -p dodb-core`
- `cargo test -p dodb-storage`
- `cargo test -p dodb-storage --bin phase0-bench`
- `cargo test --workspace`
- `git diff --check`
- Planned-Blink local width-1 smoke: 16 writers, different-leaf-heavy,
  working set and cache 4096, sync disabled, zero delay, 1 s warmup and
  duration, one repetition; errors 0, overloads 0, full-state clones 0, and
  state-clone nanoseconds 0.

The initial parallel storage-test invocation had two recovery child tests fail
to reach their expected boundary. The recovery integration tests passed 8/8
when rerun serially, and the complete storage and workspace suites passed on
subsequent runs.

## OCI Method

The release benchmark ran at implementation commit
`8782790cd5344f47b28e62399f8716b02f107ad2` on the 2-OCPU OCI A1 host. Width 1
used 16 writers, transaction width 1, different-leaf-heavy distribution,
working set 100,000, cache 4,096, 16-byte keys, 64-byte values, group limit
64, group byte limit 4 MiB, queue 256, zero collection delay, disabled sync,
two Tokio workers, 1 s warmup, 2 s duration, three repetitions, and seed
`0x3a042026`. Width 16 used the same settings with transaction width 16,
uniform distribution, and seed `0x3a032026`.

These are sync-disabled CPU and engine diagnostics. The benchmark repository
was on `/dev/mapper/ocivolume-root`, a 30 GB XFS root filesystem. The 200 GB
device was not mounted as the benchmark filesystem, so these are not
durability results or measurements on that volume.

## Width-1 Result

Before values are the medians from the allocation-free key-validation
artifacts. After values are medians from the three direct-encoding runs. Times
are milliseconds unless the metric is throughput.

| metric | before | after | change |
|---|---:|---:|---:|
| throughput (mut/s) | 17,683.23 | 17,629.63 | -0.30% |
| processing | 1,891.908 | 1,889.516 | -0.13% |
| physical execution | 490.236 | 431.832 | -11.91% |
| physical mutation | 214.466 | 211.257 | -1.50% |
| mutation residual | 86.661 | 85.303 | -1.57% |
| physical restamp | 3.791 | 3.684 | -2.83% |
| physical page encode | 142.722 | 82.583 | -42.14% |
| physical superblock encode | 35.565 | 41.264 | +16.03% |
| planning | 398.405 | 395.312 | -0.78% |
| WAL append | 576.668 | 619.449 | +7.42% |
| WAL group encode | 442.159 | 493.217 | +11.55% |
| catalog construction | 99.825 | 100.931 | +1.11% |
| generation publication | 77.636 | 80.032 | +3.09% |

Mutation residual is physical mutation minus the three leaf clone medians,
floored at zero. Page encode reduction is `1 - 82.583 / 142.722 = 42.14%`.
Width-1 throughput speedup is `17,629.63 / 17,683.23 = 0.997x`. Physical
execution fell by 11.91%.

## Physical Breakdown

Clone timers are nested within physical mutation. Cached refresh is shown as
its own physical category. These rows are measured independently and their
medians need not sum exactly to physical execution.

| component | before (ms) | after (ms) | change |
|---|---:|---:|---:|
| physical mutation | 214.466 | 211.257 | -1.50% |
| └ leaf load clone | 28.995 | 27.470 | -5.26% |
| └ leaf entries clone | 42.035 | 40.547 | -3.54% |
| └ leaf install clone | 56.775 | 57.937 | +2.05% |
| mutation residual | 86.661 | 85.303 | -1.57% |
| physical restamp | 3.791 | 3.684 | -2.83% |
| physical cached refresh | 46.775 | 46.524 | -0.54% |
| physical page encode | 142.722 | 82.583 | -42.14% |
| physical superblock encode | 35.565 | 41.264 | +16.03% |
| physical residual | 46.918 | 46.520 | -0.85% |

Leaf load clone, leaf entries clone, leaf install clone, and cached refresh
total 172.478 ms after the change, or 39.94% of physical execution. The
largest top-level physical category remains physical mutation at 211.257 ms.

## Width-16 Check

| metric | before | after | change |
|---|---:|---:|---:|
| throughput (mut/s) | 16,902.53 | 17,425.13 | +3.09% |

The width-16 regression gate passed. All three records reported zero errors,
overloads, full-state clones, and state-clone nanoseconds.

## Interpretation

The page-encoding target fell by 42.14%, exceeding the 15% measurable
improvement threshold. Throughput was 0.30% below the baseline, so the result
does not meet the structural-success throughput threshold. Classification:
**partial**.

Physical mutation changed by -1.50%. Leaf load and entry clone timers decreased
by 5.26% and 3.54%; leaf install clone increased 2.05%; cached refresh
decreased 0.54%. The largest unrelated movement above 15% was superblock
encoding at +16.03%, recorded as run variance. WAL group encode changed
+11.55%, below that variance threshold.

## Next Bottleneck

For physical execution, the clone and cached-refresh total is 39.94%, above the
30% threshold. The next engineering priority is leaf ownership and clone
elimination. This experiment did not change leaf ownership.

Among top-level processing categories, WAL append is largest at 619.449 ms;
WAL group encode is 493.217 ms and is an attribution within WAL append. WAL
encoding was not changed.

## Experiment Status

Implementation commit: `8782790cd5344f47b28e62399f8716b02f107ad2`.
Classification: partial. The page format is unchanged. Direct encoding
produces byte-identical 4096-byte page images. B-link split policy, batching
semantics, WAL semantics, and recovery format are unchanged. Existing Phase 4
status is unchanged; this work did not continue Phase 4 or start Phase 5.

## Artifacts

- Width 1: [`planned-direct-encoding-width1.jsonl`](results/oci-a1-2ocpu-12g-200g/direct-encoding/planned-direct-encoding-width1.jsonl), SHA256 `8a88f34f67fa75a047d78cedd7d8c494a0df0b663027ccc602ca1be29966e1e8`
- Width 16: [`planned-direct-encoding-width16.jsonl`](results/oci-a1-2ocpu-12g-200g/direct-encoding/planned-direct-encoding-width16.jsonl), SHA256 `60ac47b0af2413a94b1b0c477af4ff21e08ac4743189a1912e6ad4d5c6d27945`

Each artifact contains three validated records at the implementation SHA.
Their local SHA256 values match the OCI files. The OCI raw files were moved to
`/home/opc/dodb-oci-artifacts-direct-encoding-8782790/` after copying.
