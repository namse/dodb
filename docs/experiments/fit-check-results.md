# Allocation-Free Blink Fit Check Results

## Motivation

The physical execution attribution identified `leaf_fits()` as a leading
candidate inside the width-1 mutation residual. A normal leaf mutation asks
whether the updated entries fit before encoding the actual page image.

## Existing Problem

`leaf_fits()` called `encode_leaf_body()` and discarded the result. This
allocated and populated a page body, allocated each leaf record, allocated the
slot list, and copied the finished body. `internal_fits()` used the same full
encoding path. Neither fit check needs serialized bytes.

## Design

### Exact leaf layout calculation

`leaf_body_layout()` validates sorted canonical leaf keys, accounts for the
header, slots, optional high key, and each record length, and checks the lower
and upper regions for overlap. `leaf_record_encoded_len()` accounts for the
fixed record header, encoded key, and inline value bytes; tombstones and
overflow references have no inline value bytes.

### Exact internal layout calculation

`internal_body_layout()` validates the leftmost child and sorted canonical
separators, then calculates header, slots, optional high key, and separator
record lengths with checked arithmetic.

### Encoder equivalence

`leaf_fits()` and `internal_fits()` now call the layout calculators. The
right-sibling and level arguments remain in the existing signatures because
they do not affect fit. The page encoders and their output bytes are unchanged.

## Correctness

The leaf differential covers empty and single-entry leaves, inline values,
tombstones, overflow references, high keys, invalid ordering, invalid high
keys, and a constructed exact-capacity value plus one extra byte. The internal
differential covers single and many separators, high keys, invalid child IDs,
invalid ordering, invalid high keys, and the largest fitting collection plus
one additional separator. Layout offsets are compared with the encoded header.

A fixed-seed test checks 500 randomized leaf and internal layouts, including
valid and invalid ordering, varying key/value lengths, optional high keys, and
invalid child IDs. All layout acceptance results matched the existing
encoders.

Passed locally:

- `cargo fmt --all -- --check`
- `cargo test -p dodb-storage` (92 tests)
- `cargo test -p dodb-storage --bin phase0-bench` (10 tests)
- `cargo test --workspace`
- `git diff --check`
- `cargo build --release -p dodb-storage --bin phase0-bench`

The requested local debug smoke completed one planned-blink width-1
different-leaf-heavy run with 16 writers, working set and cache 4096, disabled
sync, 1 s warmup, and 1 s duration. It reported zero errors, zero overloads,
zero full-state clones, and zero state-clone nanoseconds. This smoke is not
performance evidence.

## OCI Method

The OCI benchmark has not run. `ssh opc@217.142.246.204` was rejected with
`Permission denied (publickey,gssapi-keyex,gssapi-with-mic)`, including a retry
with the separately named local SSH identity. Consequently, the required remote
worktree, running-process, filesystem, and mounted-volume prechecks were not
available. No release artifact or benchmark output was copied to the host.

## Width-1 Result

The physical-attribution result is the comparison baseline. The after value is
unmeasured until the OCI run can be performed.

| metric | before | after | change |
|---|---:|---:|---:|
| throughput | 15,222.51 mut/s | not measured | not measured |
| physical execution | 619.932 ms | not measured | not measured |
| physical mutation | 319.628 ms | not measured | not measured |
| mutation residual | 209.696 ms | not measured | not measured |
| page encode | 185.561 ms | not measured | not measured |
| superblock encode | 31.052 ms | not measured | not measured |

## Physical Execution Breakdown

No new physical execution measurements are available. Existing component
medians remain the baseline only; no new largest component or next measured
engineering priority can be determined from this task.

## Width-16 Regression Check

The width-16 control did not run. Its baseline was 14,575.44 mut/s, with
591.779 ms physical execution, 392.672 ms physical mutation, 228.007 ms
mutation residual, and 183.652 ms page encoding.

## Interpretation

The implementation removes the duplicate full-body serialization from both
fit checks and the differential tests establish matching acceptance for the
tested cases. Performance success is unclassified because neither OCI workload
ran. The configured strong, structural-success, partial, failure, and
width-16 regression gates have not been evaluated.

## Next Bottleneck

Rerun only the specified width-1 primary and width-16 control after restoring
authorized SSH access and completing the remote prechecks. Do not infer a new
bottleneck or start another optimization before those measurements.

## Experiment Status

B-link layout, split selection, page and WAL formats, batching semantics, and
WAL semantics are unchanged. The implementation commit is
`f168b40074f1d9578015f11d258a9d609790079d`. The experiment is incomplete while
OCI measurements and artifacts are unavailable. Phase 4 was not changed or
started by this task; Phase 5 has not started.

## Artifacts

- Local debug smoke: `/tmp/dodb-fit-check-smoke.jsonl`; smoke only, not
  committed performance data.
- OCI width-1 and width-16 artifacts: not created.
- OCI raw-artifact backup: not created.
