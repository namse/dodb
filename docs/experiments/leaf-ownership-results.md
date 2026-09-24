# Planned Blink Leaf Ownership Results

## Motivation

The direct page encoding run left 176.729 ms of measured width-1 clone work,
40.97% of physical execution. Most of that work came from copying a leaf page
into a private cache, copying its entry vector for each mutation, copying the
result back into the working state, and refreshing the cached copy at each
transaction boundary.

## Previous Duplicate Ownership

The planned serial path held one `BlinkPage` in `WorkingBlinkState.pages` and a
second `BlinkPage` in `CachedLeaf`. Mutations copied the entry vector again,
then installed another page copy and refreshed the cache after restamping.

## Design

### Working overlay ownership

`WorkingBlinkState.pages` owns the only mutable page copy for the batch. Its
`ensure_overlay_page()` helper copies a page from immutable committed state
only when that page first enters the overlay.

### PageId-only CachedLeaf

`CachedLeaf` stores only the leaf `PageId`. Cache range checks read the current
overlay-first page through `WorkingBlinkState`, preserving high-key and first
entry lower-bound checks.

### First-write COW

Committed `BlinkState` remains immutable until WAL success. The first planned
mutation of a committed leaf copies it once into `WorkingBlinkState`; later
mutations of that page reuse the overlay-owned copy. The `leaf_load_clone`
metric now measures only this base-to-overlay COW. Its historical field names
remain for artifact compatibility.

### In-place leaf mutation

Existing-key replacement and insertion mutate the overlay leaf's entries
directly. The code preserves provisional page LSN updates, exact leaf fit
checks, dirty tracking, value allocation, and freeing replaced overflow
chains. Normal leaf mutations no longer increment entry-vector or install
clone counters.

### Split ownership transfer

On insertion overflow, the planned path verifies the route before taking the
old high key and moving the entry vector out with `mem::take`. Existing split
helpers then install the left and right pages and parent separator. This avoids
a pre-split entry-vector clone while retaining split behavior.

## Correctness

The focused same-leaf transaction test verifies one base-to-overlay clone for
four mutations, zero entry/install/refresh clones, separate revisions and
values in each transaction's WAL page image, and the existing final read and
invariant checks. A focused oversized existing-key update test verifies that
an error leaves committed pages, root/free-list/high-water state, superblock,
slot, revision, LSN, batch ID, publication count, WAL commit count, and visible
contents unchanged.

At implementation commit `fd0c1029ad053a00ea99eef45f524f6ca795b289`, these
local correctness gates passed:

- `cargo fmt --all -- --check`
- `cargo test -p dodb-core`
- `cargo test -p dodb-storage`
- `cargo test -p dodb-storage --bin phase0-bench`
- `cargo test --workspace`
- `git diff --check`
- Existing planned randomized differential, leaf/internal/root split,
  planned WAL failure, WAL sync failure, parallel WAL failure, and pinned
  generation tests all passed.
- The recovery child suite passed 8/8 in both storage and workspace runs; no
  recovery child failed during this task.

Local width-1 smoke used 16 writers, `different-leaf-heavy`, working set and
cache 4096, disabled sync, zero delay, and 1 s warmup/duration. It reported
zero errors, overloads, full-state clones, entry clones, install clones, and
cached refresh clones. This is local smoke evidence only.

## OCI Method

The requested OCI width-1 and width-16 release measurements were not run. The
required precheck could not be completed: SSH to `opc@217.142.246.204` was
rejected with `Permission denied (publickey,gssapi-keyex,gssapi-with-gssapi,gssapi-with-mic)`
after attempts with the configured default identity and the local `id_rsa` and
`id_late_sh_ed25519` identities. Therefore the OCI worktree state, running
benchmark process state, release build, raw artifacts, and OCI backup path
remain unverified. No benchmark was started.

## Width-1 Result

The prior OCI median is retained as the comparison baseline. No OCI after
measurement exists, so throughput, processing, physical execution, mutation,
encoding, planning, WAL, catalog, and publication changes are not available.

| metric | before | after | change |
|---|---:|---:|---:|
| throughput (mut/s) | 17,964.27 | not measured | not available |
| processing (ms) | 1,891.10 | not measured | not available |
| physical execution (ms) | 431.334 | not measured | not available |
| physical mutation (ms) | 217.881 | not measured | not available |
| mutation residual (ms) | 89.449 | not measured | not available |
| physical restamp (ms) | 3.857 | not measured | not available |
| leaf load clone (ms) | 28.236 | not measured | not available |
| leaf entries clone (ms) | 40.149 | not measured | not available |
| leaf install clone (ms) | 60.047 | not measured | not available |
| cached refresh (ms) | 48.297 | not measured | not available |
| physical residual (ms) | 45.128 | not measured | not available |
| clone bundle (ms) | 176.729 | not measured | not available |
| physical page encode (ms) | 79.592 | not measured | not available |
| superblock encode (ms) | 36.580 | not measured | not available |
| planning (ms) | 397.507 | not measured | not available |
| WAL append (ms) | 606.015 | not measured | not available |
| WAL group encode (ms) | 475.750 | not measured | not available |
| catalog construction (ms) | 103.312 | not measured | not available |
| generation publication (ms) | 81.850 | not measured | not available |

## Clone Elimination

The local smoke and focused test reported zero entry-vector, install, and
cached-refresh clone counts. Their aggregate timings were also zero in the
smoke artifact. This does not substitute for validation on the requested OCI
primary run. The first-write base-to-overlay COW remains expected and was
reported 53,054 times in the one-second local smoke, equal to 1.0 COW per
planned mutation for that workload.

## Width-16 Check

The OCI width-16 check was not run. The previous comparison median is
18,016.82 mut/s; no after value or regression-gate result is available.

## Interpretation

The ownership structure and zero-clone behavior are validated locally. OCI
throughput and physical execution effects have not been measured, so this
experiment cannot be classified as strong success, structural success,
partial performance success, or failure under the defined OCI criteria.

## Next Bottleneck

There is no new OCI measurement from which to select the next bottleneck. The
prior direct-encoding report measured WAL append at 606.015 ms, including
475.750 ms of WAL group encoding, but this remains a previous-run observation.
Do not start the next optimization until the leaf-ownership OCI primary is
available.

## Allocator / Arena Assessment

**Insufficient evidence.** The requested OCI run that would expose the next
largest component was unavailable. This task added no allocator or arena
dependency. Long-lived Blink pages remain owned by their existing state and
generation structures.

## Experiment Status

Implementation commit: `fd0c1029ad053a00ea99eef45f524f6ca795b289`.

Implementation and local correctness work are complete. OCI performance
validation, artifact collection, and the corresponding performance
classification are incomplete because the OCI SSH identity was rejected.
Committed Blink state remains immutable until WAL success. Only batch-local
`WorkingBlinkState` ownership changed. Page format, split semantics, WAL
semantics, transaction ordering, and publication semantics are unchanged.
No arena was introduced. Phase 4 status is unchanged and Phase 5 has not
started.

## Artifacts

- Local smoke: `/tmp/dodb-leaf-ownership-smoke.jsonl` (local-only, not an OCI
  performance artifact).
- OCI width-1 and width-16 artifacts: not created.
- OCI backup path: not created; remote worktree and artifact state were not
  inspected.
