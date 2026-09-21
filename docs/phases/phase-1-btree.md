# Phase 1: Single-file B+Tree storage engine

## Scope

Phase 1 provides a correct, testable, single-shard mutable B+Tree over the
canonical `(pk, sk)` encoding from Phase 0. It implements get, unconditional
upsert, delete, same-primary-key query, ordered scan, clean reopen, page
cache, reusable overflow/free pages, and an offline invariant checker.

It does not implement WAL, crash recovery, transactional commit, MVCC,
snapshotting, networking, or background compaction. Direct page publication
is synchronized for deterministic clean-reopen tests, but Phase 1 does not
claim crash-safe commit semantics.

## Legacy implementation relationship

The legacy `NamseEnt/namseent` `luda-editor/new-server/bptree` implementation
was inspected as proven architectural experience. dodb reuses its useful
ideas: 4 KiB pages, mutable leaf/internal splits, recursive root growth,
right-linked leaves, variable-sized values, free-page reuse, page caching,
operation-local page overlays, and a single async coordinator that collects a
short bounded batch.

dodb changes those ideas to use checked decoders, explicit magic/version
fields, Phase 0 page IDs and CRC32C checksums, page LSN fields, canonical
binary keys, structured Phase 0 errors, persistent tombstone revisions, and a
full invariant checker. The legacy fixed `u128` layout, assertions on disk
bytes, and WAL/shadow-file behavior are not carried over. dodb files are not
binary-compatible with the legacy files.

## Tree algorithms

Internal separators use one rule everywhere: a separator is a partition
boundary; the left child contains keys strictly less than it and the right
child contains keys greater than or equal to it. Lookup selects the right
child for `key >= separator`. A leaf split chooses a byte-fitting boundary,
links the new right leaf, and promotes the first right key. Internal splits
promote the separator between the two resulting internal nodes. A split at
the root creates a new root. Deletes replace a live value with a tombstone
and release overflow pages; they intentionally do not merge or rebalance
underfull leaves or internal pages.

The leaf chain is used for ordered scan and for same-`pk` query traversal.
Queries begin at the physical lower bound for the requested primary key,
apply an exclusive sort-key cursor, stop after leaving that primary key, and
skip tombstones. Scans use the same exclusive rule for the complete document
key.

## Page and allocator layout

Pages 0 and 1 are the Phase 0 superblock copies. Page 2 is the initial root.
The high-water field is the last allocated page ID, and the free-list field is
the head of a checked singly linked stack of `Free` pages. Leaf and internal
records are variable-length slotted records. Overflow pages carry explicit
chain and total-length metadata; the maximum value is 64 MiB and the inline
threshold is 512 bytes. The maximum canonical encoded document key is 3992
bytes. A value chain is bounded, cycle-checked, and reclaimed on replacement
or deletion.

Every persistent page is encoded through the Phase 0 full-page codec, so the
page ID, type, version, LSN field, and CRC32C are checked before its
page-specific layout is decoded. Existing non-empty corrupt files are never
reinitialized.

## Mutation boundary and async execution

`prepare_batch` constructs a private overlay over committed/cache pages. It
returns full encoded images for every changed page, read pages, the resulting
allocator/root state, the alternate superblock image/slot, and logical
responses. `publish_prepared` is the only direct data-file publication
boundary. It writes pages, writes the alternate generation superblock,
synchronizes the data file, and then updates the cache.
If preparation fails, no overlay state is published.

The optional `AsyncShard` owns a bounded Tokio request queue and one
coordinator. It collects at most 64 requests for up to 1 ms, executes them in
queue order against one private storage batch, and replies in request order.
This establishes the future group-commit shape without pretending that the
Phase 1 direct publisher is a WAL commit protocol.

## Correctness coverage

The storage crate covers empty-tree insertion, leaf and internal splits, root
growth beyond two levels, binary keys, tombstone revisions, overflow
replacement/reuse, cache capacities 0/1/small/normal, malformed page
rejection, prepared-batch isolation, and clean reopen. The testkit contains
deterministic differential tests against `ReferenceDb` using seeds
`0x5eed_cafe`, `0x0123_4567_89ab_cdef`, and `0xd0db_2026_0001`, with 5000
operations per seed, periodic invariant checks, and clean reopens.
