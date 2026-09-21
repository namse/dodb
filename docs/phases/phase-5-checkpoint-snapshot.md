# Phase 5: Checkpoint, WAL reclamation, snapshot, and restore

## Durability-state investigation

Phase 4 publishes only WAL-durable page images. `BTreeStore::dirty_pages` is a
`BTreeMap<PageId, [u8; PAGE_SIZE]>`, so the latest committed image for each
page is retained once, including allocator, free-list, root, overflow, and
tree pages. `dirty_superblock` retains the latest committed alternate
superblock image. The existing `flush` writes those images, extends the file
to the current high-water page, and syncs the data file. It does not advance
`checkpoint_lsn` and it does not change the WAL.

The selected double-superblock copy is the valid copy with the greatest
generation. Ordinary commits preserve `checkpoint_lsn` and advance generation
in the alternate slot. The data file is therefore independently reopenable at
the last formal checkpoint, while retained WAL supplies later commits.

The WAL has one identity `INIT` record followed by strictly increasing record
LSNs. Page-image records consume LSNs and each logical `COMMIT` record supplies
the commit LSN used by revisions and page headers. Phase 5 WAL format version 2
extends `INIT` with `start_after_lsn`; version 1 WALs remain readable and use
zero as their history start. An empty WAL is initialized with the database
checkpoint as its history start.

## Checkpoint model

`BTreeStore::checkpoint` is a synchronous local primitive. `AsyncShard` exposes
the same operation through its bounded FIFO coordinator. A checkpoint operation
is ordered after already admitted mutations; mutations admitted after it stay
behind it. Collected mutations before and after `TransactGet`, `Checkpoint`, or
`Snapshot` barriers are committed as separate physical groups, and responses
for a completed group are released before a later barrier runs. Ordinary
immutable reads continue through the committed read view.

The checkpoint chooses the last durable WAL commit LSN, or the current
checkpoint LSN when there is no later commit. It rejects a decreasing boundary,
writes all committed dirty page images and the current committed superblock,
and syncs the database file before writing checkpoint metadata. It then writes
the alternate superblock with a higher generation and the selected checkpoint
LSN, syncs that metadata, runs the invariant checker, and only then resets the
WAL.

The checkpoint report contains the LSN, page count, bytes written, reclaimed
WAL bytes, and elapsed time. `bytes_written` counts every dirty data-page image
and every superblock image written by the checkpoint, including the pre-existing
dirty superblock and the new checkpoint metadata superblock. Repeated
checkpoints with no writes do not move the boundary or rewrite the WAL
unnecessarily.

## WAL reclamation and recovery

The local v1 policy always checkpoints the latest durable commit before
reclamation. WAL reset is performed while the write coordinator is paused:

1. truncate the WAL and sync the truncation;
2. write a new identity `INIT` with `start_after_lsn = checkpoint_lsn`;
3. sync the new initialization;
4. resume writes with the next record LSN equal to `checkpoint_lsn + 1`.

LSNs are never restarted at zero. Batch IDs are segment-local after reset; the
LSN sequence is the durable ordering authority.

Recovery reads the newest valid superblock, uses its checkpoint as the data-file
authority through N, and replays only complete committed WAL units with commit
LSN greater than N. Older WAL history is ignored. Page-LSN comparison remains
the idempotence guard for data pages. A crash before checkpoint metadata sync
leaves the previous checkpoint plus WAL authoritative. A crash after metadata
sync leaves the checkpointed database authoritative through N even if WAL reset
is incomplete; reset recovery can reopen an empty or old WAL and resume above
N.

## Snapshot format

A finalized snapshot directory contains:

```text
database
manifest
```

`database` is a byte-for-byte copy of the checkpointed database file. The
versioned textual manifest records the snapshot format, database UUID, tenant
ID, shard ID, shard epoch, database format, page size, checkpoint LSN, database
file size, whole-file CRC32C, and a manifest CRC32C. The physical identity is
preserved on restore; tenant and shard identity remain separate fields.

Snapshot creation uses a temporary sibling directory, copies and syncs the
database, writes and syncs the manifest, validates the temporary directory with
the normal superblock/page/B+Tree checker, syncs the temporary directory
metadata, and atomically renames it to the requested destination. The parent
directory is synced after the rename. Restore uses the same file-sync, atomic
rename, and parent-directory-sync ordering. Directory synchronization is
explicitly supported only on platforms where the filesystem exposes a
directory file descriptor; unsupported platforms return a snapshot error
instead of claiming durable finalization. Temporary artifacts use unique
process-local names, so stale artifacts from an interrupted attempt do not
block a later operation. A failed copy or validation removes the temporary
artifact on a best-effort basis and does not alter the live database.

`validate_snapshot` checks manifest integrity, file size, whole-file checksum,
identity, format, checkpoint metadata, and normal structural invariants. The
whole-file CRC32C is calculated with a bounded streaming buffer rather than a
database-sized allocation.
`restore_snapshot` requires a new destination path, validates the source,
copies the database to a temporary file, syncs and validates it with the normal
database codecs, and atomically installs the destination file.

The snapshot contains no mutable live-WAL reference. A compatible WAL beginning
after the snapshot checkpoint can be supplied separately and replayed by normal
database open/recovery.

## Fault and test coverage

Named fault points cover checkpoint gate acquisition, data page writes and sync,
alternate superblock writes and sync, WAL truncation, WAL initialization, WAL
reset sync, snapshot creation/copy/sync/manifest/directory finalization, and
restore copy/sync/directory finalization. If a reset leaves a prefix of the new
INIT, recovery recognizes only a prefix of the expected identity-bearing INIT,
reconstructs it from the durable checkpoint, and continues with LSNs greater
than that checkpoint. A complete but corrupted record remains an error. Before
initializing an empty WAL for an existing data file, open validates the data
file identity, so a wrong-identity open cannot persistently modify the WAL.
Deterministic tests reopen after every checkpoint reset boundary and run the
invariant checker.

The test suite includes:

- checkpoint flush, metadata ordering, repeated checkpoint, and monotonic LSN tests;
- checkpoint byte accounting including both written superblock images;
- a deterministic checkpoint fault matrix using volatile/durable file state;
- partial-persistence torn-INIT recovery tests at multiple frame boundaries;
- real subprocess abrupt-termination tests at page, metadata, and WAL-reset boundaries;
- snapshot copy failure cleanup and restore-copy failure cleanup;
- unique stale-temporary-artifact and large streaming-validation tests;
- FIFO coordinator grouping tests around transaction reads and checkpoints;
- malformed manifest and corrupted database rejection;
- snapshot-at-N plus compatible post-N WAL recovery;
- deterministic randomized checkpoint/snapshot/restore differential testing against `ReferenceDb`.

`phase5-bench` reports checkpoint LSN, dirty pages, bytes written, reclaimed WAL
bytes, snapshot copy and validation time, and restore time for small and medium
local databases. One local run produced:

| Rows | Checkpoint pause | Pages | Bytes | WAL reclaimed | Snapshot bytes | Copy | Validation | Restore |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 32 | 25.8 ms | 4 | 24,576 | 284,784 | 24,576 | 14.2 ms | 0.9 ms | 8.9 ms |
| 512 | 24.3 ms | 28 | 122,880 | 4,506,672 | 122,880 | 16.4 ms | 5.1 ms | 20.6 ms |

These are local observations, not compatibility thresholds. The benchmark
intentionally measures the portable full-copy pause rather than relying on
reflink support. In the concurrent 8-client, 512-operation run, checkpoint
request-to-completion was 77.3 ms, of which the checkpoint operation reported
32.1 ms and the bounded coordinator wait accounted for about 45.3 ms.

## Limitations

Phase 5 is local and single-shard. Snapshot creation pauses writes for the
checkpoint and full database copy. There is no remote object storage,
replication, public PITR API, distributed checkpoint, background checkpoint
policy, online shard migration, or network endpoint.
