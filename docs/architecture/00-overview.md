# dodb architecture overview

Phase 0 established contracts for a transactional document/KV database whose
logical key is `(tenant, (pk, sk))` and whose value is opaque bytes. Phase 1
adds a checked single-file B+Tree storage engine for one shard. Transaction
execution, WAL, and crash recovery remain future phases.

The planned v1 deployment is one compute process with one file per shard. A
tenant may later span multiple shards, so `TenantId` and `ShardId` remain
separate types and routing stays outside the physical document key.

## Workspace

- `dodb-core`: strong identifiers, revision state, key codec, semantic error
  and transaction-set types.
- `dodb-storage`: fixed-page/header and double-superblock codecs, the
  `DurableFile` contract and production adapter, and the Phase 1 B+Tree,
  allocator, overflow storage, cache, invariant checker, and async shard
  facade.
- `dodb-testkit`: deterministic volatile/durable file simulation, named crash
  points, and the simple reference database/transaction oracle.

The Phase 1 engine is a mutable fixed-4KiB slotted-page B+Tree with inline
small values and overflow pages. Its direct publisher is suitable for clean
reopen testing but is not crash-safe. Future durability will use redo-only
full-page after-image WAL, NO-STEAL, and NO-FORCE; those components are not
present yet.
