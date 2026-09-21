# dodb architecture overview

Phase 0 establishes contracts for a transactional document/KV database whose
logical key is `(tenant, (pk, sk))` and whose value is opaque bytes. The
current repository contains contracts and test oracles only; it does not yet
contain a storage engine or a transaction executor.

The planned v1 deployment is one compute process with one file per shard. A
tenant may later span multiple shards, so `TenantId` and `ShardId` remain
separate types and routing stays outside the physical document key.

## Workspace

- `dodb-core`: strong identifiers, revision state, key codec, semantic error
  and transaction-set types.
- `dodb-storage`: fixed-page/header and double-superblock codecs plus the
  `DurableFile` contract and a thin production file adapter.
- `dodb-testkit`: deterministic volatile/durable file simulation, named crash
  points, and the simple reference database/transaction oracle.

The future engine is a mutable fixed-4KiB slotted-page B+Tree with inline
small values and overflow pages. Durability will use redo-only full-page
after-image WAL, NO-STEAL, and NO-FORCE. Those components are intentionally
not present in Phase 0.

