# Transaction semantic contract

The future executor is optimistic. During execution it records:

```text
read_set:  DocumentKey -> observed Revision
write_set: DocumentKey -> Put(bytes) | Delete
```

Writes also establish an observation for their key, so write/write conflicts
are validated even when a caller did not explicitly read the key. At commit,
every read-set revision is compared with the current committed revision. A
single mismatch rejects the transaction before any write is applied.

The Phase 0 `ReferenceDb` is the deterministic oracle for these semantics. It
uses a `BTreeMap<DocumentKey, RevisionState>`, keeps deleted keys as
`Missing(revision)`, gives each non-empty commit one synthetic monotonically
increasing LSN, and applies all writes with that one revision. It supports
point get/put/delete, same-pk ordered query with an exclusive sort-key cursor,
ordered scan with an exclusive document-key cursor, and limit.

This is semantic reference code, not a production concurrency-control engine:
there is no locking, MVCC, WAL, recovery, or range-read validation.

