# Phase 6: Native service, QUIC protocol, and Rust client

## Scope

Phase 6 exposes the existing local dodb semantics through raw QUIC and a
first-party async Rust client. The implementation does not add HTTP/1.1,
HTTP/2, HTTP/3, REST, SQL, fn0 integration, authentication, tenant
authorization, replication, remote WAL, distributed transactions, or admin
checkpoint/snapshot RPCs.

## Dependency and service structure

The workspace dependency graph is:

```text
dodb-core
    -> dodb-service
    -> dodb-protocol
        -> dodb-client
        -> dodb-server -> dodb-storage
```

`dodb-core` remains the semantic source for opaque document keys, revisions,
revision states, transaction conditions, transaction mutations, and
transaction requests. `dodb-service` defines transport-independent request and
response values plus:

```rust
trait DodbService {
    fn execute(&self, tenant: TenantId, request: Request) -> ServiceFuture;
}
```

The trait returns a boxed future so a QUIC server, an in-memory test double,
and a future host adapter can share the same boundary. It carries a tenant for
routing only; authorization is not part of dodb.

The local server service uses a process-lifetime cache of opened shards with a
configurable maximum. The v1 resolver maps one tenant to one distinct
`ShardId` value, while retaining separate tenant and shard types so a later
routing implementation can change the mapping without changing the wire
protocol.

## Binary protocol

Every message uses this big-endian frame:

```text
magic          4 bytes: DODB
version        u16: 1
message type   u8: request or response
payload length u32
payload        payload length bytes
```

The codec checks the magic, version, message type, checked length arithmetic,
payload length, operation discriminants, count limits, boolean flags, response
types, and trailing bytes. Requests use explicit operation codes for Get, Put,
Delete, Query, Scan, Batch, TransactGet, and Transact. Responses use the same
operation code for successful results and a separate status for structured
application errors.

Batch is an atomic condition-free collection of Put/Delete mutations. The
existing transaction primitive remains the general atomic operation with
conditions. The protocol does not expose B+Tree pages, WAL records, page LSNs,
superblock slots, checkpoint state, or snapshot implementation details.

The default network limits are:

- 68 MiB request frame and 68 MiB response frame, including the header;
- 64 MiB maximum value, matching the storage maximum;
- 3,990 bytes per key component;
- 4,096 point keys;
- 256 conditions and 256 mutations;
- 4,096 Query or Scan rows per response;
- 4,096 bytes of human-readable error detail.

The decoder validates declared frame length before allocating the payload, and
all nested counts and byte lengths are checked against these limits. A caller
may lower the limits but cannot raise key or value limits above the storage
contract through `ProtocolLimits`.

## QUIC and TLS

Quinn 0.11 provides one reusable connection per client. Each application
request opens one bidirectional stream, writes one request frame, finishes the
send side, and reads one response frame. Independent streams are concurrent;
the client does not serialize them behind a connection mutex and does not
create a connection per operation.

The server has configurable connection and active-stream semaphores. A full
connection or stream budget rejects additional work, and a full `AsyncShard`
coordinator is returned as the structured `Overloaded` application category.
QUIC transport failure remains distinct from an application error response.

The server accepts a certificate chain and private key in DER or PEM form.
The client requires one or more trusted root certificates and passes the
configured server name to Quinn for certificate-name validation. Client
certificate authentication is not enabled in this phase.

## Tenant routing and lazy creation

The v1 physical policy is one tenant database file pair:

```text
TenantId -> resolver -> ShardId -> tenant-<tenant>-shard-<shard>.db/.wal
```

The resolver is explicit and does not make `TenantId` and `ShardId` the same
type or identity. The local service holds an `AsyncShard` for each opened
tenant and keeps the cache entry while the process lives. The cache lock covers
the existence check and open operation, so concurrent first mutations cannot
create duplicate shard coordinators.

Read-only Get, Query, Scan, and TransactGet requests against a tenant with no
database return an empty state and do not create data or WAL files. A
condition-only Transact reads the same missing revision-zero state and also
does not create files. The first mutation opens the paired files and creates
the local database. A service restart reopens the same files through the normal
WAL recovery path.

## Transaction semantics

Put and Delete are single-key atomic mutations. Batch submits multiple
condition-free mutations as one transaction. Transact preserves
`RevisionEquals`, `Exists`, and `NotExists`, including the revision of a
missing key and the missing-revision ABA check.

The storage transaction primitive requires at least one mutation. A
condition-only Transact is therefore handled by the service boundary: it
rejects duplicate condition keys, gathers the unique keys with one
coordinator-serialized `TransactGet`, evaluates conditions in input order, and
returns the first deterministic structured Conflict. On success it returns an
outcome with no commit LSN and performs no mutation. It never implements this
operation as independently observed ordinary Gets.

Ordinary Query and Scan retain ordered results and exclusive cursors. TransactGet
retains input order and one committed point-read state. It does not create a
cross-key snapshot promise for ordinary independent Gets, Query, or Scan.

## Errors and unknown outcomes

The wire error categories are InvalidRequest, Overloaded, Conflict,
StorageFailure, Corruption, DurabilityFailure, Internal, and
UnsupportedProtocol. Conflict includes the key, expected condition, and
actual `Present(value, revision)` or `Missing(revision)` state. The category is
machine-readable; human-readable detail is supplementary.

An application error response is distinct from a QUIC stream or connection
failure. There is no v1 idempotency key or deduplication table. If a mutation
request has been sent and the response cannot be received or decoded, the Rust
client returns `UnknownMutationOutcome` and does not retry automatically. A
read-only request may be retried by a caller after a transport failure, but
the client does not add an automatic retry loop in this phase.

## Observability

`dodb-server` exposes in-process counters for total and active connections,
active streams, requests by operation, request and response bytes, request
latency, protocol errors, transport errors, application errors, and overloaded
responses. No metrics HTTP endpoint is added.

## Validation coverage

The protocol tests cover all request and response variants, empty and non-UTF8
binary data, missing revision zero, near-limit values, malformed headers and
payloads, invalid magic/version/opcodes, absurd lengths, response type
mismatch, structured Conflict, and structured server errors.

The loopback Quinn tests cover TLS connection setup, mutations, Get, Query,
Scan, TransactGet, atomic Batch, transaction commit and conflict, condition
only transactions, concurrent streams, same-tenant write ordering, tenant
isolation, missing-revision ABA, lazy file creation, disconnect/reconnect
through server restart, and persisted WAL-backed state after reopen.

## Deferred features

The protocol intentionally leaves application authentication, tenant
authorization, idempotency, distributed routing, ownership transfer,
replication, secondary replicas, shard migration, distributed transactions,
service discovery, load balancing, and operational checkpoint/snapshot/restore
control surfaces for later phases.
