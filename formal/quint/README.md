# Transaction semantics model

This directory contains the first dodb formal model. It covers only the
logical transaction contract. Coordinator, WAL, recovery, checkpoint,
publication, end-to-end, and B-link models are outside this phase.

## Toolchain

The repository pins `@informalsystems/quint` to `0.32.0` in the root
`package.json` and `package-lock.json`. Install the local dependency with:

```sh
npm install
```

The verification scripts use Apalache `0.56.1`, which Quint downloads when it
is not already available. TLC uses the JDK installed on the host through the
Quint CLI. The checked environment used JDK 21.0.9.

## Model

`common.qnt` defines the finite logical vocabulary:

- `Key`: `KeyA` and `KeyB` are used by the initial model.
- `Value`: two finite values.
- `LogicalState`: `Present((value, revision))` or `Missing(revision)`.
- `Condition`: `RevisionEquals`, `Exists`, and `NotExists`.
- `Mutation`: `Put` and `Delete`.

`transaction_semantics.qnt` has one active transaction and permits at most two
setup commits before it. Setup commits create the states needed for the ABA
trace. The candidate transaction moves through `Validating`, `Validated`,
`Prepared`, and one of `Committed`, `Conflict`, `ConditionSatisfied`, or
`InvalidRequest`.

`transaction_group.qnt` models ordered logical processing of one fixed request
sequence at a time. It keeps the group result private until `PublishGroup` and
checks each processed prefix against a separate serial reference evaluator.
The four bounded sequences cover accepted/conflict/accepted, staged-state
condition-only success, insert-if-absent conflict, and invalid-then-accepted.
The invalid case uses an empty transaction as a representative invalid request
to check that one invalid request does not block later requests in the group;
it does not formalize all Rust input-validation cases, such as duplicate
conditions or mutations and oversized input.

`coordinator_segments.qnt` models FIFO coordinator segmentation for one fixed
request sequence. Mutation-like requests, including `ConditionOnly`, are
consumed as maximal contiguous segments; each barrier is processed separately
and records the current logical state. Independent serial and segmentation
references check each processed prefix. The model does not reimplement
transaction semantics.

`coordinator_collection.qnt` verifies physical group formation for one fixed
FIFO-admitted request stream. It checks request conservation and FIFO order,
pending-request handoff, request-count and byte bounds, and the oversized-first
request exception. Timing is not modeled; sealing a group early is an
nondeterministic choice. Transaction and barrier semantics, WAL, and fsync are
the responsibility of the other models or are outside this model's scope.

Validation reads the immutable `baseState`. Mutation preparation computes a
private `stagedState` with one commit LSN for every changed key. Only
`commitPrepared` changes `committedState` and advances `nextLsn`. A
condition-only success stops before preparation and returns `NoCommit`.

The model uses a finite catalog of transactions rather than modeling B+Tree
pages. The catalog includes empty, condition-only, one-key, two-key, delete,
conflicting, and stale-missing-revision cases. This is intentional: the model
checks logical state and transaction ordering, not the Rust page algorithm.

## Named invariants

- `TypeOK`
- `Atomicity`
- `ConflictHasNoEffect`
- `OneRevisionPerCommit`
- `MissingRevisionPreserved`
- `ConditionOnlyHasNoCommit`
- `CommittedStateWellFormed`
- `EmptyRequestInvalid`
- `ValidationBeforeMutation`
- `RevisionConditionSound`
- `AbaStaleRevisionRejected`

The `state_invariants` value combines all of them for simulation and model
checking.

## Commands

```sh
npm run formal:typecheck
npm run formal:test
npm run formal:simulate
npm run formal:verify:tlc
npm run formal:verify:apalache
npm run formal:group:typecheck
npm run formal:group:test
npm run formal:group:simulate
npm run formal:group:verify:tlc
npm run formal:group:verify:apalache
npm run formal:segments:typecheck
npm run formal:segments:test
npm run formal:segments:simulate
npm run formal:segments:verify:tlc
npm run formal:segments:verify:apalache
npm run formal:collection:typecheck
npm run formal:collection:test
npm run formal:collection:simulate
npm run formal:collection:verify:tlc
npm run formal:collection:verify:apalache
```

The initial verification bound is two keys, two setup commits, finite values,
bounded integer revisions/LSNs, and `--max-steps=20`. The simulation uses seed
`0x20260923` and 200 samples.

## Concrete mapping

| dodb concept | Model concept |
| --- | --- |
| `RevisionState::Present` | `Present((value, revision))` |
| `RevisionState::Missing` | `Missing(revision)` |
| `TransactionCondition` | `Condition` |
| `TransactionMutation` | `Mutation` |
| `TransactionRequest` | `Transaction` |
| `Overlay` | `stagedState` and `baseState` |
| transaction commit LSN | `HasCommit(nextLsn)` |
| `TransactionResult { commit_lsn: None }` | `NoCommit` |
| `prepare_transaction` validation | `validateConditionOnly`, `validateMutation`, `rejectConflict` |
| `finish_transaction` logical result | `prepareTransaction` |
| committed publication in this logical model | `commitPrepared` |

WAL records, fsync, page images, superblocks, read views, coordinator groups,
and network responses are intentionally absent. They belong to later models.

## WAL durability model

`wal_durability.qnt` models two logical commits written in fixed WAL frame order:
`APage`, `ACommit`, `BPage1`, `BPage2`, `BCommit`. Each logical commit is
recoverable only when its own COMMIT marker is part of the complete durable
prefix. Both commits share one physical fsync.

Unsynced writes may partially persist before a crash. A failed fsync has an
uncertain persistence outcome, so recovery reads the complete durable prefix
and can preserve an earlier logical commit from a failed physical group. The
physical group is a durability amortization unit, not a logical atomicity
boundary. A successful fsync makes the full frame group durable, even if a
fault occurs before `append_group()` reports success.

The model stops after WAL append success and recovery. B+Tree publication,
client acknowledgment, checkpointing, WAL reset, and page flush are outside
its scope.

```sh
npm run formal:wal:typecheck
npm run formal:wal:test
npm run formal:wal:simulate
npm run formal:wal:verify:tlc
npm run formal:wal:verify:apalache
```
