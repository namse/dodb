# Admitted Blink Mutation Key Reuse

## Result

The planned-path optimization was implemented and measured, then rejected. The
candidate did not reduce normalized admission-plus-planning cost or improve
throughput on the OCI A1 host. Both implementation commits were normally
reverted; the branch ends on the good source plus this report and preserved raw
artifacts.

## Duplicate-work audit and design

Before the change, planned admission called `validate_request_values()`, which
encoded and validated each mutation key. `LogicalOverlay::accept()` encoded it
again for the overlay's owned `BTreeMap` key. `plan_batch()` encoded and
validated it a third time before routing and dependency construction. The
planner then copied the key into `PlannedMutation.encoded_key` and its owned
`last_key_writer` map. Admission also cloned each accepted `TransactionRequest`,
after which the planner cloned each mutation into `PlannedMutation`.

The final measured candidate SHA was
`1eaabbce8ec29948fe886008dd3e4548d1a8d748`; its initial implementation commit
was `f085e7b8d6e28761e31b55a4b4c0539c968c649f`. Changes were limited to
`crates/dodb-storage/src/blink/mod.rs`.

Planned admission used `validate_and_encode_mutation_keys()` to encode and
validate mutation keys once, preserving canonical and maximum encoded-key
validation and the Put value-size check. `AdmittedTransaction<'a>` borrowed the
original request and held the prepared keys in mutation order. Admission
passed those keys to `LogicalOverlay::accept_preencoded()`, which checked the
mutation/key count and copied each key into the overlay's owned map.
`plan_batch()` checked the count and reused the prepared bytes for routing,
dependency construction, and `PlannedMutation` construction. It no longer
encoded or revalidated admitted mutation keys. The accepted request clone was
removed.

The required `PlannedMutation.mutation` clone and owned planner/overlay keys
remained. No shared-key representation, overlay representation change, or
mutation ownership change was made. Follow-up commit
`1eaabbce8ec29948fe886008dd3e4548d1a8d748` restored the serial
`validate_request_values()` loop unchanged so the preparation vector was only
created on the planned path. `TransactionRequest::validate()` and its duplicate
mutation/condition and empty-mutation semantics were not changed. Condition-key
encoding in `LogicalOverlay::observed_state()` and planner condition dependency
construction also remained unchanged.

## Correctness

Focused tests covered prepared-key count and byte identity, oversized key and
value rejection, unchanged route IDs, same-key and same-page dependencies,
structural-route dependencies, multi-mutation `mutated_key_set` contents,
provisional revisions, and split/reroute behavior. Existing planned admission
tests verified that later conditions see earlier accepted writes and conflicted
requests do not leak writes. Existing core malformed-encoding tests and the
preserved canonical validation cover malformed-key rejection.

All requested gates passed on the final implementation source before revert:

- `cargo fmt --all -- --check`
- `cargo test -p dodb-core` (6 passed)
- `cargo test -p dodb-storage` (115 unit tests and 8 recovery tests passed)
- `cargo test -p dodb-storage --bin phase0-bench` (10 passed)
- `cargo test --workspace` (all tests passed, including testkit and Phase 1
  integration tests)
- `git diff --check`

## Local Apple M1 smoke

Base and final candidate used the same Apple M1 release build and the requested
16 writers, width 1, `different-leaf-heavy`, 100,000 key working set, 4,096
cache, 16-byte key, 64-byte value, group limit 64, 4 MiB group limit, queue
256, zero delay, disabled sync, two Tokio workers, one-second warmup and
duration, one repetition, and seed `0x3a042026`. Both had zero errors and
overloads.

| Metric | Base | Candidate | Candidate/base |
| --- | ---: | ---: | ---: |
| Throughput (mutations/s) | 68,423 | 75,947 | 1.110x |
| Logical admission (ns/mutation) | 574.2 | 398.2 | 0.693x |
| Planning (ns/mutation) | 1,295.0 | 1,055.3 | 0.815x |
| Route (ns/mutation) | 566.2 | 509.6 | 0.900x |
| Front-end total (ns/mutation) | 1,869.2 | 1,453.5 | 0.778x |
| Front-end non-route (ns/mutation) | 1,302.9 | 944.0 | 0.725x |

The candidate passed the local gate. These single-repetition local measurements
are smoke evidence only.

## OCI build and workload

The same-host A/B used `opc@217.142.246.204`, Oracle Linux 9.8 AArch64,
`rustc 1.98.1`, and two logical CPUs. The checkout was clean before creating
detached worktrees. The root XFS filesystem was 30 GiB with 18 GiB free.

Base `ac181f5900ed46a83e616f3d28bcb85bac88ca48` and candidate
`1eaabbce8ec29948fe886008dd3e4548d1a8d748` were built in clean detached
worktrees with separate targets using:

```bash
cargo build --locked --release -p dodb-storage --bin phase0-bench
```

The repository `.cargo/config.toml` supplied `-C target-feature=+crc`; no
`RUSTFLAGS` override was used.

| Binary | SHA256 |
| --- | --- |
| Base | `8f402d6b3660e8b16d8899b49830bc4b6bb93d70398aa66e56cdc3e7fff0baa3` |
| Candidate | `3899baaff31950565f21eed0d832b29fe2f1df268299ac7a740aba57619aa53f` |

Width 1 used 16 writers, width 1, `different-leaf-heavy`, working set 100,000,
cache 4,096, key size 16, value size 64, group limit 64, group bytes
4,194,304, queue 256, delay 0, disabled sync, two Tokio workers, one-second
warmup, two-second duration, three repetitions, and seed `0x3a042026`. Width
16 used the same settings with width 16, `uniform`, and seed `0x3a032026`.
All repetitions had zero errors and overloads.

The following component values are medians of per-repetition values normalized
by successful mutations. Throughput includes median and min–max. Front-end
values use `logical_admission_nanos`, `planning_nanos`, and
`planner_route_nanos`.

### Width-1 primary

| Metric | Base | Candidate | Candidate/base |
| --- | ---: | ---: | ---: |
| Throughput median (min–max), mutations/s | 47,805 (46,063–48,133) | 42,971 (41,960–43,446) | 0.899x |
| Logical admission (ns/mutation) | 931.0 | 974.9 | 1.047x |
| Planning (ns/mutation) | 2,315.0 | 2,630.5 | 1.136x |
| Route (ns/mutation) | 1,106.5 | 1,138.9 | 1.029x |
| Non-route planning (ns/mutation) | 1,208.4 | 1,491.6 | 1.234x |
| Front-end total (ns/mutation) | 3,246.0 | 3,598.5 | 1.109x |
| Front-end non-route (ns/mutation) | 2,175.8 | 2,508.9 | 1.153x |
| Physical execution (ns/mutation) | 4,023.7 | 4,851.6 | 1.206x |
| Page encode (ns/mutation) | 1,699.1 | 2,486.9 | 1.464x |
| WAL append (ns/mutation) | 4,349.3 | 5,097.6 | 1.172x |
| WAL group encode (ns/mutation) | 2,212.1 | 2,973.0 | 1.344x |

### Width-16 control

| Metric | Base | Candidate | Candidate/base |
| --- | ---: | ---: | ---: |
| Throughput median (min–max), mutations/s | 42,431 (41,904–42,936) | 36,820 (36,802–38,470) | 0.868x |
| Logical admission (ns/mutation) | 1,009.5 | 1,052.9 | 1.043x |
| Planning (ns/mutation) | 3,732.8 | 4,010.6 | 1.074x |
| Route (ns/mutation) | 1,625.9 | 1,638.0 | 1.007x |
| Front-end total (ns/mutation) | 4,744.0 | 5,087.9 | 1.073x |
| Front-end non-route (ns/mutation) | 3,160.1 | 3,360.5 | 1.063x |

The post-candidate base repeat produced 46,850 mutations/s median at width 1
and 42,076 at width 16, with zero errors and overloads. The primary base and
repeat were consistent. Candidate-to-repeat throughput ratios were 0.917x at
width 1 and 0.875x at width 16. Repeat raw files are retained alongside the
four required artifacts.

## Classification and remaining duplication

**Reject.** The width-1 front-end non-route ratio was 1.153x and throughput
ratio was 0.899x, beyond the reject thresholds. Width-16 throughput was 0.868x,
below its 0.95x floor. Physical execution, page encoding, WAL append, and WAL
group encoding each increased by at least 15% in the primary comparison. The
base repeat confirms the throughput drop was not explained by a one-off high
base run. These are measured regressions; no WAL changes or follow-up WAL work
were made.

Remaining duplicate work included condition-key encoding in overlay observation
and planner condition-dependency construction, plus necessary owned-key copies
for the overlay, `PlannedMutation`, `last_key_writer`, and `mutated_key_set`.
This task did not change those paths or representations.

## Artifacts and rollback

| Artifact | SHA256 |
| --- | --- |
| [`planned-base-width1.jsonl`](results/oci-a1-2ocpu-12g-200g/admitted-key-reuse/planned-base-width1.jsonl) | `3dc83495e56c2cf6206f0450bf5c9d978cda3abe8a826d317790f19f745d72b5` |
| [`planned-candidate-width1.jsonl`](results/oci-a1-2ocpu-12g-200g/admitted-key-reuse/planned-candidate-width1.jsonl) | `4c4f298252d224b6ec4f9ca08b76507f3850380cd29716669a29ba6742c3c1b5` |
| [`planned-base-width16.jsonl`](results/oci-a1-2ocpu-12g-200g/admitted-key-reuse/planned-base-width16.jsonl) | `7b7e5ed26c57065b2c9152412caa644bc6cf66c5b044c543bc38f9b0910a436f` |
| [`planned-candidate-width16.jsonl`](results/oci-a1-2ocpu-12g-200g/admitted-key-reuse/planned-candidate-width16.jsonl) | `439e8d375b814e4c18161c4b761ac197b8ced4a7151c8c7183420c3d4b999e41` |
| [`planned-base-width1-repeat.jsonl`](results/oci-a1-2ocpu-12g-200g/admitted-key-reuse/planned-base-width1-repeat.jsonl) | `092ad02f63b61be0e6e3924e3f26bba4385b1389bee88860a738a0f3046898c4` |
| [`planned-base-width16-repeat.jsonl`](results/oci-a1-2ocpu-12g-200g/admitted-key-reuse/planned-base-width16-repeat.jsonl) | `fdb9d234de5e55a532105b502e4ddaea9b7094ee7b27bdbdc1e35a38703fa5fd` |

The artifacts are backed up at
`/home/opc/dodb-oci-artifacts-admitted-key-reuse-1eaabbc/`; remote and local
SHA256 values matched.

Implementation commits `f085e7b8d6e28761e31b55a4b4c0539c968c649f` and
`1eaabbce8ec29948fe886008dd3e4548d1a8d748` were reverted normally as
`4ac65c33437fdd224b947c3ddfd9a6f31613ee25` and
`6fb9d7c2e0a475de110c6a875fa1cf17e6d92b5c`, respectively. No history rewrite
was used.
