---
Feature Name: Object Store Metadata Backend — Authority and Leader Election
Tracking Issue: TBD
Date: 2026-11-10
Author: Alorun
Status: Draft for discussion
---

# Summary
Add an object-store-backed authority and leader election mechanism to GreptimeDB's Metasrv. The design uses strongly consistent single-object reads and atomic conditional writes to maintain one fixed **Authority Head**, stored as versioned JSON in `head.json`. Its contents record the elected owner, the logical election epoch, a diagnostic lease deadline, and a write sequence. The object store returns the ETag used for conditional updates separately from those contents. Every metadata write that changes authoritative state must pass through this Head. The Head has three responsibilities:

- It identifies the elected owner and supplies a routing hint; the owner may already have stepped down locally.
- It carries the fencing epoch that invalidates an old Leader.
- It provides the single serialization point for later metadata commits.

This RFC defines the Authority and Leader Election contract for the first phase. Batching, immutable batches, and the B-link Tree will be specified in later RFCs and will reuse the Head defined here.

# Motivation
Metasrv currently uses etcd, MySQL, or PostgreSQL as the metadata authority and leader-election backend. The data path already relies on object storage, so a bucket-backed deployment still needs an independently operated metadata service. Replacing the metadata service with object storage is not a simple key-value translation. The current metadata workflow depends on several properties:

- A write must be serialized with other related writes.
- A Leader that has lost its term must not commit later.
- A failed request must not be reported as rejected while its outcome is unknown.
- A new Leader must be able to take over after the previous Leader fails.

The central design choice is to make the Leader the single writer of authoritative metadata. Conditional object versions reject stale updates, and the epoch fences stale terms. With this backend, every authoritative metadata write is routed through the Leader, so failover pauses metadata writes. The capacity and failover estimates below describe this trade-off; the full storage requirements are in *Object-store semantics*.

# Goals

- Elect exactly one effective Leader among concurrently starting nodes.
- Allow a surviving node to take over after the previous Leader expires.
- Prevent an old Leader from modifying the Head after a takeover.
- Anchor leases to the store's modification timestamp, with a bounded candidate clock offset.
- Handle uncertain conditional writes without blindly retrying them.
- Serialize Head mutations on each node so renewal and commit do not race locally.
- Preserve the existing Metasrv `Election` trait where possible.
- Provide a stable authority interface for the later phase-one RFCs.
- Reuse the existing OpenDAL object-store abstraction without extending OpenDAL.
- Make the design testable against MinIO and the cloud object stores in scope.

# Non-goals

- The batching and immutable batch format.
- Transaction commit semantics and commit-tail comparisons.
- B-link Tree nodes, manifests, splitting, and the read path.
- Checkpointing, garbage collection, and materialization.
- Quorum replication and follower-local consistent reads.
- A migration path from etcd, MySQL, or PostgreSQL.
- Running the new backend and an existing backend at the same time.
- Changing the persisted format of existing metadata keys.
- Supporting standalone Metasrv through this election mechanism.

# Phase boundary
The first phase is divided into three related RFCs:

| RFC | Responsibility | Relation to the Head |
| --- | --- | --- |
| Authority and election | Leader, epoch, lease, fencing | Defines and owns the Head authority |
| Batching and immutable batch | Batch construction and commit | Publishes committed batches through the Head |
| B-link Tree layout | Nodes, manifests, and reads | Publishes checkpoint roots through the Head |

This RFC defines the shared authority contract. It does not implement the `KvBackend` write methods because their visibility semantics depend on the batching RFC. Later RFCs may extend the Head payload with planned fields such as:

- `committed_sequence`
- `last_batch`
- `checkpoint_root`

The election fields and their meanings are fixed by this RFC.

# Capacity and failover estimates

At roughly 50 ms per storage request, a Head commit requiring one or two serial requests takes about 50–100 ms, or roughly **10–20 Head commits per second**. This is an estimate, not a throughput guarantee; renewal, contention, read-back, and tail latency reduce the available capacity. Later metadata commits must batch multiple operations behind each Head update.

Failover time includes the remaining store-side lease, the clock guard, the campaign interval with jitter, and the storage requests needed to read and replace the Head. A ten-second lease alone is not a ten-second failover bound. Storage outages or a delayed write that advances `last_modified` can delay takeover further.

# Design overview
The design separates three questions:

- **Who may write?** The current epoch, Head ownership, active local Leader state, and local deadline decide this.
- **When is a write visible?** The Head commit performed by the batching layer decides this.
- **Where does data live?** Immutable batches and B-link Tree objects decide this.

The Authority Head is rewritten in place. Every mutation checks a confirmed Head state and uses the ETag corresponding to that state in `If-Match`.
The high-level sequence is:

1. An operator explicitly initializes an empty Head in a new namespace.
2. Nodes register themselves as candidates.
3. Candidates read the Head and campaign when the lease is absent or expired.
4. Exactly one candidate replaces the Head using the previously read ETag.
5. The winner becomes Leader and receives the new epoch as its fencing token.
6. The Leader renews the Head through the same serialized writer used by commits.
7. The first failed or unresolved renewal immediately clears local leadership and emits one `StepDown` for the term. Other uncertain Head updates pause subsequent updates while their outcomes are resolved.
8. A candidate, including the former Leader, must win a higher epoch before `Elected` restores Leader services.

The election decision is made against the Head state. Candidate listing is never used to decide who may write.

# Authority Head

## Object path
The Head uses a fixed object path:

```text
<root>/<store_key_prefix>/authority/head.json
```

`root` comes from the selected storage configuration. The existing Metasrv `store_key_prefix` defines the cluster namespace within that root; it must remain stable and be unique among clusters sharing the root. An empty prefix dedicates the root itself to one cluster. There is no separate cluster identifier or Authority path-prefix option. Paths in this RFC include the storage root for clarity; calls to the opened operator use relative paths and do not add the root again. The Head path is reserved for Authority updates.

## Payload
The first phase fixes the following logical payload:

```rust
struct AuthorityHead {
    format_version: u32,
    owner: Option<NodeIdentity>,
    epoch: u64,
    lease_deadline_ms: u64,
    write_seq: u64,
}
```

The initial Head has `owner = null`, `epoch = 0`, and `write_seq = 0`:

```json
{
  "format_version": 1,
  "owner": null,
  "epoch": 0,
  "lease_deadline_ms": 0,
  "write_seq": 0
}
```

`owner` identifies the Leader process and carries its gRPC address; `NodeIdentity` must distinguish process incarnations so a restart does not inherit ownership. `epoch` is the monotonic logical term and fencing token. `lease_deadline_ms` is diagnostic only and is never used to judge lease validity. Every successful Head mutation increments `write_seq`, including renewal and resignation. This changes the serialized contents even when the other fields are unchanged, preventing identical-content ETag reuse. Epoch and sequence increments must reject overflow.

## Encoding
Serialize the Head directly to `head.json` with `serde_json`, with `format_version: 1` at the top level. A reader validates the JSON object, the supported format version, and the presence and types of every required field before using it. `owner` is required even when its value is `null`; missing fields must not silently reset authority state. Malformed JSON and unknown versions are rejected. Additional fields may be ignored only when the supported version's compatibility rules permit it. The persisted format and its compatibility fixtures use this JSON representation.

# Object-store semantics

## Object-store capabilities

The Authority implementation requires strong single-object read-after-write consistency and atomic conditional writes. Head reads must not regress to versions already superseded by completed writes. The Head contents, ETag, and `last_modified` must describe the same object version; separate metadata and content requests must be conditionally bound or retried to obtain a coherent state. Listing consistency is not required for election or commit decisions.

| Capability | Purpose |
| --- | --- |
| Strong single-object read | Read the Authority Head and resolve uncertain writes |
| `If-None-Match` | Initialize the Authority Head |
| `If-Match` | Takeover, renewal, resignation, and later commits |
| ETag | Identify the object version |
| `last_modified` | Determine lease expiration |
| Unconditional write | Register candidate nodes |
| Object listing | Discover candidate nodes |
| Object deletion | Clean up temporary probes; candidate cleanup additionally requires a safe conditional delete |

The first phase supports S3-compatible backends, including MinIO, and Azure Blob. Each actual endpoint and its configured OpenDAL layers must pass the startup probe; declared capabilities alone are insufficient. GCS, OSS, and the local filesystem are outside the supported set. Adapters for other conditional APIs are future work.

GreptimeDB already provides object-store access through OpenDAL. The Metasrv adapter owns JSON encoding, coherent Head reads, conditional-write handling, election, fencing, candidate registration, and capability validation. The startup probe verifies the configured operator and layers; it does not expand the supported backend set.

## Deployment requirements

The bucket is the storage container holding the Head object; on Azure Blob the corresponding resource is a container. The epoch is a logical term stored inside the Head, independent of bucket configuration or object-version IDs. All nodes must use the same selected store, root, and `store_key_prefix`.

The Authority prefix must be excluded from lifecycle expiration and storage-class transition rules. Administrative deletion, rollback to an older version, or replacement outside this protocol can destroy the authority history and requires operator recovery.

In a versioned S3 bucket, `If-None-Match: *` can succeed when the current version is a delete marker, even if older Head versions exist. A successful conditional create therefore does not prove that the namespace has never been initialized. See the [S3 conditional-write behavior](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html#conditional-write-behavior). Explicit initialization is restricted to a namespace the operator has established is new.

## Conditional writes

The design uses two conditional write forms:

| Operation | Condition | Purpose |
| --- | --- | --- |
| Create if absent | `If-None-Match: *` | Initialize the Head and immutable objects |
| Replace if unchanged | `If-Match: <etag>` | Takeover, renewal, resignation, and later commits |

OpenDAL exposes these through `write_with(...).if_not_exists(true)` and `write_with(...).if_match(etag)`. The condition must be evaluated atomically with the visible object mutation, including the final completion of a multipart upload. A `stat` followed by an unconditional write is not acceptable. The backend rejects operators or layers that ignore conditions. Transparent write retries must not replay Head mutations; the Authority protocol owns uncertain-write recovery. Storage error handling branches on OpenDAL `ErrorKind`, as specified below.

## Object version

The object version is represented by the ETag returned in object metadata:

```rust
struct ObjectVersion(String);
```

The ETag is an opaque version identifier returned by the object store alongside the Head. Pass it back to `If-Match` exactly as returned by OpenDAL, including surrounding quotes when present. The epoch is a logical term inside the JSON contents. Renewal preserves the epoch while changing the ETag. GCS generation conditions use a different API and are outside this RFC.

## Store timestamp and lease

Candidates compare their wall clocks with the store's `last_modified` using the complete rule in *Expiration test*. Deployments must keep candidate-to-store clock offsets within the stated bound. The Leader uses a monotonic clock for its local deadline, as specified in *Renewal*. The diagnostic `lease_deadline_ms` written by a node is never a lease authority.

## Capability probe

After opening the selected storage and applying its configured layers, and before campaigning, validate the resolved backend against the supported set and check declared capabilities, including `write_with_if_not_exists` and `write_with_if_match`. The provider's configuration name is not its backend type. Then issue real requests against a unique temporary key:

```text
<root>/<store_key_prefix>/probe/<uuid>.json
```

1. Create distinct contents with `If-None-Match: *`; require success, an ETag, and a non-empty `last_modified`.
2. Repeat the conditional create on the same key with different contents; require `ConditionNotMatch` and verify that the object is unchanged.
3. Replace it with different contents using the current ETag; require success and obtain the new ETag and `last_modified`. The new ETag must differ.
4. Attempt another replacement using the old ETag; require `ConditionNotMatch` and verify that the latest contents and ETag are unchanged.

The probe uses the same write path and layers as the Head. Ignored conditions, missing capabilities, unavailable metadata, or an unresolved probe result fail startup. Also require unconditional writes, listing, and probe deletion. Clean up the temporary object on both success and failure, and report cleanup failures. Probe keys are isolated from `authority/` and candidate records; a probe must never create or modify the Head. Timestamp availability and actual conditional semantics cannot be established from capability flags alone.

# Initialization
Head creation is an explicit operator operation for a new namespace. Add a planned Metasrv subcommand following the existing `greptime metasrv <subcommand> --config-file` CLI style:

```sh
greptime metasrv init --config-file metasrv.toml
```

This proposed command opens the configured store, runs the isolated capability probe, and creates the version-1 empty Head with `If-None-Match: *`. It does not campaign. Concurrent initializers that receive `ConditionNotMatch` read and validate the existing Head and report it as already initialized; they never reset its epoch or contents. Other conflicts require read-back and re-evaluation. An uncertain create is read back without blindly resending it. Creation and takeover remain separate operations:

| Property | Initialization | Takeover |
| --- | --- | --- |
| Owner | Empty | Current candidate |
| Epoch | `0` | Previous epoch plus one |
| Condition | Object absent | Previously read ETag |
| Lease | Not established | Established by this write |

The initial empty Head has no lease. Normal `greptime metasrv start --config-file metasrv.toml` requires an existing, valid Head. If the Head is missing at startup or runtime, stop election and writes and report an explicit error naming its path: `Authority Head is missing; operator recovery is required`. Do not invoke initialization as a fallback.

Recovery requires fencing or stopping existing writers, checking the configured store and namespace, and inspecting retained object versions or backups for the last valid Head. Restore the authority history and its referenced metadata from a consistent recovery point before restarting nodes. A missing current object, including an S3 delete marker, is insufficient evidence of a new namespace. If history cannot be recovered, any destructive reset is a separate recovery decision outside this initialization command.

# Leader election

## Takeover
A candidate repeatedly attempts takeover:

```text
read Head
if owner is empty or lease is expired:
    write owner = candidate
    write epoch = old_epoch + 1
    write_seq = old_write_seq + 1
    use If-Match with the read ETag
else:
    wait with jitter
```

The lease check and the conditional write belong to one authority operation. If two candidates use the same Head version, only one can replace its ETag, and the epoch increases exactly once. The loser rereads and re-evaluates. Candidate listing is never a vote or quorum. A takeover increments the epoch even after resignation; epoch zero represents an initialized Head with no election term.

The attempt has a provisional monotonic deadline anchored to its send time plus the lease duration. It grants leadership only when its result is confirmed before that deadline. A node that has stepped down must win an epoch higher than its retired term before emitting `Elected`. An uncertain takeover follows the read-back procedure below; a late result cannot activate an expired attempt.

## Expiration test
A Head with no owner can be taken over immediately. Otherwise, a candidate considers its lease expired only when:

```text
wall_now > last_modified + lease_duration + clock_guard
clock_guard = max_clock_skew + timestamp_resolution
```

`last_modified` comes from the same object version as the Head and its ETag. `wall_now` is the candidate's local wall clock in the store timestamp's time domain. The initial internal allowances are one second for the maximum candidate-to-store clock offset and one second for timestamp resolution. Deployments must keep clock offsets within that bound; the lease timing guarantee does not hold outside it. The resolution allowance must cover the supported backend's timestamp precision, verified during implementation.

This expiration rule determines when takeover is permitted. Routing uses a separate, earlier cutoff defined in *Existing Election integration*. CAS arbitrates competing updates to the same Head version. If renewal replaces that version first, the candidate must reread and repeat the expiration check. If takeover wins, the old Leader's conditional update fails. CAS does not replace the clock assumptions or validate a candidate's expiration calculation.

## Renewal
The Leader renews through the shared Head writer at an internal fixed interval. Renewal keeps `owner` and `epoch` unchanged and increments `write_seq`. A successful PUT refreshes the store-side `last_modified`. Sample the monotonic clock when sending the request and calculate the proposed deadline:

```text
proposed_deadline = sent_at + lease_duration
```

While the request is pending, retain the existing local deadline. Install the proposed deadline only on a successful renewal response received before that deadline, while local leadership remains active and this node still owns the term. Anchoring at send time avoids counting response latency as additional lease time.

On the first renewal failure or unknown result, immediately clear the local Leader flag, stop admitting new Leader writes, retire the local term, and emit one `StepDown`. Existing event handlers close heartbeat streams and stop or switch background tasks. Read-back may continue within the original deadline solely to determine whether the PUT applied. Neither successful read-back nor a late success response can extend the retired term or restore its services, even before the original deadline. Recovery requires winning a higher epoch through normal takeover and emitting `Elected`.

Reaching the local deadline also retires an active term; repeated failures or deadline expiry after an earlier `StepDown` do not emit it again. A late PUT may refresh `last_modified`, but the old Leader remains stopped and candidates use the updated timestamp for takeover. The batching RFC will define whether later batch commits also count as local lease renewal.

## Resignation
A Leader stops admitting new writes when it begins resignation, clears `owner`, retains the epoch, and increments `write_seq` through a fenced Head update. Confirmed resignation ends the local term. An uncertain result follows the same read-back and deadline rules as other mutations. The next takeover increments the epoch and starts a new term.

# Fencing and comparison

## Fencing token
A Leader receives the current epoch as its fencing token:

```rust
struct FencingToken {
    epoch: u64,
}
```

The token carries the logical term. The ETag belongs to the current Head version and can change during renewal without changing the term or a queued commit's business state. Later commit callers also supply a business precondition, such as the committed tail against which a batch was prepared.

## Version and term checks

Every successful Head mutation changes its contents and ETag. Takeover also changes the owner and epoch, invalidating an old writer's ETag and logical token. Before a fenced update:

1. Obtain a coherent Head state and its ETag, by reading it or using the writer's latest confirmed state.
2. Check that the token epoch equals the Head epoch.
3. Check that the owner is this node's process identity, local Leader state is active, the term has not been retired, and the local deadline has not passed.
4. Check the caller's business precondition.
5. Apply the mutation with `If-Match` using the ETag corresponding to that checked state.

A failed condition requires read-back and re-evaluation of the term, owner, deadline, and applicable business preconditions. An old-epoch operation is rejected even if the writer has since obtained a fresh ETag. An old-ETag write is rejected by the store even if the caller has not yet observed the new epoch. All authoritative metadata publication must pass through these checks.

# Head write serialization
Each node uses one writer to serialize all Head updates. Renewal may take priority over operations still in the queue so a busy queue does not starve the lease. Subsequent operations recheck the confirmed state, ETag, local leadership, deadline, and business preconditions. Renewal alone does not require rebuilding a prepared batch; a moved commit tail does. An unknown outcome pauses subsequent updates. A failed or unresolved renewal also immediately retires the term, so queued writes for that term are rejected even if read-back later succeeds. Operation entry points are defined in *Authority interface*.

# Uncertain writes

## Problem
A conditional write can apply before its response is lost, or can remain in flight and apply after the client has timed out. A client timeout does not cancel server-side execution. The protocol preserves the distinction between a Head mutation's outcome, current ownership, and a historical client transaction's outcome.

## Read-back procedure
After a timeout or a write error that may follow persistence, immediately pause new Head updates. If the operation was a renewal, first retire the local term and emit `StepDown` as specified above. Read the Head with backoff within the original local deadline solely to resolve the PUT outcome; the unresolved request does not extend that deadline. A takeover attempt uses its provisional deadline. Never blindly resend the original write.

Confirm application only when the read-back evidence uniquely identifies this operation. Compare the complete expected state, including the exact epoch, `write_seq`, and process ownership where applicable; matching only an owner address, diagnostic deadline, or empty owner is insufficient. For example, `owner = null` alone does not prove that this node's resignation applied. If a later state overwrote the mutation, the current Head may no longer contain enough evidence to determine its history.

## Outcomes

| Read-back result | Interpretation |
| --- | --- |
| ETag unchanged, before or after the client timeout | Still unknown; the original request may apply later |
| Evidence uniquely identifies this mutation | Confirmed applied; this does not undo `StepDown` or reactivate a retired term |
| Higher epoch or changed owner | Immediately abandon the current term; the original outcome remains unknown without independent evidence |
| ETag changed without evidence unique to this mutation | Still unknown; it may have applied and been superseded |
| Read-back fails | Retry read-back with backoff within the original deadline |
| Original local deadline reached before confirmation | Retire the term if still active, without duplicating `StepDown`, and preserve the operation result as unknown |

A success response proves that the object mutation applied, but cannot restore a retired term. A renewal outcome may therefore become known while local Leader services remain stopped. The node may campaign under the normal expiration rules and must win a higher epoch before `Elected` restores those services. A late PUT that refreshes `last_modified` delays takeover according to the same expiration rule.

Once a later legal takeover replaces the old ETag, a delayed request still using that ETag cannot modify the Head. This fences future execution; it does not prove whether the request applied before takeover. Historical commit outcomes require independent evidence. The batching RFC may use operation IDs and durable commit records in the published batch chain, rather than relying only on the current Head's last-batch pointer.

# Candidate registration
Candidate registration is separate from the Authority Head. Each node owns one object:

```text
<root>/<store_key_prefix>/candidates/<encoded-node-address>.json
```

The JSON record contains the existing `MetasrvNodeInfo` and a unique refresh identifier. The existing candidate methods and their return types remain unchanged; callers receive `MetasrvNodeInfo`. A node unconditionally overwrites its own path so a restart at the same address can replace the previous record. Refresh runs at an internal interval and retries temporary errors with backoff until stopped. Each refresh attempt uses a fresh identifier, ensuring distinguishable contents and versions even when the node information is unchanged.

Freshness is judged from the store's `last_modified` and the internal candidate TTL. `all_candidates()` lists records and filters stale entries. Candidate records serve display and peer discovery only; they never decide takeover, epoch assignment, fencing, or commit ordering. Duplicate addresses remain a configuration error; process identities must remain distinct for Authority ownership.

Periodically clean up stale candidate records using an atomic conditional delete bound to the same version whose timestamp was checked. A refresh after the scan must cause the stale delete to fail and preserve the refreshed record. An unconditional delete after a freshness check is unsafe. Identical-content refreshes could reuse an ETag, which is why refresh identifiers must change. Verify each backend's actual conditional-delete semantics, including behavior in versioned buckets, during implementation. Where safe deletion is unavailable, retain stale filtering and defer deletion to offline cleanup with refreshers stopped; never fall back to a racy delete.

# Authority interface
The authority exposes a small interface to the election and later metadata layers:

```rust
trait Authority {
    async fn head(&self) -> Result<AuthorityState, AuthorityError>;
    async fn takeover(&self) -> Result<AuthorityState, AuthorityError>;
    async fn mutate(
        &self,
        token: &FencingToken,
        precondition: Precondition,
        mutation: Mutation,
    ) -> Result<AuthorityState, AuthorityError>;
}
```

All update entry points use the same local writer:

| Entry point | Requirement | Effect |
| --- | --- | --- |
| `takeover()` | Empty owner or expired lease | Establish ownership and increment epoch and write sequence |
| `mutate(..., Mutation::Renew)` | Current token, owner, active local leadership, and local deadline | Refresh the lease and increment write sequence |
| `mutate(..., Mutation::Resign)` | Current token, owner, active local leadership, and local deadline | Clear owner and increment write sequence |

Takeover creates the first token of a term and is not a `Mutation` variant. Later RFCs may add `Commit` with a commit-tail precondition and `PublishCheckpoint` through the same writer.

`AuthorityState` contains:

```rust
struct AuthorityState {
    head: AuthorityHead,
    version: ObjectVersion,
    last_modified: Timestamp,
}
```

`head()` reads a coherent object version from the store. An active writer may use its latest confirmed state for the next conditional update. Read-back of a retired term's PUT resolves that operation only and never reactivates the term. Followers and leader discovery read the Head from the store.

# Existing Election integration
Keep the existing `Election` trait and routing flow. `campaign()` waits for confirmed takeover and maintains renewal until the term ends. `is_leader()` is a local flag that can lag the store, but is cleared immediately on the first failed or unresolved renewal. Request admission checks that flag, the active term, and its local deadline; Head updates also enforce fencing and business preconditions.

Retiring a term stops new Leader writes and emits one `StepDown` through the existing broadcast channel. Heartbeat streams and background tasks close or change state through their existing event handlers. Confirmed ownership loss, deadline expiry, reset, and resignation also retire an active term. Remember the retired term locally even while the Head still names this node. Emit `Elected` only when a confirmed takeover activates a higher epoch, allowing the existing handlers to restore Leader services and background tasks. Renewal read-back success never triggers `Elected`. Events occur only on actual transitions, the infancy flag remains one-shot, and standalone Metasrv does not use this backend.

## Routing hints

`leader()` reads the Head and returns an owner address that the caller may try to contact. `LeaderValue` continues to carry the gRPC address. Return `ElectionNoLeader` when the owner is empty, the routing deadline has been reached, or this node knows the observed term has retired. The caller stops routing the current write request on this error.

Followers use a separate, conservative wall-clock routing deadline:

```text
routing_guard = max_clock_skew + timestamp_resolution
route_deadline = last_modified + lease_duration - routing_guard
return a hint only if wall_now < route_deadline
```

The clock and precision allowances shorten the hint's lifetime; takeover still adds `clock_guard` under *Expiration test*. Sample `wall_now` after reading the Head, so read latency consumes the routing window rather than extending it from response receipt. With the proposed ten-second lease and two-second allowances, a Head last modified at second zero stops yielding hints at second eight, while takeover is permitted only after second twelve. A lookup at second eleven returns `ElectionNoLeader` while candidates still wait.

A delayed PUT may place `last_modified` later than the owner's local send time. An owner may also step down early without changing the Head. Followers that do not know this may return its address within the routing window, and forwarding delay can further age the hint. The receiving node must check active local leadership, the current term, and its local deadline and reject writes for a retired or expired term. The first phase uses Head-based hints without an owner-confirmation RPC in `leader()`; an address is not proof that the receiver will accept a write.

# Configuration
Add `BackendImpl::ExperimentalObjectStore`, serialized as `experimental_object_store`, to the existing Metasrv backend selection. Extend Metasrv options with the shared storage configuration and `storage_provider`, following the object-store WAL's provider resolution:

```toml
backend = "experimental_object_store"
store_key_prefix = "cluster-a"
# Empty selects the default store configured in [storage].
storage_provider = ""

[storage]
type = "S3"
bucket = "example-metasrv-bucket"
root = "metasrv"
```

An empty `storage_provider` selects the default `[storage]` store. A non-empty value is matched case-insensitively to the configured names in `[storage]` and `[[storage.providers]]`, using the same rule as `wal_object_store` in `src/datanode/src/datanode.rs` and `ObjectStoreManager::find`. Unknown names fail configuration validation. Validate the resolved backend type and operator capabilities, rather than comparing the provider name with `s3` or `azblob`.

For this example, the Head is `metasrv/cluster-a/authority/head.json`, candidates are under `metasrv/cluster-a/candidates/`, and probes are under `metasrv/cluster-a/probe/`. `store_key_prefix` is the only cluster namespace setting.

Election timings follow the repository's internal-constant convention in `src/common/meta/src/distributed_time_constants.rs` and `src/common/meta/src/election.rs`. The proposed backend starts with a ten-second lease, renewal at half the lease, and a 500 ms campaign interval with jitter. Clock allowances are defined in *Expiration test*. Candidate TTL and refresh reuse the existing 600-second lease and half-lease refresh interval. These are internal constants, not additional configuration options. Implementation must validate that the renewal interval leaves time for requests and read-back before the local deadline.

# Error handling
Storage branches use OpenDAL `ErrorKind` and its retry metadata, rather than raw HTTP statuses or Metasrv status codes. Every renewal failure or unknown result first clears local leadership and emits the term's single `StepDown`, before any read-back:

| OpenDAL result | Action |
| --- | --- |
| `ConditionNotMatch` | Read back and re-evaluate ownership, expiration, and preconditions; never resend the write as-is or reactivate a retired term |
| `Conflict` | Read back and re-evaluate contention; retryability alone does not justify replaying the mutation or resuming a retired term |
| `NotFound` from `If-Match` | Read the Head to confirm whether it is missing; confirmed absence stops election and writes and requires recovery |
| `NotFound` on a Head read | Stop and enter operator recovery; never initialize automatically |
| `Unexpected` or a temporary write error where persistence is uncertain | Pause updates and use the uncertain-write procedure |
| Temporary read-back failure | Retry the read with backoff within the original local deadline |
| `Unsupported`, `ConfigInvalid`, or `PermissionDenied` | Report the capability, configuration, or permission failure; fail startup or the operation |

A client-side timeout also follows the uncertain-write procedure; for renewal it immediately retires the term. It is not a separate OpenDAL error kind. Confirmed epoch or owner mismatch or local deadline expiry retires an active term without repeating an earlier `StepDown`. Business-precondition failures return the corresponding conflict for the caller to rebuild and re-evaluate. At the API boundary, preserve an unresolved operation as unknown rather than reporting an ordinary business rejection that would invite duplicate execution.

# Metrics and observability
Existing backend-labelled Metasrv metrics cover ordinary request and transaction latency. Add Head write latency and outcome, conditional-write conflicts, uncertain writes and read-back attempts, takeover count and current epoch, renewal failures, late successes, forced step-downs, observed timestamp skew, and stale candidate and cleanup-failure counts. These distinguish storage latency, clock-bound violations, lease churn, and candidate accumulation. Clock-skew metrics do not remove the deployment's bounded-clock requirement.

# Compatibility and backup
The Authority Head is a persisted JSON format covered by the repository compatibility framework. Add a frozen `format_version: 1` JSON fixture and deterministic `serde_json` encode/decode coverage. Reject malformed JSON, missing or invalid required fields, and unsupported versions. Future compatible fields must preserve the existing version's meanings; incompatible changes require a new supported format version.

A consistent backup contains the Head and, once later RFCs add them, the published batch chain reachable from `last_batch` and the required tree objects. Candidate records and probes are excluded. The Head and referenced metadata must come from one consistent recovery point. Restoring a backup is an operator recovery action with existing writers fenced, not initialization of an empty Head.

# Failure scenarios
The protocol must cover the following cases:

| Scenario | Expected result |
| --- | --- |
| Concurrent initialization in a new namespace | One create succeeds; other initializers read and validate the existing Head without resetting it |
| Two candidates take over using the same ETag | Exactly one wins and the epoch increases once |
| Leader stops renewing | A candidate takes over after the guarded expiry |
| First renewal failure or unknown result | Immediately clear the local Leader flag, stop new Leader writes, and emit one `StepDown` to close heartbeat streams and stop or switch background tasks |
| Old Leader resumes | The store rejects its old ETag; the writer rejects its old epoch even with a fresh ETag |
| Renewal races with takeover | Exactly one conditional mutation succeeds |
| Write applies but its response is lost | Unique read-back evidence confirms it; otherwise the result remains unknown |
| Read-back confirms renewal after timeout, even before the original deadline | The PUT is known to have applied, but the retired term and its Leader services remain stopped |
| Request applies after the client timeout | An unchanged ETag before application remains inconclusive; new updates stay paused |
| Renewal success is confirmed after the original deadline | The old Leader remains stepped down, even if the store timestamp advanced |
| The former Leader wins a higher epoch | `Elected` restores Leader services and background tasks for the new term |
| Routing has expired but takeover is not yet permitted | `leader()` returns `ElectionNoLeader` and the current write request stops routing |
| A Head-based hint points to a node that stepped down early | A follower may return the stale hint within its routing window; the receiver rejects the write, and a node that knows the term retired returns `ElectionNoLeader` |
| Takeover precedes a delayed old-term request | The changed ETag prevents the delayed request from applying |
| Head is missing at startup or runtime, including behind a delete marker | Stop and require operator recovery; never automatically initialize |
| Candidate crashes or restarts | Its record becomes stale or is refreshed with a distinguishable version |
| Candidate refresh races with stale cleanup | Conditional deletion preserves the refreshed record |
| Multipart publication uses a stale ETag | Final completion rejects the update without replacing the Head |
| Leader resigns | Owner becomes empty; the next takeover uses the next epoch |

# Validation plan

## Format and protocol tests

Add compatibility coverage for the frozen version-1 JSON fixture, deterministic serialization, malformed JSON, missing required fields (including `owner`), invalid field types, and unsupported versions. Unit tests cover epoch and sequence increments and overflow, resignation, separate routing and takeover boundaries, bounded clock offsets, monotonic deadlines, business preconditions, and one-shot leadership events. Inject timestamps into `AuthorityState` where the in-memory service lacks real `last_modified` metadata.

Use controlled concurrency and failure injection to verify:

- Two candidates read the same Head and ETag, race takeover, and produce exactly one winner with epoch incremented once. A failed condition requires a fresh expiration check.
- After takeover, the old Leader's PUT using the old ETag is rejected by the store. Separately, an operation with the old epoch is rejected locally even when supplied the latest Head state and ETag.
- The first renewal failure, including an unknown result, immediately clears `is_leader()`, rejects new Leader writes, and emits one `StepDown` before read-back or deadline expiry. Existing handlers close heartbeat streams and stop or switch background tasks; subsequent failures and deadline expiry emit no duplicate event.
- A write applies but its response is lost. Unique read-back evidence confirms application; for renewal, successful read-back even before the original deadline never restores the retired term, emits `Elected`, or restarts background tasks. A superseding Head alone cannot prove the historical result.
- A renewal request remains pending after client timeout. Read-back with an unchanged ETag remains unknown; allow the original request to apply later, and verify that no subsequent update from the retired term was issued. Also take over before releasing that request and verify rejection of its old ETag.
- A renewal PUT succeeds but confirmation arrives after the original local deadline. The node has already emitted one `StepDown` on renewal failure or deadline expiry, never extends the retired term's deadline or resumes its services, and candidates honor the updated store timestamp. A late successful takeover confirmation must not emit `Elected` or activate an expired attempt.
- The same process that stepped down later wins a higher epoch through normal takeover. Only its confirmed takeover emits `Elected`, and the existing event handlers restore Leader services and background tasks for the new term.
- Routing ends before takeover is allowed. With `last_modified` at second zero, a ten-second lease, and two-second guards, hints end at second eight and takeover waits until after second twelve. At second eleven, `leader()` returns `ElectionNoLeader` and the current write request stops routing. Cover exact cutoffs, clock offsets, and Head-read delays that consume the routing window.
- Node A steps down early while its Head routing window remains open. A follower may return A's stale address, but A rejects the write based on local Leader state, the retired term, or its deadline. A's own `leader()` returns `ElectionNoLeader` for its known retired term. Verify forwarding delay cannot bypass receiver checks and `leader()` issues no owner-confirmation RPC.
- An uncertain write is followed by a higher epoch or changed owner, causing immediate term abandonment. At the original deadline, unresolved operations remain unknown. Late evidence may resolve history but never restore the abandoned term.
- Renewal can precede queued operations, which then recheck confirmed state, active local leadership, and preconditions. An unresolved operation blocks subsequent updates; queued writes for a retired term remain rejected after read-back success. Reset and resignation emit events only on actual transitions.
- Concurrent initializers validate the winner's Head. Normal startup and runtime Head loss enter recovery without creating a replacement. Unique probe paths never overlap Authority paths, and probe cleanup runs on success and failure. Ignored conditions cause startup failure.
- Candidate refresh retries temporary errors with backoff. Stale filtering and cleanup handle node restarts and refresh/delete races; a refresh with unchanged node information still changes its version, and a stale conditional delete cannot remove it.

## Real-backend qualification

Run integration tests against MinIO, supported S3 deployments, and Azure Blob where credentials are available, using the actual OpenDAL operator and configured layers. Before enabling each backend, verify:

- All startup probe requests have the required create/update conditions, current ETags succeed, old ETags fail, and temporary probe objects are cleaned up.
- Head contents, ETag, and timestamp describe one version; completed updates are immediately visible. Verify ETag round trips, quoted ETags, distinct versions across Head mutations, timestamp availability and precision, and candidate listing metadata.
- Conditional-write failures map to `ConditionNotMatch` or `Conflict` as appropriate. Exercise concurrent deletion so an `If-Match` `NotFound` is followed by a confirming Head read and recovery, rather than initialization. Verify that configured retry layers do not replay uncertain Head writes.
- Force multipart upload, update the Head through another writer after upload begins, and complete the original upload with the stale ETag. Final completion must reject it. Verify equivalent conditional publication on Azure's block-upload path; checking only upload initiation is insufficient.
- In a versioned S3 bucket, a current delete marker permits a conditional create on a test key despite retained history. Normal Head reads must treat that state as missing and require recovery, not proof of a fresh namespace.
- For each backend, verify the advertised and actual conditional-delete behavior, including versioned storage: a stale cleanup operation must fail after a candidate refresh. If that guarantee is unavailable, exercise the safe filtering and offline-cleanup fallback.

Run response-loss, delayed-request, and process-pause scenarios through a failure-injection layer around these backends. Verify immediate renewal-failure `StepDown`, successful read-back without old-term recovery, and restoration through a higher epoch. Exercise routing cutoffs and stale hints with actual request delays. Measure Head-update latency, sustainable commit rate, and renewal scheduling under queued load to qualify the capacity estimate and internal timings. No real-backend behavior is assumed verified by the in-memory tests.

# Implementation plan
The work is split into reviewable changes:

- Add Authority types, versioned JSON encoding, and compatibility fixtures.
- Add resolved-backend validation and the isolated runtime conditional-write probe.
- Add coherent Head reads, conditional mutations, explicit initialization CLI, and missing-Head recovery errors.
- Add takeover, epoch fencing, renewal, local deadlines, and resignation.
- Add the single-writer task and uncertain-write read-back.
- Add retrying candidate refresh, safe stale cleanup, and the existing `Election` integration.
- Add backend selection and storage options, metrics, and real-backend qualification with failure injection.

The batching RFC then adds commit-tail mutations without changing the election contract. The B-link Tree RFC adds checkpoint publication through the same writer.

# Future work
The batching RFC will define immutable batches, operation identifiers, commit-tail preconditions, and client-visible commit confirmation. The B-link Tree RFC will define nodes, manifests, checkpoint roots, and read semantics. A later phase may add checkpoint garbage collection, quorum replication, and follower-local consistent reads.
