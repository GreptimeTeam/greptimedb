---
Feature Name: Object Store Metadata Backend — Authority and Leader Election
Tracking Issue: TBD
Date: 2026-11-10
Author: Alorun
Status: Draft for discussion
---

# Summary
Add an object-store-backed authority and leader election mechanism to GreptimeDB's Metasrv. Strongly consistent single-object reads and atomic conditional replacements maintain one fixed **Authority Head**, stored as versioned JSON in `head.json`. Its contents record the elected owner, logical epoch, diagnostic lease deadline, and write sequence. OpenDAL `Metadata` from the same object version supplies the condition for `if_not_changed(&metadata)`. Every metadata write that changes authoritative state must pass through this Head. The Head has three responsibilities:

- It identifies the elected owner and supplies a routing hint; the owner may already have stepped down locally.
- It carries the fencing epoch that invalidates an old Leader.
- It provides the single serialization point for later metadata commits.

This RFC defines the Authority and Leader Election contract for the first phase. A separate batching RFC will define metadata commit semantics; it will be added later. Storage layout and read semantics will be specified separately and will reuse the Head defined here.

# Motivation
Metasrv currently uses etcd, MySQL, or PostgreSQL as the metadata authority and leader-election backend. The data path already relies on object storage, so a bucket-backed deployment still needs an independently operated metadata service. Replacing the metadata service with object storage is not a simple key-value translation. The current metadata workflow depends on several properties:

- A write must be serialized with other related writes.
- A Leader that has lost its term must not commit later.
- A failed request must not be reported as rejected while its outcome is unknown.
- A new Leader must be able to take over after the previous Leader fails.

The central design choice is to make the Leader the single writer of authoritative metadata. Conditional object versions reject stale updates, and the epoch fences stale terms. With this backend, every authoritative metadata write is routed through the Leader, so failover pauses metadata writes. Renewal costs and failover estimates are below; the full storage requirements are in *Object-store semantics*.

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

Keeping a KV service would retain the independent service dependency, while a routing-only Head would still need another component for fencing and commit serialization. The WAL-style alternative advances history by conditionally creating monotonically named immutable objects and needs rules for discovering the latest valid record and handling concurrent publication. This RFC renews a fixed Head so `owner`, `epoch`, and store-side `last_modified` can be read from one version, requiring atomic replacement conditioned on the version read.

# Phase boundary
The first phase is divided into three related RFCs:

| RFC | Responsibility | Relation to the Head |
| --- | --- | --- |
| Authority and election | Leader, epoch, lease, fencing | Defines and owns the Head authority |
| Batching and immutable batch | Batch construction and commit | Publishes committed batches through the Head |
| B-link Tree layout | Nodes, manifests, and reads | Publishes checkpoint roots through the Head |

This RFC defines the shared authority contract. It does not implement the `KvBackend` write methods because their visibility semantics belong to the future batching RFC. Later RFCs may extend the Head payload with planned fields such as:

- `committed_sequence`
- `last_batch`
- `checkpoint_root`

The election fields and their meanings are fixed by this RFC.

# Renewal costs and failover estimates

Renewal shares the serialized Head writer with later metadata commits. Each renewal requires a conditional replacement and enough time to obtain coherent response metadata or resolve an uncertain result. Renewal priority and request latency must leave sufficient time within the local lease; commit throughput and batching requirements belong to the future batching RFC.

Failover time includes the remaining store-side lease, the clock guard, the campaign interval with jitter, and the storage requests needed to read and replace the Head. A ten-second lease alone is not a ten-second failover bound. Storage outages or a delayed write that advances `last_modified` can delay takeover further.

# Design overview
The design separates three questions:

- **Who may write?** The current epoch, Head ownership, active local Leader state, and local deadline decide this.
- **When is a write visible?** The Head commit performed by the batching layer decides this.
- **Where does data live?** Immutable batches and B-link Tree objects decide this.

The Authority Head is rewritten in place. Every mutation checks a confirmed Head state and passes its corresponding OpenDAL `Metadata` to `if_not_changed`.
The high-level sequence is:

1. An operator explicitly initializes an empty Head in a new namespace.
2. Nodes register themselves as candidates.
3. Candidates read the Head and campaign when the lease is absent or expired.
4. Exactly one candidate conditionally replaces the previously read Head version.
5. The winner becomes Leader and receives the new epoch as its fencing token.
6. The Leader renews the Head through the same serialized writer used by commits.
7. Renewal failure follows the local retirement transition in *Renewal*; uncertain results follow *Outcomes*.
8. Candidates continue campaigning under the takeover rules.

The election decision is made against the Head state. Candidate listing is never used to decide who may write.

# Authority Head

## Object path
The Head uses a fixed object path:

```text
<root>/<store_key_prefix>/authority/head.json
```

`root` comes from the selected storage configuration. When clusters share a root, each must explicitly set a non-empty, unique, and stable `store_key_prefix`. An empty prefix is permitted only when the cluster exclusively owns that root. If two clusters resolve to the same Head path, either may take over the other's Head after lease expiry, and later commits may enter the same chain. First-phase isolation relies on deployment owners assigning paths correctly, using the existing configuration without a separate cluster identifier or Authority prefix option.

Paths in this RFC include the storage root for clarity; calls to the opened operator use relative paths and do not add the root again. The Head path is reserved for Authority updates.

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

struct NodeIdentity {
    addr: String,
    incarnation_id: Uuid,
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

A version-1 Head after takeover contains a non-empty owner:

```json
{
  "format_version": 1,
  "owner": {
    "addr": "127.0.0.1:3002",
    "incarnation_id": "550e8400-e29b-41d4-a716-446655440000"
  },
  "epoch": 1,
  "lease_deadline_ms": 1794528010000,
  "write_seq": 1
}
```

`addr` is the Metasrv gRPC address. `incarnation_id` is a new UUID generated once at each process start, stable for that process's lifetime, and serialized as a canonical hyphenated UUID string. Ownership checks compare both fields, so restarting at the same address does not inherit a term. `lease_deadline_ms` is diagnostic only. Every successful Head mutation increments `write_seq`; both `epoch` and `write_seq` use checked increments and reject overflow without writing.

## Encoding
Serialize the Head to `head.json` with `serde_json` and top-level `format_version: 1`; v1 readers ignore unknown fields in both Head and nested `NodeIdentity`, while validating required fields and types (`owner` must be present even when `null`). Only optional fields that old readers can ignore and old writers can discard on rewrite without affecting correctness may be added to v1. Adding required fields or changing ownership, lease, fencing, commit visibility, or other protocol semantics requires a higher `format_version`; malformed JSON and unsupported versions are rejected.

# Object-store semantics

## Object-store capabilities

Admission requires strong single-object read-after-write consistency and atomic replacement conditioned on the version that was read. Head contents and the complete OpenDAL `Metadata`, including the condition's version identifier and `last_modified`, must describe the same object version. Separate metadata and content requests must be conditionally bound or retried to obtain a coherent state. Reads must not regress to superseded versions. Listing consistency is not required for election or commit decisions.

| Capability | Purpose |
| --- | --- |
| Strong single-object read | Read the Authority Head and resolve uncertain writes |
| `if_not_exists(true)` | Initialize the Authority Head |
| `if_not_changed(&metadata)` backed by atomic replacement | Takeover, renewal, resignation, and later commits |
| Backend condition identifier in `Metadata` | Identify the version used for replacement and read-back comparison |
| `last_modified` | Determine lease expiration |
| Unconditional write | Register candidate nodes |
| Object listing | Discover candidate nodes |
| Object deletion | Clean up temporary probes; candidate cleanup additionally requires a safe conditional delete |

First-phase qualification covers S3-compatible backends, including MinIO, Azure Blob, and GCS. Each actual endpoint must pass the startup probe through the production Head read/write path and configured OpenDAL layers. S3 and Azure use ETag conditions; GCS uses the generation exposed as `Metadata::version()`. GCS must pass real-endpoint generation-path tests before it is enabled; that validation has not been performed for this RFC revision.

The current OpenDAL OSS write path lacks atomic replacement conditioned on the version read, so OSS is deferred. Reassessment requires real-bucket tests of `PutObject` conditional-write behavior and a conforming adapter. The local filesystem and other backends without the same atomic condition, coherent metadata, or strong-read guarantees are also excluded; declared flags or read-then-unconditional-write emulation do not satisfy admission.

The current OpenDAL write interfaces for HDFS and MySQL also lack the required conditional replacement; the MySQL restriction applies to the adapter. HDFS `append` only appends bytes and cannot replace this version-checked Head update: using it would require a separate election protocol, and append support varies by backend.

GreptimeDB already provides object-store access through OpenDAL. The Metasrv adapter owns JSON encoding, coherent Head reads, conditional-write handling, election, fencing, candidate registration, and capability validation. The functional probe verifies the endpoint and layers against the same admission contract.

## Deployment requirements

The bucket is the storage container holding the Head object; on Azure Blob the corresponding resource is a container. The epoch is a logical term stored inside the Head, independent of bucket configuration or object-version IDs. All nodes must use the same selected store, root, and `store_key_prefix`.

The Authority prefix must be excluded from lifecycle expiration and storage-class transition rules. Administrative deletion, rollback to an older version, or replacement outside this protocol can destroy the authority history and requires operator recovery.

In a versioned S3 bucket, `If-None-Match: *` can succeed when the current version is a delete marker, even if older Head versions exist. A successful conditional create therefore does not prove that the namespace has never been initialized. See the [S3 conditional-write behavior](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html#conditional-write-behavior). Explicit initialization is restricted to a namespace the operator has established is new.

## Conditional writes

The design uses two conditional write forms:

| Operation | Condition | Purpose |
| --- | --- | --- |
| Create if absent | `write_with(...).if_not_exists(true)` | Initialize the Head and immutable objects |
| Replace if unchanged | `write_with(...).if_not_changed(&metadata)` | Takeover, renewal, resignation, and later commits |

Use the `Metadata` paired with the decision's Head contents for every takeover, renewal, and resignation. OpenDAL lowers `if_not_changed` to a supported native version-match condition, or an ETag match when that is the supported primitive. The condition must be atomic with publication, including final multipart or staged-upload completion. A `stat` followed by an unconditional write is not acceptable. Operators or layers that ignore conditions are rejected, and transparent retries must not replay Head mutations. Storage branches use OpenDAL `ErrorKind`.

## Object version

Preserve the complete OpenDAL `Metadata` associated with the observed Head path and contents. The identifier selected for conditional writes is opaque: compare it for equality during read-back, without interpreting or ordering it. In OpenDAL 0.59.4, S3 and Azure select `Metadata::etag()`, while GCS selects the object generation in `Metadata::version()`. These physical identifiers differ from the logical epoch; renewal keeps the epoch while changing the conditional version.

For S3, retain ETags exactly as returned, including surrounding quotes, when OpenDAL passes them to `If-Match`. Writing identical contents may reuse an ETag, so incrementing `write_seq` must change the Head contents on every mutation. Bucket version IDs are not substituted for the ETag condition. Versioned-bucket delete-marker behavior is covered in *Deployment requirements*.

## Store timestamp and lease

Candidates compare their wall clocks with `last_modified` from the Head's OpenDAL `Metadata`, using the complete rule in *Expiration test*. Deployments must keep candidate-to-store clock offsets within the stated bound. The Leader uses a monotonic clock for its local deadline, as specified in *Renewal*. The diagnostic `lease_deadline_ms` written by a node is never a lease authority.

## Capability probe

After opening the selected storage and applying its configured layers, and before campaigning, check admission status and the declared primitives: `write_with_if_not_exists` plus `write_with_if_version_match` or `write_with_if_match` with a usable metadata identifier. `if_not_changed` has no independent capability flag. The provider's configuration name is not its backend type. Then issue real requests against a unique temporary key under the same `probes/` naming convention as the object-store WAL:

```text
<root>/<store_key_prefix>/probes/<uuid>.json
```

1. Create distinct contents with `if_not_exists(true)`. Read them through the production Head read path; require coherent contents, a usable conditional version identifier, and non-empty `last_modified` in the returned `Metadata`.
2. Repeat the conditional create on the same key with different contents. Accept `ConditionNotMatch` or `AlreadyExists`, then read back and verify the original contents and conditional identifier are unchanged.
3. Replace it with different contents using `if_not_changed(&current_metadata)`. Require success and read back coherent new contents and metadata, with a changed conditional identifier and available `last_modified`.
4. Attempt another replacement with the old metadata. Require `ConditionNotMatch`, then read back and verify the latest contents and identifier are unchanged.

Probe payloads use the version-1 Head codec, with distinct contents produced through `write_seq`. The probe uses the production Head read/write path and OpenDAL layers; for GCS it must exercise generation-based conditions. Ignored conditions, missing capabilities, incoherent or incomplete metadata, or an unresolved result fail startup. Also require unconditional writes, listing, and probe deletion. Clean up the temporary object on both success and failure, and report cleanup failures. Probe keys are isolated from `authority/` and candidate records; a probe must never create or modify the Head. Flags alone cannot prove these semantics.

# Initialization
Head creation is an explicit operator operation for a new namespace. Add a planned Metasrv subcommand following the existing `greptime metasrv <subcommand> --config-file` CLI style:

```sh
greptime metasrv init --config-file metasrv.toml
```

This proposed command opens the configured store, runs the isolated capability probe, and creates the version-1 empty Head with `if_not_exists(true)`. It does not campaign. Concurrent initializers that receive `ConditionNotMatch` or `AlreadyExists` read and validate the existing Head and report it as already initialized; they never reset its epoch or contents. Other conflicts require read-back and re-evaluation. An uncertain create is read back without blindly resending it. Creation and takeover remain separate operations:

| Property | Initialization | Takeover |
| --- | --- | --- |
| Owner | Empty | Current candidate |
| Epoch | `0` | Previous epoch plus one |
| Condition | Object absent | Metadata from the previously read Head version |
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
    replace using if_not_changed(&metadata) from that Head read
else:
    wait with jitter
```

The lease check and the conditional replacement belong to one authority operation. If two candidates use metadata from the same Head version, only one replacement succeeds, and the epoch increases exactly once. The loser rereads and re-evaluates. Candidate listing is never a vote or quorum. A takeover increments the epoch even after resignation; epoch zero represents an initialized Head with no election term.

The attempt has a provisional monotonic deadline anchored to its send time plus the lease duration. It grants leadership only when its result is confirmed before that deadline. Uncertain takeover and recovery from a retired term follow *Outcomes*; a late result cannot activate an expired attempt.

## Expiration test
A Head with no owner can be taken over immediately. Otherwise, a candidate considers its lease expired only when:

```text
wall_now > last_modified + lease_duration + clock_guard
clock_guard = max_clock_skew + timestamp_resolution
```

`last_modified` comes from the same `Metadata` as the Head version used for the decision. `wall_now` is the candidate's local wall clock in the store timestamp's time domain. The initial internal allowances are one second for the maximum candidate-to-store clock offset and one second for timestamp resolution. Deployments must keep clock offsets within that bound; the lease timing guarantee does not hold outside it. The resolution allowance must cover the supported backend's timestamp precision, verified during implementation.

This expiration rule determines when takeover is permitted. Routing uses a separate, earlier cutoff defined in *Existing Election integration*. CAS arbitrates competing updates to the same Head version. If renewal replaces that version first, the candidate must reread and repeat the expiration check. If takeover wins, the old Leader's conditional update fails. CAS does not replace the clock assumptions or validate a candidate's expiration calculation.

## Renewal
Use the independent object-store election constant `OBJECT_STORE_META_LEASE_SECS: u64 = 10`, allowing time for storage requests and result read-back without changing existing KV election leases. Default renewal runs at half this lease, every five seconds, through the shared Head writer. Renewal keeps `owner` and `epoch` unchanged and increments `write_seq`. A successful PUT refreshes the store-side `last_modified`. Sample the monotonic clock when sending the request and calculate the proposed deadline:

```text
proposed_deadline = sent_at + lease_duration
```

While the request is pending, retain the existing local deadline. Install the proposed deadline only on a successful renewal response received before that deadline, while local leadership remains active and this node still owns the term. Anchoring at send time avoids counting response latency as additional lease time.

On the first renewal failure or unknown result, immediately clear the local Leader flag, stop admitting new Leader writes, retire the local term, and emit one `StepDown`. Existing event handlers close heartbeat streams and stop or switch background tasks. Read-back may continue within the original deadline solely to determine whether the PUT applied. Late confirmation and service recovery follow *Outcomes*.

Reaching the local deadline also retires an active term; repeated failures or deadline expiry after an earlier `StepDown` do not emit it again. The future batching RFC will define whether later batch commits also count as local lease renewal.

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

The token carries the logical term. The conditional version identifier in `Metadata` can change during renewal without changing the term or a queued commit's business state. Later commit callers also supply a business precondition, such as the committed tail against which a batch was prepared.

## Version and term checks

Every successful Head mutation changes its contents and conditional version. Takeover also changes the owner and epoch, invalidating the old writer's metadata condition and logical token. Before a fenced update:

1. Obtain coherent Head contents and their complete `Metadata`, by reading them or using the writer's latest confirmed state.
2. Check that the token epoch equals the Head epoch.
3. Check that the owner is this node's process identity, local Leader state is active, the term has not been retired, and the local deadline has not passed.
4. Check the caller's business precondition.
5. Apply the mutation with `if_not_changed(&state.metadata)` for that checked state.

A failed condition requires Head read-back and re-evaluation of the term, owner, deadline, and applicable preconditions, or recovery if the Head is missing, as specified in *Error handling*. An old-epoch operation is rejected even with fresh metadata. A stale metadata condition is rejected by the store even if the caller has not observed the new epoch. All authoritative publication passes through these checks.

# Head write serialization
Each node uses one writer to serialize all Head updates. Renewal may take priority over queued operations so a busy queue does not starve the lease. Subsequent operations recheck the confirmed Head and metadata, local leadership, deadline, and business preconditions. Renewal alone does not require rebuilding a prepared batch; a moved commit tail does. An unknown outcome pauses subsequent updates. Renewal failures follow *Renewal*, and queued operations for a retired term remain rejected under *Outcomes*. Entry points are defined in *Authority interface*.

# Uncertain writes

## Problem
A conditional write can apply before its response is lost, or can remain in flight and apply after the client has timed out. A client timeout does not cancel server-side execution. The protocol preserves the distinction between a Head mutation's outcome, current ownership, and a historical client transaction's outcome.

## Read-back procedure
After a timeout or a write error that may follow persistence, pause new Head updates and apply the transition in *Renewal* when appropriate. Read the Head with backoff within the original local deadline solely to resolve the PUT outcome; the unresolved request does not extend that deadline. A takeover attempt uses its provisional deadline. Never blindly resend the original write.

Compare the read-back identifier selected by the backend for conditional writes with the pre-write identifier, then confirm application only from evidence unique to this operation. Check the complete expected state, including epoch, `write_seq`, and process identity where applicable. An owner address, diagnostic deadline, or empty owner alone is insufficient. A later Head may no longer contain enough evidence to establish an overwritten mutation's history.

## Outcomes

| Read-back result | Interpretation |
| --- | --- |
| Conditional version identifier unchanged, before or after client timeout | Still unknown; the original request may apply later |
| Evidence uniquely identifies this mutation | Confirmed applied; service recovery is handled separately below |
| Higher epoch or changed owner | Immediately abandon the current term; the original outcome remains unknown without independent evidence |
| Conditional version identifier changed without unique evidence | Still unknown; the mutation may have applied and been superseded |
| Read-back fails | Retry read-back with backoff within the original deadline |
| Original local deadline reached before confirmation | Retire the term if still active, without duplicating `StepDown`, and preserve the operation result as unknown |

A late success response or successful read-back can establish only that the PUT applied. It cannot extend or reactivate a retired term, even before the original deadline, and queued writes for that term remain rejected. Restoring Leader services and background tasks requires winning a higher epoch through normal takeover and emitting `Elected`. If a late PUT refreshes `last_modified`, candidates use that new timestamp in the unchanged expiration rule.

Once a legal takeover replaces the observed version, a delayed request with the old metadata condition cannot modify the Head. This fences future execution without proving whether it applied earlier. Historical commit outcomes need independent evidence, such as operation IDs and durable records in the published batch chain, to be defined by the future batching RFC.

# Candidate registration
Candidate registration is separate from the Authority Head. Each node owns one object:

```text
<root>/<store_key_prefix>/candidates/<encoded-node-address>.json
```

The JSON record contains the existing `MetasrvNodeInfo` and a unique refresh identifier. The existing candidate methods and their return types remain unchanged; callers receive `MetasrvNodeInfo`. A node unconditionally overwrites its own path so a restart at the same address can replace the previous record. Refresh runs at an internal interval and retries temporary errors with backoff until stopped. Each refresh attempt uses a fresh identifier, ensuring distinguishable contents and versions even when the node information is unchanged.

Freshness is judged from the store's `last_modified` and the internal candidate TTL. `all_candidates()` lists records and filters stale entries. Candidate records serve display and peer discovery only; they never decide takeover, epoch assignment, fencing, or commit ordering. Duplicate addresses remain a configuration error; process identities must remain distinct for Authority ownership.

Periodically clean up stale candidate records using an atomic conditional delete bound to the same version whose timestamp was checked. A refresh after the scan must cause the stale delete to fail and preserve the refreshed record. An unconditional delete after a freshness check is unsafe. On S3, identical-content refreshes could reuse an ETag, which is why refresh identifiers must change. Verify each backend's conditional-delete semantics, including versioned buckets, during implementation. Where safe deletion is unavailable, retain filtering and defer deletion to offline cleanup with refreshers stopped.

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
    metadata: opendal::Metadata,
}
```

`head()` returns contents and complete metadata from one coherent object version. Every update entry point uses that metadata with `if_not_changed`; lease formulas use its `last_modified`. An active writer may use its latest confirmed state for the next conditional update. Result resolution follows *Outcomes*. Followers and leader discovery read the Head from the store.

# Existing Election integration
Keep the existing `Election` trait and routing flow. `campaign()` maintains the term using *Renewal*, and result resolution and service recovery follow *Outcomes*. `is_leader()` is local state that can lag the store. Request admission checks it, the active term, and the local deadline; Head updates also enforce fencing and preconditions.

Confirmed ownership loss, reset, and resignation retire an active term through the same existing event handlers. Remember retired terms locally even while the Head still names this node. `Elected` and `StepDown` occur only on actual transitions, and the infancy flag remains one-shot. Standalone Metasrv does not use this backend.

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

For this example, the Head is `metasrv/cluster-a/authority/head.json`, candidates are under `metasrv/cluster-a/candidates/`, and probes are under `metasrv/cluster-a/probes/`. `store_key_prefix` is the only cluster namespace setting; its shared-root isolation rules are defined in *Object path*.

Election timings follow the repository's internal-constant convention. `OBJECT_STORE_META_LEASE_SECS` and its half-lease renewal interval are defined in *Renewal*, independently of existing KV election timing. Campaigning uses a 500 ms interval with jitter. Clock allowances remain in *Expiration test*. Candidate TTL and refresh reuse the existing 600-second lease and half-lease refresh interval. These are internal constants, not additional configuration options.

# Error handling
Storage branches use OpenDAL `ErrorKind` and its retry metadata. Renewal failures first follow *Renewal*; unknown results and service recovery follow *Outcomes*:

| OpenDAL result | Action |
| --- | --- |
| `ConditionNotMatch` | Read the Head; if present, recheck the term, owner, deadline, and operation preconditions against the new state; if missing, stop election and writes and enter operator recovery |
| `Conflict` | Read back and re-evaluate contention; retryability alone does not justify replaying the mutation |
| `NotFound` on a Head read | Stop and enter operator recovery; never initialize automatically |
| `Unexpected` or a temporary write error where persistence is uncertain | Pause updates and use the uncertain-write procedure |
| Temporary read-back failure | Retry the read with backoff within the original local deadline |
| `Unsupported`, `ConfigInvalid`, or `PermissionDenied` | Report the capability, configuration, or permission failure; fail startup or the operation |

A client timeout follows the uncertain-write procedure; it is not a separate OpenDAL error kind. A missing target during conditional replacement is a failed condition, not proof that a new Head should be created: only the follow-up read establishes absence. Business-precondition failures return the corresponding conflict for the caller to rebuild and re-evaluate. Preserve unresolved operations as unknown at the API boundary, rather than inviting duplicate execution through an ordinary rejection.

# Metrics and observability
Existing backend-labelled Metasrv metrics cover ordinary request and transaction latency. Add Head write latency and outcome, conditional-write conflicts, uncertain writes and read-back attempts, takeover count and current epoch, renewal failures, late successes, forced step-downs, observed timestamp skew, and stale candidate and cleanup-failure counts. These distinguish storage latency, clock-bound violations, lease churn, and candidate accumulation. Clock-skew metrics do not remove the deployment's bounded-clock requirement.

# Compatibility and backup
The Authority Head is a persisted JSON format covered by the repository compatibility framework. Freeze both version-1 examples, including the non-empty `NodeIdentity`, and verify deterministic `serde_json` encoding and decoding under the rules in [Encoding](#encoding).

A consistent backup contains the Head and, once later RFCs add them, the published batch chain reachable from `last_batch` and the required tree objects. Candidate records and probes are excluded. The Head and referenced metadata must come from one consistent recovery point. Restoring a backup is an operator recovery action with existing writers fenced, not initialization of an empty Head.

# Failure scenarios

This table defines the expected outcomes. The validation plan references scenario IDs instead of repeating them; retirement and service recovery use the contracts in *Renewal* and *Outcomes*.

| ID | Scenario | Expected behavior |
| --- | --- | --- |
| F1 | Concurrent initialization in a new namespace | One create succeeds; other initializers validate the existing Head without resetting it |
| F2 | Two candidates take over from the same observed version | Exactly one wins and the epoch increases once |
| F3 | Leader stops renewing | A candidate takes over after the guarded expiration of the latest store timestamp |
| F4 | First renewal failure or unknown result | Perform the immediate local retirement and service shutdown defined in *Renewal* |
| F5 | Old Leader uses stale metadata or an old epoch | The store rejects the stale condition; logical checks reject the old epoch even with fresh metadata |
| F6 | Renewal races with takeover on the same version | Exactly one conditional replacement succeeds |
| F7 | A write applies but its response is lost | Unique read-back evidence confirms application; otherwise preserve the unknown result |
| F8 | A timed-out renewal applies or is confirmed later | Apply *Outcomes* even before the original deadline; candidates honor any refreshed store timestamp |
| F9 | The former Leader wins a higher epoch | Service recovery follows *Outcomes* through the existing event handlers |
| F10 | Routing has expired while takeover is not yet permitted | Return `ElectionNoLeader` and stop routing the current write request |
| F11 | A routing hint names a node that stepped down early | An uninformed follower may return the hint before its cutoff; the receiver rejects the write, and a node aware of retirement returns `ElectionNoLeader` |
| F12 | Takeover precedes a delayed old-term request | The old metadata condition cannot replace the new Head |
| F13 | Head is missing at startup or runtime, including behind an S3 delete marker | Stop election and writes and require operator recovery; never initialize automatically |
| F14 | Head is deleted between observation and conditional replacement | `ConditionNotMatch` triggers read-back: recheck term, owner, and preconditions if present, or follow F13 if missing |
| F15 | Candidate refresh fails temporarily or a node restarts | Refresh retries with backoff; records become stale or are replaced by distinguishable refreshes |
| F16 | Candidate refresh races with stale cleanup | Safe conditional deletion preserves the refreshed record; otherwise use the offline-cleanup fallback |
| F17 | Multipart or staged publication uses stale metadata | Final publication rejects the condition and leaves the current Head unchanged |
| F18 | Leader resigns | Owner becomes empty; the next takeover starts a higher epoch |
| F19 | Startup probe encounters ignored conditions or missing metadata/capabilities | Fail startup, clean up the unique probe key, and never modify the Head |
| F20 | A process restarts at the same address | Its new incarnation does not inherit the previous process's ownership |

# Validation plan

## Format and deterministic protocol tests

Freeze both version-1 JSON examples and cover deterministic serialization, required fields (including `owner`, `addr`, and `incarnation_id`), UUID encoding and validation, malformed JSON, invalid types, and unsupported versions. Add checked-overflow cases for both counters. Use injected wall and monotonic clocks and coherent mock `Metadata`; in-memory timestamps do not qualify a backend's real semantics.

Add three explicit compatibility cases:

- A v1 reader accepts an otherwise valid Head with an unknown top-level field and decodes the known fields unchanged.
- A v1 reader accepts an unknown field inside a non-empty `owner` and decodes its `NodeIdentity` unchanged.
- A v1 reader rejects a Head with `format_version: 2` even when the rest of its body follows the v1 structure.

Unit and in-process tests cover the following scenarios, using the outcomes in [Failure scenarios](#failure-scenarios):

| Scenarios | Test-layer-specific checks |
| --- | --- |
| F1, F19 | Exercise both `ConditionNotMatch` and `AlreadyExists` on repeated create; inject ignored conditions, incomplete or mismatched metadata, and probe cleanup failures. Check path isolation under `probes/` |
| F2, F5, F6, F18, F20 | Synchronize candidates on one snapshot; test stale metadata separately from a stale epoch with current metadata. Reuse an address with a new UUID, and keep each process UUID stable across operations |
| F3, F4, F9–F11 | Inject exact routing/takeover cutoffs, clock offsets, and Head-read delay. Observe leadership flags, event counts, heartbeat closure, and background-task callbacks through the existing handlers |
| F13, F14 | Delete between read and replacement; drive both present and missing follow-up reads and inspect the re-evaluated term, owner, and operation preconditions |
| F15, F16 | Keep node information unchanged across refreshes, advance refresh identifiers, pause cleanup after its scan, and inject transient refresh errors |

Also check writer serialization, renewal priority over queued operations, queue rejection after retirement, and business-precondition re-evaluation against confirmed metadata. Mock both native version-match and ETag-match lowering, including unusable condition metadata; do not add an `if_not_changed` capability flag.

## Failure injection

Cover F4, F7–F9, and F12 by controlling persistence and response delivery independently: lose a response after persistence, hold a request until after client timeout, return successful read-back before and after the original deadline, and release an old request after takeover. Pause processes and interleave queued operations. Record the original deadline and event-handler ordering to check the referenced outcomes without relying on sleeps or eventual flags alone. Include late successful takeover confirmation against its provisional deadline.

## Real-backend qualification

Qualify S3-compatible endpoints including MinIO, Azure Blob, and GCS through the production Head read/write path and configured OpenDAL layers. Run the scenario groups below with real requests; mock tests alone do not enable a backend.

- **F2, F5, F6, F17, F19:** verify coherent contents, conditional identifier, and `last_modified` from one version. Test current and stale metadata at the actual publication point, including forced S3 multipart completion and Azure block-list publication. Check completed-write visibility, timestamp precision, and that retry layers do not replay unresolved Head writes.
- **GCS qualification for F2, F5, F6, F17, F19:** use a real endpoint to verify `Metadata::version()` contains the observed generation and `if_not_changed` selects the generation condition, including any staged publication path. Apply the same admission criteria as other backends. **Status: not yet tested for this RFC revision; enable only after these tests pass.**
- **F1, F13, F14, F19:** verify repeated-create error normalization and read-back. A missing conditional-replacement target must map to `ConditionNotMatch`; only a subsequent Head read determines absence. In a versioned S3 bucket, test a delete marker on a disposable key and inspect retained history. Verify S3 ETag quotes survive round trips unchanged.
- **F15, F16:** check candidate listing metadata and each backend's actual conditional-delete capability and behavior, including versioned storage and refresh/delete interleavings.
- **F4, F7–F12:** repeat fault-injection schedules with real request delays, routing cutoffs, and event handlers. Measure renewal request and read-back latency under queued load to validate the margin provided by the ten-second lease and five-second renewal interval.

OSS remains deferred. Its future qualification must test real-bucket `PutObject` conditional writes and the adapter's preservation of the same atomic replacement contract before admission is reconsidered.

# Implementation plan
The work is split into reviewable changes:

- Add Authority types, persisted process identities, versioned JSON encoding, and both compatibility fixtures.
- Add capability-based backend admission and the isolated `probes/` runtime checks, including GCS generation-path qualification before enablement.
- Add coherent Head/Metadata reads, `if_not_changed` mutations, explicit initialization CLI, and missing-Head recovery errors.
- Add takeover, epoch fencing, the independent `OBJECT_STORE_META_LEASE_SECS` constant, renewal, local deadlines, and resignation. Validate request and read-back timing margins at the five-second renewal interval within the ten-second lease.
- Add the single-writer task and uncertain-write read-back.
- Add retrying candidate refresh, safe stale cleanup, and the existing `Election` integration.
- Add backend selection and storage options, metrics, and real-backend qualification with failure injection.

The future batching RFC adds commit-tail mutations without changing the election contract. The B-link Tree RFC adds checkpoint publication through the same writer.

# Future work
The future batching RFC will define immutable batches, operation identifiers, commit-tail preconditions, and client-visible commit confirmation. The B-link Tree RFC will define nodes, manifests, checkpoint roots, and read semantics. A later phase may add checkpoint garbage collection, quorum replication, and follower-local consistent reads.
