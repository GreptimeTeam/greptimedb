---
Feature Name: Object Store Metadata Backend — Authority and Leader Election
Tracking Issue: TBD
Date: 2026-11-10
Author: Alorun
Status: Draft for discussion
---

# Summary
Add an object-store-backed authority and leader election mechanism to GreptimeDB's Metasrv. The object store already contains the data files used by GreptimeDB. Moving the metadata authority to the same storage layer can reduce the separate metadata service dependency for deployments that want a bucket-based architecture. Object stores do not provide the multi-key transactions and lease primitives that etcd provides.
They do provide strongly consistent single-object reads and atomic conditional writes. This RFC builds the missing authority layer from those single-object primitives. The design introduces one fixed **Authority Head** object. The Head records the current Leader, the election epoch, the write sequence, and the physical object version needed for conditional updates. Every metadata write that changes authoritative state must pass through this Head. The Head has three responsibilities:

- It identifies the current Leader.
- It carries the fencing epoch that invalidates an old Leader.
- It provides the single serialization point for later metadata commits.

This RFC defines the Authority and Leader Election contract for the first phase. Batching, immutable batches, and the B-link Tree will be specified in later RFCs and will reuse the Head defined here.

# Motivation
Metasrv currently uses etcd, MySQL, or PostgreSQL as the metadata authority and leader-election backend. The data path already relies on object storage, so a bucket-backed deployment still needs an independently operated metadata service. Replacing the metadata service with object storage is not a simple key-value translation. The current metadata workflow depends on several properties:

- A write must be serialized with other related writes.
- A Leader that has lost its term must not commit later.
- A failed request must not leave the caller unable to determine its outcome.
- A new Leader must be able to take over after the previous Leader fails.

The object store lacks a cross-object transaction and does not expose a lease primitive. It does support the following operations on one object:

| Capability | Required property | Use in this design |
| --- | --- | --- |
| Read one object | A read does not go backwards | Read the Head and resolve uncertain writes |
| Create conditionally | Create only if the object is absent | Initialize the Head and immutable objects |
| Replace conditionally | Replace only if the ETag is unchanged | Takeover, renewal, resignation, and commits |
| Object metadata | ETag and last-modified timestamp | Version checks and lease judgement |
| Listing | Discover objects | Candidate display only |

The central design choice is therefore to make the Leader the single writer of authoritative metadata. The object store rejects a writer whose conditional version is stale, while the epoch rejects a writer whose term is stale. This introduces an availability trade-off. With etcd, ordinary metadata writes can continue on followers because etcd serializes them. With the object-store backend, every authoritative
metadata write is routed through the Leader. A failover can therefore pause all metadata writes for one lease interval. The lease duration is consequently an operational parameter. It must be long enough for the object-store round trip under load and short enough to meet the deployment's failover requirement.

# Goals

- Elect exactly one effective Leader among concurrently starting nodes.
- Allow a surviving node to take over after the previous Leader expires.
- Prevent an old Leader from modifying the Head after a takeover.
- Make the object store, rather than the Leader's wall clock, the lease authority.
- Resolve uncertain conditional writes without blindly retrying the original write.
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

This RFC defines the shared authority contract. It does not implement the `KvBackend` write methods because their visibility semantics depend on the batching RFC. The later RFCs may extend the Head payload with fields that are declared here:

- `committed_sequence`
- `last_batch`
- `checkpoint_root`

The election fields and their meanings are fixed by this RFC.

# Design overview
The design separates three questions:

- **Who may write?** The current epoch and ownership of the Head decide this.
- **When is a write visible?** The Head commit performed by the batching layer decides this.
- **Where does data live?** Immutable batches and B-link Tree objects decide this.

The Authority Head is rewritten in place. Every mutation reads the current Head and uses its ETag in `If-Match`.
The high-level sequence is:

1. An operator explicitly initializes an empty Head.
2. Nodes register themselves as candidates.
3. Candidates read the Head and campaign when the lease is absent or expired.
4. Exactly one candidate replaces the Head using the previously read ETag.
5. The winner becomes Leader and receives the new epoch as its fencing token.
6. The Leader renews the Head through the same serialized writer used by commits.
7. A node that loses the epoch or cannot renew before its local deadline stops writing.
8. Another candidate takes over with a higher epoch.

The election decision is made against the Head state. Candidate listing is never used to decide who may write.

# Authority Head

## Object path
The Head uses a fixed object path:

```text
<prefix>/<cluster_id>/authority/head.json
```

`cluster_id` is a required, operator-supplied configuration value. It must be stable for the lifetime of a cluster and unique within the configured object-store prefix. It is not generated from a node identity and is not discovered by listing. Including it in the path prevents two clusters from addressing each other's metadata by mistake. The path is rewritten in place. It is never appended to, and no other object may be written to this path.

## Payload
The first phase fixes the following logical payload:

```rust
struct AuthorityHead {
    cluster_id: String,
    owner: Option<NodeIdentity>,
    epoch: u64,
    lease_deadline_ms: u64,
    write_seq: u64,
}
```

The fields have the following meanings:

| Field | Meaning | Owner in phase one |
| --- | --- | --- |
| `cluster_id` | Cluster identity encoded with the object | This RFC |
| `owner` | Address of the current Leader | This RFC |
| `epoch` | Monotonic election term and fencing token | This RFC |
| `lease_deadline_ms` | Writer's diagnostic wall-clock deadline | This RFC |
| `write_seq` | Count of successful Head writes | This RFC |

The initial Head contains no owner and starts at epoch zero:

```json
{
  "cluster_id": "example-cluster",
  "owner": null,
  "epoch": 0,
  "lease_deadline_ms": 0,
  "write_seq": 0
}
```

`lease_deadline_ms` is diagnostic only. Lease judgement never trusts the deadline written by a node because that value uses the node's wall clock. `write_seq` increases for every successful Head write. It makes the payload change even when a renewal writes otherwise identical logical state. This is necessary on content-hash backends such as S3, where writing the same bytes could otherwise leave the ETag unchanged.

## Encoding
The object uses a fixed binary envelope in the same style as the object-store WAL format, but with its own magic and layout:

```text
| magic (8 bytes, for example `GTAUTHHD`) | format_version | cluster_id_len |
| cluster_id | payload_len | payload | crc32 |
```

This is not a reuse of the WAL header or trailer. The magic value is distinct from `GTWALOBJ` and the layout is independently versioned. The header carries the cluster identity before business fields are decoded. The checksum covers the payload and detects truncated or corrupted content. A reader rejects an unknown format version instead of guessing its layout. Unknown fields from a supported future-compatible payload may be ignored when the format rules permit it. The exact binary field widths and serialization library are implementation details, but they must be frozen before the first persisted compatibility fixture is added.

# Object-store semantics

## Object-store capabilities

The Authority implementation depends on the following object-store capabilities. Strong read-after-write consistency for a single object is a deployment prerequisite. It is required for Head reads and read-back, while listing consistency is not required for election or commit decisions.

| Capability | Purpose |
| --- | --- |
| Strong single-object read | Read the Authority Head and resolve uncertain writes |
| `If-None-Match` | Initialize the Authority Head |
| `If-Match` | Takeover, renewal, and resignation |
| ETag | Identify the object version |
| `last_modified` | Determine lease expiration |
| Unconditional write | Register candidate nodes |
| Object listing | Discover candidate nodes |

The first phase explicitly supports S3-compatible object stores, including MinIO, and Azure Blob. OpenDAL 0.59.4 declares the ETag `If-Match` capability for these backends. GCS exposes generation-based conditions instead of the ETag `If-Match` capability used by this RFC, while OSS and the local filesystem do not declare the required write condition. They are therefore rejected by this backend in the first phase. Supporting GCS through generation-based CAS or adding missing capabilities to another backend requires a separate adapter or OpenDAL change and is outside this RFC.

| Backend | ETag `If-Match` CAS | First-phase status |
| --- | --- | --- |
| S3, including MinIO | Supported | Supported |
| Azure Blob | Supported | Supported |
| GCS | Not declared. Generation conditions are a different API | Unsupported |
| OSS | Not declared | Unsupported |
| Local filesystem | Not declared | Unsupported |

GreptimeDB already provides object-store access through OpenDAL. The new logic belongs in the Metasrv adapter and covers Head encoding and decoding, reading ETag and `last_modified` metadata, conditional-write error handling, election, lease and fencing logic, candidate registration, and capability validation. The capability probe remains a startup safety check. It does not expand the supported backend set.

Distributed election, leases, and fencing are implemented by this RFC in Metasrv because the object store does not provide these semantics itself.

## Conditional writes

The design uses two conditional write forms:

| Operation | Condition | Purpose |
| --- | --- | --- |
| Create if absent | `If-None-Match: *` | Initialize the Head and immutable objects |
| Replace if unchanged | `If-Match: <etag>` | Takeover, renewal, and resignation |

OpenDAL exposes these through `write_with(...).if_not_exists(true)` and `write_with(...).if_match(etag)`. The condition must be evaluated atomically with the visible object mutation. A `stat` followed by an unconditional write is not acceptable. If a layer cannot preserve conditional-write semantics, the backend must reject that layer during startup instead of silently dropping the condition. Conditional-operation error kinds are used for branching. A raw HTTP status such as `412` is not sufficient to distinguish an explicit condition failure from an unrelated service conflict.

## Object version

The object version is represented by the ETag returned in object metadata:

```rust
struct ObjectVersion(String);
```

The ETag is opaque and is never interpreted as a content checksum. It must be passed back to `If-Match` exactly as returned by OpenDAL, including surrounding quotes when present. GCS object generation is not used as the version argument because `If-Match` expects an ETag, while generation belongs to a different conditional API. The ETag is the physical concurrency token. The epoch is the logical fencing token. They serve different purposes and cannot replace each other.

## Store timestamp and lease

Lease expiration is determined using the `last_modified` value supplied by the object store:

```text
now > last_modified + lease_duration + clock_guard
```

where:

- `last_modified` is the time recorded by the object store for the most recent Head modification.
- `lease_duration` is the configured lease duration.
- `clock_guard` covers the clock offset between the node and the object store, as well as timestamp precision error.

The guard is calculated as:

```text
clock_guard = clock_skew_ms + timestamp_resolution_ms
```

The target object stores usually report `Last-Modified` with one-second precision, so the default `timestamp_resolution_ms` allowance is one second. Candidates evaluate the comparison with a local wall clock in the same timestamp domain as `last_modified`. The monotonic clock is used for local deadlines and elapsed intervals. The lease is anchored to the object-store timestamp rather than to a deadline written by the Leader.

The Leader also maintains a local deadline. The monotonic clock is sampled when a renewal request is sent:

```text
local_deadline = sent_at + lease_duration
```

An uncertain renewal must not extend the local deadline. Once the deadline passes, the node stops writing and returns to the election process. Reads must not regress to an older version already observed during the same protocol execution. Election and commit decisions do not rely on listing consistency. Listing is used only for candidate discovery.

## Capability probe

At startup, after all configured layers have been applied, the backend validates the required object-store capabilities:

- `write_with_if_not_exists`
- `write_with_if_match` on an explicitly supported backend
- An ETag after a successful Head write
- A non-empty `last_modified` timestamp after a probe write
- Unconditional writes for candidate registration
- Listing for candidate display

The configured `storage_provider` is checked against the explicit first-phase allowlist before the store is opened. The first two capabilities are then checked through OpenDAL's declared capability flags. Timestamp availability is checked at runtime because OpenDAL has no corresponding capability flag. The backend writes a temporary probe object, stats it, and requires a non-empty `last_modified` value. Missing timestamp support or conditional-write support causes startup failure, and the error names the missing capability and the operator that was checked.

# Initialization
Head creation is an explicit operator operation, not an action performed by a running election loop. Creation uses `If-None-Match: *` and writes an empty Head with epoch zero. If several operators or nodes initialize concurrently, exactly one conditional create succeeds. The others read the created Head and campaign normally. Creation and takeover remain separate operations:

| Property | Initialization | Takeover |
| --- | --- | --- |
| Owner | Empty | Current candidate |
| Epoch | `0` | Previous epoch plus one |
| Condition | Object absent | Previously read ETag |
| Lease | Not established | Established by this write |

The initial empty Head has no lease deadline. The first takeover establishes the first lease using the store timestamp. A running node that cannot find the Head must stop and report an error. It must not recreate an empty Head automatically because a missing Head may mean that an existing commit tail has been lost. Reinitializing over a lost Head is an operator recovery decision. It may orphan published batches and
reset the authority history.

# Leader election

## Takeover
A candidate repeatedly calls `Authority::takeover`:

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

The lease check and the conditional write are part of one authority operation. The candidate does not use the candidate list as a vote or quorum. If two candidates race, both may observe an expired lease, but only one can replace the old ETag. The losing candidate rereads the Head and continues campaigning. A takeover increments the epoch even when the previous owner resigned and left the Head empty. Epoch zero
therefore means an initialized Head with no election term. Every active term has a positive epoch.

## Expiration test
A candidate applies the expiration formula defined in *Store timestamp and lease*. `clock_guard` covers the candidate's offset from the store and truncation of the store timestamp. For the stores in scope, the default resolution allowance is one second because `Last-Modified` is reported at one-second precision. The candidate's judgement may be early or late, but a wrong early judgement is prevented from becoming a safety violation by the ETag conditional write. If the current Leader renewed first, the takeover condition fails. If the takeover landed first, the renewal condition fails and the old Leader steps down after observing the new epoch.

## Renewal
The Leader renews on a fixed interval through the same Head writer used for other mutations. A renewal keeps `owner` and `epoch` unchanged and increments `write_seq`. The store's new `last_modified` timestamp extends the store-side lease. The Leader's local deadline is anchored at the time the renewal request is sent, using a monotonic clock:

```text
local_deadline = sent_at + lease_duration
```

Anchoring at send time is conservative. Anchoring at response time could make the Leader believe it may write after the store-side lease has expired. A renewal that fails transiently does not extend the local deadline. A renewal whose outcome is unknown is resolved by read-back before another Head mutation is started. If the local deadline passes, the node stops attempting to renew and stops writing.

## Resignation
A Leader may resign by clearing `owner` and keeping the current `epoch`. The resignation still changes the Head version and invalidates the old writer's ETag. The epoch is not incremented because resignation ends a term. The next takeover starts the next term and increments the epoch. A resigning node stops writing immediately after the resignation is confirmed.

# Fencing and comparison

## Fencing token
A Leader receives the current epoch as its fencing token:

```rust
struct FencingToken {
    epoch: u64,
}
```

The token contains the logical term only. It does not contain the ETag. The ETag belongs to the writer's current read-modify-write operation and may change because of a renewal that does not change the business state of a queued commit. The writer checks both the token and the current owner before applying a mutation. The caller supplies a business precondition for later commit operations, such as the committed tail
against which a batch was prepared.

## Why ETag subsumes epoch in CAS
The Head payload changes on every successful write, and the ETag changes with it for the supported backends. Therefore, a conditional write using the ETag read before the mutation succeeds only if no other Head mutation has landed since that read. A takeover changes owner, epoch, and write sequence, so an old ETag cannot pass through a takeover. The epoch check remains necessary as an early logical check and as the
durable fencing token passed through the metadata write path. The write procedure is therefore:

1. Read the Head and its ETag.
2. Check that the token epoch equals the Head epoch.
3. Check that the owner is the current node.
4. Check the caller's business precondition.
5. Apply the mutation with `If-Match` using the current ETag.

A failed ETag condition triggers a reread and re-evaluation rather than an unconditional retry.

# Head write serialization
Each node has one writer task that owns its current Head state and serializes all Head operations. Takeover is a separate `Authority::takeover()` operation because it mints the first token for a term. It is not a `Mutation` variant. Renewal and resignation use `Authority::mutate()`:

| Operation | Entry point | Term required | Main effect |
| --- | --- | --- | --- |
| `Takeover` | `takeover()` | No | Establish owner and increment epoch |
| `Renew` | `mutate(..., Mutation::Renew)` | Yes | Extend the lease and increment write sequence |
| `Resign` | `mutate(..., Mutation::Resign)` | Yes | Clear owner and invalidate the old version |

Later batching and layout RFCs may add `Commit` and `PublishCheckpoint` mutations to the same writer. Renewal has priority over queued mutations. A busy queue must not starve lease renewal. Before each operation, the writer rechecks:

- The current epoch.
- The current owner.
- The caller's business precondition.
- The local deadline.

A renewal that runs before a queued future commit changes only the writer's ETag and lease metadata. The queued commit uses the refreshed ETag and does not rebuild its batch. A future commit whose prepared tail has already moved fails its business precondition. The caller rebuilds the batch and re-evaluates its comparisons. A failed conditional write is resolved by rereading the Head. The writer does not start a second operation while the first result is unresolved.

# Uncertain writes

## Problem
A conditional write can time out after the object store applied the mutation but before the response reached the node. Treating every timeout as failure can cause a caller to repeat a transaction. Treating every timeout as success can allow a node to continue after losing its term. The protocol therefore distinguishes object-write outcome from client transaction outcome. The object-write layer resolves the first
question. The batching layer later uses an operation identifier in the published batch chain to resolve the second.

## Read-back procedure
After a write failure or conflict, the writer reads the Head again. The read-back answers two independent questions:

- Has any write landed since the pre-write read?
- Does the resulting Head state prove that our mutation applied?

The ETag answers the first question. The Head fields and, for commits, the published batch chain answer the second. The protocol never compares only the intended fields because a later mutation may have overwritten them after the original write landed.

## Outcomes

| Read-back result | Interpretation |
| --- | --- |
| ETag changed and the state proves the mutation is current | Confirmed applied |
| A prior read or durable operation record proved our mutation, and a later Head state has replaced it | Applied and superseded |
| ETag changed and the state proves another mutation, with no evidence that ours landed | Confirmed not applied |
| ETag changed and the state is newer but cannot prove whether ours landed | Unknown or possibly superseded |
| ETag unchanged before request timeout | Still unknown |
| ETag unchanged after request timeout | Confirmed not applied |
| Read-back fails | Unknown and retry read-back |
| Local deadline passes before resolution | Abandon term and stop writing |

A successful write is immediately treated as confirmed applied. The original write is never retried. Only the side-effect-free read-back may be retried with backoff. The final Head alone cannot prove that an overwritten mutation ever landed. A newer owner or epoch without operation-specific evidence is therefore classified as unknown or possibly superseded, and the original write is not retried. For renewal, the write is applied when the owner, epoch, and written deadline match. For takeover, the write is applied when the node owns the Head at the incremented epoch. For resignation, the write is applied when the owner is empty. For a later commit, the operation identifier is searched in the published batch chain rather than only in the current Head's last-batch field. While a write is unresolved, the writer pauses the queue. This prevents a second operation from making the first outcome ambiguous. If the resolution cannot finish before the local deadline, the node drops its term and returns to campaigning.

# Candidate registration
Candidate registration is separate from the Authority Head. Each node owns one object:

```text
<prefix>/<cluster_id>/candidates/<encoded-node-address>.json
```

The record is the existing `MetasrvNodeInfo` structure. It includes the node address, process start timestamp, software version, git commit, and the optional resource and hostname fields already defined by Metasrv. The election methods use the existing trait signatures:

```rust
async fn register_candidate(&self, node_info: &MetasrvNodeInfo) -> Result<()>;
async fn all_candidates(&self) -> Result<Vec<MetasrvNodeInfo>>;
```

A node unconditionally overwrites its own object at a fixed refresh interval. Unconditional writes are required because a node restarting at the same address must be able to replace its previous record. The object store has no server-side lease for this record, so freshness is judged from the object's `last_modified` timestamp. A candidate is stale when its timestamp is older than the configured candidate TTL.
`all_candidates()` lists candidate objects and filters stale entries. The list is used for operator display and peer discovery only. It does not participate in takeover, epoch assignment, fencing, or commit ordering. This separation prevents candidate heartbeats from contending with the Head commit path. A duplicate node address is an operational configuration error. The latest process may overwrite the earlier
candidate record, but the epoch and ETag still prevent both processes from becoming valid writers.

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

`takeover()` is a dedicated operation and is not represented by the `Mutation` enum. `mutate()` is used for fenced operations in an existing term, initially renewal and resignation. Later RFCs may add commit and checkpoint mutations without changing the election contract.

`AuthorityState` contains:

```rust
struct AuthorityState {
    head: AuthorityHead,
    version: ObjectVersion,
    last_modified: Timestamp,
}
```

`head()` reads from the object store rather than returning a cached copy. The writer's local copy is authoritative only for the next conditional mutation it is about to issue. Followers and forwarded requests must observe the current Head from the store. The later batching RFC supplies a `Commit` mutation and a commit-tail precondition. The later layout RFC supplies a `PublishCheckpoint` mutation.

# Existing Election integration
The object-store election backend implements the existing `Election` trait with the following behavior:

| Method | Behavior |
| --- | --- |
| `is_leader()` | Returns the local leadership flag |
| `in_leader_infancy()` | One-shot flag after a successful takeover |
| `campaign()` | Blocks until takeover, then renews until leadership is lost |
| `reset_campaign()` | Clears local campaign state |
| `leader()` | Leader answers locally; followers read the Head |
| `resign()` | Clears owner through a fenced Head mutation |
| `register_candidate()` | Refreshes the candidate object until stopped |
| `all_candidates()` | Lists and filters candidate objects |
| `subscribe_leader_change()` | Reuses the existing broadcast channel |

`LeaderValue` continues to carry the node's gRPC address. The custom `LeaderKey` maps the owner address to `name()`, a stable serialized authority key containing the Head path, owner, and current epoch to `key()`, the epoch to `revision()` after checking that it fits in `i64`, and the epoch to `lease_id()` as the identifier of the current logical lease. `lease_id()` is not a deadline. No change to the existing trait is required for the election semantics described here. The clustered Metasrv adds the object-store backend to its backend enumeration. The standalone deployment is unchanged because it supplies its own local metadata backend and does not run an election.

# Configuration
The backend uses one experimental configuration table because the provider, path, cluster identity, and election parameters form one coherent backend configuration. This is the same experimental naming family as the existing object-store WAL provider, while keeping Metasrv Authority settings separate from WAL settings.

```toml
[experimental_object_store_meta]
storage_provider = "s3"
prefix = "metasrv"
cluster_id = "example-cluster"
lease_secs = 10
renew_interval_secs = 3
clock_skew_ms = 1000
campaign_interval_ms = 500
candidate_refresh_secs = 300
candidate_ttl_secs = 600
```

The configuration has the following meaning:

| Setting | Purpose |
| --- | --- |
| `storage_provider` | OpenDAL storage provider name. Empty selects the configured default object store |
| `prefix` | Root prefix for Authority and candidate objects |
| `cluster_id` | Stable operator-supplied cluster identity, unique within `prefix` |
| `lease_secs` | Lease duration and maximum failover pause |
| `renew_interval_secs` | Renewal frequency, strictly below lease duration |
| `clock_skew_ms` | Candidate clock offset tolerated during expiry checks |
| `campaign_interval_ms` | Base delay between takeover attempts |
| `candidate_refresh_secs` | Candidate object refresh interval |
| `candidate_ttl_secs` | Candidate freshness window |

The initial ten-second lease is a starting value rather than a universal constant. It is longer than the existing etcd lease because each renewal is an independent object-store request and may experience greater latency. It must be calibrated against the target object store and the deployment's failover requirement. The renewal interval must leave enough time for retries and read-back before the local deadline. The
candidate refresh and TTL defaults mirror the existing candidate lease shape.

# Error handling
The backend maps authority failures to existing Metasrv error categories:

| Condition | Status | Retry behavior |
| --- | --- | --- |
| Epoch mismatch | `IllegalState` | Step down, do not retry as Leader |
| Not the current owner | `IllegalState` | Stop writing and campaign again |
| Missing Head | `IllegalState` | Stop and require operator recovery |
| Store unavailable | `StorageUnavailable` | Retry or back off |
| Read-back unavailable | `StorageUnavailable` | Retry the read-back |
| Invalid configuration | `InvalidArguments` | Fail startup or request |
| Missing capability | `Unsupported` | Fail startup |
| Business precondition failed | Existing conflict status | Rebuild and re-evaluate |

An unknown write result is not exposed as a normal business error while the writer can still resolve it. A read-back failure may be surfaced as retryable storage unavailability when the local deadline has not yet expired. An epoch mismatch is non-retryable for the current term. Repeating it would keep a stale node in the Leader role.

# Metrics and observability
Existing backend-labelled Metasrv metrics continue to cover ordinary request and transaction latency. The object-store backend adds Head write latency and outcome, conditional-write conflicts, uncertain writes and read-back attempts, takeover count and current epoch, renewal failures and forced step-downs, lease expiry skew, and stale candidate count. These metrics separate object-store latency, lease churn, and
leadership changes. A rising epoch or renewal-failure rate indicates an unstable lease configuration. A rising expiry skew indicates that a Leader's wall clock is unreliable, while lease judgement remains based on the store timestamp.

# Compatibility and backup
The Authority Head is a persisted internal format and is covered by the repository compatibility framework. The implementation adds a frozen Head fixture and a deterministic encode/decode test. Unsupported format versions are rejected, and compatible readers may ignore explicitly allowed future fields. A consistent backup contains the Authority Head, the published batch chain reachable from `last_batch`, and the tree
objects required by the layout RFC. Candidate objects are excluded because they are rebuilt. The Head and referenced objects must come from one consistent backup point.

# Failure scenarios
The protocol must cover the following cases:

| Scenario | Expected result |
| --- | --- |
| Concurrent initialization | One conditional create succeeds; other nodes read and campaign |
| Concurrent takeover | One ETag replacement succeeds and increments the epoch |
| Leader stops renewing | A candidate takes over after the guarded expiry |
| Old Leader resumes | Epoch or ETag validation rejects its writes |
| Renewal races with takeover | Exactly one conditional mutation succeeds |
| Write response is lost | Read-back confirms the applied or rejected state |
| Head is missing at runtime | The node stops and requires operator recovery |
| Candidate crashes or restarts | Its record becomes stale or is overwritten at its own path |
| Leader resigns | Owner becomes empty; the next takeover uses the next epoch |

# Validation plan
Unit tests cover Head encoding, epoch changes, resignation, lease boundaries, clock guards, local deadlines, fencing tokens, business preconditions, and uncertain-write state transitions. In-process object-store tests cover conditional initialization, concurrent takeover, renewal, resignation, ETag conflicts, writer serialization, candidate registration, and stale filtering. Timestamps are injected into
`AuthorityState` because the in-memory service does not provide real `last_modified` metadata. Integration tests run against MinIO and Azure Blob where credentials are available. They verify atomic conditional writes, ETag round trips, timestamps, completed-write visibility, candidate listing metadata, response loss, and network or process pauses through a failure-injection layer. Compatibility
tests decode a frozen Head fixture, check deterministic encoding, and reject unsupported format versions. Integration coverage is limited to the supported S3-compatible and Azure Blob backends.


# Implementation plan
The work is split into reviewable changes:

- Add authority types, deterministic encoding, and compatibility fixtures.
- Add capability validation and the runtime timestamp probe.
- Add Head reads, conditional mutations, and explicit initialization.
- Add takeover, epoch fencing, renewal, local deadlines, and resignation.
- Add the single-writer task and uncertain-write read-back.
- Add candidate registration, stale filtering, and the `Election` implementation.
- Add metrics and MinIO or failure-injection integration tests.

The batching RFC then adds commit-tail mutations without changing the election contract. The B-link Tree RFC adds checkpoint publication through the same writer.


# Future work
The batching RFC will define immutable batches, operation identifiers, commit-tail preconditions, and client-visible commit confirmation. The B-link Tree RFC will define nodes, manifests, checkpoint roots, and read semantics. A later phase may add checkpoint garbage collection, quorum replication, and follower-local consistent reads.
