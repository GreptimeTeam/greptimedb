# Query regression runners and benchmark Job

The `Query Regression` workflow has two phases:

1. **Build** on the trusted local ARC runner scale set `perf-regression-8-cores`
   (a dedicated `minipc-3` node with a persistent build cache).
2. **Benchmark** in a one-shot Kubernetes `Job` in the Alibaba Cloud ACK
   cluster. The Job pod runs on the dedicated bulk nodepool, has no
   GitHub/cloud/kube credentials, and is created, driven, and deleted by a
   trusted controller step that runs on the local runner.

There is **no ARC scale set in the cloud**: the ACK cluster contains no
`gha-runner-scale-set` deployment, no runner listener, and does no
compilation. The benchmark `Job` is created directly by the controller through
the ACK API server.

## Architecture (v1)

```
local runner (perf-regression-8-cores)              ACK cluster (bulk-ingestion-test)
+------------------------------------------------+  +---------------------------------------------+
| build job                                      |  | one-shot Job query-regression-<run-id>-<a>  |
|  - resolves/verifies base & candidate SHAs     |  |  - digest-pinned runtime image              |
|  - compiles base+candidate binaries (cache)    |  |  - nodepool-id selector + taint toleration  |
|  - uploads BASE artifact BEFORE candidate code |  |  - non-root, no SA token, no credentials    |
|  - uploads candidate artifact separately       |  |  - waits for /payload/.ready marker         |
|  - runs tooling tests (non-ACK, non-controller)|  |  - runs perf driver only (no tooling tests) |
|  - exposes verified SHAs + artifact IDs        |  |  - writes reports + /work/.done, stays up   |
| controller job                                 |  |  - results pulled via kubectl cp             |
|  - restores trusted scripts from base commit   |  |  - Job deleted (foreground, retried) after   |
|    into $RUNNER_TEMP (never from artifact)     |  |    collect + absence verification           |
|  - downloads artifacts by exact ID, verifies   |  |  - nodepool autoscaling is implicit         |
|    manifests/attestation/checkout SHAs         |  |                                             |
|  - kubectl create Job  ────────────────────────┼─▶                                             |
|  - kubectl cp payload + exec marker            |  |                                             |
|  - collects/validates results, deletes Job     |  |                                             |
+------------------------------------------------+  +---------------------------------------------+
```

Nodepool scaling is **implicit only**: the Pending Job pod is the scale-up
signal for ACK Cluster Autoscaler, and foreground deletion of the Job/pod is
the scale-down signal. The controller never calls any nodepool
desired-size/scale API.

All candidate binaries, scripts, and cases execute **only inside the ACK Job
pod**. The controller step on the local runner only copies them
(`kubectl cp`) and never runs them.

## Prerequisites and trust admission

Install the local ARC scale set controller if it is not already installed:

```bash
helm upgrade --install arc \
  oci://ghcr.io/actions/actions-runner-controller-charts/gha-runner-scale-set-controller \
  --namespace arc-systems \
  --create-namespace \
  --version 0.14.2
```

Create the GitHub App secret in the runner namespace. Prefer an App limited to
`GreptimeTeam/greptimedb`:

```bash
kubectl -n arc-runners create secret generic greptimedb-arc-github-app \
  --from-literal=github_app_id=<app-id> \
  --from-literal=github_app_installation_id=<installation-id> \
  --from-file=github_app_private_key=<private-key.pem>
```

The values files here reference that secret by name.

A maintainer applying the `query-regression` or `heavy-regression` label is
**trust admission for that exact PR revision**. `query-regression` runs the six
routine default cases; `heavy-regression` runs only the high-cardinality
`prom_remote_write_7913` remote-write case. The admitted job may use this scale
set's dedicated, writable persistent cache. `pull_request: labeled` is the only
PR trigger: the label event snapshots its merge, head, and base SHAs. A queued
job fetches that immutable event merge SHA directly, verifies it is a two-parent
merge whose parents include the snapshotted head exactly once, and uses its
other parent as the actual base build revision. The snapshotted event base is
retained for audit only, so a difference from the merge's non-head parent is
not a failure. The job never follows a newer mutable PR merge ref. An
unavailable event merge, or one that does not contain exactly one snapshotted
head parent, fails closed. A later PR head change does not retarget an already
queued run: it may execute only its previously trusted event revision if that
revision remains fetchable. To run the new revision, the maintainer must review
it, remove the label, and re-add the desired regression label; cancel the old
run if it is no longer wanted. An existing label does not automatically rerun
the benchmark.

Admission does not relax runner hardening or GitHub permissions. Keep
service-account token mounting disabled; do not mount host paths, the Docker
socket, kubeconfig, or long-lived credentials. The runner and cache initializer
use UID/GID 1001, disallow privilege escalation, drop all capabilities, and use
the RuntimeDefault seccomp profile. Keep GitHub tokens least-privilege and
review workflow changes before admission. Where the CNI supports it, restrict
egress to required GitHub Actions, artifact/cache, Rust/crate/toolchain, DNS,
image-registry, and ACK API endpoints; block unrelated cluster services,
private ranges, and metadata endpoints unless a case requires them.

### Local scale set availability

The `build` and `query-regression-controller` jobs run on
`perf-regression-8-cores`. The live scale set is **paused** (`minRunners=0`,
`maxRunners=0`) until explicitly resumed with approval; a query-regression run
queues until a runner Pod exists. Resuming it is a prerequisite for the
workflow.

### Network routing prerequisite

Required split routing is an **external environment-specific prerequisite**. The
responsible network operator must route GitHub Actions, GitHub content,
artifact/cache, crates.io, Rust toolchain, image-registry, and the ACK API
endpoint traffic through the approved path rather than the VPN where
applicable. Neither this repository nor Kubernetes configures that route.
Verify it with the responsible network operator before any canary.

## Runner image and workflow tools

Build and push the derived runner image; it preserves the official
`/home/runner/run.sh` entrypoint and supplies CI tools needed at runtime. The
image builds `otelgen` from
[`WenyXu/otelgen`](https://github.com/WenyXu/otelgen) commit
[`863a3f395d062c7322cc1de08a38774b7fdaa6c8`](https://github.com/WenyXu/otelgen/commit/863a3f395d062c7322cc1de08a38774b7fdaa6c8)
so trace cases do not download or compile tools during a benchmark run:

```bash
docker build \
  --platform linux/amd64 \
  -f .github/runner-scale-sets/query-regression/Dockerfile \
  -t greptime-registry.cn-hangzhou.cr.aliyuncs.com/greptime/greptimedb-query-regression-runner:latest \
  .github/runner-scale-sets/query-regression

docker push greptime-registry.cn-hangzhou.cr.aliyuncs.com/greptime/greptimedb-query-regression-runner:latest
```

Deploy by digest, not mutable tag, by updating both image references in
`values-8-cores.yaml` after a rebuild. The same digest is used for the ACK
benchmark `Job` image and is pinned in `query-regression.yml` (`JOB_IMAGE`) and
in the trusted controller (`DEFAULT_IMAGE` in
`query-regression-ack-controller.py`). Update all four references and bump
`RUNNER_IMAGE_EPOCH` at the same time. If the registry is private, use a
dedicated read-only pull secret only as `imagePullSecrets` (in the ACK cluster,
in the namespace the Job runs in); never expose registry credentials to the
Job container itself. Both digest-pinned init and runner containers use
`IfNotPresent`: the immutable digest makes a cached image safe and avoids
adding a registry dependency to every runner startup.

The runner optionally imports only the non-sensitive `HTTP_PROXY`,
`HTTPS_PROXY`, and `NO_PROXY` variables from the
`query-regression-runner-local-env` ConfigMap. Manage that ConfigMap locally in
the target namespace; private endpoint configuration must not be committed, and
credentials or secrets must never be placed in a ConfigMap.

Before builds, the workflow asserts UID/GID 1001 and exact image tool versions:
`libprotoc 3.21.12`, `mold 2.30.0`, `sccache 0.16.0`, root-owned `rustup
1.29.0`, and the image-baked `nightly-2026-03-21` Rust toolchain. Rustup,
Cargo, and Rustc must resolve from `/opt/cargo/bin`; the runner cannot write
`/opt/rustup` or `/opt/cargo/bin`.
`actions-rust-lang/setup-rust-toolchain@v1` is intentionally removed. The
workflow sets its warning-denying mold `RUSTFLAGS` directly, disables automatic
Rustup installation, and performs no runtime toolchain downloads.

The workflow no longer uses GitHub `rust-cache`, `setup-protoc`, `setup-uv`, or
runtime Rust setup: the image establishes immutable executable state and the PVC
supplies only reusable Cargo data. Do not reintroduce those actions unless the
corresponding cache or image contract changes.

## Capacity and persistent cache

`values-8-cores.yaml` is normal operation: `minRunners=0`, `maxRunners=1`.
`values-paused.yaml` is the mandatory pause overlay: `minRunners=0`,
`maxRunners=0`. Both jobs use group `query-regression-persistent-cache-v1`,
`queue: max`, and `cancel-in-progress: false`; admitted runs queue rather than
replacing older pending runs. During maintenance, cancel admitted queued runs as
well as pausing ARC. Runner Pods have `activeDeadlineSeconds=12600`.
The runner requests 6 CPU and limits at 8 CPU to preserve `minipc-3`
allocatable-capacity scheduling headroom; do not reset the request to 8 CPU
without revalidating scheduling capacity.

The cache claim `query-regression-build-cache` is a nominal 600Gi `local-path`
PVC in `arc-runners`. It is `ReadWriteOnce`; `local-path` uses
WaitForFirstConsumer binding and Delete reclaim behavior, produces a
node-affine local PV, is non-expandable, and the 600Gi request is not a hard
storage quota. The runner's `minipc-3` selector is its only consumer candidate.

The initializer mounts the PVC root at `/cache`, creates and write-tests these
versioned subpaths as non-root UID/GID 1001, and the runner mounts them as:

| Persistent state | PVC subpath | Runner mount |
| --- | --- | --- |
| Ephemeral Cargo home | `emptyDir` | `/home/runner/.cargo` |
| Cargo registry data | `cargo-registry-v1` | `/home/runner/.cargo/registry` |
| Cargo Git data | `cargo-git-v1` | `/home/runner/.cargo/git` |
| Cargo target | `query-regression-target-v1` | `/home/runner/query-regression-target` |
| Cache metadata | `meta-v1` | `/home/runner/query-regression-cache-meta` |
| sccache local disk cache | `sccache-v1` | `/home/runner/.cache/sccache` |
| Immutable Rust toolchain | image-owned | `/opt/rustup`, `/opt/cargo/bin` |

The Pod security context uses UID/GID and `fsGroup` 1001 with
`fsGroupChangePolicy: OnRootMismatch`; no privileged `chown` or raw `hostPath`
is used. `CARGO_HOME` is a per-Pod `emptyDir`; only its nested `registry` and
`git` mounts are persistent. `RUSTUP_HOME=/opt/rustup` and `/opt/cargo/bin` are
image-owned immutable paths, while `CARGO_TARGET_DIR`, cache metadata, and
`SCCACHE_DIR` are persistent absolute paths. The runner sets
`RUSTC_WRAPPER=/usr/local/bin/sccache`,
`SCCACHE_DIR=/home/runner/.cache/sccache`, `SCCACHE_CACHE_SIZE=40G`, and
`CARGO_INCREMENTAL=0`. sccache uses its local PVC disk backend and self-evicts
at 40G; do not add runtime downloads, object storage, or a shared backend.

The repository's `.cargo/config.toml` remains a trusted per-revision build input.
In contrast, `$CARGO_HOME/config*`, credentials, installed bins, and Cargo
metadata outside the persistent `registry` and `git` data mounts are ephemeral
and cannot survive to another Pod.

The local disk backend has a one-server constraint. `maxRunners=1` and the
unchanged `query-regression-persistent-cache-v1` workflow concurrency group
serialize runs; do not increase runner capacity or relax that serialization
while this backend is in use. Base and candidate builds share the target; Cargo
fingerprints invalidate source and dependency changes. The workflow records the
sccache version and relevant environment in the target ABI marker, starts and
zeros sccache after cache and toolchain checks, shows initial/base/candidate
statistics, and resets statistics between base and candidate builds.

### Disk preflight and cleanup contract

Before applying or unpausing, verify the backing filesystem on `minipc-3` has
at least 900GiB free. The current local-path provisioner source is
`/opt/local-path-provisioner`; measure the filesystem containing it:

```bash
df -PB1G /opt/local-path-provisioner
```

The workflow reports `du`, `df -P`, human-readable free space, and inode
availability before builds and in an always-run report. Its cleanup is narrow
and non-destructive:

- warn at target size 400GiB; at 450GiB clear only the complete target root;
- warn at Cargo registry-plus-Git data size 60GiB; at 80GiB remove only
  `registry/src` and `git/checkouts`, then abort if that persistent data remains
  at least 80GiB;
- below 300GiB backing free space, clear the complete target root first,
  remeasure, then remove only those Cargo extracted trees and checkouts; abort
  if free space is still below 300GiB;
- never automatically remove Cargo registry cache/index, Git database, the
  image-owned Cargo bin or Rustup toolchain, cache metadata, the self-evicting
  sccache directory, or the PVC.

The target clear uses fixed absolute roots and removes all entries, including
dotfiles. After migration validation, remove obsolete `cargo-home-v1` and
`rustup-home-v1` only in explicit maintenance while ARC is 0/0 and no runner Pod
exists; they are not mounted by the current configuration.

## Deploy and pause safely

First verify the configured external network route and the disk preflight. Apply the PVC; while
the scale set is paused, it is expected to remain `Pending` because
WaitForFirstConsumer has no scheduled runner:

```bash
kubectl apply --dry-run=server \
  -f .github/runner-scale-sets/query-regression/cache-pvc.yaml
kubectl apply -f .github/runner-scale-sets/query-regression/cache-pvc.yaml
```

Render normal and paused configurations. Normal values are always first; the
pause overlay is always last:

```bash
helm template perf-regression-8-cores \
  oci://ghcr.io/actions/actions-runner-controller-charts/gha-runner-scale-set \
  --namespace arc-runners --version 0.14.2 \
  --set controllerServiceAccount.name=arc-gha-rs-controller \
  --set controllerServiceAccount.namespace=arc-systems \
  -f .github/runner-scale-sets/query-regression/values-8-cores.yaml

helm template perf-regression-8-cores \
  oci://ghcr.io/actions/actions-runner-controller-charts/gha-runner-scale-set \
  --namespace arc-runners --version 0.14.2 \
  --set controllerServiceAccount.name=arc-gha-rs-controller \
  --set controllerServiceAccount.namespace=arc-systems \
  -f .github/runner-scale-sets/query-regression/values-8-cores.yaml \
  -f .github/runner-scale-sets/query-regression/values-paused.yaml
```

The **first post-merge Helm deployment must reconcile the release in paused
mode**. Keep the pause overlay last:

```bash
# First post-merge deployment and every return to paused mode: 0/0.
helm upgrade --install perf-regression-8-cores \
  oci://ghcr.io/actions/actions-runner-controller-charts/gha-runner-scale-set \
  --namespace arc-runners --create-namespace --version 0.14.2 \
  --reset-values --wait \
  -f .github/runner-scale-sets/query-regression/values-8-cores.yaml \
  -f .github/runner-scale-sets/query-regression/values-paused.yaml

# Expect 0/0 and no runner resources before considering normal mode.
kubectl -n arc-runners get autoscalingrunnerset perf-regression-8-cores \
  -o jsonpath='{.spec.minRunners}{"/"}{.spec.maxRunners}{"\n"}'
kubectl -n arc-runners get ephemeralrunners,pods \
  -l actions.github.com/scale-set-name=perf-regression-8-cores
```

Only after that verification and separate explicit approval, apply normal 0/1
operation without the pause overlay:

```bash
helm upgrade --install perf-regression-8-cores \
  oci://ghcr.io/actions/actions-runner-controller-charts/gha-runner-scale-set \
  --namespace arc-runners --create-namespace --version 0.14.2 \
  --reset-values --wait \
  -f .github/runner-scale-sets/query-regression/values-8-cores.yaml
```

Do not use bare `helm rollback`, `--atomic`, or `--reuse-values`: a stored
revision can restore nonzero runner capacity. Inspect rendered manifests for
capacity, the `minipc-3` selector, cache claim and mounts, initializer,
security context, and resources. After approved normal mode receives its first
canary, the PVC binds to `minipc-3`.

For local-PV node loss, cache recovery is intentionally disposable: return to
0/0, recreate the PVC on a healthy node, cold-fill it, and run a new canary.

## Canary and rollback

With explicit approval, run two identical `workflow_dispatch` canaries on
`perf-regression-8-cores`, using immutable full base and candidate commit SHAs
and `cargo_profile=nightly`. The first is the cold fill; the second verifies warm
reuse. Record the workflow's base/candidate build elapsed logs and cache
ABI-marker output, initial/base/candidate sccache statistics, and cache report.
Confirm the image tool contract (root-owned Rustup/Cargo paths, baked nightly,
and non-writable `/opt` roots) and that the ephemeral Cargo home contains only
the mounted registry/Git data before Cargo creates per-Pod state.
Obtain dependency and tool network byte counters from the configured
environment counter source, filtered to `minipc-3` and the dependency/tool
destinations.

Accept the canary only when all of the following hold:

- exactly one runner Pod runs on `minipc-3`, and both jobs use the same bound PV;
- UID/GID 1001 cache mounts are writable; the warm run does not invalidate the
  target or bulk-redownload crates or toolchains; sccache reports separate base
  and candidate build statistics without server or cache-path errors; immutable
  Rustup/Cargo roots remain non-writable and only registry/Git data persists;
- warm base build time is at most 50% of cold base build time;
- warm dependency/tool network bytes are at most 10% of cold fill bytes;
- cache sizes remain below soft watermarks, node free space remains at least
  300GiB, and the benchmark is correct without TLS EOFs or timeouts;
- the configured environment counter source confirms this traffic is outside VPN
  accounting.

Immediately return to 0/0 after either canary unless ongoing normal operation
has been explicitly approved; return immediately on any traffic, cache, disk,
TLS, or correctness failure. To roll back, use the paused Helm upgrade above,
or another explicit `helm upgrade --install` with known-good values followed by
`values-paused.yaml`, `--reset-values`, and `--wait`. Do not delete the PVC
automatically; preserve it for diagnosis unless intentionally discarding cache.

## ACK benchmark Job (cloud execution)

The benchmark runs in the Alibaba Cloud Hangzhou ACK cluster
`bulk-ingestion-test` (`c72c097b8b2bf4fd9946d31e9d41f632f`) as a one-shot
`batch/v1` `Job` created by the trusted controller. The Job pod runs on the
dedicated bulk nodepool `bulk-ingestion-pool`
(`npb5ff93bea3a447a698fe31ebc997ea31`). **The cloud does no compilation and
has no ARC controller or listener.**

### Cloud prerequisites

All of the following are Alibaba Cloud side configuration and must be completed
**before** the first controller run:

1. **Node instance specs**: use enterprise dedicated instance types (c7/g7
   series, e.g. `ecs.c7.2xlarge`). Do **not** use economy (`e`), burstable
   (`t`), or shared (`s`) types: their non-dedicated CPU scheduling contends
   with other workloads and has no performance SLA, which pollutes perf data.
2. **Elastic scaling and scheduling**: enable elastic scaling on the nodepool
   (Cluster Autoscaler, min 0 / max as needed), taint it with
   `dedicated=perf-regression:NoSchedule`, and apply the corresponding label.
   The Job tolerates that taint and pins to the nodepool via
   `alibabacloud.com/nodepool-id`. Scaling is implicit: the Pending Job pod
   scales the nodepool up; deleting the Job/pod scales it back down. **Never
   call nodepool desired-size/scale APIs from the controller.**
3. **Egress**: the local runner must reach the ACK API server; the cluster
   must be able to pull the runner image from ACR.
4. **Job namespace**: the controller creates Jobs in namespace `arc-runners`
   by default (`ACK_JOB_NAMESPACE` / `--namespace` to override). The namespace
   must exist and must be able to pull the digest-pinned image (add a
   read-only `imagePullSecrets` if the registry is private). The namespace may
   be reused from the retired cloud ARC deployment; the ARC Helm releases
   themselves must be removed per the goal state.

### Controller prerequisites (local runner)

The controller job runs on `perf-regression-8-cores` and needs:

- `kubectl` installed on the runner (it is not part of the runner image).
- A kubeconfig for the ACK cluster, provided either as the `ACK_KUBECONFIG`
  repository secret (written to a temp file by the workflow step) or as a
  `KUBECONFIG` environment variable/path available on the runner. The
  kubeconfig never leaves the controller job and is never placed in the Job
  manifest or pod.
- Explicit RBAC for the kubeconfig identity in the ACK cluster, scoped to the
  Job namespace (v1 uses `kubectl cp`/`exec`, so `pods/exec` is required):

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: query-regression-controller
  namespace: arc-runners
rules:
  - apiGroups: ["batch"]
    resources: ["jobs"]
    verbs: ["create", "get", "list", "watch", "delete"]
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list", "watch"]
  - apiGroups: [""]
    resources: ["pods/exec"]   # kubectl cp + exec-gated markers (v1)
    verbs: ["create"]
  - apiGroups: [""]
    resources: ["pods/log"]
    verbs: ["get"]
  - apiGroups: [""]
    resources: ["events"]
    verbs: ["get", "list"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: query-regression-controller
  namespace: arc-runners
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: query-regression-controller
subjects:
  - kind: User            # or ServiceAccount/Group matching the kubeconfig identity
    name: <kubeconfig-identity>
    apiGroup: rbac.authorization.k8s.io
```

No nodepool, autoscaling, secret, or cluster-wide permissions are granted. Do
not broaden these permissions.

### Job manifest contract

The trusted controller renders the Job manifest (see
`query-regression-ack-controller.py`, `--dry-run` renders it without touching a
cluster) with a fixed security contract:

- **Name/labels**: deterministic `query-regression-<run-id>-<run-attempt>` with
  `app=query-regression`, `run-id`, `run-attempt` labels; base/candidate SHAs
  recorded as annotations. Deletion is exact-name validated against this
  pattern.
- **Image**: digest-pinned existing runtime image (`@sha256:...`), enforced by
  the controller; `imagePullPolicy: IfNotPresent`. No ACR push happens in this
  change.
- **Scheduling**: `nodeSelector` `alibabacloud.com/nodepool-id=
  npb5ff93bea3a447a698fe31ebc997ea31` and toleration
  `dedicated=perf-regression:NoSchedule`.
- **Pod hardening**: `automountServiceAccountToken: false`, non-root
  UID/GID 1001, seccomp `RuntimeDefault`, `allowPrivilegeEscalation: false`,
  all capabilities dropped, `restartPolicy: Never`, `backoffLimit: 0`,
  `activeDeadlineSeconds: 10800`, bounded resources (requests 4 CPU / 12Gi /
  20Gi ephemeral; limits 8 CPU / 16Gi / 40Gi ephemeral — the nodepool nodes
  are 8C16G).
- **No credentials**: the Job contains no GitHub token, cloud credential,
  kubeconfig, or service-account token; the pod needs no egress after startup.
  Payload is delivered with `kubectl cp` and gated by a `/payload/.ready`
  marker written via `kubectl exec` once the transfer is verified.
- **Perf only**: the pod entrypoint runs the performance driver and its
  required helpers/processes; no tooling/unit tests run on ACK.

### Trust boundaries

- **Trusted scripts are restored from the verified base commit, never from
  the build artifact.** The controller job checks out the verified base SHA
  (`needs.build.outputs.verified-base-sha`) and restores
  `query-regression-ack-controller.py`, `query-regression-summary.py`, and
  `query-regression-pr-metadata.py` via `git show` into a fresh
  `$RUNNER_TEMP/query-regression-trusted-scripts` directory, recording
  `trusted-scripts-manifest.json` (source SHA + per-file sha256). The
  controller step executes only that restored copy, and the controller
  verifies the summary script it embeds into the payload against the
  manifest. Candidate compilation therefore cannot replace any code that runs
  in the credentialed controller job.
- **The base binary is immutable before candidate code runs.** The build job
  uploads `query-regression-base-binaries` (with `base-manifest.json`) in a
  step that precedes "Switch source to candidate"; candidate build scripts
  cannot overwrite it after it is in Actions. Candidate binaries/helpers ship
  in a separate `query-regression-candidate-binaries` artifact
  (`candidate-manifest.json`). The controller job downloads both by exact
  artifact ID (`needs.build.outputs.{base,candidate}-artifact-id`) and
  cross-validates: base/candidate manifest SHAs must equal the build job's
  verified SHAs, and the candidate checkout HEAD must equal the verified
  candidate SHA. The verified SHAs themselves come from real build job
  outputs — never re-derived from the PR head.
- **Candidate payload paths are symlink-free and contained.** Before copying,
  the controller rejects every symlink in any path component from the
  candidate checkout root down (ancestor symlinks such as
  `candidate/tests -> outside` included) and every non-regular file
  (FIFO/socket/device) under the candidate `tests/perf` tree, rejects a
  symlinked driver script, and enforces that the resolved `tests/perf` and
  driver paths stay under the resolved candidate checkout root. A candidate
  symlink is never dereferenced.
- **ACK runs performance regression only.** The pod entrypoint invokes the
  perf driver and its required helpers/processes; no tooling/unit tests run
  on ACK. The four query regression tooling tests run in the build job
  (non-ACK, non-controller context — the same trust context as compiling the
  candidate).

### Controller flow and cleanup

1. Restore trusted scripts from the verified base commit (see above) and
   verify the manifests/attestation/checkout SHAs.
2. Reconcile: if a Job with the exact deterministic name exists (leaked from a
   killed controller), delete it (foreground, bounded retries), then create
   the Job. The name embeds `run-id`/`run-attempt`, so re-runs get a fresh
   name.
3. Wait for the pod to be Running (bounded, default 25 min) — the Pending pod
   is the ACK autoscaling scale-up signal.
4. `kubectl cp` the payload (`bins/`, `repo/tests/perf` cases + driver,
   trusted summary, generated `run.sh`), exec-verify the files, re-verify the
   binaries' sha256 inside the pod against the base/candidate manifests, then
   arm `/payload/.ready`.
5. The pod runs the performance driver, writes `query-regression-work/**` and
   `query-regression-summary.md` under `/work`, then writes
   `/work/benchmark-status` + `/work/.done` and stays alive for a bounded
   collection window (600 s).
6. The controller reads the status, `kubectl cp`s the results back (mandatory:
   missing/invalid reports or summary fail the run even if the benchmark
   status was 0) and best-effort `kubectl logs`/`describe`, then touches
   `/work/.collected` and deletes the exact Job with foreground cascade.
7. Deletion uses bounded retries (default 3 attempts x 240 s) and verifies
   the Job's absence; if deletion cannot be confirmed the workflow fails even
   when the benchmark status was 0. On success and ordinary failure the
   controller's `finally` path performs the deletion; on graceful cancellation
   (SIGTERM/SIGINT) it skips optional diagnostics/collection and immediately
   performs a short bounded exact Job deletion/absence check. Cleanup is
   **best effort** under GitHub hard cancellation (see below).

Timeout budget (one global monotonic lifecycle deadline): the controller job
runs under a 180 min workflow timeout (10800 s). The controller lifecycle is
150 min (9000 s) with a 15 min internal cleanup reserve, leaving a 30 min
explicit margin (minimum documented 25 min) above the lifecycle for
checkout, trusted-script restore, artifact download/attestation, kubeconfig
setup, and the second-process cleanup-only step (<= 10 min). Normal phases —
pod-ready 25 min, payload transfer + marker <= 10 min, benchmark + mandatory
collection 80 min — clamp to `cleanup_begins` (the lifecycle deadline minus
the reserve), so no ordinary path can consume the reserve; every
kubectl/subprocess call and every poll/sleep is clamped to the remaining
phase budget. A normal-phase operation whose budget is exhausted raises
instead of degrading to a 1-second cluster call: before any preflight job
lookup, leaked-job reconcile deletion, or Job creation the controller asserts
a positive normal-phase budget, so no Job is ever created or reconciled after
`cleanup_begins`. Deletion is the only operation allowed inside the reserve
and clamps to the hard deadline. The pod's own `activeDeadlineSeconds` (3 h)
is reached only if the controller dies.

Second-process cleanup: an `if: always()` step runs the same trusted
controller script with `--cleanup-only` after the controller step. It uses the
deterministic `query-regression-<run-id>-<run-attempt>` name (identical
exact-name validation) and deletes only that exact Job, so a controller step
that failed or was terminated without running its `finally` still gets a
second cleanup process. Under GitHub hard cancellation (run cancelled /
runner SIGKILL) no further steps or jobs execute; see "Leaked Job recovery".

Artifacts and comments are unchanged: `query-regression-report` and
`query-regression-comment` artifacts (including `query-regression-pr.json`
consumed by the trusted comment workflow) are uploaded from the controller job.

### Leaked Job recovery

Cleanup is **best effort** under GitHub hard cancellation: when a run is
cancelled or the runner is SIGKILLed, the controller's signal handler and any
further steps/jobs do not run, so the Job may outlive the run. The concurrency
group prevents overlapping runs, but a leaked Job/pod would keep the nodepool
scaled. Recovery is deterministic because the Job name is exact:

```bash
kubectl -n arc-runners delete job query-regression-<run-id>-<run-attempt> \
  --cascade=foreground --wait=true
```

The name is visible in the run's step log (`created job ...`) and on the pod
via `kubectl get pods -l app=query-regression`. Deleting the pod/job is the
scale-down signal. The residual gap for full automation is an **independent
reaper** (a scheduled/triggered process outside the workflow, or manual
cleanup by an operator) that deletes any `query-regression-*` Job older than
the lifecycle budget; the deterministic name pattern is the reaper's exact
match key. Do not use a broad `kubectl delete job -l app=query-regression`
without the same exact-name validation in the reaper.

### Notes

- Perf runs are serialized by the same concurrency group as the local scale
  set, so benchmark Jobs never overlap on the nodepool.
- Base and candidate run on same-spec nodes because the `nodeSelector` pins
  the Job to the same nodepool.
- All ACK nodepool/quota changes are write operations and require approval by
  the responsible owner before execution. This document only records the
  ready-state configuration.

## Future hardening (not in v1)

- Replace `kubectl cp`/exec-gated markers with an init container that pulls a
  signed payload from object storage and a finalizer/sidecar that uploads
  results, dropping `pods/exec` from the RBAC.
- Sign the payload bundle and verify signatures in the pod before execution.
- Move the kubeconfig out of the runner into a short-lived token exchange.
- Replace the local ARC build runner with a trusted non-ARC build controller if
  ARC is ever decommissioned.

## Retired: cloud ARC scale set

The former ACK ARC scale set (`perf-regression-ack`,
`.github/runner-scale-sets/query-regression/values-cloud.yaml`) is **retired**
and the values file is removed. Do not reintroduce an ARC deployment in the
ACK cluster: the benchmark runs as a one-shot `Job` created by the trusted
controller, and the goal state has no ARC controller/listener in the cloud.
