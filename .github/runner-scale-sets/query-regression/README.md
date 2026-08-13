# Query regression runners and benchmark Job (secure architecture, v2)

The `Query Regression` performance gate is split into two workflows plus a
comment workflow:

1. **`query-regression.yml`** — the **unprivileged build** half. It runs on
   the trusted local ARC runner scale set `perf-regression-8-cores` (a
   dedicated `minipc-3` node with a persistent build cache) and has **no
   ACK/cloud secret references, no kubeconfig, no kubectl cluster calls, and
   no privileged controller job**.
2. **`query-regression-controller.yml`** — the **trusted default-branch
   follower**. It is triggered by `workflow_run` completion of the build
   workflow, runs on a **separate** runner label
   (`query-regression-ack-controller`, never the build runner), validates the
   originating run/artifacts/PR/SHA chain from GitHub API data, and only then
   uses a **runner-local exec-plugin kubeconfig** to create a one-shot ACK
   benchmark `Job` in the Alibaba Cloud ACK cluster.
3. **`query-regression-comment.yml`** — follows the controller run and posts
   the sticky PR comment. It is the only workflow with `pull-requests: write`
   (write isolation).

There is **no ARC scale set in the cloud**: the ACK cluster contains no
`gha-runner-scale-set` deployment, no runner listener, and does no
compilation. The benchmark `Job` is created directly by the controller through
the ACK API server.

## Architecture (v2)

```
local ARC runner                    controller runner (trusted, external)     ACK cluster (bulk-ingestion-test)
+--------------------------------+  +-------------------------------------+  +-------------------------------------+
| build job (UNPRIVILEGED)       |  | controller job (workflow_run of     |  | one-shot Job                        |
|  - resolves/verifies SHAs      |  |  "Query Regression", default branch)|  |  query-regression-<run-id>-<attempt> |
|  - compiles base+candidate     |  |  - runs on query-regression-ack-    |  |  - namespace query-regression-perf   |
|  - uploads BASE artifact FIRST |  |    controller (NOT the build label) |  |  - digest-pinned image (fixed)       |
|  - uploads candidate artifact  |  |  - admission gate: validates run/   |  |  - nodepool-id selector + taint      |
|    (bins + tests/perf + driver)|  |    PR/SHA/artifact ids via GitHub   |  |  - tokenless SA, non-root, no creds  |
|  - uploads strict metadata     |  |    API (metadata artifact treated   |  |  - TTL 600s, podReplacementPolicy    |
|    artifact (context only)     |  |    as UNTRUSTED)                   |  |  - waits for /payload/.ready marker  |
|  - runs tooling tests (local)  |  |  - downloads artifacts by exact id |  |  - runs perf driver only             |
|  - NO ACK creds/kubectl        |  |  - validates manifests/attestation |  |  - writes reports + .done, stays up  |
+--------------------------------+  |  - validates runner-local kubeconfig|  |  - results pulled via kubectl cp     |
                                   |    (exec plugin; fail closed on any  |  |  - Job deleted (foreground, retried) |
                                   |    embedded credential; never prints)|  |  - nodepool autoscaling is implicit  |
                                   |  - creates Job ──────────────────────┼─▶                                     |
                                   |  - kubectl cp payload + exec marker  |  |                                     |
                                   |  - collects/validates, deletes Job  |  |                                     |
                                   |  - regenerates comment metadata     |  |                                     |
                                   +-------------------------------------+  +-------------------------------------+
```

Nodepool scaling is **implicit only**: the Pending Job pod is the scale-up
signal for ACK Cluster Autoscaler, and foreground deletion of the Job/pod is
the scale-down signal. The controller never calls any nodepool
desired-size/scale API.

All candidate binaries, scripts, and cases execute **only inside the ACK Job
pod**. The controller only copies them (`kubectl cp`) and never runs them.

## Trust flow (who trusts what)

* **The build runner and candidate content never see the ACK credential.**
  `query-regression.yml` has no `ACK_KUBECONFIG` secret, no kubeconfig, and
  no kubectl. It produces three artifacts: the base binaries (uploaded
  *before* any candidate-controlled build step), the candidate binaries plus
  the candidate `tests/perf` tree and driver (under `repo/...`, because the
  controller never checks out PR code), and a strict metadata artifact with
  only GitHub-context values and the verified build outputs.
* **The controller uses only default-branch trusted code.** `workflow_run`
  semantics run the controller workflow from the default branch; it checks
  out the default branch with `persist-credentials: false` and executes
  `query-regression-admission.cjs` and `query-regression-ack-controller.py`
  from that checkout. It never checks out or executes PR code.
* **Admission is API-verified, metadata is untrusted.** The admission script
  validates, from the GitHub API only: run id/attempt/repository/workflow
  name/event/conclusion/head SHA, PR membership (or the fork fallback for
  empty `pull_requests`), current PR state/head, label admission (the current
  PR must carry exactly one of `query-regression`/`heavy-regression` matching
  the metadata), the candidate merge SHA parent relationship (exactly one
  head parent + the built base SHA), and the metadata/base/candidate artifact
  ids. The metadata artifact is downloaded and every cross-checkable field is
  compared against the API; any disagreement fails closed before any ACK
  credential is used. Replay (a stale artifact from another run) is rejected
  by the run id/attempt pinning.
* **Manifests are re-verified against the validated values.** The controller
  downloads the base/candidate artifacts by the exact validated ids and
  re-checks `base-manifest.json`/`candidate-manifest.json` against the
  validated SHAs; the summary script embedded in the payload is verified
  against a manifest of the default-branch checkout.
* **The comment metadata is regenerated, not trusted.** The controller writes
  `query-regression-pr.json` from the validated admission values only (the
  controller run id/attempt are used because the comment workflow follows the
  controller run; the source build run is recorded separately).

## Fail-closed bootstrap for PR runs

`workflow_run` followers only exist once their workflow file is on the default
branch. Until `query-regression-controller.yml` (and the admission/controller
scripts) are merged, PR-label build runs complete and upload artifacts but no
ACK benchmark runs and no PR comment is posted — the gate is **fail-closed**,
never partial. The same holds for any future PR that tries to change the
controller code: the change only takes effect after a maintainer merges it.

## Prerequisites and trust admission

### Local ARC scale set (build)

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
`prom_remote_write_7913` remote-write case. `pull_request: labeled` is the only
PR trigger: the label event snapshots its merge, head, and base SHAs. A queued
job fetches that immutable event merge SHA directly, verifies it is a two-parent
merge whose parents include the snapshotted head exactly once, and uses its
other parent as the actual base build revision. The job never follows a newer
mutable PR merge ref. A later PR head change does not retarget an already
queued run. The trusted controller additionally requires the **current** PR to
still carry a regression label matching the run (labels are maintainer-only),
so removing the label after a run starts makes the controller fail closed
(re-add the label to admit the run; cancel unwanted runs).

Admission does not relax runner hardening or GitHub permissions. Keep
service-account token mounting disabled; do not mount host paths, the Docker
socket, kubeconfig, or long-lived credentials. The runner and cache initializer
use UID/GID 1001, disallow privilege escalation, drop all capabilities, and use
the RuntimeDefault seccomp profile. Keep GitHub tokens least-privilege and
review workflow changes before admission.

### Controller runner (trusted external identity)

The controller workflow runs on the **`query-regression-ack-controller`**
label — a separate trusted runner, **not** the build runner and **never** an
ephemeral runner inside the ACK cluster (the ACK cluster stays sterile). It
needs:

* `kubectl` — pinned to exactly **v1.34.2** (an ABI: `kubectl cp`/`exec`
  translate into CONNECT argv + stream-flag tuples that the exec
  ValidatingAdmissionPolicy allowlist matches exactly; the controller fails
  closed on any other client version) — and `python3` (with PyYAML
  recommended for structured kubeconfig validation) and `node` (for the
  admission script).
* A **runner-local kubeconfig** at a fixed path (default
  `/etc/query-regression/ack-kubeconfig`, overridable via the non-secret
  repository variable `ACK_KUBECONFIG_PATH`; a path is not a credential).
  There is **no `ACK_KUBECONFIG` GitHub secret** and no admin/static
  credential path anywhere.
* The kubeconfig's active user **must use an `exec` credential plugin** — an
  external broker that mints short-lived tokens (refresh every 10–15 min) for
  the trusted hardware identity of this runner. PyYAML is mandatory (the
  controller fails closed if it is unavailable); the kubeconfig must have an
  explicit `current-context` whose cluster and user resolve, ONLY the active
  user must provide exec auth (an inactive user's exec plugin never counts),
  and the exec block's `apiVersion`/`command`/`args`/`env` policy is
  validated. The controller fails closed if the kubeconfig embeds
  `token`/`tokenFile`, `client-key-data` / `client-certificate-data`,
  `username`/`password`, `auth-provider`, or static exec env secrets
  (secret-named or credential-looking values). The kubeconfig is **never
  printed**; errors name only the path and the offending key.

The ACK-side RBAC for that identity lives in
`.github/runner-scale-sets/query-regression/ack/rbac.yaml` (exact namespaced
Role/RoleBinding in `query-regression-perf`). The broker/hardware identity
assumption: the exec plugin proves the runner is the trusted controller host;
the plugin itself holds no long-lived material in the kubeconfig.

## Runner image and workflow tools (build)

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
benchmark `Job` image and is pinned in the trusted controller
(`DEFAULT_IMAGE` in `query-regression-ack-controller.py`; the Job image is
**fixed** — there is no image override). Update the digest references and bump
`RUNNER_IMAGE_EPOCH` at the same time. If the registry is private, use a
dedicated read-only pull secret only as `imagePullSecrets` (in the ACK
namespace); never expose registry credentials to the Job container itself.

The runner optionally imports only the non-sensitive `HTTP_PROXY`,
`HTTPS_PROXY`, and `NO_PROXY` variables from the
`query-regression-runner-local-env` ConfigMap. Credentials or secrets must
never be placed in a ConfigMap.

Before builds, the workflow asserts UID/GID 1001 and exact image tool versions:
`libprotoc 3.21.12`, `mold 2.30.0`, `sccache 0.16.0`, root-owned `rustup
1.29.0`, and the image-baked `nightly-2026-03-21` Rust toolchain. Rustup,
Cargo, and Rustc must resolve from `/opt/cargo/bin`; the runner cannot write
`/opt/rustup` or `/opt/cargo/bin`. The workflow sets its warning-denying mold
`RUSTFLAGS` directly, disables automatic Rustup installation, and performs no
runtime toolchain downloads.

## Capacity and persistent cache (build)

`values-8-cores.yaml` is normal operation: `minRunners=0`, `maxRunners=1`.
`values-paused.yaml` is the mandatory pause overlay: `minRunners=0`,
`maxRunners=0`. Both build runs use group `query-regression-persistent-cache-v1`,
`queue: max`, and `cancel-in-progress: false`; admitted runs queue rather than
replacing older pending runs. Runner Pods have `activeDeadlineSeconds=12600`.
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
The local disk backend has a one-server constraint. `maxRunners=1` and the
unchanged `query-regression-persistent-cache-v1` concurrency group serialize
build runs; do not increase runner capacity or relax that serialization while
this backend is in use. Base and candidate builds share the target; Cargo
fingerprints invalidate source and dependency changes. The controller workflow
uses its own global serialization group (`query-regression-ack-controller`)
because the ACK namespace ResourceQuota allows exactly one Job/Pod.

### Disk preflight and cleanup contract

Same as before: verify at least 900GiB free on the filesystem containing
`/opt/local-path-provisioner`; the workflow's cleanup is narrow and
non-destructive (warn/clear only the complete target root and Cargo
registry/src + git/checkouts at watermarks; never remove the registry cache
index, Git database, image-owned toolchains, cache metadata, sccache, or the
PVC).

## Deploy and pause safely (build scale set)

Render normal and paused configurations (normal values first, pause overlay
last) and reconcile in paused mode first; the first post-merge deployment must
be paused (0/0). See the commands in the retired v1 section below for the exact
`helm upgrade --install` invocations — only the runner scale set changes are
managed here; the ACK side is configured by the manifests under `ack/` (next
section) and the external controller runner/kubeconfig.

## ACK benchmark boundary (declarative manifests)

The declarative least-privilege boundary lives in
[`.github/runner-scale-sets/query-regression/ack/`](ack/README.md): Namespace
with PSA `restricted`, tokenless service accounts, the exact namespaced
Role/RoleBinding, ResourceQuota (max 1 Job/1 Pod with the exact resource
totals), LimitRange, deny-all NetworkPolicy, and **deployment-gated**
ValidatingAdmissionPolicies + bindings (workload conformance, direct-pods
denial, exec CONNECT) with an offline policy test
(`query-regression-policy-test.py`, run in `checks.yml`) and a server-side
`apply-test.sh`.

**These manifests are not deployed by this repository.** Deployment requires
explicit cluster-owner approval: run the offline policy test, run
`apply-test.sh` against the target cluster (server `--dry-run=server` compiles
the CEL and validates binding actions but does **not** exercise CONNECT;
`QR_APPLY_TEST_CANARY=1` additionally creates a real disposable canary Job in
`query-regression-perf` to validate the exact exec/cp argv+stream tuples and
deletes it — destructive to the canary only; without the flag the script exits
2), inspect each VAP's `status.typeChecking.expressionWarnings`, then apply
the boundary (enforcement bindings are exactly `["Deny","Audit"]`; the
optional `["Warn","Audit"]` rollout bindings are a separate, mutually
exclusive file) and set the RoleBinding subject in `rbac.yaml` to the
kubeconfig exec identity.

Threat model: if the controller credential is stolen, the server-side boundary
caps it to **one conforming Job/Pod in an otherwise sterile namespace** with no
cluster or other-namespace access. The exec CONNECT policy sees the
`PodExecOptions` object (target via `request.namespace`/`request.name`,
options via `object.container`/`command`/`tty`/`stdin`/`stdout`/`stderr`) —
**not** the parent Pod's metadata — and allows exactly the canonical
controller exec and pinned-kubectl cp argv+stream tuples (kubectl v1.34.2
ABI). Residual capability (intentional): the allowlist pins argv and the
`/payload` destination, not the payload bytes, so a stolen credential can
still upload arbitrary payload content and run arbitrary code **inside the one
sandbox pod** (non-root, tokenless, emptyDir-only, network denied). Hard gates
before the boundary is trusted: the real server-side canary, NetworkPolicy/CNI
enforcement (manifest alone is not enforcement), and the kubectl ABI pin.

### Cloud prerequisites

All of the following are Alibaba Cloud side configuration and must be completed
**before** the first controller run:

1. **Node instance specs**: use enterprise dedicated instance types (c7/g7
   series, e.g. `ecs.c7.2xlarge`). Do **not** use economy (`e`), burstable
   (`t`), or shared (`s`) types.
2. **Elastic scaling and scheduling**: enable elastic scaling on the nodepool
   (Cluster Autoscaler, min 0 / max as needed), taint it with
   `dedicated=perf-regression:NoSchedule`, and apply the corresponding label.
   The Job tolerates that taint and pins to the nodepool via
   `alibabacloud.com/nodepool-id`. Scaling is implicit. **Never call nodepool
   desired-size/scale APIs from the controller.**
3. **Egress**: the controller runner must reach the ACK API server; the
   cluster must be able to pull the runner image from ACR.
4. **Job namespace**: `query-regression-perf` (fixed — the controller has no
   namespace override). Apply the `ack/` manifests, ensure the namespace can
   pull the digest-pinned image (read-only `imagePullSecrets` only if the
   registry is private), and bind `rbac.yaml` to the exec identity.

### Controller prerequisites (local runner)

See "Controller runner" above: `kubectl`, `python3`, `node`, the runner-local
exec-plugin kubeconfig (never a secret), and the `query-regression-ack-controller`
runner label. Explicit RBAC for the kubeconfig identity in the ACK cluster is
in `ack/rbac.yaml` (scoped to the Job namespace; v1 uses `kubectl cp`/`exec`,
so `pods/exec` is required). No nodepool, autoscaling, secret, or cluster-wide
permissions are granted. Do not broaden these permissions.

## Job manifest contract

The trusted controller renders the Job manifest (see
`query-regression-ack-controller.py`, `--dry-run` renders it without touching a
cluster) with a fixed security contract:

- **Namespace/image are fixed**: `query-regression-perf`, digest-pinned image
  (`@sha256:...`); there is no override.
- **Name/labels**: deterministic `query-regression-<source-run-id>-<source-run-attempt>`
  with `app=query-regression`, `run-id`, `run-attempt` labels; base/candidate
  SHAs recorded as annotations. Deletion is exact-name validated against this
  pattern.
- **Job lifecycle**: `backoffLimit: 0`, `completions: 1`, `parallelism: 1`,
  `activeDeadlineSeconds: 10800`, **`ttlSecondsAfterFinished: 600`** (server-
  side leak bound), **`podReplacementPolicy: Failed`**.
- **Scheduling**: `nodeSelector` `alibabacloud.com/nodepool-id=
  npb5ff93bea3a447a698fe31ebc997ea31` and toleration
  `dedicated=perf-regression:NoSchedule`.
- **Pod hardening**: `automountServiceAccountToken: false`, the tokenless
  **`serviceAccountName: query-regression-workload`**, non-root UID/GID 1001,
  seccomp `RuntimeDefault`, `allowPrivilegeEscalation: false`, all capabilities
  dropped, `restartPolicy: Never`, bounded resources (requests 4 CPU / 12Gi /
  20Gi ephemeral; limits 8 CPU / 16Gi / 40Gi ephemeral).
- **No credentials**: the Job contains no GitHub token, cloud credential,
  kubeconfig, or service-account token; the pod needs no egress after startup
  (deny-all NetworkPolicy). Payload is delivered with `kubectl cp` and gated by
  a `/payload/.ready` marker written via `kubectl exec` once the transfer is
  verified.
- **Payload byte cap**: the controller measures the payload and refuses to
  create the Job above a conservative pre-Job cap (default 2 GiB, well under
  the 40Gi ephemeral limit); an over-cap payload fails the run before any
  cluster call.
- **Perf only**: the pod entrypoint runs the performance driver and its
  required helpers/processes; no tooling/unit tests run on ACK (the four query
  regression tooling tests run in the build job).

### Trust boundaries

- **Trusted scripts come from the default-branch checkout, never from the
  build artifact.** The controller workflow checks out the default branch and
  executes `query-regression-admission.cjs` / `query-regression-ack-controller.py`
  from it. The controller verifies the summary script it embeds into the
  payload against a manifest of that checkout.
- **The base binary is immutable before candidate code runs.** The build job
  uploads `query-regression-base-binaries` (with `base-manifest.json`) before
  "Switch source to candidate". Candidate binaries/helpers, the candidate
  `tests/perf` tree, and the candidate driver ship in a separate
  `query-regression-candidate-binaries` artifact (`candidate-manifest.json`).
- **Admission is API-verified and metadata is untrusted** (see "Trust flow").
- **Candidate payload paths are symlink-free and contained.** Before copying,
  the controller rejects every symlink in any path component from the
  candidate artifact root down (ancestor symlinks included) and every
  non-regular file (FIFO/socket/device) under the candidate `tests/perf`
  tree, rejects a symlinked driver script, and enforces containment under the
  candidate artifact root. A candidate symlink is never dereferenced.
- **ACK runs performance regression only** (see "Job manifest contract").

## Controller flow and cleanup

1. Admit the originating run/artifacts/PR/SHA chain (fail closed; see the
   admission script), download the base/candidate artifacts by the exact
   validated ids, and re-verify the manifests/attestation against the
   validated SHAs.
2. Validate the runner-local exec-plugin kubeconfig (fail closed on any
   embedded credential; never printed).
3. Reconcile: if a Job with the exact deterministic name exists (leaked from a
   killed controller), delete it (foreground, bounded retries), then create
   the Job after measuring the payload size.
4. Wait for the pod to be Running (bounded, default 25 min) — the Pending pod
   is the ACK autoscaling scale-up signal.
5. `kubectl cp` the payload (`bins/`, `repo/tests/perf` cases + driver from the
   candidate artifact, trusted summary, generated `run.sh`), exec-verify the
   files, re-verify the binaries' sha256 inside the pod against the
   base/candidate manifests, then arm `/payload/.ready`.
6. The pod runs the performance driver, writes `query-regression-work/**` and
   `query-regression-summary.md` under `/work`, then writes
   `/work/benchmark-status` + `/work/.done` and stays alive for a bounded
   collection window (600 s).
7. The controller reads the status, `kubectl cp`s the results back (mandatory:
   missing/invalid reports or summary fail the run even if the benchmark
   status was 0) and best-effort `kubectl logs`/`describe`, then touches
   `/work/.collected` and deletes the exact Job with foreground cascade.
8. Deletion uses bounded retries (default 3 attempts x 240 s) and verifies the
   Job's absence; if deletion cannot be confirmed the workflow fails even when
   the benchmark status was 0. On success and ordinary failure the controller's
   `finally` path performs the deletion; on graceful cancellation
   (SIGTERM/SIGINT) it skips optional diagnostics/collection and immediately
   performs a short bounded exact Job deletion/absence check.
9. The controller regenerates `query-regression-pr.json` from the validated
   admission values (PR events only) and the workflow uploads the
   `query-regression-report` and `query-regression-comment` artifacts; the
   comment workflow follows the controller run and posts the sticky comment
   with `pull-requests: write`.

Timeout budget (one global monotonic lifecycle deadline): the controller job
runs under a 180 min workflow timeout (10800 s). The controller lifecycle is
150 min (9000 s) with a 15 min internal cleanup reserve, leaving a 30 min
explicit margin (minimum documented 25 min) above the lifecycle for checkout,
admission, artifact download/attestation, kubeconfig setup, and the
second-process cleanup-only step (<= 10 min). Normal phases — pod-ready
25 min, payload transfer + marker <= 10 min, benchmark + mandatory collection
80 min — clamp to `cleanup_begins`, so no ordinary path can consume the
reserve; every kubectl/subprocess call and every poll/sleep is clamped to the
remaining phase budget. A normal-phase operation whose budget is exhausted
raises instead of degrading to a 1-second cluster call: no Job is ever created
or reconciled after `cleanup_begins`. Deletion is the only operation allowed
inside the reserve and clamps to the hard deadline. The pod's own
`activeDeadlineSeconds` (3 h) is reached only if the controller dies, and
`ttlSecondsAfterFinished: 600` cleans the Job up server-side shortly after any
completion.

Second-process cleanup: an `if: always()` step runs the same trusted
controller script with `--cleanup-only` after the controller step. It uses the
deterministic `query-regression-<run-id>-<attempt>` name (identical exact-name
validation) and deletes only that exact Job, so a controller step that failed
or was terminated without running its `finally` still gets a second cleanup
process. Under GitHub hard cancellation (run cancelled / runner SIGKILL) no
further steps or jobs execute; see "Leaked Job recovery".

## Leaked Job recovery

Cleanup is **best effort** under GitHub hard cancellation: when a run is
cancelled or the runner is SIGKILLed, the controller's signal handler and any
further steps/jobs do not run, so the Job may outlive the run. Three layers
bound the residual risk:

1. `ttlSecondsAfterFinished: 600` deletes the Job (and its pod) server-side
   shortly after the pod finishes, even if the controller is gone.
2. The concurrency group prevents overlapping runs, but a leaked Job/pod would
   keep the nodepool scaled; recovery is deterministic because the Job name is
   exact:

   ```bash
   kubectl -n query-regression-perf delete job query-regression-<run-id>-<attempt> \
     --cascade=foreground --wait=true
   ```

   The name is visible in the controller run's step log and on the pod via
   `kubectl -n query-regression-perf get pods -l app=query-regression`.
3. The residual gap for full automation is an **independent reaper** (a
   scheduled/triggered process outside the workflow, or manual cleanup by an
   operator) that deletes any `query-regression-*` Job older than the
   lifecycle budget; the deterministic name pattern is the reaper's exact
   match key. Do not use a broad `kubectl delete job -l app=query-regression`
   without the same exact-name validation in the reaper.

## Release (workflow_call) gating

The release perf gate is **synchronous**: `release.yml` calls the unprivileged
build workflow (`query-regression.yml`, `runner: perf-regression-8-cores`,
immutable full candidate SHA) and then immediately calls the trusted
controller workflow (`query-regression-controller.yml`) with the build's
EXACT verified outputs as typed inputs: run id/attempt, verified base and
candidate SHAs, and the base/candidate artifact ids (exposed as
`on.workflow_call.outputs` of the build job). The controller's admission gate
validates the caller from the github context (caller workflow must be
`Release`, triggering event one of `push`/`schedule`/`workflow_dispatch`,
caller ref a release tag or the default branch, caller SHA equal to the build
run's head SHA) and cross-checks every typed input against the values
validated from the build run via the GitHub API. Downstream release jobs
(`release-images-to-dockerhub`, `publish-github-release`) wait on the
controller job's result, never on the build alone — a silent build-only gate
is impossible (enforced by `query-regression-policy-test.py`).

For a release build the GitHub API reports the caller's own event (`push` for
tag releases, `schedule` for nightly, `workflow_dispatch` for manual
releases), and the metadata artifact records the same event; the controller
admits those events without PR checks — the release trigger itself is the
maintainer admission. Reusable-call runs are NOT followed via `workflow_run`
(reusable runs do not emit workflow_run events); the explicit `workflow_call`
is the only release controller path, and the controller job skips any
`workflow_run` event whose run event is `push`/`schedule` (release-only build
events). If a release run's metadata ever disagrees with the API (e.g.
`head_sha` propagation changes), the controller fails closed: the build
artifacts are still produced, but no ACK benchmark runs and the release gate
blocks — the failure is loud in the controller run, never silent. If this
ever happens, the release integration must be repaired before the next
release; do not weaken the admission checks.

## Notes

- Perf runs are serialized: the build concurrency group serializes builds, and
  the controller's global group (plus the ResourceQuota cap of one Job/Pod)
  serializes benchmark Jobs.
- Base and candidate run on same-spec nodes because the `nodeSelector` pins
  the Job to the same nodepool.
- All ACK nodepool/quota/policy changes are write operations and require
  approval by the responsible owner before execution. This document only
  records the ready-state configuration.
- `query-regression-pr-metadata.py` is retired (the controller regenerates the
  comment metadata from validated values); the file is retained only for
  reference.

## Future hardening (not in v2)

- Replace `kubectl cp`/exec-gated markers with an init container that pulls a
  signed payload from object storage and a finalizer/sidecar that uploads
  results, dropping `pods/exec` from the RBAC.
- Server-side exec command allowlisting via a validating webhook: the
  deployment-gated exec VAP already matches the PodExecOptions argv and
  stream-flag tuples exactly (the options object IS exposed to admission
  policies), so this item is now about going beyond exact-tuple matching
  (e.g. policy-as-code that compiles the allowlist from the controller at
  deploy time instead of at review time).
- Sign the payload bundle and verify signatures in the pod before execution.
- Move the controller runner onto an even shorter-lived token exchange or a
  trusted non-ARC controller if the local ARC build runner is ever
  decommissioned.

## Retired: cloud ARC scale set

The former ACK ARC scale set (`perf-regression-ack`,
`.github/runner-scale-sets/query-regression/values-cloud.yaml`) is **retired**
and the values file is removed. Do not reintroduce an ARC deployment in the
ACK cluster: the benchmark runs as a one-shot `Job` created by the trusted
controller, and the goal state has no ARC controller/listener in the cloud.
