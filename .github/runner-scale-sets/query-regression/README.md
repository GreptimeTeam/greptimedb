# Query regression self-hosted runners

The `Query Regression` workflow uses the dedicated ARC runner scale set
`perf-regression-8-cores`. ARC runner Pods run in the target Kubernetes cluster
and connect outbound to GitHub. The live scale set is currently **paused**:
`minRunners=0`, `maxRunners=0`, and no runner Pods. Do not resume it without
explicit approval.

## Prerequisites and trust admission

Install the ARC scale set controller if it is not already installed:

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

A maintainer applying the `query-regression` label is **trust admission for
that exact PR revision**. The admitted job may use this scale set's dedicated,
writable persistent cache. Before running a changed PR head, review the new
head, remove the label, and reapply it. Adding the label is the pull-request
trigger; an existing label does not automatically rerun the benchmark.

Admission does not relax runner hardening or GitHub permissions. Keep
service-account token mounting disabled; do not mount host paths, the Docker
socket, kubeconfig, or long-lived credentials. The runner and cache initializer
use UID/GID 1001, disallow privilege escalation, drop all capabilities, and use
the RuntimeDefault seccomp profile. Keep GitHub tokens least-privilege and
review workflow changes before admission. Where the CNI supports it, restrict
egress to required GitHub Actions, artifact/cache, Rust/crate/toolchain, DNS,
and image-registry endpoints; block unrelated cluster services, private ranges,
and metadata endpoints unless a case requires them.

### Office routing prerequisite

Direct split routing through `.2` is an **external office-network prerequisite**.
The gateway at `192.168.50.2` must route GitHub Actions, GitHub content,
artifact/cache, crates.io, Rust toolchain, and image-registry traffic directly
rather than through the VPN. Neither this repository nor Kubernetes configures
that route. Verify it with the responsible network operator before any canary.

## Runner image and workflow tools

Build and push the derived runner image; it preserves the official
`/home/runner/run.sh` entrypoint and supplies CI tools needed at runtime:

```bash
docker build \
  -f .github/runner-scale-sets/query-regression/Dockerfile \
  -t greptime-registry.cn-hangzhou.cr.aliyuncs.com/greptime/greptimedb-query-regression-runner:latest \
  .github/runner-scale-sets/query-regression

docker push greptime-registry.cn-hangzhou.cr.aliyuncs.com/greptime/greptimedb-query-regression-runner:latest
```

Deploy by digest, not mutable tag, by updating `values-8-cores.yaml` after a
rebuild. If the registry is private, use a dedicated read-only pull secret only
as `imagePullSecrets`; never expose registry credentials to runner containers.

Before Rust setup, the workflow asserts UID/GID 1001 and exact image tool
versions: `libprotoc 3.21.12`, `uv 0.11.26`, `mold 2.30.0`, and `Python 3.12.3`.
Rustup is intentionally absent from the base image. The workflow adds
`${CARGO_HOME}/bin` to `GITHUB_PATH`, then keeps
`actions-rust-lang/setup-rust-toolchain@v1` with `rust-src-dir: src`,
`cache: false`, and mold warning-denying rustflags. It logs Rustup, Rustc, and
Cargo versions after setup.

The workflow no longer uses GitHub `rust-cache`, `setup-protoc`, or `setup-uv`:
the PVC supplies Rust/Cargo build state and the image assertions establish the
tool contract. Do not reintroduce those actions unless the corresponding cache
or image contract changes.

## Capacity and persistent cache

`values-8-cores.yaml` is normal operation: `minRunners=0`, `maxRunners=1`.
`values-paused.yaml` is the mandatory pause overlay: `minRunners=0`,
`maxRunners=0`. The job uses group `query-regression-persistent-cache-v1`,
`queue: max`, and `cancel-in-progress: false`; admitted jobs queue rather than
replacing older pending jobs. During maintenance, cancel admitted queued runs as
well as pausing ARC. Runner Pods have `activeDeadlineSeconds=12600`.

The cache claim `query-regression-build-cache` is a nominal 600Gi `local-path`
PVC in `arc-runners`. It is `ReadWriteOnce`; `local-path` uses
WaitForFirstConsumer binding and Delete reclaim behavior, produces a
node-affine local PV, is non-expandable, and the 600Gi request is not a hard
storage quota. The runner's `minipc-3` selector is its only consumer candidate.

The initializer mounts the PVC root at `/cache`, creates and write-tests these
versioned subpaths as non-root UID/GID 1001, and the runner mounts them as:

| Persistent state | PVC subpath | Runner mount |
| --- | --- | --- |
| Cargo home | `cargo-home-v1` | `/home/runner/.cargo` |
| Rustup home | `rustup-home-v1` | `/home/runner/.rustup` |
| Cargo target | `query-regression-target-v1` | `/home/runner/query-regression-target` |
| Cache metadata | `meta-v1` | `/home/runner/query-regression-cache-meta` |

The Pod security context uses UID/GID and `fsGroup` 1001 with
`fsGroupChangePolicy: OnRootMismatch`; no privileged `chown` or raw `hostPath`
is used. `CARGO_HOME`, `RUSTUP_HOME`, `CARGO_TARGET_DIR`, and cache metadata
are persistent absolute paths. Base and candidate builds share the target; Cargo
fingerprints invalidate source and dependency changes.

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
- warn at Cargo size 60GiB; at 80GiB remove only `registry/src` and
  `git/checkouts`, then abort if Cargo remains at least 80GiB;
- below 300GiB backing free space, clear the complete target root first,
  remeasure, then remove only those Cargo extracted trees and checkouts; abort
  if free space is still below 300GiB;
- never automatically remove Cargo registry cache/index, Git database, Cargo
  bin, Rustup, cache metadata, or the PVC.

The target clear uses fixed absolute roots and removes all entries, including
dotfiles. Remove old versioned subpaths only in explicit maintenance while ARC
is 0/0 and no runner Pod exists.

## Deploy and pause safely

First verify external `.2` routing and the disk preflight. Apply the PVC; while
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
ABI-marker output. Obtain dependency and tool network byte counters from the
identified `.2` counter source, filtered to `minipc-3` and the dependency/tool
destinations.

Accept the canary only when all of the following hold:

- exactly one runner Pod runs on `minipc-3`, and both jobs use the same bound PV;
- UID/GID 1001 cache mounts are writable; the warm run does not invalidate the
  target or bulk-redownload crates or toolchains;
- warm base build time is at most 50% of cold base build time;
- warm dependency/tool network bytes are at most 10% of cold fill bytes;
- cache sizes remain below soft watermarks, node free space remains at least
  300GiB, and the benchmark is correct without TLS EOFs or timeouts;
- `.2` confirms this traffic is outside VPN accounting.

Immediately return to 0/0 after either canary unless ongoing normal operation
has been explicitly approved; return immediately on any traffic, cache, disk,
TLS, or correctness failure. To roll back, use the paused Helm upgrade above,
or another explicit `helm upgrade --install` with known-good values followed by
`values-paused.yaml`, `--reset-values`, and `--wait`. Do not delete the PVC
automatically; preserve it for diagnosis unless intentionally discarding cache.

## Future optional phases

The first phase does not require a new image or shared cache service. Optional
follow-ups are a digest-pinned image seeded with the exact Rust toolchain,
protoc, uv, sccache, and `cargo fetch --locked`; and an internal read/write
sccache backend or Cargo/Git mirror. Evaluate them only if persistent PVC reuse
is insufficient.
