# Query regression self-hosted runners

The `Query Regression` workflow targets self-hosted GitHub Actions runners via
runner labels or ARC runner scale set names:

- `perf-regression-8-cores`

The name intentionally avoids generic labels such as `ubuntu-22.04-8-cores`,
which may already be used by GitHub-hosted larger runners or other runner pools.

For Kubernetes-based runners, ARC runner pods run inside the target Kubernetes
cluster and connect outbound to GitHub; GitHub can then dispatch jobs whose
`runs-on` value matches the runner scale set name or runner labels.

## Prerequisites

Install the ARC scale set controller in the Kubernetes cluster if it is not
already installed:

```bash
helm upgrade --install arc \
  oci://ghcr.io/actions/actions-runner-controller-charts/gha-runner-scale-set-controller \
  --namespace arc-systems \
  --create-namespace
```

Create the GitHub App or PAT secret in the namespace that will host the runner
scale sets. Prefer a GitHub App with access limited to `GreptimeTeam/greptimedb`.

```bash
kubectl -n arc-runners create secret generic greptimedb-arc-github-app \
  --from-literal=github_app_id=<app-id> \
  --from-literal=github_app_installation_id=<installation-id> \
  --from-file=github_app_private_key=<private-key.pem>
```

The values files in this directory reference that secret by name.

## Build the runner image

Use a derived ARC runner image instead of the minimal upstream runner image. The
image keeps the official `/home/runner/run.sh` entrypoint layout and adds the
tools this workflow expects to be present on a normal CI host, including `wget`,
`uv`, C/C++ build tools, OpenSSL headers, and protobuf tooling.

```bash
docker build \
  -f .github/runner-scale-sets/query-regression/Dockerfile \
  -t greptime-registry.cn-hangzhou.cr.aliyuncs.com/greptime/greptimedb-query-regression-runner:latest \
  .github/runner-scale-sets/query-regression

docker push greptime-registry.cn-hangzhou.cr.aliyuncs.com/greptime/greptimedb-query-regression-runner:latest
```

Deploy the runner image by digest rather than by a mutable tag. Update
`values-8-cores.yaml` after pushing a rebuilt image.

The workflow still runs setup actions for pinned Rust and `uv` behavior. `mold`
is installed in the image and selected through `CARGO_BUILD_RUSTFLAGS`, so jobs
do not need privileged package installation at runtime.

Decide whether the registry repository is public or private. Public pull access
avoids distributing registry credentials to the runner namespace. If the image
must be private, create a dedicated read-only image-pull secret for this image
and attach it only as `imagePullSecrets`; do not expose registry credentials to
runner containers.

## Install the query-regression scale set

Install the runner scale set. The Helm release name and `runnerScaleSetName`
should match the `runs-on` value used by the workflow.

```bash
helm upgrade --install perf-regression-8-cores \
  oci://ghcr.io/actions/actions-runner-controller-charts/gha-runner-scale-set \
  --namespace arc-runners \
  --create-namespace \
  -f .github/runner-scale-sets/query-regression/values-8-cores.yaml
```

Check registration and pods:

```bash
kubectl -n arc-runners get pods
kubectl -n arc-runners get autoscalingrunnersets
```

The scale sets should also appear under repository Actions runner settings.

## Security notes for fork PRs

Maintainer-approved fork PRs can run on self-hosted runners. Approval only lets
the workflow execute; it does not make the fork code trusted.

The `query-regression` label is the explicit trigger for PR runs. Updating a PR
does not rerun the benchmark automatically; remove and re-add the label after
reviewing the updated changes.

These runners execute PR code and should be treated as untrusted execution
capacity:

- keep them isolated from sensitive internal services unless explicitly required;
- do not mount host paths, Docker socket, kubeconfig, or long-lived credentials;
- use ephemeral runner pods and no shared work directory with trusted jobs;
- disable service account token mounting in runner pods unless Kubernetes API
  access is intentionally required;
- use a runner image whose default user is non-root, disable privilege
  escalation, drop Linux capabilities, and use the runtime-default seccomp
  profile;
- keep GitHub tokens least-privilege and rely on normal `pull_request` behavior
  for fork PRs, where repository secrets are withheld and `GITHUB_TOKEN` is
  read-only;
- review fork workflow changes before approving the run.

If stronger isolation is required, install a separate runner group/namespace just
for query-regression PR workloads and restrict repository/workflow access to the
`Query Regression` workflow.

Use namespace or cluster network policy to restrict runner egress where the CNI
supports the needed controls. Query-regression runners need outbound access to
GitHub Actions services, GitHub artifact/cache endpoints, Rust/crate/toolchain
endpoints, DNS, and the configured image registries. Block access to unrelated
cluster services, private network ranges, and cloud metadata endpoints unless a
case explicitly needs them.

## Build cache

The workflow builds base and candidate in the same job, the same source path,
and a shared `CARGO_TARGET_DIR`. It checks out the base ref into `src`, builds
and copies the base binary aside, then resets that same `src` checkout to the
candidate ref before building the candidate binary. Keeping the workspace path
stable improves Cargo incremental reuse for local workspace crates compared with
building separate `base-src` and `candidate-src` checkouts.

The workflow also uses the GitHub Actions Rust cache for restore-only cache
reuse. PR and dispatch runs do not save cache entries. Refresh shared caches from
trusted maintenance workflows only.

The ARC values in this directory do not configure a cross-run runner-local
compiler cache. If cross-run compile time still dominates the benchmark,
deployers may add one of the following at the runner infrastructure layer:

- a custom runner image with Rust tooling, `mold`, and `sccache` preinstalled;
- `RUSTC_WRAPPER=sccache` plus a shared `sccache` backend such as object storage,
  Redis, or a Kubernetes storage class that is safe for concurrent runner pods;
- separate cache namespaces/buckets for untrusted PR code and trusted branches to
  avoid cache poisoning across trust boundaries.

Do not mount a shared writable host path, Docker socket, kubeconfig, or other
privileged credentials into runners that execute PR code.
