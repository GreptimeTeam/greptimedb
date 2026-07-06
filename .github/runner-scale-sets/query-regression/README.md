# Query regression self-hosted runners

The `Query Regression` workflow targets self-hosted GitHub Actions runners via
runner labels or ARC runner scale set names:

- `perf-regression-8-cores`
- `perf-regression-16-cores`
- `perf-regression-32-cores`
- `perf-regression-64-cores`

The names intentionally avoid generic labels such as `ubuntu-22.04-8-cores`,
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

## Install the query-regression scale sets

Install one or more sizes. The Helm release name and `runnerScaleSetName` should
match the `runs-on` value used by the workflow.

```bash
helm upgrade --install perf-regression-8-cores \
  oci://ghcr.io/actions/actions-runner-controller-charts/gha-runner-scale-set \
  --namespace arc-runners \
  --create-namespace \
  -f .github/runner-scale-sets/query-regression/values-8-cores.yaml

helm upgrade --install perf-regression-16-cores \
  oci://ghcr.io/actions/actions-runner-controller-charts/gha-runner-scale-set \
  --namespace arc-runners \
  --create-namespace \
  -f .github/runner-scale-sets/query-regression/values-16-cores.yaml

helm upgrade --install perf-regression-32-cores \
  oci://ghcr.io/actions/actions-runner-controller-charts/gha-runner-scale-set \
  --namespace arc-runners \
  --create-namespace \
  -f .github/runner-scale-sets/query-regression/values-32-cores.yaml

helm upgrade --install perf-regression-64-cores \
  oci://ghcr.io/actions/actions-runner-controller-charts/gha-runner-scale-set \
  --namespace arc-runners \
  --create-namespace \
  -f .github/runner-scale-sets/query-regression/values-64-cores.yaml
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

These runners execute PR code and should be treated as untrusted execution
capacity:

- keep them isolated from sensitive internal services unless explicitly required;
- do not mount host paths, Docker socket, kubeconfig, or long-lived credentials;
- use ephemeral runner pods and no shared work directory with trusted jobs;
- keep GitHub tokens least-privilege and rely on normal `pull_request` behavior
  for fork PRs, where repository secrets are withheld and `GITHUB_TOKEN` is
  read-only;
- review fork workflow changes before approving the run.

If stronger isolation is required, install a separate runner group/namespace just
for query-regression PR workloads and restrict repository/workflow access to the
`Query Regression` workflow.

## Build cache

The workflow builds the base and candidate checkouts in the same job with a
shared `CARGO_TARGET_DIR`. After the base binary is built, it is copied aside;
the candidate build then reuses the dependency artifacts from the base build in
that same target directory.

The workflow also uses the GitHub Actions Rust cache. Fork PRs restore existing
trusted caches but do not save new cache entries; workflow dispatch and
same-repository PRs may refresh the cache.

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
