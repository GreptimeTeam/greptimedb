# ACK least-privilege boundary for the query-regression benchmark

This directory contains the **declarative, not-yet-deployed** Kubernetes
boundary for the one-shot ACK query-regression benchmark. Nothing here is
applied by CI or by any workflow in this repository; the manifests are the
reviewable contract and must be applied by the cluster owner together with the
controller runner/kubeconfig setup. No static/admin kubeconfig, no ACK Secret,
no cloud/nodepool-scale API, and no deployment automation exists here or in
the workflows.

## Threat model

Assume the controller credential (the kubeconfig `exec` plugin identity) is
stolen. What can it do?

* The namespaced Role in `rbac.yaml` limits it to `jobs create/get/delete`,
  `pods get/list`, `pods/exec create`, `pods/log get`, and `events list` in
  `query-regression-perf` only. No cluster scope, no secrets, no nodepool API.
* `resourcequota.yaml` caps the namespace at exactly **one Job and one Pod**
  with the exact benchmark resource totals — a stolen credential can hold at
  most one Job/pod, and nothing else fits.
* `networkpolicy.yaml` denies all ingress and egress in the namespace
  (enforcement depends on the cluster CNI; see the hard gates below).
* The deployment-gated ValidatingAdmissionPolicies
  (`validatingadmissionpolicy.yaml` + `validatingadmissionpolicybinding.yaml`)
  deny every Job that is not the exact conforming benchmark shape: exact
  deterministic name, label set and full-SHA annotation set, the exact
  digest-pinned image, one container, no init/ephemeral/extra containers, an
  exact env contract (count, uniqueness, literal allowlist, fixed path values,
  bounded variable formats), exact resources / nodeSelector / toleration /
  security, no host features / secrets / PVC / hostPath (emptyDir only), no
  scheduling bypass or control fields (`nodeName`, `suspend`, custom
  `schedulerName`, `priorityClassName`, `priority`, `affinity`,
  `schedulingGates` — placement is only via the pinned nodeSelector), and the
  exact command / bootstrap-args / deadline / TTL / replacement policy.
  Direct Pod creation is denied (RBAC `no pods/create` is the primary control;
  the ownerReferences VAP is defense-in-depth only). `pods/exec` CONNECT is
  restricted to benchmark pods and to the exact canonical controller / pinned
  kubectl command tuples.
* PSA `restricted` is enforced at the namespace level (`namespace.yaml`).

### Residual capability (intentional, contained)

A stolen credential can still upload **arbitrary payload content** via
`kubectl cp` (the allowlist pins the `tar` argv and destination `/payload`,
not the file bytes) and run **arbitrary code inside the one sandbox pod**
(non-root 1001, tokenless, emptyDir-only, network denied by NetworkPolicy).
This is the designed benchmark path (the pod exists to run candidate code);
the boundary deliberately contains the capability to a single disposable pod
and nothing else.

## Files

| File | Purpose |
| --- | --- |
| `namespace.yaml` | Namespace `query-regression-perf` with PSA restricted labels |
| `serviceaccounts.yaml` | Tokenless `default` / `query-regression-controller` / `query-regression-workload` SAs |
| `rbac.yaml` | Exact namespaced Role/RoleBinding for the controller exec identity |
| `resourcequota.yaml` | Max 1 Job/1 Pod + exact resource totals |
| `limitrange.yaml` | Container resource bounds within the benchmark contract |
| `networkpolicy.yaml` | Deny-all ingress/egress |
| `validatingadmissionpolicy.yaml` | Deployment-gated workload / direct-pods / exec CONNECT policies |
| `validatingadmissionpolicybinding.yaml` | Enforcement bindings (`["Deny","Audit"]`) scoped to the namespace |
| `validatingadmissionpolicybinding-rollout.yaml` | Optional `["Warn","Audit"]` rollout bindings — mutually exclusive with the enforcement bindings |
| `apply-test.sh` | Offline negative-mutation self-test (`QR_APPLY_TEST_SELF=1`) + server-side CEL compile + typeChecking inspection + disposable-canary exec/cp probes |
| `query-regression-policy-test.py` | Offline policy tests (run in `checks.yml`) |

## Deployment gate

The ValidatingAdmissionPolicies are annotated `greptimedb.io/deployment-gated:
"true"` because their CEL expressions can only be compiled by a live API
server. Before deploying:

1. Run the offline tests: `uv run --no-project --with pyyaml python
   .github/runner-scale-sets/query-regression/ack/query-regression-policy-test.py`.
2. Prove the negative Job mutation pipeline locally (no kubectl, no cluster):
   `QR_APPLY_TEST_SELF=1 ./apply-test.sh` — every negative mutation reads the
   controller-rendered manifest JSON from stdin, must emit valid JSON, and
   must change its expected field; this runs again at the start of step 5
   before any mutated manifest is sent to the API server. Then run
   `apply-test.sh` against the target cluster: server `--dry-run=server`
   (compiles CEL — it does **not** exercise CONNECT), apply the gated
   policies, inspect each VAP's `status.typeChecking.expressionWarnings`, run
   positive/negative Job/Pod probes, then — with
   `QR_APPLY_TEST_CANARY=1` — create a real disposable canary Job in
   `query-regression-perf`, probe exec/cp admission, and delete it. The
   canary is destructive to the canary only and targets only
   `query-regression-perf`; without the flag the script exits 2 because
   exec/cp CONNECT validation is a hard gate.
3. Only after both pass, apply the rest of the boundary:
   `kubectl apply -f .../ack/` and set the RoleBinding subject in `rbac.yaml`
   to the kubeconfig exec identity.

Enforcement actions: Kubernetes 1.34 forbids `Deny` together with `Warn` in
one binding. The enforced bindings use exactly `["Deny", "Audit"]`; the
optional audit/warn rollout lives in `validatingadmissionpolicybinding-rollout.yaml`
(`["Warn", "Audit"]`) and is **mutually exclusive** — apply one or the other
per policy, never both.

### Hard gates (do not weaken silently)

* **Real server tests**: `apply-test.sh` with `QR_APPLY_TEST_CANARY=1` — the
  API server is authoritative for CEL compilation, typeChecking, binding
  actions, and exec/cp CONNECT behavior; the offline tests only mirror the
  contract structurally.
* **NetworkPolicy/CNI enforcement**: the deny-all NetworkPolicy must be
  actually enforced by the cluster CNI (verify with an in-namespace egress
  probe); the manifest alone is not enforcement.
* **kubectl ABI pin**: exec/cp argv+stream tuples are pinned to kubectl
  `v1.34.2` (see the controller's `KUBECTL_PINNED_VERSION`); the controller
  fails closed on any other client version, and the canary re-validates the
  tuples against the real binary.
* **No `Deny`+`Warn`**: enforcement bindings must never combine `Deny` and
  `Warn`.

Known limits (do not weaken silently):

* The exec VAP sees the `PodExecOptions` object (target via
  `request.namespace`/`request.name`, options via
  `object.container`/`command`/`tty`/`stdin`/`stdout`/`stderr`) — it does
  **not** see the parent Pod's `metadata` (labels/ownerReferences), so pod
  identity pinning in the exec policy relies on the name regex plus the pods
  VAP and RBAC as defense-in-depth.
* The exec command allowlist is expressed in CEL as exact argv tuples (not
  parent-Pod-derived); `kubectl cp` content (the payload bytes) is not
  constrained — that is the intentional residual capability above.
* A per-command CEL-visible exec allowlist that also checked Pod exec options
  in depth would require a validating webhook; the current exact-argv VAP plus
  the disposable-canary verification is the documented control.
