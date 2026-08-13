#!/usr/bin/env python3
# Copyright 2023 Greptime Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Trusted controller for the one-shot ACK query-regression benchmark Job.

This script is trusted default-branch code: the controller workflow (a
``workflow_run`` follower of the unprivileged ``Query Regression`` build
workflow) checks out the repository default branch and executes this script
from that checkout. A PR cannot replace it, and it is never taken from the
build artifact.

Trust boundaries (v2, secure architecture):

* The **build** workflow (``query-regression.yml``) is unprivileged: it runs
  on the local ARC runner, compiles base/candidate binaries, uploads the base
  artifact *before* any candidate-controlled build step runs, then uploads the
  candidate artifact and a strict metadata artifact. It has no ACK/cloud
  credential references, no kubeconfig, no kubectl cluster calls, and no
  privileged controller job.
* The **controller** workflow (``query-regression-controller.yml``) is a
  trusted default-branch follower. It never checks out or executes PR code.
  Its admission step validates the originating run/artifacts/PR/SHA chain
  from GitHub API data (see ``query-regression-admission.cjs``) and only then
  downloads the base/candidate artifacts by exact validated ids and executes
  this script with the validated values. The metadata artifact is treated as
  untrusted; every cross-checkable field is re-validated against the API, and
  the manifests inside the artifacts are re-checked against the validated
  SHAs here.
* The controller requires a **runner-local kubeconfig** whose active user uses
  an ``exec`` credential plugin (a short-lived-token broker). The kubeconfig
  is never a GitHub secret, is never printed, and fails closed if it embeds a
  token, client key/cert (inline data or file-backed), username/password,
  auth-provider, or static exec env secrets. Token refresh is 10-15 minutes
  (see the README); the
  runner itself must be a trusted external/hardware identity.
* Every kubectl call uses exactly the kubeconfig ``current-context`` that
  ``validate_kubeconfig`` resolved and verified exec-only. Context overrides
  are removed or fail closed: the ``--context`` flag / ``KUBECTL_CONTEXT``
  env override is deleted from the CLI (argparse rejects ``--context``), and
  ``KUBECTL_CONTEXT`` is stripped from every kubectl subprocess environment,
  so no runner-side override can redirect the controller to a different
  cluster than the validated exec-plugin kubeconfig's current-context.
* The benchmark Job is created in the fixed namespace
  ``query-regression-perf`` with a deterministic name
  ``query-regression-<run-id>-<attempt>``. The namespace/image are not
  overridable. The payload byte size is measured before Job creation and
  capped conservatively under the 40Gi ephemeral-storage limit.
* The benchmark Job pod has ``automountServiceAccountToken: false``, the
  tokenless ``query-regression-workload`` service account, no GitHub / cloud /
  kube credentials, no secrets, non-root UID/GID 1001, seccomp RuntimeDefault,
  no privilege escalation, and all capabilities dropped. It runs the
  digest-pinned runtime image on the dedicated
  ``alibabacloud.com/nodepool-id=npb5ff93bea3a447a698fe31ebc997ea31`` bulk pool
  (which must carry the ``dedicated=perf-regression:NoSchedule`` taint). The
  ACK pod runs only the performance driver and its required helpers; no
  tooling/unit tests run on ACK. Server-side ValidatingAdmissionPolicies
  (deployment-gated) and RBAC/quota/network-policy manifests under
  ``.github/runner-scale-sets/query-regression/ack/`` cap a stolen controller
  credential to one conforming Job/Pod in an otherwise sterile namespace.
* Nodepool scaling is implicit only: the Pending Job pod is the scale-up
  signal for ACK Cluster Autoscaler and foreground deletion of the Job/pod is
  the scale-down signal. This controller never calls any nodepool API.
* Deletion is exact-name validated against the deterministic pattern, retried
  with a bounded number of attempts, and the Job's absence is verified before
  the controller reports success. If deletion cannot be confirmed the workflow
  fails even when the benchmark itself reported status 0.
* Result collection is mandatory: the controller fails the run (status != 0)
  when the required report files or the summary are missing or invalid.
  Logs/pod diagnostics remain best-effort.
* One global monotonic lifecycle deadline bounds every phase; normal phases
  clamp to ``cleanup_begins`` (the deadline minus an explicit cleanup
  reserve) so no ordinary path can consume the reserve, and every kubectl /
  subprocess operation is clamped to the remaining phase budget. Deletion is
  the only operation allowed inside the reserve. Normal-phase operations
  (preflight job lookup, leaked-job reconcile deletion, Job creation, pod
  polling, payload transfer, benchmark polling, result collection) raise when
  their phase budget is exhausted instead of degrading to a 1-second cluster
  call: no Job is ever created or reconciled after ``cleanup_begins``.
* Cancellation: SIGTERM/SIGINT skip optional diagnostics/collection and
  immediately perform a short bounded exact Job deletion/absence check via the
  ``finally`` path. Synchronous cleanup is **not** guaranteed under GitHub
  hard cancellation (SIGKILL / run cancellation): no further steps or jobs
  run, so the workflow also includes a separate ``if: always()`` second-
  process cleanup step, and the README documents deterministic exact-name
  recovery plus the residual need for an independent reaper or manual cleanup
  (``ttlSecondsAfterFinished`` bounds the leak server-side).

Future hardening (documented, not implemented here): replace `kubectl cp` /
exec-gated markers with an init container + signed object-storage upload so the
cluster RBAC no longer needs ``pods/exec``.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import signal
import stat
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

try:  # PyYAML is optional for structured kubeconfig validation; the raw scan
    # still fails closed on every banned key without it.
    import yaml
except ImportError:  # pragma: no cover - depends on the environment
    yaml = None

# ---------------------------------------------------------------------------
# Time source (indirection so tests can drive a fake clock)
# ---------------------------------------------------------------------------

def _now() -> float:
    return time.monotonic()


def _sleep(seconds: float) -> None:
    time.sleep(max(0.0, seconds))


def _remaining_until(deadline: float) -> float:
    return max(0.0, deadline - _now())


def clamp_int(seconds: float, requested: int, minimum: int = 1) -> int:
    """Clamp ``requested`` seconds to ``seconds`` remaining, never below ``minimum``."""
    return int(max(minimum, min(requested, seconds)))


def clamp_remaining_or_raise(remaining: float, requested: int, what: str) -> int:
    """Clamp ``requested`` seconds to ``remaining``; raise when exhausted.

    Normal-phase cluster operations must never run on a zero/negative budget:
    clamping an exhausted budget to a 1-second call could still introduce or
    retain cloud compute with no time left to finish or clean up. Cleanup-only
    operations use ``clamp_int`` instead (they are allowed inside the reserve).
    """
    if remaining <= 0:
        raise ControllerError(f"no budget remains for {what}")
    return clamp_int(remaining, requested)


# ---------------------------------------------------------------------------
# Constants (single source of truth for the Job manifest and budgets)
# ---------------------------------------------------------------------------

# Deterministic per-run Job name pattern. `run-id`/`run-attempt` are decimal
# GitHub run identifiers of the *originating* build run, so the name is always
# lowercase alphanumeric plus dashes, within the 63-char label/name segment
# limit, and stable for the reaper.
JOB_NAME_RE = re.compile(r"^query-regression-[1-9][0-9]*-[1-9][0-9]*$")

# Fixed, non-overridable Job namespace (requirement: namespace override removed
# or fail-closed to the exact name; the override is removed entirely).
DEFAULT_NAMESPACE = "query-regression-perf"

# Fixed, non-overridable digest-pinned runtime image (the same image the ARC
# runner scale sets used; no new image is built or pushed by this change).
DEFAULT_IMAGE = (
    "greptime-registry.cn-hangzhou.cr.aliyuncs.com/greptime/"
    "greptimedb-query-regression-runner@sha256:"
    "e713b294e23b7e15184e558866c90025e59930033e72c97650dbc7f1ca022d11"
)

# Conservative pre-Job payload byte cap, well under the pod's 40Gi
# ephemeral-storage limit (2 GiB).
PAYLOAD_BYTES_CAP_DEFAULT = 2 * 1024**3

# The ACK bulk-ingestion nodepool (Hangzhou cluster bulk-ingestion-test). The
# nodepool taint dedicated=perf-regression:NoSchedule must be present.
NODEPOOL_SELECTOR = {"alibabacloud.com/nodepool-id": "npb5ff93bea3a447a698fe31ebc997ea31"}
NODEPOOL_TAINT = {
    "key": "dedicated",
    "operator": "Equal",
    "value": "perf-regression",
    "effect": "NoSchedule",
}

# One global monotonic lifecycle budget. The controller job timeout is
# WORKFLOW_JOB_TIMEOUT_SECONDS (180 min) and must strictly exceed the
# controller lifecycle LIFECYCLE_TIMEOUT_DEFAULT (150 min) by at least
# SETUP_AND_OUTER_CLEANUP_MARGIN_MINIMUM (25 min) so that checkout, admission,
# artifact download/attestation, kubeconfig setup, and the second-process
# cleanup-only step all fit inside the job. Normal phases
# (pod-ready, payload transfer, benchmark + mandatory collection) clamp to
# cleanup_begins = hard_deadline - CLEANUP_RESERVE_DEFAULT, so no ordinary
# path can consume the reserve; deletion is the only operation allowed inside
# the reserve and clamps to the hard deadline. A normal-phase operation whose
# budget is exhausted raises instead of clamping to a 1-second cluster call:
# no Job is ever created or reconciled after cleanup_begins.
POD_READY_TIMEOUT_DEFAULT = 1500    # 25 min: nodepool scale-up + image pull
RUN_TIMEOUT_DEFAULT = 4800          # 80 min: benchmark run + mandatory result collection
DELETE_TIMEOUT_DEFAULT = 240        # per-attempt foreground delete wait (s)
DELETE_ATTEMPTS_DEFAULT = 3         # bounded retries; sleeps 2s between attempts
DELETE_RETRY_SLEEP = 2              # seconds between deletion attempts
COLLECTION_WINDOW_ITERATIONS = 300  # pod-side window after .done: 2s * 300 = 600 s
WORKFLOW_JOB_TIMEOUT_SECONDS = 10800  # controller job timeout (180 min)
LIFECYCLE_TIMEOUT_DEFAULT = 9000      # 150 min global lifecycle budget for the controller
CLEANUP_RESERVE_DEFAULT = 900         # 15 min reserved exclusively for deletion
SETUP_AND_OUTER_CLEANUP_MARGIN_MINIMUM = 1500  # 25 min min margin above the lifecycle
PAYLOAD_TRANSFER_ALLOWANCE = 600     # worst-case kubectl cp + marker allowance (s)
CANCELLATION_DELETE_BUDGET = 120     # short bounded deletion on SIGTERM/SIGINT (s)
CLEANUP_ONLY_BUDGET = 600            # second-process cleanup-only deletion budget (s)

# Total bound on the pod lifetime. Reached only if the controller dies; the
# controller's own budgets are all shorter. TTL-based Job cleanup (600 s after
# finish) bounds the leak server-side.
ACTIVE_DEADLINE_SECONDS = 10800
TTL_SECONDS_AFTER_FINISHED = 600

# Env vars the controller passes through from its own environment into the
# benchmark Job pod. None of these can carry credentials.
PASSTHROUGH_ENV = [
    "CASE_PATHS",
    "HTTP_TIMEOUT",
    "ALLOW_LARGE_FIXTURE",
    "RUN_URL",
    "CASE_NAME",
    "BASE_REF",
    "CANDIDATE_REF",
    "CARGO_PROFILE",
]

# Pod env vars computed from the payload layout inside the pod.
POD_ENV = [
    ("BASE_BIN", "/payload/bins/base/greptime"),
    ("CANDIDATE_BIN", "/payload/bins/candidate/greptime"),
    ("FIXTURE_GENERATOR", "/payload/bins/candidate/query_perf_fixture"),
    ("QUERY_REGRESSION_RUNNER", "/payload/bins/candidate/query_regression_runner"),
    ("OTELGEN_BIN", "/usr/local/bin/otelgen"),
    ("SUMMARY_SCRIPT", "/payload/repo/.github/scripts/query-regression-summary.py"),
    ("UV_CACHE_DIR", "/tmp/uv-cache"),
    # The python driver appends `status=N` to $GITHUB_OUTPUT; the pod entrypoint
    # reads it back to learn the benchmark result.
    ("GITHUB_OUTPUT", "/work/github-output"),
]

# Pod binary paths -> artifact-relative manifest paths, used for in-pod
# sha256 verification against the base/candidate manifests after `kubectl cp`.
BIN_PATH_MAP = {
    "/payload/bins/base/greptime": "query-regression-bins/base/greptime",
    "/payload/bins/candidate/greptime": "query-regression-bins/candidate/greptime",
    "/payload/bins/candidate/query_perf_fixture": "query-regression-bins/candidate/query_perf_fixture",
    "/payload/bins/candidate/query_regression_runner": "query-regression-bins/candidate/query_regression_runner",
}

# ---------------------------------------------------------------------------
# kubectl ABI pin and canonical exec/cp protocol
# ---------------------------------------------------------------------------
# The controller's `kubectl exec`/`kubectl cp` invocations translate into
# CONNECT requests whose argv and stream flags depend on the exact kubectl
# client version (kubectl cp shells out to remote `tar`). The deployment-gated
# exec ValidatingAdmissionPolicy allows exactly these argv+stream tuples, so
# the client version is an ABI: the controller fails closed unless the
# runner's kubectl matches this exact pinned patch version. Upgrading kubectl
# requires bumping this pin, updating the tuples below (the offline policy
# test cross-checks them against the VAP), and re-running apply-test.sh's
# disposable-canary exec/cp validation.
KUBECTL_PINNED_VERSION = "v1.34.2"

# Plain `kubectl exec <ns>/<pod> -- sh -c '<script>'` invocations. kubectl
# sends no -i/-t, so the CONNECT stream flags are stdin=false, stdout=true,
# stderr=true, tty=false. Each script is exactly one argv tuple in the exec
# policy allowlist; changing one here requires changing the VAP with it.
EXEC_LAYOUT_CHECK = (
    "test -f /payload/run.sh && test -d /payload/bins/base && "
    "test -d /payload/bins/candidate && test -d /payload/repo/tests/perf/query_cases"
)
EXEC_CHMOD = (
    "chmod +x /payload/run.sh /payload/bins/base/greptime "
    "/payload/bins/candidate/greptime /payload/bins/candidate/query_perf_fixture "
    "/payload/bins/candidate/query_regression_runner"
)
EXEC_SHA256 = "sha256sum " + " ".join(BIN_PATH_MAP)
EXEC_READY = "touch /payload/.ready"
EXEC_DONE = "test -f /work/.done"
EXEC_STATUS = "cat /work/benchmark-status 2>/dev/null || true"
EXEC_COLLECTED = "touch /work/.collected"
EXEC_SCRIPTS = [
    EXEC_LAYOUT_CHECK,
    EXEC_CHMOD,
    EXEC_SHA256,
    EXEC_READY,
    EXEC_DONE,
    EXEC_STATUS,
    EXEC_COLLECTED,
]
EXEC_STREAMS = {"stdin": False, "stdout": True, "stderr": True, "tty": False}

# `kubectl cp` remote `tar` argv for the pinned kubectl ABI. Push pipes a
# local `tar cf -` into `kubectl exec -i <pod> -- tar xmf - -C <dest>`; pull
# runs `kubectl exec <pod> -- tar cf - <abspath>` piped to a local untar.
# Stream flags follow how the pinned kubectl wires the remoteexec streams
# (stdout is always wired; stdin only for push). These tuples are part of the
# exec policy allowlist and are verified end-to-end by apply-test.sh's canary;
# if the pinned kubectl ever differs, update constants + VAP + tests together.
CP_PUSH_COMMANDS = [["tar", "xmf", "-", "-C", "/payload"]]
CP_PUSH_STREAMS = {"stdin": True, "stdout": True, "stderr": True, "tty": False}
CP_PULL_COMMANDS = [
    ["tar", "cf", "-", "/work/query-regression-work"],
    ["tar", "cf", "-", "/work/query-regression-summary.md"],
]
CP_PULL_STREAMS = {"stdin": False, "stdout": True, "stderr": True, "tty": False}

# Trusted scripts restored by the workflow from the default-branch checkout;
# the controller verifies the summary script against the recorded manifest.
# query-regression-pr-metadata.py is retired: the controller regenerates the
# comment metadata from the validated admission values.
TRUSTED_SCRIPT_NAMES = [
    "query-regression-ack-controller.py",
    "query-regression-summary.py",
]

# Pod entrypoint: wait for the payload marker (bounded), then run the trusted
# entrypoint. 300 iterations * 2s = 600s marker window. Bash is required: the
# entrypoint uses `set -o pipefail`, which dash (/bin/sh) does not support.
BOOTSTRAP_SCRIPT = """\
set -eu
i=0
until [ -f /payload/.ready ]; do
  if [ "${i}" -ge 300 ]; then
    echo "query-regression-ack: timed out waiting for /payload/.ready marker" >&2
    exit 1
  fi
  sleep 2
  i=$((i + 1))
done
cd /work
exec /bin/bash /payload/run.sh
"""

# Trusted pod-side entrypoint. Generated from this template by the controller
# and copied into the pod; it invokes ONLY the performance driver and its
# required helpers/processes inside the ACK pod (no tooling/unit tests — ACK
# runs perf only). It signals completion with /work/.done and
# /work/benchmark-status, then stays alive (bounded collection window) so the
# controller can copy results back while the container is still running.
RUN_SH_TEMPLATE = """\
#!/bin/bash
# Trusted query-regression ACK Job entrypoint, generated by
# query-regression-ack-controller.py (default-branch code). Executes only
# inside the one-shot ACK benchmark Job pod, never on the credentialed runner.
# ACK runs performance regression only.
set -euo pipefail

cd /work

echo "== payload inventory =="
ls -la /payload
ls -la /payload/bins/base
ls -la /payload/bins/candidate

echo "== query regression driver =="
set +e
uv run --no-project python /payload/repo/.github/scripts/query-regression-run.py \\
  --base-src /payload/repo \\
  --candidate-src /payload/repo \\
  --summary-script /payload/repo/.github/scripts/query-regression-summary.py
driver_rc=$?
set -e

status=1
if [ -s /work/github-output ]; then
  status="$(sed -n 's/^status=//p' /work/github-output | tail -n 1)"
fi
case "${status}" in
  ''|*[!0-9]*) status=1 ;;
esac
if [ "${driver_rc}" -ne 0 ] && [ "${status}" -eq 0 ]; then
  status=1
fi
printf '%s' "${status}" > /work/benchmark-status
echo "query regression status: ${status} (driver rc: ${driver_rc})"

# Signal completion, then stay alive so the controller can copy results back
# (kubectl cp/exec require a running container).
: > /work/.done
i=0
until [ -f /work/.collected ]; do
  if [ "${i}" -ge ${COLLECTION_WINDOW_ITERATIONS} ]; then
    echo "query regression: results collection window expired" >&2
    exit 1
  fi
  sleep 2
  i=$((i + 1))
done
echo "query regression: results collected; exiting"
exit 0
""".replace("${COLLECTION_WINDOW_ITERATIONS}", str(COLLECTION_WINDOW_ITERATIONS))


class ControllerError(RuntimeError):
    """Fatal controller error; the run is aborted and the Job is cleaned up."""


class Lifecycle:
    """Global monotonic lifecycle budget with an explicit cleanup reserve.

    Normal phases clamp to ``cleanup_begins`` (hard deadline minus the
    reserve); deletion is the only operation allowed inside the reserve and
    clamps to the hard deadline. Diagnostics are skipped once the reserve
    begins so the cleanup path always has its full budget.
    """

    def __init__(self, hard_timeout: int, cleanup_reserve: int):
        self.started = _now()
        self.hard_deadline = self.started + hard_timeout
        self.cleanup_reserve = cleanup_reserve
        self.cleanup_begins = self.hard_deadline - cleanup_reserve

    def remaining_until(self, deadline: float) -> float:
        return _remaining_until(deadline)

    def phase_deadline(self, phase_seconds: int | None = None) -> float:
        """Absolute deadline for a normal phase; never past ``cleanup_begins``."""
        base = self.cleanup_begins
        if phase_seconds is not None:
            base = min(base, _now() + phase_seconds)
        return base

    def phase_remaining(self, phase_seconds: int | None = None) -> float:
        return self.remaining_until(self.phase_deadline(phase_seconds))

    def in_cleanup_reserve(self) -> bool:
        return _now() >= self.cleanup_begins

    def cleanup_remaining(self) -> float:
        return self.remaining_until(self.hard_deadline)

    def clamp(self, requested: int, phase_seconds: int | None = None) -> int:
        """Clamp a subprocess timeout to the remaining phase budget.

        The phase deadline never extends past ``cleanup_begins``, so no
        ordinary phase operation can consume the cleanup reserve. ``clamp``
        never raises: use ``require_phase`` / ``phase_clamp`` for normal-phase
        operations that must not run on an exhausted budget.
        """
        return clamp_int(self.phase_remaining(phase_seconds), requested)

    def require_phase(self, phase_seconds: int | None = None, *, what: str = "operation") -> float:
        """Return the remaining normal-phase budget, raising when exhausted.

        Normal-phase cluster operations (preflight job lookup, leaked-job
        reconcile deletion, Job creation, payload transfer, benchmark polling,
        result collection) must never run with zero/negative budget: a
        clamped 1-second call could still introduce or retain cloud compute
        with no time left to finish or clean up. Raises ControllerError once
        the cleanup reserve begins (or the phase budget is exhausted).
        """
        remaining = self.phase_remaining(phase_seconds)
        if remaining <= 0:
            raise ControllerError(
                f"no budget remains for {what}: the cleanup reserve has begun "
                "and normal-phase cluster operations are refused"
            )
        return remaining

    def phase_clamp(
        self, requested: int, phase_seconds: int | None = None, *, what: str = "operation"
    ) -> int:
        """Clamp a subprocess timeout to the remaining phase budget; raise when
        the budget is exhausted instead of degrading to a 1-second call."""
        return clamp_int(self.require_phase(phase_seconds, what=what), requested)


def validate_run_id(value: str) -> int:
    if not re.match(r"^[1-9][0-9]*$", value):
        raise ControllerError(f"invalid run id/attempt (must be a positive integer): {value!r}")
    return int(value)


def build_job_name(run_id: int, run_attempt: int) -> str:
    return f"query-regression-{run_id}-{run_attempt}"


def validate_job_name(name: str) -> str:
    if not JOB_NAME_RE.match(name):
        raise ControllerError(
            f"refusing to operate on a Job whose name does not match the deterministic "
            f"pattern query-regression-<run-id>-<run-attempt>: {name!r}"
        )
    if len(name) > 63:
        raise ControllerError(f"Job name exceeds 63 characters: {name!r}")
    return name


def passthrough_env() -> list[dict[str, str]]:
    env: list[dict[str, str]] = []
    for name in PASSTHROUGH_ENV:
        default = "all" if name == "CASE_PATHS" else "300" if name == "HTTP_TIMEOUT" else "true" if name == "ALLOW_LARGE_FIXTURE" else "nightly" if name == "CARGO_PROFILE" else ""
        env.append({"name": name, "value": os.environ.get(name, default)})
    env.extend({"name": name, "value": value} for name, value in POD_ENV)
    return env


def build_manifest(
    *,
    run_id: int,
    run_attempt: int,
    job_name: str,
    base_sha: str,
    candidate_sha: str,
) -> dict[str, Any]:
    """Render the one-shot benchmark Job manifest (fixed namespace/image)."""
    if "@sha256:" not in DEFAULT_IMAGE:
        raise ControllerError(
            f"benchmark Job image must be digest-pinned (contain '@sha256:'): {DEFAULT_IMAGE!r}"
        )
    labels = {
        "app": "query-regression",
        "run-id": str(run_id),
        "run-attempt": str(run_attempt),
    }
    annotations: dict[str, str] = {}
    if base_sha:
        annotations["greptimedb.io/query-regression-base-sha"] = base_sha
    if candidate_sha:
        annotations["greptimedb.io/query-regression-candidate-sha"] = candidate_sha
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": job_name,
            "namespace": DEFAULT_NAMESPACE,
            "labels": labels,
            "annotations": annotations,
        },
        "spec": {
            "backoffLimit": 0,
            "completions": 1,
            "parallelism": 1,
            "activeDeadlineSeconds": ACTIVE_DEADLINE_SECONDS,
            "ttlSecondsAfterFinished": TTL_SECONDS_AFTER_FINISHED,
            "podReplacementPolicy": "Failed",
            "template": {
                "metadata": {"labels": labels},
                "spec": {
                    "automountServiceAccountToken": False,
                    "serviceAccountName": "query-regression-workload",
                    "restartPolicy": "Never",
                    "nodeSelector": NODEPOOL_SELECTOR,
                    "tolerations": [NODEPOOL_TAINT],
                    "securityContext": {
                        "runAsNonRoot": True,
                        "runAsUser": 1001,
                        "runAsGroup": 1001,
                        "fsGroup": 1001,
                        "seccompProfile": {"type": "RuntimeDefault"},
                    },
                    "containers": [
                        {
                            "name": "benchmark",
                            "image": DEFAULT_IMAGE,
                            "imagePullPolicy": "IfNotPresent",
                            "command": ["/bin/sh", "-c"],
                            "args": [BOOTSTRAP_SCRIPT],
                            "env": passthrough_env(),
                            "securityContext": {
                                "runAsNonRoot": True,
                                "runAsUser": 1001,
                                "runAsGroup": 1001,
                                "allowPrivilegeEscalation": False,
                                "capabilities": {"drop": ["ALL"]},
                            },
                            "resources": {
                                "requests": {
                                    "cpu": "4",
                                    "memory": "12Gi",
                                    "ephemeral-storage": "20Gi",
                                },
                                "limits": {
                                    "cpu": "8",
                                    "memory": "16Gi",
                                    "ephemeral-storage": "40Gi",
                                },
                            },
                            "volumeMounts": [
                                {"name": "payload", "mountPath": "/payload"},
                                {"name": "work", "mountPath": "/work"},
                            ],
                        }
                    ],
                    "volumes": [
                        {"name": "payload", "emptyDir": {}},
                        {"name": "work", "emptyDir": {}},
                    ],
                },
            },
        },
    }


# ---------------------------------------------------------------------------
# Payload assembly (copy only — nothing is executed on the controller)
# ---------------------------------------------------------------------------

def reject_non_regular(root: Path, label: str) -> None:
    """Recursively reject symlinks and non-regular files under ``root``.

    Candidate-controlled trees are copied into the ACK pod and executed there;
    a symlink could escape the payload boundary (absolute or relative target)
    or alias an in-tree path, and a FIFO/socket/device could block or confuse
    the copy. Nothing that is not a plain regular file or directory is copied,
    and a candidate symlink is never dereferenced.
    """
    if not root.exists():
        raise ControllerError(f"{label} does not exist: {root}")
    if root.is_symlink():
        raise ControllerError(f"{label} is a symlink, refusing to copy: {root}")
    for entry in root.rglob("*"):
        if entry.is_symlink():
            raise ControllerError(f"{label} contains a symlink, refusing to copy: {entry}")
        if entry.is_dir():
            continue
        if not entry.is_file():
            raise ControllerError(
                f"{label} contains a non-regular file, refusing to copy: {entry}"
            )


def resolve_contained(path: Path, root: Path, label: str) -> Path:
    """Resolve ``path``, enforce containment under resolved ``root``, and
    reject every symlink in any path component from ``root`` down to the leaf.

    An ancestor symlink (e.g. ``candidate/tests -> outside``) could make the
    payload escape the checkout boundary even when the leaf is a regular
    file. Every component is checked with ``is_symlink()`` before any
    resolution follows it, so a candidate symlink is never dereferenced.
    """
    root_resolved = root.resolve()
    if not root_resolved.is_dir():
        raise ControllerError(f"{label} root is not a directory: {root_resolved}")
    try:
        rel = path.relative_to(root)
    except ValueError:
        raise ControllerError(f"{label} is not under the candidate artifact root: {path}")
    cursor = root
    for part in rel.parts:
        cursor = cursor / part
        if cursor.is_symlink():
            raise ControllerError(
                f"{label} has a symlink component (ancestor of {path}), refusing to copy: {cursor}"
            )
    resolved = path.resolve()
    if not resolved.is_relative_to(root_resolved):
        raise ControllerError(f"{label} escapes the candidate artifact root: {resolved}")
    return resolved


def load_manifest(artifact_dir: Path, name: str) -> dict[str, Any]:
    path = artifact_dir / name
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as err:
        raise ControllerError(f"could not read {path}: {err}") from err
    if not isinstance(data, dict):
        raise ControllerError(f"{path} is not a JSON object")
    return data


def assemble_payload(
    base_artifact_dir: Path,
    candidate_artifact_dir: Path,
    trusted_summary: Path,
    payload_dir: Path,
) -> Path:
    """Assemble the payload directory that is `kubectl cp`'d into the pod.

    Content:
      bins/...                     base + candidate binaries (two artifacts)
      repo/tests/perf/**           candidate case files (symlink-free, contained)
      repo/.github/scripts/query-regression-run.py   candidate driver (PR content)
      repo/.github/scripts/query-regression-summary.py  TRUSTED (default branch)
      run.sh                       generated from the trusted template

    The controller workflow never checks out PR code: the candidate driver and
    the tests/perf tree travel inside the candidate artifact (staged by the
    unprivileged build workflow) and are copied from there, after the same
    symlink/containment rejection as the binaries. The controller only copies
    these files; candidate content is executed exclusively inside the ACK pod.
    """
    if payload_dir.exists():
        shutil.rmtree(payload_dir)
    payload_dir.mkdir(parents=True, exist_ok=True)

    # Binaries come from the two immutable artifacts only.
    bin_sources = {
        ("base", "query-regression-bins/base/greptime"),
        ("candidate", "query-regression-bins/candidate/greptime"),
        ("candidate", "query-regression-bins/candidate/query_perf_fixture"),
        ("candidate", "query-regression-bins/candidate/query_regression_runner"),
    }
    for kind, src_rel in sorted(bin_sources):
        artifact_dir = base_artifact_dir if kind == "base" else candidate_artifact_dir
        src = artifact_dir / src_rel
        if src.is_symlink() or not src.is_file():
            raise ControllerError(f"missing or non-regular artifact file: {src_rel}")
        dst = payload_dir / "bins" / kind / Path(src_rel).name
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        dst.chmod(dst.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

    # Candidate-controlled case tree: enforce containment + reject every
    # symlink component (ancestors included) and non-regular file, then copy
    # (no symlinks remain, so nothing is dereferenced). It lives inside the
    # candidate artifact: repo/tests/perf.
    tests_perf = resolve_contained(
        candidate_artifact_dir / "repo" / "tests" / "perf",
        candidate_artifact_dir,
        "candidate tests/perf",
    )
    if not tests_perf.is_dir():
        raise ControllerError(f"candidate artifact has no repo/tests/perf directory: {tests_perf}")
    reject_non_regular(tests_perf, "candidate tests/perf")
    shutil.copytree(tests_perf, payload_dir / "repo" / "tests" / "perf", symlinks=False)

    # Candidate-controlled driver: resolve, enforce containment, reject
    # symlink components, and require a regular file.
    driver = resolve_contained(
        candidate_artifact_dir / "repo" / ".github" / "scripts" / "query-regression-run.py",
        candidate_artifact_dir,
        "candidate driver",
    )
    if not driver.is_file():
        raise ControllerError(f"candidate driver is not a regular file: {driver}")
    driver_dst = payload_dir / "repo" / ".github" / "scripts" / "query-regression-run.py"
    driver_dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(driver, driver_dst)

    # Trusted summary script: comes from the default-branch checkout via the
    # workflow, never from the build artifact or the candidate.
    if trusted_summary.is_symlink() or not trusted_summary.is_file():
        raise ControllerError(f"trusted summary script is missing or not a regular file: {trusted_summary}")
    summary_dst = payload_dir / "repo" / ".github" / "scripts" / "query-regression-summary.py"
    shutil.copy2(trusted_summary, summary_dst)

    run_sh = payload_dir / "run.sh"
    run_sh.write_text(RUN_SH_TEMPLATE, encoding="utf-8")
    run_sh.chmod(0o755)
    return payload_dir


def measure_payload(payload_dir: Path, cap_bytes: int) -> int:
    """Measure the payload byte size and fail closed above the pre-Job cap.

    The cap is conservative (default 2 GiB) and sits well under the pod's 40Gi
    ephemeral-storage limit so a hostile or accidental payload can never
    exhaust the node's storage class budget.
    """
    total = 0
    for path in payload_dir.rglob("*"):
        if path.is_file():
            try:
                total += path.stat().st_size
            except OSError as err:
                raise ControllerError(f"could not stat payload file {path}: {err}") from err
    if total > cap_bytes:
        raise ControllerError(
            f"payload byte size {total} exceeds the pre-Job cap of {cap_bytes} bytes; "
            "refusing to create the benchmark Job"
        )
    print(f"payload byte size: {total} (cap {cap_bytes})")
    return total


# ---------------------------------------------------------------------------
# Kubeconfig validation (runner-local exec-plugin kubeconfig, fail closed)
# ---------------------------------------------------------------------------

# Keys that, if present with a value, embed long-lived or static credentials.
# This includes file-backed client-certificate/client-key: the exec-plugin
# broker model must be the ONLY authentication mechanism, so no static client
# certificate/key authentication (inline base64 data or file paths) is allowed
# on any user, active or inactive, even when an exec block is present.
BANNED_KUBECONFIG_KEYS = (
    "token",
    "tokenFile",
    "client-key-data",
    "client-certificate-data",
    "client-key",
    "client-certificate",
    "username",
    "password",
    "auth-provider",
)

# Exec env names that could carry a credential. Fail closed on any of these.
SECRET_ENV_NAME_RE = re.compile(
    r"(token|secret|password|credential|access[_-]?key|private[_-]?key|api[_-]?key|"
    r"ak[_-]?(id|secret)|pem|cert)",
    re.IGNORECASE,
)

# Credential-looking values: PEM blocks, JWTs, and long base64 blobs.
CREDENTIAL_VALUE_RE = re.compile(
    r"-----BEGIN [A-Z ]*PRIVATE KEY-----|"
    r"^[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+$|"
    r"^[A-Za-z0-9+/]{40,}={0,2}$",
    re.MULTILINE,
)

BANNED_KUBECONFIG_LINE_RE = re.compile(r"^[ \t]*([A-Za-z0-9_-]+)[ \t]*:")


def _credential_looking(value: str) -> bool:
    return bool(CREDENTIAL_VALUE_RE.search(value))


def _secret_env_name(name: str) -> bool:
    return bool(SECRET_ENV_NAME_RE.search(name))


def validate_kubeconfig(path: Path) -> None:
    """Validate a runner-local kubeconfig; fail closed on any embedded secret.

    PyYAML is mandatory: without it the kubeconfig cannot be validated
    structurally and the controller refuses to run. The kubeconfig must have a
    resolvable ``current-context`` whose cluster and user exist, and the
    ACTIVE user must use an ``exec`` credential plugin (a short-lived-token
    broker); exec must be the active user's ONLY authentication mechanism.
    It must not embed ``token``/``tokenFile``, ``client-key-data`` /
    ``client-certificate-data``, file-backed ``client-certificate`` /
    ``client-key``, ``username``/``password``, ``auth-provider``, or static
    exec env secrets — on any user, active or inactive, even when an exec
    block is present. The file content is never printed: errors name only the
    path and the offending key.
    """
    if not path.exists():
        raise ControllerError(f"runner-local kubeconfig not found: {path}")
    if not path.is_file():
        raise ControllerError(f"runner-local kubeconfig is not a regular file: {path}")
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as err:
        raise ControllerError(f"could not read runner-local kubeconfig {path}: {err}") from err

    # 1. Dependency-free raw scan: any occurrence of a banned key with a value
    #    anywhere in the file rejects, regardless of YAML structure.
    for line in text.splitlines():
        match = BANNED_KUBECONFIG_LINE_RE.match(line)
        if match and match.group(1) in BANNED_KUBECONFIG_KEYS:
            raise ControllerError(
                f"kubeconfig {path} embeds banned key {match.group(1)!r}; "
                "only an exec credential plugin kubeconfig is allowed"
            )

    # 2. Structured validation. PyYAML is MANDATORY: the raw scan cannot
    #    enforce the exec-plugin/current-context contract, so without it the
    #    controller fails closed instead of weakening the check.
    if yaml is None:
        raise ControllerError(
            "PyYAML is required to validate the runner-local kubeconfig; install it on the "
            "query-regression-ack-controller runner (e.g. python3 -m pip install pyyaml or "
            "uv run --no-project --with pyyaml) and re-run"
        )
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as err:
        raise ControllerError(f"kubeconfig {path} is not valid YAML: {err}") from err
    if not isinstance(data, dict):
        raise ControllerError(f"kubeconfig {path} is not a YAML mapping")
    users = data.get("users") or []
    clusters = data.get("clusters") or []
    contexts = data.get("contexts") or []
    if not isinstance(users, list):
        raise ControllerError(f"kubeconfig {path} has no users list")
    if not isinstance(clusters, list):
        raise ControllerError(f"kubeconfig {path} has no clusters list")
    if not isinstance(contexts, list):
        raise ControllerError(f"kubeconfig {path} has no contexts list")

    # 3. current-context must be present and fully resolvable: the context
    #    must exist, and its cluster and user must exist. A kubeconfig without
    #    an explicit active context is ambiguous and rejected.
    current_context = data.get("current-context")
    if not isinstance(current_context, str) or not current_context:
        raise ControllerError(
            f"kubeconfig {path} has no current-context; the active context must be explicit"
        )
    user_map = {}
    for user_entry in users:
        if not isinstance(user_entry, dict) or not user_entry.get("name"):
            raise ControllerError(f"kubeconfig {path} has a malformed user entry")
        user_map[str(user_entry["name"])] = user_entry
    cluster_map = {}
    for cluster_entry in clusters:
        if not isinstance(cluster_entry, dict) or not cluster_entry.get("name"):
            raise ControllerError(f"kubeconfig {path} has a malformed cluster entry")
        cluster_map[str(cluster_entry["name"])] = cluster_entry
    active_context = None
    for ctx in contexts:
        if isinstance(ctx, dict) and ctx.get("name") == current_context:
            active_context = ctx
            break
    if active_context is None:
        raise ControllerError(
            f"kubeconfig {path} current-context {current_context!r} is not in contexts"
        )
    ctx_body = active_context.get("context") or {}
    if not isinstance(ctx_body, dict):
        raise ControllerError(f"kubeconfig {path} current-context {current_context!r} has a malformed context body")
    ctx_cluster = ctx_body.get("cluster")
    ctx_user = ctx_body.get("user")
    if not ctx_cluster:
        raise ControllerError(f"kubeconfig {path} current-context {current_context!r} has no cluster")
    if not ctx_user:
        raise ControllerError(f"kubeconfig {path} current-context {current_context!r} has no user")
    if str(ctx_cluster) not in cluster_map:
        raise ControllerError(
            f"kubeconfig {path} current-context {current_context!r} references unknown cluster {ctx_cluster!r}"
        )
    if str(ctx_user) not in user_map:
        raise ControllerError(
            f"kubeconfig {path} current-context {current_context!r} references unknown user {ctx_user!r}"
        )
    active_user = str(ctx_user)

    # 4. Per-user validation. Banned keys (static credentials, including
    #    file-backed client-certificate/client-key) are rejected on every
    #    user, active or inactive, even when an exec block is present. ONLY
    #    the active user must provide exec auth (the exec requirement applies
    #    to the user kubectl will actually authenticate as).
    for user_name, user_entry in user_map.items():
        user = user_entry.get("user") or {}
        if not isinstance(user, dict):
            raise ControllerError(f"kubeconfig {path} user {user_name!r} has no user mapping")
        for banned in BANNED_KUBECONFIG_KEYS:
            if banned in user:
                raise ControllerError(
                    f"kubeconfig {path} user {user_name!r} embeds banned key {banned!r}"
                )
        exec_block = user.get("exec")
        if user_name != active_user:
            # Inactive users: exec env policy still applies (they must not
            # smuggle static secrets past the raw scan).
            if isinstance(exec_block, dict):
                _validate_exec_block(path, user_name, exec_block)
            continue
        if not isinstance(exec_block, dict):
            raise ControllerError(
                f"kubeconfig {path} active user {user_name!r} does not use an exec credential plugin"
            )
        _validate_exec_block(path, user_name, exec_block)

    # 5. Raw credential-value scan (dependency-free): PEM/JWT/long-base64
    #    values anywhere in the file reject.
    if _credential_looking(text):
        raise ControllerError(
            f"kubeconfig {path} embeds a credential-looking value; "
            "only an exec credential plugin kubeconfig is allowed"
        )
    print(f"kubeconfig {path} validated (exec-plugin, no embedded credentials)")


def _validate_exec_block(path: Path, user_name: str, exec_block: dict) -> None:
    """Validate one kubeconfig user exec block (apiVersion/command/args/env)."""
    api_version = exec_block.get("apiVersion")
    if api_version is not None and not re.match(
        r"^client\.authentication\.k8s\.io/v1(beta1)?$", str(api_version)
    ):
        raise ControllerError(
            f"kubeconfig {path} user {user_name!r} exec apiVersion {api_version!r} is not a "
            "supported client.authentication.k8s.io version"
        )
    if not exec_block.get("command"):
        raise ControllerError(f"kubeconfig {path} user {user_name!r} exec block has no command")
    args = exec_block.get("args")
    if args is not None:
        if not isinstance(args, list) or not all(isinstance(arg, str) for arg in args):
            raise ControllerError(f"kubeconfig {path} user {user_name!r} exec args must be a list of strings")
    env_list = exec_block.get("env") or []
    if not isinstance(env_list, list):
        raise ControllerError(f"kubeconfig {path} user {user_name!r} exec env is not a list")
    for entry in env_list:
        if not isinstance(entry, dict):
            raise ControllerError(f"kubeconfig {path} user {user_name!r} has a malformed exec env entry")
        env_name = entry.get("name")
        if env_name is None:
            raise ControllerError(f"kubeconfig {path} user {user_name!r} exec env entry has no name")
        env_value = entry.get("value")
        if _secret_env_name(str(env_name)):
            raise ControllerError(
                f"kubeconfig {path} user {user_name!r} exec env {env_name!r} may carry a static secret"
            )
        if env_value is not None and _credential_looking(str(env_value)):
            raise ControllerError(
                f"kubeconfig {path} user {user_name!r} exec env {env_name!r} looks like a credential"
            )


# ---------------------------------------------------------------------------
# kubectl plumbing
# ---------------------------------------------------------------------------

class Kubectl:
    def __init__(self, binary: str, kubeconfig: str | None):
        """Wrap the pinned kubectl ABI; context is never overridable.

        Every invocation uses exactly the ``current-context`` of the validated
        kubeconfig (see ``validate_kubeconfig``): no ``--context`` flag is
        ever passed and ``KUBECTL_CONTEXT`` is stripped from the subprocess
        environment, so a runner-side override cannot redirect the controller
        to a different cluster.
        """
        if shutil.which(binary) is None:
            raise ControllerError(
                f"kubectl binary not found: {binary!r}. Install kubectl {KUBECTL_PINNED_VERSION} "
                f"on the query-regression-ack-controller runner (see the query regression README)."
            )
        self.binary = binary
        self.namespace = DEFAULT_NAMESPACE
        self.kubeconfig = kubeconfig
        self._verify_pinned_version()

    def _verify_pinned_version(self) -> None:
        """Fail closed unless kubectl matches the pinned ABI version.

        The exec/cp CONNECT argv and stream-flag tuples the controller sends
        (and the deployment-gated exec ValidatingAdmissionPolicy allows) are
        pinned to ``KUBECTL_PINNED_VERSION``; any other client version is
        refused rather than risk a silently drifting command allowlist.
        """
        try:
            proc = subprocess.run(
                [self.binary, "version", "--client", "-o", "json"],
                capture_output=True,
                text=True,
                timeout=30,
                env=self._env(),
            )
        except (OSError, subprocess.TimeoutExpired) as err:
            raise ControllerError(f"could not determine kubectl client version: {err}") from err
        if proc.returncode != 0:
            raise ControllerError(
                f"kubectl version probe failed ({proc.returncode}): "
                f"{proc.stderr.strip()[-500:]}"
            )
        try:
            data = json.loads(proc.stdout)
        except json.JSONDecodeError as err:
            raise ControllerError(f"unparsable kubectl version output: {err}") from err
        version = str((data.get("clientVersion") or {}).get("gitVersion", ""))
        if version != KUBECTL_PINNED_VERSION:
            raise ControllerError(
                f"kubectl client version {version!r} does not match the pinned ABI "
                f"{KUBECTL_PINNED_VERSION!r}; the exec/cp protocol and the admission policy "
                "allowlist are pinned to that exact version (bump the pin, update the "
                "canonical command tuples, and re-run apply-test.sh's canary together)"
            )

    def _env(self) -> dict[str, str]:
        env = dict(os.environ)
        # Fail closed against a context override: kubectl honors a
        # KUBECTL_CONTEXT env var from the runner environment, which would
        # redirect calls away from the validated kubeconfig current-context.
        # The validated kubeconfig is the ONLY configuration source kubectl
        # may use.
        env.pop("KUBECTL_CONTEXT", None)
        if self.kubeconfig:
            env["KUBECONFIG"] = self.kubeconfig
        return env

    def run(
        self,
        args: list[str],
        *,
        check: bool = True,
        input_text: str | None = None,
        timeout: int = 180,
    ) -> subprocess.CompletedProcess[str]:
        # No --context override is ever passed: kubectl must use exactly the
        # current-context of the validated kubeconfig (see validate_kubeconfig).
        cmd = [self.binary] + args
        try:
            proc = subprocess.run(
                cmd,
                input=input_text,
                capture_output=True,
                text=True,
                timeout=timeout,
                env=self._env(),
            )
        except subprocess.TimeoutExpired as err:
            raise ControllerError(f"kubectl timed out: {' '.join(cmd)}") from err
        if check and proc.returncode != 0:
            raise ControllerError(
                f"kubectl command failed ({proc.returncode}): {' '.join(cmd)}\n"
                f"stdout: {proc.stdout.strip()[-2000:]}\nstderr: {proc.stderr.strip()[-2000:]}"
            )
        return proc

    def pod_spec(self, pod: str) -> str:
        return f"{self.namespace}/{pod}"


# ---------------------------------------------------------------------------
# Cluster operations
# ---------------------------------------------------------------------------

def job_lookup(kubectl: Kubectl, job_name: str, timeout: int = 180) -> bool:
    """Get the exact Job fail-closed: only a canonical result is meaningful.

    ``kubectl get job NAME --ignore-not-found -o name`` exits 0 and prints
    nothing when the Job is genuinely absent, prints exactly the single line
    ``job.batch/NAME`` (optionally with the single standard trailing newline)
    when it exists, and exits non-zero on authorization (403), API/transport,
    and other failures (the client additionally raises on a subprocess
    timeout). Only two outputs are ever read as a verdict:

    * RAW empty stdout (``""``, no whitespace at all) proves absence; a
      whitespace-only stdout (spaces/tabs/newlines) is malformed and raises.
    * The exact ``job.batch/NAME`` resource name, with at most the single
      standard trailing newline kubectl emits, proves presence; leading/
      trailing spaces, a different resource name, extra content, or multiple
      lines all raise.
    * Any stderr at all on a successful get -- judged on the raw captured
      bytes (``proc.stderr != ""``), so whitespace-only or newline-only
      exec-plugin noise is pollution too -- makes the result non-canonical
      and raises.

    A malformed get must never be read as either absent or present, and a
    failing get must NEVER be read as absence: a 403 would otherwise let a
    still-running Job be reported as deleted (fail-open). Returns True when
    the Job exists and raises ControllerError on any non-zero exit or
    non-canonical output so callers fail the run / retry instead of trusting
    absence from an error.
    """
    validate_job_name(job_name)
    proc = kubectl.run(
        [
            "get", "job", job_name, "-n", kubectl.namespace,
            "--ignore-not-found", "-o", "name",
        ],
        check=False,
        timeout=timeout,
    )
    if proc.returncode != 0:
        raise ControllerError(
            f"cannot determine whether job {job_name} exists: kubectl get failed "
            f"({proc.returncode}): {proc.stderr.strip()[-1000:]}"
        )
    if proc.stderr != "":
        # A successful get writes nothing to stderr; ANY stderr at all --
        # judged on the RAW captured bytes (``proc.stderr != ""``), so even
        # whitespace-only/newline-only exec-plugin noise is pollution -- means
        # the result is not canonical and must fail closed instead of being
        # read as absent or present.
        raise ControllerError(
            f"malformed output from kubectl get for job {job_name}: the get "
            f"succeeded but wrote to stderr: {proc.stderr.strip()[-500:]}"
        )
    stdout = proc.stdout
    if stdout == "":
        # Raw empty stdout (nothing at all, not even a newline): the ONLY
        # proof of absence. Whitespace-only output falls through to the
        # malformed branch below.
        return False
    expected = f"job.batch/{job_name}"
    if stdout == expected or stdout == expected + "\n":
        # The exact present resource name, optionally with the single
        # standard trailing newline kubectl's `-o name` output ends with.
        # Two newlines, leading/trailing spaces, or any other byte sequence
        # is malformed (see below).
        return True
    raise ControllerError(
        f"malformed output from kubectl get for job {job_name}: expected raw "
        f"empty stdout (absent) or exactly {expected!r} (present, optionally "
        f"with one trailing newline), got {proc.stdout!r}"
    )


def job_exists(kubectl: Kubectl, job_name: str, timeout: int = 180) -> bool:
    return job_lookup(kubectl, job_name, timeout=timeout)


def job_absent(kubectl: Kubectl, job_name: str, timeout: int = 180) -> bool:
    """Confirm the Job is absent: only a successful empty get proves it.

    Raises ControllerError on authorization/API/transport failures and client
    timeouts (see job_lookup); absence is never inferred from a failing call.
    """
    return not job_lookup(kubectl, job_name, timeout=timeout)


def _require_remaining(deadline: float, what: str) -> float:
    """Return the remaining budget, raising immediately when exhausted.

    Cleanup operations re-check the budget immediately before each cluster
    call (delete, confirmation lookup): an exhausted budget raises
    ControllerError instead of ``clamp_int`` degrading it to a 1-second
    operation that could still start against the cluster with no time left.
    """
    remaining = _remaining_until(deadline)
    if remaining <= 0:
        raise ControllerError(f"cleanup budget exhausted before {what}")
    return remaining


def delete_job(
    kubectl: Kubectl,
    job_name: str,
    deadline: float,
    attempts: int = DELETE_ATTEMPTS_DEFAULT,
    delete_timeout: int = DELETE_TIMEOUT_DEFAULT,
) -> None:
    """Delete the exact Job with bounded retries and verify its absence.

    Absence is proven ONLY by a successful exact lookup (see ``job_lookup``):
    ``kubectl get job NAME --ignore-not-found -o name`` with rc=0 and raw
    empty stdout. A non-zero delete NEVER proves absence on its own -- not
    even an API ``NotFound`` in stderr: with an exec credential plugin, "not
    found" text can come from the plugin itself while the delete failed for an
    unrelated reason (timeout/transport/authz). Every success conclusion goes
    through a subsequent successful exact lookup. Authorization/API/transport/
    timeout failures on the lookups raise (fail closed) and are retried
    within the bounded attempt loop; a delete that succeeded (or failed) but
    whose confirmation lookup still shows the Job present is also retried.
    Every operation is clamped to ``deadline`` and the remaining budget is
    re-checked immediately before the delete and before the confirmation
    lookup: an exhausted budget raises ControllerError instead of clamping to
    a 1-second operation, so the caller fails the run even if the benchmark
    itself reported status 0.
    """
    validate_job_name(job_name)
    last_error: str | None = None
    for attempt in range(1, attempts + 1):
        remaining = _require_remaining(deadline, "deletion could be confirmed")
        # 1. Exact absence pre-check: a successful empty get proves the Job is
        #    already gone (e.g. a previous run's cleanup) and needs no delete.
        try:
            if job_absent(kubectl, job_name, timeout=clamp_int(remaining, 30)):
                print(f"job {job_name} already gone; nothing to delete")
                return
        except ControllerError as err:
            # A failing absence check must never be read as "absent": record
            # it and retry within the bounded attempt budget (fail closed).
            last_error = str(err)
            print(
                f"absence check attempt {attempt}/{attempts} failed for {job_name}: {last_error}",
                file=sys.stderr,
            )
            if attempt < attempts:
                remaining = _remaining_until(deadline)
                if remaining > 0:
                    _sleep(min(DELETE_RETRY_SLEEP, remaining))
            continue
        # 2. Delete attempt. The budget is re-checked immediately before the
        #    delete: an exhausted budget raises instead of degrading to a
        #    1-second clamped delete against the cluster.
        remaining = _require_remaining(deadline, "issuing the delete")
        flag = clamp_int(remaining, delete_timeout)
        proc = kubectl.run(
            [
                "delete", "job", job_name, "-n", kubectl.namespace,
                "--cascade=foreground", "--wait=true", f"--timeout={flag}s",
            ],
            check=False,
            timeout=clamp_int(_require_remaining(deadline, "the delete subprocess"), flag + 60),
        )
        if proc.returncode != 0:
            # A non-zero delete NEVER proves absence by itself -- not even an
            # API NotFound in stderr: with an exec credential plugin the
            # "not found" text can come from the plugin itself while the
            # delete failed for an unrelated reason. Absence is concluded
            # only by the exact lookup below.
            last_error = proc.stderr.strip()[-1000:]
            print(
                f"delete attempt {attempt}/{attempts} failed for {job_name}: {last_error}",
                file=sys.stderr,
            )
        # 3. Confirmation: the ONLY success conclusion is a subsequent
        #    successful exact lookup proving absence. The budget is re-checked
        #    immediately before the confirmation too, so an exhausted budget
        #    raises instead of clamping to a 1-second lookup.
        _require_remaining(deadline, "confirming deletion")
        try:
            absent = job_absent(
                kubectl, job_name, timeout=clamp_int(_remaining_until(deadline), 30)
            )
        except ControllerError as err:
            last_error = str(err)
            print(
                f"confirmation check attempt {attempt}/{attempts} failed for {job_name}: {err}",
                file=sys.stderr,
            )
        else:
            if absent:
                print(f"job {job_name} deleted and confirmed absent")
                return
            if last_error is None:
                last_error = (
                    f"job {job_name} still present after foreground delete; deletion not confirmed"
                )
                print(last_error, file=sys.stderr)
        if attempt < attempts:
            remaining = _remaining_until(deadline)
            if remaining > 0:
                _sleep(min(DELETE_RETRY_SLEEP, remaining))
    raise ControllerError(
        f"failed to delete job {job_name} after {attempts} attempts: {last_error}"
    )


def create_job(kubectl: Kubectl, manifest: dict[str, Any], timeout: int = 180) -> None:
    kubectl.run(
        ["create", "-f", "-"],
        input_text=json.dumps(manifest, sort_keys=True),
        timeout=timeout,
    )


def get_pod_name(kubectl: Kubectl, job_name: str, timeout: int = 180) -> str | None:
    proc = kubectl.run(
        [
            "get", "pod", "-l", f"job-name={job_name}", "-n", kubectl.namespace,
            "-o", "jsonpath={.items[0].metadata.name}",
        ],
        check=False,
        timeout=timeout,
    )
    if proc.returncode != 0:
        return None
    name = proc.stdout.strip()
    return name or None


def pod_phase(kubectl: Kubectl, pod: str, timeout: int = 180) -> str:
    proc = kubectl.run(
        ["get", "pod", kubectl.pod_spec(pod), "-o", "jsonpath={.status.phase}"],
        check=False,
        timeout=timeout,
    )
    return proc.stdout.strip() if proc.returncode == 0 else ""


def exec_sh(
    kubectl: Kubectl, pod: str, command: str, *, check: bool = True, timeout: int = 180
) -> subprocess.CompletedProcess[str]:
    return kubectl.run(
        ["exec", kubectl.pod_spec(pod), "--", "sh", "-c", command],
        check=check,
        timeout=timeout,
    )


def wait_for_pod(kubectl: Kubectl, job_name: str, deadline: float) -> str:
    """Wait for the Job pod to be Running, clamped to ``deadline``.

    The pod is Pending while ACK Cluster Autoscaler provisions a node for the
    dedicated nodepool; that Pending pod is the intended scale-up signal and is
    not an error. Every poll and sleep is clamped to the remaining budget; an
    exhausted budget raises instead of issuing a 1-second poll.
    """
    while True:
        remaining = _remaining_until(deadline)
        if remaining <= 0:
            raise ControllerError(
                "timed out waiting for the Job pod to start (nodepool scale-up). The "
                "Pending pod is the ACK autoscaling signal; check the nodepool's "
                "Cluster Autoscaler and the pod events in the ACK cluster."
            )
        pod = get_pod_name(
            kubectl, job_name, timeout=clamp_remaining_or_raise(remaining, 30, "pod poll")
        )
        if pod:
            phase = pod_phase(
                kubectl, pod,
                timeout=clamp_remaining_or_raise(_remaining_until(deadline), 30, "pod phase poll"),
            )
            if phase == "Running":
                print(f"job pod {pod} is Running")
                return pod
            if phase in ("Succeeded", "Failed", "Unknown"):
                raise ControllerError(f"job pod {pod} reached phase {phase} before payload transfer")
        _sleep(min(10, _remaining_until(deadline)))


def transfer_payload(
    kubectl: Kubectl,
    pod: str,
    payload_dir: Path,
    manifest_binaries: list[dict[str, str]],
    deadline: float,
) -> None:
    """Copy the payload into the running pod, verify it, then arm the marker.

    Every cp/exec operation is clamped to ``deadline`` so the transfer phase
    can never consume the cleanup reserve.
    """
    for item in ("bins", "repo", "run.sh"):
        src = payload_dir / item
        if not src.exists():
            raise ControllerError(f"payload item missing: {src}")
        proc = kubectl.run(
            ["cp", str(src), f"{kubectl.pod_spec(pod)}:/payload/"],
            check=False,
            timeout=clamp_remaining_or_raise(_remaining_until(deadline), 600, f"kubectl cp of {item}"),
        )
        if proc.returncode != 0:
            raise ControllerError(
                f"kubectl cp of {item} failed: {proc.stderr.strip()[-1000:]}"
            )
        print(f"copied payload item {item}")

    exec_sh(
        kubectl,
        pod,
        EXEC_LAYOUT_CHECK,
        timeout=clamp_remaining_or_raise(_remaining_until(deadline), 60, "payload layout check"),
    )

    exec_sh(
        kubectl,
        pod,
        EXEC_CHMOD,
        timeout=clamp_remaining_or_raise(_remaining_until(deadline), 60, "payload chmod"),
    )

    # Verify the binaries inside the pod against the base/candidate manifests.
    proc = exec_sh(
        kubectl,
        pod,
        EXEC_SHA256,
        timeout=clamp_remaining_or_raise(_remaining_until(deadline), 120, "in-pod sha256 verification"),
    )
    manifest_binary_map = {entry["path"]: entry["sha256"] for entry in manifest_binaries}
    for line in proc.stdout.splitlines():
        parts = line.split(None, 1)
        if len(parts) != 2:
            raise ControllerError(f"unparsable in-pod sha256sum output: {line!r}")
        digest, pod_path = parts
        manifest_path = BIN_PATH_MAP.get(pod_path.strip())
        if manifest_path is None:
            raise ControllerError(f"unexpected path in in-pod sha256sum output: {pod_path!r}")
        expected = manifest_binary_map.get(manifest_path)
        if expected != digest:
            raise ControllerError(
                f"in-pod sha256 mismatch for {pod_path}: expected {expected}, got {digest}"
            )
    print(f"verified {len(BIN_PATH_MAP)} binaries in pod against the build manifests")

    exec_sh(
        kubectl,
        pod,
        EXEC_READY,
        timeout=clamp_remaining_or_raise(_remaining_until(deadline), 30, "payload marker"),
    )
    print("payload marker /payload/.ready set")


def collect_results(kubectl: Kubectl, pod: str, result_dir: Path, pull_timeout: int) -> None:
    """Copy the benchmark results out of the running pod (mandatory).

    Raises ControllerError when a required result cannot be collected, so the
    run fails even if the benchmark reported status 0.
    """
    pulls = [
        ("/work/query-regression-work", result_dir / "query-regression-work"),
        ("/work/query-regression-summary.md", result_dir / "query-regression-summary.md"),
    ]
    for remote, local in pulls:
        if local.exists():
            if local.is_dir():
                shutil.rmtree(local)
            else:
                local.unlink()
        proc = kubectl.run(
            ["cp", f"{kubectl.pod_spec(pod)}:{remote}", str(local)],
            check=False,
            timeout=pull_timeout,
        )
        if proc.returncode != 0:
            raise ControllerError(
                f"could not collect required result {remote}: {proc.stderr.strip()[-500:]}"
            )
        print(f"collected {remote} -> {local}")


def validate_results(result_dir: Path) -> None:
    """Validate that required benchmark results exist and are well-formed.

    Raises ControllerError when a required file is missing or invalid; the run
    then fails even if the benchmark reported status 0.
    """
    summary = result_dir / "query-regression-summary.md"
    if not summary.is_file() or summary.stat().st_size == 0:
        raise ControllerError("required result query-regression-summary.md is missing or empty")
    work = result_dir / "query-regression-work"
    if not work.is_dir():
        raise ControllerError("required result directory query-regression-work is missing")
    reports = list(work.rglob("query-regression-report.json"))
    if not reports:
        raise ControllerError("no query-regression-report.json found under query-regression-work")
    for report in reports:
        try:
            data = json.loads(report.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as err:
            raise ControllerError(f"report is not valid JSON: {report}: {err}") from err
        if not isinstance(data, dict):
            raise ControllerError(f"report is not a JSON object: {report}")
    print(f"validated {len(reports)} report(s) and the summary")


def collect_logs(kubectl: Kubectl, pod: str, result_dir: Path, deadline: float) -> None:
    """Best-effort diagnostics (logs + describe), clamped to ``deadline``.

    Diagnostics never fail the run; once the deadline is reached they stop so
    the cleanup path keeps its budget.
    """
    result_dir.mkdir(parents=True, exist_ok=True)
    try:
        remaining = _remaining_until(deadline)
        if remaining <= 0:
            return
        logs = kubectl.run(
            ["logs", kubectl.pod_spec(pod), "--all-containers=true", "--prefix=true"],
            check=False,
            timeout=clamp_int(remaining, 300),
        )
        if logs.returncode == 0:
            (result_dir / "query-regression-pod.log").write_text(logs.stdout, encoding="utf-8")
            print(f"saved pod logs to {result_dir / 'query-regression-pod.log'}")
    except ControllerError as err:
        print(f"warning: log collection failed: {err}", file=sys.stderr)
    try:
        remaining = _remaining_until(deadline)
        if remaining <= 0:
            return
        describe = kubectl.run(
            ["describe", "pod", kubectl.pod_spec(pod)], check=False, timeout=clamp_int(remaining, 300)
        )
        if describe.returncode == 0:
            (result_dir / "query-regression-pod-describe.txt").write_text(
                describe.stdout, encoding="utf-8"
            )
            print(f"saved pod describe output to {result_dir / 'query-regression-pod-describe.txt'}")
    except ControllerError as err:
        print(f"warning: describe collection failed: {err}", file=sys.stderr)


def read_benchmark_status(kubectl: Kubectl, pod: str, timeout: int = 30) -> int:
    proc = exec_sh(
        kubectl, pod, EXEC_STATUS, check=False, timeout=timeout
    )
    text = proc.stdout.strip() if proc.returncode == 0 else ""
    if text.isdigit():
        return int(text)
    return 1


def wait_for_benchmark(
    kubectl: Kubectl, pod: str, deadline: float, result_dir: Path
) -> int:
    """Wait for /work/.done (or pod termination), clamped to ``deadline``.

    Returns the benchmark status (0 ok, non-zero failed). Required results are
    copied and validated while the container is still alive, before any
    deletion; collection/validation failures raise so the run fails even when
    the benchmark status was 0. Every poll, exec, sleep, and pull is clamped
    to the remaining budget.
    """
    while _now() < deadline:
        remaining = _remaining_until(deadline)
        phase = pod_phase(
            kubectl, pod, timeout=clamp_remaining_or_raise(remaining, 30, "benchmark pod phase poll")
        )
        if phase in ("Succeeded", "Failed"):
            print(f"job pod reached {phase} without a completion marker")
            return 1
        done = exec_sh(
            kubectl, pod, EXEC_DONE, check=False,
            timeout=clamp_remaining_or_raise(_remaining_until(deadline), 30, "completion marker check"),
        )
        if done.returncode == 0:
            status = read_benchmark_status(
                kubectl, pod,
                timeout=clamp_remaining_or_raise(_remaining_until(deadline), 30, "benchmark status read"),
            )
            print(f"benchmark done; status={status}")
            remaining = _remaining_until(deadline)
            if remaining <= 0:
                raise ControllerError("no time left in the run budget to collect results")
            collect_results(
                kubectl, pod, result_dir,
                pull_timeout=clamp_remaining_or_raise(remaining, 600, "result collection"),
            )
            validate_results(result_dir)
            exec_sh(
                kubectl, pod, EXEC_COLLECTED,
                timeout=clamp_remaining_or_raise(_remaining_until(deadline), 30, "collection marker"),
            )
            return status
        _sleep(min(10, _remaining_until(deadline)))
    print("timed out waiting for the benchmark to finish; collecting what exists")
    remaining = _remaining_until(deadline)
    if remaining > 0:
        collect_results(
            kubectl, pod, result_dir,
            pull_timeout=clamp_remaining_or_raise(remaining, 600, "result collection"),
        )
    validate_results(result_dir)
    return 1


def load_validation(path: Path | None) -> dict[str, Any]:
    """Load the validated values produced by the admission script."""
    if path is None:
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as err:
        raise ControllerError(f"could not read validated admission output {path}: {err}") from err
    if not isinstance(data, dict):
        raise ControllerError(f"validated admission output {path} is not a JSON object")
    return data


def write_github_output(status: int) -> None:
    path = os.environ.get("GITHUB_OUTPUT")
    if path:
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(f"status={status}\n")


def write_comment_metadata(path: Path, validation: dict[str, Any], status: int) -> None:
    """Regenerate the comment metadata from the validated values only.

    ``run_id``/``run_attempt`` are the *controller* workflow's own run
    identifiers (the comment workflow follows the controller run), while the
    source build run is recorded separately for provenance. Every other field
    comes from the admission output, never from the untrusted metadata
    artifact directly.
    """
    try:
        controller_run_id = int(os.environ["CONTROLLER_RUN_ID"])
        controller_run_attempt = int(os.environ["CONTROLLER_RUN_ATTEMPT"])
    except (KeyError, ValueError) as err:
        raise ControllerError(
            "CONTROLLER_RUN_ID/CONTROLLER_RUN_ATTEMPT must be positive integers "
            "to write the comment metadata"
        ) from err
    data = {
        "pr_number": validation.get("pr_number"),
        "head_sha": validation.get("head_sha"),
        "event_base_sha": validation.get("event_base_sha"),
        "built_base_sha": validation.get("built_base_sha"),
        "candidate_sha": validation.get("candidate_sha"),
        "head_repo": validation.get("head_repo"),
        "base_repo": validation.get("base_repo"),
        "label": validation.get("label"),
        "run_id": controller_run_id,
        "run_attempt": controller_run_attempt,
        "source_run_id": validation.get("source_run_id"),
        "source_run_attempt": validation.get("source_run_attempt"),
        "status": status,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, sort_keys=True) + "\n", encoding="utf-8")
    print(f"wrote regenerated comment metadata to {path}")


def run_cleanup(
    lifecycle: Lifecycle,
    kubectl: Kubectl,
    job_name: str,
    pod: str | None,
    result_dir: Path,
    cancelled: bool,
    delete_attempts: int,
    delete_timeout: int,
) -> None:
    """Cleanup path: skip diagnostics when cancelled or in the reserve, then
    delete the exact Job (short bounded deletion on cancellation).

    On cancellation the optional diagnostics/collection are skipped entirely
    and a short bounded exact Job deletion/absence check runs immediately.
    Otherwise diagnostics run only before ``cleanup_begins``, and deletion
    clamps to the hard deadline inside the reserve.
    """
    if cancelled:
        delete_deadline = _now() + CANCELLATION_DELETE_BUDGET
    else:
        if pod is not None and not lifecycle.in_cleanup_reserve():
            collect_logs(kubectl, pod, result_dir, deadline=lifecycle.cleanup_begins)
        delete_deadline = lifecycle.hard_deadline
    delete_job(
        kubectl, job_name, delete_deadline, attempts=delete_attempts, delete_timeout=delete_timeout
    )
    print(f"cleanup complete for job {job_name}")


def run_cleanup_only(
    kubectl: Kubectl, job_name: str, delete_attempts: int, delete_timeout: int
) -> int:
    """Second-process cleanup: delete only the exact deterministic Job.

    Used by the workflow's ``if: always()`` cleanup step so a second process
    retries deletion when the controller step failed without cancelling the
    run. Exact-name validation is identical to the controller path.
    """
    deadline = _now() + CLEANUP_ONLY_BUDGET
    try:
        delete_job(kubectl, job_name, deadline, attempts=delete_attempts, delete_timeout=delete_timeout)
    except ControllerError as err:
        print(f"error: {err}", file=sys.stderr)
        return 1
    print(f"cleanup-only complete for job {job_name}")
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create/drive/clean up the one-shot ACK query-regression benchmark Job."
    )
    parser.add_argument(
        "--run-id",
        default=os.environ.get("SOURCE_RUN_ID", ""),
        help="Originating build run id (validated by the admission step).",
    )
    parser.add_argument(
        "--run-attempt",
        default=os.environ.get("SOURCE_RUN_ATTEMPT", "1"),
        help="Originating build run attempt (validated by the admission step).",
    )
    parser.add_argument(
        "--validation-file",
        type=Path,
        default=None,
        help="Validated admission output JSON (built/candidate SHAs, run ids, artifact ids).",
    )
    parser.add_argument(
        "--kubeconfig",
        default=os.environ.get("ACK_KUBECONFIG_PATH", ""),
        help="Runner-local kubeconfig path using an exec credential plugin (never a secret).",
    )
    parser.add_argument(
        "--trusted-source-sha",
        default=os.environ.get("TRUSTED_SOURCE_SHA", ""),
        help="Default-branch HEAD SHA the trusted scripts were checked out at.",
    )
    parser.add_argument("--base-artifact-dir", type=Path, default=None)
    parser.add_argument("--candidate-artifact-dir", type=Path, default=None)
    parser.add_argument("--trusted-summary", type=Path, default=None)
    parser.add_argument("--trusted-scripts-manifest", type=Path, default=None)
    parser.add_argument("--payload-dir", type=Path, default=None)
    parser.add_argument("--result-dir", type=Path, default=Path("."))
    parser.add_argument(
        "--comment-metadata",
        type=Path,
        default=None,
        help="Write the regenerated comment metadata (query-regression-pr.json) here.",
    )
    parser.add_argument("--kubectl", default=os.environ.get("KUBECTL", "kubectl"))
    parser.add_argument("--pod-ready-timeout", type=int, default=POD_READY_TIMEOUT_DEFAULT)
    parser.add_argument("--run-timeout", type=int, default=RUN_TIMEOUT_DEFAULT)
    parser.add_argument("--delete-timeout", type=int, default=DELETE_TIMEOUT_DEFAULT)
    parser.add_argument("--delete-attempts", type=int, default=DELETE_ATTEMPTS_DEFAULT)
    parser.add_argument("--lifecycle-timeout", type=int, default=LIFECYCLE_TIMEOUT_DEFAULT)
    parser.add_argument("--cleanup-reserve", type=int, default=CLEANUP_RESERVE_DEFAULT)
    parser.add_argument(
        "--payload-bytes-cap",
        type=int,
        default=PAYLOAD_BYTES_CAP_DEFAULT,
        help="Pre-Job payload byte cap (default 2 GiB, under the 40Gi ephemeral limit).",
    )
    parser.add_argument(
        "--cleanup-only",
        action="store_true",
        help="Delete only the exact deterministic Job for this run, then exit.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Render and validate the Job manifest without contacting any cluster.",
    )
    return parser.parse_args(argv)


def verify_trusted_scripts(
    trusted_summary: Path, manifest_path: Path | None, expected_source_sha: str
) -> None:
    """Verify the trusted summary script against the restore manifest.

    The restore manifest is written by the workflow from the default-branch
    checkout (source_sha = the checked-out default-branch HEAD); this fails
    closed if the summary the controller is about to embed into the payload
    does not match it.
    """
    if trusted_summary.is_symlink() or not trusted_summary.is_file():
        raise ControllerError(f"trusted summary script is missing or not a regular file: {trusted_summary}")
    if manifest_path is None:
        raise ControllerError("--trusted-scripts-manifest is required for a real run")
    manifest = load_manifest(manifest_path.parent, manifest_path.name)
    if str(manifest.get("source_sha", "")).lower() != expected_source_sha.lower():
        raise ControllerError(
            f"trusted scripts manifest source {manifest.get('source_sha')} does not match "
            f"the trusted source SHA {expected_source_sha}"
        )
    files = manifest.get("files")
    if not isinstance(files, dict) or "query-regression-summary.py" not in files:
        raise ControllerError("trusted scripts manifest has no query-regression-summary.py entry")
    digest = hashlib_sha256(trusted_summary)
    if files["query-regression-summary.py"] != digest:
        raise ControllerError(
            f"trusted summary script sha256 {digest} does not match the restore manifest"
        )
    print("trusted summary script verified against the restore manifest")


def hashlib_sha256(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


# Set by the signal handler; read by main()'s finally to choose the short
# cancellation cleanup path.
_CANCELLED: dict[str, bool] = {"flag": False}


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        run_id = validate_run_id(args.run_id)
        run_attempt = validate_run_id(args.run_attempt)
        job_name = validate_job_name(build_job_name(run_id, run_attempt))
        validation = load_validation(args.validation_file)
        if validation:
            if validation.get("source_run_id") != run_id:
                raise ControllerError(
                    f"validated source run id {validation.get('source_run_id')} does not match "
                    f"--run-id {run_id}"
                )
            if validation.get("source_run_attempt") != run_attempt:
                raise ControllerError(
                    f"validated source run attempt {validation.get('source_run_attempt')} does not "
                    f"match --run-attempt {run_attempt}"
                )
        lifecycle = Lifecycle(args.lifecycle_timeout, args.cleanup_reserve)

        # The kubeconfig is validated before any cluster call in every mode
        # that touches the cluster (main run and second-process cleanup).
        kubeconfig = args.kubeconfig or None
        if not args.dry_run:
            if not kubeconfig:
                raise ControllerError(
                    "a runner-local kubeconfig path is required (ACK_KUBECONFIG_PATH or "
                    "--kubeconfig); it must use an exec credential plugin and is never a secret"
                )
            validate_kubeconfig(Path(kubeconfig))

        # Second-process cleanup needs no SHAs or payload: delete only the
        # exact deterministic Job (validated above).
        if args.cleanup_only:
            kubectl = Kubectl(args.kubectl, kubeconfig)
            return run_cleanup_only(kubectl, job_name, args.delete_attempts, args.delete_timeout)

        base_sha = str(validation.get("built_base_sha") or os.environ.get("VERIFIED_BASE_SHA", ""))
        candidate_sha = str(
            validation.get("candidate_sha") or os.environ.get("VERIFIED_CANDIDATE_SHA", "")
        )
        for sha in (base_sha, candidate_sha):
            if not re.match(r"^[0-9a-f]{40}$", sha):
                raise ControllerError(
                    f"verified SHAs must be full 40-hex digests from the admission output: {sha!r}"
                )

        # Informational pod env: base/candidate refs default to the validated
        # SHAs (display only; the pod never uses them for admission).
        os.environ.setdefault("BASE_REF", base_sha)
        os.environ.setdefault("CANDIDATE_REF", candidate_sha)

        manifest = build_manifest(
            run_id=run_id,
            run_attempt=run_attempt,
            job_name=job_name,
            base_sha=base_sha,
            candidate_sha=candidate_sha,
        )

        if args.dry_run:
            print(json.dumps(manifest, indent=2, sort_keys=True))
            return 0

        kubectl = Kubectl(args.kubectl, kubeconfig)

        required = {
            "--base-artifact-dir": args.base_artifact_dir,
            "--candidate-artifact-dir": args.candidate_artifact_dir,
            "--trusted-summary": args.trusted_summary,
            "--trusted-scripts-manifest": args.trusted_scripts_manifest,
            "--payload-dir": args.payload_dir,
        }
        missing = [name for name, value in required.items() if value is None]
        if missing:
            raise ControllerError(f"missing required arguments: {', '.join(missing)}")

        # Manifests must agree with the validated SHAs from the admission output.
        base_manifest = load_manifest(args.base_artifact_dir, "base-manifest.json")
        candidate_manifest = load_manifest(args.candidate_artifact_dir, "candidate-manifest.json")
        if str(base_manifest.get("base_sha", "")).lower() != base_sha.lower():
            raise ControllerError(
                f"base-manifest base_sha {base_manifest.get('base_sha')} does not match "
                f"the validated base SHA {base_sha}"
            )
        if str(candidate_manifest.get("candidate_sha", "")).lower() != candidate_sha.lower():
            raise ControllerError(
                f"candidate-manifest candidate_sha {candidate_manifest.get('candidate_sha')} "
                f"does not match the validated candidate SHA {candidate_sha}"
            )
        manifest_binaries = list(base_manifest.get("binaries", [])) + list(
            candidate_manifest.get("binaries", [])
        )
        if not manifest_binaries:
            raise ControllerError("base/candidate manifests contain no binaries")

        verify_trusted_scripts(
            args.trusted_summary, args.trusted_scripts_manifest, args.trusted_source_sha
        )

        payload_dir = assemble_payload(
            args.base_artifact_dir,
            args.candidate_artifact_dir,
            args.trusted_summary,
            args.payload_dir,
        )
        print(f"payload assembled at {payload_dir}")
        measure_payload(payload_dir, args.payload_bytes_cap)

        status = 1
        cleanup_ok = True
        pod: str | None = None
        try:
            # Normal-phase cluster operations must never run on an exhausted
            # budget: a 1-second clamped call could still introduce/retain
            # cloud compute with no time left to finish or clean up. The guard
            # before each operation also guarantees no Job is created or
            # reconciled after cleanup_begins.
            lifecycle.require_phase(what="preflight job lookup")
            if job_exists(
                kubectl, job_name, timeout=lifecycle.phase_clamp(60, what="preflight job lookup")
            ):
                print(f"job {job_name} already exists; deleting the previous run (exact name)")
                lifecycle.require_phase(what="leaked-job reconcile deletion")
                delete_job(
                    kubectl,
                    job_name,
                    lifecycle.phase_deadline(None),
                    attempts=args.delete_attempts,
                    delete_timeout=args.delete_timeout,
                )
            lifecycle.require_phase(what="Job creation")
            create_job(kubectl, manifest, timeout=lifecycle.phase_clamp(120, what="Job creation"))
            print(f"created job {job_name} in namespace {kubectl.namespace}")

            pod = wait_for_pod(kubectl, job_name, lifecycle.phase_deadline(args.pod_ready_timeout))
            transfer_payload(
                kubectl,
                pod,
                payload_dir,
                manifest_binaries,
                lifecycle.phase_deadline(PAYLOAD_TRANSFER_ALLOWANCE),
            )
            status = wait_for_benchmark(
                kubectl, pod, lifecycle.phase_deadline(args.run_timeout), args.result_dir
            )
        except Exception as err:  # noqa: BLE001 - report and always clean up
            print(f"controller error: {err}", file=sys.stderr)
            status = 1
            if pod is None:
                pod = get_pod_name(kubectl, job_name, timeout=lifecycle.clamp(60))
        finally:
            cancelled = _CANCELLED["flag"]
            try:
                run_cleanup(
                    lifecycle,
                    kubectl,
                    job_name,
                    pod,
                    args.result_dir,
                    cancelled,
                    args.delete_attempts,
                    args.delete_timeout,
                )
            except Exception as err:  # noqa: BLE001 - failure must fail the run
                cleanup_ok = False
                print(
                    f"error: job deletion could not be confirmed: {err}; the workflow will fail",
                    file=sys.stderr,
                )

        if not cleanup_ok and status == 0:
            status = 1
        if args.comment_metadata is not None and validation.get("event") == "pull_request":
            write_comment_metadata(args.comment_metadata, validation, status)
        write_github_output(status)
        print(f"query regression status: {status}")
        return 0
    except ControllerError as err:
        print(f"error: {err}", file=sys.stderr)
        return 1


def _handle_termination(signum: int, frame: Any) -> None:
    # Cancellation path: mark the run cancelled so main()'s finally skips
    # optional diagnostics/collection and immediately performs a short bounded
    # exact Job deletion/absence check. A hard SIGKILL (GitHub hard
    # cancellation) cannot run this handler; recovery is documented in the
    # README (deterministic exact-name deletion, TTL, independent reaper/manual).
    _CANCELLED["flag"] = True
    raise SystemExit(
        f"query-regression-ack-controller: received signal {signum}; cleaning up (cancellation path)"
    )


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, _handle_termination)
    signal.signal(signal.SIGINT, _handle_termination)
    raise SystemExit(main())
