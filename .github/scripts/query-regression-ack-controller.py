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

This script is trusted base/default-branch code: the controller job restores
it (and the other trusted scripts) from the verified base commit via `git
show` into a fresh ``$RUNNER_TEMP`` directory, and only that restored copy is
executed. A PR cannot replace it, and it is never taken from the build
artifact.

Trust boundaries (v1, pragmatic):

* The controller job runs on the trusted local self-hosted runner
  (`perf-regression-8-cores`) which holds the GitHub runner token and the ACK
  kubeconfig. It never executes candidate payload locally: candidate binaries,
  scripts, and cases are copied into the ACK Job pod and run only there.
* Base and candidate binaries travel in two separate immutable artifacts. The
  base artifact is uploaded *before* any candidate-controlled build step runs,
  so candidate build scripts cannot overwrite the base binary. Both artifacts
  are downloaded by exact artifact ID (build job outputs) and their manifests
  are cross-validated against the verified SHAs; the candidate checkout HEAD
  must equal the verified candidate SHA.
* All candidate-controlled payload paths (the driver script and the
  recursive tests/perf tree) are scanned before copying: every symlink in any
  path component from the candidate root down (ancestor symlinks included) and
  every non-regular file (FIFO, socket, device) is rejected, sources are
  resolved, and containment under the resolved candidate checkout root is
  enforced. A candidate symlink is never dereferenced.
* The benchmark Job pod has `automountServiceAccountToken: false`, no GitHub /
  cloud / kube credentials, no secrets, non-root UID/GID 1001, seccomp
  RuntimeDefault, no privilege escalation, and all capabilities dropped. It
  runs the digest-pinned runtime image on the dedicated
  `alibabacloud.com/nodepool-id=npb5ff93bea3a447a698fe31ebc997ea31` bulk pool
  (which must carry the `dedicated=perf-regression:NoSchedule` taint). The
  ACK pod runs only the performance driver and its required helpers; no
  tooling/unit tests run on ACK.
* Nodepool scaling is implicit only: the Pending Job pod is the scale-up
  signal for ACK Cluster Autoscaler and foreground deletion of the Job/pod is
  the scale-down signal. This controller never calls any nodepool API.
* The Job name is deterministic per run: `query-regression-<run-id>-<attempt>`.
  Deletion is exact-name validated against that pattern, retried with a
  bounded number of attempts, and the Job's absence is verified before the
  controller reports success. If deletion cannot be confirmed the workflow
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
  recovery plus the residual need for an independent reaper or manual cleanup.

Future hardening (documented, not implemented here): replace `kubectl cp` /
exec-gated markers with an init container + signed object-storage upload so the
cluster RBAC no longer needs `pods/exec`.
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
from typing import Any, Callable

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
# GitHub run identifiers, so the name is always lowercase alphanumeric plus
# dashes, within the 63-char label/name segment limit.
JOB_NAME_RE = re.compile(r"^query-regression-[1-9][0-9]*-[1-9][0-9]*$")

DEFAULT_NAMESPACE = "arc-runners"

# The ACK bulk-ingestion nodepool (Hangzhou cluster bulk-ingestion-test). The
# nodepool taint dedicated=perf-regression:NoSchedule must be present.
NODEPOOL_SELECTOR = {"alibabacloud.com/nodepool-id": "npb5ff93bea3a447a698fe31ebc997ea31"}
NODEPOOL_TAINT = {
    "key": "dedicated",
    "operator": "Equal",
    "value": "perf-regression",
    "effect": "NoSchedule",
}

# Digest-pinned runtime image (same image the ARC runner scale sets used; no
# new image is built or pushed by this change).
DEFAULT_IMAGE = (
    "greptime-registry.cn-hangzhou.cr.aliyuncs.com/greptime/"
    "greptimedb-query-regression-runner@sha256:"
    "e713b294e23b7e15184e558866c90025e59930033e72c97650dbc7f1ca022d11"
)

# One global monotonic lifecycle budget. The controller job timeout is
# WORKFLOW_JOB_TIMEOUT_SECONDS (180 min) and must strictly exceed the
# controller lifecycle LIFECYCLE_TIMEOUT_DEFAULT (150 min) by at least
# SETUP_AND_OUTER_CLEANUP_MARGIN_MINIMUM (25 min) so that checkout, trusted
# script restore, artifact download/attestation, kubeconfig setup, and the
# second-process cleanup-only step all fit inside the job. Normal phases
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
# controller's own budgets are all shorter.
ACTIVE_DEADLINE_SECONDS = 10800

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

# Trusted scripts restored by the workflow from the verified base commit; the
# controller verifies the summary script against the recorded manifest.
TRUSTED_SCRIPT_NAMES = [
    "query-regression-ack-controller.py",
    "query-regression-summary.py",
    "query-regression-pr-metadata.py",
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
# query-regression-ack-controller.py (base-branch code). Executes only inside
# the one-shot ACK benchmark Job pod, never on the credentialed runner.
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
    namespace: str,
    image: str,
    base_sha: str,
    candidate_sha: str,
) -> dict[str, Any]:
    if "@sha256:" not in image:
        raise ControllerError(
            f"benchmark Job image must be digest-pinned (contain '@sha256:'): {image!r}"
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
            "namespace": namespace,
            "labels": labels,
            "annotations": annotations,
        },
        "spec": {
            "backoffLimit": 0,
            "completions": 1,
            "parallelism": 1,
            "activeDeadlineSeconds": ACTIVE_DEADLINE_SECONDS,
            "template": {
                "metadata": {"labels": labels},
                "spec": {
                    "automountServiceAccountToken": False,
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
                            "image": image,
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
        raise ControllerError(f"{label} is not under the candidate checkout root: {path}")
    cursor = root
    for part in rel.parts:
        cursor = cursor / part
        if cursor.is_symlink():
            raise ControllerError(
                f"{label} has a symlink component (ancestor of {path}), refusing to copy: {cursor}"
            )
    resolved = path.resolve()
    if not resolved.is_relative_to(root_resolved):
        raise ControllerError(f"{label} escapes the candidate checkout root: {resolved}")
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
    candidate_src: Path,
    trusted_summary: Path,
    payload_dir: Path,
) -> Path:
    """Assemble the payload directory that is `kubectl cp`'d into the pod.

    Content:
      bins/...                     base + candidate binaries (two artifacts)
      repo/tests/perf/**           candidate case files (symlink-free, contained)
      repo/.github/scripts/query-regression-run.py   candidate driver (PR content)
      repo/.github/scripts/query-regression-summary.py  TRUSTED (verified base commit)
      run.sh                       generated from the trusted template

    The controller only copies these files; candidate content is executed
    exclusively inside the ACK pod.
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
    # (no symlinks remain, so nothing is dereferenced).
    tests_perf = resolve_contained(
        candidate_src / "tests" / "perf", candidate_src, "candidate tests/perf"
    )
    if not tests_perf.is_dir():
        raise ControllerError(f"candidate checkout has no tests/perf directory: {tests_perf}")
    reject_non_regular(tests_perf, "candidate tests/perf")
    shutil.copytree(tests_perf, payload_dir / "repo" / "tests" / "perf", symlinks=False)

    # Candidate-controlled driver: resolve, enforce containment, reject
    # symlink components, and require a regular file.
    driver = resolve_contained(
        candidate_src / ".github" / "scripts" / "query-regression-run.py",
        candidate_src,
        "candidate driver",
    )
    if not driver.is_file():
        raise ControllerError(f"candidate driver is not a regular file: {driver}")
    driver_dst = payload_dir / "repo" / ".github" / "scripts" / "query-regression-run.py"
    driver_dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(driver, driver_dst)

    # Trusted summary script: comes from the verified base commit via the
    # workflow's restore step, never from the build artifact or the candidate.
    if trusted_summary.is_symlink() or not trusted_summary.is_file():
        raise ControllerError(f"trusted summary script is missing or not a regular file: {trusted_summary}")
    summary_dst = payload_dir / "repo" / ".github" / "scripts" / "query-regression-summary.py"
    shutil.copy2(trusted_summary, summary_dst)

    run_sh = payload_dir / "run.sh"
    run_sh.write_text(RUN_SH_TEMPLATE, encoding="utf-8")
    run_sh.chmod(0o755)
    return payload_dir


# ---------------------------------------------------------------------------
# kubectl plumbing
# ---------------------------------------------------------------------------

class Kubectl:
    def __init__(self, binary: str, namespace: str, kubeconfig: str | None, context: str | None):
        if shutil.which(binary) is None:
            raise ControllerError(
                f"kubectl binary not found: {binary!r}. Install kubectl on the "
                f"perf-regression-8-cores runner (see the query regression README)."
            )
        self.binary = binary
        self.namespace = namespace
        self.kubeconfig = kubeconfig
        self.context = context

    def _env(self) -> dict[str, str]:
        env = dict(os.environ)
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
        cmd = [self.binary]
        if self.context:
            cmd += ["--context", self.context]
        cmd += args
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

def job_exists(kubectl: Kubectl, job_name: str, timeout: int = 180) -> bool:
    validate_job_name(job_name)
    proc = kubectl.run(
        ["get", "job", job_name, "-n", kubectl.namespace, "-o", "name"],
        check=False,
        timeout=timeout,
    )
    return proc.returncode == 0 and "job.batch/" in proc.stdout


def job_absent(kubectl: Kubectl, job_name: str, timeout: int = 180) -> bool:
    return not job_exists(kubectl, job_name, timeout=timeout)


def delete_job(
    kubectl: Kubectl,
    job_name: str,
    deadline: float,
    attempts: int = DELETE_ATTEMPTS_DEFAULT,
    delete_timeout: int = DELETE_TIMEOUT_DEFAULT,
) -> None:
    """Delete the exact Job with bounded retries and verify its absence.

    Every operation is clamped to ``deadline``; when the budget is exhausted
    the attempt loop stops and a ControllerError is raised so the caller fails
    the run even if the benchmark itself reported status 0.
    """
    validate_job_name(job_name)
    last_error: str | None = None
    for attempt in range(1, attempts + 1):
        remaining = _remaining_until(deadline)
        if remaining <= 0:
            raise ControllerError("cleanup budget exhausted before deletion could be confirmed")
        if job_absent(kubectl, job_name, timeout=clamp_int(remaining, 30)):
            print(f"job {job_name} already gone; nothing to delete")
            return
        remaining = _remaining_until(deadline)
        flag = clamp_int(remaining, delete_timeout)
        proc = kubectl.run(
            [
                "delete", "job", job_name, "-n", kubectl.namespace,
                "--cascade=foreground", "--wait=true", f"--timeout={flag}s",
            ],
            check=False,
            timeout=clamp_int(_remaining_until(deadline), flag + 60),
        )
        if proc.returncode == 0:
            last_error = None
            break
        if "not found" in proc.stderr:
            print(f"job {job_name} already gone; nothing to delete")
            return
        last_error = proc.stderr.strip()[-1000:]
        print(
            f"delete attempt {attempt}/{attempts} failed for {job_name}: {last_error}",
            file=sys.stderr,
        )
        if attempt < attempts:
            _sleep(min(DELETE_RETRY_SLEEP, _remaining_until(deadline)))
    if last_error is not None:
        raise ControllerError(
            f"failed to delete job {job_name} after {attempts} attempts: {last_error}"
        )
    if not job_absent(kubectl, job_name, timeout=clamp_int(_remaining_until(deadline), 30)):
        raise ControllerError(
            f"job {job_name} still present after foreground delete; deletion not confirmed"
        )
    print(f"job {job_name} deleted and confirmed absent")


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
        "test -f /payload/run.sh && test -d /payload/bins/base && "
        "test -d /payload/bins/candidate && test -d /payload/repo/tests/perf/query_cases",
        timeout=clamp_remaining_or_raise(_remaining_until(deadline), 60, "payload layout check"),
    )

    exec_sh(
        kubectl,
        pod,
        "chmod +x /payload/run.sh /payload/bins/base/greptime "
        "/payload/bins/candidate/greptime /payload/bins/candidate/query_perf_fixture "
        "/payload/bins/candidate/query_regression_runner",
        timeout=clamp_remaining_or_raise(_remaining_until(deadline), 60, "payload chmod"),
    )

    # Verify the binaries inside the pod against the base/candidate manifests.
    proc = exec_sh(
        kubectl,
        pod,
        "sha256sum " + " ".join(BIN_PATH_MAP),
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
        "touch /payload/.ready",
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
        kubectl, pod, "cat /work/benchmark-status 2>/dev/null || true", check=False, timeout=timeout
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
            kubectl, pod, "test -f /work/.done", check=False,
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
                kubectl, pod, "touch /work/.collected",
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


def verify_checkout_sha(candidate_src: Path, expected_sha: str) -> None:
    """Fail closed unless the candidate checkout HEAD is the verified SHA."""
    try:
        proc = subprocess.run(
            ["git", "-C", str(candidate_src), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=30,
        )
    except subprocess.TimeoutExpired as err:
        raise ControllerError(f"git rev-parse timed out for {candidate_src}") from err
    if proc.returncode != 0:
        raise ControllerError(
            f"could not resolve candidate checkout HEAD at {candidate_src}: {proc.stderr.strip()[-500:]}"
        )
    actual = proc.stdout.strip().lower()
    if actual != expected_sha.lower():
        raise ControllerError(
            f"candidate checkout HEAD {actual} does not match the verified candidate SHA {expected_sha}"
        )
    print(f"candidate checkout HEAD matches the verified candidate SHA {expected_sha}")


def write_github_output(status: int) -> None:
    path = os.environ.get("GITHUB_OUTPUT")
    if path:
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(f"status={status}\n")


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
    parser.add_argument("--run-id", default=os.environ.get("RUN_ID", ""))
    parser.add_argument("--run-attempt", default=os.environ.get("RUN_ATTEMPT", "1"))
    parser.add_argument("--namespace", default=os.environ.get("ACK_JOB_NAMESPACE", DEFAULT_NAMESPACE))
    parser.add_argument("--image", default=os.environ.get("JOB_IMAGE", ""))
    parser.add_argument("--base-artifact-dir", type=Path, default=None)
    parser.add_argument("--candidate-artifact-dir", type=Path, default=None)
    parser.add_argument("--candidate-src", type=Path, default=None)
    parser.add_argument("--trusted-summary", type=Path, default=None)
    parser.add_argument("--trusted-scripts-manifest", type=Path, default=None)
    parser.add_argument("--payload-dir", type=Path, default=None)
    parser.add_argument("--result-dir", type=Path, default=Path("."))
    parser.add_argument("--kubectl", default=os.environ.get("KUBECTL", "kubectl"))
    parser.add_argument("--kubeconfig", default=os.environ.get("KUBECONFIG"))
    parser.add_argument("--context", default=os.environ.get("KUBECTL_CONTEXT"))
    parser.add_argument("--pod-ready-timeout", type=int, default=POD_READY_TIMEOUT_DEFAULT)
    parser.add_argument("--run-timeout", type=int, default=RUN_TIMEOUT_DEFAULT)
    parser.add_argument("--delete-timeout", type=int, default=DELETE_TIMEOUT_DEFAULT)
    parser.add_argument("--delete-attempts", type=int, default=DELETE_ATTEMPTS_DEFAULT)
    parser.add_argument("--lifecycle-timeout", type=int, default=LIFECYCLE_TIMEOUT_DEFAULT)
    parser.add_argument("--cleanup-reserve", type=int, default=CLEANUP_RESERVE_DEFAULT)
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


def verify_trusted_scripts(trusted_summary: Path, manifest_path: Path | None, expected_source_sha: str) -> None:
    """Verify the trusted summary script against the restore manifest.

    The restore manifest is written by the workflow's restore step from the
    verified base commit; this fails closed if the summary the controller is
    about to embed into the payload does not match it.
    """
    if trusted_summary.is_symlink() or not trusted_summary.is_file():
        raise ControllerError(f"trusted summary script is missing or not a regular file: {trusted_summary}")
    if manifest_path is None:
        raise ControllerError("--trusted-scripts-manifest is required for a real run")
    manifest = load_manifest(manifest_path.parent, manifest_path.name)
    if str(manifest.get("source_sha", "")).lower() != expected_source_sha.lower():
        raise ControllerError(
            f"trusted scripts manifest source {manifest.get('source_sha')} does not match "
            f"the verified base SHA {expected_source_sha}"
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
        image = args.image or DEFAULT_IMAGE
        lifecycle = Lifecycle(args.lifecycle_timeout, args.cleanup_reserve)

        # Second-process cleanup needs no SHAs or payload: delete only the
        # exact deterministic Job (validated above).
        if args.cleanup_only:
            kubectl = Kubectl(args.kubectl, args.namespace, args.kubeconfig, args.context)
            return run_cleanup_only(kubectl, job_name, args.delete_attempts, args.delete_timeout)

        base_sha = os.environ.get("VERIFIED_BASE_SHA", "")
        candidate_sha = os.environ.get("VERIFIED_CANDIDATE_SHA", "")
        for sha in (base_sha, candidate_sha):
            if not re.match(r"^[0-9a-f]{40}$", sha):
                raise ControllerError(
                    f"verified SHAs must be full 40-hex digests from the build job outputs: {sha!r}"
                )

        manifest = build_manifest(
            run_id=run_id,
            run_attempt=run_attempt,
            job_name=job_name,
            namespace=args.namespace,
            image=image,
            base_sha=base_sha,
            candidate_sha=candidate_sha,
        )

        if args.dry_run:
            print(json.dumps(manifest, indent=2, sort_keys=True))
            return 0

        kubectl = Kubectl(args.kubectl, args.namespace, args.kubeconfig, args.context)

        required = {
            "--base-artifact-dir": args.base_artifact_dir,
            "--candidate-artifact-dir": args.candidate_artifact_dir,
            "--candidate-src": args.candidate_src,
            "--trusted-summary": args.trusted_summary,
            "--trusted-scripts-manifest": args.trusted_scripts_manifest,
            "--payload-dir": args.payload_dir,
        }
        missing = [name for name, value in required.items() if value is None]
        if missing:
            raise ControllerError(f"missing required arguments: {', '.join(missing)}")

        # Manifests must agree with the verified SHAs from the build job outputs.
        base_manifest = load_manifest(args.base_artifact_dir, "base-manifest.json")
        candidate_manifest = load_manifest(args.candidate_artifact_dir, "candidate-manifest.json")
        if str(base_manifest.get("base_sha", "")).lower() != base_sha.lower():
            raise ControllerError(
                f"base-manifest base_sha {base_manifest.get('base_sha')} does not match "
                f"the verified base SHA {base_sha}"
            )
        if str(candidate_manifest.get("candidate_sha", "")).lower() != candidate_sha.lower():
            raise ControllerError(
                f"candidate-manifest candidate_sha {candidate_manifest.get('candidate_sha')} "
                f"does not match the verified candidate SHA {candidate_sha}"
            )
        manifest_binaries = list(base_manifest.get("binaries", [])) + list(
            candidate_manifest.get("binaries", [])
        )
        if not manifest_binaries:
            raise ControllerError("base/candidate manifests contain no binaries")

        verify_checkout_sha(args.candidate_src, candidate_sha)
        verify_trusted_scripts(args.trusted_summary, args.trusted_scripts_manifest, base_sha)

        payload_dir = assemble_payload(
            args.base_artifact_dir,
            args.candidate_artifact_dir,
            args.candidate_src,
            args.trusted_summary,
            args.payload_dir,
        )
        print(f"payload assembled at {payload_dir}")

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
    # README (deterministic exact-name deletion, independent reaper/manual).
    _CANCELLED["flag"] = True
    raise SystemExit(
        f"query-regression-ack-controller: received signal {signum}; cleaning up (cancellation path)"
    )


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, _handle_termination)
    signal.signal(signal.SIGINT, _handle_termination)
    raise SystemExit(main())
