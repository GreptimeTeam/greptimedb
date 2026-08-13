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

"""Focused tests for the trusted query-regression ACK controller."""

import hashlib
import importlib.util
import io
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from typing import Callable
from unittest import mock

SCRIPT_DIR = Path(__file__).resolve().parent
CONTROLLER_PATH = SCRIPT_DIR / "query-regression-ack-controller.py"
SPEC = importlib.util.spec_from_file_location("query_regression_ack_controller_under_test", CONTROLLER_PATH)
assert SPEC is not None and SPEC.loader is not None
controller = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = controller
SPEC.loader.exec_module(controller)

try:  # PyYAML is optional; workflow structural tests skip when unavailable.
    import yaml
except ImportError:  # pragma: no cover - depends on the environment
    yaml = None

BASE_SHA = "a" * 40


def render_manifest() -> dict:
    return controller.build_manifest(
        run_id=123456789,
        run_attempt=2,
        job_name="query-regression-123456789-2",
        namespace="arc-runners",
        image=controller.DEFAULT_IMAGE,
        base_sha=BASE_SHA,
        candidate_sha="b" * 40,
    )


class JobNameTest(unittest.TestCase):
    def test_deterministic_name(self) -> None:
        self.assertEqual(controller.build_job_name(123, 1), "query-regression-123-1")
        self.assertEqual(controller.build_job_name(987654321, 12), "query-regression-987654321-12")

    def test_validate_job_name_accepts_pattern(self) -> None:
        for name in ("query-regression-1-1", "query-regression-1234567890-99"):
            self.assertEqual(controller.validate_job_name(name), name)

    def test_validate_job_name_rejects_anything_else(self) -> None:
        for name in (
            "query-regression-0-1",          # zero run id
            "query-regression-1-0",          # zero attempt
            "query-regression-abc-1",        # non-numeric
            "query-regression-1",            # missing attempt
            "other-job-1-1",                 # wrong prefix
            "query-regression-1-1-extra",    # extra suffix
            "query_regression_1_1",          # underscores
            "Query-Regression-1-1",          # uppercase
            "",                              # empty
        ):
            with self.assertRaises(controller.ControllerError, msg=name):
                controller.validate_job_name(name)

    def test_run_id_validation(self) -> None:
        self.assertEqual(controller.validate_run_id("42"), 42)
        for bad in ("0", "-1", "abc", "1.5", ""):
            with self.assertRaises(controller.ControllerError, msg=bad):
                controller.validate_run_id(bad)


class ManifestSecurityTest(unittest.TestCase):
    def test_nodepool_pin_and_taint(self) -> None:
        spec = render_manifest()["spec"]["template"]["spec"]
        self.assertEqual(
            spec["nodeSelector"],
            {"alibabacloud.com/nodepool-id": "npb5ff93bea3a447a698fe31ebc997ea31"},
        )
        self.assertIn(
            {
                "key": "dedicated",
                "operator": "Equal",
                "value": "perf-regression",
                "effect": "NoSchedule",
            },
            spec["tolerations"],
        )

    def test_no_credentials_or_secrets(self) -> None:
        spec = render_manifest()["spec"]["template"]["spec"]
        self.assertIs(spec["automountServiceAccountToken"], False)
        for volume in spec["volumes"]:
            self.assertNotIn("secret", volume)
            self.assertNotIn("hostPath", volume)
            self.assertIn("emptyDir", volume)
        env = spec["containers"][0]["env"]
        names = [entry["name"] for entry in env]
        for name in names:
            lowered = name.lower()
            for banned in ("kube", "token", "secret", "cloud", "aws", "azure", "credential"):
                self.assertNotIn(banned, lowered, f"env var {name} could carry credentials")
        # GITHUB_OUTPUT is the standard Actions output-path variable, not a token.
        self.assertNotIn("GITHUB_TOKEN", names)
        self.assertNotIn("KUBECONFIG", names)
        self.assertNotIn("ACK_KUBECONFIG", names)

    def test_hardened_pod_security_context(self) -> None:
        spec = render_manifest()["spec"]["template"]["spec"]
        self.assertEqual(
            spec["securityContext"],
            {
                "runAsNonRoot": True,
                "runAsUser": 1001,
                "runAsGroup": 1001,
                "fsGroup": 1001,
                "seccompProfile": {"type": "RuntimeDefault"},
            },
        )
        container = spec["containers"][0]
        self.assertEqual(
            container["securityContext"],
            {
                "runAsNonRoot": True,
                "runAsUser": 1001,
                "runAsGroup": 1001,
                "allowPrivilegeEscalation": False,
                "capabilities": {"drop": ["ALL"]},
            },
        )

    def test_job_execution_contract(self) -> None:
        job = render_manifest()
        self.assertEqual(job["spec"]["backoffLimit"], 0)
        self.assertEqual(job["spec"]["completions"], 1)
        self.assertEqual(job["spec"]["parallelism"], 1)
        self.assertLessEqual(job["spec"]["activeDeadlineSeconds"], 10800)
        spec = job["spec"]["template"]["spec"]
        self.assertEqual(spec["restartPolicy"], "Never")

    def test_bounded_resources_and_digest_pinned_image(self) -> None:
        container = render_manifest()["spec"]["template"]["spec"]["containers"][0]
        self.assertIn("@sha256:", container["image"])
        self.assertEqual(container["imagePullPolicy"], "IfNotPresent")
        self.assertEqual(container["resources"]["requests"]["cpu"], "4")
        self.assertEqual(container["resources"]["requests"]["memory"], "12Gi")
        self.assertEqual(container["resources"]["requests"]["ephemeral-storage"], "20Gi")
        self.assertEqual(container["resources"]["limits"]["cpu"], "8")
        self.assertEqual(container["resources"]["limits"]["memory"], "16Gi")
        self.assertEqual(container["resources"]["limits"]["ephemeral-storage"], "40Gi")

    def test_labels_and_annotations(self) -> None:
        job = render_manifest()
        labels = job["metadata"]["labels"]
        self.assertEqual(labels["app"], "query-regression")
        self.assertEqual(labels["run-id"], "123456789")
        self.assertEqual(labels["run-attempt"], "2")
        self.assertEqual(job["spec"]["template"]["metadata"]["labels"], labels)
        annotations = job["metadata"]["annotations"]
        self.assertEqual(annotations["greptimedb.io/query-regression-base-sha"], BASE_SHA)
        self.assertEqual(annotations["greptimedb.io/query-regression-candidate-sha"], "b" * 40)

    def test_image_must_be_digest_pinned(self) -> None:
        with self.assertRaises(controller.ControllerError):
            controller.build_manifest(
                run_id=1,
                run_attempt=1,
                job_name="query-regression-1-1",
                namespace="arc-runners",
                image="example.com/runner:latest",
                base_sha="",
                candidate_sha="",
            )

    def test_bootstrap_waits_for_marker_then_runs_entrypoint(self) -> None:
        container = render_manifest()["spec"]["template"]["spec"]["containers"][0]
        self.assertEqual(container["command"], ["/bin/sh", "-c"])
        bootstrap = container["args"][0]
        self.assertIn("/payload/.ready", bootstrap)
        self.assertIn("exec /bin/bash /payload/run.sh", bootstrap)
        # The marker wait must be bounded.
        self.assertIn('"${i}" -ge 300', bootstrap)

    def test_run_sh_template_runs_perf_only(self) -> None:
        template = controller.RUN_SH_TEMPLATE
        # ACK runs performance regression only: no tooling/unit tests.
        for tooling_test in (
            "test_query_regression_runner_compaction_toctou.py",
            "test_query_regression_runner_otlp_trace_load.py",
            "test_query_regression_summary_otlp.py",
            "test_query_regression_case_selection.py",
        ):
            self.assertNotIn(tooling_test, template)
        self.assertIn("query-regression-run.py", template)
        self.assertIn("/work/benchmark-status", template)
        self.assertIn("/work/.done", template)
        self.assertIn("/work/.collected", template)
        # The entrypoint must stay alive after .done so results can be pulled.
        self.assertIn("until [ -f /work/.collected ]", template)
        # The collection window must be bounded.
        self.assertIn(str(controller.COLLECTION_WINDOW_ITERATIONS), template)

    def test_passthrough_env_never_contains_credentials(self) -> None:
        # Any env the workflow may pass through must be on the explicit list.
        env = controller.passthrough_env()
        names = {entry["name"] for entry in env}
        self.assertIn("GITHUB_OUTPUT", names)
        self.assertIn("BASE_BIN", names)
        self.assertNotIn("KUBECONFIG", names)
        self.assertNotIn("ACK_KUBECONFIG", names)

    def test_timeout_budget_has_cleanup_reserve(self) -> None:
        # Normal phases (pod-ready, transfer, run+collection) clamp to
        # cleanup_begins and must fit before the reserve.
        phase_worst = (
            controller.POD_READY_TIMEOUT_DEFAULT
            + controller.PAYLOAD_TRANSFER_ALLOWANCE
            + controller.RUN_TIMEOUT_DEFAULT
        )
        delete_worst = (
            controller.DELETE_TIMEOUT_DEFAULT * controller.DELETE_ATTEMPTS_DEFAULT
            + controller.DELETE_RETRY_SLEEP * (controller.DELETE_ATTEMPTS_DEFAULT - 1)
        )
        self.assertLessEqual(
            phase_worst,
            controller.LIFECYCLE_TIMEOUT_DEFAULT - controller.CLEANUP_RESERVE_DEFAULT,
        )
        # The reserve alone must cover the worst-case deletion budget.
        self.assertLessEqual(delete_worst, controller.CLEANUP_RESERVE_DEFAULT)
        # The workflow timeout must strictly exceed the lifecycle by at least
        # the documented setup + outer-cleanup margin (checkout, trusted
        # restore, artifact download/attestation, kubeconfig setup, and the
        # second-process cleanup-only step).
        self.assertGreater(
            controller.WORKFLOW_JOB_TIMEOUT_SECONDS, controller.LIFECYCLE_TIMEOUT_DEFAULT
        )
        self.assertGreaterEqual(
            controller.WORKFLOW_JOB_TIMEOUT_SECONDS - controller.LIFECYCLE_TIMEOUT_DEFAULT,
            controller.SETUP_AND_OUTER_CLEANUP_MARGIN_MINIMUM,
        )
        self.assertGreater(
            controller.WORKFLOW_JOB_TIMEOUT_SECONDS - controller.LIFECYCLE_TIMEOUT_DEFAULT,
            controller.CLEANUP_ONLY_BUDGET,
        )
        # The pod deadline must exceed every controller budget.
        self.assertGreater(
            controller.ACTIVE_DEADLINE_SECONDS,
            controller.POD_READY_TIMEOUT_DEFAULT + controller.RUN_TIMEOUT_DEFAULT,
        )


class DryRunTest(unittest.TestCase):
    def test_dry_run_renders_valid_manifest_without_cluster(self) -> None:
        env = dict(os.environ)
        env.update(
            {
                "RUN_ID": "123456789",
                "RUN_ATTEMPT": "2",
                "JOB_IMAGE": controller.DEFAULT_IMAGE,
                "VERIFIED_BASE_SHA": BASE_SHA,
                "VERIFIED_CANDIDATE_SHA": "b" * 40,
                "CASE_PATHS": "all",
                "HTTP_TIMEOUT": "300",
                "ALLOW_LARGE_FIXTURE": "true",
                "CARGO_PROFILE": "nightly",
                "GITHUB_OUTPUT": "",
            }
        )
        proc = subprocess.run(
            [sys.executable, str(CONTROLLER_PATH), "--dry-run"],
            capture_output=True,
            text=True,
            timeout=60,
            env=env,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        manifest = json.loads(proc.stdout)
        self.assertEqual(manifest["metadata"]["name"], "query-regression-123456789-2")
        self.assertEqual(manifest["kind"], "Job")
        self.assertEqual(manifest["apiVersion"], "batch/v1")


class SymlinkContainmentTest(unittest.TestCase):
    """Candidate-controlled payload paths must reject symlinks and non-regular files."""

    def _make_candidate(self, root: Path) -> tuple[Path, Path]:
        candidate = root / "candidate"
        (candidate / "tests" / "perf" / "query_cases" / "smoke").mkdir(parents=True)
        (candidate / ".github" / "scripts").mkdir(parents=True)
        (candidate / "tests" / "perf" / "query_cases" / "smoke" / "case.toml").write_text(
            "[scenario]\nkind = \"direct_readable_sst\"\n", encoding="utf-8"
        )
        (candidate / ".github" / "scripts" / "query-regression-run.py").write_text(
            "#!/usr/bin/env python3\nprint('candidate driver')\n", encoding="utf-8"
        )
        return candidate, candidate / ".github" / "scripts" / "query-regression-run.py"

    def _assemble(self, candidate: Path) -> None:
        base_artifact, candidate_artifact, trusted_dir, _ = make_artifacts(candidate.parent, candidate)
        controller.assemble_payload(
            base_artifact,
            candidate_artifact,
            candidate,
            trusted_dir / "query-regression-summary.py",
            candidate.parent / "payload",
        )

    def test_rejects_absolute_symlink_in_case_tree(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-symlink-") as tmp_str:
            candidate, _ = self._make_candidate(Path(tmp_str))
            (candidate / "tests" / "perf" / "query_cases" / "smoke" / "evil").symlink_to("/etc")
            with self.assertRaises(controller.ControllerError) as ctx:
                self._assemble(candidate)
            self.assertIn("symlink", str(ctx.exception))

    def test_rejects_relative_escape_symlink_in_case_tree(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-symlink-") as tmp_str:
            candidate, _ = self._make_candidate(Path(tmp_str))
            (candidate / "tests" / "perf" / "query_cases" / "smoke" / "escape").symlink_to(
                "../../../../../../outside"
            )
            with self.assertRaises(controller.ControllerError) as ctx:
                self._assemble(candidate)
            self.assertIn("symlink", str(ctx.exception))

    def test_rejects_in_tree_symlink(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-symlink-") as tmp_str:
            candidate, _ = self._make_candidate(Path(tmp_str))
            target = candidate / "tests" / "perf" / "query_cases" / "smoke" / "case.toml"
            (candidate / "tests" / "perf" / "alias").symlink_to(target)
            with self.assertRaises(controller.ControllerError) as ctx:
                self._assemble(candidate)
            self.assertIn("symlink", str(ctx.exception))

    def test_rejects_symlink_driver(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-symlink-") as tmp_str:
            candidate, driver = self._make_candidate(Path(tmp_str))
            driver.unlink()
            driver.symlink_to("/etc/passwd")
            with self.assertRaises(controller.ControllerError) as ctx:
                self._assemble(candidate)
            self.assertIn("symlink", str(ctx.exception))

    def test_rejects_driver_escaping_checkout_root(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-symlink-") as tmp_str:
            candidate, _ = self._make_candidate(Path(tmp_str))
            # A symlinked parent directory makes the regular driver file resolve
            # outside the candidate checkout root -> the ancestor symlink must
            # be rejected before any resolution follows it.
            outside = candidate.parent / "outside-scripts"
            (outside / "scripts").mkdir(parents=True)
            (outside / "scripts" / "query-regression-run.py").write_text(
                "#!/usr/bin/env python3\n", encoding="utf-8"
            )
            (candidate / ".github").rename(candidate / ".github-real")
            (candidate / ".github").symlink_to(outside)
            with self.assertRaises(controller.ControllerError) as ctx:
                self._assemble(candidate)
            self.assertIn("symlink component", str(ctx.exception))

    def test_rejects_tests_symlink_to_outside(self) -> None:
        """Exact regression: candidate/tests -> outside must be rejected as an
        ancestor symlink of the tests/perf payload root."""
        with tempfile.TemporaryDirectory(prefix="qr-ack-symlink-") as tmp_str:
            candidate, _ = self._make_candidate(Path(tmp_str))
            outside = candidate.parent / "outside-tests"
            (outside / "perf" / "query_cases" / "smoke").mkdir(parents=True)
            (outside / "perf" / "query_cases" / "smoke" / "case.toml").write_text(
                "[scenario]\nkind = \"direct_readable_sst\"\n", encoding="utf-8"
            )
            (candidate / "tests").rename(candidate / "tests-real")
            (candidate / "tests").symlink_to(outside)
            with self.assertRaises(controller.ControllerError) as ctx:
                self._assemble(candidate)
            self.assertIn("symlink component", str(ctx.exception))
            # The escape target must never have been copied into the payload.
            self.assertFalse((candidate.parent / "payload" / "repo").exists())

    def test_rejects_in_tree_ancestor_symlink(self) -> None:
        """A symlinked ancestor that points back inside the tree is also
        rejected (an in-tree alias could confuse the copy or execution)."""
        with tempfile.TemporaryDirectory(prefix="qr-ack-symlink-") as tmp_str:
            candidate, _ = self._make_candidate(Path(tmp_str))
            real = candidate / "tests-real"
            (candidate / "tests").rename(real)
            (candidate / "tests").symlink_to(real)
            with self.assertRaises(controller.ControllerError) as ctx:
                self._assemble(candidate)
            self.assertIn("symlink component", str(ctx.exception))

    def test_rejects_nested_symlinked_query_cases_parent(self) -> None:
        """A symlink two levels deep (tests/perf -> elsewhere) must also be
        rejected as an ancestor of the leaf case files."""
        with tempfile.TemporaryDirectory(prefix="qr-ack-symlink-") as tmp_str:
            candidate, _ = self._make_candidate(Path(tmp_str))
            real = candidate / "perf-real"
            (candidate / "tests" / "perf").rename(real)
            (candidate / "tests" / "perf").symlink_to(real)
            with self.assertRaises(controller.ControllerError) as ctx:
                self._assemble(candidate)
            self.assertIn("symlink component", str(ctx.exception))

    def test_rejects_fifo_in_case_tree(self) -> None:
        if not hasattr(os, "mkfifo"):
            self.skipTest("os.mkfifo unavailable")
        with tempfile.TemporaryDirectory(prefix="qr-ack-fifo-") as tmp_str:
            candidate, _ = self._make_candidate(Path(tmp_str))
            fifo = candidate / "tests" / "perf" / "query_cases" / "smoke" / "pipe"
            os.mkfifo(fifo)
            with self.assertRaises(controller.ControllerError) as ctx:
                self._assemble(candidate)
            self.assertIn("non-regular", str(ctx.exception))

    def test_rejects_symlinked_tests_perf_root(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-symlink-") as tmp_str:
            candidate, _ = self._make_candidate(Path(tmp_str))
            real_perf = candidate / "tests" / "perf"
            moved = candidate / "tests" / "perf-real"
            real_perf.rename(moved)
            real_perf.symlink_to(moved)
            with self.assertRaises(controller.ControllerError) as ctx:
                self._assemble(candidate)
            self.assertIn("symlink", str(ctx.exception))


class FakeClock:
    """Deterministic monotonic clock for lifecycle deadline tests."""

    def __init__(self, start: float = 1000.0):
        self.t = start

    def now(self) -> float:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.t += max(0.0, seconds)

    def advance(self, seconds: float) -> None:
        self.t += max(0.0, seconds)


class ScriptedKubectl:
    """Records every call (args + clamped timeout) and returns scripted results."""

    def __init__(self, responder: Callable[[list[str], bool, str | None, int], subprocess.CompletedProcess[str]]):
        self.responder = responder
        self.calls: list[tuple[list[str], int]] = []
        self.namespace = "arc-runners"
        self.binary = "kubectl"

    def run(
        self, args: list[str], check: bool = True, input_text: str | None = None, timeout: int = 180
    ) -> subprocess.CompletedProcess[str]:
        self.calls.append((list(args), timeout))
        proc = self.responder(args, check, input_text, timeout)
        if check and proc.returncode != 0:
            raise controller.ControllerError(f"kubectl command failed: {proc.stderr}")
        return proc

    def pod_spec(self, pod: str) -> str:
        return f"{self.namespace}/{pod}"


def completed(
    args: list[str], rc: int = 0, stdout: str = "", stderr: str = ""
) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(args, rc, stdout, stderr)


class LifecycleDeadlineTest(unittest.TestCase):
    """Fake-clock tests proving phase overrun clamping and that cleanup always
    begins by its deadline."""

    def setUp(self) -> None:
        self.clock = FakeClock()
        self.now_patcher = mock.patch.object(controller, "_now", side_effect=self.clock.now)
        self.sleep_patcher = mock.patch.object(controller, "_sleep", side_effect=self.clock.sleep)
        self.now_patcher.start()
        self.sleep_patcher.start()
        self.addCleanup(self.now_patcher.stop)
        self.addCleanup(self.sleep_patcher.stop)

    def test_clamp_int_basic(self) -> None:
        self.assertEqual(controller.clamp_int(30.0, 600), 30)
        self.assertEqual(controller.clamp_int(600.0, 30), 30)
        self.assertEqual(controller.clamp_int(0.5, 600), 1)
        self.assertEqual(controller.clamp_int(120.0, 600, minimum=10), 120)

    def test_lifecycle_phases_clamp_to_cleanup_begins(self) -> None:
        lifecycle = controller.Lifecycle(hard_timeout=100, cleanup_reserve=20)
        self.assertEqual(lifecycle.cleanup_begins, 1000 + 80)
        self.assertEqual(lifecycle.hard_deadline, 1000 + 100)
        # A long phase never extends past cleanup_begins.
        self.assertLessEqual(lifecycle.phase_deadline(3600), lifecycle.cleanup_begins)
        self.assertEqual(lifecycle.phase_deadline(3600), lifecycle.cleanup_begins)
        self.assertEqual(lifecycle.clamp(600, 30), 30)
        self.assertEqual(lifecycle.clamp(600), 80)
        self.assertFalse(lifecycle.in_cleanup_reserve())
        # Once inside the reserve, phase budget is zero but cleanup remains.
        self.clock.advance(80)
        self.assertTrue(lifecycle.in_cleanup_reserve())
        self.assertEqual(lifecycle.cleanup_remaining(), 20)
        self.assertEqual(lifecycle.phase_remaining(3600), 0)
        # clamp still returns a positive minimum for subprocess timeouts.
        self.assertGreaterEqual(lifecycle.clamp(600), 1)

    def test_wait_for_pod_stops_at_deadline_with_clamped_timeouts(self) -> None:
        def responder(args, check, input_text, timeout):
            if args[0] == "get" and "pod" in args and "-l" in args:
                return completed(args, 0, "")  # pod never appears
            return completed(args, 0, "Running")

        kubectl = ScriptedKubectl(responder)
        deadline = self.clock.now() + 10
        with self.assertRaises(controller.ControllerError):
            controller.wait_for_pod(kubectl, "query-regression-1-1", deadline)
        # The loop ran to the deadline without overrunning.
        self.assertEqual(self.clock.now(), 1010)
        # Every kubectl call was clamped to the remaining phase budget.
        self.assertTrue(kubectl.calls)
        for args, timeout in kubectl.calls:
            self.assertLessEqual(timeout, 10)

    def test_wait_for_pod_returns_when_running(self) -> None:
        def responder(args, check, input_text, timeout):
            if args[0] == "get" and "pod" in args and "-l" in args:
                return completed(args, 0, "fake-pod-1")
            if args[0] == "get" and args[1] == "pod":
                return completed(args, 0, "Running")
            return completed(args, 0, "")

        kubectl = ScriptedKubectl(responder)
        pod = controller.wait_for_pod(kubectl, "query-regression-1-1", self.clock.now() + 60)
        self.assertEqual(pod, "fake-pod-1")
        for args, timeout in kubectl.calls:
            self.assertLessEqual(timeout, 60)

    def test_delete_job_clamps_to_deadline_and_stops(self) -> None:
        def responder(args, check, input_text, timeout):
            if args[0] == "get" and args[1] == "job":
                return completed(args, 0, "job.batch/query-regression-1-1")  # always exists
            if args[0] == "delete":
                return completed(args, 1, "", "simulated delete failure")
            return completed(args, 0, "")

        kubectl = ScriptedKubectl(responder)
        deadline = self.clock.now() + 3  # tiny budget: the deadline must cut attempts short
        with self.assertRaises(controller.ControllerError) as ctx:
            controller.delete_job(
                kubectl, "query-regression-1-1", deadline, attempts=3, delete_timeout=240
            )
        self.assertIn("cleanup budget exhausted", str(ctx.exception))
        delete_calls = [args for args, _ in kubectl.calls if args[0] == "delete"]
        # The third attempt never started: the deadline cut it off.
        self.assertEqual(len(delete_calls), 2)
        for args, timeout in kubectl.calls:
            self.assertLessEqual(timeout, 3)

    def test_cleanup_begins_by_deadline_skips_diagnostics(self) -> None:
        lifecycle = controller.Lifecycle(hard_timeout=100, cleanup_reserve=20)
        self.clock.advance(85)  # inside the cleanup reserve (cleanup_begins = 1080)
        kubectl = ScriptedKubectl(lambda args, check, input_text, timeout: completed(args))
        result_dir = Path(tempfile.mkdtemp(prefix="qr-ack-reserve-"))
        with mock.patch.object(controller, "collect_logs") as mock_logs, mock.patch.object(
            controller, "delete_job"
        ) as mock_delete:
            controller.run_cleanup(
                lifecycle, kubectl, "query-regression-1-1", "fake-pod", result_dir,
                cancelled=False, delete_attempts=3, delete_timeout=240,
            )
        # Diagnostics are skipped once the cleanup reserve begins.
        mock_logs.assert_not_called()
        self.assertEqual(mock_delete.call_count, 1)
        # Deletion clamps to the hard deadline inside the reserve.
        self.assertEqual(mock_delete.call_args[0][2], lifecycle.hard_deadline)

    def test_cleanup_diagnostics_before_reserve_are_clamped(self) -> None:
        lifecycle = controller.Lifecycle(hard_timeout=100, cleanup_reserve=20)
        kubectl = ScriptedKubectl(lambda args, check, input_text, timeout: completed(args))
        result_dir = Path(tempfile.mkdtemp(prefix="qr-ack-reserve-"))
        with mock.patch.object(controller, "collect_logs") as mock_logs, mock.patch.object(
            controller, "delete_job"
        ) as mock_delete:
            controller.run_cleanup(
                lifecycle, kubectl, "query-regression-1-1", "fake-pod", result_dir,
                cancelled=False, delete_attempts=3, delete_timeout=240,
            )
        mock_logs.assert_called_once()
        # Diagnostics are clamped to stop at cleanup_begins.
        self.assertEqual(mock_logs.call_args.kwargs["deadline"], lifecycle.cleanup_begins)
        self.assertEqual(mock_delete.call_args[0][2], lifecycle.hard_deadline)

    def test_cancellation_skips_diagnostics_and_short_deletes(self) -> None:
        lifecycle = controller.Lifecycle(hard_timeout=100, cleanup_reserve=20)
        kubectl = ScriptedKubectl(lambda args, check, input_text, timeout: completed(args))
        result_dir = Path(tempfile.mkdtemp(prefix="qr-ack-cancel-"))
        with mock.patch.object(controller, "collect_logs") as mock_logs, mock.patch.object(
            controller, "delete_job"
        ) as mock_delete:
            controller.run_cleanup(
                lifecycle, kubectl, "query-regression-1-1", "fake-pod", result_dir,
                cancelled=True, delete_attempts=3, delete_timeout=240,
            )
        mock_logs.assert_not_called()
        self.assertEqual(mock_delete.call_count, 1)
        # Cancellation uses a short bounded deletion budget, not the full
        # lifecycle deadline.
        expected = self.clock.now() + controller.CANCELLATION_DELETE_BUDGET
        self.assertAlmostEqual(mock_delete.call_args[0][2], expected)


class ExhaustedNormalBudgetTest(unittest.TestCase):
    """Fake-clock tests proving zero/negative remaining normal budget prevents
    all Job create/reconcile calls while leaving the cleanup budget intact."""

    def setUp(self) -> None:
        self.clock = FakeClock()
        self.now_patcher = mock.patch.object(controller, "_now", side_effect=self.clock.now)
        self.sleep_patcher = mock.patch.object(controller, "_sleep", side_effect=self.clock.sleep)
        self.now_patcher.start()
        self.sleep_patcher.start()
        self.addCleanup(self.now_patcher.stop)
        self.addCleanup(self.sleep_patcher.stop)

    def test_require_phase_raises_when_budget_exhausted(self) -> None:
        lifecycle = controller.Lifecycle(hard_timeout=100, cleanup_reserve=20)
        # Positive budget: returns it, never raises.
        self.clock.advance(10)
        self.assertEqual(lifecycle.require_phase(what="Job creation"), 70)
        # Zero budget at cleanup_begins: raises instead of a 1-second op.
        self.clock.advance(70)
        with self.assertRaises(controller.ControllerError) as ctx:
            lifecycle.require_phase(what="Job creation")
        self.assertIn("no budget remains", str(ctx.exception))
        with self.assertRaises(controller.ControllerError):
            lifecycle.phase_clamp(60, what="preflight job lookup")
        # Negative budget past the hard deadline: still raises.
        self.clock.advance(30)
        with self.assertRaises(controller.ControllerError):
            lifecycle.require_phase(what="leaked-job reconcile deletion")
        # Cleanup budget is tracked separately and was never consumed by the
        # refused normal-phase operations.
        self.assertEqual(lifecycle.cleanup_remaining(), 0)
        self.assertEqual(lifecycle.phase_remaining(3600), 0)

    def test_phase_clamp_never_degrades_exhausted_budget_to_one_second(self) -> None:
        lifecycle = controller.Lifecycle(hard_timeout=100, cleanup_reserve=20)
        self.clock.advance(80)  # exactly at cleanup_begins: zero normal budget
        with self.assertRaises(controller.ControllerError):
            lifecycle.phase_clamp(60, what="Job creation")
        # While the budget is positive, clamping bounds the timeout.
        lifecycle2 = controller.Lifecycle(hard_timeout=100, cleanup_reserve=20)
        self.clock.advance(10)
        self.assertEqual(lifecycle2.phase_clamp(600, what="Job creation"), 70)

    def _run_main(
        self, lifecycle: controller.Lifecycle, *, job_exists_after_delete: bool = True
    ) -> tuple[int, ScriptedKubectl, str]:
        """Run main() with a pre-built (already-aged) Lifecycle and a stateful
        fake cluster; returns (rc, kubectl, stderr) with every call recorded."""
        state = {"exists": True}

        def responder(args, check, input_text, timeout):
            if args[0] == "get" and args[1] == "job":
                return completed(
                    args, 0, "job.batch/query-regression-123456789-2" if state["exists"] else ""
                )
            if args[0] == "delete":
                state["exists"] = job_exists_after_delete
                return completed(args, 0, "job.batch/query-regression-123456789-2 deleted")
            return completed(args, 0, "")

        kubectl = ScriptedKubectl(responder)
        env_patch = mock.patch.dict(
            os.environ,
            {
                "VERIFIED_BASE_SHA": "a" * 40,
                "VERIFIED_CANDIDATE_SHA": "b" * 40,
                "CASE_PATHS": "all",
                "RUN_URL": "https://example.invalid/run/1",
                "GITHUB_OUTPUT": "",
            },
            clear=False,
        )
        env_patch.start()
        self.addCleanup(env_patch.stop)
        stderr = io.StringIO()
        with mock.patch.object(sys, "stderr", stderr), mock.patch.object(
            controller, "Kubectl", return_value=kubectl
        ), mock.patch.object(controller, "Lifecycle", return_value=lifecycle), mock.patch.object(
            controller, "verify_checkout_sha"
        ), mock.patch.object(controller, "verify_trusted_scripts"), mock.patch.object(
            controller, "assemble_payload", return_value=Path("/tmp/fake-payload")
        ), tempfile.TemporaryDirectory(prefix="qr-ack-exhausted-") as tmp_str:
            tmp = Path(tmp_str)
            base_dir = tmp / "base"
            cand_dir = tmp / "candidate"
            base_dir.mkdir()
            cand_dir.mkdir()
            (base_dir / "base-manifest.json").write_text(
                json.dumps(
                    {"base_sha": "a" * 40, "binaries": [{"path": "query-regression-bins/base/greptime", "sha256": "aa"}]}
                ),
                encoding="utf-8",
            )
            (cand_dir / "candidate-manifest.json").write_text(
                json.dumps(
                    {"candidate_sha": "b" * 40, "binaries": [{"path": "query-regression-bins/candidate/greptime", "sha256": "bb"}]}
                ),
                encoding="utf-8",
            )
            rc = controller.main(
                [
                    "--run-id", "123456789", "--run-attempt", "2",
                    "--kubectl", "kubectl",
                    "--delete-timeout", "30", "--delete-attempts", "3",
                    "--lifecycle-timeout", "100", "--cleanup-reserve", "20",
                    "--result-dir", str(tmp / "results"),
                    "--base-artifact-dir", str(base_dir),
                    "--candidate-artifact-dir", str(cand_dir),
                    "--candidate-src", str(tmp / "src"),
                    "--trusted-summary", str(tmp / "summary.py"),
                    "--trusted-scripts-manifest", str(tmp / "trusted.json"),
                    "--payload-dir", str(tmp / "payload"),
                ]
            )
        return rc, kubectl, stderr.getvalue()

    def test_zero_budget_blocks_create_and_reconcile_keeps_cleanup(self) -> None:
        # The lifecycle began at clock 1000 (cleanup_begins = 1080, hard
        # deadline = 1100); the clock is now exactly at cleanup_begins, so the
        # normal-phase budget is zero while the full 20s reserve remains.
        lifecycle = controller.Lifecycle(hard_timeout=100, cleanup_reserve=20)
        self.clock.advance(80)
        rc, kubectl, _ = self._run_main(lifecycle)
        self.assertEqual(rc, 0)
        # No Job create ever reached the cluster.
        self.assertEqual([a for a, _ in kubectl.calls if a[0] == "create"], [])
        # The reconcile preflight job lookup (the very first cluster call in a
        # healthy run) never ran: the first call is the cleanup-path pod
        # discovery, not a job lookup.
        self.assertTrue(kubectl.calls)
        self.assertEqual(kubectl.calls[0][0][:2], ["get", "pod"])
        # The cleanup path still deleted the exact Job with the intact reserve.
        self.assertTrue(any(a[0] == "delete" for a, _ in kubectl.calls))
        for args, timeout in kubectl.calls:
            # Every call was bounded by the 20s reserve, never a 1-second
            # minimum outside it.
            self.assertGreaterEqual(timeout, 1)
            self.assertLessEqual(timeout, 20)

    def test_negative_budget_blocks_create_reconcile_and_cluster_calls(self) -> None:
        # The clock is now past the hard deadline: no normal-phase budget and
        # no cleanup budget either; even cleanup refuses cluster calls.
        lifecycle = controller.Lifecycle(hard_timeout=100, cleanup_reserve=20)
        self.clock.advance(110)
        rc, kubectl, stderr = self._run_main(lifecycle)
        self.assertEqual(rc, 0)
        self.assertEqual([a for a, _ in kubectl.calls if a[0] == "create"], [])
        # No reconcile preflight lookup and no delete/absence check either.
        self.assertEqual([a for a, _ in kubectl.calls if len(a) > 1 and a[1] == "job"], [])
        # main() reported the deletion could not be confirmed (cleanup_ok False).
        self.assertIn("job deletion could not be confirmed", stderr)

    def test_positive_budget_still_creates_job(self) -> None:
        # Sanity: with a healthy budget the normal flow reaches create_job.
        lifecycle = controller.Lifecycle(hard_timeout=100, cleanup_reserve=20)
        self.clock.advance(10)
        kubectl = ScriptedKubectl(
            lambda args, check, input_text, timeout: completed(args, 0, "Running")
            if args[0] == "get" and "pod" in args
            else completed(args, 0, "")
        )
        env_patch = mock.patch.dict(
            os.environ,
            {
                "VERIFIED_BASE_SHA": "a" * 40,
                "VERIFIED_CANDIDATE_SHA": "b" * 40,
                "CASE_PATHS": "all",
                "RUN_URL": "https://example.invalid/run/1",
                "GITHUB_OUTPUT": "",
            },
            clear=False,
        )
        env_patch.start()
        self.addCleanup(env_patch.stop)
        with mock.patch.object(controller, "Kubectl", return_value=kubectl), mock.patch.object(
            controller, "Lifecycle", return_value=lifecycle
        ), mock.patch.object(controller, "verify_checkout_sha"), mock.patch.object(
            controller, "verify_trusted_scripts"
        ), mock.patch.object(
            controller, "assemble_payload", return_value=Path("/tmp/fake-payload")
        ), tempfile.TemporaryDirectory(prefix="qr-ack-exhausted-") as tmp_str:
            tmp = Path(tmp_str)
            base_dir = tmp / "base"
            cand_dir = tmp / "candidate"
            base_dir.mkdir()
            cand_dir.mkdir()
            (base_dir / "base-manifest.json").write_text(
                json.dumps(
                    {"base_sha": "a" * 40, "binaries": [{"path": "query-regression-bins/base/greptime", "sha256": "aa"}]}
                ),
                encoding="utf-8",
            )
            (cand_dir / "candidate-manifest.json").write_text(
                json.dumps(
                    {"candidate_sha": "b" * 40, "binaries": [{"path": "query-regression-bins/candidate/greptime", "sha256": "bb"}]}
                ),
                encoding="utf-8",
            )
            rc = controller.main(
                [
                    "--run-id", "123456789", "--run-attempt", "2",
                    "--kubectl", "kubectl",
                    "--delete-timeout", "30", "--delete-attempts", "3",
                    "--lifecycle-timeout", "100", "--cleanup-reserve", "20",
                    "--result-dir", str(tmp / "results"),
                    "--base-artifact-dir", str(base_dir),
                    "--candidate-artifact-dir", str(cand_dir),
                    "--candidate-src", str(tmp / "src"),
                    "--trusted-summary", str(tmp / "summary.py"),
                    "--trusted-scripts-manifest", str(tmp / "trusted.json"),
                    "--payload-dir", str(tmp / "payload"),
                ]
            )
        self.assertEqual(rc, 0)
        self.assertTrue(any(a[0] == "create" for a, _ in kubectl.calls))
        # create_job's timeout was clamped to the remaining phase budget.
        create_timeouts = [t for a, t in kubectl.calls if a[0] == "create"]
        self.assertTrue(create_timeouts)
        self.assertLessEqual(create_timeouts[0], 90)


class CleanupOnlyTest(unittest.TestCase):
    """Second-process cleanup mode: deletes only the exact deterministic Job."""

    def _run_cleanup_only(
        self, tmp: Path, run_id: str = "123456789", run_attempt: str = "2", leaked: bool = False
    ) -> tuple[subprocess.CompletedProcess[str], Path]:
        state = tmp / "state"
        state.mkdir(exist_ok=True)
        (state / "pod" / "payload").mkdir(parents=True, exist_ok=True)
        (state / "pod" / "work").mkdir(parents=True, exist_ok=True)
        if leaked:
            (state / "job-exists").touch()
        fake = state / "kubectl"
        fake.write_text(FAKE_KUBECTL, encoding="utf-8")
        fake.chmod(0o755)
        env = dict(
            os.environ,
            RUN_ID=run_id,
            RUN_ATTEMPT=run_attempt,
            GITHUB_OUTPUT="",
            FAKE_KUBECTL_STATE=str(state),
        )
        proc = subprocess.run(
            [
                sys.executable,
                str(CONTROLLER_PATH),
                "--cleanup-only",
                "--kubectl",
                str(fake),
                "--delete-timeout",
                "30",
                "--delete-attempts",
                "3",
            ],
            capture_output=True,
            text=True,
            timeout=60,
            env=env,
        )
        return proc, state

    def test_cleanup_only_deletes_exact_leaked_job(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-cleanup-only-") as tmp_str:
            tmp = Path(tmp_str)
            proc, state = self._run_cleanup_only(tmp, leaked=True)
            self.assertEqual(proc.returncode, 0, proc.stderr)
            deleted = (state / "deleted").read_text().splitlines()
            self.assertEqual(deleted, ["job.batch/query-regression-123456789-2 deleted"])
            self.assertFalse((state / "job-exists").exists())

    def test_cleanup_only_noop_when_job_absent(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-cleanup-only-") as tmp_str:
            tmp = Path(tmp_str)
            proc, state = self._run_cleanup_only(tmp, leaked=False)
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertFalse((state / "deleted").exists())

    def test_cleanup_only_rejects_invalid_run_id(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-cleanup-only-") as tmp_str:
            tmp = Path(tmp_str)
            proc, _ = self._run_cleanup_only(tmp, run_id="abc", run_attempt="1")
            self.assertEqual(proc.returncode, 1)
            self.assertIn("invalid run id", proc.stderr)


# Fake kubectl that simulates the ACK cluster state machine without contacting
# any cluster: create/get/delete Job, pod phase, exec-gated markers, and
# kubectl cp push/pull against a fake pod filesystem under $FAKE_KUBECTL_STATE.
# Failure injection: FAKE_KUBECTL_CP_FAILS, FAKE_KUBECTL_DELETE_FAILS (count),
# FAKE_KUBECTL_DELETE_LEAVES, FAKE_KUBECTL_BAD_SHA, FAKE_KUBECTL_NEVER_DONE.
FAKE_KUBECTL = r"""#!/usr/bin/env bash
set -u
STATE="${FAKE_KUBECTL_STATE:?missing FAKE_KUBECTL_STATE}"
POD_DIR="${STATE}/pod"
mkdir -p "${POD_DIR}/payload" "${POD_DIR}/work"

cmd="$1"
shift

# Map in-pod /payload and /work paths onto the fake pod filesystem without
# re-scanning replacements (parameter expansion, unlike sed -g, never re-matches
# inside its own replacement text).
map_paths() {
  local s="$1"
  s="${s//\/payload/${POD_DIR}\/payload}"
  s="${s//\/work/${POD_DIR}\/work}"
  printf '%s' "$s"
}

case "$cmd" in
  get)
    kind="$1"
    shift
    if [ "$kind" = "job" ]; then
      if [ -f "${STATE}/job-exists" ]; then
        echo "job.batch/$1"
        exit 0
      fi
      exit 1
    fi
    if [ "$kind" = "pod" ]; then
      if [ "$1" = "-l" ]; then
        # list by selector
        if [ -f "${STATE}/job-exists" ]; then
          echo "fake-pod-1"
        fi
        exit 0
      fi
      # get by name -> phase
      phase="Running"
      if [ -f "${STATE}/phase" ]; then
        phase="$(cat "${STATE}/phase")"
      fi
      echo "${phase}"
      exit 0
    fi
    exit 1
    ;;
  create)
    cat > "${STATE}/manifest.json"
    touch "${STATE}/job-exists"
    echo "job.batch/$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["metadata"]["name"])' "${STATE}/manifest.json") created"
    exit 0
    ;;
  delete)
    kind="$1"
    shift
    if [ "$kind" = "job" ]; then
      name="$1"
      shift
      if [ -f "${STATE}/delete-fails-remaining" ]; then
        remaining="$(cat "${STATE}/delete-fails-remaining")"
        if [ "${remaining}" -gt 0 ]; then
          echo "$((remaining - 1))" > "${STATE}/delete-fails-remaining"
          echo "simulated delete failure for ${name}" >&2
          exit 1
        fi
      fi
      echo "job.batch/${name} deleted" >> "${STATE}/deleted"
      if [ "${FAKE_KUBECTL_DELETE_LEAVES:-0}" != "1" ]; then
        rm -f "${STATE}/job-exists"
      fi
      exit 0
    fi
    exit 1
    ;;
  cp)
    src="$1"
    dst="$2"
    if [[ "$src" == *:* ]]; then
      # pull: <ns>/<pod>:/work/... <local>
      remote="${src#*:}"
      if [ -e "${POD_DIR}${remote}" ]; then
        cp -r "${POD_DIR}${remote}" "$dst"
        exit 0
      fi
      exit 1
    fi
    # push: <local> <ns>/<pod>:/payload/  (trailing slash -> basename appended)
    if [ "${FAKE_KUBECTL_CP_FAILS:-0}" = "1" ]; then
      echo "simulated cp failure" >&2
      exit 1
    fi
    remote="${dst#*:}"
    if [[ "$remote" == */ ]]; then
      cp -r "$src" "${POD_DIR}/payload/"
    else
      cp -r "$src" "${POD_DIR}${remote}"
    fi
    exit 0
    ;;
  exec)
    # exec <ns>/<pod> -- sh -c '<command>'
    shift
    [ "$1" = "--" ] && shift
    command="$3"
    if [[ "$command" == *"test -f /work/.done"* ]]; then
      if [ "${FAKE_KUBECTL_NEVER_DONE:-}" = "1" ]; then
        exit 1
      fi
      polls=0
      [ -f "${STATE}/done-polls" ] && polls="$(cat "${STATE}/done-polls")"
      polls=$((polls + 1))
      echo "$polls" > "${STATE}/done-polls"
      if [ "$polls" -ge 1 ]; then
        mkdir -p "${POD_DIR}/work/query-regression-work/smoke"
        printf '%s\n' '{"status":"ok","targets":[],"thresholds":[]}' \
          > "${POD_DIR}/work/query-regression-work/smoke/query-regression-report.json"
        if [ "${FAKE_KUBECTL_NO_SUMMARY:-0}" != "1" ]; then
          printf '%s\n' '# Query regression summary' > "${POD_DIR}/work/query-regression-summary.md"
        fi
        printf '%s' '0' > "${POD_DIR}/work/benchmark-status"
        touch "${POD_DIR}/work/.done"
      fi
      [ -f "${POD_DIR}/work/.done" ] && exit 0
      exit 1
    fi
    if [[ "$command" == *"cat /work/benchmark-status"* ]]; then
      cat "${POD_DIR}/work/benchmark-status" 2>/dev/null || true
      exit 0
    fi
    if [[ "$command" == *"touch /work/.collected"* ]]; then
      touch "${POD_DIR}/work/.collected"
      exit 0
    fi
    if [[ "$command" == *"sha256sum"* ]]; then
      mapped="$(map_paths "$command")"
      if [ "${FAKE_KUBECTL_BAD_SHA:-}" = "1" ]; then
        echo "0000000000000000000000000000000000000000000000000000000000000000  /payload/bins/base/greptime"
        echo "0000000000000000000000000000000000000000000000000000000000000000  /payload/bins/candidate/greptime"
        echo "0000000000000000000000000000000000000000000000000000000000000000  /payload/bins/candidate/query_perf_fixture"
        echo "0000000000000000000000000000000000000000000000000000000000000000  /payload/bins/candidate/query_regression_runner"
        exit 0
      fi
      while IFS= read -r line; do
        read -r digest path <<< "$line"
        printf '%s  %s\n' "$digest" "${path#"$POD_DIR"}"
      done < <(bash -c "$mapped")
      exit 0
    fi
    mapped="$(map_paths "$command")"
    bash -c "$mapped"
    exit $?
    ;;
  logs)
    echo "fake pod logs line 1"
    echo "fake pod logs line 2"
    exit 0
    ;;
  describe)
    shift
    echo "Name: $1"
    echo "Events: none"
    exit 0
    ;;
  *)
    echo "unhandled fake kubectl command: $cmd" >&2
    exit 1
    ;;
esac
"""


def make_artifacts(root: Path, candidate: Path | None = None) -> tuple[Path, Path, Path, str]:
    """Create base/candidate artifact dirs, a trusted-scripts dir, and return
    the real candidate checkout SHA (a git repo whose HEAD is the candidate).
    When ``candidate`` is given, it is used as-is (no git repo required)."""
    base_sha = BASE_SHA

    base_artifact = root / "base-artifact"
    (base_artifact / "query-regression-bins" / "base").mkdir(parents=True)
    base_bin = b"base greptime binary"
    (base_artifact / "query-regression-bins" / "base" / "greptime").write_bytes(base_bin)
    base_manifest = {
        "base_sha": base_sha,
        "binaries": [
            {"path": "query-regression-bins/base/greptime", "sha256": hashlib.sha256(base_bin).hexdigest()}
        ],
    }
    (base_artifact / "base-manifest.json").write_text(json.dumps(base_manifest), encoding="utf-8")

    candidate_artifact = root / "candidate-artifact"
    bins = {
        "query-regression-bins/candidate/greptime": b"candidate greptime binary",
        "query-regression-bins/candidate/query_perf_fixture": b"fixture generator",
        "query-regression-bins/candidate/query_regression_runner": b"runner",
    }
    for rel, content in bins.items():
        path = candidate_artifact / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        path.chmod(0o755)

    if candidate is None:
        candidate = root / "candidate"
        (candidate / "tests" / "perf" / "query_cases" / "smoke").mkdir(parents=True)
        (candidate / "tests" / "perf" / "query_cases" / "smoke" / "case.toml").write_text(
            "[scenario]\nkind = \"direct_readable_sst\"\n", encoding="utf-8"
        )
        (candidate / ".github" / "scripts").mkdir(parents=True)
        (candidate / ".github" / "scripts" / "query-regression-run.py").write_text(
            "#!/usr/bin/env python3\nprint('candidate driver')\n", encoding="utf-8"
        )
        subprocess.run(["git", "init", "-q", str(candidate)], check=True)
        subprocess.run(["git", "-C", str(candidate), "config", "user.email", "test@example.com"], check=True)
        subprocess.run(["git", "-C", str(candidate), "config", "user.name", "test"], check=True)
        subprocess.run(["git", "-C", str(candidate), "add", "-A"], check=True)
        subprocess.run(["git", "-C", str(candidate), "commit", "-qm", "fixture"], check=True)
        candidate_sha = subprocess.run(
            ["git", "-C", str(candidate), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    else:
        candidate_sha = "b" * 40

    candidate_manifest = {
        "candidate_sha": candidate_sha,
        "binaries": [
            {"path": rel, "sha256": hashlib.sha256(content).hexdigest()}
            for rel, content in bins.items()
        ],
    }
    (candidate_artifact / "candidate-manifest.json").write_text(
        json.dumps(candidate_manifest), encoding="utf-8"
    )

    trusted_dir = root / "trusted"
    trusted_dir.mkdir()
    summary = "#!/usr/bin/env python3\nprint('trusted summary')\n"
    (trusted_dir / "query-regression-summary.py").write_text(summary, encoding="utf-8")
    (trusted_dir / "query-regression-pr-metadata.py").write_text("", encoding="utf-8")
    trusted_manifest = {
        "source_sha": base_sha,
        "files": {"query-regression-summary.py": hashlib.sha256(summary.encode()).hexdigest()},
    }
    (trusted_dir / "trusted-scripts-manifest.json").write_text(
        json.dumps(trusted_manifest, sort_keys=True), encoding="utf-8"
    )
    return base_artifact, candidate_artifact, trusted_dir, candidate_sha


class ControllerIntegrationTest(unittest.TestCase):
    def _run_controller(
        self,
        tmp: Path,
        *,
        phase: str | None = None,
        extra_env: dict[str, str] | None = None,
        extra_args: list[str] | None = None,
    ) -> tuple[subprocess.CompletedProcess[str], Path, Path]:
        state = tmp / "state"
        state.mkdir(exist_ok=True)
        (state / "pod" / "payload").mkdir(parents=True, exist_ok=True)
        (state / "pod" / "work").mkdir(parents=True, exist_ok=True)
        fake = state / "kubectl"
        fake.write_text(FAKE_KUBECTL, encoding="utf-8")
        fake.chmod(0o755)
        if phase is not None:
            (state / "phase").write_text(phase, encoding="utf-8")

        base_artifact, candidate_artifact, trusted_dir, candidate_sha = make_artifacts(tmp)
        result_dir = tmp / "result"
        result_dir.mkdir()
        output_file = tmp / "github-output"

        env = dict(os.environ)
        env.update(
            {
                "RUN_ID": "123456789",
                "RUN_ATTEMPT": "2",
                "JOB_IMAGE": controller.DEFAULT_IMAGE,
                "VERIFIED_BASE_SHA": BASE_SHA,
                "VERIFIED_CANDIDATE_SHA": candidate_sha,
                "CASE_PATHS": "all",
                "HTTP_TIMEOUT": "300",
                "ALLOW_LARGE_FIXTURE": "true",
                "CARGO_PROFILE": "nightly",
                "RUN_URL": "https://example.invalid/run/123",
                "CASE_NAME": "default case set",
                "BASE_REF": "main",
                "CANDIDATE_REF": "dev",
                "GITHUB_OUTPUT": str(output_file),
                "FAKE_KUBECTL_STATE": str(state),
            }
        )
        if extra_env:
            env.update(extra_env)
        args = [
            sys.executable,
            str(CONTROLLER_PATH),
            "--kubectl",
            str(fake),
            "--base-artifact-dir",
            str(base_artifact),
            "--candidate-artifact-dir",
            str(candidate_artifact),
            "--candidate-src",
            str(tmp / "candidate"),
            "--trusted-summary",
            str(trusted_dir / "query-regression-summary.py"),
            "--trusted-scripts-manifest",
            str(trusted_dir / "trusted-scripts-manifest.json"),
            "--payload-dir",
            str(tmp / "payload"),
            "--result-dir",
            str(result_dir),
            "--pod-ready-timeout",
            "30",
            "--run-timeout",
            "60",
            "--delete-timeout",
            "30",
        ]
        if extra_args:
            args.extend(extra_args)
        proc = subprocess.run(
            args,
            capture_output=True,
            text=True,
            timeout=180,
            env=env,
        )
        return proc, result_dir, output_file

    def test_success_flow_collects_results_and_deletes_exact_job(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-controller-") as tmp_str:
            tmp = Path(tmp_str)
            proc, result_dir, output_file = self._run_controller(tmp)
            self.assertEqual(proc.returncode, 0, proc.stderr + proc.stdout)
            self.assertEqual((output_file.read_text().strip()), "status=0")
            report = result_dir / "query-regression-work" / "smoke" / "query-regression-report.json"
            self.assertTrue(report.is_file(), f"report missing: {report}")
            self.assertTrue((result_dir / "query-regression-summary.md").is_file())
            self.assertTrue((result_dir / "query-regression-pod.log").is_file())
            self.assertTrue((result_dir / "query-regression-pod-describe.txt").is_file())
            deleted = (tmp / "state" / "deleted").read_text()
            self.assertIn("job.batch/query-regression-123456789-2 deleted", deleted)
            self.assertFalse((tmp / "state" / "job-exists").exists())
            # The pod entrypoint must have been armed and released.
            self.assertTrue((tmp / "state" / "pod" / "payload" / ".ready").exists())
            self.assertTrue((tmp / "state" / "pod" / "work" / ".collected").exists())
            # The trusted summary (base commit) is embedded, not the artifact's.
            payload_summary = tmp / "payload" / "repo" / ".github" / "scripts" / "query-regression-summary.py"
            self.assertIn("trusted summary", payload_summary.read_text(encoding="utf-8"))

    def test_pod_failure_still_collects_logs_and_cleans_up(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-controller-") as tmp_str:
            tmp = Path(tmp_str)
            proc, result_dir, output_file = self._run_controller(tmp, phase="Failed")
            self.assertEqual(proc.returncode, 0, proc.stderr + proc.stdout)
            self.assertEqual((output_file.read_text().strip()), "status=1")
            self.assertTrue((result_dir / "query-regression-pod.log").is_file())
            deleted = (tmp / "state" / "deleted").read_text()
            self.assertIn("job.batch/query-regression-123456789-2 deleted", deleted)

    def test_in_pod_sha_mismatch_aborts_and_cleans_up(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-controller-") as tmp_str:
            tmp = Path(tmp_str)
            proc, result_dir, output_file = self._run_controller(
                tmp, extra_env={"FAKE_KUBECTL_BAD_SHA": "1"}
            )
            self.assertEqual(proc.returncode, 0, proc.stderr + proc.stdout)
            self.assertEqual((output_file.read_text().strip()), "status=1")
            self.assertIn("in-pod sha256 mismatch", proc.stderr)
            deleted = (tmp / "state" / "deleted").read_text()
            self.assertIn("job.batch/query-regression-123456789-2 deleted", deleted)

    def test_copy_failure_propagates_and_cleans_up(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-controller-") as tmp_str:
            tmp = Path(tmp_str)
            proc, result_dir, output_file = self._run_controller(
                tmp, extra_env={"FAKE_KUBECTL_CP_FAILS": "1"}
            )
            self.assertEqual(proc.returncode, 0, proc.stderr + proc.stdout)
            self.assertEqual((output_file.read_text().strip()), "status=1")
            self.assertIn("kubectl cp of bins failed", proc.stderr)
            deleted = (tmp / "state" / "deleted").read_text()
            self.assertIn("job.batch/query-regression-123456789-2 deleted", deleted)

    def test_result_collection_failure_fails_even_when_status_zero(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-controller-") as tmp_str:
            tmp = Path(tmp_str)
            # The fake pod never creates the summary file: the benchmark
            # reports status 0 but mandatory collection must fail the run.
            proc, result_dir, output_file = self._run_controller(
                tmp, extra_env={"FAKE_KUBECTL_NO_SUMMARY": "1"}
            )
            self.assertEqual(proc.returncode, 0, proc.stderr + proc.stdout)
            self.assertEqual((output_file.read_text().strip()), "status=1")
            self.assertIn("could not collect required result", proc.stderr)
            deleted = (tmp / "state" / "deleted").read_text()
            self.assertIn("job.batch/query-regression-123456789-2 deleted", deleted)

    def test_delete_retries_then_succeeds(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-controller-") as tmp_str:
            tmp = Path(tmp_str)
            state = tmp / "state"
            state.mkdir(exist_ok=True)
            # First delete attempt fails, the retry succeeds.
            (state / "delete-fails-remaining").write_text("1", encoding="utf-8")
            proc, result_dir, output_file = self._run_controller(tmp)
            self.assertEqual(proc.returncode, 0, proc.stderr + proc.stdout)
            self.assertEqual((output_file.read_text().strip()), "status=0")
            # Exactly one successful delete after one failed attempt.
            deleted = (state / "deleted").read_text().splitlines()
            self.assertEqual(len(deleted), 1)
            self.assertIn("simulated delete failure", proc.stderr)

    def test_delete_failure_fails_run_even_when_benchmark_zero(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-controller-") as tmp_str:
            tmp = Path(tmp_str)
            state = tmp / "state"
            state.mkdir(exist_ok=True)
            # Delete always fails (more failures than the bounded retries).
            (state / "delete-fails-remaining").write_text("99", encoding="utf-8")
            proc, result_dir, output_file = self._run_controller(tmp)
            self.assertEqual(proc.returncode, 0, proc.stderr + proc.stdout)
            self.assertEqual((output_file.read_text().strip()), "status=1")
            self.assertIn("failed to delete job", proc.stderr)
            self.assertIn("workflow will fail", proc.stderr)

    def test_delete_leaves_job_fails_absence_check(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-controller-") as tmp_str:
            tmp = Path(tmp_str)
            proc, result_dir, output_file = self._run_controller(
                tmp, extra_env={"FAKE_KUBECTL_DELETE_LEAVES": "1"}
            )
            self.assertEqual(proc.returncode, 0, proc.stderr + proc.stdout)
            self.assertEqual((output_file.read_text().strip()), "status=1")
            self.assertIn("still present after foreground delete", proc.stderr)
            self.assertIn("workflow will fail", proc.stderr)

    def test_leaked_job_is_deleted_before_recreate(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-controller-") as tmp_str:
            tmp = Path(tmp_str)
            state = tmp / "state"
            state.mkdir()
            (state / "pod" / "payload").mkdir(parents=True)
            (state / "pod" / "work").mkdir(parents=True)
            # Simulate a Job leaked by a previously killed controller.
            (state / "job-exists").touch()
            fake = state / "kubectl"
            fake.write_text(FAKE_KUBECTL, encoding="utf-8")
            fake.chmod(0o755)
            base_artifact, candidate_artifact, trusted_dir, candidate_sha = make_artifacts(tmp)
            result_dir = tmp / "result"
            result_dir.mkdir()
            output_file = tmp / "github-output"
            env = dict(os.environ)
            env.update(
                {
                    "RUN_ID": "123456789",
                    "RUN_ATTEMPT": "2",
                    "JOB_IMAGE": controller.DEFAULT_IMAGE,
                    "VERIFIED_BASE_SHA": BASE_SHA,
                    "VERIFIED_CANDIDATE_SHA": candidate_sha,
                    "CASE_PATHS": "all",
                    "HTTP_TIMEOUT": "300",
                    "ALLOW_LARGE_FIXTURE": "true",
                    "CARGO_PROFILE": "nightly",
                    "GITHUB_OUTPUT": str(output_file),
                    "FAKE_KUBECTL_STATE": str(state),
                }
            )
            proc = subprocess.run(
                [
                    sys.executable,
                    str(CONTROLLER_PATH),
                    "--kubectl",
                    str(fake),
                    "--base-artifact-dir",
                    str(base_artifact),
                    "--candidate-artifact-dir",
                    str(candidate_artifact),
                    "--candidate-src",
                    str(tmp / "candidate"),
                    "--trusted-summary",
                    str(trusted_dir / "query-regression-summary.py"),
                    "--trusted-scripts-manifest",
                    str(trusted_dir / "trusted-scripts-manifest.json"),
                    "--payload-dir",
                    str(tmp / "payload"),
                    "--result-dir",
                    str(result_dir),
                    "--pod-ready-timeout",
                    "30",
                    "--run-timeout",
                    "60",
                    "--delete-timeout",
                    "30",
                ],
                capture_output=True,
                text=True,
                timeout=180,
                env=env,
            )
            self.assertEqual(proc.returncode, 0, proc.stderr + proc.stdout)
            self.assertEqual((output_file.read_text().strip()), "status=0")
            # The pre-existing leaked Job must have been deleted first, and the
            # run's own Job deleted at the end.
            deleted_lines = (state / "deleted").read_text().splitlines()
            self.assertEqual(len(deleted_lines), 2)
            for line in deleted_lines:
                self.assertEqual(line, "job.batch/query-regression-123456789-2 deleted")

    def test_sigterm_cancellation_still_deletes_the_job(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-controller-") as tmp_str:
            tmp = Path(tmp_str)
            state = tmp / "state"
            state.mkdir()
            (state / "pod" / "payload").mkdir(parents=True)
            (state / "pod" / "work").mkdir(parents=True)
            fake = state / "kubectl"
            fake.write_text(FAKE_KUBECTL, encoding="utf-8")
            fake.chmod(0o755)
            base_artifact, candidate_artifact, trusted_dir, candidate_sha = make_artifacts(tmp)
            result_dir = tmp / "result"
            result_dir.mkdir()
            output_file = tmp / "github-output"
            env = dict(os.environ)
            env.update(
                {
                    "RUN_ID": "123456789",
                    "RUN_ATTEMPT": "2",
                    "JOB_IMAGE": controller.DEFAULT_IMAGE,
                    "VERIFIED_BASE_SHA": BASE_SHA,
                    "VERIFIED_CANDIDATE_SHA": candidate_sha,
                    "CASE_PATHS": "all",
                    "HTTP_TIMEOUT": "300",
                    "ALLOW_LARGE_FIXTURE": "true",
                    "CARGO_PROFILE": "nightly",
                    "GITHUB_OUTPUT": str(output_file),
                    "FAKE_KUBECTL_STATE": str(state),
                    "FAKE_KUBECTL_NEVER_DONE": "1",
                }
            )
            proc = subprocess.Popen(
                [
                    sys.executable,
                    str(CONTROLLER_PATH),
                    "--kubectl",
                    str(fake),
                    "--base-artifact-dir",
                    str(base_artifact),
                    "--candidate-artifact-dir",
                    str(candidate_artifact),
                    "--candidate-src",
                    str(tmp / "candidate"),
                    "--trusted-summary",
                    str(trusted_dir / "query-regression-summary.py"),
                    "--trusted-scripts-manifest",
                    str(trusted_dir / "trusted-scripts-manifest.json"),
                    "--payload-dir",
                    str(tmp / "payload"),
                    "--result-dir",
                    str(result_dir),
                    "--pod-ready-timeout",
                    "30",
                    "--run-timeout",
                    "120",
                    "--delete-timeout",
                    "30",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=env,
            )
            # Wait until the Job has been created, then cancel like GitHub does.
            for _ in range(200):
                if (state / "job-exists").exists():
                    break
                time.sleep(0.1)
            self.assertTrue((state / "job-exists").exists(), "job was never created")
            proc.send_signal(signal.SIGTERM)
            stdout, stderr = proc.communicate(timeout=60)
            self.assertNotEqual(proc.returncode, 0)
            self.assertIn("cleaning up", stderr)
            # The exact Job must have been deleted despite the cancellation.
            self.assertIn(
                "job.batch/query-regression-123456789-2 deleted",
                (state / "deleted").read_text(),
            )
            # Cancellation skips optional diagnostics/collection: no pod log.
            self.assertFalse((result_dir / "query-regression-pod.log").exists())
            self.assertFalse((result_dir / "query-regression-pod-describe.txt").exists())


class TrustVerificationTest(unittest.TestCase):
    def test_manifest_sha_mismatch_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-trust-") as tmp_str:
            tmp = Path(tmp_str)
            base_artifact, candidate_artifact, trusted_dir, candidate_sha = make_artifacts(tmp)
            # Corrupt the candidate manifest SHA.
            manifest_path = candidate_artifact / "candidate-manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["candidate_sha"] = "c" * 40
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            env = dict(
                os.environ,
                RUN_ID="1",
                RUN_ATTEMPT="1",
                VERIFIED_BASE_SHA=BASE_SHA,
                VERIFIED_CANDIDATE_SHA=candidate_sha,
                GITHUB_OUTPUT="",
            )
            proc = subprocess.run(
                [
                    sys.executable,
                    str(CONTROLLER_PATH),
                    "--base-artifact-dir",
                    str(base_artifact),
                    "--candidate-artifact-dir",
                    str(candidate_artifact),
                    "--candidate-src",
                    str(tmp / "candidate"),
                    "--trusted-summary",
                    str(trusted_dir / "query-regression-summary.py"),
                    "--trusted-scripts-manifest",
                    str(trusted_dir / "trusted-scripts-manifest.json"),
                    "--payload-dir",
                    str(tmp / "payload"),
                    "--result-dir",
                    str(tmp / "result"),
                    "--kubectl",
                    "kubectl",
                ],
                capture_output=True,
                text=True,
                timeout=60,
                env=env,
            )
            self.assertEqual(proc.returncode, 1)
            self.assertIn("does not match the verified candidate SHA", proc.stderr)

    def test_checkout_sha_mismatch_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-trust-") as tmp_str:
            tmp = Path(tmp_str)
            base_artifact, candidate_artifact, trusted_dir, candidate_sha = make_artifacts(tmp)
            # Point the controller at a different checkout than the verified SHA.
            other = tmp / "other-checkout"
            other.mkdir()
            (other / "file").write_text("x", encoding="utf-8")
            subprocess.run(["git", "init", "-q", str(other)], check=True)
            subprocess.run(["git", "-C", str(other), "config", "user.email", "test@example.com"], check=True)
            subprocess.run(["git", "-C", str(other), "config", "user.name", "test"], check=True)
            subprocess.run(["git", "-C", str(other), "add", "-A"], check=True)
            subprocess.run(["git", "-C", str(other), "commit", "-qm", "other"], check=True)
            env = dict(
                os.environ,
                RUN_ID="1",
                RUN_ATTEMPT="1",
                VERIFIED_BASE_SHA=BASE_SHA,
                VERIFIED_CANDIDATE_SHA=candidate_sha,
                GITHUB_OUTPUT="",
            )
            proc = subprocess.run(
                [
                    sys.executable,
                    str(CONTROLLER_PATH),
                    "--base-artifact-dir",
                    str(base_artifact),
                    "--candidate-artifact-dir",
                    str(candidate_artifact),
                    "--candidate-src",
                    str(other),
                    "--trusted-summary",
                    str(trusted_dir / "query-regression-summary.py"),
                    "--trusted-scripts-manifest",
                    str(trusted_dir / "trusted-scripts-manifest.json"),
                    "--payload-dir",
                    str(tmp / "payload"),
                    "--result-dir",
                    str(tmp / "result"),
                    "--kubectl",
                    "kubectl",
                ],
                capture_output=True,
                text=True,
                timeout=60,
                env=env,
            )
            self.assertEqual(proc.returncode, 1)
            self.assertIn("does not match the verified candidate SHA", proc.stderr)

    def test_trusted_summary_mismatch_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-trust-") as tmp_str:
            tmp = Path(tmp_str)
            base_artifact, candidate_artifact, trusted_dir, candidate_sha = make_artifacts(tmp)
            (trusted_dir / "query-regression-summary.py").write_text(
                "#!/usr/bin/env python3\ntampered\n", encoding="utf-8"
            )
            env = dict(
                os.environ,
                RUN_ID="1",
                RUN_ATTEMPT="1",
                VERIFIED_BASE_SHA=BASE_SHA,
                VERIFIED_CANDIDATE_SHA=candidate_sha,
                GITHUB_OUTPUT="",
            )
            proc = subprocess.run(
                [
                    sys.executable,
                    str(CONTROLLER_PATH),
                    "--base-artifact-dir",
                    str(base_artifact),
                    "--candidate-artifact-dir",
                    str(candidate_artifact),
                    "--candidate-src",
                    str(tmp / "candidate"),
                    "--trusted-summary",
                    str(trusted_dir / "query-regression-summary.py"),
                    "--trusted-scripts-manifest",
                    str(trusted_dir / "trusted-scripts-manifest.json"),
                    "--payload-dir",
                    str(tmp / "payload"),
                    "--result-dir",
                    str(tmp / "result"),
                    "--kubectl",
                    "kubectl",
                ],
                capture_output=True,
                text=True,
                timeout=60,
                env=env,
            )
            self.assertEqual(proc.returncode, 1)
            self.assertIn("does not match the restore manifest", proc.stderr)

    def test_trusted_scripts_manifest_source_mismatch_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-trust-") as tmp_str:
            tmp = Path(tmp_str)
            base_artifact, candidate_artifact, trusted_dir, candidate_sha = make_artifacts(tmp)
            manifest_path = trusted_dir / "trusted-scripts-manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["source_sha"] = "d" * 40
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            env = dict(
                os.environ,
                RUN_ID="1",
                RUN_ATTEMPT="1",
                VERIFIED_BASE_SHA=BASE_SHA,
                VERIFIED_CANDIDATE_SHA=candidate_sha,
                GITHUB_OUTPUT="",
            )
            proc = subprocess.run(
                [
                    sys.executable,
                    str(CONTROLLER_PATH),
                    "--base-artifact-dir",
                    str(base_artifact),
                    "--candidate-artifact-dir",
                    str(candidate_artifact),
                    "--candidate-src",
                    str(tmp / "candidate"),
                    "--trusted-summary",
                    str(trusted_dir / "query-regression-summary.py"),
                    "--trusted-scripts-manifest",
                    str(trusted_dir / "trusted-scripts-manifest.json"),
                    "--payload-dir",
                    str(tmp / "payload"),
                    "--result-dir",
                    str(tmp / "result"),
                    "--kubectl",
                    "kubectl",
                ],
                capture_output=True,
                text=True,
                timeout=60,
                env=env,
            )
            self.assertEqual(proc.returncode, 1)
            self.assertIn("trusted scripts manifest source", proc.stderr)


@unittest.skipIf(yaml is None, "PyYAML not available")
class WorkflowStructuralTest(unittest.TestCase):
    """Structural assertions on query-regression.yml for the review findings."""

    WORKFLOW = Path(__file__).resolve().parents[2] / ".github" / "workflows" / "query-regression.yml"

    def _workflow(self) -> dict:
        with open(self.WORKFLOW, encoding="utf-8") as fh:
            return yaml.safe_load(fh)

    def test_build_job_outputs_verified_shas_and_artifact_ids(self) -> None:
        build = self._workflow()["jobs"]["build"]
        outputs = build["outputs"]
        self.assertIn("verified-base-sha", outputs)
        self.assertIn("verified-candidate-sha", outputs)
        self.assertIn("base-artifact-id", outputs)
        self.assertIn("candidate-artifact-id", outputs)
        # The resolve step must write real job outputs, not just GITHUB_ENV.
        resolve = next(s for s in build["steps"] if s.get("id") == "resolve")
        self.assertIn("printf 'verified-base-sha=", resolve["run"])
        self.assertIn("printf 'verified-candidate-sha=", resolve["run"])
        self.assertIn('"${GITHUB_OUTPUT}"', resolve["run"])

    def test_controller_uses_build_outputs_not_pr_head(self) -> None:
        ctrl = self._workflow()["jobs"]["query-regression-controller"]
        self.assertEqual(
            ctrl["env"]["VERIFIED_BASE_SHA"], "${{ needs.build.outputs.verified-base-sha }}"
        )
        self.assertEqual(
            ctrl["env"]["VERIFIED_CANDIDATE_SHA"], "${{ needs.build.outputs.verified-candidate-sha }}"
        )
        # Checkouts must use the verified SHAs from the build outputs, never
        # the PR head SHA.
        for step in ctrl["steps"]:
            if step.get("uses", "").startswith("actions/checkout"):
                self.assertNotIn("head.sha", json.dumps(step["with"]))
        checkout = next(s for s in ctrl["steps"] if s.get("name") == "Checkout candidate source")
        self.assertEqual(
            checkout["with"]["ref"], "${{ needs.build.outputs.verified-candidate-sha }}"
        )
        trusted_checkout = next(s for s in ctrl["steps"] if s.get("name") == "Checkout trusted source")
        self.assertEqual(
            trusted_checkout["with"]["ref"], "${{ needs.build.outputs.verified-base-sha }}"
        )

    def test_trusted_scripts_restored_from_verified_base_commit(self) -> None:
        ctrl = self._workflow()["jobs"]["query-regression-controller"]
        restore = next(s for s in ctrl["steps"] if s.get("name") == "Restore trusted query regression scripts")
        run = restore["run"]
        self.assertIn("${RUNNER_TEMP}/query-regression-trusted-scripts", run)
        self.assertIn("git -C \"${GITHUB_WORKSPACE}/trusted-src\" show", run)
        self.assertIn("${VERIFIED_BASE_SHA}:.github/scripts/", run)
        for script in (
            "query-regression-ack-controller.py",
            "query-regression-summary.py",
            "query-regression-pr-metadata.py",
        ):
            self.assertIn(script, run)
        self.assertIn("trusted-scripts-manifest.json", run)

    def test_controller_executed_from_runner_temp_never_from_artifact(self) -> None:
        ctrl = self._workflow()["jobs"]["query-regression-controller"]
        controller_step = next(
            s for s in ctrl["steps"] if s.get("name") == "Run ACK query regression controller"
        )
        run = controller_step["run"]
        # The executed controller must come from the restored trusted dir, not
        # from the downloaded build artifact.
        self.assertIn('python3 "${TRUSTED_SCRIPTS_DIR}/query-regression-ack-controller.py"', run)
        self.assertNotIn('python3 "${GITHUB_WORKSPACE}/query-regression-ack-controller.py"', run)
        self.assertNotIn("query-regression-artifacts/query-regression-ack-controller.py", run)
        self.assertIn("--trusted-summary", run)
        self.assertIn("--trusted-scripts-manifest", run)

    def test_artifacts_downloaded_by_exact_id(self) -> None:
        ctrl = self._workflow()["jobs"]["query-regression-controller"]
        downloads = [s for s in ctrl["steps"] if "Download query regression" in s.get("name", "")]
        self.assertEqual(len(downloads), 2)
        for step in downloads:
            self.assertIn("artifact-ids", step["with"])
        base = next(s for s in downloads if "base" in s["name"])
        candidate = next(s for s in downloads if "candidate" in s["name"])
        self.assertEqual(base["with"]["artifact-ids"], "${{ needs.build.outputs.base-artifact-id }}")
        self.assertEqual(
            candidate["with"]["artifact-ids"], "${{ needs.build.outputs.candidate-artifact-id }}"
        )

    def test_base_artifact_uploaded_before_candidate_build(self) -> None:
        build = self._workflow()["jobs"]["build"]
        step_names = [s.get("name") for s in build["steps"]]
        base_upload = step_names.index("Upload query regression base binaries")
        switch = step_names.index("Switch source to candidate")
        candidate_build = step_names.index("Build candidate greptime and query regression helpers")
        candidate_upload = step_names.index("Upload query regression candidate binaries")
        self.assertLess(base_upload, switch)
        self.assertLess(switch, candidate_build)
        self.assertLess(candidate_build, candidate_upload)

    def test_ack_job_runs_perf_only(self) -> None:
        ctrl = self._workflow()["jobs"]["query-regression-controller"]
        controller_step = next(
            s for s in ctrl["steps"] if s.get("name") == "Run ACK query regression controller"
        )
        # The controller script is the only thing executed in the controller
        # job; the pod entrypoint template (tested separately) runs perf only.
        self.assertIn("query-regression-ack-controller.py", controller_step["run"])
        # Tooling tests run in the build job, never in the controller job.
        build = self._workflow()["jobs"]["build"]
        build_steps = "\n".join(s.get("run", "") or "" for s in build["steps"])
        self.assertIn("test_query_regression_runner_compaction_toctou.py", build_steps)
        self.assertIn("test_query_regression_runner_otlp_trace_load.py", build_steps)
        self.assertIn("test_query_regression_summary_otlp.py", build_steps)
        self.assertIn("test_query_regression_case_selection.py", build_steps)
        ctrl_steps = "\n".join(s.get("run", "") or "" for s in ctrl["steps"])
        self.assertNotIn("test_query_regression_runner_compaction_toctou.py", ctrl_steps)

    def test_no_nodepool_scale_api_and_no_credentials(self) -> None:
        text = self.WORKFLOW.read_text(encoding="utf-8")
        for banned in (
            "desired-size",
            "autoscaling/nodepools",
            "nodepool desired",
            "scale-to-zero",
        ):
            self.assertNotIn(banned, text)
        # Kubeconfig must be supplied only via the dedicated secret, and only
        # in the controller job's prepare step.
        self.assertIn("secrets.ACK_KUBECONFIG", text)
        self.assertIn("JOB_IMAGE: greptime-registry.cn-hangzhou.cr.aliyuncs.com/greptime/greptimedb-query-regression-runner@sha256:", text)

    def test_workflow_actions_are_preexisting_pins(self) -> None:
        wf = self._workflow()
        actions = set()
        for job in wf["jobs"].values():
            for step in job["steps"]:
                if "uses" in step:
                    actions.add(step["uses"])
        for action in actions:
            self.assertIn(action, ("actions/checkout@v4", "actions/upload-artifact@v4", "actions/download-artifact@v4"))

    def test_second_process_cleanup_step(self) -> None:
        ctrl = self._workflow()["jobs"]["query-regression-controller"]
        step_names = [s.get("name") for s in ctrl["steps"]]
        cleanup_name = "Clean up ACK benchmark job (second process, best effort)"
        self.assertIn(cleanup_name, step_names)
        cleanup = ctrl["steps"][step_names.index(cleanup_name)]
        self.assertEqual(cleanup["if"], "always()")
        # The cleanup step must run after the controller step.
        self.assertLess(
            step_names.index("Run ACK query regression controller"),
            step_names.index(cleanup_name),
        )
        run = cleanup["run"]
        # Uses the same trusted script with deterministic exact-name
        # validation; deletes only the exact Job for this run.
        self.assertIn("query-regression-ack-controller.py", run)
        self.assertIn("--cleanup-only", run)
        self.assertIn("--run-id", run)
        self.assertIn("--run-attempt", run)
        self.assertIn("--delete-attempts", run)
        # Executed from the restored trusted scripts dir (never an artifact).
        self.assertIn('trusted_dir="${TRUSTED_SCRIPTS_DIR:-}"', run)
        self.assertIn('python3 "${trusted_dir}/query-regression-ack-controller.py"', run)
        # The second process must never call nodepool scale APIs or read
        # anything but the exact Job name.
        self.assertNotIn("desired-size", run)
        self.assertNotIn("autoscaling/nodepools", run)

    def test_workflow_timeout_leaves_setup_and_outer_cleanup_margin(self) -> None:
        ctrl = self._workflow()["jobs"]["query-regression-controller"]
        timeout_minutes = int(ctrl["timeout-minutes"])
        self.assertEqual(timeout_minutes * 60, controller.WORKFLOW_JOB_TIMEOUT_SECONDS)
        # The workflow timeout must strictly exceed the controller lifecycle:
        # the difference is the explicit setup + outer-cleanup margin covering
        # checkout, trusted-script restore, artifact download/attestation,
        # kubeconfig setup, and the second-process cleanup-only step.
        margin = controller.WORKFLOW_JOB_TIMEOUT_SECONDS - controller.LIFECYCLE_TIMEOUT_DEFAULT
        self.assertGreater(margin, 0)
        self.assertGreaterEqual(margin, controller.SETUP_AND_OUTER_CLEANUP_MARGIN_MINIMUM)
        self.assertGreater(margin, controller.CLEANUP_ONLY_BUDGET)
        # The internal cleanup reserve must be strictly smaller than the
        # lifecycle and strictly smaller than the job timeout.
        self.assertLess(controller.CLEANUP_RESERVE_DEFAULT, controller.LIFECYCLE_TIMEOUT_DEFAULT)
        self.assertLess(controller.CLEANUP_RESERVE_DEFAULT, controller.WORKFLOW_JOB_TIMEOUT_SECONDS)


if __name__ == "__main__":
    unittest.main()
