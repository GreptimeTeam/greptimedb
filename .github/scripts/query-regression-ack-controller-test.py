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

"""Focused tests for the trusted query-regression ACK controller (v2).

Covers the secure-architecture contract: fixed namespace/image, Job hardening
fields (TTL, podReplacementPolicy, tokenless workload SA), payload byte cap,
kubeconfig exec-plugin fail-closed validation, kubectl context-override
fail-closed pinning (--context deleted, KUBECTL_CONTEXT stripped so the
validated current-context is the only context ever used), deterministic
exact-name cleanup, lifecycle budget clamping, symlink/containment rejection of
the candidate-controlled payload (now sourced from the candidate artifact), and
the regenerated comment metadata.
"""

import hashlib
import importlib.util
import io
import json
import os
import re
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
TRUSTED_SOURCE_SHA = "d" * 40  # default-branch HEAD the trusted scripts come from

GOOD_KUBECONFIG = """\
apiVersion: v1
kind: Config
current-context: ack
clusters:
- name: ack
  cluster:
    server: https://api.ack.example.com
contexts:
- name: ack
  context:
    cluster: ack
    user: broker
users:
- name: broker
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1
      command: /usr/local/bin/ack-broker
      env:
      - name: REGION
        value: cn-hangzhou
"""


def write_kubeconfig(root: Path, content: str = GOOD_KUBECONFIG) -> Path:
    path = root / "ack-kubeconfig.yaml"
    path.write_text(content, encoding="utf-8")
    return path


def kubeconfig_with(
    users_block: str,
    *,
    current_context: str = "ack",
    context_user: str = "x",
    context_cluster: str = "ack",
    extra_top: str = "",
) -> str:
    """Structurally valid kubeconfig scaffold for validation tests."""
    return f"""\
apiVersion: v1
kind: Config
current-context: {current_context}
clusters:
- name: ack
  cluster:
    server: https://api.ack.example.com
contexts:
- name: ack
  context:
    cluster: {context_cluster}
    user: {context_user}
{extra_top}users:
{users_block}
"""


def render_manifest() -> dict:
    return controller.build_manifest(
        run_id=123456789,
        run_attempt=2,
        job_name="query-regression-123456789-2",
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

    def test_ttl_and_pod_replacement_and_workload_sa(self) -> None:
        job = render_manifest()
        self.assertEqual(job["spec"]["ttlSecondsAfterFinished"], 600)
        self.assertEqual(job["spec"]["podReplacementPolicy"], "Failed")
        spec = job["spec"]["template"]["spec"]
        self.assertEqual(spec["serviceAccountName"], "query-regression-workload")
        # Tokenless: the workload SA must never mount a token.
        self.assertIs(spec["automountServiceAccountToken"], False)

    def test_fixed_namespace_and_image(self) -> None:
        job = render_manifest()
        self.assertEqual(job["metadata"]["namespace"], "query-regression-perf")
        self.assertEqual(controller.DEFAULT_NAMESPACE, "query-regression-perf")
        container = job["spec"]["template"]["spec"]["containers"][0]
        self.assertEqual(container["image"], controller.DEFAULT_IMAGE)
        self.assertIn("@sha256:", container["image"])

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
        # the documented setup + outer-cleanup margin (checkout, admission,
        # artifact download/attestation, kubeconfig setup, and the
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
        # The conservative payload cap must sit under the 40Gi ephemeral limit.
        self.assertLess(controller.PAYLOAD_BYTES_CAP_DEFAULT, 40 * 1024**3)


class PayloadCapTest(unittest.TestCase):
    def test_measure_payload_accepts_under_cap(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-cap-") as tmp_str:
            payload = Path(tmp_str) / "payload"
            payload.mkdir()
            (payload / "small.bin").write_bytes(b"\0" * 100)
            size = controller.measure_payload(payload, 1024)
            self.assertEqual(size, 100)

    def test_measure_payload_rejects_over_cap(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-cap-") as tmp_str:
            payload = Path(tmp_str) / "payload"
            payload.mkdir()
            (payload / "big.bin").write_bytes(b"\0" * 2048)
            with self.assertRaises(controller.ControllerError) as ctx:
                controller.measure_payload(payload, 1000)
            self.assertIn("exceeds the pre-Job cap", str(ctx.exception))

    def test_measure_payload_counts_nested_files(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-cap-") as tmp_str:
            payload = Path(tmp_str) / "payload"
            (payload / "a" / "b").mkdir(parents=True)
            (payload / "a" / "one").write_bytes(b"\0" * 10)
            (payload / "a" / "b" / "two").write_bytes(b"\0" * 20)
            self.assertEqual(controller.measure_payload(payload, 1000), 30)


class KubeconfigValidationTest(unittest.TestCase):
    def test_valid_exec_kubeconfig_passes(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-kc-") as tmp_str:
            path = write_kubeconfig(Path(tmp_str))
            controller.validate_kubeconfig(path)

    def test_missing_kubeconfig_fails_closed(self) -> None:
        with self.assertRaises(controller.ControllerError) as ctx:
            controller.validate_kubeconfig(Path("/nonexistent/ack-kubeconfig"))
        self.assertIn("not found", str(ctx.exception))

    def test_embedded_token_rejected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-kc-") as tmp_str:
            path = write_kubeconfig(Path(tmp_str), """\
apiVersion: v1
kind: Config
users:
- name: x
  user:
    token: abc123
""")
            with self.assertRaises(controller.ControllerError) as ctx:
                controller.validate_kubeconfig(path)
            self.assertIn("token", str(ctx.exception))

    def test_token_file_rejected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-kc-") as tmp_str:
            path = write_kubeconfig(Path(tmp_str), """\
apiVersion: v1
kind: Config
users:
- name: x
  user:
    tokenFile: /var/run/secrets/token
""")
            with self.assertRaises(controller.ControllerError) as ctx:
                controller.validate_kubeconfig(path)
            self.assertIn("tokenFile", str(ctx.exception))

    def test_client_key_data_rejected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-kc-") as tmp_str:
            path = write_kubeconfig(Path(tmp_str), """\
apiVersion: v1
kind: Config
users:
- name: x
  user:
    client-key-data: LS0tLS1CRUdJTiBSU0EgUFJJVkFURSBLRVktLS0tLQo=
""")
            with self.assertRaises(controller.ControllerError) as ctx:
                controller.validate_kubeconfig(path)
            self.assertIn("client-key-data", str(ctx.exception))

    def test_client_certificate_data_rejected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-kc-") as tmp_str:
            path = write_kubeconfig(Path(tmp_str), """\
apiVersion: v1
kind: Config
users:
- name: x
  user:
    client-certificate-data: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCg==
""")
            with self.assertRaises(controller.ControllerError) as ctx:
                controller.validate_kubeconfig(path)
            self.assertIn("client-certificate-data", str(ctx.exception))

    def test_username_password_rejected(self) -> None:
        for content in (
            "users:\n- name: x\n  user:\n    username: admin\n    password: s3cret\n",
            "users:\n- name: x\n  user:\n    username: admin\n",
            "users:\n- name: x\n  user:\n    password: s3cret\n",
        ):
            with tempfile.TemporaryDirectory(prefix="qr-ack-kc-") as tmp_str:
                path = write_kubeconfig(Path(tmp_str), "apiVersion: v1\nkind: Config\n" + content)
                with self.assertRaises(controller.ControllerError) as ctx:
                    controller.validate_kubeconfig(path)
                self.assertIn("banned key", str(ctx.exception))

    def test_auth_provider_rejected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-kc-") as tmp_str:
            path = write_kubeconfig(Path(tmp_str), """\
apiVersion: v1
kind: Config
users:
- name: x
  user:
    auth-provider:
      name: gcp
""")
            with self.assertRaises(controller.ControllerError) as ctx:
                controller.validate_kubeconfig(path)
            self.assertIn("auth-provider", str(ctx.exception))

    def test_exec_env_secret_name_rejected(self) -> None:
        for name in ("TOKEN", "ACCESS_KEY_ID", "CLIENT_SECRET", "AK_ID", "PRIVATE_KEY"):
            with tempfile.TemporaryDirectory(prefix="qr-ack-kc-") as tmp_str:
                path = write_kubeconfig(Path(tmp_str), kubeconfig_with(f"""\
- name: x
  user:
    exec:
      command: broker
      env:
      - name: {name}
        value: some-value
"""))
                with self.assertRaises(controller.ControllerError) as ctx:
                    controller.validate_kubeconfig(path)
                self.assertIn("static secret", str(ctx.exception))

    def test_exec_env_credential_value_rejected(self) -> None:
        jwt = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U"
        with tempfile.TemporaryDirectory(prefix="qr-ack-kc-") as tmp_str:
            path = write_kubeconfig(Path(tmp_str), kubeconfig_with(f"""\
- name: x
  user:
    exec:
      command: broker
      env:
      - name: CRED
        value: {jwt}
"""))
            with self.assertRaises(controller.ControllerError) as ctx:
                controller.validate_kubeconfig(path)
            self.assertIn("credential", str(ctx.exception))

    def test_pem_value_rejected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-kc-") as tmp_str:
            path = write_kubeconfig(Path(tmp_str), kubeconfig_with("""\
- name: x
  user:
    exec:
      command: broker
      env:
      - name: CRED
        value: |
          -----BEGIN RSA PRIVATE KEY-----
          MIIEpAIBAAKCAQEA
          -----END RSA PRIVATE KEY-----
"""))
            with self.assertRaises(controller.ControllerError) as ctx:
                controller.validate_kubeconfig(path)
            self.assertIn("credential", str(ctx.exception))

    def test_no_exec_plugin_rejected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-kc-") as tmp_str:
            path = write_kubeconfig(Path(tmp_str), kubeconfig_with("""\
- name: x
  user: {}
"""))
            with self.assertRaises(controller.ControllerError) as ctx:
                controller.validate_kubeconfig(path)
            self.assertIn("exec credential plugin", str(ctx.exception))

    def test_non_exec_active_user_rejected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-kc-") as tmp_str:
            path = write_kubeconfig(Path(tmp_str), kubeconfig_with("""\
- name: x
  user:
    exec:
      command: broker
- name: y
  user: {}
"""))
            # User x (the active user) uses exec -> passes.
            controller.validate_kubeconfig(path)
        with tempfile.TemporaryDirectory(prefix="qr-ack-kc-") as tmp_str:
            path = write_kubeconfig(
                Path(tmp_str),
                kubeconfig_with("""\
- name: x
  user:
    exec:
      command: broker
- name: y
  user: {}
""", context_user="y"),
            )
            # The active user y has no exec -> rejected even though an
            # inactive user x has one.
            with self.assertRaises(controller.ControllerError) as ctx:
                controller.validate_kubeconfig(path)
            self.assertIn("does not use an exec credential plugin", str(ctx.exception))

    def test_inactive_exec_only_rejected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-kc-") as tmp_str:
            path = write_kubeconfig(Path(tmp_str), kubeconfig_with("""\
- name: x
  user:
    exec:
      command: broker
- name: y
  user: {}
""", context_user="y"))
            # The ACTIVE user y has no exec auth; the exec plugin on the
            # inactive user x must not count.
            with self.assertRaises(controller.ControllerError) as ctx:
                controller.validate_kubeconfig(path)
            self.assertIn("does not use an exec credential plugin", str(ctx.exception))

    def test_active_static_auth_rejected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-kc-") as tmp_str:
            path = write_kubeconfig(Path(tmp_str), kubeconfig_with("""\
- name: x
  user:
    token: abc123
"""))
            with self.assertRaises(controller.ControllerError) as ctx:
                controller.validate_kubeconfig(path)
            self.assertIn("banned key", str(ctx.exception))

    def test_file_backed_client_cert_auth_rejected(self) -> None:
        """File-backed client-certificate/client-key are static credentials
        and must be rejected even though no inline data is present."""
        for content in (
            "users:\n- name: x\n  user:\n    client-certificate: /etc/kubernetes/client.crt\n",
            "users:\n- name: x\n  user:\n    client-key: /etc/kubernetes/client.key\n",
        ):
            with tempfile.TemporaryDirectory(prefix="qr-ack-kc-") as tmp_str:
                path = write_kubeconfig(Path(tmp_str), "apiVersion: v1\nkind: Config\n" + content)
                with self.assertRaises(controller.ControllerError) as ctx:
                    controller.validate_kubeconfig(path)
                self.assertIn("banned key", str(ctx.exception))
                self.assertIn("client-", str(ctx.exception))

    def test_mixed_exec_and_client_cert_auth_rejected(self) -> None:
        """An exec block never excuses static client certificate/key auth:
        the active user's only authentication mechanism must be exec."""
        for static_line in (
            "    client-certificate: /etc/kubernetes/client.crt\n",
            "    client-key: /etc/kubernetes/client.key\n",
            "    client-certificate-data: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCg==\n",
            "    client-key-data: LS0tLS1CRUdJTiBSU0EgUFJJVkFURSBLRVktLS0tLQo=\n",
        ):
            with tempfile.TemporaryDirectory(prefix="qr-ack-kc-") as tmp_str:
                path = write_kubeconfig(Path(tmp_str), kubeconfig_with(f"""\
- name: x
  user:
    exec:
      command: broker
{static_line}"""))
                with self.assertRaises(controller.ControllerError) as ctx:
                    controller.validate_kubeconfig(path)
                self.assertIn("banned key", str(ctx.exception))

    def test_inactive_static_cert_user_rejected(self) -> None:
        """Inactive users must also carry no static credential fields: a
        file-backed client cert on an unused user rejects even though the
        active user is exec-only."""
        with tempfile.TemporaryDirectory(prefix="qr-ack-kc-") as tmp_str:
            path = write_kubeconfig(Path(tmp_str), kubeconfig_with("""\
- name: x
  user:
    exec:
      command: broker
- name: y
  user:
    client-certificate: /etc/kubernetes/y.crt
    client-key: /etc/kubernetes/y.key
"""))
            with self.assertRaises(controller.ControllerError) as ctx:
                controller.validate_kubeconfig(path)
            self.assertIn("banned key", str(ctx.exception))

    def test_active_exec_only_success(self) -> None:
        """An active user whose only authentication mechanism is exec passes;
        the exec block may still carry non-secret env."""
        with tempfile.TemporaryDirectory(prefix="qr-ack-kc-") as tmp_str:
            path = write_kubeconfig(Path(tmp_str), kubeconfig_with("""\
- name: x
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1
      command: /usr/local/bin/ack-broker
      env:
      - name: REGION
        value: cn-hangzhou
"""))
            controller.validate_kubeconfig(path)

    def test_missing_current_context_rejected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-kc-") as tmp_str:
            path = write_kubeconfig(Path(tmp_str), """\
apiVersion: v1
kind: Config
clusters:
- name: ack
  cluster:
    server: https://api.ack.example.com
contexts:
- name: ack
  context:
    cluster: ack
    user: x
users:
- name: x
  user:
    exec:
      command: broker
""")
            with self.assertRaises(controller.ControllerError) as ctx:
                controller.validate_kubeconfig(path)
            self.assertIn("no current-context", str(ctx.exception))

    def test_unknown_current_context_rejected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-kc-") as tmp_str:
            path = write_kubeconfig(Path(tmp_str), kubeconfig_with("""\
- name: x
  user:
    exec:
      command: broker
""", current_context="missing"))
            with self.assertRaises(controller.ControllerError) as ctx:
                controller.validate_kubeconfig(path)
            self.assertIn("is not in contexts", str(ctx.exception))

    def test_context_references_unknown_cluster_rejected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-kc-") as tmp_str:
            path = write_kubeconfig(Path(tmp_str), kubeconfig_with("""\
- name: x
  user:
    exec:
      command: broker
""", context_cluster="ghost"))
            with self.assertRaises(controller.ControllerError) as ctx:
                controller.validate_kubeconfig(path)
            self.assertIn("unknown cluster", str(ctx.exception))

    def test_context_references_unknown_user_rejected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-kc-") as tmp_str:
            path = write_kubeconfig(Path(tmp_str), kubeconfig_with("""\
- name: x
  user:
    exec:
      command: broker
""", context_user="ghost"))
            with self.assertRaises(controller.ControllerError) as ctx:
                controller.validate_kubeconfig(path)
            self.assertIn("unknown user", str(ctx.exception))

    def test_exec_api_version_invalid_rejected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-kc-") as tmp_str:
            path = write_kubeconfig(Path(tmp_str), kubeconfig_with("""\
- name: x
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1alpha1
      command: broker
"""))
            with self.assertRaises(controller.ControllerError) as ctx:
                controller.validate_kubeconfig(path)
            self.assertIn("apiVersion", str(ctx.exception))

    def test_exec_args_malformed_rejected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-kc-") as tmp_str:
            path = write_kubeconfig(Path(tmp_str), kubeconfig_with("""\
- name: x
  user:
    exec:
      command: broker
      args: not-a-list
"""))
            with self.assertRaises(controller.ControllerError) as ctx:
                controller.validate_kubeconfig(path)
            self.assertIn("args must be a list of strings", str(ctx.exception))

    def test_pyyaml_required_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-kc-") as tmp_str:
            path = write_kubeconfig(Path(tmp_str))
            with mock.patch.object(controller, "yaml", None):
                with self.assertRaises(controller.ControllerError) as ctx:
                    controller.validate_kubeconfig(path)
            self.assertIn("PyYAML is required", str(ctx.exception))

    def test_never_prints_kubeconfig_content(self) -> None:
        secret = "SUPERSECRETVALUE1234567890"
        with tempfile.TemporaryDirectory(prefix="qr-ack-kc-") as tmp_str:
            path = write_kubeconfig(Path(tmp_str), f"""\
apiVersion: v1
kind: Config
users:
- name: x
  user:
    token: {secret}
""")
            stderr = io.StringIO()
            with mock.patch.object(sys, "stderr", stderr):
                with self.assertRaises(controller.ControllerError) as ctx:
                    controller.validate_kubeconfig(path)
            message = str(ctx.exception)
            output = stderr.getvalue() + message + str(path)
            self.assertNotIn(secret, output)
            # The error names the path and the banned key only.
            self.assertIn("token", message)


class KubectlVersionPinTest(unittest.TestCase):
    """The exec/cp protocol is an ABI pinned to KUBECTL_PINNED_VERSION."""

    def _write_fake_kubectl(self, tmp: Path, version: str) -> Path:
        fake = tmp / "kubectl"
        fake.write_text(
            '#!/usr/bin/env bash\n'
            'set -u\n'
            'if [ "$1" = "version" ] && [ "$2" = "--client" ]; then\n'
            f'  echo \'{{"clientVersion":{{"gitVersion":"{version}"}}}}\'\n'
            '  exit 0\n'
            'fi\n'
            'echo "unexpected kubectl invocation: $*" >&2\n'
            'exit 1\n',
            encoding="utf-8",
        )
        fake.chmod(0o755)
        return fake

    def test_pinned_version_passes(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-ver-") as tmp_str:
            fake = self._write_fake_kubectl(Path(tmp_str), controller.KUBECTL_PINNED_VERSION)
            kc = controller.Kubectl(str(fake), kubeconfig=None)
            self.assertEqual(kc.binary, str(fake))

    def test_wrong_patch_version_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-ver-") as tmp_str:
            fake = self._write_fake_kubectl(Path(tmp_str), "v1.34.9")
            with self.assertRaises(controller.ControllerError) as ctx:
                controller.Kubectl(str(fake), kubeconfig=None)
            self.assertIn("does not match the pinned ABI", str(ctx.exception))
            self.assertIn(controller.KUBECTL_PINNED_VERSION, str(ctx.exception))

    def test_wrong_minor_version_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-ver-") as tmp_str:
            fake = self._write_fake_kubectl(Path(tmp_str), "v1.33.1")
            with self.assertRaises(controller.ControllerError) as ctx:
                controller.Kubectl(str(fake), kubeconfig=None)
            self.assertIn("does not match the pinned ABI", str(ctx.exception))

    def test_unparsable_version_output_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-ver-") as tmp_str:
            fake = Path(tmp_str) / "kubectl"
            fake.write_text('#!/usr/bin/env bash\necho "not json"\nexit 0\n', encoding="utf-8")
            fake.chmod(0o755)
            with self.assertRaises(controller.ControllerError) as ctx:
                controller.Kubectl(str(fake), kubeconfig=None)
            self.assertIn("unparsable kubectl version output", str(ctx.exception))

    def test_exec_protocol_constants_are_canonical(self) -> None:
        # Every exec/cp tuple the VAP allows must come from these constants,
        # and the constants must be internally consistent with the payload.
        self.assertEqual(
            controller.EXEC_SCRIPTS,
            [
                controller.EXEC_LAYOUT_CHECK,
                controller.EXEC_CHMOD,
                controller.EXEC_SHA256,
                controller.EXEC_READY,
                controller.EXEC_DONE,
                controller.EXEC_STATUS,
                controller.EXEC_COLLECTED,
            ],
        )
        self.assertEqual(
            controller.EXEC_SHA256, "sha256sum " + " ".join(controller.BIN_PATH_MAP)
        )
        self.assertEqual(
            controller.EXEC_STREAMS,
            {"stdin": False, "stdout": True, "stderr": True, "tty": False},
        )
        self.assertEqual(controller.CP_PUSH_COMMANDS, [["tar", "xmf", "-", "-C", "/payload"]])
        self.assertEqual(
            controller.CP_PULL_COMMANDS,
            [
                ["tar", "cf", "-", "/work/query-regression-work"],
                ["tar", "cf", "-", "/work/query-regression-summary.md"],
            ],
        )
        # No script in the canonical exec protocol may contain a single quote:
        # the VAP embeds them in single-quoted CEL strings.
        for script in controller.EXEC_SCRIPTS:
            self.assertNotIn("'", script)
            self.assertNotIn("\n", script)


class KubectlContextPinTest(unittest.TestCase):
    """Every kubectl call must use exactly the validated kubeconfig's
    current-context: the --context flag is deleted (argparse rejects it) and
    the native KUBECTL_CONTEXT env override is stripped from every kubectl
    subprocess."""

    def _write_fake_kubectl(self, tmp: Path, version: str) -> Path:
        fake = tmp / "kubectl"
        fake.write_text(
            '#!/usr/bin/env bash\n'
            'set -u\n'
            'if [ "$1" = "version" ] && [ "$2" = "--client" ]; then\n'
            f'  echo \'{{"clientVersion":{{"gitVersion":"{version}"}}}}\'\n'
            '  exit 0\n'
            'fi\n'
            'echo "unexpected kubectl invocation: $*" >&2\n'
            'exit 1\n',
            encoding="utf-8",
        )
        fake.chmod(0o755)
        return fake

    def test_env_strips_kubectl_context_override(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-ctx-") as tmp_str:
            fake = self._write_fake_kubectl(Path(tmp_str), controller.KUBECTL_PINNED_VERSION)
            with mock.patch.dict(
                os.environ,
                {"KUBECTL_CONTEXT": "evil-cluster", "KUBECONFIG": "/inherited/kubeconfig"},
            ):
                kc = controller.Kubectl(str(fake), kubeconfig="/validated/kubeconfig")
                env = kc._env()
            # The native override must never reach a kubectl subprocess, and
            # the validated kubeconfig is the only configuration source.
            self.assertNotIn("KUBECTL_CONTEXT", env)
            self.assertEqual(env["KUBECONFIG"], "/validated/kubeconfig")

    def test_run_never_passes_context_flag_or_env(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-ctx-") as tmp_str:
            fake = self._write_fake_kubectl(Path(tmp_str), controller.KUBECTL_PINNED_VERSION)
            with mock.patch.dict(os.environ, {"KUBECTL_CONTEXT": "evil-cluster"}):
                kc = controller.Kubectl(str(fake), kubeconfig="/validated/kubeconfig")
                captured: list[tuple[list[str], dict]] = []

                def fake_run(cmd, **kwargs):
                    if cmd[1:3] == ["version", "--client"]:
                        return subprocess.CompletedProcess(
                            cmd, 0, '{"clientVersion":{"gitVersion":"v1.34.2"}}', ""
                        )
                    captured.append((list(cmd), kwargs))
                    return subprocess.CompletedProcess(cmd, 0, "job.batch/x deleted", "")

                with mock.patch.object(controller.subprocess, "run", side_effect=fake_run):
                    kc.run(["delete", "job", "query-regression-1-1", "-n", "ns"], check=False)
            cmd, kwargs = captured[0]
            # No --context flag and no KUBECTL_CONTEXT env var in the subprocess.
            self.assertNotIn("--context", cmd)
            env = kwargs["env"]
            self.assertNotIn("KUBECTL_CONTEXT", env)
            self.assertEqual(env["KUBECONFIG"], "/validated/kubeconfig")

    def test_parse_args_rejects_context_flag(self) -> None:
        stderr = io.StringIO()
        with mock.patch.object(sys, "stderr", stderr):
            with self.assertRaises(SystemExit) as ctx:
                controller.parse_args(["--context", "evil-cluster"])
        self.assertEqual(ctx.exception.code, 2)
        self.assertIn("unrecognized arguments", stderr.getvalue())
        # The parsed namespace carries no context override at all.
        self.assertFalse(hasattr(controller.parse_args([]), "context"))

    def test_parse_args_never_reads_kubectl_context_env(self) -> None:
        with mock.patch.dict(os.environ, {"KUBECTL_CONTEXT": "evil-cluster"}):
            args = controller.parse_args([])
        self.assertFalse(hasattr(args, "context"))


class DryRunTest(unittest.TestCase):
    def test_dry_run_renders_valid_manifest_without_cluster(self) -> None:
        env = dict(os.environ)
        env.update(
            {
                "SOURCE_RUN_ID": "123456789",
                "SOURCE_RUN_ATTEMPT": "2",
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
        self.assertEqual(manifest["metadata"]["namespace"], "query-regression-perf")
        self.assertEqual(manifest["kind"], "Job")
        self.assertEqual(manifest["apiVersion"], "batch/v1")
        self.assertEqual(manifest["spec"]["ttlSecondsAfterFinished"], 600)


class SymlinkContainmentTest(unittest.TestCase):
    """Candidate-controlled payload paths must reject symlinks and non-regular files.

    In v2 the candidate tests/perf tree and driver travel inside the candidate
    artifact (repo/...) because the controller never checks out PR code; the
    same containment rules apply against the candidate artifact root.
    """

    def _make_artifacts(self, root: Path) -> tuple[Path, Path, Path]:
        base_artifact, candidate_artifact, trusted_dir, _ = make_artifacts(root)
        return base_artifact, candidate_artifact, trusted_dir

    def _assemble_reusing(self, base_artifact: Path, candidate_artifact: Path, trusted_dir: Path, root: Path) -> None:
        controller.assemble_payload(
            base_artifact,
            candidate_artifact,
            trusted_dir / "query-regression-summary.py",
            root / "payload",
        )

    def test_rejects_absolute_symlink_in_case_tree(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-symlink-") as tmp_str:
            root = Path(tmp_str)
            base_artifact, candidate_artifact, trusted_dir = self._make_artifacts(root)
            (candidate_artifact / "repo" / "tests" / "perf" / "query_cases" / "smoke" / "evil").symlink_to("/etc")
            with self.assertRaises(controller.ControllerError) as ctx:
                self._assemble_reusing(base_artifact, candidate_artifact, trusted_dir, root)
            self.assertIn("symlink", str(ctx.exception))

    def test_rejects_relative_escape_symlink_in_case_tree(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-symlink-") as tmp_str:
            root = Path(tmp_str)
            base_artifact, candidate_artifact, trusted_dir = self._make_artifacts(root)
            (candidate_artifact / "repo" / "tests" / "perf" / "query_cases" / "smoke" / "escape").symlink_to(
                "../../../../../../outside"
            )
            with self.assertRaises(controller.ControllerError) as ctx:
                self._assemble_reusing(base_artifact, candidate_artifact, trusted_dir, root)
            self.assertIn("symlink", str(ctx.exception))

    def test_rejects_in_tree_symlink(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-symlink-") as tmp_str:
            root = Path(tmp_str)
            base_artifact, candidate_artifact, trusted_dir = self._make_artifacts(root)
            target = candidate_artifact / "repo" / "tests" / "perf" / "query_cases" / "smoke" / "case.toml"
            (candidate_artifact / "repo" / "tests" / "perf" / "alias").symlink_to(target)
            with self.assertRaises(controller.ControllerError) as ctx:
                self._assemble_reusing(base_artifact, candidate_artifact, trusted_dir, root)
            self.assertIn("symlink", str(ctx.exception))

    def test_rejects_symlink_driver(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-symlink-") as tmp_str:
            root = Path(tmp_str)
            base_artifact, candidate_artifact, trusted_dir = self._make_artifacts(root)
            driver = candidate_artifact / "repo" / ".github" / "scripts" / "query-regression-run.py"
            driver.unlink()
            driver.symlink_to("/etc/passwd")
            with self.assertRaises(controller.ControllerError) as ctx:
                self._assemble_reusing(base_artifact, candidate_artifact, trusted_dir, root)
            self.assertIn("symlink", str(ctx.exception))

    def test_rejects_driver_escaping_artifact_root(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-symlink-") as tmp_str:
            root = Path(tmp_str)
            base_artifact, candidate_artifact, trusted_dir = self._make_artifacts(root)
            # A symlinked parent directory makes the regular driver file resolve
            # outside the candidate artifact root -> the ancestor symlink must
            # be rejected before any resolution follows it.
            outside = candidate_artifact.parent / "outside-scripts"
            (outside / "scripts").mkdir(parents=True)
            (outside / "scripts" / "query-regression-run.py").write_text(
                "#!/usr/bin/env python3\n", encoding="utf-8"
            )
            (candidate_artifact / "repo" / ".github").rename(candidate_artifact / "repo" / ".github-real")
            (candidate_artifact / "repo" / ".github").symlink_to(outside)
            with self.assertRaises(controller.ControllerError) as ctx:
                self._assemble_reusing(base_artifact, candidate_artifact, trusted_dir, root)
            self.assertIn("symlink component", str(ctx.exception))

    def test_rejects_tests_symlink_to_outside(self) -> None:
        """Exact regression: repo/tests -> outside must be rejected as an
        ancestor symlink of the tests/perf payload root."""
        with tempfile.TemporaryDirectory(prefix="qr-ack-symlink-") as tmp_str:
            root = Path(tmp_str)
            base_artifact, candidate_artifact, trusted_dir = self._make_artifacts(root)
            outside = candidate_artifact.parent / "outside-tests"
            (outside / "perf" / "query_cases" / "smoke").mkdir(parents=True)
            (outside / "perf" / "query_cases" / "smoke" / "case.toml").write_text(
                "[scenario]\nkind = \"direct_readable_sst\"\n", encoding="utf-8"
            )
            (candidate_artifact / "repo" / "tests").rename(candidate_artifact / "repo" / "tests-real")
            (candidate_artifact / "repo" / "tests").symlink_to(outside)
            with self.assertRaises(controller.ControllerError) as ctx:
                self._assemble_reusing(base_artifact, candidate_artifact, trusted_dir, root)
            self.assertIn("symlink component", str(ctx.exception))
            # The escape target must never have been copied into the payload.
            self.assertFalse((root / "payload" / "repo").exists())

    def test_rejects_in_tree_ancestor_symlink(self) -> None:
        """A symlinked ancestor that points back inside the tree is also
        rejected (an in-tree alias could confuse the copy or execution)."""
        with tempfile.TemporaryDirectory(prefix="qr-ack-symlink-") as tmp_str:
            root = Path(tmp_str)
            base_artifact, candidate_artifact, trusted_dir = self._make_artifacts(root)
            real = candidate_artifact / "repo" / "tests-real"
            (candidate_artifact / "repo" / "tests").rename(real)
            (candidate_artifact / "repo" / "tests").symlink_to(real)
            with self.assertRaises(controller.ControllerError) as ctx:
                self._assemble_reusing(base_artifact, candidate_artifact, trusted_dir, root)
            self.assertIn("symlink component", str(ctx.exception))

    def test_rejects_nested_symlinked_query_cases_parent(self) -> None:
        """A symlink two levels deep (tests/perf -> elsewhere) must also be
        rejected as an ancestor of the leaf case files."""
        with tempfile.TemporaryDirectory(prefix="qr-ack-symlink-") as tmp_str:
            root = Path(tmp_str)
            base_artifact, candidate_artifact, trusted_dir = self._make_artifacts(root)
            real = candidate_artifact / "repo" / "perf-real"
            (candidate_artifact / "repo" / "tests" / "perf").rename(real)
            (candidate_artifact / "repo" / "tests" / "perf").symlink_to(real)
            with self.assertRaises(controller.ControllerError) as ctx:
                self._assemble_reusing(base_artifact, candidate_artifact, trusted_dir, root)
            self.assertIn("symlink component", str(ctx.exception))

    def test_rejects_fifo_in_case_tree(self) -> None:
        if not hasattr(os, "mkfifo"):
            self.skipTest("os.mkfifo unavailable")
        with tempfile.TemporaryDirectory(prefix="qr-ack-fifo-") as tmp_str:
            root = Path(tmp_str)
            base_artifact, candidate_artifact, trusted_dir = self._make_artifacts(root)
            fifo = candidate_artifact / "repo" / "tests" / "perf" / "query_cases" / "smoke" / "pipe"
            os.mkfifo(fifo)
            with self.assertRaises(controller.ControllerError) as ctx:
                self._assemble_reusing(base_artifact, candidate_artifact, trusted_dir, root)
            self.assertIn("non-regular", str(ctx.exception))

    def test_rejects_symlinked_tests_perf_root(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-symlink-") as tmp_str:
            root = Path(tmp_str)
            base_artifact, candidate_artifact, trusted_dir = self._make_artifacts(root)
            real_perf = candidate_artifact / "repo" / "tests" / "perf"
            moved = candidate_artifact / "repo" / "tests" / "perf-real"
            real_perf.rename(moved)
            real_perf.symlink_to(moved)
            with self.assertRaises(controller.ControllerError) as ctx:
                self._assemble_reusing(base_artifact, candidate_artifact, trusted_dir, root)
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
        self.namespace = controller.DEFAULT_NAMESPACE
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


class JobAbsenceFailClosedTest(unittest.TestCase):
    """ACK Job absence confirmation must fail closed.

    Only ``kubectl get job NAME --ignore-not-found -o name`` with rc=0 and
    empty stdout proves absence; authorization (403), API/transport failures
    and client timeouts must raise/retry instead of being read as "absent",
    or a still-running Job could be reported deleted (fail-open).
    """

    def setUp(self) -> None:
        self.clock = FakeClock()
        self.now_patcher = mock.patch.object(controller, "_now", side_effect=self.clock.now)
        self.sleep_patcher = mock.patch.object(controller, "_sleep", side_effect=self.clock.sleep)
        self.now_patcher.start()
        self.sleep_patcher.start()
        self.addCleanup(self.now_patcher.stop)
        self.addCleanup(self.sleep_patcher.stop)

    def _job_get_args(self, kubectl: ScriptedKubectl) -> list[str]:
        job_gets = [args for args, _ in kubectl.calls if args[0] == "get" and args[1] == "job"]
        self.assertTrue(job_gets, "no job get was issued")
        return job_gets[0]

    def test_get_uses_ignore_not_found_name(self) -> None:
        kubectl = ScriptedKubectl(lambda args, check, input_text, timeout: completed(args, 0, ""))
        controller.job_absent(kubectl, "query-regression-1-1")
        args = self._job_get_args(kubectl)
        self.assertEqual(args[:3], ["get", "job", "query-regression-1-1"])
        self.assertIn("--ignore-not-found", args)
        self.assertEqual(args[-2:], ["-o", "name"])

    def test_genuine_not_found_proves_absence(self) -> None:
        kubectl = ScriptedKubectl(lambda args, check, input_text, timeout: completed(args, 0, ""))
        self.assertTrue(controller.job_absent(kubectl, "query-regression-1-1"))
        self.assertFalse(controller.job_exists(kubectl, "query-regression-1-1"))

    def test_present_job_is_not_absent(self) -> None:
        kubectl = ScriptedKubectl(
            lambda args, check, input_text, timeout: completed(
                args, 0, "job.batch/query-regression-1-1"
            )
        )
        self.assertFalse(controller.job_absent(kubectl, "query-regression-1-1"))
        self.assertTrue(controller.job_exists(kubectl, "query-regression-1-1"))

    def test_present_get_with_trailing_newline_is_exact(self) -> None:
        # kubectl's real output for a present Job is "job.batch/NAME\n"; the
        # single standard trailing newline is the canonical present shape.
        kubectl = ScriptedKubectl(
            lambda args, check, input_text, timeout: completed(
                args, 0, "job.batch/query-regression-1-1\n"
            )
        )
        self.assertTrue(controller.job_exists(kubectl, "query-regression-1-1"))
        self.assertFalse(controller.job_absent(kubectl, "query-regression-1-1"))

    def test_whitespace_only_stdout_fails_closed(self) -> None:
        """A whitespace-only stdout (spaces/newlines, no resource name) must
        never be read as absence: only RAW empty stdout proves a Job is gone."""
        for stdout in ("   \n", " \t ", "\n", "\r\n", "\n\n"):
            kubectl = ScriptedKubectl(
                lambda args, check, input_text, timeout, s=stdout: completed(args, 0, s)
            )
            with self.assertRaises(controller.ControllerError) as ctx:
                controller.job_absent(kubectl, "query-regression-1-1")
            self.assertIn("malformed output", str(ctx.exception), msg=repr(stdout))
            with self.assertRaises(controller.ControllerError):
                controller.job_exists(kubectl, "query-regression-1-1")

    def test_padded_stdout_fails_closed(self) -> None:
        """Leading/trailing spaces around the resource name are malformed:
        only the exact `job.batch/NAME` (optionally with one trailing
        newline) proves presence."""
        for stdout in (
            "  job.batch/query-regression-1-1",
            "job.batch/query-regression-1-1  ",
            " job.batch/query-regression-1-1\n",
            "job.batch/query-regression-1-1 \n",
            "\tjob.batch/query-regression-1-1\n",
        ):
            kubectl = ScriptedKubectl(
                lambda args, check, input_text, timeout, s=stdout: completed(args, 0, s)
            )
            with self.assertRaises(controller.ControllerError) as ctx:
                controller.job_exists(kubectl, "query-regression-1-1")
            self.assertIn("malformed output", str(ctx.exception), msg=repr(stdout))
            with self.assertRaises(controller.ControllerError):
                controller.job_absent(kubectl, "query-regression-1-1")

    def test_double_trailing_newline_fails_closed(self) -> None:
        """Exactly one trailing newline is the canonical present shape; a
        second newline makes the output multi-line and malformed (it must
        never be collapsed into a present verdict by stripping)."""
        kubectl = ScriptedKubectl(
            lambda args, check, input_text, timeout: completed(
                args, 0, "job.batch/query-regression-1-1\n\n"
            )
        )
        with self.assertRaises(controller.ControllerError) as ctx:
            controller.job_exists(kubectl, "query-regression-1-1")
        self.assertIn("malformed output", str(ctx.exception))
        with self.assertRaises(controller.ControllerError):
            controller.job_absent(kubectl, "query-regression-1-1")

    def test_stderr_pollution_on_success_fails_closed(self) -> None:
        """A successful get that also wrote to stderr (exec-plugin noise,
        warnings) is non-canonical: it must never be read as absent or
        present."""
        kubectl = ScriptedKubectl(
            lambda args, check, input_text, timeout: completed(
                args, 0, "job.batch/query-regression-1-1",
                'time="2026-01-01T00:00:00Z" level=warning msg="exec plugin noise"',
            )
        )
        with self.assertRaises(controller.ControllerError) as ctx:
            controller.job_exists(kubectl, "query-regression-1-1")
        self.assertIn("wrote to stderr", str(ctx.exception))
        # Absence must fail closed the same way: stderr pollution never means
        # "no leak".
        with self.assertRaises(controller.ControllerError) as ctx:
            controller.job_absent(kubectl, "query-regression-1-1")
        self.assertIn("wrote to stderr", str(ctx.exception))

    def test_whitespace_only_stderr_fails_closed(self) -> None:
        """A successful get whose stderr contains only whitespace (spaces,
        tabs, a bare newline) is still stderr pollution: a canonical get
        requires RAW empty stderr (``proc.stderr == ""``). Whitespace-only or
        newline-only exec-plugin noise must never be stripped into a clean
        verdict and read as absent or present."""
        for stderr in ("   \n", " \t ", "\n", "\r\n", "\n\n", " "):
            kubectl = ScriptedKubectl(
                lambda args, check, input_text, timeout, e=stderr: completed(
                    args, 0, "job.batch/query-regression-1-1", e
                )
            )
            with self.assertRaises(controller.ControllerError) as ctx:
                controller.job_exists(kubectl, "query-regression-1-1")
            self.assertIn("wrote to stderr", str(ctx.exception), msg=repr(stderr))
            # Absence must fail closed the same way: whitespace-only stderr
            # never means "no leak".
            with self.assertRaises(controller.ControllerError) as ctx:
                controller.job_absent(kubectl, "query-regression-1-1")
            self.assertIn("wrote to stderr", str(ctx.exception), msg=repr(stderr))

    def test_malformed_get_output_other_name_fails_closed(self) -> None:
        """stdout naming a different Job must be rejected: only the exact
        `job.batch/NAME` resource name proves presence."""
        kubectl = ScriptedKubectl(
            lambda args, check, input_text, timeout: completed(
                args, 0, "job.batch/query-regression-999-9"
            )
        )
        with self.assertRaises(controller.ControllerError) as ctx:
            controller.job_lookup(kubectl, "query-regression-1-1")
        self.assertIn("malformed output", str(ctx.exception))
        # job_exists/job_absent fail closed the same way: a wrong name never
        # means "present" or "absent".
        with self.assertRaises(controller.ControllerError):
            controller.job_exists(kubectl, "query-regression-1-1")
        with self.assertRaises(controller.ControllerError):
            controller.job_absent(kubectl, "query-regression-1-1")

    def test_malformed_get_output_multiline_fails_closed(self) -> None:
        """Multi-line stdout is malformed: neither absence nor presence may
        be concluded from it."""
        kubectl = ScriptedKubectl(
            lambda args, check, input_text, timeout: completed(
                args, 0, "job.batch/query-regression-1-1\njob.batch/query-regression-2-2\n"
            )
        )
        with self.assertRaises(controller.ControllerError) as ctx:
            controller.job_exists(kubectl, "query-regression-1-1")
        self.assertIn("malformed output", str(ctx.exception))

    def test_malformed_get_output_garbage_fails_closed(self) -> None:
        kubectl = ScriptedKubectl(
            lambda args, check, input_text, timeout: completed(
                args, 0, "Traceback (most recent call last):\n  File \"/x\", line 1\n"
            )
        )
        with self.assertRaises(controller.ControllerError) as ctx:
            controller.job_lookup(kubectl, "query-regression-1-1")
        self.assertIn("malformed output", str(ctx.exception))

    def test_forbidden_403_fails_closed(self) -> None:
        kubectl = ScriptedKubectl(
            lambda args, check, input_text, timeout: completed(
                args, 1, "",
                'Error from server (Forbidden): jobs.batch "query-regression-1-1" is '
                'forbidden: User "x" cannot get resource "jobs" in API group "batch"',
            )
        )
        with self.assertRaises(controller.ControllerError) as ctx:
            controller.job_absent(kubectl, "query-regression-1-1")
        self.assertIn("cannot determine whether job", str(ctx.exception))
        self.assertIn("Forbidden", str(ctx.exception))
        # job_exists fails closed the same way: a 403 never means "no leak".
        with self.assertRaises(controller.ControllerError):
            controller.job_exists(kubectl, "query-regression-1-1")

    def test_transient_api_failure_fails_closed(self) -> None:
        kubectl = ScriptedKubectl(
            lambda args, check, input_text, timeout: completed(
                args, 1, "",
                "Error from server (InternalError): the server is currently unable "
                "to handle the request",
            )
        )
        with self.assertRaises(controller.ControllerError) as ctx:
            controller.job_absent(kubectl, "query-regression-1-1")
        self.assertIn("cannot determine whether job", str(ctx.exception))

    def test_timeout_fails_closed(self) -> None:
        # Kubectl.run raises ControllerError on subprocess timeout; absence
        # must not be inferred from a timed-out get.
        def responder(args, check, input_text, timeout):
            raise controller.ControllerError(f"kubectl timed out: get job")

        kubectl = ScriptedKubectl(responder)
        with self.assertRaises(controller.ControllerError) as ctx:
            controller.job_absent(kubectl, "query-regression-1-1")
        self.assertIn("kubectl timed out", str(ctx.exception))

    def test_delete_job_retries_transient_absence_check_then_confirms_absent(self) -> None:
        calls = {"get_job": 0}

        def responder(args, check, input_text, timeout):
            if args[0] == "get" and args[1] == "job":
                calls["get_job"] += 1
                if calls["get_job"] <= 2:
                    raise controller.ControllerError("kubectl timed out: get job")
                # Transient errors cleared: the successful empty get proves
                # the Job is genuinely gone.
                return completed(args, 0, "")
            return completed(args, 0, "")

        kubectl = ScriptedKubectl(responder)
        controller.delete_job(kubectl, "query-regression-1-1", self.clock.now() + 100, attempts=3, delete_timeout=30)
        # The failure was retried (3 gets) and never read as absence; the
        # successful empty get ended the loop without issuing a delete.
        self.assertEqual(calls["get_job"], 3)
        self.assertEqual([a for a, _ in kubectl.calls if a[0] == "delete"], [])

    def test_delete_job_persistent_forbidden_fails_closed_after_retries(self) -> None:
        def responder(args, check, input_text, timeout):
            if args[0] == "get" and args[1] == "job":
                return completed(
                    args, 1, "",
                    'Error from server (Forbidden): jobs.batch "query-regression-1-1" is forbidden',
                )
            return completed(args, 0, "")

        kubectl = ScriptedKubectl(responder)
        with self.assertRaises(controller.ControllerError) as ctx:
            controller.delete_job(kubectl, "query-regression-1-1", self.clock.now() + 100, attempts=3, delete_timeout=30)
        self.assertIn("failed to delete job", str(ctx.exception))
        # The absence check was retried the bounded number of times; it was
        # never treated as "absent", so no delete was ever issued.
        job_gets = [args for args, _ in kubectl.calls if args[0] == "get" and args[1] == "job"]
        self.assertEqual(len(job_gets), 3)
        self.assertEqual([a for a, _ in kubectl.calls if a[0] == "delete"], [])

    def test_delete_job_final_absence_check_failure_fails_closed(self) -> None:
        state = {"deleted": False}

        def responder(args, check, input_text, timeout):
            if args[0] == "get" and args[1] == "job":
                if state["deleted"]:
                    # The confirmation get after a successful delete fails
                    # (authorization/transport): absence is NOT proven.
                    return completed(
                        args, 1, "",
                        "Error from server (InternalError): the server is currently unable to handle the request",
                    )
                return completed(args, 0, "job.batch/query-regression-1-1")
            if args[0] == "delete":
                state["deleted"] = True
                return completed(args, 0, "job.batch/query-regression-1-1 deleted")
            return completed(args, 0, "")

        kubectl = ScriptedKubectl(responder)
        with self.assertRaises(controller.ControllerError) as ctx:
            controller.delete_job(kubectl, "query-regression-1-1", self.clock.now() + 100, attempts=3, delete_timeout=30)
        # The delete succeeded but confirmation failed: the controller must
        # raise instead of claiming the Job is gone.
        self.assertIn("cannot determine whether job", str(ctx.exception))
        self.assertTrue(state["deleted"])

    def test_delete_job_api_not_found_concluded_only_by_exact_lookup(self) -> None:
        """A delete answered NotFound by the API server never proves absence
        on its own: the success conclusion comes only from the subsequent
        successful exact lookup (empty get)."""
        state = {"exists": True}

        def responder(args, check, input_text, timeout):
            if args[0] == "get" and args[1] == "job":
                return completed(
                    args, 0, "job.batch/query-regression-1-1" if state["exists"] else ""
                )
            if args[0] == "delete":
                state["exists"] = False
                return completed(
                    args, 1, "",
                    'Error from server (NotFound): jobs.batch "query-regression-1-1" not found',
                )
            return completed(args, 0, "")

        kubectl = ScriptedKubectl(responder)
        controller.delete_job(kubectl, "query-regression-1-1", self.clock.now() + 100, attempts=3, delete_timeout=30)
        # Exactly one delete attempt; the NotFound answer was NOT the success
        # signal -- an exact lookup (absence pre-check + confirmation) proved
        # absence before delete_job returned.
        deletes = [args for args, _ in kubectl.calls if args[0] == "delete"]
        self.assertEqual(len(deletes), 1)
        job_gets = [args for args, _ in kubectl.calls if args[0] == "get" and args[1] == "job"]
        self.assertGreaterEqual(len(job_gets), 2)

    def test_delete_job_exec_plugin_not_found_noise_fails_closed(self) -> None:
        """A non-zero delete whose stderr merely contains "not found" (exec
        credential plugin noise, not an API NotFound for the Job) must never
        be read as absence: with the Job still present, deletion fails after
        the bounded retries instead of claiming success."""
        state = {"exists": True}

        def responder(args, check, input_text, timeout):
            if args[0] == "get" and args[1] == "job":
                return completed(
                    args, 0, "job.batch/query-regression-1-1" if state["exists"] else ""
                )
            if args[0] == "delete":
                # Exec-plugin stderr noise; the delete itself failed for an
                # unrelated reason and the Job is still there.
                return completed(
                    args, 1, "",
                    'time="2024-01-01T00:00:00Z" level=error msg="exec plugin: token not found; retrying"',
                )
            return completed(args, 0, "")

        kubectl = ScriptedKubectl(responder)
        with self.assertRaises(controller.ControllerError) as ctx:
            controller.delete_job(kubectl, "query-regression-1-1", self.clock.now() + 100, attempts=3, delete_timeout=30)
        self.assertIn("failed to delete job", str(ctx.exception))
        # The delete was retried the bounded number of times; the plugin's
        # "not found" text never concluded absence, so no success was claimed.
        deletes = [args for args, _ in kubectl.calls if args[0] == "delete"]
        self.assertEqual(len(deletes), 3)

    def test_delete_job_plugin_not_found_noise_then_lookup_proves_absence(self) -> None:
        """Even when a failed delete's stderr mentions "not found" (plugin
        noise), the success conclusion comes only from the subsequent exact
        lookup -- here the Job is genuinely gone, so the empty get confirms
        it and delete_job returns."""
        state = {"exists": True}

        def responder(args, check, input_text, timeout):
            if args[0] == "get" and args[1] == "job":
                return completed(
                    args, 0, "job.batch/query-regression-1-1" if state["exists"] else ""
                )
            if args[0] == "delete":
                state["exists"] = False
                return completed(
                    args, 1, "",
                    'time="2024-01-01T00:00:00Z" level=error msg="exec plugin: token not found; retrying"',
                )
            return completed(args, 0, "")

        kubectl = ScriptedKubectl(responder)
        controller.delete_job(kubectl, "query-regression-1-1", self.clock.now() + 100, attempts=3, delete_timeout=30)
        # The exact lookup after the failed delete proved absence; without it
        # the "not found" plugin noise alone would have been a fail-open
        # "already gone" conclusion. The failed delete was not retried because
        # the lookup already confirmed absence.
        job_gets = [args for args, _ in kubectl.calls if args[0] == "get" and args[1] == "job"]
        self.assertGreaterEqual(len(job_gets), 2)
        self.assertEqual(
            len([args for args, _ in kubectl.calls if args[0] == "delete"]), 1
        )

    def test_delete_job_confirm_still_present_retries_then_fails(self) -> None:
        """A delete that reports success but whose confirmation lookup still
        shows the Job present must be retried (bounded) and then fail closed,
        never report the deletion as confirmed."""
        state = {"deletes": 0}

        def responder(args, check, input_text, timeout):
            if args[0] == "get" and args[1] == "job":
                # The Job survives every delete (e.g. foreground cascade
                # stalled server-side).
                return completed(args, 0, "job.batch/query-regression-1-1")
            if args[0] == "delete":
                state["deletes"] += 1
                return completed(args, 0, "job.batch/query-regression-1-1 deleted")
            return completed(args, 0, "")

        kubectl = ScriptedKubectl(responder)
        with self.assertRaises(controller.ControllerError) as ctx:
            controller.delete_job(kubectl, "query-regression-1-1", self.clock.now() + 100, attempts=3, delete_timeout=30)
        self.assertIn("still present after foreground delete", str(ctx.exception))
        # The delete was retried the bounded number of times because the
        # confirmation lookup never proved absence.
        self.assertEqual(state["deletes"], 3)

    def test_delete_job_rechecks_budget_before_delete(self) -> None:
        """The remaining budget is re-checked immediately before the delete:
        when the absence pre-check consumed the entire budget, the delete
        must raise ControllerError instead of starting a clamped 1-second
        delete against the cluster."""
        def responder(args, check, input_text, timeout):
            if args[0] == "get" and args[1] == "job":
                # The pre-check get consumes the entire remaining budget.
                self.clock.advance(100)
                return completed(args, 0, "job.batch/query-regression-1-1")
            if args[0] == "delete":
                return completed(args, 0, "job.batch/query-regression-1-1 deleted")
            return completed(args, 0, "")

        kubectl = ScriptedKubectl(responder)
        deadline = self.clock.now() + 30
        with self.assertRaises(controller.ControllerError) as ctx:
            controller.delete_job(
                kubectl, "query-regression-1-1", deadline, attempts=3, delete_timeout=240
            )
        self.assertIn("cleanup budget exhausted before issuing the delete", str(ctx.exception))
        # The exhausted budget was detected before the 1-second clamped
        # delete could start: no delete and no confirmation lookup ran.
        self.assertEqual([a for a, _ in kubectl.calls if a[0] == "delete"], [])
        job_gets = [a for a, _ in kubectl.calls if a[0] == "get" and a[1] == "job"]
        self.assertEqual(len(job_gets), 1)

    def test_delete_job_rechecks_budget_before_confirmation(self) -> None:
        """The remaining budget is re-checked immediately before the
        confirmation lookup: when the delete consumed the entire budget, the
        confirmation must raise ControllerError instead of starting a clamped
        1-second lookup (a 1-second get could still read a stale present and
        spin the retry loop with no time left)."""
        def responder(args, check, input_text, timeout):
            if args[0] == "get" and args[1] == "job":
                return completed(args, 0, "job.batch/query-regression-1-1")
            if args[0] == "delete":
                # The delete consumed the entire remaining budget.
                self.clock.advance(100)
                return completed(args, 0, "job.batch/query-regression-1-1 deleted")
            return completed(args, 0, "")

        kubectl = ScriptedKubectl(responder)
        deadline = self.clock.now() + 30
        with self.assertRaises(controller.ControllerError) as ctx:
            controller.delete_job(
                kubectl, "query-regression-1-1", deadline, attempts=1, delete_timeout=240
            )
        self.assertIn("cleanup budget exhausted before confirming deletion", str(ctx.exception))
        # The delete ran once; the exhausted budget stopped the confirmation
        # lookup from starting as a 1-second clamped call.
        self.assertEqual(len([a for a, _ in kubectl.calls if a[0] == "delete"]), 1)
        job_gets = [a for a, _ in kubectl.calls if a[0] == "get" and a[1] == "job"]
        self.assertEqual(len(job_gets), 1)  # only the absence pre-check


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
        with tempfile.TemporaryDirectory(prefix="qr-ack-exhausted-") as tmp_str:
            tmp = Path(tmp_str)
            kubeconfig = write_kubeconfig(tmp)
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
                controller, "verify_trusted_scripts"
            ), mock.patch.object(
                controller, "assemble_payload", return_value=Path("/tmp/fake-payload")
            ):
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
                        "--kubeconfig", str(kubeconfig),
                        "--delete-timeout", "30", "--delete-attempts", "3",
                        "--lifecycle-timeout", "100", "--cleanup-reserve", "20",
                        "--result-dir", str(tmp / "results"),
                        "--base-artifact-dir", str(base_dir),
                        "--candidate-artifact-dir", str(cand_dir),
                        "--trusted-summary", str(tmp / "summary.py"),
                        "--trusted-scripts-manifest", str(tmp / "trusted.json"),
                        "--trusted-source-sha", TRUSTED_SOURCE_SHA,
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
        with tempfile.TemporaryDirectory(prefix="qr-ack-exhausted-") as tmp_str:
            tmp = Path(tmp_str)
            kubeconfig = write_kubeconfig(tmp)
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
            ), mock.patch.object(controller, "verify_trusted_scripts"), mock.patch.object(
                controller, "assemble_payload", return_value=Path("/tmp/fake-payload")
            ):
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
                        "--kubeconfig", str(kubeconfig),
                        "--delete-timeout", "30", "--delete-attempts", "3",
                        "--lifecycle-timeout", "100", "--cleanup-reserve", "20",
                        "--result-dir", str(tmp / "results"),
                        "--base-artifact-dir", str(base_dir),
                        "--candidate-artifact-dir", str(cand_dir),
                        "--trusted-summary", str(tmp / "summary.py"),
                        "--trusted-scripts-manifest", str(tmp / "trusted.json"),
                        "--trusted-source-sha", TRUSTED_SOURCE_SHA,
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
        kubeconfig = write_kubeconfig(tmp)
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
                "--run-id",
                run_id,
                "--run-attempt",
                run_attempt,
                "--kubectl",
                str(fake),
                "--kubeconfig",
                str(kubeconfig),
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

    def test_cleanup_only_requires_kubeconfig(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-cleanup-only-") as tmp_str:
            tmp = Path(tmp_str)
            state = tmp / "state"
            state.mkdir(exist_ok=True)
            fake = state / "kubectl"
            fake.write_text(FAKE_KUBECTL, encoding="utf-8")
            fake.chmod(0o755)
            proc = subprocess.run(
                [
                    sys.executable,
                    str(CONTROLLER_PATH),
                    "--cleanup-only",
                    "--run-id",
                    "123456789",
                    "--run-attempt",
                    "2",
                    "--kubectl",
                    str(fake),
                    "--delete-timeout",
                    "30",
                ],
                capture_output=True,
                text=True,
                timeout=60,
                env=dict(os.environ, RUN_ID="123456789", RUN_ATTEMPT="2", GITHUB_OUTPUT=""),
            )
            self.assertEqual(proc.returncode, 1)
            self.assertIn("kubeconfig path is required", proc.stderr)


# Fake kubectl that simulates the ACK cluster state machine without contacting
# any cluster: create/get/delete Job, pod phase, exec-gated markers, and
# kubectl cp push/pull against a fake pod filesystem under $FAKE_KUBECTL_STATE.
# Failure injection: FAKE_KUBECTL_CP_FAILS, FAKE_KUBECTL_DELETE_FAILS (count),
# FAKE_KUBECTL_DELETE_LEAVES, FAKE_KUBECTL_BAD_SHA, FAKE_KUBECTL_NEVER_DONE,
# FAKE_KUBECTL_GET_JOB_FORBIDDEN (every `get job` returns a 403 Forbidden).
FAKE_KUBECTL = r"""#!/usr/bin/env bash
set -u
STATE="${FAKE_KUBECTL_STATE:?missing FAKE_KUBECTL_STATE}"
POD_DIR="${STATE}/pod"
mkdir -p "${POD_DIR}/payload" "${POD_DIR}/work"

# Record every invocation (argv + KUBECTL_CONTEXT env) so tests can prove the
# controller never passes a --context flag and never leaks a KUBECTL_CONTEXT
# env override into a kubectl subprocess: the validated kubeconfig's
# current-context must be the only context kubectl ever uses.
{
  printf 'argv:'
  for a in "$@"; do printf ' [%s]' "$a"; done
  printf '\n'
  printf 'KUBECTL_CONTEXT=%s\n' "${KUBECTL_CONTEXT-<unset>}"
} >> "${STATE}/invocations"

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
  version)
    # Pin matches the controller's KUBECTL_PINNED_VERSION ABI (v1.34.2).
    echo '{"clientVersion":{"major":"1","minor":"34","gitVersion":"v1.34.2"}}'
    exit 0
    ;;
  get)
    kind="$1"
    shift
    if [ "$kind" = "job" ]; then
      if [ "${FAKE_KUBECTL_GET_JOB_FORBIDDEN:-0}" = "1" ]; then
        # Authorization failure: a 403 must never be read as "absent".
        echo "Error from server (Forbidden): jobs.batch \"$1\" is forbidden: User \"simulated\" cannot get resource \"jobs\" in API group \"batch\"" >&2
        exit 1
      fi
      if [ -f "${STATE}/job-exists" ]; then
        echo "job.batch/$1"
        exit 0
      fi
      # kubectl get --ignore-not-found: genuine absence is rc=0, empty stdout
      # (the only result that proves absence).
      for a in "$@"; do
        if [ "$a" = "--ignore-not-found" ]; then
          exit 0
        fi
      done
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


def make_artifacts(root: Path) -> tuple[Path, Path, Path, str]:
    """Create base/candidate artifact dirs and a trusted-scripts dir.

    In v2 the candidate artifact carries the tests/perf tree and the driver
    (repo/...) because the controller workflow never checks out PR code. The
    trusted-scripts manifest records the default-branch HEAD as its source.
    """
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

    # Candidate-controlled case tree + driver inside the candidate artifact.
    cases = candidate_artifact / "repo" / "tests" / "perf" / "query_cases" / "smoke"
    cases.mkdir(parents=True)
    (cases / "case.toml").write_text(
        "[scenario]\nkind = \"direct_readable_sst\"\n", encoding="utf-8"
    )
    driver = candidate_artifact / "repo" / ".github" / "scripts"
    driver.mkdir(parents=True)
    (driver / "query-regression-run.py").write_text(
        "#!/usr/bin/env python3\nprint('candidate driver')\n", encoding="utf-8"
    )

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
    trusted_manifest = {
        "source_sha": TRUSTED_SOURCE_SHA,
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
        kubeconfig_content: str = GOOD_KUBECONFIG,
    ) -> tuple[subprocess.CompletedProcess[str], Path, Path, Path]:
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
        kubeconfig = write_kubeconfig(tmp, kubeconfig_content)

        env = dict(os.environ)
        env.update(
            {
                "SOURCE_RUN_ID": "123456789",
                "SOURCE_RUN_ATTEMPT": "2",
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
            "--kubeconfig",
            str(kubeconfig),
            "--base-artifact-dir",
            str(base_artifact),
            "--candidate-artifact-dir",
            str(candidate_artifact),
            "--trusted-summary",
            str(trusted_dir / "query-regression-summary.py"),
            "--trusted-scripts-manifest",
            str(trusted_dir / "trusted-scripts-manifest.json"),
            "--trusted-source-sha",
            TRUSTED_SOURCE_SHA,
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
        return proc, result_dir, output_file, state

    def test_success_flow_collects_results_and_deletes_exact_job(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-controller-") as tmp_str:
            tmp = Path(tmp_str)
            proc, result_dir, output_file, state = self._run_controller(tmp)
            self.assertEqual(proc.returncode, 0, proc.stderr + proc.stdout)
            self.assertEqual((output_file.read_text().strip()), "status=0")
            report = result_dir / "query-regression-work" / "smoke" / "query-regression-report.json"
            self.assertTrue(report.is_file(), f"report missing: {report}")
            self.assertTrue((result_dir / "query-regression-summary.md").is_file())
            self.assertTrue((result_dir / "query-regression-pod.log").is_file())
            self.assertTrue((result_dir / "query-regression-pod-describe.txt").is_file())
            deleted = (state / "deleted").read_text()
            self.assertIn("job.batch/query-regression-123456789-2 deleted", deleted)
            self.assertFalse((state / "job-exists").exists())
            # The pod entrypoint must have been armed and released.
            self.assertTrue((state / "pod" / "payload" / ".ready").exists())
            self.assertTrue((state / "pod" / "work" / ".collected").exists())
            # The trusted summary (default branch) is embedded, not the artifact's.
            payload_summary = tmp / "payload" / "repo" / ".github" / "scripts" / "query-regression-summary.py"
            self.assertIn("trusted summary", payload_summary.read_text(encoding="utf-8"))
            # The candidate driver and cases came from the candidate artifact.
            self.assertTrue((tmp / "payload" / "repo" / "tests" / "perf" / "query_cases" / "smoke" / "case.toml").is_file())
            self.assertIn("candidate driver", (tmp / "payload" / "repo" / ".github" / "scripts" / "query-regression-run.py").read_text(encoding="utf-8"))
            # The created Job carries the v2 hardening fields.
            manifest = json.loads((state / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["metadata"]["namespace"], "query-regression-perf")
            self.assertEqual(manifest["spec"]["ttlSecondsAfterFinished"], 600)
            self.assertEqual(manifest["spec"]["podReplacementPolicy"], "Failed")
            self.assertEqual(
                manifest["spec"]["template"]["spec"]["serviceAccountName"],
                "query-regression-workload",
            )

    def test_pod_failure_still_collects_logs_and_cleans_up(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-controller-") as tmp_str:
            tmp = Path(tmp_str)
            proc, result_dir, output_file, state = self._run_controller(tmp, phase="Failed")
            self.assertEqual(proc.returncode, 0, proc.stderr + proc.stdout)
            self.assertEqual((output_file.read_text().strip()), "status=1")
            self.assertTrue((result_dir / "query-regression-pod.log").is_file())
            deleted = (state / "deleted").read_text()
            self.assertIn("job.batch/query-regression-123456789-2 deleted", deleted)

    def test_in_pod_sha_mismatch_aborts_and_cleans_up(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-controller-") as tmp_str:
            tmp = Path(tmp_str)
            proc, result_dir, output_file, state = self._run_controller(
                tmp, extra_env={"FAKE_KUBECTL_BAD_SHA": "1"}
            )
            self.assertEqual(proc.returncode, 0, proc.stderr + proc.stdout)
            self.assertEqual((output_file.read_text().strip()), "status=1")
            self.assertIn("in-pod sha256 mismatch", proc.stderr)
            deleted = (state / "deleted").read_text()
            self.assertIn("job.batch/query-regression-123456789-2 deleted", deleted)

    def test_copy_failure_propagates_and_cleans_up(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-controller-") as tmp_str:
            tmp = Path(tmp_str)
            proc, result_dir, output_file, state = self._run_controller(
                tmp, extra_env={"FAKE_KUBECTL_CP_FAILS": "1"}
            )
            self.assertEqual(proc.returncode, 0, proc.stderr + proc.stdout)
            self.assertEqual((output_file.read_text().strip()), "status=1")
            self.assertIn("kubectl cp of bins failed", proc.stderr)
            deleted = (state / "deleted").read_text()
            self.assertIn("job.batch/query-regression-123456789-2 deleted", deleted)

    def test_result_collection_failure_fails_even_when_status_zero(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-controller-") as tmp_str:
            tmp = Path(tmp_str)
            # The fake pod never creates the summary file: the benchmark
            # reports status 0 but mandatory collection must fail the run.
            proc, result_dir, output_file, state = self._run_controller(
                tmp, extra_env={"FAKE_KUBECTL_NO_SUMMARY": "1"}
            )
            self.assertEqual(proc.returncode, 0, proc.stderr + proc.stdout)
            self.assertEqual((output_file.read_text().strip()), "status=1")
            self.assertIn("could not collect required result", proc.stderr)
            deleted = (state / "deleted").read_text()
            self.assertIn("job.batch/query-regression-123456789-2 deleted", deleted)

    def test_delete_retries_then_succeeds(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-controller-") as tmp_str:
            tmp = Path(tmp_str)
            state = tmp / "state"
            state.mkdir(exist_ok=True)
            # First delete attempt fails, the retry succeeds.
            (state / "delete-fails-remaining").write_text("1", encoding="utf-8")
            proc, result_dir, output_file, _ = self._run_controller(tmp)
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
            proc, result_dir, output_file, _ = self._run_controller(tmp)
            self.assertEqual(proc.returncode, 0, proc.stderr + proc.stdout)
            self.assertEqual((output_file.read_text().strip()), "status=1")
            self.assertIn("failed to delete job", proc.stderr)
            self.assertIn("workflow will fail", proc.stderr)

    def test_delete_leaves_job_fails_absence_check(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-controller-") as tmp_str:
            tmp = Path(tmp_str)
            proc, result_dir, output_file, state = self._run_controller(
                tmp, extra_env={"FAKE_KUBECTL_DELETE_LEAVES": "1"}
            )
            self.assertEqual(proc.returncode, 0, proc.stderr + proc.stdout)
            self.assertEqual((output_file.read_text().strip()), "status=1")
            self.assertIn("still present after foreground delete", proc.stderr)
            self.assertIn("workflow will fail", proc.stderr)

    def test_forbidden_job_get_fails_closed(self) -> None:
        """A 403 on the job get must never be read as "absent".

        Authorization failure on every `get job` means the controller can
        neither preflight nor confirm absence: the run fails and the deletion
        is not claimed confirmed (fail closed, no deletion reported).
        """
        with tempfile.TemporaryDirectory(prefix="qr-ack-controller-") as tmp_str:
            tmp = Path(tmp_str)
            proc, result_dir, output_file, state = self._run_controller(
                tmp, extra_env={"FAKE_KUBECTL_GET_JOB_FORBIDDEN": "1"}
            )
            self.assertEqual(proc.returncode, 0, proc.stderr + proc.stdout)
            self.assertEqual((output_file.read_text().strip()), "status=1")
            self.assertIn("cannot determine whether job", proc.stderr)
            self.assertIn("job deletion could not be confirmed", proc.stderr)
            # The absence check was retried the bounded number of times and
            # never treated as "absent": no delete was issued and none is
            # claimed.
            self.assertFalse((state / "deleted").exists())

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
            kubeconfig = write_kubeconfig(tmp)
            env = dict(os.environ)
            env.update(
                {
                    "SOURCE_RUN_ID": "123456789",
                    "SOURCE_RUN_ATTEMPT": "2",
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
                    "--kubeconfig",
                    str(kubeconfig),
                    "--base-artifact-dir",
                    str(base_artifact),
                    "--candidate-artifact-dir",
                    str(candidate_artifact),
                    "--trusted-summary",
                    str(trusted_dir / "query-regression-summary.py"),
                    "--trusted-scripts-manifest",
                    str(trusted_dir / "trusted-scripts-manifest.json"),
                    "--trusted-source-sha",
                    TRUSTED_SOURCE_SHA,
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
            kubeconfig = write_kubeconfig(tmp)
            env = dict(os.environ)
            env.update(
                {
                    "SOURCE_RUN_ID": "123456789",
                    "SOURCE_RUN_ATTEMPT": "2",
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
                    "--kubeconfig",
                    str(kubeconfig),
                    "--base-artifact-dir",
                    str(base_artifact),
                    "--candidate-artifact-dir",
                    str(candidate_artifact),
                    "--trusted-summary",
                    str(trusted_dir / "query-regression-summary.py"),
                    "--trusted-scripts-manifest",
                    str(trusted_dir / "trusted-scripts-manifest.json"),
                    "--trusted-source-sha",
                    TRUSTED_SOURCE_SHA,
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

    def test_payload_over_cap_aborts_before_job_creation(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-controller-") as tmp_str:
            tmp = Path(tmp_str)
            proc, result_dir, output_file, state = self._run_controller(
                tmp, extra_args=["--payload-bytes-cap", "10"]
            )
            # Pre-Job cap rejection: the controller exits 1 before any cluster
            # call, so the workflow fails without creating anything.
            self.assertEqual(proc.returncode, 1, proc.stderr + proc.stdout)
            self.assertIn("exceeds the pre-Job cap", proc.stderr)
            # No Job was ever created.
            self.assertFalse((state / "job-exists").exists())
            self.assertFalse((state / "manifest.json").exists())

    def test_context_env_override_is_ignored_safely(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-controller-") as tmp_str:
            tmp = Path(tmp_str)
            # A runner may have KUBECTL_CONTEXT set globally for other tooling;
            # it must never redirect the controller away from the validated
            # kubeconfig current-context (ignored safely, run still succeeds).
            proc, result_dir, output_file, state = self._run_controller(
                tmp, extra_env={"KUBECTL_CONTEXT": "evil-cluster"}
            )
            self.assertEqual(proc.returncode, 0, proc.stderr + proc.stdout)
            self.assertEqual((output_file.read_text().strip()), "status=0")
            self.assertTrue(
                (result_dir / "query-regression-work" / "smoke" / "query-regression-report.json").is_file()
            )
            # No kubectl subprocess ever saw a --context flag or the override
            # env var: the validated kubeconfig's current-context is the only
            # context kubectl ever uses.
            invocations = (state / "invocations").read_text(encoding="utf-8")
            self.assertNotIn("--context", invocations)
            self.assertNotIn("evil-cluster", invocations)
            self.assertIn("KUBECTL_CONTEXT=<unset>", invocations)

    def test_context_arg_override_is_rejected_before_any_cluster_call(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-controller-") as tmp_str:
            tmp = Path(tmp_str)
            proc, _, _, state = self._run_controller(
                tmp, extra_args=["--context", "evil-cluster"]
            )
            # The --context override is deleted from the CLI: argparse rejects
            # it before any validation or cluster call (exit code 2).
            self.assertEqual(proc.returncode, 2, proc.stderr + proc.stdout)
            self.assertIn("unrecognized arguments", proc.stderr)
            self.assertFalse((state / "invocations").exists())
            self.assertFalse((state / "job-exists").exists())


class TrustVerificationTest(unittest.TestCase):
    def _run_controller_trust(
        self, tmp: Path, mutate: Callable[[Path, Path, Path, str], None]
    ) -> tuple[subprocess.CompletedProcess[str], str]:
        base_artifact, candidate_artifact, trusted_dir, candidate_sha = make_artifacts(tmp)
        mutate(base_artifact, candidate_artifact, trusted_dir, candidate_sha)
        kubeconfig = write_kubeconfig(tmp)
        fake_kubectl = tmp / "kubectl"
        fake_kubectl.write_text(FAKE_KUBECTL, encoding="utf-8")
        fake_kubectl.chmod(0o755)
        env = dict(
            os.environ,
            SOURCE_RUN_ID="1",
            SOURCE_RUN_ATTEMPT="1",
            VERIFIED_BASE_SHA=BASE_SHA,
            VERIFIED_CANDIDATE_SHA=candidate_sha,
            GITHUB_OUTPUT="",
            FAKE_KUBECTL_STATE=str(tmp / "kube-state"),
        )
        proc = subprocess.run(
            [
                sys.executable,
                str(CONTROLLER_PATH),
                "--base-artifact-dir",
                str(base_artifact),
                "--candidate-artifact-dir",
                str(candidate_artifact),
                "--kubeconfig",
                str(kubeconfig),
                "--trusted-summary",
                str(trusted_dir / "query-regression-summary.py"),
                "--trusted-scripts-manifest",
                str(trusted_dir / "trusted-scripts-manifest.json"),
                "--trusted-source-sha",
                TRUSTED_SOURCE_SHA,
                "--payload-dir",
                str(tmp / "payload"),
                "--result-dir",
                str(tmp / "result"),
                "--kubectl",
                str(fake_kubectl),
            ],
            capture_output=True,
            text=True,
            timeout=60,
            env=env,
        )
        return proc, proc.stderr

    def test_manifest_sha_mismatch_fails_closed(self) -> None:
        def mutate(base_artifact, candidate_artifact, trusted_dir, candidate_sha):
            manifest_path = candidate_artifact / "candidate-manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["candidate_sha"] = "c" * 40
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        with tempfile.TemporaryDirectory(prefix="qr-ack-trust-") as tmp_str:
            proc, stderr = self._run_controller_trust(Path(tmp_str), mutate)
            self.assertEqual(proc.returncode, 1)
            self.assertIn("does not match the validated candidate SHA", stderr)

    def test_trusted_summary_mismatch_fails_closed(self) -> None:
        def mutate(base_artifact, candidate_artifact, trusted_dir, candidate_sha):
            (trusted_dir / "query-regression-summary.py").write_text(
                "#!/usr/bin/env python3\ntampered\n", encoding="utf-8"
            )

        with tempfile.TemporaryDirectory(prefix="qr-ack-trust-") as tmp_str:
            proc, stderr = self._run_controller_trust(Path(tmp_str), mutate)
            self.assertEqual(proc.returncode, 1)
            self.assertIn("does not match the restore manifest", stderr)

    def test_trusted_scripts_manifest_source_mismatch_fails_closed(self) -> None:
        def mutate(base_artifact, candidate_artifact, trusted_dir, candidate_sha):
            manifest_path = trusted_dir / "trusted-scripts-manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["source_sha"] = "e" * 40
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        with tempfile.TemporaryDirectory(prefix="qr-ack-trust-") as tmp_str:
            proc, stderr = self._run_controller_trust(Path(tmp_str), mutate)
            self.assertEqual(proc.returncode, 1)
            self.assertIn("trusted scripts manifest source", stderr)

    def test_validation_file_run_id_mismatch_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-trust-") as tmp_str:
            tmp = Path(tmp_str)
            base_artifact, candidate_artifact, trusted_dir, candidate_sha = make_artifacts(tmp)
            kubeconfig = write_kubeconfig(tmp)
            fake_kubectl = tmp / "kubectl"
            fake_kubectl.write_text(FAKE_KUBECTL, encoding="utf-8")
            fake_kubectl.chmod(0o755)
            validation = tmp / "validation.json"
            validation.write_text(
                json.dumps(
                    {
                        "source_run_id": 999,
                        "source_run_attempt": 1,
                        "built_base_sha": BASE_SHA,
                        "candidate_sha": candidate_sha,
                    }
                ),
                encoding="utf-8",
            )
            proc = subprocess.run(
                [
                    sys.executable,
                    str(CONTROLLER_PATH),
                    "--validation-file",
                    str(validation),
                    "--run-id",
                    "1",
                    "--run-attempt",
                    "1",
                    "--base-artifact-dir",
                    str(base_artifact),
                    "--candidate-artifact-dir",
                    str(candidate_artifact),
                    "--kubeconfig",
                    str(kubeconfig),
                    "--trusted-summary",
                    str(trusted_dir / "query-regression-summary.py"),
                    "--trusted-scripts-manifest",
                    str(trusted_dir / "trusted-scripts-manifest.json"),
                    "--trusted-source-sha",
                    TRUSTED_SOURCE_SHA,
                    "--payload-dir",
                    str(tmp / "payload"),
                    "--result-dir",
                    str(tmp / "result"),
                    "--kubectl",
                    str(fake_kubectl),
                ],
                capture_output=True,
                text=True,
                timeout=60,
                env=dict(
                    os.environ,
                    GITHUB_OUTPUT="",
                    FAKE_KUBECTL_STATE=str(tmp / "kube-state"),
                ),
            )
            self.assertEqual(proc.returncode, 1)
            self.assertIn("does not match --run-id", proc.stderr)

    def test_validation_file_shas_win_over_env(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-trust-") as tmp_str:
            tmp = Path(tmp_str)
            base_artifact, candidate_artifact, trusted_dir, candidate_sha = make_artifacts(tmp)
            kubeconfig = write_kubeconfig(tmp)
            validation = tmp / "validation.json"
            validation.write_text(
                json.dumps(
                    {
                        "source_run_id": 1,
                        "source_run_attempt": 1,
                        "built_base_sha": "f" * 40,
                        "candidate_sha": candidate_sha,
                    }
                ),
                encoding="utf-8",
            )
            env = dict(
                os.environ,
                SOURCE_RUN_ID="1",
                SOURCE_RUN_ATTEMPT="1",
                VERIFIED_BASE_SHA=BASE_SHA,
                VERIFIED_CANDIDATE_SHA=candidate_sha,
                GITHUB_OUTPUT="",
                FAKE_KUBECTL_STATE=str(tmp / "kube-state"),
            )
            fake_kubectl = tmp / "kubectl"
            fake_kubectl.write_text(FAKE_KUBECTL, encoding="utf-8")
            fake_kubectl.chmod(0o755)
            proc = subprocess.run(
                [
                    sys.executable,
                    str(CONTROLLER_PATH),
                    "--validation-file",
                    str(validation),
                    "--base-artifact-dir",
                    str(base_artifact),
                    "--candidate-artifact-dir",
                    str(candidate_artifact),
                    "--kubeconfig",
                    str(kubeconfig),
                    "--trusted-summary",
                    str(trusted_dir / "query-regression-summary.py"),
                    "--trusted-scripts-manifest",
                    str(trusted_dir / "trusted-scripts-manifest.json"),
                    "--trusted-source-sha",
                    TRUSTED_SOURCE_SHA,
                    "--payload-dir",
                    str(tmp / "payload"),
                    "--result-dir",
                    str(tmp / "result"),
                    "--kubectl",
                    str(fake_kubectl),
                ],
                capture_output=True,
                text=True,
                timeout=60,
                env=env,
            )
            # The validated base SHA (f*40) must win over the env value and
            # therefore fail against the artifact manifest's BASE_SHA.
            self.assertEqual(proc.returncode, 1)
            self.assertIn("does not match the validated base SHA", proc.stderr)


class CommentMetadataTest(unittest.TestCase):
    def _validation(self) -> dict:
        return {
            "source_run_id": 123456789,
            "source_run_attempt": 2,
            "event": "pull_request",
            "pr_number": 42,
            "head_sha": "a" * 40,
            "event_base_sha": "b" * 40,
            "built_base_sha": "b" * 40,
            "candidate_sha": "c" * 40,
            "head_repo": "GreptimeTeam/greptimedb",
            "base_repo": "GreptimeTeam/greptimedb",
            "label": "query-regression",
        }

    def test_writes_comment_metadata_from_validated_values(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-comment-") as tmp_str:
            out = Path(tmp_str) / "query-regression-pr.json"
            env_patch = mock.patch.dict(
                os.environ, {"CONTROLLER_RUN_ID": "555", "CONTROLLER_RUN_ATTEMPT": "1"}
            )
            env_patch.start()
            self.addCleanup(env_patch.stop)
            controller.write_comment_metadata(out, self._validation(), status=0)
            data = json.loads(out.read_text(encoding="utf-8"))
            self.assertEqual(data["pr_number"], 42)
            self.assertEqual(data["head_sha"], "a" * 40)
            self.assertEqual(data["run_id"], 555)
            self.assertEqual(data["run_attempt"], 1)
            self.assertEqual(data["source_run_id"], 123456789)
            self.assertEqual(data["source_run_attempt"], 2)
            self.assertEqual(data["status"], 0)
            self.assertEqual(data["label"], "query-regression")

    def test_requires_controller_run_id_env(self) -> None:
        with tempfile.TemporaryDirectory(prefix="qr-ack-comment-") as tmp_str:
            out = Path(tmp_str) / "query-regression-pr.json"
            with mock.patch.dict(os.environ, {}, clear=False):
                os.environ.pop("CONTROLLER_RUN_ID", None)
                os.environ.pop("CONTROLLER_RUN_ATTEMPT", None)
                with self.assertRaises(controller.ControllerError) as ctx:
                    controller.write_comment_metadata(out, self._validation(), status=0)
            self.assertIn("CONTROLLER_RUN_ID", str(ctx.exception))


@unittest.skipIf(yaml is None, "PyYAML not available")
class WorkflowStructuralTest(unittest.TestCase):
    """Structural assertions on the workflows for the secure architecture."""

    WORKFLOW_DIR = Path(__file__).resolve().parents[2] / ".github" / "workflows"
    BUILD_WORKFLOW = WORKFLOW_DIR / "query-regression.yml"
    CONTROLLER_WORKFLOW = WORKFLOW_DIR / "query-regression-controller.yml"
    COMMENT_WORKFLOW = WORKFLOW_DIR / "query-regression-comment.yml"
    ACK_DIR = Path(__file__).resolve().parents[2] / ".github" / "runner-scale-sets" / "query-regression" / "ack"

    def _load(self, path: Path) -> dict:
        with open(path, encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
        # GitHub Actions YAML uses `on:`; PyYAML (YAML 1.1) parses it as the
        # boolean True. Normalize so tests can use wf["on"].
        if isinstance(data, dict) and "on" not in data and True in data:
            data["on"] = data.pop(True)
        return data

    def test_build_workflow_has_only_unprivileged_build_job(self) -> None:
        wf = self._load(self.BUILD_WORKFLOW)
        jobs = wf["jobs"]
        self.assertEqual(list(jobs.keys()), ["build"])
        build = jobs["build"]
        outputs = build["outputs"]
        self.assertIn("verified-base-sha", outputs)
        self.assertIn("verified-candidate-sha", outputs)
        self.assertIn("base-artifact-id", outputs)
        self.assertIn("candidate-artifact-id", outputs)
        # The build workflow is unprivileged: no ACK/cloud secrets, no
        # kubeconfig, no kubectl, no controller job, in executable content
        # (comments describe their absence).
        executable = []
        for step in build["steps"]:
            if "run" in step:
                executable.append(step["run"])
            if "uses" in step:
                executable.append(step["uses"])
        text = "\n".join(executable)
        for banned in ("ACK_KUBECONFIG", "KUBECONFIG", "kubectl", "query-regression-ack-controller.py"):
            self.assertNotIn(banned, text)
        self.assertNotIn("secrets.", text)

    def test_base_artifact_uploaded_before_candidate_build(self) -> None:
        wf = self._load(self.BUILD_WORKFLOW)
        build = wf["jobs"]["build"]
        step_names = [s.get("name") for s in build["steps"]]
        base_upload = step_names.index("Upload query regression base binaries")
        switch = step_names.index("Switch source to candidate")
        candidate_build = step_names.index("Build candidate greptime and query regression helpers")
        candidate_upload = step_names.index("Upload query regression candidate binaries")
        metadata_upload = step_names.index("Upload query regression metadata")
        self.assertLess(base_upload, switch)
        self.assertLess(switch, candidate_build)
        self.assertLess(candidate_build, candidate_upload)
        self.assertLess(candidate_upload, metadata_upload)

    def test_build_workflow_keeps_tooling_tests_and_immutable_validation(self) -> None:
        wf = self._load(self.BUILD_WORKFLOW)
        build = wf["jobs"]["build"]
        build_steps = "\n".join(s.get("run", "") or "" for s in build["steps"])
        for tooling_test in (
            "test_query_regression_runner_compaction_toctou.py",
            "test_query_regression_runner_otlp_trace_load.py",
            "test_query_regression_summary_otlp.py",
            "test_query_regression_case_selection.py",
        ):
            self.assertIn(tooling_test, build_steps)
        resolve = next(s for s in build["steps"] if s.get("id") == "resolve")
        self.assertIn("printf 'verified-base-sha=", resolve["run"])
        self.assertIn("printf 'verified-candidate-sha=", resolve["run"])
        self.assertIn('"${GITHUB_OUTPUT}"', resolve["run"])

    def test_build_workflow_metadata_artifact_is_strict(self) -> None:
        wf = self._load(self.BUILD_WORKFLOW)
        build = wf["jobs"]["build"]
        metadata_step = next(
            s for s in build["steps"] if s.get("name") == "Stage strict query regression metadata"
        )
        run = metadata_step["run"]
        # The metadata artifact contains only GitHub-context values and the
        # verified build outputs; no candidate-controlled content.
        for field in ("run_id", "run_attempt", "pr_number", "head_sha", "label",
                      "built_base_sha", "candidate_sha", "base_artifact_id", "candidate_artifact_id"):
            self.assertIn(field, run)
        upload = next(
            s for s in build["steps"] if s.get("name") == "Upload query regression metadata"
        )
        self.assertEqual(upload["with"]["name"], "query-regression-metadata")

    def test_controller_workflow_is_default_branch_follower(self) -> None:
        wf = self._load(self.CONTROLLER_WORKFLOW)
        trigger = wf["on"]["workflow_run"]
        self.assertEqual(trigger["workflows"], ["Query Regression"])
        self.assertEqual(trigger["types"], ["completed"])
        permissions = wf["permissions"]
        self.assertEqual(permissions.get("contents"), "read")
        self.assertEqual(permissions.get("actions"), "read")
        # The controller never needs write permissions.
        for scope in ("pull-requests", "issues", "checks", "statuses"):
            self.assertNotIn(scope, permissions)
        job = wf["jobs"]["controller"]
        self.assertEqual(job["runs-on"], "query-regression-ack-controller")
        # No PR checkout: checkouts only reference the default branch.
        for step in job["steps"]:
            if step.get("uses", "").startswith("actions/checkout"):
                self.assertNotIn("pull_request", json.dumps(step.get("with", {})))
                self.assertNotIn("head.sha", json.dumps(step.get("with", {})))
        checkout = next(s for s in job["steps"] if s.get("name") == "Checkout trusted default-branch scripts")
        self.assertEqual(checkout["with"]["ref"], "${{ github.event.repository.default_branch }}")
        self.assertEqual(checkout["with"]["persist-credentials"], False)

    def test_controller_workflow_has_no_ack_secret(self) -> None:
        text = self.CONTROLLER_WORKFLOW.read_text(encoding="utf-8")
        # The ACK_KUBECONFIG GitHub *secret* must not exist anywhere; only the
        # runner-local kubeconfig *path* variable is allowed. The only secret
        # reference is the built-in GITHUB_TOKEN.
        self.assertNotIn("secrets.ACK_KUBECONFIG", text)
        self.assertNotIn("ACK_KUBECONFIG:", text)
        secret_refs = set(re.findall(r"secrets\.([A-Z0-9_]+)", text))
        self.assertNotIn("ACK_KUBECONFIG", secret_refs)
        self.assertLessEqual(secret_refs, {"GITHUB_TOKEN"})
        self.assertIn("ACK_KUBECONFIG_PATH", text)

    def test_controller_workflow_uses_admission_gate_and_validated_downloads(self) -> None:
        wf = self._load(self.CONTROLLER_WORKFLOW)
        job = wf["jobs"]["controller"]
        steps = job["steps"]
        names = [s.get("name") for s in steps]
        admit_index = names.index("Admit originating run and artifacts (fail closed)")
        self.assertLess(admit_index, names.index("Download query regression base binaries"))
        self.assertLess(admit_index, names.index("Download query regression candidate binaries"))
        admit = steps[admit_index]
        self.assertIn("query-regression-admission.cjs", admit["run"])
        for download in ("Download query regression base binaries", "Download query regression candidate binaries"):
            step = steps[names.index(download)]
            self.assertIn("artifact-ids", step["with"])
            self.assertIn("run-id", step["with"])
            # The download run id branches on the invocation mode: the
            # workflow_run event id (PR/manual path) or the typed source run id
            # input (release workflow_call path) - never a mutable ref.
            self.assertEqual(
                step["with"]["run-id"],
                "${{ github.event_name == 'workflow_run' && github.event.workflow_run.id || inputs.source_run_id }}",
            )
        # The controller step consumes the validated values file.
        controller_step = steps[names.index("Run ACK query regression controller")]
        self.assertIn("--validation-file", controller_step["run"])
        self.assertIn("--kubeconfig", controller_step["run"])
        # The Job namespace/image are not overridable in the workflow.
        self.assertNotIn("--namespace", controller_step["run"])
        self.assertNotIn("--image", controller_step["run"])

    def test_controller_workflow_has_secure_workflow_call_path(self) -> None:
        wf = self._load(self.CONTROLLER_WORKFLOW)
        on = wf["on"]
        self.assertIn("workflow_call", on)
        inputs = on["workflow_call"]["inputs"]
        # Exact build outputs must arrive as typed required inputs.
        for name in (
            "source_run_id", "source_run_attempt", "base_sha", "candidate_sha",
            "base_artifact_id", "candidate_artifact_id", "case",
        ):
            self.assertIn(name, inputs)
            self.assertEqual(inputs[name]["required"], True)
        job = wf["jobs"]["controller"]
        # The caller context is read from the github context (runner-provided),
        # never from caller inputs: the env must expose the caller workflow,
        # event, ref, ref type, sha and default branch, plus the typed inputs
        # for cross-checking.
        env = job["env"]
        for key in ("SOURCE_MODE", "CALLER_WORKFLOW", "CALLER_EVENT", "CALLER_REF",
                    "CALLER_REF_TYPE", "CALLER_SHA", "CALLER_DEFAULT_BRANCH",
                    "INPUT_BASE_SHA", "INPUT_CANDIDATE_SHA",
                    "INPUT_BASE_ARTIFACT_ID", "INPUT_CANDIDATE_ARTIFACT_ID", "INPUT_CASE"):
            self.assertIn(key, env)
        self.assertEqual(
            env["CALLER_WORKFLOW"], "${{ github.workflow }}",
            "the caller workflow must come from the github context, not an input",
        )
        self.assertEqual(
            env["SOURCE_MODE"],
            "${{ github.event_name == 'workflow_run' && 'workflow_run' || 'workflow_call' }}",
        )
        # The release path must not require any ACK secret from the caller.
        self.assertNotIn("secrets", json.dumps(on["workflow_call"]))
        # The controller job must refuse non-Release callers and skip
        # workflow_run events that belong to release-only build runs.
        job_if = job["if"]
        self.assertIn("github.workflow == 'Release'", job_if)
        self.assertIn("github.event_name == 'push'", job_if)
        self.assertIn("github.event_name == 'schedule'", job_if)
        self.assertIn("github.event_name == 'workflow_dispatch'", job_if)
        self.assertIn("github.event.workflow_run.event != 'push'", job_if)
        self.assertIn("github.event.workflow_run.event != 'schedule'", job_if)

    def test_build_workflow_exposes_exact_outputs_for_release_controller(self) -> None:
        wf = self._load(self.BUILD_WORKFLOW)
        call_outputs = wf["on"]["workflow_call"]["outputs"]
        build_outputs = wf["jobs"]["build"]["outputs"]
        for name in (
            "verified-base-sha", "verified-candidate-sha", "base-artifact-id",
            "candidate-artifact-id", "run-id", "run-attempt",
        ):
            self.assertIn(name, call_outputs)
            self.assertIn(name, build_outputs)
        self.assertEqual(
            call_outputs["run-id"]["value"], "${{ jobs.build.outputs.run-id }}"
        )
        self.assertEqual(build_outputs["run-id"], "${{ github.run_id }}")
        self.assertEqual(build_outputs["run-attempt"], "${{ github.run_attempt }}")

    def test_controller_workflow_second_process_cleanup(self) -> None:
        wf = self._load(self.CONTROLLER_WORKFLOW)
        job = wf["jobs"]["controller"]
        names = [s.get("name") for s in job["steps"]]
        cleanup_name = "Clean up ACK benchmark job (second process, best effort)"
        self.assertIn(cleanup_name, names)
        cleanup = job["steps"][names.index(cleanup_name)]
        self.assertEqual(cleanup["if"], "always()")
        self.assertLess(names.index("Run ACK query regression controller"), names.index(cleanup_name))
        run = cleanup["run"]
        self.assertIn("--cleanup-only", run)
        self.assertIn("--run-id", run)
        self.assertIn("--run-attempt", run)
        self.assertIn("--kubeconfig", run)
        self.assertNotIn("desired-size", run)
        self.assertNotIn("autoscaling/nodepools", run)

    def test_controller_workflow_regenerates_comment_metadata(self) -> None:
        wf = self._load(self.CONTROLLER_WORKFLOW)
        job = wf["jobs"]["controller"]
        steps = "\n".join(s.get("run", "") or "" for s in job["steps"])
        self.assertIn("--comment-metadata", steps)
        self.assertIn("query-regression-pr.json", steps)
        upload = next(
            s for s in job["steps"] if s.get("name") == "Upload trusted comment artifact"
        )
        self.assertIn("query-regression-pr.json", upload["with"]["path"])

    def test_controller_workflow_workflow_timeout_margin(self) -> None:
        wf = self._load(self.CONTROLLER_WORKFLOW)
        job = wf["jobs"]["controller"]
        timeout_minutes = int(job["timeout-minutes"])
        self.assertEqual(timeout_minutes * 60, controller.WORKFLOW_JOB_TIMEOUT_SECONDS)
        margin = controller.WORKFLOW_JOB_TIMEOUT_SECONDS - controller.LIFECYCLE_TIMEOUT_DEFAULT
        self.assertGreaterEqual(margin, controller.SETUP_AND_OUTER_CLEANUP_MARGIN_MINIMUM)
        self.assertGreater(margin, controller.CLEANUP_ONLY_BUDGET)

    def test_comment_workflow_follows_controller_and_keeps_write_isolation(self) -> None:
        wf = self._load(self.COMMENT_WORKFLOW)
        trigger = wf["on"]["workflow_run"]
        self.assertEqual(trigger["workflows"], ["Query Regression Controller"])
        permissions = wf["permissions"]
        self.assertEqual(permissions.get("pull-requests"), "write")
        # The controller workflow itself must not have PR write.
        ctrl = self._load(self.CONTROLLER_WORKFLOW)
        self.assertNotIn("pull-requests", ctrl["permissions"])

    def test_workflow_actions_are_preexisting_pins(self) -> None:
        for path in (self.BUILD_WORKFLOW, self.CONTROLLER_WORKFLOW, self.COMMENT_WORKFLOW):
            wf = self._load(path)
            actions = set()
            for job in wf["jobs"].values():
                for step in job["steps"]:
                    if "uses" in step:
                        actions.add(step["uses"])
            for action in actions:
                self.assertIn(
                    action,
                    ("actions/checkout@v4", "actions/upload-artifact@v4",
                     "actions/download-artifact@v4", "actions/github-script@v7",
                     "marocchino/sticky-pull-request-comment@v2"),
                )

    def test_no_nodepool_scale_api_anywhere(self) -> None:
        for path in (self.BUILD_WORKFLOW, self.CONTROLLER_WORKFLOW, self.COMMENT_WORKFLOW):
            text = path.read_text(encoding="utf-8")
            for banned in ("desired-size", "autoscaling/nodepools", "nodepool desired", "scale-to-zero"):
                self.assertNotIn(banned, text)

    def test_ack_manifests_exist_and_are_gated(self) -> None:
        for name in (
            "namespace.yaml",
            "serviceaccounts.yaml",
            "rbac.yaml",
            "resourcequota.yaml",
            "limitrange.yaml",
            "networkpolicy.yaml",
            "validatingadmissionpolicy.yaml",
            "validatingadmissionpolicybinding.yaml",
        ):
            self.assertTrue((self.ACK_DIR / name).is_file(), f"missing {name}")
        vap = self.ACK_DIR / "validatingadmissionpolicy.yaml"
        text = vap.read_text(encoding="utf-8")
        self.assertIn("deployment-gated", text)
        binding = self.ACK_DIR / "validatingadmissionpolicybinding.yaml"
        self.assertIn("deployment-gated", binding.read_text(encoding="utf-8"))
        self.assertTrue((self.ACK_DIR / "apply-test.sh").is_file())
        self.assertTrue((self.ACK_DIR / "query-regression-policy-test.py").is_file())

    def test_ack_namespace_is_psa_restricted(self) -> None:
        namespace = self._load(self.ACK_DIR / "namespace.yaml")
        labels = namespace["metadata"]["labels"]
        self.assertEqual(namespace["metadata"]["name"], "query-regression-perf")
        self.assertEqual(labels["pod-security.kubernetes.io/enforce"], "restricted")

    def test_ack_rbac_is_namespaced_and_exact(self) -> None:
        rbac_text = (self.ACK_DIR / "rbac.yaml").read_text(encoding="utf-8")
        docs = list(yaml.safe_load_all(rbac_text))
        kinds = [doc["kind"] for doc in docs]
        self.assertIn("Role", kinds)
        self.assertIn("RoleBinding", kinds)
        role = next(doc for doc in docs if doc["kind"] == "Role")
        self.assertEqual(role["metadata"]["namespace"], "query-regression-perf")
        rules = role["rules"]
        verbs: dict[tuple[str, str], tuple[str, ...]] = {}
        for rule in rules:
            for group in rule["apiGroups"]:
                for res in rule["resources"]:
                    verbs[(group, res)] = tuple(sorted(rule["verbs"]))
        self.assertEqual(verbs[("batch", "jobs")], ("create", "delete", "get"))
        self.assertEqual(verbs[("", "pods")], ("get", "list"))
        self.assertEqual(verbs[("", "pods/exec")], ("create",))
        self.assertEqual(verbs[("", "pods/log")], ("get",))
        self.assertEqual(verbs[("", "events")], ("list",))
        # No cluster-wide permissions.
        for rule in rules:
            self.assertNotIn("cluster", rule)

    def test_ack_resourcequota_caps_one_job_and_pod(self) -> None:
        quota = self._load(self.ACK_DIR / "resourcequota.yaml")
        hard = quota["spec"]["hard"]
        self.assertEqual(hard["count/jobs"], "1")
        self.assertEqual(hard["count/pods"], "1")

    def test_ack_networkpolicy_denies_all(self) -> None:
        policy = self._load(self.ACK_DIR / "networkpolicy.yaml")
        self.assertEqual(policy["spec"]["podSelector"], {})
        self.assertIn("Ingress", policy["spec"]["policyTypes"])
        self.assertIn("Egress", policy["spec"]["policyTypes"])

    def test_checks_runs_new_tests(self) -> None:
        checks = self._load(self.WORKFLOW_DIR / "checks.yml")
        text = checks and ""
        all_steps = []
        for job in checks["jobs"].values():
            for step in job.get("steps", []):
                all_steps.append(step.get("run", "") or "")
        joined = "\n".join(all_steps)
        self.assertIn("query-regression-admission.test.cjs", joined)
        self.assertIn("query-regression-policy-test.py", joined)


if __name__ == "__main__":
    unittest.main()
