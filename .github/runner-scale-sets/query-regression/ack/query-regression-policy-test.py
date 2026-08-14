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

"""Offline policy tests for the declarative ACK least-privilege boundary.

Parses the manifests under ``.github/runner-scale-sets/query-regression/ack/``
and asserts the security properties without contacting any cluster:

* namespace name + PSA restricted labels;
* tokenless service accounts (default/controller/workload);
* exact namespaced Role/RoleBinding verbs (jobs create/get/delete; pods
  get/list; pods/exec create; pods/log get; events list), no cluster scope;
* ResourceQuota max 1 Job/1 Pod with the exact resource totals;
* LimitRange within the benchmark contract;
* deny-all NetworkPolicy;
* deployment-gated ValidatingAdmissionPolicies + bindings covering the
  workload, direct-pods denial, and exec CONNECT;
* K8s 1.34 CEL correctness: the has(x.field) macro only (no receiver
  x.has(field) form anywhere), enforcement bindings are exactly
  ["Deny", "Audit"] (never Deny+Warn), and the exec policy targets the
  PodExecOptions object (request.namespace/request.name + object.container/
  command/tty/stdin/stdout/stderr) with no parent-Pod metadata;
* resources contract: requests/limits are optional map fields and must be
  has()-guarded (has(c.resources.requests), has(c.resources.limits)) before
  any access (ACK v1.34.1 typeChecking flags unguarded access as
  undefined-field), with size() == 3 and exact cpu/memory/ephemeral-storage
  keys+values on both maps, plus the missing/extra mutation semantics
  mirror;
* scheduling bypass/control field rejections (nodeName, suspend,
  schedulerName, priorityClassName, priority, affinity, schedulingGates);
* structural sync between the VAP allowlists and the trusted controller's
  canonical constants (exec/cp argv+stream tuples, bootstrap script, env
  contract, image/resources contract) plus the controller-rendered dry-run
  Job manifest. The real API server remains the authoritative check
  (apply-test.sh with QR_APPLY_TEST_CANARY=1).
* secure-split workflow structure: release.yml must gate the release on the
  controller job (never the build alone) with the build's exact outputs as
  typed inputs, the controller workflow_call path must be typed and
  secret-free, and checks.yml must set up uv before the first uv run and run
  the full controller/admission/comment/policy suites.

Run with PyYAML available (checks.yml uses ``uv run --no-project --with
pyyaml``); without PyYAML every test is skipped rather than weakened.
"""

from __future__ import annotations

import importlib.util
import json
import os
import re
import subprocess
import sys
import unittest
from pathlib import Path

try:
    import yaml
except ImportError:  # pragma: no cover - depends on the environment
    yaml = None

ACK_DIR = Path(__file__).resolve().parent
CONTROLLER_PATH = ACK_DIR.parents[2] / "scripts" / "query-regression-ack-controller.py"

NAMESPACE = "query-regression-perf"
WORKLOAD_SA = "query-regression-workload"
CONTROLLER_SA = "query-regression-controller"
IMAGE_DIGEST = (
    "greptime-registry.cn-hangzhou.cr.aliyuncs.com/greptime/"
    "greptimedb-query-regression-runner@sha256:"
    "e713b294e23b7e15184e558866c90025e59930033e72c97650dbc7f1ca022d11"
)


def load_all(path: Path) -> list[dict]:
    with open(path, encoding="utf-8") as fh:
        return list(yaml.safe_load_all(fh))


def load_controller():
    """Import the trusted controller module for its canonical constants."""
    spec = importlib.util.spec_from_file_location("qr_ack_controller", CONTROLLER_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def dry_run_env(run_id: str) -> dict:
    """Environment the controller --dry-run would see in the workflow."""
    env = dict(os.environ)
    env.update(
        {
            "SOURCE_RUN_ID": run_id,
            "SOURCE_RUN_ATTEMPT": "1",
            "VERIFIED_BASE_SHA": "a" * 40,
            "VERIFIED_CANDIDATE_SHA": "b" * 40,
            "CASE_PATHS": "all",
            "HTTP_TIMEOUT": "300",
            "ALLOW_LARGE_FIXTURE": "true",
            "RUN_URL": f"https://github.com/GreptimeTeam/greptimedb/actions/runs/{run_id}",
            "CASE_NAME": "policy-test",
            "CARGO_PROFILE": "nightly",
            # main() setdefaults these from the validated SHAs.
            "BASE_REF": "a" * 40,
            "CANDIDATE_REF": "b" * 40,
            "GITHUB_OUTPUT": "",
        }
    )
    return env


def render_manifest(controller, run_id: str = "123456789") -> dict:
    """Render the Job manifest exactly as the controller would (--dry-run)."""
    proc = subprocess.run(
        [sys.executable, str(CONTROLLER_PATH), "--dry-run"],
        capture_output=True,
        text=True,
        timeout=60,
        env=dry_run_env(run_id),
    )
    if proc.returncode != 0:
        raise AssertionError(f"controller --dry-run failed: {proc.stderr[-2000:]}")
    return json.loads(proc.stdout)


def cel_env_names_unique(env: list[dict]) -> bool:
    """Python mirror of the VAP env-name uniqueness CEL expression semantics.

    The policy expresses "env names must be unique" as
    ``c.env.all(e1, c.env.exists_one(e2, e2.name == e1.name))`` -- every env
    entry has exactly one same-named entry, which holds iff the names are
    pairwise unique (an empty env passes vacuously). The tests assert the CEL
    text itself; this mirror pins the semantics so a rewrite that keeps the
    text but changes meaning is caught, and duplicate envs (including
    byte-identical entries) stay denied.
    """
    names = [e["name"] for e in env]
    return all(names.count(name) == 1 for name in names)


# The exact benchmark resources contract (must equal the controller-rendered
# Job manifest and the CEL values pinned in the tests below; a drift in any
# of the three is a test failure).
RESOURCE_CONTRACT = {
    "requests": {"cpu": "4", "memory": "12Gi", "ephemeral-storage": "20Gi"},
    "limits": {"cpu": "8", "memory": "16Gi", "ephemeral-storage": "40Gi"},
}


def cel_resources_conform(container: dict) -> bool:
    """Python mirror of the VAP resources CEL expression semantics.

    Mirrors the exact chain in the policy: has(c.resources) &&
    has(c.resources.requests) && requests.size() == 3 &&
    has(c.resources.limits) && limits.size() == 3, then the
    'key' in <map> && <map>[key] == value membership+value checks for
    cpu/memory/ephemeral-storage in both maps. The combination is airtight:
    a missing key fails the membership check, a renamed key fails it too, an
    extra key pushes the size past 3, and a wrong value fails the equality
    check -- so each map must be exactly the three canonical entries. The
    tests assert the CEL text itself; this mirror pins the semantics so a
    rewrite that keeps the text but changes meaning is caught.
    """
    resources = container.get("resources")
    if not isinstance(resources, dict):
        return False
    for side in ("requests", "limits"):
        resource_map = resources.get(side)
        if not isinstance(resource_map, dict) or len(resource_map) != 3:
            return False
        for key, expected in RESOURCE_CONTRACT[side].items():
            if key not in resource_map or resource_map[key] != expected:
                return False
    return True


@unittest.skipIf(yaml is None, "PyYAML not available")
class PolicyManifestTest(unittest.TestCase):
    def test_namespace_is_psa_restricted(self) -> None:
        doc = load_all(ACK_DIR / "namespace.yaml")[0]
        self.assertEqual(doc["kind"], "Namespace")
        self.assertEqual(doc["metadata"]["name"], NAMESPACE)
        labels = doc["metadata"]["labels"]
        self.assertEqual(labels["pod-security.kubernetes.io/enforce"], "restricted")
        self.assertEqual(labels["pod-security.kubernetes.io/audit"], "restricted")
        self.assertEqual(labels["pod-security.kubernetes.io/warn"], "restricted")

    def test_tokenless_service_accounts(self) -> None:
        docs = load_all(ACK_DIR / "serviceaccounts.yaml")
        kinds = [d["kind"] for d in docs]
        self.assertEqual(kinds, ["ServiceAccount", "ServiceAccount", "ServiceAccount"])
        names = {d["metadata"]["name"] for d in docs}
        self.assertEqual(names, {"default", WORKLOAD_SA, CONTROLLER_SA})
        for doc in docs:
            self.assertEqual(doc["metadata"]["namespace"], NAMESPACE)
            self.assertEqual(
                doc["metadata"]["annotations"]["greptimedb.io/tokenless"], "true"
            )

    def test_rbac_is_exact_namespaced_role_and_binding(self) -> None:
        docs = load_all(ACK_DIR / "rbac.yaml")
        role = next(d for d in docs if d["kind"] == "Role")
        binding = next(d for d in docs if d["kind"] == "RoleBinding")
        self.assertEqual(role["metadata"]["namespace"], NAMESPACE)
        self.assertEqual(binding["metadata"]["namespace"], NAMESPACE)
        self.assertEqual(binding["roleRef"]["name"], role["metadata"]["name"])
        rules = {
            (r["apiGroups"][0], res): tuple(r["verbs"])
            for r in role["rules"]
            for res in r["resources"]
        }
        self.assertEqual(rules[("batch", "jobs")], ("create", "get", "delete"))
        self.assertEqual(rules[("", "pods")], ("get", "list"))
        self.assertEqual(rules[("", "pods/exec")], ("create",))
        self.assertEqual(rules[("", "pods/log")], ("get",))
        self.assertEqual(rules[("", "events")], ("list",))
        # Nothing cluster-scoped and no forbidden verbs.
        for rule in role["rules"]:
            self.assertNotIn("cluster", rule)
            for verb in rule["verbs"]:
                self.assertIn(
                    verb, ("create", "get", "delete", "list", "update", "watch", "patch")
                )

    def test_resourcequota_caps_one_job_and_pod_with_exact_totals(self) -> None:
        quota = load_all(ACK_DIR / "resourcequota.yaml")[0]
        hard = quota["spec"]["hard"]
        self.assertEqual(hard["count/jobs"], "1")
        self.assertEqual(hard["count/pods"], "1")
        self.assertEqual(hard["requests.cpu"], "4")
        self.assertEqual(hard["requests.memory"], "12Gi")
        self.assertEqual(hard["requests.ephemeral-storage"], "20Gi")
        self.assertEqual(hard["limits.cpu"], "8")
        self.assertEqual(hard["limits.memory"], "16Gi")
        self.assertEqual(hard["limits.ephemeral-storage"], "40Gi")

    def test_limitrange_within_benchmark_contract(self) -> None:
        limit_range = load_all(ACK_DIR / "limitrange.yaml")[0]
        limit = limit_range["spec"]["limits"][0]
        self.assertEqual(limit["type"], "Container")
        self.assertEqual(limit["max"]["cpu"], "8")
        self.assertEqual(limit["max"]["memory"], "16Gi")
        self.assertEqual(limit["default"]["cpu"], "8")
        self.assertEqual(limit["defaultRequest"]["cpu"], "4")

    def test_networkpolicy_denies_all(self) -> None:
        policy = load_all(ACK_DIR / "networkpolicy.yaml")[0]
        self.assertEqual(policy["spec"]["podSelector"], {})
        self.assertIn("Ingress", policy["spec"]["policyTypes"])
        self.assertIn("Egress", policy["spec"]["policyTypes"])

    def test_vaps_exist_and_are_deployment_gated(self) -> None:
        docs = load_all(ACK_DIR / "validatingadmissionpolicy.yaml")
        kinds = [d["kind"] for d in docs]
        self.assertEqual(kinds, ["ValidatingAdmissionPolicy"] * 3)
        names = {
            d["metadata"]["name"] for d in docs
        }
        self.assertEqual(
            names,
            {"query-regression-workload", "query-regression-pods", "query-regression-exec"},
        )
        for doc in docs:
            self.assertEqual(
                doc["metadata"]["annotations"]["greptimedb.io/deployment-gated"], "true"
            )
            self.assertEqual(doc["spec"]["failurePolicy"], "Fail")

    def test_no_receiver_has_form_anywhere_in_expressions(self) -> None:
        """K8s 1.34 CEL: presence is has(x.field), never x.has(field)."""
        docs = load_all(ACK_DIR / "validatingadmissionpolicy.yaml")
        expressions = [
            v["expression"]
            for doc in docs
            for v in doc["spec"]["validations"]
        ]
        for expression in expressions:
            self.assertIsNone(
                re.search(r"\.has\(", expression),
                f"receiver-style .has( found in expression: {expression}",
            )

    def test_workload_vap_covers_the_security_contract(self) -> None:
        doc = next(
            d
            for d in load_all(ACK_DIR / "validatingadmissionpolicy.yaml")
            if d["metadata"]["name"] == "query-regression-workload"
        )
        expressions = "\n".join(v["expression"] for v in doc["spec"]["validations"])
        for needle in (
            "object.metadata.namespace == 'query-regression-perf'",
            "object.metadata.name.matches('^query-regression-[1-9][0-9]*-[1-9][0-9]*$')",
            "object.metadata.labels.size() == 3",
            "object.metadata.annotations.size() == 2",
            "object.spec.backoffLimit == 0",
            "object.spec.activeDeadlineSeconds == 10800",
            "object.spec.ttlSecondsAfterFinished == 600",
            "object.spec.podReplacementPolicy == 'Failed'",
            "object.spec.template.spec.restartPolicy == 'Never'",
            "object.spec.template.spec.automountServiceAccountToken == false",
            "object.spec.template.spec.serviceAccountName == 'query-regression-workload'",
            "containers.size() == 1",
            "!has(object.spec.template.spec.initContainers)",
            "!has(object.spec.template.spec.ephemeralContainers)",
            "c.name == 'benchmark'",
            "c.command == ['/bin/sh', '-c']",
            "!has(c.envFrom)",
            "!has(c.ports)",
            "!has(e.valueFrom)",
            "c.resources.requests.cpu == '4'",
            "c.resources.limits['ephemeral-storage'] == '40Gi'",
            "object.spec.template.spec.nodeSelector.size() == 1",
            "object.spec.template.spec.nodeSelector['alibabacloud.com/nodepool-id'] == 'npb5ff93bea3a447a698fe31ebc997ea31'",
            "tolerations.size() == 1",
            "has(object.spec.template.spec.securityContext)",
            "seccompProfile.type == 'RuntimeDefault'",
            "allowPrivilegeEscalation == false",
            "capabilities.drop == ['ALL']",
            "privileged == false",
            "!has(object.spec.template.spec.hostNetwork)",
            "!has(object.spec.template.spec.hostPID)",
            "!has(object.spec.template.spec.hostIPC)",
            "!has(object.spec.template.spec.nodeName)",
            "!has(object.spec.suspend)",
            "!has(object.spec.template.spec.schedulerName)",
            "!has(object.spec.template.spec.priorityClassName)",
            "!has(object.spec.template.spec.priority)",
            "!has(object.spec.template.spec.affinity)",
            "!has(object.spec.template.spec.schedulingGates)",
            "object.spec.template.spec.volumes.size() == 2",
            "has(v.emptyDir)",
            "!has(v.secret)",
            "!has(v.persistentVolumeClaim)",
            "!has(v.hostPath)",
            "m.mountPath == '/payload'",
            "m.mountPath == '/work'",
            "object.spec.template.metadata.labels == object.metadata.labels",
        ):
            self.assertIn(needle, expressions, f"missing CEL constraint: {needle}")

    def test_workload_vap_bootstrap_is_exact_not_prefix(self) -> None:
        controller = load_controller()
        doc = next(
            d
            for d in load_all(ACK_DIR / "validatingadmissionpolicy.yaml")
            if d["metadata"]["name"] == "query-regression-workload"
        )
        expressions = "\n".join(v["expression"] for v in doc["spec"]["validations"])
        bootstrap_cel = controller.BOOTSTRAP_SCRIPT.replace("\n", "\\n")
        self.assertIn(f"c.args[0] == '{bootstrap_cel}'", expressions)
        # Exact equality means an appended bootstrap line cannot pass; the
        # single-arg cardinality check means a second arg cannot pass either.
        self.assertIn("c.args.size() == 1", expressions)
        self.assertNotIn("startsWith('set -eu')", expressions)

    def test_workload_vap_env_contract_matches_controller_constants(self) -> None:
        controller = load_controller()
        doc = next(
            d
            for d in load_all(ACK_DIR / "validatingadmissionpolicy.yaml")
            if d["metadata"]["name"] == "query-regression-workload"
        )
        expressions = "\n".join(v["expression"] for v in doc["spec"]["validations"])
        expected_names = controller.PASSTHROUGH_ENV + [name for name, _ in controller.POD_ENV]
        allowlist = "[" + ",".join(f"'{n}'" for n in expected_names) + "]"
        self.assertIn(allowlist, expressions)
        self.assertIn(f"c.env.size() == {len(expected_names)}", expressions)
        # Duplicate env entries are rejected by the K8s 1.34-safe uniqueness
        # check (nested core macros all + exists_one). The distinct() list
        # macro is undeclared in the Kubernetes CEL environment (ACK v1.34.1
        # dry-run: "undeclared reference to distinct") and must never be used.
        self.assertNotIn(".distinct(", expressions)
        self.assertIn(
            "c.env.all(e1, c.env.exists_one(e2, e2.name == e1.name))", expressions
        )
        # Fixed path values.
        for name, value in controller.POD_ENV:
            self.assertIn(
                f"e.name == '{name}' && e.value == '{value}'", expressions,
                f"missing fixed env value for {name}",
            )
        # Bounded variable value formats.
        bounded = {
            "CASE_PATHS": "e.value.matches('^(all|heavy|[A-Za-z0-9_./,-]{1,200})$')",
            "HTTP_TIMEOUT": "e.value.matches('^[0-9]{1,5}$')",
            "ALLOW_LARGE_FIXTURE": "e.value.matches('^(true|false)$')",
            "RUN_URL": "e.value.startsWith('https://') && e.value.size() <= 300 && e.value.contains('/actions/runs/')",
            "CASE_NAME": "e.value.size() >= 1 && e.value.size() <= 100",
            "BASE_REF": "e.value.matches('^[0-9a-f]{40}$')",
            "CANDIDATE_REF": "e.value.matches('^[0-9a-f]{40}$')",
            "CARGO_PROFILE": "e.value.matches('^(nightly|release|dev)$')",
        }
        for name in controller.PASSTHROUGH_ENV:
            self.assertIn(
                f"e.name == '{name}' && {bounded[name]}", expressions,
                f"missing bounded env format for {name}",
            )

    def test_no_cel_distinct_macro_anywhere_in_policies(self) -> None:
        """K8s 1.34 CEL does not declare the list distinct() macro: it comes
        from the cel-go lists extension that Kubernetes never registers, so any
        expression using it fails server-side compilation (exactly what the ACK
        v1.34.1 dry-run hit on the env-uniqueness check). No expression in any
        of the three policies may use it."""
        docs = load_all(ACK_DIR / "validatingadmissionpolicy.yaml")
        expressions = [
            v["expression"]
            for doc in docs
            for v in doc["spec"]["validations"]
        ]
        for expression in expressions:
            self.assertNotIn(
                ".distinct(", expression,
                f"undeclared .distinct( macro found in expression: {expression}",
            )

    def test_workload_vap_env_uniqueness_keeps_duplicates_denied(self) -> None:
        """The env-name uniqueness check must be the K8s 1.34-safe nested
        core-macro form, and its semantics must keep duplicate env names
        denied (fail-closed).

        Semantic pin: cel_env_names_unique() mirrors the exact CEL text
        asserted below (c.env.all(e1, c.env.exists_one(e2, e2.name ==
        e1.name))) -- every env entry has exactly one same-named entry, i.e.
        pairwise-unique names. The canonical 16-entry env passes; a name
        appearing twice fails, including byte-identical duplicate entries
        (which a naive e2 != e1 self-exclusion would wrongly let through);
        an empty env passes vacuously but is independently denied by the
        c.env.size() == 16 validation, so the policy stays fail-closed
        overall.
        """
        controller = load_controller()
        doc = next(
            d
            for d in load_all(ACK_DIR / "validatingadmissionpolicy.yaml")
            if d["metadata"]["name"] == "query-regression-workload"
        )
        expressions = "\n".join(v["expression"] for v in doc["spec"]["validations"])
        self.assertIn(
            "c.env.all(e1, c.env.exists_one(e2, e2.name == e1.name))", expressions
        )
        self.assertNotIn(".distinct(", expressions)
        canonical = [
            {"name": name, "value": ""} for name in controller.PASSTHROUGH_ENV
        ] + [
            {"name": name, "value": value} for name, value in controller.POD_ENV
        ]
        self.assertEqual(len(canonical), 16)
        self.assertTrue(cel_env_names_unique(canonical))
        # Byte-identical duplicate entry: still denied (exists_one counts it).
        duplicated = canonical + [dict(canonical[0])]
        self.assertFalse(cel_env_names_unique(duplicated))
        # Same name, different value: also denied.
        renamed = list(canonical)
        renamed[1] = {"name": canonical[0]["name"], "value": "different"}
        self.assertFalse(cel_env_names_unique(renamed))
        # Vacuous truth on an empty env; the size() == 16 validation (asserted
        # by test_workload_vap_env_contract_matches_controller_constants) is
        # what denies an empty/missing env, keeping the policy fail-closed.
        self.assertTrue(cel_env_names_unique([]))

    def test_workload_vap_resources_contract_is_guarded_and_exact(self) -> None:
        """The resources validation must be typed-CEL-safe on ACK v1.34.1 and
        pin the exact benchmark contract.

        ACK v1.34.1 type checking flags unguarded access to the optional
        requests/limits map fields under c.resources as undefined-field:
        has(c.resources) alone does not make c.resources.requests.cpu
        type-checkable because requests/limits are themselves optional. The
        fixed expression guards each optional level in one && chain --
        has(c.resources), has(c.resources.requests), has(c.resources.limits)
        -- with the guard state propagating left-to-right through the chain
        (the same pattern the securityContext/volumes checks in this policy
        already use) -- then pins size() == 3 on each map and the exact
        cpu/memory/ephemeral-storage membership+value pairs ('key' in <map>
        is the API-server-acceptable presence check for map entries).
        size() == 3 plus the three membership+value checks is airtight: a
        missing key fails the membership check, a renamed key fails it too,
        an extra key pushes the size past 3, and a wrong value fails the
        equality check, so strictly no extra resource key can pass.
        """
        doc = next(
            d
            for d in load_all(ACK_DIR / "validatingadmissionpolicy.yaml")
            if d["metadata"]["name"] == "query-regression-workload"
        )
        resource_expr = next(
            v["expression"]
            for v in doc["spec"]["validations"]
            if "has(c.resources)" in v["expression"]
        )
        for needle in (
            # Optional-field guards: each has() precedes every access of the
            # guarded field within the same && chain.
            "has(c.resources) &&",
            "has(c.resources.requests) && c.resources.requests.size() == 3",
            "has(c.resources.limits) && c.resources.limits.size() == 3",
            # Exact keys + values for both maps (size()==3 + these three
            # membership+value pairs forbid any extra/renamed/missing key).
            "'cpu' in c.resources.requests && c.resources.requests.cpu == '4'",
            "'memory' in c.resources.requests && c.resources.requests.memory == '12Gi'",
            "'ephemeral-storage' in c.resources.requests && c.resources.requests['ephemeral-storage'] == '20Gi'",
            "'cpu' in c.resources.limits && c.resources.limits.cpu == '8'",
            "'memory' in c.resources.limits && c.resources.limits.memory == '16Gi'",
            "'ephemeral-storage' in c.resources.limits && c.resources.limits['ephemeral-storage'] == '40Gi'",
        ):
            self.assertIn(needle, resource_expr, f"resources CEL missing: {needle}")
        # The old unguarded shape (requests/limits dereferenced directly after
        # has(c.resources)) must be gone from the whole workload policy.
        expressions = "\n".join(v["expression"] for v in doc["spec"]["validations"])
        self.assertNotIn("has(c.resources) && c.resources.requests", expressions)
        self.assertNotIn(".distinct(", resource_expr)

    def test_workload_vap_resources_missing_extra_mutation_semantics(self) -> None:
        """Resources mutations (missing/extra/renamed/wrong-value keys) must
        fail closed, mirroring the exact CEL semantics.

        cel_resources_conform() mirrors the policy's resources expression
        (guards + size() == 3 + membership/value checks) so a rewrite that
        keeps the CEL text but changes meaning is caught. The canonical
        controller-rendered resources pass; every mutation -- missing
        resources/requests/limits, an extra key in either map, a renamed key,
        a missing canonical key, a wrong value, and empty maps -- fails.
        RESOURCE_CONTRACT is additionally pinned against the controller-
        rendered manifest, so the CEL text (asserted in
        test_workload_vap_resources_contract_is_guarded_and_exact), the
        mirror, and the controller cannot drift apart.
        """
        controller = load_controller()
        rendered = render_manifest(controller)["spec"]["template"]["spec"]["containers"][0]["resources"]
        self.assertEqual(rendered, RESOURCE_CONTRACT)

        def canonical() -> dict:
            return {
                "resources": {
                    side: dict(entries) for side, entries in RESOURCE_CONTRACT.items()
                }
            }

        self.assertTrue(cel_resources_conform(canonical()))
        # Missing resources / requests / limits.
        self.assertFalse(cel_resources_conform({}))
        self.assertFalse(cel_resources_conform({"resources": {"limits": dict(RESOURCE_CONTRACT["limits"])}}))
        self.assertFalse(cel_resources_conform({"resources": {"requests": dict(RESOURCE_CONTRACT["requests"])}}))
        # Extra key in either map (size() == 3 must fail closed).
        extra_requests = canonical()
        extra_requests["resources"]["requests"]["hugepages-2Mi"] = "1Gi"
        self.assertFalse(cel_resources_conform(extra_requests))
        extra_limits = canonical()
        extra_limits["resources"]["limits"]["hugepages-2Mi"] = "1Gi"
        self.assertFalse(cel_resources_conform(extra_limits))
        # Renamed key (same size, membership check must fail closed).
        renamed = canonical()
        renamed["resources"]["requests"]["cpus"] = renamed["resources"]["requests"].pop("cpu")
        self.assertFalse(cel_resources_conform(renamed))
        # Missing canonical key.
        missing_key = canonical()
        del missing_key["resources"]["requests"]["memory"]
        self.assertFalse(cel_resources_conform(missing_key))
        # Wrong value.
        wrong_value = canonical()
        wrong_value["resources"]["limits"]["cpu"] = "16"
        self.assertFalse(cel_resources_conform(wrong_value))
        # Empty maps.
        self.assertFalse(cel_resources_conform({"resources": {"requests": {}, "limits": {}}}))

    def test_pods_vap_denies_direct_pods(self) -> None:
        doc = next(
            d
            for d in load_all(ACK_DIR / "validatingadmissionpolicy.yaml")
            if d["metadata"]["name"] == "query-regression-pods"
        )
        expressions = "\n".join(v["expression"] for v in doc["spec"]["validations"])
        self.assertIn("ownerReferences.exists", expressions)
        self.assertIn("o.kind == 'Job'", expressions)
        # The pod name regex matches the Job-generated <job>-<5char> pattern.
        self.assertIn(
            "object.metadata.name.matches('^query-regression-[1-9][0-9]*-[1-9][0-9]*-[a-z0-9]{5}$')",
            expressions,
        )

    def test_exec_vap_uses_podexecoptions_not_parent_pod_metadata(self) -> None:
        controller = load_controller()
        doc = next(
            d
            for d in load_all(ACK_DIR / "validatingadmissionpolicy.yaml")
            if d["metadata"]["name"] == "query-regression-exec"
        )
        rules = doc["spec"]["matchConstraints"]["resourceRules"]
        self.assertEqual(rules[0]["operations"], ["CONNECT"])
        self.assertEqual(rules[0]["resources"], ["pods/exec"])
        expressions = "\n".join(v["expression"] for v in doc["spec"]["validations"])
        # CONNECT sees the PodExecOptions object: the target pod is pinned via
        # request.namespace/request.name, options via object.* -- never
        # object.metadata/labels/ownerReferences (PodExecOptions has none).
        self.assertIn("request.namespace == 'query-regression-perf'", expressions)
        self.assertIn(
            "request.name.matches('^query-regression-[1-9][0-9]*-[1-9][0-9]*-[a-z0-9]{5}$')",
            expressions,
        )
        self.assertIn("object.container == 'benchmark'", expressions)
        self.assertIn("!has(object.tty) || object.tty == false", expressions)
        self.assertIn("object.command", expressions)
        self.assertNotIn("object.metadata", expressions)
        self.assertNotIn("ownerReferences", expressions)

    def test_exec_vap_allowlist_matches_controller_constants(self) -> None:
        controller = load_controller()
        doc = next(
            d
            for d in load_all(ACK_DIR / "validatingadmissionpolicy.yaml")
            if d["metadata"]["name"] == "query-regression-exec"
        )
        expressions = "\n".join(v["expression"] for v in doc["spec"]["validations"])
        # Every canonical sh -c script must appear as its exact argv tuple.
        for script in controller.EXEC_SCRIPTS:
            self.assertIn(
                f"['sh','-c','{script}']", expressions,
                f"exec VAP missing canonical script tuple: {script}",
            )
        # kubectl cp remote tar tuples (pinned kubectl ABI).
        for argv in controller.CP_PUSH_COMMANDS + controller.CP_PULL_COMMANDS:
            cel = "[" + ",".join(f"'{a}'" for a in argv) + "]"
            self.assertIn(cel, expressions, f"exec VAP missing cp tuple: {argv}")
        # Stream-flag tuples for the three protocol shapes.
        self.assertIn("object.stdin == true", expressions)   # cp push (stdin wired)
        self.assertIn("object.stdin == false", expressions)  # sh -c and cp pull
        self.assertIn("object.stdout == true", expressions)
        self.assertIn("object.stderr == true", expressions)
        # No allowlisted script may carry a single quote (CEL single-quoted
        # embedding) or a newline.
        for script in controller.EXEC_SCRIPTS:
            self.assertNotIn("'", script)
            self.assertNotIn("\n", script)

    def test_bindings_are_deny_audit_never_deny_warn(self) -> None:
        docs = load_all(ACK_DIR / "validatingadmissionpolicybinding.yaml")
        kinds = [d["kind"] for d in docs]
        self.assertEqual(kinds, ["ValidatingAdmissionPolicyBinding"] * 3)
        policies = {d["metadata"]["name"] for d in docs}
        self.assertEqual(
            policies,
            {"query-regression-workload", "query-regression-pods", "query-regression-exec"},
        )
        for doc in docs:
            self.assertEqual(
                doc["metadata"]["annotations"]["greptimedb.io/deployment-gated"], "true"
            )
            # K8s 1.34: Deny and Warn are mutually exclusive in one binding;
            # enforcement is exactly Deny+Audit.
            self.assertEqual(doc["spec"]["validationActions"], ["Deny", "Audit"])
            self.assertNotIn("Warn", doc["spec"]["validationActions"])
            selector = doc["spec"]["matchResources"]["namespaceSelector"]["matchLabels"]
            self.assertEqual(selector["kubernetes.io/metadata.name"], NAMESPACE)
            defined = {
                d["metadata"]["name"]
                for d in load_all(ACK_DIR / "validatingadmissionpolicy.yaml")
            }
            self.assertIn(doc["spec"]["policyName"], defined)

    def test_rollout_bindings_are_warn_audit_and_separate(self) -> None:
        path = ACK_DIR / "validatingadmissionpolicybinding-rollout.yaml"
        self.assertTrue(path.is_file(), "rollout binding file must exist")
        docs = load_all(path)
        self.assertEqual(len(docs), 3)
        for doc in docs:
            self.assertEqual(doc["kind"], "ValidatingAdmissionPolicyBinding")
            # Rollout observes without denying: Warn+Audit, never Deny.
            self.assertEqual(doc["spec"]["validationActions"], ["Warn", "Audit"])
            self.assertNotIn("Deny", doc["spec"]["validationActions"])
            self.assertEqual(
                doc["metadata"]["annotations"]["greptimedb.io/rollout"], "warn-audit-only"
            )
            selector = doc["spec"]["matchResources"]["namespaceSelector"]["matchLabels"]
            self.assertEqual(selector["kubernetes.io/metadata.name"], NAMESPACE)

    def test_dry_run_manifest_matches_vap_contract(self) -> None:
        controller = load_controller()
        manifest = render_manifest(controller)
        spec = manifest["spec"]
        template = spec["template"]
        pod_spec = template["spec"]
        container = pod_spec["containers"][0]
        meta = manifest["metadata"]

        self.assertEqual(meta["namespace"], controller.DEFAULT_NAMESPACE)
        self.assertEqual(meta["name"], "query-regression-123456789-1")
        self.assertEqual(
            meta["labels"],
            {"app": "query-regression", "run-id": "123456789", "run-attempt": "1"},
        )
        self.assertEqual(
            meta["annotations"],
            {
                "greptimedb.io/query-regression-base-sha": "a" * 40,
                "greptimedb.io/query-regression-candidate-sha": "b" * 40,
            },
        )
        self.assertEqual(spec["backoffLimit"], 0)
        self.assertEqual(spec["completions"], 1)
        self.assertEqual(spec["parallelism"], 1)
        self.assertEqual(spec["activeDeadlineSeconds"], 10800)
        self.assertEqual(spec["ttlSecondsAfterFinished"], 600)
        self.assertEqual(spec["podReplacementPolicy"], "Failed")
        self.assertEqual(pod_spec["restartPolicy"], "Never")
        self.assertIs(pod_spec["automountServiceAccountToken"], False)
        self.assertEqual(pod_spec["serviceAccountName"], "query-regression-workload")
        self.assertEqual(pod_spec["nodeSelector"], {"alibabacloud.com/nodepool-id": "npb5ff93bea3a447a698fe31ebc997ea31"})
        self.assertEqual(
            pod_spec["tolerations"],
            [{"key": "dedicated", "operator": "Equal", "value": "perf-regression", "effect": "NoSchedule"}],
        )
        self.assertEqual(
            pod_spec["securityContext"],
            {
                "runAsNonRoot": True,
                "runAsUser": 1001,
                "runAsGroup": 1001,
                "fsGroup": 1001,
                "seccompProfile": {"type": "RuntimeDefault"},
            },
        )
        self.assertEqual(pod_spec["volumes"], [{"name": "payload", "emptyDir": {}}, {"name": "work", "emptyDir": {}}])
        self.assertEqual(container["name"], "benchmark")
        self.assertEqual(container["image"], controller.DEFAULT_IMAGE)
        self.assertEqual(container["command"], ["/bin/sh", "-c"])
        self.assertEqual(container["args"], [controller.BOOTSTRAP_SCRIPT])
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
        self.assertEqual(
            container["resources"],
            {
                "requests": {"cpu": "4", "memory": "12Gi", "ephemeral-storage": "20Gi"},
                "limits": {"cpu": "8", "memory": "16Gi", "ephemeral-storage": "40Gi"},
            },
        )
        self.assertEqual(
            container["volumeMounts"],
            [{"name": "payload", "mountPath": "/payload"}, {"name": "work", "mountPath": "/work"}],
        )
        # Scheduling bypass/control fields must be absent from the rendered
        # manifest (the VAP rejects them if a mutated Job ever carries them).
        for field in ("nodeName", "schedulerName", "priorityClassName", "priority", "affinity", "schedulingGates"):
            self.assertNotIn(field, pod_spec)
        self.assertNotIn("suspend", spec)
        # The rendered env must be exactly the 16 canonical entries: the VAP
        # count/uniqueness/allowlist/value checks and the manifest cannot
        # silently drift apart.
        from unittest import mock

        with mock.patch.dict(os.environ, dry_run_env("123456789"), clear=False):
            expected_env = {e["name"]: e["value"] for e in controller.passthrough_env()}
        rendered_env = {e["name"]: e["value"] for e in container["env"]}
        self.assertEqual(rendered_env, expected_env)
        self.assertEqual(len(rendered_env), 16)

    def test_server_side_apply_test_script_is_canary_gated(self) -> None:
        script = ACK_DIR / "apply-test.sh"
        self.assertTrue(script.is_file())
        text = script.read_text(encoding="utf-8")
        self.assertIn("dry-run=server", text)
        # The script must not claim CONNECT validation from dry-run: the real
        # disposable canary is the hard gate for exec/cp.
        self.assertIn("QR_APPLY_TEST_CANARY", text)
        self.assertIn("expressionWarnings", text)
        self.assertIn("query-regression-perf", text)

    def test_apply_test_canary_absence_is_fail_closed(self) -> None:
        """The canary preflight and cleanup must confirm Job absence fail-closed.

        Only `kubectl get job NAME --ignore-not-found -o name` with rc=0 and
        raw empty stdout proves the canary Job is gone; a failing get (403/
        transport/timeout), stderr pollution, or malformed stdout must map to
        unknown/2 and fail the script, never be read as "absent".
        """
        script = ACK_DIR / "apply-test.sh"
        text = script.read_text(encoding="utf-8")
        # The shared helper requires rc=0 and empty stdout and exits 2 on a
        # failing get; the canary preflight and cleanup both go through it.
        self.assertIn('timeout --kill-after=5s "${JOB_LOOKUP_LOCAL_TIMEOUT}" kubectl -n "${ns}" get job "${job}" --ignore-not-found -o name --request-timeout="${JOB_LOOKUP_REQUEST_TIMEOUT}" >"${out_file}" 2>"${err_file}"', text)
        self.assertIn('if [ "${rc}" -ne 0 ]', text)
        self.assertIn("return 2", text)
        self.assertIn("job_lookup_status \"${CANARY_NS}\" \"${CANARY_JOB}\"", text)
        # The old fail-open shape (a bare get whose non-zero exit is read as
        # "absent") must be gone from the canary path.
        self.assertNotIn('get job "${CANARY_JOB}" >/dev/null 2>&1; then', text)

    def test_apply_test_job_lookup_has_two_layer_timeout(self) -> None:
        """job_lookup_status must bound kubectl twice over: a fixed kubectl
        --request-timeout plus a local coreutils `timeout` backstop (with a
        --kill-after=5s grace) around the whole invocation. stdout and stderr
        are captured separately; every timeout (and any failing get, stderr
        pollution, or malformed stdout) maps to status 2 (unknown) -- a final
        failure, never absence -- and the canary cleanup confirmation retries
        the lookup a bounded number of times before failing closed."""
        script = ACK_DIR / "apply-test.sh"
        text = script.read_text(encoding="utf-8")
        # Layer 1: kubectl's own fixed request timeout on the exact get.
        self.assertIn('JOB_LOOKUP_REQUEST_TIMEOUT="30s"', text)
        self.assertIn('--request-timeout="${JOB_LOOKUP_REQUEST_TIMEOUT}"', text)
        # Layer 2: the local coreutils timeout backstop around the whole
        # invocation with a --kill-after=5s grace (a hang in kubectl or the
        # exec plugin cannot block the runner); both layers bound the same
        # exact get.
        self.assertIn("JOB_LOOKUP_LOCAL_TIMEOUT=45", text)
        self.assertIn('timeout --kill-after=5s "${JOB_LOOKUP_LOCAL_TIMEOUT}" kubectl -n "${ns}" get job "${job}"', text)
        # stdout and stderr are captured into separate files: any stderr at
        # all on a successful get -- judged on the RAW bytes of the capture
        # file ([ -s ]), whitespace-only/newline-only noise included -- makes
        # the result non-canonical (unknown/2); a command substitution would
        # strip a bare-newline stderr into "clean".
        self.assertIn('>"${out_file}" 2>"${err_file}"', text)
        self.assertIn('[ -s "${err_file}" ]', text)
        self.assertNotIn('if [ -n "${stderr_text}" ]', text)
        # A timed-out kubectl exits non-zero (timeout(1) -> 124, or 137 after
        # the kill grace), so the generic rc!=0 path maps every timeout to
        # unknown/2; absence is never inferred from it.
        self.assertIn('if [ "${rc}" -ne 0 ]', text)
        self.assertIn("return 2", text)
        # The canary cleanup confirmation (the final success conclusion)
        # retries the lookup a bounded number of times, but only a successful
        # empty get ever proves absence; a persistent unknown verdict is a
        # final failure.
        self.assertIn('while [ "${confirm_attempt}" -le 3 ]', text)
        self.assertIn("deletion NOT confirmed", text)

    def test_lookup_behavior_suite_runs_offline_with_path_stubs(self) -> None:
        """The fail-closed Job lookup and the canary cleanup confirmation must
        be proven BEHAVIORALLY with temporary PATH stubs -- real stub
        kubectl/timeout executables driving the real bash functions -- never
        by string assertions alone. Covers: 3 consecutive unknown lookups
        failing closed (never zeroed into a false "absent"), fail/fail/absent
        succeeding, present failing, and the exact canonical vs malformed
        stdout/stderr verdicts, including the --kill-after=5s / 45s two-layer
        timeout wrapping every lookup."""
        script = ACK_DIR / "apply-test.sh"
        env = dict(os.environ)
        env["QR_APPLY_TEST_LOOKUP_SELF"] = "1"
        proc = subprocess.run(
            ["bash", str(script)],
            capture_output=True,
            text=True,
            timeout=120,
            env=env,
        )
        # The exit code is the behavioral verdict: the suite runs the REAL
        # functions against stub binaries on a temporary PATH and fails on any
        # scenario that deviates from the fail-closed state machine.
        self.assertEqual(
            proc.returncode,
            0,
            f"lookup behavior self-test failed:\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}",
        )
        # Per-scenario verdicts, so a suite that passes for the wrong reason
        # (e.g. every scenario mapping to unknown) is still caught.
        for marker in (
            "lookup self-test: absent -> 0 (expected 0)",
            "lookup self-test: present -> 1 (expected 1)",
            "lookup self-test: present-newline -> 1 (expected 1)",
            "lookup self-test: whitespace-only -> 2 (expected 2)",
            "lookup self-test: padded -> 2 (expected 2)",
            "lookup self-test: wrong-name -> 2 (expected 2)",
            "lookup self-test: multiline -> 2 (expected 2)",
            "lookup self-test: double-newline -> 2 (expected 2)",
            "lookup self-test: stderr-pollution -> 2 (expected 2)",
            "lookup self-test: stderr-whitespace-only -> 2 (expected 2)",
            "lookup self-test: stderr-newline-only -> 2 (expected 2)",
            "lookup self-test: fail -> 2 (expected 2)",
            "cleanup confirmation self-test: fail -> 2 (expected 2)",
            "cleanup confirmation self-test: fail-twice-then-absent -> 0 (expected 0)",
            "cleanup confirmation self-test: present -> 1 (expected 1)",
            "all-fail scenario issued 3 lookups",
        ):
            self.assertIn(marker, proc.stdout, f"missing behavior marker: {marker}")
        self.assertIn("0 failed", proc.stdout)

    def test_apply_test_negative_mutations_are_self_tested_offline(self) -> None:
        """apply-test.sh negative Job mutations must be proven offline first.

        Every negative mutation must be a stdin->stdout JSON pipeline (manifest
        JSON in, mutated JSON out) driven by one shared table: the offline
        self-test (QR_APPLY_TEST_SELF=1, no kubectl/no cluster) proves each
        mutation emits valid JSON and changes the expected field, and the
        server-side probes feed kubectl create --dry-run=server through the
        very same pipeline. A bare `python3 -c <snippet>` (which could never
        see the manifest `m` and therefore never reach kubectl as valid JSON)
        is forbidden.
        """
        script = ACK_DIR / "apply-test.sh"
        text = script.read_text(encoding="utf-8")
        # The no-cluster mode is decided before kubectl is required.
        self.assertIn("QR_APPLY_TEST_SELF", text)
        self.assertIn("run_mutation_self_test", text)
        # The stdin->stdout wrapper shared by the self-test and the probes.
        self.assertIn("m = json.load(sys.stdin)", text)
        self.assertIn("json.dump(m, sys.stdout)", text)
        # The probes must go through the proven pipeline, not a bare snippet.
        self.assertIn('run_mutation "${py}"', text)
        self.assertNotIn('python3 -c "${py}"', text)
        # One mutation table drives both the self-test and the probes, so a
        # mutation can never be probed against the API server unproven.
        self.assertIn('for entry in "${MUTATIONS[@]}"', text)
        self.assertIn("IFS='|' read -r name py check <<<\"${entry}\"", text)


# ---------------------------------------------------------------------------
# Secure-split workflow structure (release gating + CI wiring)
# ---------------------------------------------------------------------------

WORKFLOWS_DIR = ACK_DIR.parents[2] / "workflows"
RELEASE_WORKFLOW = WORKFLOWS_DIR / "release.yml"
CONTROLLER_WORKFLOW = WORKFLOWS_DIR / "query-regression-controller.yml"
BUILD_WORKFLOW = WORKFLOWS_DIR / "query-regression.yml"
CHECKS_WORKFLOW = WORKFLOWS_DIR / "checks.yml"


def load_workflow(path: Path) -> dict:
    docs = load_all(path)
    data = docs[0]
    # GitHub Actions YAML uses `on:`; PyYAML (YAML 1.1) parses it as the
    # boolean True. Normalize so tests can use wf["on"].
    if isinstance(data, dict) and "on" not in data and True in data:
        data["on"] = data.pop(True)
    return data


@unittest.skipIf(yaml is None, "PyYAML not available")
class WorkflowStructureTest(unittest.TestCase):
    def test_release_needs_chain_wires_controller_after_build(self) -> None:
        """The release path must gate on the CONTROLLER, never the build alone.

        release.yml calls the build (query-regression.yml) and then the
        controller (query-regression-controller.yml) with the build's exact
        outputs as typed inputs; every downstream release job waits on the
        controller job result. A silent build-only gate is impossible.
        """
        wf = load_workflow(RELEASE_WORKFLOW)
        jobs = wf["jobs"]
        self.assertIn("query-regression-release", jobs)
        self.assertIn("query-regression-controller-release", jobs)

        build_job = jobs["query-regression-release"]
        self.assertEqual(
            build_job["uses"], "./.github/workflows/query-regression.yml",
            "the build job must call the unprivileged build workflow",
        )
        controller_job = jobs["query-regression-controller-release"]
        self.assertEqual(
            controller_job["uses"], "./.github/workflows/query-regression-controller.yml",
            "the controller job must call the trusted controller workflow",
        )
        # The controller runs only after the build succeeded and needs the
        # build's exact outputs (run id/attempt, SHAs, artifact ids) plus the
        # validation policy inputs.
        self.assertIn("query-regression-release", controller_job["needs"])
        self.assertIn("prepare-release-validation", controller_job["needs"])
        with_inputs = controller_job["with"]
        for input_name, output_name in (
            ("source_run_id", "run-id"),
            ("source_run_attempt", "run-attempt"),
            ("base_sha", "verified-base-sha"),
            ("candidate_sha", "verified-candidate-sha"),
            ("base_artifact_id", "base-artifact-id"),
            ("candidate_artifact_id", "candidate-artifact-id"),
        ):
            self.assertEqual(
                with_inputs[input_name],
                f"${{{{ needs.query-regression-release.outputs.{output_name} }}}}",
                f"controller input {input_name} must come from the build job output {output_name}",
            )
        self.assertEqual(with_inputs["case"], "all")

        # Every downstream job that previously gated on the build result now
        # gates on the controller result instead (never build alone).
        for gate_job in ("release-images-to-dockerhub", "publish-github-release"):
            job = jobs[gate_job]
            self.assertIn(
                "query-regression-controller-release", job["needs"],
                f"{gate_job} must wait for the controller job",
            )
            self.assertNotIn(
                "query-regression-release", job["needs"],
                f"{gate_job} must not gate on the build alone",
            )
            gate_condition = job["if"]
            self.assertIn("needs.query-regression-controller-release.result", gate_condition)
            self.assertNotIn("needs.query-regression-release.result", gate_condition)
            self.assertIn(
                "needs.prepare-release-validation.outputs.run-query-regression == 'false'",
                gate_condition,
                "the skip policy must still allow a skipped controller job",
            )

    def test_controller_workflow_call_path_is_typed_and_secret_free(self) -> None:
        wf = load_workflow(CONTROLLER_WORKFLOW)
        on = wf["on"]
        self.assertIn("workflow_call", on)
        inputs = on["workflow_call"]["inputs"]
        for name in (
            "source_run_id", "source_run_attempt", "base_sha", "candidate_sha",
            "base_artifact_id", "candidate_artifact_id", "case",
        ):
            self.assertIn(name, inputs)
            self.assertIn("type", inputs[name])
            self.assertTrue(inputs[name]["required"])
        # The controller must keep its privileged runner and the runner-local
        # kubeconfig path; no ACK secret is passed by the caller.
        job = wf["jobs"]["controller"]
        self.assertEqual(job["runs-on"], "query-regression-ack-controller")
        self.assertIn("ACK_KUBECONFIG_PATH", job["env"])
        self.assertNotIn("secrets", str(on["workflow_call"]))
        self.assertIn("github.workflow == 'Release'", job["if"])

    def test_checks_uv_setup_precedes_first_uv_use(self) -> None:
        """checks.yml must set up uv before any step runs `uv run`."""
        wf = load_workflow(CHECKS_WORKFLOW)
        for job in wf["jobs"].values():
            steps = job.get("steps") or []
            setup_index = None
            first_uv_index = None
            for i, step in enumerate(steps):
                uses = str(step.get("uses", ""))
                run = str(step.get("run", ""))
                if "setup-uv" in uses:
                    setup_index = i
                if first_uv_index is None and "uv run" in run:
                    first_uv_index = i
            if setup_index is not None and first_uv_index is not None:
                self.assertLess(
                    setup_index, first_uv_index,
                    f"job {job.get('name', '?')}: Setup uv must precede the first uv run",
                )

    def test_checks_runs_full_controller_test_suite(self) -> None:
        """checks.yml must run the full controller test suite in CI."""
        wf = load_workflow(CHECKS_WORKFLOW)
        all_steps = "".join(
            str(step) for job in wf["jobs"].values() for step in (job.get("steps") or [])
        )
        for marker in (
            "query-regression-ack-controller-test.py",
            "query-regression-admission.test.cjs",
            "query-regression-comment.test.cjs",
            "query-regression-policy-test.py",
        ):
            self.assertIn(marker, all_steps, f"missing CI suite runner for {marker}")

    def test_checks_runs_offline_ack_mutation_self_test_after_prereqs(self) -> None:
        """checks.yml must run the ACK mutation self-test offline, after setup.

        The offline self-test (QR_APPLY_TEST_SELF=1) proves every negative Job
        mutation emits valid JSON and changes the expected field with no
        kubectl and no cluster. CI must run that exact command after the
        prerequisites are installed (checkout + uv setup) and must never run
        the cluster-touching canary form (QR_APPLY_TEST_CANARY=1): the
        self-test is the only apply-test.sh invocation wired into checks.yml,
        so CI cannot contact a cluster through it.
        """
        wf = load_workflow(CHECKS_WORKFLOW)
        job = wf["jobs"]["github-script-tests"]
        steps = job["steps"]
        runs = [str(step.get("run", "")) for step in steps]
        uses = [str(step.get("uses", "")) for step in steps]
        self_test = "QR_APPLY_TEST_SELF=1 .github/runner-scale-sets/query-regression/ack/apply-test.sh"
        self.assertIn(
            self_test, runs,
            "checks.yml must run the offline ACK mutation self-test",
        )
        self_test_index = runs.index(self_test)
        # Prerequisites are installed before the self-test: checkout (the
        # script + controller must exist) and the uv setup must both precede
        # the exact command.
        checkout_index = next(i for i, u in enumerate(uses) if "actions/checkout" in u)
        setup_uv_index = next(i for i, u in enumerate(uses) if "setup-uv" in u)
        self.assertLess(
            checkout_index, self_test_index,
            "offline ACK mutation self-test must run after checkout",
        )
        self.assertLess(
            setup_uv_index, self_test_index,
            "offline ACK mutation self-test must run after the uv setup",
        )
        # CI can never contact a cluster through apply-test.sh: the canary
        # form (which creates real disposable resources) is not wired into
        # checks.yml.
        self.assertNotIn(
            "QR_APPLY_TEST_CANARY", "\n".join(runs),
            "checks.yml must never run the cluster-touching canary form",
        )


if __name__ == "__main__":
    unittest.main()
