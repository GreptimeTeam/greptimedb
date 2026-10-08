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

"""Coverage for the pure parts of the Aliyun ECS runner provision/teardown scripts."""

import base64
import importlib.util
import io
import os
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

SCRIPTS_DIR = Path(__file__).parents[2] / ".github/scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import runner_utils


def load_module(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS_DIR / filename)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


provision = load_module(
    "aliyun_ecs_runner_provision_under_test", "aliyun-ecs-runner-provision.py"
)
teardown = load_module(
    "aliyun_ecs_runner_teardown_under_test", "aliyun-ecs-runner-teardown.py"
)


class GitHubApiResponseTest(unittest.TestCase):
    def test_success_response_body(self):
        cases = ((200, b'{"runners": []}', {"runners": []}), (204, b"", {}))
        for status, body, expected in cases:
            with self.subTest(status=status):
                response = Mock(status=status)
                response.read.return_value = body
                response.__enter__ = Mock(return_value=response)
                response.__exit__ = Mock(return_value=False)
                with patch.object(runner_utils.urllib.request, "urlopen", return_value=response):
                    self.assertEqual(provision.github_api("token", "DELETE", "/test"), expected)

    def test_invalid_json_still_fails(self):
        response = Mock(status=200)
        response.read.return_value = b"not json"
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        with patch.object(runner_utils.urllib.request, "urlopen", return_value=response):
            with self.assertRaises(ValueError):
                provision.github_api("token", "GET", "/test")

    def test_http_errors_still_exit_instead_of_becoming_recoverable(self):
        error = runner_utils.urllib.error.HTTPError(
            "https://api.github.com/test", 403, "Forbidden", {}, io.BytesIO(b"denied")
        )
        with patch.object(runner_utils.urllib.request, "urlopen", side_effect=error):
            with self.assertRaisesRegex(SystemExit, "HTTP 403: denied"):
                provision.github_api("token", "GET", "/test")

    def test_unregister_preserves_legacy_failure_semantics(self):
        with patch.object(runner_utils, "find_runner_by_name", return_value={"id": 42}):
            with patch.object(runner_utils, "github_api", side_effect=SystemExit("HTTP 403")):
                with self.assertRaisesRegex(SystemExit, "HTTP 403"):
                    runner_utils.deregister_runner("token", "owner/repo", "runner")
            with patch.object(runner_utils, "github_api", side_effect=RuntimeError("connection lost")):
                self.assertFalse(runner_utils.deregister_runner("token", "owner/repo", "runner"))


class ProvisionNamingTest(unittest.TestCase):
    def test_runner_name_and_label_derive_from_run_id(self) -> None:
        self.assertEqual(provision.runner_name_for_run("12345"), "qreg-ecs-12345")
        self.assertEqual(provision.runner_label_for_run("12345"), "query-regression-ecs-12345")


class ProvisionResourcesTest(unittest.TestCase):
    @staticmethod
    def config(**overrides):
        values = {
            "region_id": "region", "vswitch_id": "vswitch", "security_group_id": "sg",
            "image_id": "image", "instance_type": "ecs.u1-c1m1.2xlarge", "repo": "owner/repo",
            "run_id": "12345", "github_token": "token",
        }
        return provision.ProvisionConfig(**(values | overrides))

    def test_sdk_request_defaults_and_overrides(self):
        models = SimpleNamespace(
            RunInstancesRequest=SimpleNamespace,
            RunInstancesRequestSystemDisk=SimpleNamespace,
            RunInstancesRequestTag=SimpleNamespace,
        )
        for overrides, disk, ttl in (
            ({}, "80", None),
            ({"system_disk_gib": 500, "ttl_hours": 8}, "500", "8"),
        ):
            with (
                self.subTest(overrides=overrides),
                patch.dict(
                    sys.modules,
                    {"alibabacloud_ecs20140526": SimpleNamespace(models=models)},
                ),
            ):
                client = Mock()
                client.run_instances.return_value = SimpleNamespace(
                    body=SimpleNamespace(
                        instance_id_sets=SimpleNamespace(instance_id_set=["i-test"])
                    )
                )
                self.assertEqual(
                    provision.run_instance(
                        client, self.config(**overrides), "userdata"
                    ),
                    "i-test",
                )
                request = client.run_instances.call_args.args[0]
                self.assertEqual(request.system_disk.size, disk)
                self.assertEqual(request.system_disk.category, "cloud_essd")
                self.assertEqual(request.instance_type, "ecs.u1-c1m1.2xlarge")
                tags = {tag.key: tag.value for tag in request.tag}
                expected = {
                    provision.MANAGED_BY_TAG_KEY: provision.MANAGED_BY_TAG_VALUE,
                    provision.RUN_TAG_KEY: "12345",
                }
                if ttl is not None:
                    expected[provision.TTL_TAG_KEY] = ttl
                self.assertEqual(tags, expected)

    def test_invalid_resource_bounds(self):
        for overrides in (
            {"system_disk_gib": 19},
            {"system_disk_gib": 2049},
            {"ttl_hours": 0},
            {"ttl_hours": 169},
        ):
            with self.subTest(overrides=overrides), self.assertRaises(ValueError):
                self.config(**overrides)
        for disk in (20, 2048):
            self.assertEqual(self.config(system_disk_gib=disk).system_disk_gib, disk)
        for ttl in (1, 168):
            self.assertEqual(self.config(ttl_hours=ttl).ttl_hours, ttl)

    def test_cli_environment_defaults_and_overrides(self):
        args = [
            "provision",
            "--region-id",
            "region",
            "--vswitch-id",
            "vswitch",
            "--security-group-id",
            "sg",
            "--image-id",
            "image",
            "--instance-type",
            "ecs.u1-c1m1.2xlarge",
            "--repo",
            "owner/repo",
            "--run-id",
            "12345",
            "--github-token",
            "token",
        ]
        cases = [
            ({}, [], 80, None, False),
            (
                {"ALIYUN_ECS_SYSTEM_DISK_GIB": "500", "ALIYUN_ECS_TTL_HOURS": "8"},
                [],
                500,
                8,
                False,
            ),
            (
                {"ALIYUN_ECS_SYSTEM_DISK_GIB": "500", "ALIYUN_ECS_TTL_HOURS": "8"},
                ["--system-disk-gib", "600", "--ttl-hours", "12"],
                600,
                12,
                False,
            ),
            ({"ALIYUN_ECS_TTL_HOURS": ""}, [], 80, None, False),
            ({"ALIYUN_ECS_ENABLE_DOCKER": "true"}, [], 80, None, True),
            ({"ALIYUN_ECS_ENABLE_DOCKER": "true"}, ["--enable-docker", "false"], 80, None, False),
            ({}, ["--enable-docker", "true"], 80, None, True),
        ]
        for env, extra, disk, ttl, docker in cases:
            with (
                self.subTest(env=env, extra=extra),
                patch.dict(os.environ, env, clear=True),
                patch.object(sys, "argv", args + extra),
                patch.object(provision, "provision", return_value=0) as run,
            ):
                self.assertEqual(provision.main(), 0)
                config = run.call_args.args[0]
                self.assertEqual(
                    (config.system_disk_gib, config.ttl_hours, config.enable_docker), (disk, ttl, docker)
                )


class ProvisionUserDataTest(unittest.TestCase):
    def render(self) -> str:
        return provision.render_user_data(
            runner_name="qreg-ecs-12345",
            runner_label="query-regression-ecs-12345",
            runner_token="TOKEN",
            repo="GreptimeTeam/greptimedb",
        )

    def test_user_data_creates_cache_paths_on_the_system_disk(self) -> None:
        script = self.render()
        self.assertNotIn("DISK_SERIAL", script)
        self.assertNotIn("mount --bind", script)
        self.assertNotIn("mkfs.ext4", script)
        for destination in provision.CACHE_PATHS:
            self.assertIn(f'"{destination}"', script)

    def test_user_data_wires_runner_registration(self) -> None:
        script = self.render()
        self.assertIn("RUNNER_NAME=qreg-ecs-12345", script)
        self.assertIn("RUNNER_LABELS=query-regression-ecs-12345", script)
        self.assertIn("RUNNER_TOKEN=TOKEN", script)
        self.assertIn("REPO_URL=https://github.com/GreptimeTeam/greptimedb", script)
        self.assertIn("PATH=/opt/cargo/bin:", script)
        self.assertIn("systemctl restart --no-block ephemeral-github-runner.service", script)

    def test_docker_setup_is_opt_in_and_precedes_runner(self):
        args = ("runner-name", "label", "token", "owner/repo")
        default = provision.render_user_data(*args)
        self.assertEqual(default, provision.render_user_data(*args, enable_docker=False))
        self.assertNotIn("systemctl start docker", default)
        self.assertNotIn("usermod", default)
        enabled = provision.render_user_data(*args, runner_uid="2001", enable_docker=True)
        start = enabled.index("# Reuse Docker CE")
        end = enabled.index("cat > /etc/ephemeral-github-runner.env")
        setup = enabled[start:end]
        self.assertEqual(enabled[:start] + enabled[end:],
                         provision.render_user_data(*args, runner_uid="2001"))
        self.assertLess(end, enabled.index("systemctl restart --no-block ephemeral-github-runner.service"))
        for forbidden in ("apt-get", "sudo", "setfacl"):
            self.assertNotIn(forbidden, setup)
        mocks = '''
docker() { :; }
jq() { :; }
systemctl() { echo "systemctl $*"; return "$FAIL_START"; }
id() { [[ "$*" == "-nu 2001" ]] || return 1; echo custom-runner; }
usermod() { echo "usermod $*"; }
runuser() { echo "runuser $*"; }
'''
        for fail in ("0", "1"):
            with self.subTest(fail_start=fail):
                result = subprocess.run(["bash", "-euc", mocks + setup],
                                        env=os.environ | {"FAIL_START": fail},
                                        capture_output=True, text=True)
                self.assertEqual(result.returncode, int(fail), result.stderr)
                calls = result.stdout.splitlines()
                self.assertEqual(calls, ["docker", "jq", "systemctl start docker"] + (
                    ["usermod -aG docker custom-runner", "runuser -u custom-runner -- docker info"]
                    if fail == "0" else []))

    def test_host_cache_clear_is_opt_in_and_command_scoped(self) -> None:
        args = ("runner", "label", "token", "owner/repo")
        self.assertNotIn("o11ybench-cache-clear", provision.render_user_data(*args))
        enabled = provision.render_user_data(*args, runner_uid="2001", enable_host_cache_clear=True)
        setup = enabled[enabled.index("# Opt-in host-wide"):enabled.index("cat > /etc/ephemeral-github-runner.env")]
        self.assertIn("#2001 ALL=(root) NOPASSWD: %s /proc/sys/vm/drop_caches", setup)
        self.assertNotIn("NOPASSWD: ALL", setup)
        self.assertIn("visudo -cf", setup)
        self.assertLess(enabled.index("visudo -cf"), enabled.index("systemctl restart --no-block ephemeral-github-runner.service"))
        with self.assertRaisesRegex(ValueError, "runner-uid must be numeric"):
            provision.render_user_data(*args, runner_uid="ALL", enable_host_cache_clear=True)
        with tempfile.TemporaryDirectory() as tmp:
            # Run the actual setup in an isolated path, mocking only privileged tools.
            script = setup.replace("/etc/sudoers.d", tmp)
            mocks = '''
sudo() { :; }
visudo() { echo "visudo $*"; return "$FAIL_VALIDATE"; }
id() { [[ "$*" == "-nu 2001" ]] || return 1; echo custom-runner; }
runuser() { echo "runuser $*"; }
'''
            for fail in ("0", "1"):
                result = subprocess.run(["bash", "-euc", mocks + script],
                    env=os.environ | {"FAIL_VALIDATE": fail}, capture_output=True, text=True)
                self.assertEqual(result.returncode, int(fail), result.stderr)
                policy = Path(tmp) / "o11ybench-cache-clear"
                self.assertEqual(policy.read_text(), "#2001 ALL=(root) NOPASSWD: /usr/bin/tee /proc/sys/vm/drop_caches\n")
                self.assertEqual(policy.stat().st_mode & 0o777, 0o440)
                self.assertEqual('runuser -u custom-runner -- sudo -n -l -- tee /proc/sys/vm/drop_caches' in result.stdout, fail == "0")
                policy.chmod(0o600)

    def test_encode_user_data_round_trips(self) -> None:
        script = self.render()
        self.assertEqual(
            base64.b64decode(provision.encode_user_data(script)).decode("utf-8"), script
        )

    def test_user_data_enables_swap_and_masks_oomd(self) -> None:
        script = self.render()
        self.assertIn("systemctl mask systemd-oomd.socket systemd-oomd.service", script)
        self.assertIn("systemctl mask unattended-upgrades.service apt-daily.timer apt-daily-upgrade.timer", script)
        self.assertIn('APT::Periodic::Unattended-Upgrade "0"', script)
        self.assertIn(f'fallocate --length {provision.SWAP_SIZE_GIB}G "{provision.SWAP_FILE}"', script)
        self.assertIn(f'swapon "{provision.SWAP_FILE}"', script)
        self.assertIn("sysctl --write vm.swappiness=10", script)
        self.assertIn("OOMPolicy=continue", script)
        self.assertNotIn("OOMScoreAdjust", script)
        self.assertLess(script.index("swapon"), script.index("systemctl restart --no-block ephemeral-github-runner.service"))


class TeardownExpiryTest(unittest.TestCase):
    NOW = datetime(2026, 8, 17, 6, 0, tzinfo=timezone.utc)
    TTL = timedelta(hours=4)

    def test_parse_creation_time_formats(self) -> None:
        self.assertEqual(
            teardown.parse_creation_time("2026-08-17T01:02:03Z"),
            datetime(2026, 8, 17, 1, 2, 3, tzinfo=timezone.utc),
        )
        self.assertEqual(
            teardown.parse_creation_time("2026-08-17T01:02Z"),
            datetime(2026, 8, 17, 1, 2, tzinfo=timezone.utc),
        )
        with self.assertRaises(ValueError):
            teardown.parse_creation_time("not-a-time")

    def test_expired_instance_names_selects_only_old_instances(self) -> None:
        instances = [
            ("i-old", "qreg-ecs-1", "2026-08-17T01:00Z", None),  # 5h old: expired
            ("i-edge", "qreg-ecs-2", "2026-08-17T02:00Z", None),  # exactly TTL: expired
            ("i-fresh", "qreg-ecs-3", "2026-08-17T05:30Z", None),  # 30m old: kept
        ]
        self.assertEqual(
            teardown.expired_instance_names(instances, self.NOW, self.TTL),
            [("i-old", "qreg-ecs-1"), ("i-edge", "qreg-ecs-2")],
        )

    def test_tagged_ttl_overrides_fallback_and_preserves_legacy(self):
        instances = [
            ("i-legacy", "legacy", "2026-08-17T01:00Z", None),
            ("i-long", "long", "2026-08-17T01:00Z", "8"),
            ("i-short", "short", "2026-08-17T03:00Z", "2"),
            ("i-edge", "edge", "2026-08-16T22:00Z", "8"),
            ("i-not-yet", "not-yet", "2026-08-16T22:00:01Z", "8"),
        ]
        self.assertEqual(
            teardown.expired_instance_names(instances, self.NOW, self.TTL),
            [("i-legacy", "legacy"), ("i-short", "short"), ("i-edge", "edge")],
        )

    def test_malformed_ttl_never_falls_back_to_earlier_deletion(self):
        for ttl in ("", "bad", "-1", "0", "169", "8.5", "nan"):
            with self.subTest(ttl=ttl), redirect_stderr(io.StringIO()) as logs:
                self.assertEqual(
                    teardown.expired_instance_names(
                        [("i-live", "live", "2026-08-16T01:00Z", ttl)],
                        self.NOW,
                        self.TTL,
                    ),
                    [],
                )
                self.assertIn("Skipping i-live", logs.getvalue())

    def test_list_instances_preserves_tags_and_pagination(self):
        models = SimpleNamespace(
            DescribeInstancesRequest=SimpleNamespace,
            DescribeInstancesRequestTag=SimpleNamespace,
        )
        client = Mock()

        def response(instance, token):
            return SimpleNamespace(
                body=SimpleNamespace(
                    instances=SimpleNamespace(instance=[instance]), next_token=token
                )
            )

        client.describe_instances.side_effect = [
            response(
                SimpleNamespace(
                    instance_id="i-1",
                    instance_name="one",
                    creation_time="2026-08-17T01:00Z",
                    tags=SimpleNamespace(
                        tag=[
                            SimpleNamespace(
                                tag_key=provision.TTL_TAG_KEY, tag_value="8"
                            )
                        ]
                    ),
                ),
                "next",
            ),
            response(
                SimpleNamespace(
                    instance_id="i-2",
                    instance_name="two",
                    creation_time="2026-08-17T02:00Z",
                    tags=None,
                ),
                None,
            ),
        ]
        with patch.dict(
            sys.modules, {"alibabacloud_ecs20140526": SimpleNamespace(models=models)}
        ):
            self.assertEqual(
                teardown.list_managed_instances(client, "region"),
                [
                    ("i-1", "one", "2026-08-17T01:00Z", "8"),
                    ("i-2", "two", "2026-08-17T02:00Z", None),
                ],
            )
        self.assertEqual(
            client.describe_instances.call_args_list[1].args[0].next_token, "next"
        )
        tag = client.describe_instances.call_args_list[0].args[0].tag[0]
        self.assertEqual(
            (tag.key, tag.value),
            (provision.MANAGED_BY_TAG_KEY, provision.MANAGED_BY_TAG_VALUE),
        )


if __name__ == "__main__":
    unittest.main()
