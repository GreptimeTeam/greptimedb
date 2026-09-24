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

"""Offline regression coverage for CI runner selection and lifecycle."""

import importlib.util
import os
import re
import subprocess
import sys
import tempfile
import textwrap
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, patch

SCRIPTS = Path(__file__).parents[2] / ".github/scripts"
sys.path.insert(0, str(SCRIPTS))


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / filename)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


config = load("ci_config_test", "ci-runner-config.py")
aws = load("aws_provision_test", "aws-ec2-runner-provision.py")
teardown = load("aws_teardown_test", "aws-ec2-runner-teardown.py")


def provision_config(**overrides):
    return aws.ProvisionConfig(**({
        "region": "ap-southeast-1", "image_id": "ami-test", "instance_type": "c7i.2xlarge",
        "subnet_id": "subnet-test", "security_group_id": "sg-test", "repo": "owner/repo",
        "run_id": "123-2", "github_token": "secret", "system_disk_gib": 80,
    } | overrides))


def client_with_image(size=40, arch="x86_64"):
    client = Mock()
    client.describe_images.return_value = {"Images": [{
        "State": "available", "Architecture": arch, "RootDeviceName": "/dev/sda1",
        "BlockDeviceMappings": [{"DeviceName": "/dev/sda1", "Ebs": {"VolumeSize": size}}],
    }]}
    client.describe_instance_types.return_value = {"InstanceTypes": [{
        "ProcessorInfo": {"SupportedArchitectures": ["x86_64"]},
    }]}
    client.run_instances.return_value = {"Instances": [{"InstanceId": "i-test"}]}
    client.get_console_output.return_value = {"Output": "boot log"}
    return client


class ProviderConfigTest(unittest.TestCase):
    def test_defaults_and_overrides(self):
        for provider, requested, expected in (
            ("Aliyun", "auto", "ecs.c9i.2xlarge"), ("AWS", "auto", "c7i.2xlarge"),
            ("Aliyun", "ecs.c9i.4xlarge", "ecs.c9i.4xlarge"),
            ("AWS", "c6i.4xlarge", "c6i.4xlarge"),
            ("AWS", "c7i.metal-24xl", "c7i.metal-24xl"),
        ):
            with self.subTest(provider=provider, requested=requested):
                self.assertEqual(config.resolve_instance_type(provider, requested), expected)

    def test_wrong_provider_and_shell_fragments_rejected(self):
        for provider, requested in (("ec2", "auto"), ("AWS", "ecs.c9i.2xlarge"),
                                    ("Aliyun", "c7i.2xlarge"), ("AWS", ""),
                                    ("AWS", "c7i.2xlarge; touch /tmp/unexpected")):
            with self.subTest(provider=provider, requested=requested), self.assertRaises(ValueError):
                config.resolve_instance_type(provider, requested)

    def test_cli_writes_resolved_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "output"
            result = subprocess.run([sys.executable, str(SCRIPTS / "ci-runner-config.py"),
                                     "--provider", "AWS"],
                                    env=os.environ | {"GITHUB_OUTPUT": str(output)},
                                    capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(output.read_text(), "instance_type=c7i.2xlarge\n")



class BenchmarkSetupTest(unittest.TestCase):
    def run_setup(self, missing_tools="", docker_installed=True, docker_ready=True):
        action = (SCRIPTS.parent / "actions/setup-benchmark/action.yml").read_text()
        steps = re.findall(r"      run: \|\n((?:        .*\n|\n)+)", action)
        self.assertEqual(len(steps), 2)
        script = "\n".join(textwrap.dedent(step) for step in steps)
        # Execute the actual action twice with host commands stubbed: installation
        # changes availability, so the second pass must perform no mutations.
        prelude = r"""
function . { ID=ubuntu; VERSION_CODENAME=noble; }
uname() { echo x86_64; }
dpkg() { echo amd64; }
curl() { echo fixture-key; }
command() {
  if [[ "$1" == -v ]]; then
    if [[ "$2" == docker ]]; then [[ "$DOCKER_INSTALLED" == true ]]; return; fi
    if [[ " $MISSING_TOOLS " == *" $2 "* ]]; then return 1; fi
    return 0
  fi
  builtin command "$@"
}
docker() { [[ "$DOCKER_READY" == true ]]; }
sudo() {
  printf '%s\n' "$*" >> "$CALLS"
  if [[ "$*" == *' apt-get install '* ]]; then
    MISSING_TOOLS=''
    [[ "$*" != *' docker-ce '* ]] || DOCKER_INSTALLED=true
  fi
  if [[ "$*" == *'systemctl enable --now docker' ]]; then DOCKER_READY=true; fi
  if [[ "$*" == *' tee '* ]]; then while IFS= read -r line; do :; done; fi
  return 0
}
"""
        with tempfile.TemporaryDirectory() as tmp:
            calls = Path(tmp) / "calls"
            calls.touch()
            env = os.environ | {"CALLS": str(calls), "MISSING_TOOLS": missing_tools,
                                "DOCKER_INSTALLED": str(docker_installed).lower(),
                                "DOCKER_READY": str(docker_ready).lower()}
            result = subprocess.run(["bash", "-c", prelude + script +
                                     '\nprintf "SECOND PASS\\n" >> "$CALLS"\n' + script],
                                    env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            first, second = calls.read_text().split("SECOND PASS\n")
            self.assertEqual(second, "", "Second setup must reuse installed dependencies")
            return first

    def test_prepared_runner_skips_all_privileged_changes(self):
        self.assertEqual(self.run_setup(), "")

    def test_missing_tools_are_installed_without_changing_docker(self):
        calls = self.run_setup(missing_tools="jq rsync")
        self.assertIn("--no-install-recommends jq rsync", calls)
        self.assertIn("NEEDRESTART_MODE=l", calls)
        self.assertNotIn("docker", calls)

    def test_missing_docker_is_installed_once(self):
        calls = self.run_setup(docker_installed=False, docker_ready=False)
        self.assertIn("NEEDRESTART_MODE=l", calls)
        self.assertIn("docker-ce docker-ce-cli containerd.io docker-buildx-plugin", calls)
        self.assertIn("systemctl enable --now docker", calls)

    def test_existing_stopped_docker_is_started_without_installation(self):
        self.assertEqual(self.run_setup(docker_ready=False), "-n systemctl enable --now docker\n")


class AwsProvisionTest(unittest.TestCase):
    def test_host_policy_matches_aliyun_before_runner_activation(self):
        aliyun = load("aliyun_host_policy_test", "aliyun-ecs-runner-provision.py")
        ecs_script = aliyun.render_user_data("name", "label", "token", "owner/repo")
        aws_script = aws.render_user_data("name", "token", "owner/repo", "https://example.com/runner.tgz")
        start = ecs_script.index("# Mask systemd-oomd")
        end = ecs_script.index("free --human", start) + len("free --human")
        policy = ecs_script[start:end]
        self.assertIn(policy, aws_script)
        self.assertLess(aws_script.index(policy), aws_script.index("./svc.sh start"))
        self.assertLess(aws_script.index("OOMPolicy=continue"), aws_script.index("./svc.sh start"))
        self.assertLess(aws_script.index("./svc.sh install runner"), aws_script.index("service_name=$(cat .service)"))
        self.assertIn('"/etc/systemd/system/$service_name.d/oom.conf"', aws_script)

    def test_ubuntu_image_resolution_and_override(self):
        client = Mock()
        client.describe_images.return_value = {"Images": [
            {"ImageId": "ami-new", "CreationDate": "2026-09-04", "Name": "noble-new"},
            {"ImageId": "ami-old", "CreationDate": "2026-08-04", "Name": "noble-old"},
        ]}
        self.assertEqual(aws.resolve_image(client, "auto"), "ami-new")
        request = client.describe_images.call_args.kwargs
        self.assertEqual(request["Owners"], ["099720109477"])
        self.assertIn({"Name": "name", "Values": [
            "ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"]}, request["Filters"])
        client.reset_mock()
        self.assertEqual(aws.resolve_image(client, "ami-pinned"), "ami-pinned")
        client.describe_images.assert_not_called()
        client.describe_images.return_value = {"Images": []}
        with self.assertRaisesRegex(ValueError, "No Canonical Ubuntu"):
            aws.resolve_image(client, "auto")

    def test_disk_size_and_architecture_fail_before_allocation(self):
        for client, pattern in ((client_with_image(size=500), "smaller than AMI"),
                                (client_with_image(arch="arm64"), "x86_64")):
            with self.subTest(pattern=pattern), patch.object(aws, "create_registration_token") as token:
                with self.assertRaisesRegex(ValueError, pattern):
                    aws.run_instance(client, provision_config())
                client.run_instances.assert_not_called()
                token.assert_not_called()

    def test_arm_instance_rejected_before_registration(self):
        client = client_with_image()
        client.describe_instance_types.return_value["InstanceTypes"][0]["ProcessorInfo"]["SupportedArchitectures"] = ["arm64"]
        with patch.object(aws, "create_registration_token") as token:
            with self.assertRaisesRegex(ValueError, "x86_64"):
                aws.run_instance(client, provision_config(instance_type="r8g.2xlarge"))
            token.assert_not_called()
        client.run_instances.assert_not_called()

    def test_create_request_and_recovery_tags(self):
        client = client_with_image()
        with patch.object(aws, "create_registration_token", return_value="short-lived-token"), \
             patch.object(aws, "runner_download", return_value="https://example.com/runner.tar.gz"):
            self.assertEqual(aws.run_instance(client, provision_config(system_disk_gib=100)), "i-test")
        request = client.run_instances.call_args.kwargs
        self.assertEqual(request["BlockDeviceMappings"], [{"DeviceName": "/dev/sda1", "Ebs": {
            "VolumeSize": 100, "VolumeType": "gp3", "Iops": 3000, "Throughput": 125,
            "DeleteOnTermination": True}}])
        self.assertEqual(request["ClientToken"], "ci-ec2-123-2")
        self.assertNotIn("InstanceMarketOptions", request)
        self.assertNotIn("IamInstanceProfile", request)
        for specification in request["TagSpecifications"]:
            tags = {t["Key"]: t["Value"] for t in specification["Tags"]}
            self.assertEqual(tags["managed-by"], "query-regression-ci")
            self.assertEqual(tags[aws.REPO_TAG], "owner/repo")
            self.assertEqual(tags["query-regression-run-id"], "123-2")
            self.assertEqual(tags["runner-ttl-hours"], "8")
        self.assertNotIn("secret", request["UserData"])
        self.assertIn("--token short-lived-token", request["UserData"])
        self.assertNotIn("apt-get install", request["UserData"])
        self.assertIn('for tool in git curl tar sudo; do command -v "$tool"; done', request["UserData"])
        self.assertLess(request["UserData"].index("usermod -aG docker runner"),
                        request["UserData"].index("./svc.sh start"))
        self.assertIn("visudo -cf /etc/sudoers.d/ci-runner", request["UserData"])
        self.assertLess(request["UserData"].index("$nrconf{override_rc}"),
                        request["UserData"].index("./svc.sh start"))
        check = subprocess.run(["bash", "-n"], input=request["UserData"], text=True, capture_output=True)
        self.assertEqual(check.returncode, 0, check.stderr)

    def test_partial_provision_keeps_outputs_and_console(self):
        client = client_with_image()
        client.get_waiter.return_value.wait.side_effect = TimeoutError("not running")
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "output"
            with patch.dict(os.environ, {"GITHUB_OUTPUT": str(output)}), patch.object(aws, "run_instance", return_value="i-test"):
                with self.assertRaises(TimeoutError):
                    aws.provision(client, provision_config())
            self.assertEqual(output.read_text().splitlines(), [
                "instance_id=i-test", "label=ci-ec2-123-2",
                "runner_name=ci-ec2-123-2"])
        client.get_console_output.assert_called_once()

    def test_online_poll_and_timeout(self):
        for online in (True, False):
            with self.subTest(online=online), patch.object(aws, "run_instance", return_value="i-test"), \
                 patch.object(aws, "find_runner_by_name", side_effect=[None, {"status": "online"}]), \
                 patch.object(aws.time, "sleep"), \
                 patch.object(aws.time, "monotonic", side_effect=[0, 1, 2 if online else 601]):
                client = client_with_image()
                if online:
                    self.assertEqual(aws.provision(client, provision_config()), 0)
                else:
                    with self.assertRaises(TimeoutError):
                        aws.provision(client, provision_config())
                client.get_console_output.assert_called_once()


class AwsTeardownTest(unittest.TestCase):
    NOW = datetime(2026, 9, 23, 10, tzinfo=timezone.utc)

    def setUp(self):
        sleeper = patch.object(teardown.time, "sleep")
        sleeper.start()
        self.addCleanup(sleeper.stop)

    def instance(self, identity="i-test", age=9, ttl="8"):
        tags = [{"Key": teardown.RUNNER_TAG, "Value": "ci-ec2-123-2"}]
        if ttl is not None:
            tags.append({"Key": teardown.TTL_TAG_KEY, "Value": ttl})
        return {"InstanceId": identity, "LaunchTime": self.NOW - timedelta(hours=age), "Tags": tags}

    def test_pagination_and_ownership_filters(self):
        client = Mock()
        client.get_paginator.return_value.paginate.return_value = [
            {"Reservations": [{"Instances": [self.instance("i-a")]}]},
            {"Reservations": [{"Instances": [self.instance("i-b")]}]},
        ]
        self.assertEqual([i["InstanceId"] for i in teardown.managed_instances(client, "owner/repo", "123-2")], ["i-a", "i-b"])
        self.assertEqual(client.get_paginator.return_value.paginate.call_args.kwargs["Filters"], [
            {"Name": "tag:managed-by", "Values": ["query-regression-ci"]},
            {"Name": "tag:github-repository", "Values": ["owner/repo"]},
            {"Name": "tag:query-regression-run-id", "Values": ["123-2"]},
        ])

    def test_sweep_expiry_and_missing_or_invalid_ttl(self):
        instances = [self.instance("expired", age=8), self.instance("fresh", age=7),
                     self.instance("invalid", ttl="bad"),
                     self.instance("missing-expired", age=4, ttl=None),
                     self.instance("missing-fresh", age=3, ttl=None)]
        with patch.object(teardown, "managed_instances", return_value=instances), \
             patch.object(teardown, "delete_instance", return_value=True) as delete, \
             patch.object(teardown, "deregister_runner", return_value=True) as unregister:
            self.assertEqual(teardown.teardown(Mock(), "owner/repo", "token", now=self.NOW), 0)
            self.assertEqual([c.args[1] for c in delete.call_args_list], ["expired", "missing-expired"])
            self.assertEqual(unregister.call_count, 2)

    def test_ttl_decisions_match_aliyun(self):
        aliyun = load("aliyun_ttl_policy_test", "aliyun-ecs-runner-teardown.py")
        for age, ttl in [(3, None), (4, None), (7, "8"), (8, "8"), (9, "8"),
                         (10, "bad"), (10, ""), (10, "0"), (200, "169"), (168, "168")]:
            instance = self.instance(age=age, ttl=ttl)
            created = instance["LaunchTime"].strftime("%Y-%m-%dT%H:%M:%SZ")
            expected = aliyun.expired_instance_names(
                [(instance["InstanceId"], "runner", created, ttl)], self.NOW, timedelta(hours=4))
            with self.subTest(age=age, ttl=ttl), \
                 patch.object(teardown, "managed_instances", return_value=[instance]), \
                 patch.object(teardown, "delete_instance", return_value=True) as delete, \
                 patch.object(teardown, "deregister_runner", return_value=True):
                self.assertEqual(teardown.teardown(Mock(), "owner/repo", "token", now=self.NOW), 0)
                self.assertEqual([call.args[1] for call in delete.call_args_list],
                                 [identity for identity, _ in expected])

    def test_targeted_cleanup_recovers_missing_outputs_without_waiting_for_ttl(self):
        with patch.object(teardown, "managed_instances", return_value=[self.instance(age=0)]), \
             patch.object(teardown, "delete_instance", return_value=True) as delete, \
             patch.object(teardown, "deregister_runner", return_value=True) as unregister:
            self.assertEqual(teardown.teardown(Mock(), "owner/repo", "token", run_id="123-2"), 0)
            delete.assert_called_once()
            unregister.assert_called_once_with("token", "owner/repo", "ci-ec2-123-2")

    def test_independent_cloud_and_github_cleanup_failures(self):
        for cloud_ok, github_result in ((False, True), (True, RuntimeError("GitHub unavailable"))):
            with self.subTest(cloud_ok=cloud_ok), \
                 patch.object(teardown, "managed_instances", return_value=[self.instance()]), \
                 patch.object(teardown, "delete_instance", return_value=cloud_ok) as delete, \
                 patch.object(teardown, "deregister_runner", side_effect=[github_result]) as unregister:
                self.assertEqual(teardown.teardown(Mock(), "owner/repo", "token", run_id="123-2"), 1)
                delete.assert_called_once()
                unregister.assert_called_once()

    def test_already_gone_instance_still_unregisters(self):
        with patch.object(teardown, "managed_instances", return_value=[]), \
             patch.object(teardown, "deregister_runner", return_value=True) as unregister:
            self.assertEqual(teardown.teardown(Mock(), "owner/repo", "token", run_id="123-2"), 0)
            unregister.assert_called_once()

    def test_retried_teardown_uses_original_instance_and_runner(self):
        client = Mock()
        client.get_paginator.return_value.paginate.return_value = [
            {"Reservations": [{"Instances": [self.instance()]}]},
        ]
        with patch.object(teardown, "delete_instance", return_value=True) as delete, \
             patch.object(teardown, "deregister_runner", return_value=True) as unregister:
            self.assertEqual(teardown.teardown(client, "owner/repo", "token", run_id="123-3",
                                         instance_id="i-test", runner_name="ci-ec2-123-2"), 0)
            filters = client.get_paginator.return_value.paginate.call_args.kwargs["Filters"]
            self.assertIn({"Name": "instance-id", "Values": ["i-test"]}, filters)
            self.assertFalse(any(f["Name"] == "tag:query-regression-run-id" for f in filters))
            delete.assert_called_once_with(client, "i-test")
            unregister.assert_called_once_with("token", "owner/repo", "ci-ec2-123-2")
        client.get_paginator.return_value.paginate.return_value = []
        with patch.object(teardown, "deregister_runner", return_value=True) as unregister:
            self.assertEqual(teardown.teardown(client, "owner/repo", "token", run_id="123-3",
                                         instance_id="i-test", runner_name="ci-ec2-123-2"), 0)
            unregister.assert_called_once_with("token", "owner/repo", "ci-ec2-123-2")

    def test_terminate_waits_and_handles_not_found(self):
        client = client_with_image()
        self.assertTrue(teardown.delete_instance(client, "i-test"))
        client.get_waiter.assert_called_with("instance_terminated")
        error = RuntimeError("gone")
        error.response = {"Error": {"Code": "InvalidInstanceID.NotFound"}}
        client.terminate_instances.side_effect = error
        with patch.object(teardown.time, "sleep"):
            self.assertTrue(teardown.delete_instance(client, "i-test"))

    def test_transient_not_found_is_retried(self):
        client = client_with_image()
        error = RuntimeError("not visible yet")
        error.response = {"Error": {"Code": "InvalidInstanceID.NotFound"}}
        client.terminate_instances.side_effect = [error, {}]
        self.assertTrue(teardown.delete_instance(client, "i-test"))
        self.assertEqual(client.terminate_instances.call_count, 2)

    def test_missing_tags_are_polled_before_cleanup(self):
        with patch.object(teardown, "managed_instances", side_effect=[[], [self.instance()]]), \
             patch.object(teardown, "delete_instance", return_value=True) as delete, \
             patch.object(teardown, "deregister_runner", return_value=True):
            self.assertEqual(teardown.teardown(Mock(), "owner/repo", "token", run_id="123-2"), 0)
            delete.assert_called_once()

    def test_github_system_exit_does_not_skip_other_instances(self):
        with patch.object(teardown, "managed_instances", return_value=[self.instance("i-a"), self.instance("i-b")]), \
             patch.object(teardown, "delete_instance", return_value=True) as delete, \
             patch.object(teardown, "deregister_runner", side_effect=[SystemExit("HTTP 403"), True]):
            self.assertEqual(teardown.teardown(Mock(), "owner/repo", "token", run_id="123-2"), 1)
            self.assertEqual([c.args[1] for c in delete.call_args_list], ["i-a", "i-b"])


if __name__ == "__main__":
    unittest.main()
