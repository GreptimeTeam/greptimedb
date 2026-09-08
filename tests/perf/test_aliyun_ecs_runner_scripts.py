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
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path


SCRIPTS_DIR = Path(__file__).parents[2] / ".github/scripts"


def load_module(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS_DIR / filename)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


provision = load_module("aliyun_ecs_runner_provision_under_test", "aliyun-ecs-runner-provision.py")
teardown = load_module("aliyun_ecs_runner_teardown_under_test", "aliyun-ecs-runner-teardown.py")


class ProvisionNamingTest(unittest.TestCase):
    def test_runner_name_and_label_derive_from_run_id(self) -> None:
        self.assertEqual(provision.runner_name_for_run("12345"), "qreg-ecs-12345")
        self.assertEqual(provision.runner_label_for_run("12345"), "query-regression-ecs-12345")


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
            ("i-old", "qreg-ecs-1", "2026-08-17T01:00Z"),  # 5h old: expired
            ("i-edge", "qreg-ecs-2", "2026-08-17T02:00Z"),  # exactly TTL: expired
            ("i-fresh", "qreg-ecs-3", "2026-08-17T05:30Z"),  # 30m old: kept
        ]
        self.assertEqual(
            teardown.expired_instance_names(instances, self.NOW, self.TTL),
            [("i-old", "qreg-ecs-1"), ("i-edge", "qreg-ecs-2")],
        )


if __name__ == "__main__":
    unittest.main()
