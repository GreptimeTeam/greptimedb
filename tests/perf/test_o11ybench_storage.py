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

"""Offline contracts for File/S3 benchmark arm selection."""
import importlib.util
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('storage', ROOT / '.github/scripts/o11ybench-storage.py')
storage = importlib.util.module_from_spec(spec)
spec.loader.exec_module(storage)


class StorageTests(unittest.TestCase):
    def test_admission(self):
        self.assertEqual(storage.validate({}), 'file')
        env = dict(GREPTIMEDB_STORAGE='both', RUN_GREPTIMEDB='true', PROVIDER='AWS',
                   S3_BUCKET='test', S3_REGION='us-east-1', RUNNER_REGION='us-east-1')
        self.assertEqual(storage.validate(env), 'both')
        for key, value in [('PROVIDER', 'Aliyun'), ('RUNNER_REGION', 'us-west-2'),
                           ('S3_BUCKET', ''), ('RUN_GREPTIMEDB', 'false'), ('GREPTIMEDB_STORAGE', 'invalid')]:
            with self.assertRaises(ValueError):
                storage.validate(dict(env, **{key: value}))
    def test_target_arms_share_data(self):
        command = ['bash', 'runner', '--stage', 'target', '--root', '/run', '--owner', 'owner']
        primary, secondary = storage.commands(command, 'both')
        self.assertEqual(primary[-2:], ['--greptimedb-storage', 'file'])
        self.assertEqual(secondary[-2:], ['--greptimedb-storage', 's3'])
        self.assertEqual(secondary[secondary.index('--root') + 1], '/run/s3')
        self.assertEqual(secondary[secondary.index('--data-root') + 1], '/run')
        self.assertEqual(secondary[secondary.index('--owner') + 1], 'owner-s3')
        self.assertEqual(storage.commands(command, 's3')[0][-2:], ['--greptimedb-storage', 's3'])
    def test_baseline_and_report(self):
        command = ['python3', 'runner', '--stage', 'target', '--root', '/run/baseline',
                   '--owner', 'owner-base', '--data-root', '/run']
        _, secondary = storage.commands(command, 'both')
        self.assertEqual(secondary[secondary.index('--data-root') + 1], '/run')
        command = ['python3', 'runner', '--stage', 'report', '--root', '/run', '--owner', 'owner',
                   '--targets', 'greptimedb,tempo', '--baseline-run', '/run/baseline/runs/greptimedb/run.json']
        _, secondary = storage.commands(command, 'both')
        self.assertEqual(secondary[secondary.index('--targets') + 1], 'greptimedb')
        self.assertNotIn('--baseline-run', secondary)
    def test_cleanup_both(self):
        command = ['runner', '--stage', 'cleanup', '--root', '/run', '--owner', 'owner']
        calls = storage.commands(command, 'both')
        self.assertEqual(len(calls), 2)
        self.assertTrue(all('--greptimedb-storage' not in c for c in calls))
    def test_workflow_wiring(self):
        for name in ('agent-observability', 'tracesbench', 'vmbench-long-range'):
            text = (ROOT / '.github/workflows' / (name + '.yml')).read_text()
            self.assertIn('options: [file, s3, both]', text)
            self.assertIn('o11ybench-storage.py validate', text)
            self.assertIn('S3_REGION:', text)
            self.assertIn('AWS_SECRET_ACCESS_KEY:', text)
            if name == 'vmbench-long-range':
                self.assertIn('--greptimedb-storage "$GREPTIMEDB_STORAGE"', text)
                self.assertIn('-e S3_BUCKET -e S3_REGION', text)
            else:
                self.assertIn('o11ybench-storage.py run --', text)


if __name__ == '__main__':
    unittest.main()
