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
import copy
import json
import tempfile
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
    def test_execution_must_not_expand_arms(self):
        command = ['runner', '--stage', 'target', '--root', '/run', '--owner', 'owner']
        with self.assertRaises(ValueError):
            storage.commands(command, 'both')

    def test_metrics_split_preserves_data_and_target_contract(self):
        plan = dict(output_root='/tmp/o11ybench-test', run_id='test',
                    data=dict(generate=True, data_home='/tmp/o11ybench-test/data'),
                    targets=[dict(name='greptimedb', storage=dict(type='file')),
                             dict(name='greptimedb-s3', storage=dict(type='s3', root='owned/prefix/'))],
                    report=dict(root='old', index_html='old/index.html'),
                    artifacts=dict(run_manifest='old/run-manifest.json'),
                    query=dict(modes=['serial'], serial_tries=5))
        original = copy.deepcopy(plan)
        children = storage.split_metrics(plan)
        self.assertEqual(plan, original)
        for name, child in children.items():
            self.assertEqual(child['query'], plan['query'])
            self.assertEqual(child['data'], dict(plan['data'], generate=False))
            self.assertEqual(len(child['targets']), 1)
            self.assertEqual(child['targets'][0]['name'], name)
            self.assertEqual(child['output_root'], '/tmp/o11ybench-test/o11ybench/o11ybench-' + name)
            self.assertTrue(child['report']['root'].startswith(child['output_root']))
        self.assertNotEqual(children['greptimedb']['run_id'], children['greptimedb-s3']['run_id'])

    def test_missing_s3_arm_is_not_a_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report = root / 'report.json'
            report.write_text(json.dumps(dict(workloads=[dict(participants=[
                dict(target='greptimedb', role='candidate', storage=dict(type='file'))], scenarios=[])])))
            with self.assertRaisesRegex(ValueError, 'participant mismatch'):
                storage.audit(root, report, ['greptimedb'], 'both')

    def test_s3_summary_is_explicit_when_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            text = storage.summary('agent', Path(tmp), ['greptimedb'], 'benchmark', 'both')
            self.assertIn('GreptimeDB File', text)
            self.assertIn('GreptimeDB S3', text)
            self.assertIn('S3 cleanup: **failed / unavailable**', text)

    def test_cleanup_both(self):
        command = ['runner', '--stage', 'cleanup', '--root', '/run', '--owner', 'owner']
        calls = storage.commands(command, 'both')
        self.assertEqual(len(calls), 2)
        self.assertTrue(all('--greptimedb-storage' not in c for c in calls))
    def test_cleanup_s3_only_still_visits_primary_competitors(self):
        command = ['runner', '--stage', 'cleanup', '--root', '/run', '--owner', 'owner']
        calls = storage.commands(command, 's3')
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[1][calls[1].index('--root') + 1], '/run/s3')

    def test_workflow_wiring(self):
        for name in ('agent-observability', 'tracesbench', 'vmbench-long-range'):
            text = (ROOT / '.github/workflows' / (name + '.yml')).read_text()
            self.assertIn('options: [file, s3, both]', text)
            self.assertIn('o11ybench-storage.py validate', text)
            self.assertIn('o11ybench-storage.py audit', text)
            self.assertIn('ci-summary.md', text)
            self.assertIn('AWS_SECRET_ACCESS_KEY:', text)
            self.assertNotIn('o11ybench-storage.py run -- bash o11ybench/scripts/run-agent-observability.sh --stage target', text)
            if name == 'vmbench-long-range':
                self.assertIn('name: Load and benchmark GreptimeDB File', text)
                self.assertIn('name: Load and benchmark GreptimeDB S3', text)
                self.assertIn('metrics-prepare', text)
                self.assertIn('metrics-validate', text)
            else:
                self.assertIn('--stage target --greptimedb-storage file', text)
                self.assertIn('--stage target --greptimedb-storage s3', text)


if __name__ == '__main__':
    unittest.main()
