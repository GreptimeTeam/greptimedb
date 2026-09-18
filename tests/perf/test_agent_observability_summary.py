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

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).parents[2] / '.github/scripts/agent-observability-summary.py'
spec = importlib.util.spec_from_file_location('observability_summary', SCRIPT)
summary = importlib.util.module_from_spec(spec)
spec.loader.exec_module(summary)


class SummaryTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def write(self, path, data):
        path = self.root / path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data))
        return path

    def measured(self, target, value=10, **overrides):
        data = dict(passed=True, contract_sha256='same', data_profile={'id':'S'},
                    query_window={'start':0,'end':1}, warmup_runs=1, measured_runs=5,
                    per_query=[dict(query_id='Q01', p50_ms=value, p95_ms=value,
                                    p99_ms=value, result_fingerprint='same-result', success=5, errors=0, timeouts=0)])
        data.update(overrides)
        return self.write(f'{target}-1/{target}-1/measured-summary.json', data)

    def test_duration_units_and_invalid(self):
        for value, expected in [(0.25,'250 µs'), (12.3,'12.3 ms'), (1250,'1.25 s'),
                                (130000,'2m 10s'), (3661000,'1h 1m 1s'),
                                (None,'—'), (-1,'—'), (float('nan'),'—')]:
            with self.subTest(value=value):
                self.assertEqual(summary.duration(value), expected)

    def test_relative_latency_and_measurement_counts(self):
        self.measured('greptimedb')
        self.measured('clickhouse',20)
        self.measured('victorialogs',5)
        text = summary.benchmark(self.root, summary.TARGETS)
        self.assertIn('| Q01 | Scan / index | greptimedb | 10.0 ms | — |', text)
        self.assertEqual(text.count('+100.0%'),3)
        self.assertEqual(text.count('-50.0%'),3)
        self.assertEqual(text.count('| 5 | 0 | 0 |'),3)

    def test_missing_failed_and_incompatible_baseline(self):
        self.measured('clickhouse',20)
        for base in [None, {'passed':False}, {'data_profile':{'id':'M'}}]:
            if base is not None:
                self.measured('greptimedb', **base)
            text = summary.benchmark(self.root, summary.TARGETS)
            self.assertNotIn('+100.0%',text)
        self.assertEqual(summary.delta(10,0),'—')
        self.assertEqual(summary.delta(None,10),'—')

    def test_mismatched_results_have_no_comparison(self):
        self.measured('greptimedb')
        path = self.measured('clickhouse', 20)
        data = json.loads(path.read_text())
        data['per_query'][0]['result_fingerprint'] = 'different-result'
        path.write_text(json.dumps(data))
        self.assertNotIn('+100.0%', summary.benchmark(self.root, summary.TARGETS))

    def test_collapsed_explain_escaped_and_bounded(self):
        self.write('greptimedb-1/greptimedb-1/query-plans.json',
                   {'capability':{'state':'supported'}, 'queries':[
                       {'query_name':'Q01','state':'passed','explain_sql':'SELECT <x>',
                        'raw_response':'<script>'+'x'*10000}]})
        self.write('victorialogs-1/victorialogs-1/query-plans.json',
                   {'capability':{'state':'unsupported','reason':'No plan API'}})
        text=summary.benchmark(self.root, summary.TARGETS)
        self.assertIn('<details>',text)
        self.assertNotIn('<details open',text)
        self.assertNotIn('<script>',text)
        self.assertIn('&lt;script&gt;',text)
        self.assertIn('Truncated; see query-plans.json',text)
        self.assertIn('No plan API',text)
        self.assertLess(len(text.encode()),20000)

    def test_physical_and_post_load_evidence(self):
        self.write('corpus/size.json',{'raw_jsonl_bytes':2048})
        self.write('greptimedb-1/greptimedb-1/physical-evidence.json',
                   {'passed':True,'storage':{'engine_total_bytes':1024,'sst_files':1},
                    'materialization':{'state':'materialized'}})
        self.write('greptimedb-1/post-load-docker-stats.json',
                   {'CPUPerc':'1.2%','MemUsage':'20MiB / 4GiB'})
        text=summary.lifecycle(self.root,['greptimedb'])
        self.assertIn('2.00 KiB',text)
        self.assertIn('| greptimedb | 1.00 KiB | 2.00× | materialized | 1 | 1.2% | 20MiB / 4GiB |',text)
        self.assertIn('<summary>DDL',text)

    def test_legacy_timing_not_relabelled_and_missing_artifacts(self):
        path=self.root/'timings.jsonl'
        path.write_text(json.dumps(dict(phase='generate',started_at_ms=1789629388230252120,
                                       ended_at_ms=1789629396310721272,elapsed_ms=8080469248,exit_code=0))+'\n')
        text=summary.lifecycle(self.root, ['greptimedb'])
        self.assertIn('invalid timestamp units (legacy artifact)',text)
        self.assertNotIn('2244h',text)
        self.assertIn('unavailable',text)
        path.write_text(json.dumps(dict(phase='generate',started_at_ms=1789629388230,
                                       ended_at_ms=1789629396310,elapsed_ms=8080,exit_code=7))+'\n')
        self.assertIn('| generate | 8.08 s | 7 |',summary.lifecycle(self.root,['greptimedb']))


if __name__ == '__main__':
    unittest.main()
