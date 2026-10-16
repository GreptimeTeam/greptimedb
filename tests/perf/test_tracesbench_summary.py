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
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).parents[2] / '.github/scripts/tracesbench-summary.py'
sys.path.insert(0, str(SCRIPT.parent))
spec = importlib.util.spec_from_file_location('tracesbench_summary', SCRIPT)
summary = importlib.util.module_from_spec(spec)
spec.loader.exec_module(summary)


class LifecycleTest(unittest.TestCase):
    def test_ingestion_layouts(self):
        metrics = {'wall_seconds': 0.121723417, 'successful_requests': 17, 'failed_requests': 0}
        cases = [
            (target, {'ingest': metrics}, ['121.7 ms', '17', '0'])
            for target in summary.TARGETS
        ] + [
            ('tempo', {'transport': {'ingest': metrics}}, ['121.7 ms', '17', '0']),
            ('tempo', {'ingest': {'wall_seconds': 0, 'successful_requests': 0, 'failed_requests': 0},
                       'transport': {'ingest': metrics}}, ['0 µs', '0', '0']),
            ('tempo', {'ingest': {}, 'transport': {'ingest': metrics}}, ['—', '—', '—']),
            ('tempo', {'transport': {}}, ['—', '—', '—']),
        ] + [(target, {}, ['—', '—', '—']) for target in summary.TARGETS] + [
            (target, {'transport': {'ingest': metrics}}, ['—', '—', '—'])
            for target in ('greptimedb', 'victoriatraces')
        ]
        with tempfile.TemporaryDirectory() as tmp:
            for target, load, expected in cases:
                with self.subTest(target=target, load=load):
                    text = summary.lifecycle({}, [{'target': target, 'load': load}], Path(tmp), [target])
                    row = next(line for line in text.splitlines() if line.startswith('| ' + target + ' |'))
                    cells = [cell.strip() for cell in row.split('|')[1:-1]]
                    self.assertEqual(cells[4:7], expected)


if __name__ == '__main__':
    unittest.main()
