import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

SCRIPTS = Path(__file__).resolve().parents[2] / '.github/scripts'
spec = importlib.util.spec_from_file_location('comparison', SCRIPTS / 'benchmark-version-comparison.py')
comparison = importlib.util.module_from_spec(spec)
spec.loader.exec_module(comparison)


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


class VersionComparison(unittest.TestCase):
    def test_opt_in_validation(self):
        comparison.validate({})
        comparison.validate({'COMPARE_GREPTIMEDB': 'false', 'GREPTIMEDB_TAG': 'invalid\n'})
        good = dict(COMPARE_GREPTIMEDB='true', RUN_GREPTIMEDB='true',
                    BASELINE_GREPTIMEDB_TAG='v1.2.1', GREPTIMEDB_TAG='v1.3.0-beta.1')
        comparison.validate(good)
        for key, value in [('RUN_GREPTIMEDB', 'false'), ('BASELINE_GREPTIMEDB_TAG', ''),
                           ('BASELINE_GREPTIMEDB_TAG', 'v1.3.0-beta.1'),
                           ('BASELINE_GREPTIMEDB_TAG', 'a,image=other'), ('GREPTIMEDB_TAG', 'x\ny'),
                           ('COMPARE_GREPTIMEDB', 'yes')]:
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                comparison.validate(good | {key: value})

    def test_improvement_direction(self):
        self.assertAlmostEqual(comparison.improvement(100, 80), 20)
        self.assertAlmostEqual(comparison.improvement(100, 120), -20)
        self.assertAlmostEqual(comparison.improvement(100, 120, True), 20)
        for old, new in [(0, 3), (None, 4), (1, -1), (1, float('nan')), (True, 2)]:
            self.assertIsNone(comparison.improvement(old, new))

    def test_agent_result_gates(self):
        summary = dict(passed=True, contract_sha256='contract', data_profile='S', query_window=[1, 2],
                       warmup_runs=1, measured_runs=5, per_query=[dict(query_id='Q01', result_fingerprint='answer',
                       success=5, errors=0, timeouts=0, p50_ms=100, p95_ms=100, p99_ms=100)])
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            base = root / 'baseline/greptimedb-1/greptimedb-1/measured-summary.json'
            candidate = root / 'greptimedb-1/greptimedb-1/measured-summary.json'
            for r in (root / 'baseline', root):
                (r / 'greptimedb-1').mkdir(parents=True, exist_ok=True)
                (r / 'greptimedb-1/exit-code.txt').write_text('0\n')
            write(base, summary)
            changed = copy.deepcopy(summary); changed['per_query'][0]['p50_ms'] = 80
            write(candidate, changed)
            self.assertAlmostEqual(comparison.agent(root)[0]['improvement_percent'], 20)
            for key, value in [('errors', 1), ('timeouts', 1), ('result_fingerprint', 'wrong'), ('success', 0)]:
                broken = copy.deepcopy(changed); broken['per_query'][0][key] = value
                write(candidate, broken)
                self.assertTrue(all(r['improvement_percent'] is None for r in comparison.agent(root)))
            (root / 'greptimedb-1/exit-code.txt').write_text('1\n')
            write(candidate, changed)
            self.assertTrue(all(r['improvement_percent'] is None for r in comparison.agent(root)))
            (root / 'greptimedb-1/exit-code.txt').write_text('0\n')
            changed['query_window'] = [2, 3]; write(candidate, changed)
            self.assertTrue(all(r['improvement_percent'] is None for r in comparison.agent(root)))
            candidate.unlink()
            self.assertTrue(all(r['improvement_percent'] is None for r in comparison.agent(root)))

    def test_traces_uses_measured_only_and_rejects_failed_query(self):
        answer = dict(query_id='T01', outcome='passed', fingerprint='answer', row_count=1, elapsed_ms=100)
        sample = dict(answer, phase='measured')
        record = dict(success=True, expected={'contracts': {'T01': {'fingerprint': 'answer', 'row_count': 1}}},
                      protocol={'runs': 5}, audit_expected={}, corpus='sanity',
                      execution=dict(db_cpus='8', db_memory='16g', load_workers_requested=4, max_in_flight=8),
                      correctness=[answer], samples=[dict(sample, phase='warmup', elapsed_ms=99999), sample, sample])
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp);base = root / 'baseline/runs/greptimedb/run.json'; candidate = root / 'runs/greptimedb/run.json'
            write(base, record); changed = copy.deepcopy(record)
            for s in changed['samples']:
                if s['phase'] == 'measured': s['elapsed_ms'] = 80
            write(candidate, changed)
            self.assertAlmostEqual(comparison.traces(root)[0]['improvement_percent'], 20)
            self.assertIsNone(comparison.traces(root)[1]['improvement_percent'])
            for key, value in [('outcome', 'failed'), ('fingerprint', 'wrong'), ('row_count', 2)]:
                broken = copy.deepcopy(changed); broken['samples'][1][key] = value; write(candidate, broken)
                self.assertIsNone(comparison.traces(root)[0]['improvement_percent'])

    def test_long_range_gates(self):
        summary = dict(workload='vmbench-long-range', query_file_sha256='hash', start='start', end='end', step='30m',
                       profile='small', data_duration='32d', scrape_interval='30s', cache_policy_effective='warm',
                       mode='serial', connection_mode='force-close', request_timeout_seconds=120, tries=5, warmup_runs=1,
                       errors=0, timeouts=0, per_query=[dict(query_index=1, query='up', series=1, points=2,
                       success=5, errors=0, timeouts=0, p50_ms=100)])
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); base = root/'greptimedb-baseline/results-vmbench-long-range/serial-summary.json'
            candidate = root/'greptimedb/results-vmbench-long-range/serial-summary.json'
            health = {phase: {target: {'state': 'Running=true ExitCode=0 OOMKilled=false'}
                               for target in ('greptimedb-baseline', 'greptimedb')}
                      for phase in ('post_load_results', 'post_query_results')}
            health['post_load_results'] = {k: {'docker': v} for k, v in health['post_load_results'].items()}
            write(root / 'run-manifest.json', health)
            write(base, summary); write(candidate, summary)
            self.assertEqual(comparison.long_range(root)[0]['improvement_percent'], 0)
            for key, value in [('start','different'),('tries',1),('query_file_sha256','other'),('errors',1)]:
                write(candidate, summary | {key:value})
                self.assertIsNone(comparison.long_range(root)[0]['improvement_percent'])
            write(candidate, summary)
            broken=copy.deepcopy(health);broken['post_query_results']['greptimedb']['state']='Running=false ExitCode=137 OOMKilled=true'
            write(root / 'run-manifest.json', broken)
            self.assertIsNone(comparison.long_range(root)[0]['improvement_percent'])
            write(root / 'run-manifest.json', health)
            empty=copy.deepcopy(summary);empty['per_query'][0]['points']=0;write(candidate,empty)
            self.assertIsNone(comparison.long_range(root)[0]['improvement_percent'])

    def test_missing_results_and_rendering(self):
        with tempfile.TemporaryDirectory() as tmp:
            for load in (comparison.agent, comparison.traces, comparison.long_range):
                self.assertTrue(all(r['improvement_percent'] is None for r in load(Path(tmp))))
        rendered = comparison.render(dict(baseline_tag='old', candidate_tag='<new|tag>', rows=[]))
        self.assertIn('&lt;new&#124;tag&gt;', rendered)
        self.assertIn('Results unavailable', rendered)


if __name__ == '__main__':
    unittest.main()
