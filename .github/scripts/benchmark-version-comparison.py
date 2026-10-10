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

"""Compare validated GreptimeDB query results; never gate CI on noisy latency deltas."""
import argparse
import html
import json
import math
import os
from pathlib import Path
import re
import statistics


def read(path):
    return json.loads(path.read_text()) if path.is_file() else {}


def validate(env):
    enabled = env.get('COMPARE_GREPTIMEDB', 'false')
    if enabled not in ('true', 'false'):
        raise ValueError('COMPARE_GREPTIMEDB must be true or false')
    if enabled == 'false':
        return
    if env.get('RUN_GREPTIMEDB') != 'true':
        raise ValueError('Version comparison requires Run GreptimeDB')
    for key in ('BASELINE_GREPTIMEDB_TAG', 'GREPTIMEDB_TAG'):
        if not re.fullmatch(r'[a-zA-Z0-9_][a-zA-Z0-9_.-]{0,127}(@sha256:[0-9a-f]{64})?', env.get(key, '')):
            raise ValueError(f'Invalid {key}')
    if env['BASELINE_GREPTIMEDB_TAG'] == env['GREPTIMEDB_TAG']:
        raise ValueError('Baseline and candidate must have different image tags')


def same(a, b, fields):
    return all(a.get(k) is not None and a.get(k) == b.get(k) for k in fields)


def finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value >= 0


def improvement(old, new, throughput=False):
    if not finite(old) or not finite(new) or old <= 0:
        return None
    return (new / old - 1 if throughput else 1 - new / old) * 100


def row(query, metric, old, new, compatible, reason):
    delta = improvement(old, new, metric == 'QPS') if compatible else None
    return dict(query=query, metric=metric, baseline=old, candidate=new,
                improvement_percent=delta, status='comparable' if delta is not None else reason)


def agent(root):
    def load(r):
        return read(r / 'greptimedb-1/greptimedb-1/measured-summary.json')
    a, b = load(root / 'baseline'), load(root)
    finished = [r / 'greptimedb-1/exit-code.txt' for r in (root / 'baseline', root)]
    compatible = (all(p.is_file() and p.read_text().strip() == '0' for p in finished)
                  and a.get('passed') is True and b.get('passed') is True
                  and same(a, b, ('contract_sha256', 'data_profile', 'query_window', 'warmup_runs', 'measured_runs')))
    qa, qb = ({q['query_id']: q for q in x.get('per_query', [])} for x in (a, b))
    rows = []
    for q in sorted(qa.keys() | qb.keys()):
        old, new = qa.get(q, {}), qb.get(q, {})
        ok = compatible and same(old, new, ('result_fingerprint', 'success')) and old.get('success', 0) > 0
        ok = ok and all(v.get('errors') == 0 and v.get('timeouts') == 0 for v in (old, new))
        for key in ('p50_ms', 'p95_ms', 'p99_ms'):
            rows.append(row(q, key, old.get(key), new.get(key), ok, 'failed, missing or incompatible result'))
    return rows


def traces(root):
    a, b = (read(r / 'runs/greptimedb/run.json') for r in (root / 'baseline', root))
    compatible = (a.get('success') is True and b.get('success') is True
                  and same(a, b, ('expected', 'protocol', 'audit_expected', 'corpus'))
                  and same(a.get('execution', {}), b.get('execution', {}),
                           ('db_cpus', 'db_memory', 'load_workers_requested', 'max_in_flight')))
    rows = []
    for q in (f'T{i:02}' for i in range(1, 9)):
        values, identities, counts = [], [], []
        ok = compatible
        for run in (a, b):
            correctness = [s for s in run.get('correctness', []) if s.get('query_id') == q]
            all_samples = [s for s in run.get('samples', []) if s.get('query_id') == q]
            measured = [s for s in all_samples if s.get('phase') == 'measured']
            passed = bool(correctness and measured) and all(s.get('outcome') == 'passed' for s in correctness + all_samples)
            expected = run.get('expected', {}).get('contracts', {}).get(q, {})
            passed = passed and all(s.get('fingerprint') == expected.get('fingerprint') and s.get('fingerprint') is not None
                                    and s.get('row_count') == expected.get('row_count') for s in correctness + all_samples)
            elapsed = [s.get('elapsed_ms') for s in measured]
            passed = passed and all(finite(v) for v in elapsed)
            ok = ok and passed
            values.append(statistics.median(elapsed) if passed else None)
            identities.append(expected)
            counts.append((len(correctness), len(all_samples), len(measured)))
        ok = ok and identities[0] == identities[1] and counts[0] == counts[1]
        rows.append(row(q, 'median_ms', *values, ok, 'failed, skipped, missing or incompatible result'))
    return rows


def long_range(root):
    rows = []
    manifest = read(root / 'run-manifest.json')
    healthy = all(
        all(field in (manifest.get(phase, {}).get(target, {}).get('docker', {}) if phase == 'post_load_results'
                      else manifest.get(phase, {}).get(target, {})).get('state', '').split()
            for field in ('Running=true', 'ExitCode=0', 'OOMKilled=false'))
        for phase in ('post_load_results', 'post_query_results')
        for target in ('greptimedb-baseline', 'greptimedb'))
    for mode in ('serial', 'concurrent-duration-c1', 'concurrent-duration-c4'):
        a, b = (read(root / name / 'results-vmbench-long-range' / (mode + '-summary.json'))
                for name in ('greptimedb-baseline', 'greptimedb'))
        fields = ('workload', 'query_file_sha256', 'start', 'end', 'step', 'profile', 'data_duration',
                  'scrape_interval', 'cache_policy_effective', 'mode', 'connection_mode', 'request_timeout_seconds')
        fields += ('tries', 'warmup_runs') if mode == 'serial' else ('concurrency', 'duration_seconds', 'warmup_seconds')
        compatible = healthy and same(a, b, fields) and all(x.get('errors') == 0 and x.get('timeouts') == 0 for x in (a, b))
        qa, qb = ({q['query_index']: q for q in x.get('per_query', [])} for x in (a, b))
        for q in sorted(qa.keys() | qb.keys()):
            old, new = qa.get(q, {}), qb.get(q, {})
            ok = compatible and same(old, new, ('query', 'series', 'points'))
            ok = ok and all(v.get('success', 0) > 0 and v.get('series', 0) > 0 and v.get('points', 0) > 0
                            and v.get('errors') == 0 and v.get('timeouts') == 0 for v in (old, new))
            for key in ('p50_ms', 'qps') if mode != 'serial' else ('p50_ms',):
                rows.append(row(f'{mode}/Q{q}', 'QPS' if key == 'qps' else key,
                                old.get(key), new.get(key), ok, 'failed, missing or incompatible result'))
    return rows


def render(document):
    def cell(value):
        return html.escape(str(value)).replace('|', '&#124;').replace('\n', ' ')
    def value(v, metric):
        if not finite(v):
            return '—'
        if metric == 'QPS':
            return f'{v:.3f}'
        return f'{v / 1000:.2f} s' if v >= 1000 else f'{v:.2f} ms'
    lines = ['# GreptimeDB version comparison', '',
             f'Baseline: `{cell(document["baseline_tag"])}` → candidate: `{cell(document["candidate_tag"])}`.', '',
             'Same-host sequential runs with fresh databases and shared input data. Positive = improvement; '
             'latency: (baseline − candidate) / baseline; QPS: (candidate − baseline) / baseline. '
             'Single runs are diagnostic, not a statistically established regression. No performance threshold gates CI.', '',
             '| Query | Metric | Baseline | Candidate | Improvement | Status |',
             '| --- | --- | --- | --- | --- | --- |']
    for r in document['rows']:
        delta = r['improvement_percent']
        lines.append('| ' + ' | '.join(map(cell, [r['query'], r['metric'], value(r['baseline'], r['metric']),
                     value(r['candidate'], r['metric']), f'{delta:+.1f}%' if delta is not None else '—', r['status']])) + ' |')
    if document.get('kind') == 'long-range':
        lines += ['', 'Long-range retains the existing non-empty series/point-count checks; these do not establish equality of every returned value.']
    if not document['rows']:
        lines += ['', 'Results unavailable; comparison not performed.']
    return '\n'.join(lines) + '\n'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('validate', 'report'))
    parser.add_argument('--kind', choices=('agent', 'traces', 'long-range'))
    parser.add_argument('--root', type=Path)
    args = parser.parse_args()
    if args.command == 'validate':
        validate(os.environ)
        return
    if args.root is None or args.kind is None:
        parser.error('report requires --root and --kind')
    document = {'kind': args.kind, 'baseline_tag': os.environ.get('BASELINE_GREPTIMEDB_TAG', ''),
                'candidate_tag': os.environ.get('GREPTIMEDB_TAG', ''),
                'rows': {'agent': agent, 'traces': traces, 'long-range': long_range}[args.kind](args.root)}
    text = render(document)
    # Preserve partial comparisons even if a target failed before creating output.
    args.root.mkdir(parents=True, exist_ok=True)
    (args.root / 'version-comparison.json').write_text(json.dumps(document, indent=2, allow_nan=False) + '\n')
    (args.root / 'version-comparison.md').write_text(text)
    print(text, end='')


if __name__ == '__main__':
    main()
