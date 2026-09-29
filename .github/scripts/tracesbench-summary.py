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

import argparse
import json
import html
import math
from pathlib import Path

from benchmark_resource_summary import render_resources

TARGETS = ('greptimedb', 'victoriatraces', 'tempo')


def duration(ms):
    if not isinstance(ms, (int, float)) or not math.isfinite(ms) or ms < 0:
        return '—'
    if ms < 1:
        return f'{ms * 1000:.0f} µs'
    if ms < 1000:
        return f'{ms:.1f} ms'
    if ms < 60_000:
        return f'{ms / 1000:.2f} s'
    minutes, seconds = divmod(round(ms / 1000), 60)
    hours, minutes = divmod(minutes, 60)
    return f'{hours}h {minutes}m {seconds}s' if hours else f'{minutes}m {seconds}s'


def cell(value):
    return html.escape(str(value if value is not None else '—')).replace('|', '&#124;').replace('\n', ' ')


def read(path):
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def table(headers, rows):
    return '\n'.join(['| ' + ' | '.join(map(cell, headers)) + ' |',
                      '| ' + ' | '.join('---' for _ in headers) + ' |'] +
                     ['| ' + ' | '.join(map(cell, row)) + ' |' for row in rows])


def delta(value, baseline):
    if not all(isinstance(v, (int, float)) and math.isfinite(v) for v in (value, baseline)):
        return '—'
    return f'{(value / baseline - 1) * 100:+.1f}%' if baseline > 0 and value >= 0 else '—'



def load(root):
    report = read(root / 'report.json')
    runs = [read(p) for p in sorted((root / 'runs').glob('*/run.json'))]
    return report, runs


def benchmark(report, runs):
    lines = ['# Tracesbench benchmark summary', '',
             'Median Δ = (target / GreptimeDB − 1) × 100%; negative is faster. '
             'Min/median/max come from the validated report, not recalculated samples. '
             'Counts exclude warmup. Missing or excluded results have no latency comparison.',
             '', f'Classification: {cell(report.get("classification"))}. '
             'See the uploaded HTML for query semantics and client-side compensation.', '']
    statistics = {}
    for corpus in report.get('per_corpus', []):
        for stat in corpus.get('statistics', []):
            statistics[(corpus['corpus'], stat['round'], stat['target'], stat['query_id'])] = stat
    rows, details = [], []
    for target in TARGETS:
        selected = [run for run in runs if run.get('target') == target]
        if not selected:
            lines.append(f'- **{target}**: unavailable / not run.')
        for run in selected:
            lines.append(f'- **{target}**: lifecycle '
                         f'{"completed" if run.get("success") is True else "failed / incomplete"}; '
                         f'query failures: {len(run.get("query_failures", []))}.')
            for qid in (f'T{i:02}' for i in range(1, 9)):
                correct = [q for q in run.get('correctness', []) if q.get('query_id') == qid]
                measured = [q for q in run.get('samples', [])
                            if q.get('query_id') == qid and q.get('phase') == 'measured']
                errors = [q for q in correct + run.get('samples', [])
                          if q.get('query_id') == qid and q.get('outcome') not in ('passed', 'not_executed')]
                reason = (run.get('exclusions', {}).get(qid) or
                          run.get('execution', {}).get('query_skips', {}).get(qid))
                stat = statistics.get((run.get('corpus'), run.get('round'), target, qid), {})
                base = statistics.get((run.get('corpus'), run.get('round'), 'greptimedb', qid), {})
                status = ('failed' if errors else 'excluded' if reason else
                          'passed' if stat else
                          'correctness passed; report unavailable' if correct and
                          all(q.get('outcome') == 'passed' for q in correct) else 'unavailable / incomplete')
                rows.append([run.get('corpus'), run.get('round'), qid, target, status,
                             duration(stat.get('min_ms')), duration(stat.get('median_ms')),
                             duration(stat.get('max_ms')),
                             delta(stat.get('median_ms'), base.get('median_ms')) if target != 'greptimedb' else '—',
                             sum(q.get('outcome') == 'passed' for q in measured),
                             sum(q.get('outcome') != 'passed' for q in measured)])
                if reason or errors:
                    messages = [str(q.get('error') or q.get('outcome')) for q in errors]
                    details.append(f'- **{target} / {qid}**: {cell(str(reason or "") + " " + "; ".join(messages))[:4000]}')
    lines += ['', table(['Dataset', 'Round', 'Query', 'DB', 'Status', 'Min', 'Median', 'Max',
                         'Median Δ', 'Measured success', 'Measured errors'], rows), '',
              '<details>', '<summary>Query failures and exclusions</summary>', '', *details, '', '</details>']
    if not report:
        lines += ['', 'Validated report unavailable; raw outcomes are shown without latency statistics.']
    return '\n'.join(lines)


def lifecycle(report, runs, root):
    rows = []
    for target in TARGETS:
        selected = [r for r in runs if r.get('target') == target] or [{}]
        for run in selected:
            load = run.get('load', {})
            ingest = load.get('ingest', {})
            health = run.get('post_load_health', {})
            execution = run.get('execution', {})
            start, finish = run.get('started_unix_ns'), run.get('finished_unix_ns')
            elapsed = (finish - start) / 1_000_000 if isinstance(start, int) and isinstance(finish, int) else None
            seconds = ingest.get('wall_seconds')
            rows.append([target, run.get('corpus'), run.get('load_summary', {}).get('after'),
                         duration(elapsed), duration(seconds * 1000 if isinstance(seconds, (int, float)) else None),
                         ingest.get('successful_requests'), ingest.get('failed_requests'),
                         execution.get('load_workers_requested'), execution.get('max_in_flight'),
                         execution.get('db_cpus'), execution.get('db_memory'),
                         health.get('running'), health.get('oom_killed'),
                         run.get('cleanup', {}).get('verified_absent')])
    return '\n'.join(['# Tracesbench load / lifecycle summary', '',
                       'Total is the recorded target lifecycle; ingest is the sender wall time, '
                       'not the entire load/readiness phase. Health is the post-load snapshot. '
                       'Missing evidence remains blank.', '',
                       table(['DB', 'Dataset', 'Loaded spans', 'Total', 'Ingest',
                              'Write success', 'Write errors', 'Load workers', 'Max in flight',
                              'DB CPU', 'DB memory', 'Running after load', 'OOM after load',
                              'Container removed'], rows), '',
                       render_resources([('dataset', root / 'runs' / 'generate')] +
                                        [(t, root / 'runs' / t) for t in TARGETS], table)])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--section', choices=('benchmark', 'lifecycle'), required=True)
    args = parser.parse_args()
    report, runs = load(args.root)
    print(benchmark(report, runs) if args.section == 'benchmark' else lifecycle(report, runs, args.root))


if __name__ == '__main__':
    main()
