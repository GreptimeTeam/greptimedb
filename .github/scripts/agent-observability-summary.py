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
import html
import json
import math
from pathlib import Path

TARGETS = ('greptimedb', 'clickhouse', 'victorialogs')


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


def size(value):
    if not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
        return '—'
    for unit in ('B', 'KiB', 'MiB', 'GiB', 'TiB'):
        if value < 1024 or unit == 'TiB':
            return f'{value:.2f} {unit}'
        value /= 1024


def category(qid):
    if qid in ('Q01', 'Q03', 'Q04'):
        return 'Scan / index'
    if qid in ('Q05', 'Q08', 'Q11', 'Q14', 'Q15'):
        return 'Keyword search'
    if qid in ('Q02', 'Q06', 'Q09', 'Q10', 'Q13', 'Q18', 'Q19', 'Q20'):
        return 'Aggregation'
    return '—'


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


def result_dir(root, target):
    return root / f'{target}-1' / f'{target}-1'


def benchmark(root, targets):
    summaries = {t: read(result_dir(root, t) / 'measured-summary.json') for t in targets}
    base = summaries.get('greptimedb', {})
    base_queries = {q['query_id']: q for q in base.get('per_query', [])}
    ids = sorted({q['query_id'] for s in summaries.values() for q in s.get('per_query', [])})
    lines = ['# Benchmark summary', '',
             'Latency Δ = (target / GreptimeDB − 1) × 100%; positive is slower, negative is faster. '
             'Missing, failed or incompatible results have no comparison. Counts exclude warmup.', '',
             'Percentiles from small smoke samples are diagnostic, not stable performance estimates.', '']
    for t, s in summaries.items():
        lines.append(f'- **{t}**: {"passed" if s.get("passed") is True else "failed / unavailable"}; '
                     f'warmup/query: {cell(s.get("warmup_runs"))}; measured/query: {cell(s.get("measured_runs"))}.')
    rows = []
    for qid in ids:
        for t, s in summaries.items():
            q = next((q for q in s.get('per_query', []) if q['query_id'] == qid), {})
            compatible = (t != 'greptimedb' and s.get('passed') is True and base.get('passed') is True
                          and q.get('result_fingerprint') is not None
                          and q.get('result_fingerprint') == base_queries.get(qid, {}).get('result_fingerprint')
                          and all(s.get(k) == base.get(k) and s.get(k) is not None
                                  for k in ('contract_sha256', 'data_profile', 'query_window')))
            row = [qid, category(qid), t]
            for key in ('p50_ms', 'p95_ms', 'p99_ms'):
                row += [duration(q.get(key)), delta(q.get(key), base_queries.get(qid, {}).get(key)) if compatible else '—']
            rows.append(row + [q.get('success'), q.get('errors'), q.get('timeouts')])
    lines += ['', table(['Query', 'Category', 'DB', 'P50', 'Δ', 'P95', 'Δ', 'P99', 'Δ', 'Success', 'Errors', 'Timeouts'], rows)]
    comparison = read(root / 'comparison.json')
    lines += ['', 'Cross-target comparison: ' + ('passed' if comparison.get('passed') is True else
                                               'failed' if comparison else 'not available / not run'), '']
    lines += ['<details>', '<summary>EXPLAIN diagnostics (after measurement)</summary>', '']
    for t in targets:
        plans = read(result_dir(root, t) / 'query-plans.json')
        if not plans:
            lines += [f'**{t}**: unavailable.', '']
            continue
        capability = plans.get('capability', {})
        if capability.get('state') == 'unsupported':
            lines += [f'**{t}**: {cell(capability.get("reason"))}', '']
            continue
        for q in plans.get('queries', []):
            # Bound summary size; complete responses remain in the uploaded JSON.
            raw = str(q.get('raw_response') or q.get('error') or '')
            limit = 3000
            excerpt = raw[:limit] + ('\n[Truncated; see query-plans.json artifact.]' if len(raw) > limit else '')
            lines += ['<details>', f'<summary>{cell(t)} / {cell(q.get("query_name"))}: {cell(q.get("state"))}</summary>', '',
                      '<pre>' + html.escape(str(q.get('explain_sql', ''))[:2000]) + '</pre>',
                      '<pre>' + html.escape(excerpt) + '</pre>', '', '</details>', '']
    lines += ['</details>', '']
    return '\n'.join(lines)


def lifecycle(root, targets):
    manifest = read(root / 'run-manifest.json')
    corpus = read(root / 'corpus' / 'summary.json')
    lines = ['# Load / lifecycle summary', '',
             f'Dataset: **{cell(manifest.get("profile", corpus.get("data_profile", {}).get("id")))}**; rows: **{cell(corpus.get("line_count"))}**; '
             f'seed: `{cell(corpus.get("seed"))}`.',
             f'DB limits: {cell(manifest.get("db_cpus"))} CPU / {cell(manifest.get("db_memory"))}.',
             f'Data window: {cell(corpus.get("start"))} → {cell(corpus.get("end"))}.',
             f'Query window: {cell(corpus.get("query_start"))} → {cell(corpus.get("query_end"))}.', '']
    rows = []
    for t in targets:
        d = result_dir(root, t)
        load = read(d / 'load-result.json')
        check = read(d / 'correctness-result.json')
        state_path = root / f'{t}-1' / 'target-after-load.json'
        states = read(state_path)
        state = states[0].get('State', {}) if states else {}
        exit_path = root / f'{t}-1' / 'exit-code.txt'
        rows.append([t, load.get('count_before'), load.get('count_after'), load.get('smoke_matched_rows'),
                     sum(q.get('status') == 'passed' for q in check.get('queries', [])) if check else None,
                     state.get('Running'), state.get('OOMKilled'),
                     exit_path.read_text().strip() if exit_path.exists() else None])
    lines += [table(['DB', 'Rows before', 'Rows after', 'Load smoke rows', 'Correct queries',
                     'Running after load', 'OOM after load', 'Target exit code'], rows), '',
              ]
    physical_rows = []
    load_rows = []
    details = ['<details>', '<summary>DDL, index configuration and image provenance</summary>', '']
    raw_bytes = read(root / 'corpus' / 'size.json').get('raw_jsonl_bytes')
    for t in targets:
        directory = result_dir(root, t)
        physical = read(directory / 'physical-evidence.json')
        storage = physical.get('storage', {})
        total = storage.get('engine_total_bytes')
        stats = read(root / f'{t}-1' / 'post-load-docker-stats.json')
        ratio = f'{raw_bytes / total:.2f}×' if (physical.get('passed') is True and
                 isinstance(raw_bytes, (int, float)) and raw_bytes > 0 and
                 isinstance(total, (int, float)) and total > 0) else '—'
        physical_rows.append([t, size(total), ratio, physical.get('materialization', {}).get('state'),
                              storage.get('sst_files', storage.get('active_parts')),
                              stats.get('CPUPerc'), stats.get('MemUsage')])
        load = read(directory / 'load-result.json')
        metadata = read(directory / 'target-metadata.json')
        load_rows.append([t, load.get('load_transport', load.get('transport', '/insert/jsonline' if t == 'victorialogs' and load else None)),
                          load.get('batches', load.get('batch_count')), load.get('smoke_matched_rows')])
        ddl = metadata.get('schema_sql') or physical.get('schema_definition')
        detail = {'image': manifest.get('images', {}).get(t), 'schema': ddl,
                  'physical_design': metadata.get('physical_design'),
                  'storage': storage, 'coverage': physical.get('coverage')}
        details += [f'### {t}', '<pre>' + html.escape(json.dumps(detail, indent=2, ensure_ascii=False)) + '</pre>', '']
    lines += ['## Load and physical evidence', '', table(['DB', 'Write transport', 'Batches', 'Load smoke rows'], load_rows), '',
              f'Raw JSONL size: **{size(raw_bytes)}**. Ratio = raw JSONL / engine bytes at collection time; '
              'not a settled-storage guarantee. CPU/memory are post-load snapshots, not peaks. '
              'Missing artifacts remain blank; pre-cleanup stats are not substituted.', '',
              table(['DB', 'Engine size', 'Size ratio', 'Materialization', 'SSTs / active parts',
                     'Post-load CPU', 'Post-load memory / limit'], physical_rows), '', *details, '</details>', '']
    rows = []
    for context, path in [('dataset / final cleanup', root / 'timings.jsonl')] + [
            (t, root / f'{t}-1' / 'timings.jsonl') for t in targets]:
        if not path.exists():
            rows.append([context, 'unavailable', '—', '—'])
            continue
        for line in path.read_text().splitlines():
            entry = json.loads(line)
            # Older date implementations emitted ns-like values under ms keys; never guess units.
            valid = all(isinstance(entry.get(k), (int, float)) and 0 <= entry[k] < 100_000_000_000_000
                        for k in ('started_at_ms', 'ended_at_ms'))
            rows.append([context, entry.get('phase'), duration(entry.get('elapsed_ms')) if valid else
                         'invalid timestamp units (legacy artifact)', entry.get('exit_code')])
    lines += ['## Phase timings', '', 'Phase duration includes orchestration overhead. Container cleanup does not prove ECS teardown.', '', table(['DB / scope', 'Phase', 'Duration', 'Exit code'], rows), '']
    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser(description='Render observability artifacts as GitHub step summaries.')
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--targets', required=True)
    parser.add_argument('--section', choices=('benchmark', 'lifecycle'), required=True)
    args = parser.parse_args()
    targets = args.targets.split(',')
    if not targets or any(t not in TARGETS for t in targets) or len(set(targets)) != len(targets):
        parser.error('targets must be a comma-separated subset of greptimedb,clickhouse,victorialogs')
    print((benchmark if args.section == 'benchmark' else lifecycle)(args.root, targets))


if __name__ == '__main__':
    main()
