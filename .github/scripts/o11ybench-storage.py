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

"""Select File/S3 benchmark arms without changing workload adapters."""
import argparse
import copy
import importlib.util
import json
from pathlib import Path
import sys
import os
import subprocess


def validate(env):
    mode = env.get('GREPTIMEDB_STORAGE', 'file')
    if mode not in ('file', 's3', 'both'):
        raise ValueError('GREPTIMEDB_STORAGE must be file, s3 or both')
    if mode != 'file':
        if env.get('RUN_GREPTIMEDB') != 'true':
            raise ValueError('S3 requires Run GreptimeDB')
        if env.get('PROVIDER') != 'AWS':
            raise ValueError('S3 comparisons require an AWS runner')
        if not env.get('S3_BUCKET') or not env.get('S3_REGION'):
            raise ValueError('AWS_CI_TEST_BUCKET and AWS_CI_TEST_BUCKET_REGION must be configured')
        if env['S3_REGION'] != env.get('RUNNER_REGION'):
            raise ValueError('S3 bucket and EC2 runner regions must match')
    return mode


def replace(command, key, value):
    result = list(command)
    if key in result:
        result[result.index(key) + 1] = value
    else:
        result += [key, value]
    return result


def commands(command, mode):
    """Recovery visits each owned root; execution uses explicit workflow steps."""
    stage = command[command.index('--stage') + 1]
    if stage != 'cleanup':
        raise ValueError('only cleanup may expand multiple arms')
    result = [list(command)]
    if mode != 'file':
        root = command[command.index('--root') + 1]
        owner = command[command.index('--owner') + 1]
        secondary = replace(command, '--root', root + '/s3')
        result.append(replace(secondary, '--owner', owner + '-s3'))
    return result


def module(path):
    sys.path.insert(0, str(path.parent))
    spec = importlib.util.spec_from_file_location(path.stem.replace('-', '_'), path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def summary(kind, root, targets, section, mode):
    filename = 'agent-observability-summary.py' if kind == 'agent' else 'tracesbench-summary.py'
    renderer = module(Path(__file__).with_name(filename))
    primary = [t for t in targets if t != 'greptimedb' or mode != 's3']
    arms = [(root, primary, 'File')]
    if mode != 'file' and 'greptimedb' in targets:
        arms.append((root / 's3', ['greptimedb'], 'S3'))
    output = []
    for arm, selected, label in arms:
        if not selected:
            continue
        output.append('## ' + ('GreptimeDB S3' if label == 'S3' else 'Primary targets (GreptimeDB File)'))
        if kind == 'agent':
            body = (renderer.benchmark(arm, selected) if section == 'benchmark'
                    else renderer.lifecycle(arm, selected, data_root=root.parent if root.name == 'baseline' else root))
        else:
            report, runs = renderer.load(arm)
            body = (renderer.benchmark(report, runs, selected) if section == 'benchmark'
                    else renderer.lifecycle(report, runs, arm, selected))
        body = body.replace('| greptimedb |', '| GreptimeDB ' + label + ' |')
        body = body.replace('**greptimedb**', '**GreptimeDB ' + label + '**')
        output.append(body)
        if label == 'S3':
            directories = [arm / 'greptimedb-1'] if kind == 'agent' else [arm / 'runs/greptimedb']
            for directory in directories:
                for action in ('probe', 'inventory', 'cleanup'):
                    path = directory / ('s3-' + action + '.json')
                    data = json.loads(path.read_text()) if path.exists() else {}
                    status = 'passed' if data.get('passed') is True else 'failed / unavailable'
                    count = data.get('object_count', data.get('deleted_objects', '—'))
                    output.append(f'- S3 {action}: **{status}**; objects: {count}.')
    return '\n\n'.join(output)


def split_metrics(plan):
    """One executor root per target, with the immutable dataset shared by all."""
    result = {}
    for target in plan['targets']:
        child = copy.deepcopy(plan)
        name = target['name']
        dest = str(Path(plan['output_root']) / 'o11ybench' / ('o11ybench-' + name))
        child['output_root'] = dest
        child['run_id'] = plan['run_id'] + '-' + name
        child['targets'] = [copy.deepcopy(target)]
        child['data']['generate'] = False
        child['report'].update(root=dest + '/report', index_html=dest + '/report/index.html')
        child['artifacts'] = {k: dest + '/' + Path(v).name for k, v in plan['artifacts'].items()}
        result[name] = child
    return result


def metrics(action, args):
    parser = argparse.ArgumentParser()
    parser.add_argument('--plan', type=Path, required=True)
    parser.add_argument('--tools', type=Path, required=True)
    parser.add_argument('--runtime-image', required=True)
    parser.add_argument('--target')
    a = parser.parse_args(args)
    plan = json.loads(a.plan.read_text())
    plan['runtime_image'] = a.runtime_image
    executor = module(a.tools / 'scripts/run_benchmark_plan.py')
    root = Path(plan['output_root'])
    if action == 'metrics-prepare':
        executor.require_safe_execute_root(root)
        root.mkdir(parents=True, exist_ok=False)
        manifest = executor.build_manifest(plan)
        executor.run_checked([str(executor.DOWNLOAD_DATA)], manifest,
                             env=executor.generation_environment(plan))
        (root / 'generation.json').write_text(json.dumps(manifest, indent=2) + '\n')
        for name, child in split_metrics(plan).items():
            (a.plan.parent / (name + '.json')).write_text(json.dumps(child, indent=2) + '\n')
    elif action == 'metrics-run':
        path = a.plan.parent / (a.target + '.json')
        child = json.loads(path.read_text())
        executor.execute_plan(child, executor.build_manifest(child), replace=False)
        subprocess.run([sys.executable, str(a.tools / 'scripts/check_run_benchmark_plan.py'),
                        '--artifacts', child['output_root']], check=True)
    else:
        # Validate every selected target together, retaining cross-target gates.
        manifests = [executor.build_manifest(child) for child in split_metrics(plan).values()]
        combined = executor.build_manifest(plan)
        combined['targets'] = [target for m in manifests for target in m['targets']]
        for command in executor.validation_commands(combined):
            subprocess.run(command, check=True)


def overview(path):
    """Render the same validated report data into the GitHub job summary."""
    data = json.loads(path.read_text())
    renderer = module(Path(__file__).with_name('agent-observability-summary.py'))
    lines = ['# Storage comparison', '', 'Smoke results are diagnostic; see classification per arm. '
             'No cold-object-store performance claim. Missing results are not passes.', '']
    for workload in data['workloads']:
        labels = {p['id']: p['label'] for p in workload['participants']}
        for scenario in workload['scenarios']:
            lines += ['## ' + workload['title'] + ' / ' + scenario['label'], '']
            rows = []
            for result in scenario['results']:
                lines += [labels[result['participant']] + ': ' + str(result.get('classification')), '']
                for query_id, query in result['queries'].items():
                    m = query['metrics']
                    rows.append([query_id, labels[result['participant']], query['status'],
                                 renderer.duration(m.get('p50_ms', m.get('median_ms'))),
                                 renderer.duration(m.get('p95_ms')), renderer.duration(m.get('p99_ms'))])
            lines.append(renderer.table(['Query', 'DB / storage', 'Status', 'P50 / median', 'P95', 'P99'], rows))
    return '\n'.join(lines)


def audit(root, report, targets, mode, baseline=False, allow_trace_query_failures=False):
    data = json.loads(report.read_text())
    actual = {(p['target'], p.get('storage', {}).get('type'), p['role'])
              for w in data['workloads'] for p in w['participants']}
    expected = {(t, None, 'comparison') for t in targets if t != 'greptimedb'}
    modes = ['file', 's3'] if mode == 'both' else [mode]
    if 'greptimedb' in targets:
        expected |= {('greptimedb', m, role) for m in modes
                     for role in (['candidate', 'baseline'] if baseline else ['candidate'])}
    if actual != expected:
        raise ValueError(f'participant mismatch: expected {sorted(map(str, expected))}, got {sorted(map(str, actual))}')
    query_failures = []
    for w in data['workloads']:
        partial = allow_trace_query_failures and w.get('id') == 'traces'
        if partial and not w['scenarios']:
            raise ValueError('missing trace scenarios')
        for scenario in w['scenarios']:
            if partial and {r['participant'] for r in scenario['results']} != {p['id'] for p in w['participants']}:
                raise ValueError('missing trace participant results')
            for result in scenario['results']:
                if partial and result.get('health') != 'recorded healthy snapshots':
                    raise ValueError('missing or unhealthy trace evidence: ' + result['participant'])
                queries = result['queries']
                if not any(q['status'] == 'passed' for q in queries.values()):
                    raise ValueError('no successful measured queries: ' + result['participant'])
                if any(q['status'] == 'unavailable' or (q['status'] == 'failed' and not partial)
                       for q in queries.values()):
                    raise ValueError('failed or unavailable queries: ' + result['participant'])
                for query_id, query in queries.items():
                    if query['status'] == 'failed':
                        query_failures.append(dict(workload=w['id'], scenario=scenario['id'],
                                                   participant=result['participant'], query=query_id,
                                                   reason=query.get('reason', '')))
    for manifest_path in root.glob('o11ybench/*/run-manifest.json'):
        manifest = json.loads(manifest_path.read_text())
        if manifest.get('status') != 'passed':
            raise ValueError('incomplete target manifest: ' + str(manifest_path))
    probes = list(root.rglob('s3-probe.json'))
    expected_s3 = sum(m == 's3' for _, m, _ in expected)
    if len(probes) != expected_s3:
        raise ValueError('missing or duplicate S3 evidence')
    for probe in probes:
        for action in ('probe', 'inventory', 'cleanup'):
            payload = json.loads(probe.with_name('s3-' + action + '.json').read_text())
            if payload.get('passed') is not True or (action == 'inventory' and payload.get('object_count', 0) <= 0):
                raise ValueError('failed S3 ' + action)
    text = (root / 'ci-summary.md').read_text()
    for m in modes if 'greptimedb' in targets else []:
        if m.upper() not in text:
            raise ValueError('storage mode missing from saved summary: ' + m)
    result = dict(passed=True, participants=[dict(target=t, storage=m, role=role)
                  for t, m, role in sorted(expected, key=str)], s3_arms=len(probes), query_failures=query_failures)
    (root / 'artifact-audit.json').write_text(json.dumps(result, indent=2) + '\n')
    lines = ['## Artifact validation', '', '- Selected participants: ' + str(len(expected)) + '; all present.',
             '- Query result coverage and S3 probe/inventory/cleanup: passed.', '']
    if query_failures:
        lines += ['- Collection completed with query failures; this is not an all-pass query run.', '']
        lines += [f"- {f['participant']} / {f['query']}: failed (excluded from comparison)."
                  for f in query_failures]
        lines.append('')
    renderer = module(Path(__file__).with_name('agent-observability-summary.py'))
    rows = []
    for probe in probes:
        inv = json.loads(probe.with_name('s3-inventory.json').read_text())
        cleanup = json.loads(probe.with_name('s3-cleanup.json').read_text())
        rows.append([str(probe.parent.relative_to(root)), inv['object_count'], inv['object_bytes'],
                     cleanup.get('deleted_objects'), 'passed'])
    if rows:
        lines.append(renderer.table(['S3 arm', 'Objects', 'Remote bytes', 'Deleted current objects', 'Cleanup'], rows))
    text = '\n'.join(lines) + '\n'
    with (root / 'ci-summary.md').open('a') as output:
        output.write(text)
    if os.environ.get('GITHUB_STEP_SUMMARY'):
        with Path(os.environ['GITHUB_STEP_SUMMARY']).open('a') as output:
            output.write(text)
    return result


def main():
    if len(sys.argv) > 1 and sys.argv[1] == 'audit':
        parser = argparse.ArgumentParser()
        parser.add_argument('--root', type=Path, required=True)
        parser.add_argument('--report', type=Path, required=True)
        parser.add_argument('--targets', required=True)
        parser.add_argument('--allow-trace-query-failures', action='store_true',
                            help='Retain failed Traces queries after native validation; missing results still fail')
        args = parser.parse_args(sys.argv[2:])
        print(json.dumps(audit(args.root, args.report, args.targets.split(','),
                              os.environ.get('GREPTIMEDB_STORAGE', 'file'),
                              os.environ.get('COMPARE_GREPTIMEDB') == 'true', args.allow_trace_query_failures)))
        return
    if len(sys.argv) > 1 and sys.argv[1] == 'normalize-metrics':
        path = Path(sys.argv[2])
        plan = json.loads(path.read_text())
        for target in plan['targets']:
            if target.get('storage', {}).get('type') == 's3' and '-s3' not in target['name']:
                target['name'] = target['name'].replace('-baseline', '-s3-baseline') if target['name'].endswith('-baseline') else target['name'] + '-s3'
        path.write_text(json.dumps(plan, indent=2) + '\n')
        return
    if len(sys.argv) > 1 and sys.argv[1].startswith('metrics-'):
        metrics(sys.argv[1], sys.argv[2:])
        return
    if len(sys.argv) > 1 and sys.argv[1] == 'overview':
        print(overview(Path(sys.argv[2])))
        return
    if len(sys.argv) > 1 and sys.argv[1] == 'summary':
        parser = argparse.ArgumentParser()
        parser.add_argument('--kind', choices=['agent', 'traces'], required=True)
        parser.add_argument('--root', type=Path, required=True)
        parser.add_argument('--targets', required=True)
        parser.add_argument('--section', choices=['benchmark', 'lifecycle'], required=True)
        args = parser.parse_args(sys.argv[2:])
        print(summary(args.kind, args.root, args.targets.split(','), args.section,
                      os.environ.get('GREPTIMEDB_STORAGE', 'file')))
        return
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['validate', 'run'])
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    mode = validate(os.environ)
    if args.action == 'validate':
        print('GreptimeDB storage: ' + mode)
        return
    command = args.command[1:] if args.command[:1] == ['--'] else args.command
    planned = commands(command, mode)
    failed = 0
    for cmd in planned:
        result = subprocess.run(cmd)
        failed = failed or result.returncode
        # Cleanup must try both arms even if one failed. Target failures remain
        # visible, while the other arm can still produce diagnostic evidence.
    raise SystemExit(failed)


if __name__ == '__main__':
    main()
