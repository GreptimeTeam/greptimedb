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

"""Render shared O11yBench sampled resource artifacts for lifecycle summaries."""
import json
import math


def number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value >= 0


def metric(values, key, statistic):
    entry = values.get(key, {})
    value = entry.get(statistic)
    return value if number(entry.get('samples')) and entry['samples'] > 0 and number(value) else None


def decimal(value, suffix=''):
    return f'{value:.2f}{suffix}' if number(value) else '—'


def memory(value):
    if not number(value):
        return '—'
    for unit in ('B', 'KiB', 'MiB', 'GiB', 'TiB'):
        if value < 1024 or unit == 'TiB':
            return decimal(value, f' {unit}')
        value /= 1024


def capacities(path):
    # Quotas are metadata, not recomputed CPU statistics. Bound reads on long runs.
    result = {}
    try:
        with path.open() as source:
            for index, line in enumerate(source):
                if index == 256:
                    break
                record = json.loads(line)
                cores = record.get('host', {}).get('cpu_count')
                if number(cores) and cores > 0:
                    result.setdefault('host', cores)
                for role, values in record.get('containers', {}).items():
                    quota = values.get('configured_cpu_limit_cores')
                    if number(quota):
                        result.setdefault(role, quota)
                if all(role in result for role in ('host', 'runtime', 'db')):
                    break
    except (OSError, ValueError, TypeError, AttributeError):
        pass
    return result


def render_resources(scopes, table):
    rows = []
    for scope, directory in scopes:
        try:
            summary = json.loads((directory / 'resources.summary.json').read_text())
            phases = summary.get('phases', {}) if summary.get('schema') == 'o11ybench-resources-v1' else {}
        except (OSError, ValueError, TypeError, AttributeError):
            phases = {}
        if not isinstance(phases, dict) or not phases:
            rows.append([scope, 'unavailable', *['—'] * 9])
            continue
        limits = capacities(directory / 'resources.jsonl')
        for phase, data in phases.items():
            for role in ('host', 'runtime', 'db'):
                values = data.get('host', {}) if role == 'host' else data.get('containers', {}).get(role, {})
                if role == 'db' and scope == 'dataset':
                    continue
                quota = limits.get(role)
                cpu_samples = values.get('cpu_used_cores', {}).get('samples')
                rows.append([scope, phase, role, cpu_samples,
                             decimal(metric(values, 'cpu_used_cores', 'mean')),
                             decimal(metric(values, 'cpu_used_cores', 'max')),
                             'unlimited' if quota == 0 else decimal(quota),
                             memory(metric(values, 'memory_used_bytes' if role == 'host' else 'memory_usage_bytes', 'max')),
                             decimal(metric(values, 'io_wait_percent', 'mean'), '%'),
                             decimal(metric(values, 'io_wait_percent', 'max'), '%'),
                             memory(metric(values, 'swap_used_bytes', 'max'))])
    return '\n'.join(['## Sampled resources', '',
                      'CPU is measured in cores. Mean/max summarize available samples, not continuous peaks; '
                      'short phases may have no CPU sample. CPU capacity is host cores or the configured container quota. '
                      'Host memory is total minus available; Docker memory excludes cache where supported. '
                      'Swap is host-wide usage, not swap activity. Missing evidence stays —.', '',
                      table(['Scope', 'Phase', 'Component', 'CPU samples', 'Mean CPU cores', 'Max CPU cores',
                             'CPU capacity', 'Max memory', 'Mean I/O wait', 'Max I/O wait', 'Max swap'], rows), ''])
