#!/usr/bin/env python3
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

"""Exercise GreptimeDB's experimental query/write workload scheduler.

Start a standalone server, then run this script against its HTTP port. The
script creates and seeds a real Mito table and executes concurrent request
phases. By default it also verifies the experimental workload scheduler's
Prometheus poll-admission counters; metrics can be disabled when benchmarking
against a scheduler-disabled baseline.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import dataclasses
import json
import math
import re
import statistics
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


QUERY_TABLE = "catio_scheduler_query_load"
WRITE_TABLE = "catio_scheduler_write_load"
SHARDS = 64
QUERY_PARTITIONS = 32
WRITE_PARTITIONS = 64
METRIC_RE = re.compile(
    r'^greptime_workload_scheduler_polls\{workload="(query|write)"\}\s+([0-9.eE+-]+)$'
)


@dataclasses.dataclass
class RequestStats:
    requests: int = 0
    failures: int = 0
    latencies_ms: list[float] = dataclasses.field(default_factory=list)
    failure_samples: list[str] = dataclasses.field(default_factory=list)

    def merge(self, other: RequestStats) -> None:
        self.requests += other.requests
        self.failures += other.failures
        self.latencies_ms.extend(other.latencies_ms)
        self.failure_samples.extend(other.failure_samples[: 3 - len(self.failure_samples)])

    def summary(self, duration: float) -> dict[str, Any]:
        successful = self.requests - self.failures
        latencies = sorted(self.latencies_ms)
        return {
            "requests": self.requests,
            "failures": self.failures,
            "failure_samples": self.failure_samples,
            "successful_rps": successful / duration,
            "mean_ms": statistics.fmean(latencies) if latencies else None,
            "p50_ms": percentile(latencies, 0.50),
            "p95_ms": percentile(latencies, 0.95),
        }


@dataclasses.dataclass
class PhaseClock:
    warmup: float
    duration: float
    measurement_start: float = 0.0
    deadline: float = 0.0

    def start(self) -> None:
        started = time.monotonic()
        self.measurement_start = started + self.warmup
        self.deadline = self.measurement_start + self.duration


def percentile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    index = min(math.ceil(len(values) * quantile) - 1, len(values) - 1)
    return values[max(index, 0)]


class SqlClient:
    def __init__(self, base_url: str, database: str, timeout: float) -> None:
        self.base_url = base_url.rstrip("/")
        self.database = database
        self.timeout = timeout
        # Validation targets a local standalone process; inherited development
        # proxies can otherwise turn overload into unrelated HTTP 502 errors.
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def sql(self, sql: str) -> tuple[bool, float, Any]:
        data = urllib.parse.urlencode(
            {"sql": sql, "db": self.database, "format": "json"}
        ).encode()
        request = urllib.request.Request(
            f"{self.base_url}/v1/sql", data=data, method="POST"
        )
        started = time.monotonic()
        try:
            with self.opener.open(request, timeout=self.timeout) as response:
                body = json.loads(response.read().decode())
            ok = response.status < 400 and not response_has_error(body)
            return ok, (time.monotonic() - started) * 1_000, body
        except urllib.error.HTTPError as error:
            body = error.read().decode(errors="replace")
            return (
                False,
                (time.monotonic() - started) * 1_000,
                f"HTTP {error.code}: {body}",
            )
        except (OSError, ValueError, urllib.error.URLError) as error:
            return False, (time.monotonic() - started) * 1_000, str(error)

    def scheduler_polls(self, required: bool = True) -> dict[str, int] | None:
        with self.opener.open(f"{self.base_url}/metrics", timeout=self.timeout) as response:
            text = response.read().decode()
        result: dict[str, int] = {}
        for line in text.splitlines():
            match = METRIC_RE.match(line)
            if match:
                result[match.group(1)] = int(float(match.group(2)))
        if result.keys() != {"query", "write"}:
            if required:
                raise RuntimeError(
                    "scheduler metrics are missing; is "
                    "runtime.experimental_workload_scheduler.enable=true?"
                )
            return None
        return result


def response_has_error(body: Any) -> bool:
    if not isinstance(body, dict):
        return False
    if body.get("error") or body.get("err_msg") or body.get("error_msg"):
        return True
    code = str(body.get("code", "")).lower()
    return "output" not in body and code not in ("", "0", "success")


def setup_table(client: SqlClient, seed_rows: int, batch_size: int) -> None:
    def create_table(table: str, partitions: int) -> str:
        partition_width = SHARDS // partitions
        partition_predicates = [f"shard < {partition_width}"]
        partition_predicates.extend(
            f"shard >= {lower} AND shard < {lower + partition_width}"
            for lower in range(
                partition_width, SHARDS - partition_width, partition_width
            )
        )
        partition_predicates.append(f"shard >= {SHARDS - partition_width}")
        return (
            f"CREATE TABLE {table} ("
            "host STRING, shard INT, val DOUBLE, ts TIMESTAMP TIME INDEX, "
            "PRIMARY KEY(host, shard)) "
            f"PARTITION ON COLUMNS(shard) ({','.join(partition_predicates)}) "
            "ENGINE=mito"
        )

    statements = [
        f"DROP TABLE IF EXISTS {QUERY_TABLE}",
        f"DROP TABLE IF EXISTS {WRITE_TABLE}",
        create_table(QUERY_TABLE, QUERY_PARTITIONS),
        create_table(WRITE_TABLE, WRITE_PARTITIONS),
    ]
    for statement in statements:
        ok, _, body = client.sql(statement)
        if not ok:
            raise RuntimeError(f"setup failed for {statement!r}: {body}")

    timestamp = 1_700_000_000_000
    for offset in range(0, seed_rows, batch_size):
        count = min(batch_size, seed_rows - offset)
        values = ",".join(
            f"('host-{(offset + row) % 64}',{(offset + row) % 64},"
            f"{offset + row},{timestamp + offset + row})"
            for row in range(count)
        )
        ok, _, body = client.sql(
            f"INSERT INTO {QUERY_TABLE} (host,shard,val,ts) VALUES {values}"
        )
        if not ok:
            raise RuntimeError(f"seed insert at row {offset} failed: {body}")


def record_request(
    stats: RequestStats,
    started: float,
    completed: float,
    clock: PhaseClock,
    ok: bool,
    latency: float,
    body: Any,
) -> None:
    if started < clock.measurement_start or completed > clock.deadline:
        return
    stats.requests += 1
    stats.failures += not ok
    stats.latencies_ms.append(latency)
    if not ok and len(stats.failure_samples) < 3:
        stats.failure_samples.append(str(body)[:500])


def query_worker(
    client: SqlClient, clock: PhaseClock, start: threading.Barrier
) -> RequestStats:
    stats = RequestStats()
    query = (
        f"SELECT host, count(*), sum(val), avg(val) FROM {QUERY_TABLE} "
        "GROUP BY host ORDER BY host"
    )
    start.wait()
    while time.monotonic() < clock.deadline:
        started = time.monotonic()
        ok, latency, body = client.sql(query)
        record_request(
            stats, started, time.monotonic(), clock, ok, latency, body
        )
    return stats


def write_worker(
    client: SqlClient,
    clock: PhaseClock,
    start: threading.Barrier,
    sequence: "Sequence",
    batch_size: int,
    delay: float,
) -> RequestStats:
    stats = RequestStats()
    start.wait()
    while time.monotonic() < clock.deadline:
        started = time.monotonic()
        offset = sequence.take(batch_size)
        values = ",".join(
            f"('writer-{(offset + row) % 64}',{(offset + row) % 64},"
            f"{offset + row},{offset + row})"
            for row in range(batch_size)
        )
        ok, latency, body = client.sql(
            f"INSERT INTO {WRITE_TABLE} (host,shard,val,ts) VALUES {values}"
        )
        record_request(
            stats, started, time.monotonic(), clock, ok, latency, body
        )
        if delay:
            time.sleep(delay)
    return stats


class Sequence:
    def __init__(self, initial: int) -> None:
        self.value = initial
        self.lock = threading.Lock()

    def take(self, count: int) -> int:
        with self.lock:
            value = self.value
            self.value += count
            return value


def run_phase(
    client: SqlClient,
    name: str,
    duration: float,
    warmup: float,
    query_workers: int,
    write_workers: int,
    write_batch_size: int,
    write_delay: float,
    sequence: Sequence,
    scheduler_metrics: str,
) -> dict[str, Any]:
    worker_count = query_workers + write_workers
    clock = PhaseClock(warmup, duration)
    start = threading.Barrier(worker_count + 1, action=clock.start)
    query_stats = RequestStats()
    write_stats = RequestStats()

    with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as executor:
        query_futures = [
            executor.submit(query_worker, client, clock, start)
            for _ in range(query_workers)
        ]
        write_futures = [
            executor.submit(
                write_worker,
                client,
                clock,
                start,
                sequence,
                write_batch_size,
                write_delay,
            )
            for _ in range(write_workers)
        ]
        start.wait()
        remaining = clock.measurement_start - time.monotonic()
        if remaining > 0:
            time.sleep(remaining)
        before = (
            client.scheduler_polls(required=scheduler_metrics == "required")
            if scheduler_metrics != "disabled"
            else None
        )
        remaining = clock.deadline - time.monotonic()
        if remaining > 0:
            time.sleep(remaining)
        # Scrape at the phase boundary, while the last requests are still
        # backlogged. Waiting for slow queries to drain would incorrectly count
        # their post-load polls as part of the saturated interval.
        at_deadline = (
            client.scheduler_polls(required=True) if before is not None else None
        )
        for future in query_futures:
            query_stats.merge(future.result())
        for future in write_futures:
            write_stats.merge(future.result())

    poll_delta = (
        {
            workload: at_deadline[workload] - before[workload]
            for workload in before
        }
        if before is not None and at_deadline is not None
        else None
    )
    total_polls = sum(poll_delta.values()) if poll_delta is not None else 0
    shares = (
        {
            workload: (polls / total_polls if total_polls else 0.0)
            for workload, polls in poll_delta.items()
        }
        if poll_delta is not None
        else None
    )
    return {
        "name": name,
        "duration_s": duration,
        "warmup_s": warmup,
        "workers": {"query": query_workers, "write": write_workers},
        "requests": {
            "query": query_stats.summary(duration),
            "write": write_stats.summary(duration),
        },
        "polls": poll_delta,
        "poll_share": shares,
    }


def verify(phases: list[dict[str, Any]]) -> None:
    by_name = {phase["name"]: phase for phase in phases}
    query_only = by_name["query_only"]
    light_write = by_name["light_write"]
    saturated = by_name["saturated"]

    if any(phase["polls"] is None for phase in phases):
        raise AssertionError("scheduler metrics are required for verification")
    if query_only["polls"]["query"] <= 0 or query_only["polls"]["write"] != 0:
        raise AssertionError(f"query-only phase did not borrow all capacity: {query_only}")
    if light_write["poll_share"]["query"] <= 0.20:
        raise AssertionError(f"query did not borrow unused write share: {light_write}")
    if saturated["polls"]["query"] < 100 or saturated["polls"]["write"] < 100:
        raise AssertionError(f"saturated phase did not generate enough work: {saturated}")
    if saturated["poll_share"]["write"] < 0.799:
        raise AssertionError(f"write admission share is below 80% (0.1% tolerance): {saturated}")

    for phase in phases:
        for workload in ("query", "write"):
            request = phase["requests"][workload]
            if request["requests"] and request["failures"] / request["requests"] >= 0.01:
                raise AssertionError(
                    f"{phase['name']} {workload} failure rate was at least 1%: {request}"
                )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:4000")
    parser.add_argument("--database", default="public")
    parser.add_argument("--duration", type=float, default=10.0)
    parser.add_argument("--warmup", type=float, default=2.0)
    parser.add_argument("--query-workers", type=int, default=2)
    parser.add_argument("--write-workers", type=int, default=1152)
    parser.add_argument("--seed-rows", type=int, default=10_000)
    parser.add_argument("--seed-batch-size", type=int, default=500)
    parser.add_argument("--write-batch-size", type=int, default=32)
    parser.add_argument("--light-write-delay", type=float, default=0.1)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument(
        "--phase",
        choices=("all", "query_only", "write_only", "light_write", "saturated"),
        default="all",
    )
    parser.add_argument(
        "--scheduler-metrics",
        choices=("required", "optional", "disabled"),
        default="required",
        help="whether scheduler Prometheus metrics must be collected",
    )
    parser.add_argument("--skip-setup", action="store_true")
    parser.add_argument("--no-verify", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    client = SqlClient(args.url, args.database, args.timeout)
    if not args.skip_setup:
        setup_table(client, args.seed_rows, args.seed_batch_size)

    sequence = Sequence(1_800_000_000_000)
    phase_options = {
        "query_only": (args.query_workers, 0, 0),
        "write_only": (0, args.write_workers, 0),
        "light_write": (args.query_workers, 1, args.light_write_delay),
        "saturated": (args.query_workers, args.write_workers, 0),
    }
    selected = (
        {
            name: phase_options[name]
            for name in ("query_only", "light_write", "saturated")
        }
        if args.phase == "all"
        else {args.phase: phase_options[args.phase]}
    )
    phases = []
    for name, (query_workers, write_workers, write_delay) in selected.items():
        phases.append(
            run_phase(
                client,
                name,
                args.duration,
                args.warmup,
                query_workers,
                write_workers,
                args.write_batch_size,
                write_delay,
                sequence,
                args.scheduler_metrics,
            )
        )
    if not args.no_verify:
        if args.phase != "all":
            raise ValueError("--phase requires --no-verify unless all phases are selected")
        if args.scheduler_metrics != "required":
            raise ValueError("verification requires --scheduler-metrics=required")
        verify(phases)

    result = {
        "verified": not args.no_verify,
        "mean_write_share_saturated": statistics.fmean(
            [
                phase["poll_share"]["write"]
                for phase in phases
                if phase["name"] == "saturated" and phase["poll_share"] is not None
            ]
        )
        if any(
            phase["name"] == "saturated" and phase["poll_share"] is not None
            for phase in phases
        )
        else None,
        "phases": phases,
    }
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
