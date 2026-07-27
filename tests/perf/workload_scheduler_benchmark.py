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

"""Compare end-to-end GreptimeDB performance with the scheduler off and on.

Every sample starts a fresh standalone server and seeds an equivalent Mito
table. Modes are interleaved to reduce time/order bias. Request latency is
measured at the HTTP client, from request submission through response parsing.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

import workload_scheduler_runner as workload


PHASE_OPTIONS = {
    "query_only": ("query",),
    "write_only": ("write",),
    "light_write": ("query", "write"),
    "saturated": ("query", "write"),
}
MAX_CAPACITY_NORMALIZED_REGRESSION_PERCENT = 5.0


def reserve_ports(count: int) -> list[int]:
    sockets = []
    try:
        for _ in range(count):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(("127.0.0.1", 0))
            sockets.append(sock)
        return [int(sock.getsockname()[1]) for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


def write_config(
    path: Path,
    enabled: bool,
    ports: list[int],
    runtime_size: int,
    max_concurrent_polls: int,
) -> None:
    path.write_text(
        f"""[runtime]
global_rt_size = {runtime_size}
compact_rt_size = 1
query_rt_size = {runtime_size}
ingest_rt_size = {runtime_size}

[runtime.experimental_workload_scheduler]
enable = {str(enabled).lower()}
max_concurrent_polls = {max_concurrent_polls}
query_weight = 2
write_weight = 8

[http]
addr = "127.0.0.1:{ports[0]}"

[grpc]
bind_addr = "127.0.0.1:{ports[1]}"

[mysql]
enable = false
addr = "127.0.0.1:{ports[2]}"

[postgres]
enable = false
addr = "127.0.0.1:{ports[3]}"
"""
    )


def wait_for_server(
    client: workload.SqlClient,
    process: subprocess.Popen[bytes],
    log_path: Path,
    timeout: float,
) -> None:
    deadline = time.monotonic() + timeout
    last_error: Any = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            tail = log_path.read_text(errors="replace")[-8_000:]
            raise RuntimeError(
                f"GreptimeDB exited with {process.returncode} during startup:\n{tail}"
            )
        ok, _, last_error = client.sql("SELECT 1")
        if ok:
            return
        time.sleep(0.2)
    raise TimeoutError(f"GreptimeDB did not become ready: {last_error}")


def stop_server(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=30)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=10)


def phase_workers(args: argparse.Namespace, phase: str) -> tuple[int, int, float]:
    if phase == "query_only":
        return args.query_workers, 0, 0
    if phase == "write_only":
        return 0, args.write_workers, 0
    if phase == "light_write":
        return args.query_workers, 1, args.light_write_delay
    return args.query_workers, args.write_workers, 0


def run_sample(
    args: argparse.Namespace,
    root: Path,
    phase: str,
    enabled: bool,
    sample_number: int,
) -> dict[str, Any]:
    mode = "scheduled" if enabled else "baseline"
    sample_root = root / f"{phase}-{mode}-{sample_number}"
    data_home = sample_root / "data"
    log_dir = sample_root / "logs"
    config_path = sample_root / "config.toml"
    process_log_path = sample_root / "process.log"
    sample_root.mkdir(parents=True)
    data_home.mkdir()
    log_dir.mkdir()
    ports = reserve_ports(4)
    write_config(
        config_path,
        enabled,
        ports,
        args.runtime_size,
        args.max_concurrent_polls,
    )

    environment = os.environ.copy()
    for variable in (
        "ALL_PROXY",
        "HTTPS_PROXY",
        "HTTP_PROXY",
        "all_proxy",
        "https_proxy",
        "http_proxy",
    ):
        environment.pop(variable, None)

    command = [
        str(args.binary),
        "standalone",
        "start",
        "--config-file",
        str(config_path),
        "--data-home",
        str(data_home),
        "--log-dir",
        str(log_dir),
        "--log-level",
        "warn",
    ]
    with process_log_path.open("wb") as process_log:
        process = subprocess.Popen(
            command,
            stdout=process_log,
            stderr=subprocess.STDOUT,
            env=environment,
        )
        try:
            client = workload.SqlClient(
                f"http://127.0.0.1:{ports[0]}", "public", args.timeout
            )
            wait_for_server(client, process, process_log_path, args.start_timeout)
            workload.setup_table(client, args.seed_rows, args.seed_batch_size)
            query_workers, write_workers, write_delay = phase_workers(args, phase)
            result = workload.run_phase(
                client,
                phase,
                args.duration,
                args.warmup,
                query_workers,
                write_workers,
                args.write_batch_size,
                write_delay,
                workload.Sequence(1_800_000_000_000),
                "required" if enabled else "disabled",
            )
        finally:
            stop_server(process)

    result["mode"] = mode
    result["sample"] = sample_number
    for workload_name in PHASE_OPTIONS[phase]:
        requests = result["requests"][workload_name]
        if requests["requests"] and requests["failures"] / requests["requests"] >= 0.01:
            raise AssertionError(
                f"{mode} {phase} {workload_name} failure rate was at least 1%: "
                f"{requests}"
            )
    if enabled and phase == "saturated":
        result["saturation_verified"] = result["poll_share"]["write"] >= 0.799
    return result


def median_request_metric(
    samples: list[dict[str, Any]], workload_name: str, metric: str
) -> float | None:
    values = [
        sample["requests"][workload_name].get(metric)
        for sample in samples
        if sample["requests"][workload_name].get(metric) is not None
    ]
    return statistics.median(values) if values else None


def percent_change(current: float | None, baseline: float | None) -> float | None:
    if current is None or baseline in (None, 0):
        return None
    return (current / baseline - 1.0) * 100.0


def summarize(samples: list[dict[str, Any]], phases: list[str]) -> dict[str, Any]:
    report: dict[str, Any] = {}
    for phase in phases:
        phase_samples = [sample for sample in samples if sample["name"] == phase]
        modes: dict[str, Any] = {}
        for mode in ("baseline", "scheduled"):
            mode_samples = [
                sample for sample in phase_samples if sample["mode"] == mode
            ]
            query_rps = median_request_metric(
                mode_samples, "query", "successful_rps"
            )
            write_rps = median_request_metric(
                mode_samples, "write", "successful_rps"
            )
            modes[mode] = {
                "query_rps": query_rps,
                "write_rps": write_rps,
                "total_request_rps": (query_rps or 0) + (write_rps or 0),
                "query_p50_ms": median_request_metric(
                    mode_samples, "query", "p50_ms"
                ),
                "query_mean_ms": median_request_metric(
                    mode_samples, "query", "mean_ms"
                ),
                "query_p95_ms": median_request_metric(
                    mode_samples, "query", "p95_ms"
                ),
                "write_p50_ms": median_request_metric(
                    mode_samples, "write", "p50_ms"
                ),
                "write_mean_ms": median_request_metric(
                    mode_samples, "write", "mean_ms"
                ),
                "write_p95_ms": median_request_metric(
                    mode_samples, "write", "p95_ms"
                ),
            }
            if mode == "scheduled":
                shares = [
                    sample["poll_share"]["write"]
                    for sample in mode_samples
                    if sample["poll_share"] is not None
                ]
                modes[mode]["write_poll_share"] = (
                    statistics.median(shares) if shares else None
                )
                modes[mode]["minimum_write_poll_share"] = (
                    min(shares) if shares else None
                )

        baseline = modes["baseline"]
        scheduled = modes["scheduled"]
        report[phase] = {
            "baseline": baseline,
            "scheduled": scheduled,
            "scheduled_vs_baseline_percent": {
                "query_throughput": percent_change(
                    scheduled["query_rps"], baseline["query_rps"]
                ),
                "write_throughput": percent_change(
                    scheduled["write_rps"], baseline["write_rps"]
                ),
                "total_request_throughput": percent_change(
                    scheduled["total_request_rps"], baseline["total_request_rps"]
                ),
                "query_p50_latency": percent_change(
                    scheduled["query_p50_ms"], baseline["query_p50_ms"]
                ),
                "query_mean_latency": percent_change(
                    scheduled["query_mean_ms"], baseline["query_mean_ms"]
                ),
                "query_p95_latency": percent_change(
                    scheduled["query_p95_ms"], baseline["query_p95_ms"]
                ),
                "write_p50_latency": percent_change(
                    scheduled["write_p50_ms"], baseline["write_p50_ms"]
                ),
                "write_mean_latency": percent_change(
                    scheduled["write_mean_ms"], baseline["write_mean_ms"]
                ),
                "write_p95_latency": percent_change(
                    scheduled["write_p95_ms"], baseline["write_p95_ms"]
                ),
            },
        }

    if "query_only" in report and "write_only" in report:
        query_capacity = report["query_only"]["baseline"]["query_rps"]
        write_capacity = report["write_only"]["baseline"]["write_rps"]
        if query_capacity and write_capacity:
            for phase in phases:
                modes = report[phase]
                for mode in ("baseline", "scheduled"):
                    values = modes[mode]
                    values["capacity_normalized_work_rate"] = (
                        values["query_rps"] / query_capacity
                        + values["write_rps"] / write_capacity
                    )
                modes["scheduled_vs_baseline_percent"][
                    "capacity_normalized_work_rate"
                ] = percent_change(
                    modes["scheduled"]["capacity_normalized_work_rate"],
                    modes["baseline"]["capacity_normalized_work_rate"],
                )

        samples_by_key = {
            (sample["sample"], sample["name"], sample["mode"]): sample
            for sample in samples
        }
        sample_numbers = sorted({sample["sample"] for sample in samples})
        for phase in phases:
            paired = []
            for sample_number in sample_numbers:
                query_only = samples_by_key.get(
                    (sample_number, "query_only", "baseline")
                )
                write_only = samples_by_key.get(
                    (sample_number, "write_only", "baseline")
                )
                baseline = samples_by_key.get(
                    (sample_number, phase, "baseline")
                )
                scheduled = samples_by_key.get(
                    (sample_number, phase, "scheduled")
                )
                if not all((query_only, write_only, baseline, scheduled)):
                    continue
                query_capacity = query_only["requests"]["query"]["successful_rps"]
                write_capacity = write_only["requests"]["write"]["successful_rps"]
                if not query_capacity or not write_capacity:
                    continue

                def normalized(sample: dict[str, Any]) -> float:
                    requests = sample["requests"]
                    return (
                        requests["query"]["successful_rps"] / query_capacity
                        + requests["write"]["successful_rps"] / write_capacity
                    )

                baseline_rate = normalized(baseline)
                scheduled_rate = normalized(scheduled)
                paired.append(
                    {
                        "sample": sample_number,
                        "baseline": baseline_rate,
                        "scheduled": scheduled_rate,
                        "scheduled_vs_baseline_percent": percent_change(
                            scheduled_rate, baseline_rate
                        ),
                    }
                )
            if paired:
                changes = [
                    sample["scheduled_vs_baseline_percent"] for sample in paired
                ]
                report[phase]["paired_capacity_normalized"] = {
                    "samples": paired,
                    "median_scheduled_vs_baseline_percent": statistics.median(
                        changes
                    ),
                    "worst_scheduled_vs_baseline_percent": min(changes),
                    "within_five_percent": min(changes)
                    >= -MAX_CAPACITY_NORMALIZED_REGRESSION_PERCENT,
                }
    return report


def parse_args() -> argparse.Namespace:
    default_binary = Path(__file__).resolve().parents[2] / "target/release/greptime"
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, default=default_binary)
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument("--duration", type=float, default=8.0)
    parser.add_argument("--warmup", type=float, default=4.0)
    parser.add_argument("--runtime-size", type=int, default=4)
    parser.add_argument(
        "--max-concurrent-polls",
        type=int,
        default=0,
        help="scheduler in-flight poll limit; zero uses 4 * --runtime-size",
    )
    parser.add_argument("--query-workers", type=int, default=2)
    parser.add_argument("--write-workers", type=int, default=1152)
    parser.add_argument("--seed-rows", type=int, default=10_000)
    parser.add_argument("--seed-batch-size", type=int, default=500)
    parser.add_argument("--write-batch-size", type=int, default=32)
    parser.add_argument("--light-write-delay", type=float, default=0.1)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--start-timeout", type=float, default=60.0)
    parser.add_argument(
        "--phases",
        nargs="+",
        choices=tuple(PHASE_OPTIONS),
        default=list(PHASE_OPTIONS),
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="optional path for the JSON report; stdout is always populated",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.binary = args.binary.resolve()
    if not args.binary.is_file():
        raise FileNotFoundError(args.binary)
    if args.iterations <= 0:
        raise ValueError("--iterations must be greater than zero")
    if args.max_concurrent_polls == 0:
        args.max_concurrent_polls = args.runtime_size * 4

    samples = []
    with tempfile.TemporaryDirectory(prefix="greptime-workload-benchmark-") as temp:
        root = Path(temp)
        for iteration in range(args.iterations):
            for phase_index, phase in enumerate(args.phases):
                order = (
                    (False, True)
                    if (iteration + phase_index) % 2 == 0
                    else (True, False)
                )
                for enabled in order:
                    mode = "scheduled" if enabled else "baseline"
                    print(
                        f"running iteration={iteration + 1} phase={phase} mode={mode}",
                        file=sys.stderr,
                        flush=True,
                    )
                    samples.append(
                        run_sample(args, root, phase, enabled, iteration + 1)
                    )

    summary = summarize(samples, args.phases)
    normalized_checks = [
        values["paired_capacity_normalized"]["within_five_percent"]
        for values in summary.values()
        if "paired_capacity_normalized" in values
    ]
    saturated_checks = [
        sample["saturation_verified"]
        for sample in samples
        if sample["name"] == "saturated"
        and sample["mode"] == "scheduled"
        and "saturation_verified" in sample
    ]
    regression_verified = all(normalized_checks) if normalized_checks else None
    saturation_verified = all(saturated_checks) if saturated_checks else None
    applicable_checks = [
        check
        for check in (regression_verified, saturation_verified)
        if check is not None
    ]
    report = {
        "configuration": {
            "binary": str(args.binary),
            "iterations": args.iterations,
            "duration_s": args.duration,
            "warmup_s": args.warmup,
            "runtime_size": args.runtime_size,
            "max_concurrent_polls": args.max_concurrent_polls,
            "query_workers": args.query_workers,
            "write_workers": args.write_workers,
            "seed_rows": args.seed_rows,
            "write_batch_size": args.write_batch_size,
        },
        "verification": {
            "capacity_normalized_regression_budget_percent": (
                MAX_CAPACITY_NORMALIZED_REGRESSION_PERCENT
            ),
            "capacity_normalized_regression_verified": regression_verified,
            "saturated_write_poll_share_at_least_80_percent": saturation_verified,
            "passed": all(applicable_checks) if applicable_checks else None,
        },
        "summary": summary,
        "samples": samples,
    }
    rendered = json.dumps(report, indent=2, sort_keys=True)
    if args.output:
        args.output.write_text(rendered + "\n")
    print(rendered)


if __name__ == "__main__":
    main()
