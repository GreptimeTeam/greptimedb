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

"""Run one or more query regression cases after the binaries are built."""

from __future__ import annotations

import argparse
import json
import os
import re
import signal
import socket
import subprocess
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_CASES = [
    "tests/perf/query_cases/smoke_direct_sst/case.toml",
    "tests/perf/query_cases/prom_remote_write_seeded_random/case.toml",
    "tests/perf/query_cases/prom_remote_write_run_heavy/case.toml",
    "tests/perf/query_cases/prom_remote_write_mixed_every/case.toml",
    "tests/perf/query_cases/prom_remote_write_integer_counter/case.toml",
    "tests/perf/query_cases/promql_range_boundary/case.toml",
]

HEAVY_CASES = [
    "tests/perf/query_cases/prom_remote_write_7913/case.toml",
]

CASE_GROUPS = {
    "all": DEFAULT_CASES,
    "heavy": HEAVY_CASES,
}


def split_cases(values: list[str]) -> list[str]:
    tokens: list[str] = []
    for value in values:
        tokens.extend(part for part in re.split(r"[\s,]+", value.strip()) if part)
    if not tokens:
        return DEFAULT_CASES.copy()
    if "all" in tokens and len(tokens) > 1:
        raise ValueError("'all' cannot be mixed with other case selectors")
    cases: list[str] = []
    for token in tokens:
        cases.extend(CASE_GROUPS.get(token, [token]))
    return list(dict.fromkeys(cases))


def parse_bool(value: str) -> bool:
    return value.lower() in {"1", "true", "yes", "on"}


def profile_dir(cargo_profile: str) -> str:
    if cargo_profile == "dev":
        return "debug"
    return cargo_profile


def configured_path(value: str | None) -> Path | None:
    if not value or not value.strip():
        return None
    return Path(value.strip())


def resolve_case_path(candidate_src: Path, case: str) -> Path:
    path = Path(case)
    if path.is_absolute() or path.parts[:1] == (candidate_src.name,):
        return path
    return candidate_src / path


def case_slug(case_path: Path) -> str:
    raw = case_path.parent.name if case_path.name == "case.toml" else case_path.stem
    return re.sub(r"[^A-Za-z0-9_.-]+", "-", raw).strip("-") or "case"


def append_github_output(path: str | None, status: int) -> None:
    if not path:
        return
    with open(path, "a", encoding="utf-8") as fp:
        fp.write(f"status={status}\n")


def append_step_summary(summary: Path) -> None:
    step_summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if not step_summary or not summary.exists():
        return
    with open(step_summary, "a", encoding="utf-8") as out:
        out.write(summary.read_text())


@dataclass(frozen=True)
class RunTarget:
    name: str
    binary: Path
    work_dir: Path
    http_port: int
    grpc_port: int
    mysql_port: int
    postgres_port: int
    metasrv_rpc_port: int
    metasrv_http_port: int
    datanode_rpc_port: int
    datanode_http_port: int
    datanode_data_dir: Path
    frontend_config: Path | None = None


def allocate_ports(n: int) -> list[int]:
    socks = []
    try:
        for _ in range(n):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(("127.0.0.1", 0))
            socks.append(sock)
        return [sock.getsockname()[1] for sock in socks]
    finally:
        for sock in socks:
            sock.close()


def make_target(
    name: str,
    binary: Path,
    root: Path,
    ports: list[int],
    frontend_config: Path | None = None,
) -> RunTarget:
    work_dir = root / name
    return RunTarget(
        name=name,
        binary=binary,
        work_dir=work_dir,
        http_port=ports[4],
        grpc_port=ports[5],
        mysql_port=ports[6],
        postgres_port=ports[7],
        metasrv_rpc_port=ports[0],
        metasrv_http_port=ports[1],
        datanode_rpc_port=ports[2],
        datanode_http_port=ports[3],
        datanode_data_dir=work_dir / "datanode-0" / "data",
        frontend_config=frontend_config,
    )


def wait_health(port: int, timeout_s: float = 60.0) -> None:
    deadline = time.monotonic() + timeout_s
    last: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=2) as response:
                if response.status < 500:
                    return
        except Exception as err:  # noqa: BLE001 - retain the last health diagnostic
            last = err
        time.sleep(0.5)
    raise TimeoutError(f"health check timed out on port {port}: {last}")


def component_log_dir(target: RunTarget, name: str) -> Path:
    return target.work_dir / "logs" / ("datanode-0" if name == "datanode" else name)


def component_command(target: RunTarget, name: str) -> list[str]:
    log_dir = component_log_dir(target, name)
    if name == "metasrv":
        return [
            str(target.binary), "metasrv", "start",
            "--grpc-bind-addr", f"127.0.0.1:{target.metasrv_rpc_port}",
            "--grpc-server-addr", f"127.0.0.1:{target.metasrv_rpc_port}",
            "--http-addr", f"127.0.0.1:{target.metasrv_http_port}",
            "--backend", "memory-store", "--enable-region-failover", "false",
            "--log-dir", str(log_dir),
        ]
    if name == "datanode":
        return [
            str(target.binary), "datanode", "start",
            "--grpc-bind-addr", f"127.0.0.1:{target.datanode_rpc_port}",
            "--grpc-server-addr", f"127.0.0.1:{target.datanode_rpc_port}",
            "--http-addr", f"127.0.0.1:{target.datanode_http_port}",
            "--data-home", str(target.datanode_data_dir), "--log-dir", str(log_dir),
            "--node-id", "0", "--metasrv-addrs", f"127.0.0.1:{target.metasrv_rpc_port}",
        ]
    if name == "frontend":
        command = [str(target.binary), "frontend", "start"]
        if target.frontend_config is not None:
            command.extend(["--config-file", str(target.frontend_config)])
        command.extend([
            "--metasrv-addrs", f"127.0.0.1:{target.metasrv_rpc_port}",
            "--http-addr", f"127.0.0.1:{target.http_port}",
            "--grpc-bind-addr", f"127.0.0.1:{target.grpc_port}",
            "--grpc-server-addr", f"127.0.0.1:{target.grpc_port}",
            "--mysql-addr", f"127.0.0.1:{target.mysql_port}",
            "--postgres-addr", f"127.0.0.1:{target.postgres_port}",
            "--log-dir", str(log_dir),
        ])
        return command
    raise ValueError(f"unknown component: {name}")


def start_component(
    target: RunTarget,
    name: str,
    procs: dict[tuple[str, str], subprocess.Popen[bytes]],
) -> None:
    if name != "metasrv":
        metasrv = procs.get((target.name, "metasrv"))
        if metasrv is None or metasrv.poll() is not None:
            raise RuntimeError("metasrv exited; memory-store metadata is no longer valid")
    if name == "datanode":
        target.datanode_data_dir.mkdir(parents=True, exist_ok=True)
    logs = component_log_dir(target, name)
    logs.mkdir(parents=True, exist_ok=True)
    with (logs / "stdout.log").open("ab") as out, (logs / "stderr.log").open("ab") as err:
        procs[(target.name, name)] = subprocess.Popen(
            component_command(target, name),
            stdout=out,
            stderr=err,
            start_new_session=True,
        )
    wait_health({
        "metasrv": target.metasrv_http_port,
        "datanode": target.datanode_http_port,
        "frontend": target.http_port,
    }[name])


def stop_component(
    target: RunTarget,
    name: str,
    procs: dict[tuple[str, str], subprocess.Popen[bytes]],
) -> None:
    proc = procs.pop((target.name, name), None)
    if proc is None or proc.poll() is not None:
        return
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        proc.wait(timeout=20)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            return
        proc.wait(timeout=20)


def restart_component(
    target: RunTarget,
    name: str,
    procs: dict[tuple[str, str], subprocess.Popen[bytes]],
) -> None:
    stop_component(target, name, procs)
    start_component(target, name, procs)


def stop_all(targets: list[RunTarget], procs: dict[tuple[str, str], subprocess.Popen[bytes]]) -> None:
    for target in reversed(targets):
        for name in ("frontend", "datanode", "metasrv"):
            stop_component(target, name, procs)


def load_plan(fixture_generator: Path, case_path: Path) -> dict[str, Any]:
    result = subprocess.run(
        [str(fixture_generator), "plan", "--case", str(case_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"query_perf_fixture plan failed: {result.stderr[:2000]}")
    return json.loads(result.stdout)


def run_direct_case(
    args: argparse.Namespace,
    case_path: Path,
    work_dir: Path,
    base_bin: Path,
    candidate_bin: Path,
    fixture_generator: Path,
    runner: Path,
) -> int:
    ports = allocate_ports(16)
    targets = [
        make_target("base", base_bin, work_dir, ports[:8]),
        make_target("candidate", candidate_bin, work_dir, ports[8:]),
    ]
    for target in targets:
        if target.work_dir.exists() and any(target.work_dir.iterdir()):
            raise RuntimeError(f"target work_dir exists and is non-empty: {target.work_dir}")
        target.work_dir.mkdir(parents=True, exist_ok=True)

    procs: dict[tuple[str, str], subprocess.Popen[bytes]] = {}
    try:
        for target in targets:
            start_component(target, "metasrv", procs)
            start_component(target, "datanode", procs)
            start_component(target, "frontend", procs)

        prepare = [
            str(runner), "prepare-direct", "--case", str(case_path),
            "--fixture-generator", str(fixture_generator),
            "--base-http-port", str(targets[0].http_port),
            "--candidate-http-port", str(targets[1].http_port),
            "--fixture-dir", str(work_dir / "fixture"),
            "--output", str(work_dir / "prepare-direct.json"),
            "--http-timeout", str(args.http_timeout),
        ]
        if parse_bool(args.allow_large_fixture):
            prepare.append("--allow-large-fixture")
        prepare_status = subprocess.run(prepare, check=False).returncode
        if prepare_status != 0:
            return prepare_status

        for target in targets:
            stop_component(target, "datanode", procs)

        prepared = json.loads((work_dir / "prepare-direct.json").read_text(encoding="utf-8"))
        fixtures = prepared.get("fixtures")
        if not isinstance(fixtures, list):
            raise RuntimeError("prepare-direct report has no fixtures array")
        for target in targets:
            destination = target.work_dir / "materialize-destination.toml"
            destination.write_text(
                f"data_home = {json.dumps(str(target.datanode_data_dir))}\n"
                "object_store = { type = \"File\" }\n",
                encoding="utf-8",
            )
            for fixture in fixtures:
                fixture_dir = fixture.get("fixture_dir") if isinstance(fixture, dict) else None
                if not isinstance(fixture_dir, str):
                    raise RuntimeError("prepare-direct fixture record has no fixture_dir")
                materialize = [
                    str(runner), "materialize", "--fixture-dir", fixture_dir,
                    "--destination", str(destination),
                ]
                materialize_status = subprocess.run(materialize, check=False).returncode
                if materialize_status != 0:
                    return materialize_status

        for target in targets:
            start_component(target, "datanode", procs)

        measure = [
            str(runner), "measure", "--case", str(case_path),
            "--fixture-generator", str(fixture_generator),
            "--base-http-port", str(targets[0].http_port),
            "--candidate-http-port", str(targets[1].http_port),
            "--output", str(work_dir / "query-regression-report.json"),
            "--http-timeout", str(args.http_timeout),
        ]
        status = subprocess.run(measure, check=False).returncode
        if status != 0:
            for target in targets:
                restart_component(target, "frontend", procs)
            status = subprocess.run(measure, check=False).returncode
        return status
    finally:
        stop_all(targets, procs)


def run_remote_case(
    args: argparse.Namespace,
    case_path: Path,
    work_dir: Path,
    base_bin: Path,
    candidate_bin: Path,
    fixture_generator: Path,
    runner: Path,
) -> int:
    ports = allocate_ports(16)
    targets = [
        make_target(
            "base",
            base_bin,
            work_dir,
            ports[:8],
            work_dir / "base" / "frontend-prom-store.toml",
        ),
        make_target(
            "candidate",
            candidate_bin,
            work_dir,
            ports[8:],
            work_dir / "candidate" / "frontend-prom-store.toml",
        ),
    ]
    for target in targets:
        if target.work_dir.exists() and any(target.work_dir.iterdir()):
            raise RuntimeError(f"target work_dir exists and is non-empty: {target.work_dir}")
        target.work_dir.mkdir(parents=True, exist_ok=True)
        if target.frontend_config is None:
            raise RuntimeError(f"remote target has no frontend config path: {target.name}")
        render = [
            str(runner), "render-remote-config", "--case", str(case_path),
            "--fixture-generator", str(fixture_generator),
            "--output", str(target.frontend_config),
        ]
        render_status = subprocess.run(render, check=False).returncode
        if render_status != 0:
            return render_status

    procs: dict[tuple[str, str], subprocess.Popen[bytes]] = {}
    report = work_dir / "query-regression-report.json"
    try:
        for target in targets:
            start_component(target, "metasrv", procs)
            start_component(target, "datanode", procs)
            start_component(target, "frontend", procs)

        prepare = [
            str(runner), "prepare-remote", "--case", str(case_path),
            "--fixture-generator", str(fixture_generator),
            "--base-http-port", str(targets[0].http_port),
            "--candidate-http-port", str(targets[1].http_port),
            "--output", str(work_dir / "prepare-remote.json"),
            "--http-timeout", str(args.http_timeout),
        ]
        prepare_status = subprocess.run(prepare, check=False).returncode
        if prepare_status != 0:
            return prepare_status

        measure = [
            str(runner), "measure", "--case", str(case_path),
            "--fixture-generator", str(fixture_generator),
            "--base-http-port", str(targets[0].http_port),
            "--candidate-http-port", str(targets[1].http_port),
            "--output", str(report), "--http-timeout", str(args.http_timeout),
        ]
        measure_status = subprocess.run(measure, check=False).returncode
        if measure_status != 0 and not report.exists():
            return measure_status

        for target in targets:
            stop_component(target, "datanode", procs)

        finalize = [
            str(runner), "finalize-remote", "--case", str(case_path),
            "--fixture-generator", str(fixture_generator),
            "--candidate-bin", str(candidate_bin),
            "--base-data-home", str(targets[0].datanode_data_dir),
            "--candidate-data-home", str(targets[1].datanode_data_dir),
            "--report", str(report),
        ]
        finalize_status = subprocess.run(finalize, check=False).returncode
        if finalize_status != 0:
            return finalize_status
        final_report = json.loads(report.read_text(encoding="utf-8"))
        return 1 if measure_status != 0 or final_report.get("status") == "failed" else 0
    finally:
        stop_all(targets, procs)


def run_otlp_case(
    args: argparse.Namespace,
    case_path: Path,
    work_dir: Path,
    base_bin: Path,
    candidate_bin: Path,
    fixture_generator: Path,
    runner: Path,
) -> int:
    if args.otelgen_bin is None:
        raise ValueError("--otelgen-bin is required for otlp_trace_load")
    ports = allocate_ports(16)
    targets = [
        make_target("base", base_bin, work_dir, ports[:8]),
        make_target("candidate", candidate_bin, work_dir, ports[8:]),
    ]
    for target in targets:
        if target.work_dir.exists() and any(target.work_dir.iterdir()):
            raise RuntimeError(f"target work_dir exists and is non-empty: {target.work_dir}")
        target.work_dir.mkdir(parents=True, exist_ok=True)

    procs: dict[tuple[str, str], subprocess.Popen[bytes]] = {}
    try:
        for target in targets:
            try:
                start_component(target, "metasrv", procs)
                start_component(target, "datanode", procs)
                start_component(target, "frontend", procs)
                command = [
                    str(runner), "run-otlp-target", "--case", str(case_path),
                    "--fixture-generator", str(fixture_generator),
                    "--otelgen-bin", str(args.otelgen_bin),
                    "--http-port", str(target.http_port),
                    "--target-name", target.name,
                    "--work-dir", str(target.work_dir),
                    "--output", str(target.work_dir / "report.json"),
                    "--http-timeout", str(args.http_timeout),
                ]
                status = subprocess.run(command, check=False).returncode
            finally:
                stop_all([target], procs)
            if status != 0:
                return status

        output = work_dir / "query-regression-report.json"
        finalize = [
            str(runner), "finalize-otlp", "--case", str(case_path),
            "--fixture-generator", str(fixture_generator),
            "--base-result", str(targets[0].work_dir / "report.json"),
            "--candidate-result", str(targets[1].work_dir / "report.json"),
            "--output", str(output),
        ]
        status = subprocess.run(finalize, check=False).returncode
        if status != 0:
            return status
        report = json.loads(output.read_text(encoding="utf-8"))
        return 1 if report.get("status") == "failed" else 0
    finally:
        stop_all(targets, procs)


def run_case(args: argparse.Namespace, case_path: Path, work_dir: Path) -> int:
    target_dir = profile_dir(args.cargo_profile)
    base_bin = args.base_bin or args.base_src / "target" / target_dir / "greptime"
    candidate_bin = args.candidate_bin or args.candidate_src / "target" / target_dir / "greptime"
    fixture_generator = args.fixture_generator or args.candidate_src / "target" / target_dir / "query_perf_fixture"
    runner = args.runner or args.candidate_src / "target" / target_dir / "query_regression_runner"
    plan = load_plan(fixture_generator, case_path)
    scenario = plan.get("scenario")
    kind = scenario.get("kind") if isinstance(scenario, dict) else None
    print(f"::group::Query regression case: {case_path}", flush=True)
    try:
        if kind == "direct_readable_sst":
            return run_direct_case(
                args,
                case_path,
                work_dir,
                base_bin,
                candidate_bin,
                fixture_generator,
                runner,
            )
        if kind == "prom_remote_write_then_query":
            return run_remote_case(
                args,
                case_path,
                work_dir,
                base_bin,
                candidate_bin,
                fixture_generator,
                runner,
            )
        if kind == "otlp_trace_load":
            return run_otlp_case(
                args,
                case_path,
                work_dir,
                base_bin,
                candidate_bin,
                fixture_generator,
                runner,
            )
        raise ValueError(
            f"unsupported scenario kind {kind!r}; supported: "
            "'direct_readable_sst', 'prom_remote_write_then_query', 'otlp_trace_load'"
        )
    finally:
        print("::endgroup::", flush=True)


def write_summary(args: argparse.Namespace, reports: list[Path]) -> int:
    cmd = ["uv", "run", "--no-project", "python", str(args.summary_script)]
    for report in reports:
        cmd.extend(["--report", str(report)])
    cmd.extend(
        [
            "--run-url",
            args.run_url,
            "--case-name",
            args.case_name,
            "--base-ref",
            args.base_ref,
            "--candidate-ref",
            args.candidate_ref,
            "--output",
            str(args.summary_output),
        ]
    )
    return subprocess.run(cmd, check=False).returncode


def write_failed_report(work_dir: Path, case_path: Path, error: Exception) -> None:
    work_dir.mkdir(parents=True, exist_ok=True)
    path = work_dir / "query-regression-report.json"
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(report, dict) or not isinstance(report.get("targets"), list) or not isinstance(report.get("thresholds"), list):
            raise ValueError("existing report is malformed")
    except (OSError, ValueError, json.JSONDecodeError):
        report = {
            "case_path": str(case_path),
            "targets": [],
            "thresholds": [],
        }
    report["status"] = "failed"
    report["error"] = repr(error)
    path.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cases", action="append", help="'all', 'heavy', or comma/space separated case paths")
    parser.add_argument("--base-src", type=Path, default=Path("base-src"))
    parser.add_argument("--candidate-src", type=Path, default=Path("candidate-src"))
    parser.add_argument("--base-bin", type=Path, default=configured_path(os.environ.get("BASE_BIN")))
    parser.add_argument("--candidate-bin", type=Path, default=configured_path(os.environ.get("CANDIDATE_BIN")))
    parser.add_argument(
        "--fixture-generator",
        type=Path,
        default=configured_path(os.environ.get("FIXTURE_GENERATOR")),
    )
    parser.add_argument("--otelgen-bin", type=Path, default=configured_path(os.environ.get("OTELGEN_BIN")))
    parser.add_argument("--runner", type=Path, default=configured_path(os.environ.get("QUERY_REGRESSION_RUNNER")))
    parser.add_argument("--cargo-profile", default=os.environ.get("CARGO_PROFILE", "nightly"))
    parser.add_argument("--work-dir", default=Path("query-regression-work"), type=Path)
    parser.add_argument("--http-timeout", default=os.environ.get("HTTP_TIMEOUT", "300"))
    parser.add_argument("--allow-large-fixture", default=os.environ.get("ALLOW_LARGE_FIXTURE", "false"))
    parser.add_argument(
        "--summary-script",
        type=Path,
        default=Path("candidate-src/.github/scripts/query-regression-summary.py"),
    )
    parser.add_argument("--summary-output", type=Path, default=Path("query-regression-summary.md"))
    parser.add_argument("--run-url", default=os.environ.get("RUN_URL", ""))
    parser.add_argument("--case-name", default=os.environ.get("CASE_NAME", "default case set"))
    parser.add_argument("--base-ref", default=os.environ.get("BASE_REF", ""))
    parser.add_argument("--candidate-ref", default=os.environ.get("CANDIDATE_REF", ""))
    parser.add_argument("--github-output", default=os.environ.get("GITHUB_OUTPUT"))
    args = parser.parse_args()
    try:
        cases = split_cases(args.cases or [os.environ.get("CASE_PATHS", "all")])
    except ValueError as err:
        print(f"error: {err}", flush=True)
        append_github_output(args.github_output, 1)
        return 0

    reports: list[Path] = []
    status = 0
    for case in cases:
        case_path = resolve_case_path(args.candidate_src, case)
        work_dir = args.work_dir / case_slug(case_path)
        reports.append(work_dir / "query-regression-report.json")
        try:
            case_status = run_case(args, case_path, work_dir)
        except Exception as err:  # noqa: BLE001 - preserve an aggregate report for the summary step
            print(f"error: query regression case {case_path}: {err}", flush=True)
            write_failed_report(work_dir, case_path, err)
            case_status = 1
        if case_status != 0:
            status = case_status or 1

    summary_status = write_summary(args, reports)
    if summary_status != 0 and status == 0:
        status = summary_status
    append_step_summary(args.summary_output)
    append_github_output(args.github_output, status)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
