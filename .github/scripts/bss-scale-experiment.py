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

"""Run the isolated default-versus-BSS scale experiment on an external runner."""

import argparse
import datetime
import hashlib
import importlib.util
import json
import math
import os
import re
import resource
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
RUNNER_PATH = ROOT / ".github/scripts/query-regression-run.py"
SOURCE_CASE = ROOT / "tests/perf/query_cases/sst_float_bss/case.toml"
SCALES = (1, 10, 50)
ORDERS = (("base", "candidate"), ("candidate", "base"), ("base", "candidate"))
ANSI = re.compile(r"\x1b\[[0-9;]*m")
AVERAGE = re.compile(r"Average:.*?(\d+)\s+rows.*?in\s+([0-9.]+)\s*(ns|µs|us|ms|s)", re.I)
ITERATION = re.compile(
    r"(?:Iteration\s+(\d+)\s*:\s*|\[iter\s+(\d+)\]\s+)(\d+)\s+rows.*?\sin\s+([0-9.]+)\s*(ns|µs|us|ms|s)",
    re.I,
)


def load_outer_runner() -> Any:
    spec = importlib.util.spec_from_file_location("bss_query_regression_outer", RUNNER_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot import {RUNNER_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


outer = load_outer_runner()


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def required_env(name: str) -> Path:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is required")
    path = Path(value)
    if not path.exists():
        raise RuntimeError(f"{name} does not exist: {path}")
    return path


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for block in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def scale_end_date(scale: int) -> str:
    return (datetime.date(2024, 1, 1) + datetime.timedelta(days=3 * scale)).isoformat()


def scaled_case(source: str, scale: int) -> str:
    replacements = {
        "samples_per_series = 4320": f"samples_per_series = {4320 * scale}",
        "timeout_seconds = 600": "timeout_seconds = 3600",
        "visibility_timeout_seconds = 300": "visibility_timeout_seconds = 600",
        "'compaction.twcs.trigger_file_num'='100'": "'compaction.twcs.trigger_file_num'='10000'",
        "iterations = 7": "iterations = 1",  # Native finalization only seeds commands.
        "2024-01-04 00:00:00": f"{scale_end_date(scale)} 00:00:00",
    }
    for old, new in replacements.items():
        expected = 2 if "trigger_file_num" in old else 1
        if source.count(old) != expected:
            raise RuntimeError(f"source case replacement is not unique: {old}")
        source = source.replace(old, new)
    return source


def expected_rows(scale: int) -> int:
    return 1000 * 4320 * scale


def row_denominator(rows: int, iterations: int) -> int:
    return rows * iterations


def paired_orders() -> list[tuple[str, str]]:
    return list(ORDERS)


def parse_proc_cpu_seconds(stat: str, ticks: int) -> float:
    """Parse user plus system CPU seconds from a Linux ``/proc/<pid>/stat`` record."""
    fields = stat[stat.rfind(")") + 2 :].split()
    return (int(fields[11]) + int(fields[12])) / ticks


def target_cpu_seconds(target: str, procs: dict[tuple[str, str], Any]) -> float:
    processes = [
        process for (name, component), process in procs.items()
        if name == target and component in ("frontend", "datanode")
    ]
    if len(processes) != 2 or any(process.poll() is not None for process in processes):
        raise RuntimeError(f"{target} frontend and datanode must remain live during SQL measurement")
    ticks = os.sysconf("SC_CLK_TCK")
    return sum(
        parse_proc_cpu_seconds(Path(f"/proc/{process.pid}/stat").read_text(encoding="utf-8"), ticks)
        for process in processes
    )


def success_code(value: Any) -> bool:
    return str(value).lower() in ("", "0", "success", "none")


def response_has_error(body: Any) -> bool:
    if not isinstance(body, dict):
        return True
    if "code" in body and not success_code(body["code"]):
        return True
    return any(body.get(key) not in (None, "", False, 0, "0", "success") for key in ("error", "err_msg", "error_msg"))


def row_values(row: Any) -> list[Any]:
    if isinstance(row, dict):
        if "time_window" in row:
            sums = [value for name, value in row.items() if name != "time_window"]
            return [row["time_window"], *sums]
        return list(row.values())
    if isinstance(row, list):
        return row
    raise RuntimeError(f"unexpected SQL row: {row!r}")


def numeric_row(row: Any) -> bool:
    values = row_values(row)
    if not values:
        return False
    value = values[-1]
    if isinstance(value, bool) or value is None:
        return False
    if isinstance(value, (int, float)):
        return math.isfinite(float(value))
    if isinstance(value, str):
        try:
            return math.isfinite(float(value))
        except ValueError:
            return False
    return False


def hourly_timestamp(value: Any) -> datetime.datetime | None:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if not math.isfinite(value):
            return None
        try:
            return datetime.datetime(1970, 1, 1) + datetime.timedelta(milliseconds=value)
        except OverflowError:
            return None
    if not isinstance(value, str):
        return None
    try:
        timestamp = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if timestamp.tzinfo is not None:
        timestamp = timestamp.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    if timestamp.minute or timestamp.second or timestamp.microsecond:
        return None
    return timestamp


def response_rows(body: Any, expected: int) -> list[Any]:
    """Extract exactly one supported Greptime JSON result, never a wrapper list."""
    if not isinstance(body, dict):
        raise RuntimeError("SQL response must be an object")
    if "output" in body:
        output = body["output"]
        if not isinstance(output, list) or len(output) != 1 or not isinstance(output[0], dict):
            raise RuntimeError("SQL output must contain exactly one result")
        records = output[0].get("records")
        rows = records.get("rows") if isinstance(records, dict) else None
    else:
        rows = body.get("data")
    if not isinstance(rows, list) or len(rows) != expected:
        raise RuntimeError(f"SQL response has {len(rows) if isinstance(rows, list) else 'no'} rows; expected {expected}")
    if not all(numeric_row(row) for row in rows):
        raise RuntimeError("SQL result has a row without a finite, non-boolean numeric sum")
    if expected > 1:
        first_hour = datetime.datetime(2024, 1, 1)
        for index, row in enumerate(rows):
            values = row_values(row)
            if len(values) != 2 or hourly_timestamp(values[0]) != first_hour + datetime.timedelta(hours=index):
                raise RuntimeError("SQL hourly result must contain sequential Jan 1 hourly timestamp and sum rows")
    return rows


def post_sql(port: int, sql: str, timeout: float) -> dict[str, Any]:
    data = urllib.parse.urlencode({"sql": sql, "db": "public", "format": "json"}).encode()
    request = urllib.request.Request(f"http://127.0.0.1:{port}/v1/sql", data=data, method="POST")
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            status, raw = response.status, response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as error:
        raise RuntimeError(f"SQL HTTP {error.code}: {error.read().decode(errors='replace')[:2000]}") from error
    except urllib.error.URLError as error:
        raise RuntimeError(f"SQL request failed: {error}") from error
    try:
        body = json.loads(raw)
    except json.JSONDecodeError as error:
        raise RuntimeError(f"SQL response is not JSON: {raw[:2000]}") from error
    if status >= 400 or response_has_error(body):
        raise RuntimeError(f"SQL response error: {json.dumps(body)[:2000]}")
    return {"status": status, "wall_seconds": time.perf_counter() - started, "response": body}


def same_result(left: Any, right: Any, tolerance: float = 1e-9) -> bool:
    if isinstance(left, bool) or isinstance(right, bool):
        return left == right
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return math.isclose(float(left), float(right), rel_tol=tolerance, abs_tol=tolerance)
    if isinstance(left, str) and isinstance(right, str):
        try:
            return math.isclose(float(left), float(right), rel_tol=tolerance, abs_tol=tolerance)
        except ValueError:
            return left == right
    if type(left) is not type(right):
        return False
    if isinstance(left, list):
        return len(left) == len(right) and all(same_result(a, b, tolerance) for a, b in zip(left, right))
    if isinstance(left, dict):
        return left.keys() == right.keys() and all(same_result(left[k], right[k], tolerance) for k in left)
    return left == right


def sql_round(target: Any, procs: dict[tuple[str, str], Any], queries: list[tuple[str, str, int]], input_rows: int) -> dict[str, Any]:
    measurements = []
    for name, sql, returned_rows in queries:
        for _ in range(3):
            response_rows(post_sql(target.http_port, sql, 3600)["response"], returned_rows)
        started, before = time.perf_counter(), target_cpu_seconds(target.name, procs)
        samples = []
        for _ in range(15):
            sample = post_sql(target.http_port, sql, 3600)
            sample["rows"] = response_rows(sample["response"], returned_rows)
            samples.append(sample)
        cpu_seconds = target_cpu_seconds(target.name, procs) - before
        denominator = row_denominator(input_rows, len(samples))
        measurements.append({
            "name": name, "warmup": 3, "iterations": 15, "returned_rows_per_query": returned_rows,
            "input_rows_per_query": input_rows, "samples": samples,
            "block_wall_seconds": time.perf_counter() - started, "block_cpu_seconds": cpu_seconds,
            "cpu_ns_per_input_row": cpu_seconds / denominator * 1e9,
        })
    return {"target": target.name, "measurements": measurements}


def compare_sql_pair(round_data: dict[str, Any]) -> None:
    left, right = round_data["targets"]
    for lhs, rhs in zip(left["measurements"], right["measurements"]):
        if lhs["name"] != rhs["name"]:
            raise RuntimeError("target query ordering differs")
        for a, b in zip(lhs["samples"], rhs["samples"]):
            if not same_result(a["rows"], b["rows"]):
                raise RuntimeError(f"SQL results differ for {lhs['name']}")
    round_data["result_comparison"] = "equal within numeric tolerance; not bitwise comparison"


def milliseconds(value: str, unit: str) -> float:
    return float(value) * {"ns": 1e-6, "µs": 1e-3, "us": 1e-3, "ms": 1.0, "s": 1000.0}[unit.lower()]


def parse_average(stdout: str) -> tuple[int, float] | None:
    match = AVERAGE.search(ANSI.sub("", stdout))
    return None if match is None else (int(match[1]), milliseconds(match[2], match[3]))


def parse_iterations(stdout: str, rows: int, iterations: int) -> list[float]:
    parsed: dict[int, tuple[int, float]] = {}
    for match in ITERATION.finditer(ANSI.sub("", stdout)):
        index = int(match[1] or match[2])
        value_rows = int(match[3])
        if index in parsed:
            raise RuntimeError(f"duplicate reader iteration {index}")
        parsed[index] = (value_rows, milliseconds(match[4], match[5]))
    if set(parsed) != set(range(1, iterations + 1)):
        raise RuntimeError(f"reader iterations {sorted(parsed)} do not equal 1..{iterations}")
    if any(value_rows != rows for value_rows, _ in parsed.values()):
        raise RuntimeError(f"reader reported partial rows: {parsed}")
    return [parsed[index][1] for index in range(1, iterations + 1)]


def command_with_iterations(command: list[str], iterations: int) -> list[str]:
    result = command.copy()
    result[result.index("--iterations") + 1] = str(iterations)
    return result


def reader_run(command: list[str], cpu: int, rows: int, iterations: int) -> dict[str, Any]:
    prefixed = ["taskset", "-c", str(cpu), *command]
    environment = {**os.environ, "RAYON_NUM_THREADS": "1"}
    before, started = resource.getrusage(resource.RUSAGE_CHILDREN), time.perf_counter()
    result = subprocess.run(prefixed, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=environment, check=False, timeout=3600)
    after = resource.getrusage(resource.RUSAGE_CHILDREN)
    durations = parse_iterations(result.stdout, rows, iterations) if result.returncode == 0 else []
    cpu_seconds = (after.ru_utime - before.ru_utime) + (after.ru_stime - before.ru_stime)
    record = {
        "command": prefixed, "returncode": result.returncode, "stdout": result.stdout, "stderr": result.stderr,
        "wall_seconds": time.perf_counter() - started, "cpu_seconds": cpu_seconds,
        "rows_per_iteration": rows, "iterations": iterations, "iteration_ms": durations,
        "average_ms_from_iterations": sum(durations) / len(durations) if durations else None,
        "cpu_ns_per_input_row": cpu_seconds / row_denominator(rows, iterations) * 1e9,
        "cpu_note": "RUSAGE_CHILDREN includes whole reader child startup and work, not pure decoder CPU",
    }
    if result.returncode != 0:
        raise RuntimeError(f"reader command failed: {record}")
    return record


def storage_entry(report: dict[str, Any], name: str) -> dict[str, Any]:
    return next(target for target in report["targets"] if target["name"] == name)


def footer_and_templates(report: dict[str, Any], name: str, expected: int) -> tuple[list[dict[str, Any]], dict[str, int]]:
    entry = storage_entry(report, name)
    inspection = entry["storage_inspection"]["summary"]
    summary, files = inspection["summary"], inspection["files"]
    if (summary["total_rows"] != expected or not files
            or summary["file_count"] != len(files)
            or sum(file["num_rows"] for file in files) != expected):
        raise RuntimeError(
            f"{name} footer rows/files are incomplete: rows={summary['total_rows']}, files={len(files)}"
        )
    for file in files:
        columns = [column for column in file["columns"] if column["column_path"] == "greptime_value"]
        if not columns:
            raise RuntimeError(f"{name} SST lacks greptime_value footer: {file['relative_path']}")
        for column in columns:
            encodings = [encoding.upper() for encoding in column["encodings"]]
            if name == "candidate":
                if "BYTE_STREAM_SPLIT" not in encodings or "RLE_DICTIONARY" in encodings or "PLAIN_DICTIONARY" in encodings:
                    raise RuntimeError(f"candidate SST is not BSS/non-dictionary: {file['relative_path']}")
            elif "BYTE_STREAM_SPLIT" in encodings:
                raise RuntimeError(f"default SST unexpectedly has BSS: {file['relative_path']}")
    templates = entry["read_bench"]["parquetbench"] + entry["read_bench"]["scanbench"]
    if not templates:
        raise RuntimeError(f"{name} finalization produced no reader commands")
    return templates, {"file_count": len(files), "total_rows": summary["total_rows"]}


def command_rows(run: dict[str, Any], files: dict[str, int]) -> int:
    paths = run.get("files") or [run["relative_path"]]
    matches = [value for path, value in files.items() if any(path.endswith(str(item)) or str(item).endswith(path) for item in paths)]
    if not matches:
        raise RuntimeError(f"cannot map benchmark files to footer rows: {paths}")
    return sum(matches)


def verify_native_template(template: dict[str, Any], rows: int) -> None:
    values = parse_iterations(template["stdout"], rows, 1)
    average = parse_average(template["stdout"])
    if average is not None and (average[0] != rows or not math.isclose(average[1], values[0], rel_tol=1e-6)):
        raise RuntimeError("native reader average does not match its iteration line")


def aggregate_runs(runs: list[dict[str, Any]]) -> dict[str, Any]:
    totals: dict[str, dict[str, Any]] = {}
    for run in runs:
        measured = run["measured"]
        total = totals.setdefault(run["kind"], {"commands": 0, "input_rows_per_iteration": 0, "whole_wall_seconds": 0.0, "cpu_seconds": 0.0, "native_average_ms_sum": 0.0})
        total["commands"] += 1
        total["input_rows_per_iteration"] += measured["rows_per_iteration"]
        total["whole_wall_seconds"] += measured["wall_seconds"]
        total["cpu_seconds"] += measured["cpu_seconds"]
        total["native_average_ms_sum"] += measured["average_ms_from_iterations"]
    for total in totals.values():
        total["cpu_ns_per_input_row"] = total["cpu_seconds"] / row_denominator(total["input_rows_per_iteration"], 7) * 1e9
    return totals


def hashes(targets: list[Any]) -> dict[str, str]:
    result = {}
    for target in targets:
        for path in sorted(target.datanode_data_dir.rglob("*.parquet")):
            result[f"{target.name}/{path.relative_to(target.datanode_data_dir)}"] = sha256(path)
    return result


def context(path: Path) -> dict[str, Any]:
    return {"disk_usage": shutil.disk_usage(path)._asdict(), "meminfo": Path("/proc/meminfo").read_text(encoding="utf-8")}


def runner_command(runner: Path, phase: str, case: Path, fixture: Path, targets: list[Any], output: Path | None = None) -> list[str]:
    command = [str(runner), phase, "--case", str(case), "--fixture-generator", str(fixture)]
    if phase in ("prepare-remote", "measure"):
        command += ["--base-http-port", str(targets[0].http_port), "--candidate-http-port", str(targets[1].http_port), "--http-timeout", "3600"]
    return command + ([] if output is None else ["--output", str(output)])


def run_scale(scale: int, root: Path, fixture: Path, runner: Path, base_bin: Path, candidate_bin: Path, provenance: dict[str, Any]) -> None:
    work = root / f"scale-{scale}x"
    if work.exists():
        raise RuntimeError(f"refusing existing experiment directory: {work}")
    work.mkdir(parents=True)
    write_json(work / "context-before-scale.json", context(root))
    case = work / "case.toml"
    case.write_text(scaled_case(SOURCE_CASE.read_text(encoding="utf-8"), scale), encoding="utf-8")
    outer.load_plan(fixture, case)
    ports = outer.allocate_ports(16)
    targets = [
        outer.make_target("base", candidate_bin, work, ports[:8], work / "base/frontend-prom-store.toml"),
        outer.make_target("candidate", candidate_bin, work, ports[8:], work / "candidate/frontend-prom-store.toml"),
    ]
    for target in targets:
        target.work_dir.mkdir(parents=True)
        subprocess.run([str(runner), "render-remote-config", "--case", str(case), "--fixture-generator", str(fixture), "--output", str(target.frontend_config)], check=True, timeout=3600)
    procs: dict[tuple[str, str], Any] = {}
    report_path = work / "native-report.json"
    try:
        for target in targets:
            for component in ("metasrv", "datanode", "frontend"):
                outer.start_component(target, component, procs)
        subprocess.run(runner_command(runner, "prepare-remote", case, fixture, targets, work / "prepare-remote.json"), check=True, timeout=3600)
        subprocess.run(runner_command(runner, "measure", case, fixture, targets, report_path), check=False, timeout=3600)
        report = json.loads(report_path.read_text(encoding="utf-8"))
        if any(target.get("status") != "measured" for target in report.get("targets", [])):
            raise RuntimeError("native measure reported a target error")
        report["bss_scale_experiment"] = {"native_measurement": "one runner measure only; not a three-round result"}
        write_json(report_path, report)
        queries = [
            ("sum_all_values", "SELECT sum(greptime_value) FROM sst_float_bss", 1),
            ("hourly_sum_values", f"SELECT date_bin(INTERVAL '1 hour', greptime_timestamp) AS time_window, sum(greptime_value) FROM sst_float_bss WHERE greptime_timestamp >= TIMESTAMP '2024-01-01 00:00:00' AND greptime_timestamp < TIMESTAMP '{scale_end_date(scale)} 00:00:00' GROUP BY time_window ORDER BY time_window", 72 * scale),
        ]
        sql_rounds: list[dict[str, Any]] = []
        for number, order in enumerate(paired_orders(), 1):
            pair = {"round": number, "order": list(order), "targets": [sql_round(next(t for t in targets if t.name == name), procs, queries, expected_rows(scale)) for name in order], "cpu_note": "frontend+datanode CPU includes background work; it is not pure decode CPU"}
            compare_sql_pair(pair)
            sql_rounds.append(pair)
            write_json(work / "sql-rounds.json", sql_rounds)
            print(f"scale={scale} round={number} SQL order={'/'.join(order)}", flush=True)
        outer.stop_all(targets, procs)
        finalized = subprocess.run([str(runner), "finalize-remote", "--case", str(case), "--fixture-generator", str(fixture), "--candidate-bin", str(candidate_bin), "--base-data-home", str(targets[0].datanode_data_dir), "--candidate-data-home", str(targets[1].datanode_data_dir), "--report", str(report_path)], check=False, timeout=3600)
        report = json.loads(report_path.read_text(encoding="utf-8"))
        templates, summaries = {}, {}
        for name in ("base", "candidate"):
            templates[name], summaries[name] = footer_and_templates(report, name, expected_rows(scale))
        before = hashes(targets)
        write_json(work / "hashes-before-reader.json", before)
        reader_rounds: list[dict[str, Any]] = []
        cpu = min(os.sched_getaffinity(0))
        for number, order in enumerate(paired_orders(), 1):
            pair = {"round": number, "order": list(order), "targets": []}
            for name in order:
                entry = storage_entry(report, name)
                files = {file["relative_path"]: file["num_rows"] for file in entry["storage_inspection"]["summary"]["files"]}
                runs = []
                for template in templates[name]:
                    rows, command = command_rows(template, files), template["command"]
                    verify_native_template(template, rows)
                    runs.append({"kind": "parquetbench" if "--file-id" in command else "scanbench", "prewarm": reader_run(command_with_iterations(command, 1), cpu, rows, 1), "measured": reader_run(command_with_iterations(command, 7), cpu, rows, 7)})
                parquet_rows = sum(run["measured"]["rows_per_iteration"] for run in runs if run["kind"] == "parquetbench")
                scan_rows = sum(run["measured"]["rows_per_iteration"] for run in runs if run["kind"] == "scanbench")
                if parquet_rows != expected_rows(scale) or scan_rows != expected_rows(scale):
                    raise RuntimeError(f"{name} reader coverage is incomplete")
                pair["targets"].append({"target": name, "file_count": summaries[name]["file_count"], "total_row_count": summaries[name]["total_rows"], "runs": runs, "totals": aggregate_runs(runs)})
            reader_rounds.append(pair)
            write_json(work / "reader-rounds.json", reader_rounds)
            print(f"scale={scale} round={number} reader order={'/'.join(order)}", flush=True)
        after = hashes(targets)
        write_json(work / "hashes-after-reader.json", after)
        if before != after:
            raise RuntimeError("reader mutated SST files")
        write_json(work / "metrics.json", {"scale": scale, "expected_rows": expected_rows(scale), "context_after_scale": context(root), "provenance": provenance, "footer_summaries": summaries, "native_report": str(report_path), "finalize_returncode": finalized.returncode, "finalize_status": report.get("status"), "reader_hashes_equal": True, "limitations": ["SST hashes guard reader immutability only, not target equality", "SQL result comparison is numeric-tolerance based, not bitwise", "no cold-cache claim is made", "ru_maxrss is omitted because it is a global maximum, not per-child RSS"]})
    finally:
        outer.stop_all(targets, procs)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-root", type=Path, default=Path("query-regression-work/bss-scale"))
    args = parser.parse_args()
    args.work_root.mkdir(parents=True, exist_ok=True)
    provenance = {"source_branch": os.environ.get("SOURCE_BRANCH", os.environ.get("GITHUB_REF_NAME", "")), "source_sha": os.environ.get("SOURCE_SHA", os.environ.get("GITHUB_SHA", "")), "base_bin_env": os.environ.get("BASE_BIN", ""), "candidate_bin": os.environ.get("CANDIDATE_BIN", ""), "targets_use_same_candidate_binary": True}
    write_json(args.work_root / "provenance.json", provenance)
    fixture, runner = required_env("FIXTURE_GENERATOR"), required_env("QUERY_REGRESSION_RUNNER")
    base_bin, candidate_bin = required_env("BASE_BIN"), required_env("CANDIDATE_BIN")
    if shutil.which("taskset") is None:
        raise RuntimeError("taskset is required to pin reader processes")
    provenance.update({"base_bin_env": str(base_bin), "candidate_bin": str(candidate_bin), "candidate_bin_sha256": sha256(candidate_bin)})
    write_json(args.work_root / "provenance.json", provenance)
    failures = []
    for scale in SCALES:
        try:
            run_scale(scale, args.work_root, fixture, runner, base_bin, candidate_bin, provenance)
        except Exception as error:  # noqa: BLE001 - retain per-scale artifacts and continue.
            failures.append({"scale": scale, "error": str(error)})
            work = args.work_root / f"scale-{scale}x"
            work.mkdir(parents=True, exist_ok=True)
            write_json(work / "error.json", failures[-1])
            print(f"scale={scale} failed: {error}", flush=True)
    if failures:
        write_json(args.work_root / "errors.json", failures)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
