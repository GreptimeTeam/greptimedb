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

"""Manual perf suite for MetricEngine value-column Parquet encoding modes.

This is intentionally not part of the default query-regression case set. It is a
one-command local suite for comparing `plain`, `no_dictionary`, and
`byte_stream_split` on generated direct-readable Mito SST fixtures, followed by
`parquetbench` and `scanbench`.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import textwrap
import time
from pathlib import Path
from typing import Any


MODES = ("plain", "no_dictionary", "byte_stream_split")
AVERAGE_RE = re.compile(
    r"(?:ℹ\s+)?(?:Summary\s+)?Average:.*?\bin\s+([0-9.]+)(ns|µs|us|ms|s)\s+over"
)
SINGLE_ITERATION_RE = re.compile(
    r"(?:Iteration 1:|\[iter 1\]).*?\bin\s+([0-9.]+)(ns|µs|us|ms|s)\b"
)


def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def run(cmd: list[str], *, cwd: Path, log: Path | None = None) -> dict[str, Any]:
    started = time.monotonic()
    proc = subprocess.run(cmd, cwd=cwd, text=True, capture_output=True, check=False)
    elapsed_s = time.monotonic() - started
    result = {
        "cmd": cmd,
        "returncode": proc.returncode,
        "elapsed_s": elapsed_s,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
    }
    if log is not None:
        log.parent.mkdir(parents=True, exist_ok=True)
        log.write_text(
            "$ " + " ".join(cmd) + "\n\n"
            + "# stdout\n"
            + proc.stdout
            + "\n# stderr\n"
            + proc.stderr
        )
    if proc.returncode != 0:
        raise RuntimeError(
            f"command failed ({proc.returncode}): {' '.join(cmd)}\n"
            f"stdout:\n{proc.stdout[-2000:]}\n"
            f"stderr:\n{proc.stderr[-2000:]}"
        )
    return result


def logged_result(result: dict[str, Any], log: Path) -> dict[str, Any]:
    entry = {
        "cmd": result["cmd"],
        "returncode": result["returncode"],
        "elapsed_s": result["elapsed_s"],
        "log": str(log),
    }
    average_s = parse_benchmark_average_s(result["stdout"])
    if average_s is not None:
        entry["average_s"] = average_s
        entry["average_ms"] = average_s * 1000
    return entry


def parse_benchmark_average_s(output: str) -> float | None:
    match = AVERAGE_RE.search(output) or SINGLE_ITERATION_RE.search(output)
    if match is None:
        return None
    value = float(match.group(1))
    unit = match.group(2)
    if unit == "ns":
        return value / 1_000_000_000
    if unit in ("µs", "us"):
        return value / 1_000_000
    if unit == "ms":
        return value / 1_000
    if unit == "s":
        return value
    raise AssertionError(f"unknown duration unit {unit}")


def format_average_ms(result: dict[str, Any]) -> str:
    average_ms = result.get("average_ms")
    if average_ms is None:
        return ""
    return f"{average_ms:.3f}"


def cargo_target_dir(repo: Path) -> Path:
    metadata = run(
        ["cargo", "metadata", "--format-version", "1", "--no-deps"],
        cwd=repo,
    )
    return Path(json.loads(metadata["stdout"])["target_directory"])


def write_case(
    path: Path,
    *,
    mode: str,
    rows_per_sst: int,
    sst_count: int,
    row_group_size: int,
    series_count: int,
    source_batch_rows: int,
) -> None:
    table_name = f"metric_value_encoding_{mode}"
    path.write_text(
        textwrap.dedent(
            f"""
            [case]
            name = "{table_name}"
            description = "Manual MetricEngine value encoding perf fixture: {mode}"

            [scenario]
            kind = "direct_readable_sst"
            seed = 8414

            [[scenario.tables]]
            database = "public"
            name = "{table_name}"
            engine = "mito"
            append_mode = true
            sst_format = "flat"
            metric_physical = true
            metric_engine_value_encoding = "{mode}"
            primary_key = ["job"]
            time_index = "greptime_timestamp"

            [[scenario.tables.columns]]
            name = "job"
            type = "STRING"
            semantic = "tag"
            distribution = {{ kind = "cardinality", values = {series_count}, prefix = "job_" }}

            [[scenario.tables.columns]]
            name = "greptime_value"
            type = "DOUBLE"
            semantic = "field"
            distribution = {{ kind = "metric_signal", min = 0.0, max = 1000.0 }}

            [[scenario.tables.columns]]
            name = "greptime_timestamp"
            type = "TIMESTAMP(3)"
            semantic = "timestamp"

            [scenario.layout]
            regions = 1
            sst_count = {sst_count}
            rows_per_sst = {rows_per_sst}
            source_batch_rows = {source_batch_rows}
            row_group_size = {row_group_size}
            series_count = {series_count}
            start_unix_nanos = 1704067200000000000
            step_nanos = 1000000000
            time_range_layout = "non_overlapping_per_sst"
            series_layout = "round_robin"
            """
        ).strip()
        + "\n"
    )


def write_bench_config(path: Path, data_home: Path) -> None:
    path.write_text(
        textwrap.dedent(
            f"""
            [storage]
            data_home = "{data_home}"
            type = "File"

            [[region_engine]]
            [region_engine.mito]
            """
        ).strip()
        + "\n"
    )


def copy_tree_contents(src: Path, dst: Path) -> None:
    dst.mkdir(parents=True, exist_ok=True)
    for child in src.iterdir():
        target = dst / child.name
        if child.is_dir():
            shutil.copytree(child, target, dirs_exist_ok=True)
        else:
            shutil.copy2(child, target)


def materialize_fixture(fixture_dir: Path, data_home: Path) -> dict[str, Any]:
    summary = json.loads((fixture_dir / "summary.json").read_text())
    if data_home.exists():
        shutil.rmtree(data_home)
    data_home.mkdir(parents=True, exist_ok=True)

    copy_tree_contents(fixture_dir / "object-store", data_home)
    region_dir = summary["region_dir"].strip("/")
    manifest_dir = data_home / region_dir / "manifest"
    if manifest_dir.exists():
        shutil.rmtree(manifest_dir)
    copy_tree_contents(fixture_dir / "manifest", manifest_dir)
    return summary


def read_first_file(files_jsonl: Path) -> dict[str, Any]:
    with files_jsonl.open() as f:
        for line in f:
            line = line.strip()
            if line:
                return json.loads(line)
    raise RuntimeError(f"no SST entries in {files_jsonl}")


def write_scan_configs(work_dir: Path) -> dict[str, Path]:
    configs = {
        "value": {"projection_names": ["greptime_value"]},
        "timestamp_value": {"projection_names": ["greptime_timestamp", "greptime_value"]},
    }
    paths = {}
    for name, config in configs.items():
        path = work_dir / "scan-configs" / f"{name}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(config, indent=2) + "\n")
        paths[name] = path
    return paths


def summarize_metadata(mode: str, summary: dict[str, Any], first_file: dict[str, Any]) -> dict[str, Any]:
    metadata = {
        "mode": mode,
        "region_id": summary["region_id"],
        "table_dir": summary["table_dir"],
        "region_dir": summary["region_dir"],
        "total_rows": summary["total_rows"],
        "sst_count": summary["sst_count"],
        "rows_per_sst": summary["rows_per_sst"],
        "row_group_size": summary["row_group_size"],
        "first_file_id": first_file["file_id"],
        "first_file_size": first_file["file_size"],
        "first_file_rows": first_file["num_rows"],
    }
    for key in ("metric_physical", "metric_engine_value_encoding", "source_batch_rows", "columns"):
        if key in summary:
            metadata[key] = summary[key]
        if key in first_file:
            metadata[f"first_file_{key}"] = first_file[key]
    return metadata


def validate_encoding_metadata(mode: str, first_file: dict[str, Any]) -> dict[str, Any]:
    value_column = first_file.get("columns", {}).get("greptime_value")
    if value_column is None:
        raise RuntimeError("files.jsonl does not contain greptime_value column metadata")

    has_bss = bool(value_column.get("has_byte_stream_split"))
    has_dictionary = bool(value_column.get("has_dictionary"))
    if mode == "plain":
        if has_bss:
            raise RuntimeError("plain mode unexpectedly used BYTE_STREAM_SPLIT")
    elif mode == "no_dictionary":
        if has_bss or has_dictionary:
            raise RuntimeError(
                f"no_dictionary mode expected no BSS and no dictionary, got {value_column}"
            )
    elif mode == "byte_stream_split":
        if not has_bss or has_dictionary:
            raise RuntimeError(
                f"byte_stream_split mode expected BSS and no dictionary, got {value_column}"
            )
    else:
        raise RuntimeError(f"unknown mode {mode}")

    return {
        "greptime_value": value_column,
        "validated": True,
    }


def run_suite(args: argparse.Namespace) -> dict[str, Any]:
    repo = repo_root()
    work_dir = args.work_dir.resolve()
    work_dir.mkdir(parents=True, exist_ok=True)
    logs_dir = work_dir / "logs"

    if not args.no_build:
        run(["cargo", "build", "-p", "cmd", "--bin", "query_perf_fixture"], cwd=repo, log=logs_dir / "build-query_perf_fixture.log")
        run(["cargo", "build", "-p", "cmd", "--bin", "greptime", "--features", "dev-tools"], cwd=repo, log=logs_dir / "build-greptime-dev-tools.log")

    target_dir = cargo_target_dir(repo)
    query_perf_fixture = target_dir / "debug" / "query_perf_fixture"
    greptime = target_dir / "debug" / "greptime"
    if not query_perf_fixture.exists() or not greptime.exists():
        raise RuntimeError(f"missing built binaries under {target_dir}/debug")

    scan_configs = write_scan_configs(work_dir)
    modes = args.modes or list(MODES)
    report: dict[str, Any] = {
        "work_dir": str(work_dir),
        "repo": str(repo),
        "modes": modes,
        "parameters": {key: str(value) if isinstance(value, Path) else value for key, value in vars(args).items()},
        "results": {},
    }

    for mode in modes:
        mode_dir = work_dir / mode
        fixture_dir = mode_dir / "fixture"
        data_home = mode_dir / "data"
        config_path = mode_dir / "bench.toml"
        case_path = mode_dir / "case.toml"
        mode_logs = logs_dir / mode
        mode_dir.mkdir(parents=True, exist_ok=True)

        write_case(
            case_path,
            mode=mode,
            rows_per_sst=args.rows_per_sst,
            sst_count=args.sst_count,
            row_group_size=args.row_group_size,
            series_count=args.series_count,
            source_batch_rows=args.source_batch_rows,
        )
        if fixture_dir.exists():
            shutil.rmtree(fixture_dir)
        run(
            [
                str(query_perf_fixture),
                "--case",
                str(case_path),
                "--out-dir",
                str(fixture_dir),
                "--allow-large",
            ],
            cwd=repo,
            log=mode_logs / "generate.log",
        )

        summary = materialize_fixture(fixture_dir, data_home)
        write_bench_config(config_path, data_home)
        first_file = read_first_file(fixture_dir / "files.jsonl")
        encoding_validation = validate_encoding_metadata(mode, first_file)
        mode_result = {
            "metadata": summarize_metadata(mode, summary, first_file),
            "encoding_validation": encoding_validation,
            "parquetbench": {},
            "scanbench": {},
        }

        for projection, scan_config in scan_configs.items():
            parquet_log = mode_logs / f"parquetbench-{projection}.log"
            parquet_result = run(
                [
                    str(greptime),
                    "datanode",
                    "parquetbench",
                    "--config",
                    str(config_path),
                    "--region-id",
                    str(summary["region_id"]),
                    "--table-dir",
                    summary["table_dir"],
                    "--file-id",
                    first_file["file_id"],
                    "--reader",
                    "direct",
                    "--iterations",
                    str(args.iterations),
                    "--scan-config",
                    str(scan_config),
                ],
                cwd=repo,
                log=parquet_log,
            )
            mode_result["parquetbench"][projection] = logged_result(parquet_result, parquet_log)

            scan_log = mode_logs / f"scanbench-{projection}.log"
            scan_result = run(
                [
                    str(greptime),
                    "datanode",
                    "scanbench",
                    "--config",
                    str(config_path),
                    "--region-id",
                    str(summary["region_id"]),
                    "--table-dir",
                    summary["table_dir"],
                    "--scanner",
                    "seq",
                    "--iterations",
                    str(args.iterations),
                    "--scan-config",
                    str(scan_config),
                ],
                cwd=repo,
                log=scan_log,
            )
            mode_result["scanbench"][projection] = logged_result(scan_result, scan_log)
        report["results"][mode] = mode_result

    (work_dir / "report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    write_markdown_report(work_dir / "report.md", report)
    return report


def write_markdown_report(path: Path, report: dict[str, Any]) -> None:
    lines = ["# Metric value encoding perf suite", ""]
    lines.append(f"Work dir: `{report['work_dir']}`")
    lines.append("")
    lines.append("| mode | first SST bytes | value column bytes | value encodings | parquet value avg ms | parquet ts+value avg ms | scan value avg ms | scan ts+value avg ms | rows | row group size | source batch rows |")
    lines.append("| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for mode, result in report["results"].items():
        meta = result["metadata"]
        value_column = result["encoding_validation"]["greptime_value"]
        lines.append(
            f"| `{mode}` | {meta.get('first_file_size', '')} | "
            f"{value_column.get('compressed_size', '')} | "
            f"{', '.join(value_column.get('encodings', []))} | "
            f"{format_average_ms(result['parquetbench']['value'])} | "
            f"{format_average_ms(result['parquetbench']['timestamp_value'])} | "
            f"{format_average_ms(result['scanbench']['value'])} | "
            f"{format_average_ms(result['scanbench']['timestamp_value'])} | "
            f"{meta.get('total_rows', '')} | {meta.get('row_group_size', '')} | "
            f"{meta.get('source_batch_rows', '')} |"
        )
    lines.append("")
    lines.append("Raw command output is under `logs/<mode>/`.")
    lines.append("")
    path.write_text("\n".join(lines))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, default=Path("/tmp/opencode/metric-value-encoding-perf"))
    parser.add_argument("--rows-per-sst", type=int, default=65_536)
    parser.add_argument("--sst-count", type=int, default=1)
    parser.add_argument("--row-group-size", type=int, default=65_536)
    parser.add_argument("--series-count", type=int, default=64)
    parser.add_argument("--source-batch-rows", type=int, default=4_096)
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument("--mode", dest="modes", choices=MODES, action="append")
    parser.add_argument("--no-build", action="store_true")
    return parser.parse_args()


def main() -> None:
    report = run_suite(parse_args())
    print(f"Wrote {report['work_dir']}/report.md")
    print(f"Wrote {report['work_dir']}/report.json")


if __name__ == "__main__":
    main()
