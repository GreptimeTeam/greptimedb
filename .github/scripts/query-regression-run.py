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
import os
import re
import subprocess
from pathlib import Path


DEFAULT_CASES = [
    "tests/perf/query_cases/smoke_direct_sst/case.toml",
    "tests/perf/query_cases/prom_remote_write_seeded_random/case.toml",
    "tests/perf/query_cases/prom_remote_write_run_heavy/case.toml",
    "tests/perf/query_cases/prom_remote_write_mixed_every/case.toml",
    "tests/perf/query_cases/prom_remote_write_integer_counter/case.toml",
    "tests/perf/query_cases/promql_pushdown_7913/case.toml",
    "tests/perf/query_cases/analyze_verbose_many_files/case.toml",
    "tests/perf/query_cases/sql_topk_order_by/case.toml",
    "tests/perf/query_cases/sql_aggregate_order_by/case.toml",
    "tests/perf/query_cases/sql_join_filter_order/case.toml",
]


def split_cases(values: list[str]) -> list[str]:
    tokens: list[str] = []
    for value in values:
        tokens.extend(part for part in re.split(r"[\s,]+", value.strip()) if part)
    if not tokens or tokens == ["all"]:
        return DEFAULT_CASES.copy()
    if "all" in tokens:
        raise ValueError("'all' cannot be mixed with explicit case paths")
    return list(dict.fromkeys(tokens))


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


def run_case(args: argparse.Namespace, case_path: Path, work_dir: Path) -> int:
    target_dir = profile_dir(args.cargo_profile)
    base_bin = args.base_bin or args.base_src / "target" / target_dir / "greptime"
    candidate_bin = args.candidate_bin or args.candidate_src / "target" / target_dir / "greptime"
    fixture_generator = args.fixture_generator or args.candidate_src / "target" / target_dir / "query_perf_fixture"
    cmd = [
        "uv",
        "run",
        "--no-project",
        "python",
        str(args.candidate_src / "tests/perf/query_regression_runner.py"),
        "--case",
        str(case_path),
        "--base-bin",
        str(base_bin),
        "--candidate-bin",
        str(candidate_bin),
        "--fixture-generator",
        str(fixture_generator),
        "--work-dir",
        str(work_dir),
        "--http-timeout",
        str(args.http_timeout),
    ]
    if parse_bool(args.allow_large_fixture):
        cmd.append("--allow-large-fixture")

    print(f"::group::Query regression case: {case_path}", flush=True)
    try:
        return subprocess.run(cmd, check=False).returncode
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


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cases", action="append", help="'all' or comma/space separated case paths")
    parser.add_argument("--base-src", type=Path, default=Path("base-src"))
    parser.add_argument("--candidate-src", type=Path, default=Path("candidate-src"))
    parser.add_argument("--base-bin", type=Path, default=configured_path(os.environ.get("BASE_BIN")))
    parser.add_argument("--candidate-bin", type=Path, default=configured_path(os.environ.get("CANDIDATE_BIN")))
    parser.add_argument(
        "--fixture-generator",
        type=Path,
        default=configured_path(os.environ.get("FIXTURE_GENERATOR")),
    )
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
        case_status = run_case(args, case_path, work_dir)
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
