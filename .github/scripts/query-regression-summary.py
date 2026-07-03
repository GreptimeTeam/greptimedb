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

"""Format tests/perf query regression JSON reports as GitHub Markdown."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def fmt_ms(value: Any) -> str:
    if value is None:
        return "N/A"
    try:
        return f"{float(value):.2f}"
    except (TypeError, ValueError):
        return "N/A"


def esc(value: Any) -> str:
    text = "N/A" if value is None else str(value)
    return text.replace("|", "\\|").replace("\n", " ")


def status_emoji(status: str | None) -> str:
    return {"ok": "✅", "measured": "✅", "failed": "❌", "planned": "📝", "fixture-ready": "🧪"}.get(
        status or "", "⚠️"
    )


def measurement_map(target: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {m.get("name") or f"query-{i}": m for i, m in enumerate(target.get("measurements", []))}


def regression_pct(base: Any, candidate: Any) -> str:
    try:
        b = float(base)
        c = float(candidate)
    except (TypeError, ValueError):
        return "N/A"
    if b == 0:
        return "N/A"
    return f"{(c - b) / b * 100:+.1f}%"


def threshold_status(thresholds: list[dict[str, Any]], query: str) -> str:
    hits = [t for t in thresholds if t.get("query") == query]
    if not hits:
        return "N/A"
    return ", ".join(f"{t.get('threshold')}: {t.get('status')}" for t in hits)


def target_table(targets: list[dict[str, Any]]) -> str:
    rows = ["| Target | Status | Validation errors | Region | Datanode data home |", "| --- | --- | ---: | --- | --- |"]
    for target in targets:
        errors = len(target.get("validation_errors") or [])
        discovered = target.get("discovered") or {}
        if isinstance(discovered, list):
            region = ", ".join(str(item.get("region_id")) for item in discovered)
        else:
            region = discovered.get("region_id")
        rows.append(
            "| {name} | {status} {raw} | {errors} | {region} | `{data}` |".format(
                name=esc(target.get("name")),
                status=status_emoji(target.get("status")),
                raw=esc(target.get("status")),
                errors=errors,
                region=esc(region),
                data=esc(target.get("datanode_data_home") or target.get("data_dir")),
            )
        )
    return "\n".join(rows)


def comparison_table(targets: list[dict[str, Any]], thresholds: list[dict[str, Any]]) -> str:
    if len(targets) < 2:
        return "No base/candidate measurements found."
    base = measurement_map(targets[0])
    candidate = measurement_map(targets[1])
    names = sorted(set(base) | set(candidate))
    rows = [
        "| Query | Base median ms | Base p95 ms | Candidate median ms | Candidate p95 ms | Regression | Threshold |",
        "| --- | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for name in names:
        bm = base.get(name, {})
        cm = candidate.get(name, {})
        rows.append(
            "| {q} | {bm} | {bp} | {cm} | {cp} | {reg} | {th} |".format(
                q=esc(name),
                bm=fmt_ms(bm.get("latency_ms_median")),
                bp=fmt_ms(bm.get("latency_ms_p95")),
                cm=fmt_ms(cm.get("latency_ms_median")),
                cp=fmt_ms(cm.get("latency_ms_p95")),
                reg=regression_pct(bm.get("latency_ms_median"), cm.get("latency_ms_median")),
                th=esc(threshold_status(thresholds, name)),
            )
        )
    return "\n".join(rows)


def build_markdown(
    report: dict[str, Any],
    report_path: Path,
    run_url: str | None,
    case_name: str | None,
    base_ref: str | None,
    candidate_ref: str | None,
) -> str:
    status = report.get("status", "missing")
    case = report.get("case") or {}
    title_case = case_name or case.get("name") or report_path.name
    lines = [f"## {status_emoji(status)} Query regression report: `{esc(title_case)}`", ""]
    lines.append(f"- **Status:** `{esc(status)}`")
    lines.append(f"- **Case path:** `{esc(report.get('case_path'))}`")
    lines.append(f"- **Query mode:** `{esc(report.get('query_mode'))}`")
    if base_ref:
        lines.append(f"- **Base ref:** `{esc(base_ref)}`")
    if candidate_ref:
        lines.append(f"- **Candidate ref:** `{esc(candidate_ref)}`")
    if run_url:
        lines.append(f"- **Workflow run:** {run_url}")
    if report.get("error"):
        lines.append(f"- **Error:** `{esc(report.get('error'))}`")
    lines.append("- **Artifacts:** query-regression-work logs, fixture metadata, and JSON report are uploaded with this run.")

    targets = report.get("targets") or []
    lines.extend(["", "### Targets", "", target_table(targets)])
    lines.extend(["", "### Query comparison", "", comparison_table(targets, report.get("thresholds") or [])])

    not_enforced = [t for t in report.get("thresholds") or [] if t.get("status") == "not_enforced"]
    if not_enforced:
        lines.extend(["", "### Not enforced thresholds", ""])
        for item in not_enforced:
            lines.append(f"- `{esc(item.get('query'))}` / `{esc(item.get('threshold'))}`: {esc(item.get('reason'))}")

    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--run-url")
    parser.add_argument("--case-name")
    parser.add_argument("--base-ref")
    parser.add_argument("--candidate-ref")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    if args.report.exists():
        report = json.loads(args.report.read_text())
    else:
        report = {"status": "failed", "error": f"report not found: {args.report}", "targets": []}
    markdown = build_markdown(
        report,
        args.report,
        args.run_url,
        args.case_name,
        args.base_ref,
        args.candidate_ref,
    )
    if args.output:
        args.output.write_text(markdown)
    else:
        print(markdown, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
