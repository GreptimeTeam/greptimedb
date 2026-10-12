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

"""Summarize the object store WAL S3 benchmark as Markdown and summary.json."""

from __future__ import annotations

import argparse
import json
import math
import re
from pathlib import Path

PREFIX = "greptime_logstore_object_store_wal_"
COUNTERS = ("created_objects_total", "stalled_appends_total", "poisoned_total",
            "create_failures_total", "create_conflicts_total")
SAMPLE = re.compile(r'([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+(\S+)')


def parse_metrics(text: str) -> dict:
    """Map each unlabeled sample to its value and each histogram to its buckets."""
    samples, buckets = {}, {}
    for line in text.splitlines():
        match = SAMPLE.fullmatch(line.strip())
        if not match or not match.group(1).startswith(PREFIX):
            continue
        name, labels, value = match.group(1)[len(PREFIX):], match.group(2) or "", float(match.group(3))
        le = re.search(r'le="([^"]+)"', labels)
        if name.endswith("_bucket") and le:
            buckets.setdefault(name[:-len("_bucket")], []).append((float(le.group(1)), value))
        else:
            samples[name] = value
    return {"samples": samples, "buckets": {name: sorted(items) for name, items in buckets.items()}}


def quantile(q: float, buckets: list) -> float | None:
    """Prometheus histogram_quantile over cumulative (upper bound, count) buckets."""
    if not buckets or buckets[-1][1] == 0:
        return None
    rank = q * buckets[-1][1]
    lower, below = 0.0, 0.0
    for upper, count in buckets:
        if count >= rank:
            if math.isinf(upper):
                return lower
            return lower + (upper - lower) * (rank - below) / (count - below) if count > below else upper
        lower, below = upper, count
    return lower


def geomean(values: list) -> float | None:
    return math.exp(sum(map(math.log, values)) / len(values)) if values and all(v > 0 for v in values) else None


def load_cell(path: Path) -> dict:
    cell = json.loads((path / "cell.json").read_text())
    result_path = path / "greptimedb-1" / "load-result.json"
    result = json.loads(result_path.read_text()) if result_path.exists() else {}
    metrics_path = path / "metrics.prom"
    metrics = parse_metrics(metrics_path.read_text() if metrics_path.exists() else "")
    ack = metrics["buckets"].get("append_ack_seconds", [])
    load_seconds = result.get("load_seconds")
    row = {
        "id": cell["id"], "round": cell["round"], "concurrency": cell["concurrency"],
        "config": cell["config"], "exit_code": cell["exit_code"], "wal_device": cell["wal_device"],
        "version": cell.get("version"), "load_wall_seconds": cell.get("load_wall_seconds"),
        "rows": result.get("count_after"), "load_seconds": load_seconds,
        "rows_per_second": result.get("rows_per_second") if cell["exit_code"] == 0 else None,
        "append_ack_p50_seconds": quantile(0.5, ack), "append_ack_p99_seconds": quantile(0.99, ack),
    }
    for counter in COUNTERS:
        row[counter] = metrics["samples"].get(counter)
    created = row["created_objects_total"]
    row["creates_per_minute"] = created * 60 / load_seconds if created is not None and load_seconds else None
    return row


def ratios(rows: list) -> list:
    """D/R and E/R per round and their geometric mean over the rounds, per concurrency."""
    speed = {(r["round"], r["concurrency"], r["config"]): r["rows_per_second"] for r in rows}
    result = []
    for concurrency in sorted({r["concurrency"] for r in rows}):
        rounds = sorted({r["round"] for r in rows if r["concurrency"] == concurrency})
        for config in ("D", "E"):
            per_round = []
            for round_number in rounds:
                base, value = speed.get((round_number, concurrency, "R")), speed.get((round_number, concurrency, config))
                per_round.append(value / base if base and value else None)
            complete = [value for value in per_round if value is not None]
            result.append({"concurrency": concurrency, "ratio": f"{config}/R", "per_round": per_round,
                           "geomean": geomean(complete) if len(complete) == len(per_round) else None})
    return result


def fmt(value, digits=0, scale=1.0) -> str:
    return "—" if value is None else f"{value * scale:,.{digits}f}"


def render(run: dict, rows: list, pairs: list) -> str:
    lines = [f"## Object store WAL against S3 ({'smoke, MinIO' if run.get('smoke') else run.get('instance_type')})", ""]
    lines += [f"GreptimeDB `{run.get('greptimedb_image')}` ({rows[0]['version'] if rows else '—'}); "
              f"{fmt(run.get('rows'))} rows in {run.get('batch_rows')}-row batches; bucket region {run.get('region')}.", ""]
    lines += ["| Cell | Rows/s | Load s | Wall s | Rows | Ack p50 ms | Ack p99 ms | Creates | Creates/min "
              "| Stalled | Poisoned | Create failures | Conflicts | WAL on |",
              "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |"]
    for r in rows:
        cell = r["id"] if r["exit_code"] == 0 else f"{r['id']} (failed, exit {r['exit_code']})"
        lines.append(" | ".join([
            f"| {cell}", fmt(r["rows_per_second"]), fmt(r["load_seconds"], 1), fmt(r["load_wall_seconds"], 1),
            fmt(r["rows"]), fmt(r["append_ack_p50_seconds"], 1, 1000), fmt(r["append_ack_p99_seconds"], 1, 1000),
            *(fmt(r[c]) for c in COUNTERS[:1]), fmt(r["creates_per_minute"], 1),
            *(fmt(r[c]) for c in COUNTERS[1:]), f"`{r['wal_device']}` |"]))
    lines += ["", "| Concurrency | Ratio | Per round | Geometric mean |", "| ---: | --- | --- | ---: |"]
    for p in pairs:
        lines.append(f"| {p['concurrency']} | {p['ratio']} | {', '.join(fmt(v, 3) for v in p['per_round'])} "
                     f"| {fmt(p['geomean'], 3)} |")
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args()
    args.root.mkdir(parents=True, exist_ok=True)
    run_path = args.root / "run.json"
    run = json.loads(run_path.read_text()) if run_path.exists() else {}
    plan_path = args.root / "plan.json"
    order = [cell["id"] for cell in json.loads(plan_path.read_text())["cells"]] if plan_path.exists() else []
    cells = {path.name: load_cell(path) for path in (args.root / "cells").glob("*") if (path / "cell.json").exists()}
    rows = [cells[i] for i in order if i in cells] + [cells[i] for i in sorted(cells) if i not in order]
    pairs = ratios(rows)
    (args.root / "summary.json").write_text(json.dumps({"run": run, "cells": rows, "ratios": pairs}, indent=2) + "\n")
    print(render(run, rows, pairs) if rows else "No benchmark cell completed.\n")


if __name__ == "__main__":
    main()
