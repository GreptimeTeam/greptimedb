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
# The Raft Engine controls every object store WAL cell is compared with: fsync per write and the default.
BASELINES = ("R", "R0")
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


def read_metrics(path: Path) -> dict:
    return parse_metrics(path.read_text() if path.exists() else "")


def load_cell(path: Path, spec: dict | None = None) -> dict:
    """One cell's row; a planned cell that never recorded a result has no values, so no ratio uses its round."""
    recorded = path / "cell.json"
    cell = json.loads(recorded.read_text()) if recorded.exists() else spec | {"exit_code": None, "missing": True}
    result_path = path / "greptimedb-1" / "load-result.json"
    result = json.loads(result_path.read_text()) if result_path.exists() else {}
    metrics, restarted = read_metrics(path / "metrics.prom"), read_metrics(path / "metrics-restart.prom")
    load_seconds = result.get("load_seconds")
    wall, cpu = cell.get("load_wall_seconds"), cell.get("server_cpu_seconds")
    row = {
        "id": cell["id"], "round": cell["round"], "concurrency": cell["concurrency"],
        "config": cell["config"], "exit_code": cell["exit_code"], "wal_device": cell.get("wal_device"),
        "missing": cell.get("missing", False), "env": cell.get("env", {}), "sweep_value": cell.get("sweep_value"),
        "version": cell.get("version"), "load_wall_seconds": wall,
        "rows": result.get("count_after"), "load_seconds": load_seconds,
        "rows_per_second": result.get("rows_per_second") if cell["exit_code"] == 0 else None,
        "server_cpu_cores": cpu / wall if cpu is not None and wall else None,
        "host_cpu_busy": cell.get("host_cpu_busy"), "host_cpus": cell.get("host_cpus"),
    }
    for name, key in (("append_ack_seconds", "append_ack"), ("seal_to_durable_seconds", "seal_to_durable"),
                      ("object_bytes", "object_bytes")):
        buckets = metrics["buckets"].get(name, [])
        row[f"{key}_p50"], row[f"{key}_p99"] = quantile(0.5, buckets), quantile(0.99, buckets)
    if cell.get("restart"):
        # A restarted process observed exactly one recovery, so the sum is its duration.
        count = restarted["samples"].get("recovery_seconds_count")
        row["restart"] = {
            "wall_seconds": cell.get("restart_seconds"), "query_seconds": cell.get("restart_query_seconds"),
            "rows": cell.get("restart_count"),
            "recovery_seconds": restarted["samples"].get("recovery_seconds_sum") if count == 1 else None,
            "recovered_objects": restarted["samples"].get("recovered_objects_total")
            if cell.get("restart_scraped") else None,
        }
    # The counters register on their first event, so a scrape of an object store WAL
    # that created objects omits the ones that never moved: those are zero, not unknown.
    active = (cell.get("metrics_scraped") and cell.get("wal", {}).get("PROVIDER") == "experimental_object_store"
              and metrics["samples"].get("created_objects_total"))
    for counter in COUNTERS:
        row[counter] = metrics["samples"].get(counter, 0 if active else None)
    created = row["created_objects_total"]
    row["creates_per_minute"] = created * 60 / load_seconds if created is not None and load_seconds else None
    return row


def ratios(rows: list) -> list:
    """Each object store WAL cell over each Raft Engine control per swept value and round, and their geometric mean over the rounds, per concurrency."""
    speed = {(r["round"], r["concurrency"], r["config"], r["sweep_value"]): r["rows_per_second"] for r in rows}
    series = sorted({(r["config"], r["sweep_value"]) for r in rows if r["config"] not in BASELINES},
                    key=lambda s: (s[0], s[1] or 0))
    baselines = [b for b in BASELINES if any(r["config"] == b for r in rows)]
    result = []
    for concurrency in sorted({r["concurrency"] for r in rows}):
        rounds = sorted({r["round"] for r in rows if r["concurrency"] == concurrency})
        for (config, value), baseline in ((s, b) for s in series for b in baselines):
            per_round = []
            for round_number in rounds:
                base = speed.get((round_number, concurrency, baseline, None))
                measured = speed.get((round_number, concurrency, config, value))
                per_round.append(measured / base if base and measured else None)
            complete = [ratio for ratio in per_round if ratio is not None]
            result.append({"concurrency": concurrency, "ratio": f"{config}/{baseline}", "sweep_value": value,
                           "per_round": per_round,
                           "geomean": geomean(complete) if len(complete) == len(per_round) else None})
    return result


def overrides(env: dict) -> str:
    return " ".join(f"{k.removeprefix('GREPTIMEDB_OSWAL_')}={v}" for k, v in sorted(env.items())) or "—"


def pair(a, b, digits=0, scale=1.0) -> str:
    return f"{fmt(a, digits, scale)} / {fmt(b, digits, scale)}"


def fmt(value, digits=0, scale=1.0) -> str:
    return "—" if value is None else f"{value * scale:,.{digits}f}"


def render(run: dict, rows: list, pairs: list) -> str:
    lines = [f"## Object store WAL against S3 ({'smoke, MinIO' if run.get('smoke') else run.get('instance_type')})", ""]
    lines += [f"GreptimeDB `{run.get('greptimedb_image')}` ({next((r['version'] for r in rows if r['version']), '—')}); "
              f"{fmt(run.get('rows'))} rows in {run.get('batch_rows')}-row batches; bucket region {run.get('region')}.", ""]
    lines += ["| Cell | Overrides | Rows/s | Load s | Wall s | Ack p50 / p99 ms | Seal to durable p50 / p99 ms "
              "| Object p50 / p99 MiB | Creates | Creates/min | Stalled | Poisoned | Create failures | Conflicts "
              "| Server CPUs | Host busy | WAL on |",
              "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: "
              "| ---: | ---: | --- |"]
    for r in rows:
        cell = f"{r['id']} (not run)" if r["missing"] else r["id"] if r["exit_code"] == 0 \
            else f"{r['id']} (failed, exit {r['exit_code']})"
        lines.append(" | ".join([
            f"| {cell}", overrides(r["env"]), fmt(r["rows_per_second"]), fmt(r["load_seconds"], 1),
            fmt(r["load_wall_seconds"], 1), pair(r["append_ack_p50"], r["append_ack_p99"], 1, 1000),
            pair(r["seal_to_durable_p50"], r["seal_to_durable_p99"], 1, 1000),
            pair(r["object_bytes_p50"], r["object_bytes_p99"], 1, 1 / 2**20),
            *(fmt(r[c]) for c in COUNTERS[:1]), fmt(r["creates_per_minute"], 1),
            *(fmt(r[c]) for c in COUNTERS[1:]), fmt(r["server_cpu_cores"], 2),
            "—" if r["host_cpu_busy"] is None else f"{r['host_cpu_busy']:.0%} of {r['host_cpus'] or '?'}",
            f"`{r['wal_device'] or '—'}` |"]))
    restarts = [r for r in rows if "restart" in r]
    if restarts:
        lines += ["", "| Cell | Overrides | Objects created | Restart to /health s | Restart to count s "
                  "| Recovery s | Recovered objects | Rows after restart |",
                  "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |"]
        for r in restarts:
            x = r["restart"]
            lines.append(" | ".join([
                f"| {r['id']}", overrides(r["env"]), fmt(r["created_objects_total"]), fmt(x["wall_seconds"], 2),
                fmt(x["query_seconds"], 2), fmt(x["recovery_seconds"], 3), fmt(x["recovered_objects"]),
                f"{fmt(x['rows'])} |"]))
    lines += ["", "| Concurrency | Ratio | Swept value | Per round | Geometric mean |", "| ---: | --- | ---: | --- | ---: |"]
    for p in pairs:
        lines.append(f"| {p['concurrency']} | {p['ratio']} | {fmt(p['sweep_value'])} "
                     f"| {', '.join(fmt(v, 3) for v in p['per_round'])} | {fmt(p['geomean'], 3)} |")
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args()
    args.root.mkdir(parents=True, exist_ok=True)
    run_path = args.root / "run.json"
    run = json.loads(run_path.read_text()) if run_path.exists() else {}
    plan_path = args.root / "plan.json"
    planned = json.loads(plan_path.read_text())["cells"] if plan_path.exists() else []
    # The plan, not what happened to run, defines the expected cells and rounds.
    rows = [load_cell(args.root / "cells" / spec["id"], spec) for spec in planned]
    rows += [load_cell(path) for path in sorted((args.root / "cells").glob("*"))
             if path.name not in {spec["id"] for spec in planned} and (path / "cell.json").exists()]
    pairs = ratios(rows)
    missing = [r["id"] for r in rows if r["missing"]]
    (args.root / "summary.json").write_text(json.dumps(
        {"run": run, "planned_cells": len(planned), "missing_cells": missing, "cells": rows, "ratios": pairs},
        indent=2) + "\n")
    print(render(run, rows, pairs) if rows else "No benchmark cell completed.\n")


if __name__ == "__main__":
    main()
