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
# Mito's line for every region it opened, with the WAL range it replayed.
REPLAY = re.compile(r"Replay WAL for region: (\d+)\(.*?provider: (\w+).*?rows recovered: (\d+), "
                    r"replay from entry id: (\d+), last entry id: (\d+),.*?elapsed: ([0-9.]+)(ns|µs|us|ms|s)\b")
UNITS = {"ns": 1e-9, "µs": 1e-6, "us": 1e-6, "ms": 1e-3, "s": 1.0}
# An object store WAL entry id is the object sequence above a 20-bit position.
POSITION_BITS = 20


def parse_metrics(text: str) -> dict:
    """Map each unlabeled sample to its value, each histogram to its buckets, and the S3
    requests (data files and WAL together) to their count per operation; the local cache
    stores report through the same layer under another scheme and are left out."""
    samples, buckets, requests = {}, {}, {}
    for line in text.splitlines():
        match = SAMPLE.fullmatch(line.strip())
        if match and match.group(1) == "opendal_operation_duration_seconds_count":
            operation = re.search(r'operation="([^"]+)"', match.group(2) or "")
            if operation and re.search(r'scheme="s3"', match.group(2) or ""):
                requests[operation.group(1)] = requests.get(operation.group(1), 0) + float(match.group(3))
            continue
        if not match or not match.group(1).startswith(PREFIX):
            continue
        name, labels, value = match.group(1)[len(PREFIX):], match.group(2) or "", float(match.group(3))
        le = re.search(r'le="([^"]+)"', labels)
        if name.endswith("_bucket") and le:
            buckets.setdefault(name[:-len("_bucket")], []).append((float(le.group(1)), value))
        else:
            samples[name] = value
    return {"samples": samples, "buckets": {name: sorted(items) for name, items in buckets.items()},
            "requests": requests}


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


def ratio(a, b):
    return a / b if a is not None and b else None


def replay(log: Path, metrics: dict) -> dict | None:
    """What a restarted server replayed: every region that had rows to replay, the objects of
    its WAL range, and the log store's time serving region reads (`read_seconds`)."""
    if not log.exists():
        return None
    regions = []
    for match in REPLAY.finditer(log.read_text(errors="replace")):
        region, provider, rows, first, last, elapsed, unit = match.groups()
        # The range runs from the first entry to replay to the last one; it is empty when first > last.
        objects = None if provider != "ObjectStore" else 0 if int(first) > int(last) else \
            (int(last) >> POSITION_BITS) - (int(first) >> POSITION_BITS) + 1
        regions.append({"region": int(region), "rows": int(rows), "first_entry_id": int(first),
                        "last_entry_id": int(last), "seconds": float(elapsed) * UNITS[unit], "objects": objects})
    samples = metrics["samples"]
    return {"regions": regions, "rows": sum(r["rows"] for r in regions),
            "seconds": max((r["seconds"] for r in regions), default=None),
            "objects": sum(r["objects"] for r in regions) if regions and regions[0]["objects"] is not None else None,
            "read_seconds": samples.get("read_seconds_sum"), "reads": samples.get("read_seconds_count")}


def replayed(regions: list) -> str:
    """Each region that replayed rows: its rows, the objects of its range and its time."""
    return "; ".join(f"{r['region']}: {r['rows']:,} rows" + (
        f", objects {r['first_entry_id'] >> POSITION_BITS:,}–{r['last_entry_id'] >> POSITION_BITS:,}"
        if r["objects"] else "") + f", {r['seconds']:.2f} s" for r in regions if r["rows"]) or "—"


def client_latency(path: Path) -> tuple:
    """The 50th and 99th percentile of the loader's own time per write request."""
    durations = []
    if path.exists():
        for line in path.read_text().splitlines():
            record = json.loads(line) if line.strip() else {}
            if record.get("step") == "grpc_write_batch" and record.get("status") == "success":
                durations.append(record["duration_seconds"])
    durations.sort()
    return tuple(durations[max(0, math.ceil(q * len(durations)) - 1)] if durations else None for q in (0.5, 0.99))


def region_rows(path: Path) -> list | None:
    """Rows per region of a partitioned load cell, as the server reported them after the load."""
    if not path.exists():
        return None
    return [int(row[2] or 0) for row in json.loads(path.read_text())["output"][0]["records"]["rows"]]


def interval_seconds(wal: dict) -> float:
    value = wal.get("FLUSH_INTERVAL", "100ms")
    return float(value[:-2]) / 1000 if value.endswith("ms") else float(value.rstrip("s"))


def traffic(end: dict, start: dict, seconds, wal: dict) -> dict:
    """WAL traffic between two scrapes of one process over `seconds`; more objects than seal
    ticks, or objects near the 8 MiB threshold, mean batches sealed on size rather than on the timer."""
    def delta(name):
        value = end["samples"].get(name)
        return None if value is None else value - start["samples"].get(name, 0)
    created, written, objects = delta("created_objects_total"), delta("object_bytes_sum"), delta("object_bytes_count")
    per_second = created / seconds if created and seconds else None
    mean = written / objects if written and objects else None
    return {"wal_mb_per_second": written / seconds / 1e6 if written and seconds else None,
            "objects_per_second": per_second, "mean_object_bytes": mean,
            "sealed_on_size": None if per_second is None else
            per_second > 1.05 / interval_seconds(wal) or (mean or 0) >= 0.9 * 8 * 2**20,
            "requests": {op: n - start["requests"].get(op, 0) for op, n in end["requests"].items()
                         if n != start["requests"].get(op, 0)}}


def crash_cycles(path: Path, wal: dict) -> list:
    """Each cycle's check plus the loaded server's metrics before the kill and the restarted one's after."""
    cycles = [json.loads(line) for line in (path / "cycles.jsonl").read_text().splitlines()
              if line.strip()] if (path / "cycles.jsonl").exists() else []
    for cycle in cycles:
        start = read_metrics(path / f"metrics-start-{cycle['cycle']}.prom")
        killed = read_metrics(path / f"metrics-cycle-{cycle['cycle']}.prom")
        before = killed["samples"]
        after = read_metrics(path / f"metrics-restart-{cycle['cycle']}.prom")
        once = after["samples"].get("recovery_seconds_count") == 1
        cycle |= traffic(killed, start, cycle.get("load_seconds"), wal)
        cycle |= {"created_before_kill": before.get("created_objects_total"),
                  "indexed_before_kill": before.get("indexed_objects"),
                  "recovery_seconds": after["samples"].get("recovery_seconds_sum") if once else None,
                  "recovered_objects": after["samples"].get("recovered_objects_total", 0 if once else None),
                  "restart_requests": after["requests"],
                  "replay": replay(path / f"server-restart-{cycle['cycle']}.log", after)}
    return cycles


def load_cell(path: Path, spec: dict | None = None) -> dict:
    """One cell's row; a planned cell that never recorded a result has no values, so no ratio uses its round."""
    recorded = path / "cell.json"
    cell = json.loads(recorded.read_text()) if recorded.exists() else spec | {"exit_code": None, "missing": True}
    result_path = path / "greptimedb-1" / "load-result.json"
    result = json.loads(result_path.read_text()) if result_path.exists() else {}
    final, restarted = read_metrics(path / "metrics.prom"), read_metrics(path / "metrics-restart.prom")
    # With an idle window the scrape taken when the load ended describes the load.
    metrics = read_metrics(path / "metrics-idle.prom") if cell.get("idle_seconds") else final
    load_seconds = result.get("load_seconds")
    wall, cpu = cell.get("load_wall_seconds"), cell.get("server_cpu_seconds")
    row = {
        "id": cell["id"], "group": cell.get("group", ""), "mode": cell.get("mode", "load"),
        "round": cell["round"], "concurrency": cell["concurrency"],
        "config": cell["config"], "exit_code": cell["exit_code"], "wal_device": cell.get("wal_device"),
        "missing": cell.get("missing", False), "env": cell.get("env", {}),
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
    row["client_p50"], row["client_p99"] = client_latency(path / "greptimedb-1" / "load-transcript.jsonl")
    # An entry is one region's write; an append, counted only in the durable mode, carries one or more.
    samples = metrics["samples"]
    row["entries_per_object"] = ratio(samples.get("object_entries_sum"), samples.get("object_entries_count"))
    row["appends_per_object"] = ratio(samples.get("append_ack_seconds_count"), samples.get("object_bytes_count"))
    row["partitions"] = cell.get("partitions", 1)
    row["region_rows"] = region_rows(path / "regions.json")
    if cell.get("restart"):
        # A restarted process observed exactly one recovery, so the sum is its duration.
        count = restarted["samples"].get("recovery_seconds_count")
        row["restart"] = {
            "wall_seconds": cell.get("restart_seconds"), "query_seconds": cell.get("restart_query_seconds"),
            "rows": cell.get("restart_count"),
            "recovery_seconds": restarted["samples"].get("recovery_seconds_sum") if count == 1 else None,
            "recovered_objects": restarted["samples"].get("recovered_objects_total")
            if cell.get("restart_scraped") else None,
            "indexed_before_kill": cell.get("indexed_before_kill"), "min_objects": cell.get("min_objects", 0),
            "replay": replay(path / "server-restart.log", restarted),
        }
    # The counters register on their first event, so a scrape of an object store WAL
    # that created objects omits the ones that never moved: those are zero, not unknown.
    active = (cell.get("metrics_scraped") and cell.get("wal", {}).get("PROVIDER") == "experimental_object_store"
              and metrics["samples"].get("created_objects_total"))
    for counter in COUNTERS:
        row[counter] = metrics["samples"].get(counter, 0 if active else None)
    created = row["created_objects_total"]
    row["creates_per_minute"] = created * 60 / load_seconds if created is not None and load_seconds else None
    row |= traffic(metrics, {"samples": {}, "requests": {}}, load_seconds, cell.get("wal", {}))
    row["restart_requests"] = restarted["requests"]
    if cell.get("idle_seconds"):
        idle = traffic(final, metrics, cell["idle_seconds"], cell.get("wal", {}))
        end = final["samples"].get("created_objects_total")
        row["idle"] = {"seconds": cell["idle_seconds"], "requests": idle["requests"],
                       "creates": None if end is None else end - metrics["samples"].get("created_objects_total", 0)}
    if row["mode"] == "drop":
        written = read_metrics(path / "metrics-written.prom")["samples"]
        restarted_samples = restarted["samples"]
        samples = [line.split() for line in (path / "collected.tsv").read_text().splitlines()
                   if line.strip()] if (path / "collected.tsv").exists() else []
        row["drop"] = {
            "acked_batches": sum(len((p.read_text()).split()) for p in sorted((path / "dropped").glob("acked-*.log"))),
            "indexed_before_drop": written.get("indexed_objects"),
            "restart_seconds": cell.get("restart_seconds"),
            "recovered_objects": restarted_samples.get("recovered_objects_total"),
            "indexed_at_end": int(float(samples[-1][1])) if samples else None,
            "deleted_after_restart": restarted_samples.get("deleted_objects_total"),
            "seconds_to_collect": int(samples[-1][0]) - int(samples[0][0]) if samples else None,
        }
    if row["mode"] == "crash":
        row["cycles"] = crash_cycles(path, cell.get("wal", {}))
        if (path / "objects.tsv").exists():
            # A sample is time, indexed, deleted, created; a failed scrape is time and "missing".
            row["objects_samples"] = [[int(float(line.split()[0]))] + (
                [None] * 3 if line.split()[1:] == ["missing"] else [int(float(v)) for v in line.split()[1:]])
                for line in (path / "objects.tsv").read_text().splitlines() if line.strip()]
    return row


def ratios(rows: list) -> list:
    """Each object store WAL cell over each Raft Engine control per round, and their geometric mean over the rounds, per concurrency."""
    rows = [r for r in rows if r["mode"] == "load"]
    speed = {(r["group"], r["round"], r["concurrency"], r["config"]): r["rows_per_second"]
             for r in rows}
    result = []
    for group in dict.fromkeys(r["group"] for r in rows):
        members = [r for r in rows if r["group"] == group]
        series = sorted({r["config"] for r in members if r["config"] not in BASELINES})
        baselines = [b for b in BASELINES if any(r["config"] == b for r in members)]
        for concurrency in sorted({r["concurrency"] for r in members}):
            rounds = sorted({r["round"] for r in members if r["concurrency"] == concurrency})
            for config, baseline in ((s, b) for s in series for b in baselines):
                per_round = []
                for round_number in rounds:
                    base = speed.get((group, round_number, concurrency, baseline))
                    measured = speed.get((group, round_number, concurrency, config))
                    per_round.append(measured / base if base and measured else None)
                complete = [ratio for ratio in per_round if ratio is not None]
                result.append({"group": group, "concurrency": concurrency, "ratio": f"{config}/{baseline}",
                               "per_round": per_round,
                               "geomean": geomean(complete) if len(complete) == len(per_round) else None})
    return result


def operations(requests: dict) -> str:
    return ", ".join(f"{op} {n:,.0f}" for op, n in sorted(requests.items())) or "—"


def overrides(env: dict) -> str:
    return " ".join(f"{k}={v}" for k, v in sorted(env.items())) or "—"


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
    def label(r):
        return f"{r['id']} (not run)" if r["missing"] else r["id"] if r["exit_code"] == 0 \
            else f"{r['id']} (failed, exit {r['exit_code']})"
    loads = [r for r in rows if r["mode"] == "load"]
    for r in loads:
        cell = label(r)
        lines.append(" | ".join([
            f"| {cell}", overrides(r["env"]), fmt(r["rows_per_second"]), fmt(r["load_seconds"], 1),
            fmt(r["load_wall_seconds"], 1), pair(r["append_ack_p50"], r["append_ack_p99"], 1, 1000),
            pair(r["seal_to_durable_p50"], r["seal_to_durable_p99"], 1, 1000),
            pair(r["object_bytes_p50"], r["object_bytes_p99"], 1, 1 / 2**20),
            *(fmt(r[c]) for c in COUNTERS[:1]), fmt(r["creates_per_minute"], 1),
            *(fmt(r[c]) for c in COUNTERS[1:]), fmt(r["server_cpu_cores"], 2),
            "—" if r["host_cpu_busy"] is None else f"{r['host_cpu_busy']:.0%} of {r['host_cpus'] or '?'}",
            f"`{r['wal_device'] or '—'}` |"]))
    lines += ["", "| Cell | WAL MB/s | Objects/s | Mean object MiB | Sealed on size | Requests during the load "
              "| Requests of the restart | Idle s | Idle creates | Idle requests |",
              "| --- | ---: | ---: | ---: | --- | --- | --- | ---: | ---: | --- |"]
    for r in loads:
        idle = r.get("idle") or {}
        lines.append(" | ".join([
            f"| {label(r)}", fmt(r["wal_mb_per_second"], 1), fmt(r["objects_per_second"], 1),
            fmt(r["mean_object_bytes"], 2, 1 / 2**20),
            "—" if r["sealed_on_size"] is None else ("yes" if r["sealed_on_size"] else "no"),
            operations(r["requests"]), operations(r["restart_requests"]) if "restart" in r else "—",
            fmt(idle.get("seconds")), fmt(idle.get("creates")), operations(idle["requests"]) if idle else "—"]) + " |")
    crashes = [r for r in rows if r["mode"] == "crash"]
    if crashes:
        lines += ["", "| Cell | Cycle | Kill after s | Acked | Present | Lost | Lost rows | Extra | In flight "
                  "| All found | All acked | All lost | Indexed at kill | Restart to /health s | Recovery s "
                  "| Recovered objects | Pin rows | Verdict |",
                  "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: "
                  "| ---: | ---: | ---: | --- |"]
        for r in crashes:
            for c in r["cycles"] or [{}]:
                verdict = "—" if not c else "FAIL: " + "; ".join(c.get("kill_errors", []) + c.get("verify_errors", [])) \
                    if not c.get("passed") else "pass" if c.get("metrics_complete", True) else "pass, scrape missing"
                lines.append(" | ".join([
                    f"| {label(r)}", fmt(c.get("cycle")), fmt(c.get("kill_after_seconds"), 1),
                    fmt(c.get("cycle_acked_batches")), fmt(c.get("cycle_present_batches")),
                    fmt(c.get("cycle_lost_batches")), fmt(c.get("cycle_lost_rows")), fmt(c.get("cycle_extra_batches")),
                    fmt(c.get("in_flight")), fmt(c.get("total_found_batches")), fmt(c.get("total_acked_batches")),
                    fmt(c.get("total_lost_batches")), fmt(c.get("indexed_before_kill")),
                    fmt(c.get("restart_seconds"), 2), fmt(c.get("recovery_seconds"), 3),
                    fmt(c.get("recovered_objects")), fmt(c.get("pin_rows")), verdict]) + " |")
        lines += ["", "| Cell | Cycle | Load s | WAL MB/s | Objects/s | Mean object MiB | Sealed on size "
                  "| Requests until the kill | Requests of the restart |",
                  "| --- | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |"]
        for r in crashes:
            for c in r["cycles"]:
                lines.append(" | ".join([
                    f"| {r['id']}", fmt(c["cycle"]), fmt(c.get("load_seconds"), 1), fmt(c["wal_mb_per_second"], 1),
                    fmt(c["objects_per_second"], 1), fmt(c["mean_object_bytes"], 2, 1 / 2**20),
                    "—" if c["sealed_on_size"] is None else ("yes" if c["sealed_on_size"] else "no"),
                    operations(c["requests"]), operations(c["restart_requests"])]) + " |")
        pinned = [r for r in crashes if r.get("objects_samples")]
        if pinned:
            lines += ["", "| Cell | Indexed objects every 30 s | Missing samples | Deleted objects at the end "
                      "| Created objects at the end |", "| --- | --- | ---: | ---: | ---: |"]
            for r in pinned:
                samples = r["objects_samples"]
                last = next((s for s in reversed(samples) if s[1] is not None), [None] * 4)
                lines.append(f"| {r['id']} | {', '.join('missing' if s[1] is None else str(s[1]) for s in samples)} "
                             f"| {sum(s[1] is None for s in samples)} | {fmt(last[2])} | {fmt(last[3])} |")
    details = [r for r in loads if any(r[k] is not None for k in ("client_p50", "entries_per_object", "region_rows"))]
    if details:
        lines += ["", "| Cell | Client p50 / p99 ms | Entries per object | Appends per object | Regions "
                  "| Rows per region min / max |", "| --- | ---: | ---: | ---: | ---: | ---: |"]
        for r in details:
            spread = r["region_rows"] or []
            lines.append(" | ".join([
                f"| {label(r)}", pair(r["client_p50"], r["client_p99"], 1, 1000), fmt(r["entries_per_object"], 1),
                fmt(r["appends_per_object"], 2), fmt(len(spread) if spread else r["partitions"]),
                pair(min(spread), max(spread)) if spread else "—"]) + " |")
    replays = [(r["id"], "—", r["restart"]) for r in loads if r.get("restart", {}).get("replay")]
    replays += [(r["id"], fmt(c["cycle"]), c | {"wall_seconds": c.get("restart_seconds")})
                for r in crashes for c in r["cycles"] if c.get("replay")]
    if replays:
        lines += ["", "| Cell | Cycle | Restart to /health s | Recovery s | Recovered objects | Regions opened "
                  "| Rows replayed | Objects replayed | Slowest region replay s | Read seconds (reads) "
                  "| Regions with rows |",
                  "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |"]
        for cell, cycle, x in replays:
            y = x["replay"]
            lines.append(" | ".join([
                f"| {cell}", cycle, fmt(x.get("wall_seconds"), 2), fmt(x.get("recovery_seconds"), 3),
                fmt(x.get("recovered_objects")), fmt(len(y["regions"])), fmt(y["rows"]), fmt(y["objects"]),
                fmt(y["seconds"], 2), f"{fmt(y['read_seconds'], 2)} ({fmt(y['reads'])})",
                replayed(y["regions"])]) + " |")
    drops = [r for r in rows if r.get("drop")]
    if drops:
        lines += ["", "| Cell | Acked batches before the drop | Indexed before the drop | Restart to /health s "
                  "| Recovered objects | Indexed at the end | Deleted after the restart | Seconds to collect | Verdict |",
                  "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |"]
        for r in drops:
            x = r["drop"]
            # Objects the release already collected before the kill leave nothing to show.
            verdict = "—" if x["indexed_at_end"] is None else \
                "nothing left at the kill" if (x["recovered_objects"] or 0) <= 3 else \
                "collected" if r["exit_code"] == 0 else "still indexed"
            lines.append(" | ".join([
                f"| {label(r)}", fmt(x["acked_batches"]), fmt(x["indexed_before_drop"]), fmt(x["restart_seconds"], 2),
                fmt(x["recovered_objects"]), fmt(x["indexed_at_end"]),
                fmt(x["deleted_after_restart"]), fmt(x["seconds_to_collect"]), verdict]) + " |")
    restarts = [r for r in rows if "restart" in r]
    if restarts:
        lines += ["", "| Cell | Overrides | Objects created | Indexed at kill | Restart to /health s | Restart to count s "
                  "| Recovery s | Recovered objects | Rows after restart |",
                  "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"]
        for r in restarts:
            x = r["restart"]
            short = x["min_objects"] and (x["indexed_before_kill"] or 0) < x["min_objects"]
            lines.append(" | ".join([
                f"| {label(r)}", overrides(r["env"]), fmt(r["created_objects_total"]),
                fmt(x["indexed_before_kill"]) + (f" (under {x['min_objects']:,})" if short else ""),
                fmt(x["wall_seconds"], 2),
                fmt(x["query_seconds"], 2), fmt(x["recovery_seconds"], 3), fmt(x["recovered_objects"]),
                f"{fmt(x['rows'])} |"]))
    lines += ["", "| Group | Concurrency | Ratio | Per round | Geometric mean |",
              "| --- | ---: | --- | --- | ---: |"]
    for p in pairs:
        lines.append(f"| {p['group'] or '—'} | {p['concurrency']} | {p['ratio']} "
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
