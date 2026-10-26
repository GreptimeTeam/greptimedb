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

"""Fixed adapter for the pinned standalone O11yBench vmbench-lite comparison."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
import tomllib
import struct
import urllib.parse
import urllib.request

O_SHA = "46dfc3635ed1da84914bef50f4c488023747dd52"
T_SHA = "8c9b9927fbdb140e96d9d208facb7a7369a96187"
O_REPO = "https://github.com/GreptimeTeam/o11ybench.git"
T_REPO = "https://github.com/waynexia/tsdg.git"
HERE = Path(__file__).resolve()
ROOT = HERE.parents[2]
GATE = ROOT / ".github/scripts/native-promql-vmbench-validate.py"
TEST = ROOT / "tests/perf/test_native_promql_vmbench_lite.py"
DESCRIPTOR_KEYS = {"schema_version", "o11ybench_sha", "tsdg_sha", "phase", "start", "end", "step", "scan_partitions", "serial_warmup", "serial_tries", "duration_seconds", "warmup_seconds", "concurrencies", "rtol", "atol"}
SHA_RE = re.compile(r"[0-9a-f]{40}\Z")
SQL_AUDIT = []


def validate_descriptor(path: Path) -> dict:
    d = tomllib.loads(path.read_text(encoding="utf-8"))
    if not isinstance(d, dict) or set(d) != DESCRIPTOR_KEYS:
        raise ValueError("descriptor must contain exactly the fixed schema keys")
    if d["phase"] not in ("smoke", "baseline") or not isinstance(d["phase"], str):
        raise ValueError("phase must be smoke or baseline")
    fixed = {"schema_version": 1, "o11ybench_sha": O_SHA, "tsdg_sha": T_SHA,
             "start": "2025-03-21T09:50:00Z", "end": "2025-03-21T10:20:00Z",
             "step": "30s", "scan_partitions": 4, "concurrencies": [1, 4, 8]}
    for key, value in fixed.items():
        if type(d[key]) is not type(value) or d[key] != value:
            raise ValueError(f"descriptor {key} must equal {value!r}")
    if any(type(value) is not int for value in d["concurrencies"]):
        raise ValueError("concurrencies must be integers")
    expected = (1, 2, 2, 1) if d["phase"] == "smoke" else (10, 50, 60, 10)
    for key, value in zip(("serial_warmup", "serial_tries", "duration_seconds", "warmup_seconds"), expected):
        if type(d[key]) is not int or d[key] != value:
            raise ValueError(f"descriptor {key} must equal {value}")
    for key in ("rtol", "atol"):
        if type(d[key]) not in (int, float) or not math.isfinite(d[key]) or d[key] < 0:
            raise ValueError(f"descriptor {key} must be finite and nonnegative")
    return d


def header(native: bool) -> str:
    return f"X-Greptime-Hints:query.experimental_native_promql={'true' if native else 'false'}"


DB_URL = "http://127.0.0.1:14240/v1/prometheus/api/v1/query_range"


def lane_order(query_index: int, block: int) -> list[str]:
    native_first = (query_index + block) % 2 == 1
    return ["native", "legacy"] if native_first else ["legacy", "native"]


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered=sorted(values)
    return ordered[max(0, math.ceil(pct * len(ordered) / 100)-1)]


def split_tries(total: int) -> tuple[int, int]:
    if total < 2:
        raise ValueError("serial tries must be >= 2")
    return total // 2, total - total // 2


def run(command: list[str], *, cwd: Path | None = None, env: dict | None = None, log: Path | None = None) -> None:
    result = subprocess.run(command, cwd=cwd, env=env, text=True, capture_output=True)
    if log:
        log.parent.mkdir(parents=True, exist_ok=True)
        log.write_text(json.dumps({"command": command, "cwd": str(cwd) if cwd else None,
                                   "returncode": result.returncode, "stdout": result.stdout,
                                   "stderr": result.stderr}, indent=2) + "\n")
    if result.returncode:
        raise RuntimeError(f"command failed ({result.returncode}): {command!r}: {result.stderr}")


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for b in iter(lambda: f.read(1024 * 1024), b""):
            h.update(b)
    return h.hexdigest()


def git_checkout(repo: str, sha: str, dest: Path) -> None:
    run(["git", "init", str(dest)])
    run(["git", "-C", str(dest), "remote", "add", "origin", repo])
    run(["git", "-C", str(dest), "fetch", "--depth=1", "origin", sha])
    run(["git", "-C", str(dest), "checkout", "--detach", "FETCH_HEAD"])
    actual = subprocess.check_output(["git", "-C", str(dest), "rev-parse", "HEAD"], text=True).strip()
    if actual != sha:
        raise RuntimeError(f"pinned checkout mismatch: expected {sha}, got {actual}")


def version_identity(binary: Path, expected: str) -> dict:
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise ValueError("candidate binary must exist and be executable")
    if not SHA_RE.fullmatch(expected):
        raise ValueError("candidate ref must be a full commit SHA")
    p = subprocess.run([str(binary), "--version"], text=True, capture_output=True, timeout=15)
    match = re.search(r"^commit: ([0-9a-f]{40})$\n^clean: (true|false)$", p.stdout, re.M)
    if p.returncode or p.stderr.strip() or not match or match.group(1) != expected or match.group(2) != "true" or "commit:" not in p.stdout or "clean: true" not in p.stdout:
        raise ValueError("candidate --version must report the exact candidate commit and clean: true")
    return {"path": str(binary.resolve()), "sha256": sha256(binary), "version": p.stdout.strip(),
            "candidate_ref": expected, "clean": True}


def cpu_rss(pid: int) -> tuple[int, float]:
    raw = Path(f"/proc/{pid}/stat").read_text()
    fields = raw[raw.rfind(")") + 2 :].split()
    ticks = int(fields[11]) + int(fields[12])
    status = Path(f"/proc/{pid}/status").read_text()
    match = re.search(r"^VmRSS:\s+(\d+) kB", status, re.M)
    if match is None:
        raise RuntimeError("owned server VmRSS sample is missing")
    return ticks, int(match.group(1)) / 1024


def execute_client(command: list[str], env: dict, logdir: Path, label: str, pid: int) -> dict:
    logdir.mkdir(parents=True, exist_ok=True)
    if label in {"measurement", "warmup"} and (logdir / f"{label}.stdout").exists():
        raise RuntimeError(f"refusing to overwrite existing {label} artifacts in {logdir}")
    out, err, samples_path = (logdir / f"{label}.{suffix}" for suffix in ("stdout", "stderr", "samples.jsonl"))
    samples = []
    proc = None
    stdout_file = out.open("wb")
    stderr_file = err.open("wb")
    sample_file = samples_path.open("w")
    try:
        # Sample baseline before the benchmark process starts.
        before, _rss_before = cpu_rss(pid)
        started_mono = time.monotonic()
        started_utc = dt.datetime.now(dt.timezone.utc).isoformat()
        baseline={"monotonic":started_mono,"utc":started_utc,"ticks":before,"rss_mib":_rss_before}
        samples.append(baseline); sample_file.write(json.dumps(baseline)+"\n"); sample_file.flush()
        proc = subprocess.Popen(command, env=env, stdout=stdout_file, stderr=stderr_file)
        while proc.poll() is None:
            ticks, rss = cpu_rss(pid)
            sample = {"monotonic": time.monotonic(), "utc": dt.datetime.now(dt.timezone.utc).isoformat(), "ticks": ticks, "rss_mib": rss}
            samples.append(sample); sample_file.write(json.dumps(sample)+"\n"); sample_file.flush()
            time.sleep(.1)
        rc = proc.wait()
        after, rss_after = cpu_rss(pid)
        ended = time.monotonic(); ended_utc = dt.datetime.now(dt.timezone.utc).isoformat()
        sample = {"monotonic": ended, "utc": ended_utc, "ticks": after, "rss_mib": rss_after}
        samples.append(sample); sample_file.write(json.dumps(sample)+"\n"); sample_file.flush()
        if not samples or max(item["rss_mib"] for item in samples) <= 0:
            raise RuntimeError("owned server RSS sampling produced no positive samples")
        return {"returncode":rc,"wall_seconds":ended-started_mono,"started_at":started_utc,"ended_at":ended_utc,
                "cpu_seconds":max(0,after-before)/os.sysconf("SC_CLK_TCK"),"rss_peak_mib":max(x["rss_mib"] for x in samples),
                "sample_count":len(samples),"samples_jsonl":str(samples_path),"stdout":str(out),"stderr":str(err)}
    except BaseException:
        if proc is not None and proc.poll() is None:
            proc.terminate()
            try: proc.wait(timeout=5)
            except subprocess.TimeoutExpired: proc.kill(); proc.wait()
        raise
    finally:
        sample_file.close(); stdout_file.close(); stderr_file.close()


def validate_client_result(summary_path: Path, output_path: Path, query: str, native: bool,
                           expected_tries: int | None, expected_counts: dict, warmup: bool = False,
                           start: str = "2025-03-21T09:50:00Z", end: str = "2025-03-21T10:20:00Z") -> list[dict]:
    summary=json.loads(summary_path.read_text())
    rows=[json.loads(line) for line in output_path.read_text().splitlines() if line.strip()]
    expected_header=f"query.experimental_native_promql={'true' if native else 'false'}"
    if summary.get("query_count")!=1 or summary.get("errors")!=0 or summary.get("timeouts")!=0:
        raise RuntimeError(f"invalid benchmark summary counts: {summary}")
    if type(summary.get("successful_requests")) is not int or summary["successful_requests"] <= 0:
        raise RuntimeError("benchmark summary has no successful requests")
    if not rows or any(row.get("status")!="success" or row.get("error") or row.get("warmup") is not warmup for row in rows):
        raise RuntimeError("benchmark JSONL rows have errors or unexpected warmup status")
    if expected_tries is not None and len(rows)!=expected_tries:
        raise RuntimeError(f"expected {expected_tries} measured serial rows; got {len(rows)}")
    for row in rows:
        if row.get("query")!=query or row.get("start")!=start or row.get("end")!=end or row.get("effective_start",start)!=start or row.get("effective_end",end)!=end or row.get("step")!="30s":
            raise RuntimeError("benchmark JSONL query/window does not match requested canonical query")
        if not math.isfinite(float(row.get("latency_ms",float("nan")))) or float(row["latency_ms"])<0:
            raise RuntimeError("benchmark latency is invalid")
        headers=row.get("request_headers",{})
        if headers.get("X-Greptime-Hints")!=expected_header:
            raise RuntimeError("benchmark header does not match lane")
        if row.get("series")!=expected_counts.get("series") or row.get("points")!=expected_counts.get("points"):
            raise RuntimeError("timing result shape differs from qualification")
    per=summary.get("per_query")
    if not isinstance(per,list) or len(per)!=1 or per[0].get("success")!=len(rows) or per[0].get("errors")!=0 or per[0].get("timeouts")!=0 or summary.get("successful_requests")!=len(rows) or summary.get("measured_requests")!=len(rows) or summary.get("total_requests")!=len(rows):
        raise RuntimeError("benchmark summary does not match JSONL")
    return rows


def query_commands(py: Path, script: Path, query_file: Path, root: Path, query_id: str,
                   mode: str, native: bool, args, concurrency: int, tries: int | None,
                   duration: float | None, warmup: bool) -> tuple[list[str], Path, Path]:
    output = root / f"{query_id}-{mode}-{'native' if native else 'legacy'}-{concurrency}-{('warmup' if warmup else 'measure')}.jsonl"
    summary = output.with_suffix(".summary.json")
    command = [str(py), str(script), "--target", "greptimedb", "--url", args.url,
               "--queries", str(query_file), "--base", str(args.base_file), "--workload", "vmbench-lite", "--start", args.start, "--end", args.end,
               "--step", args.step, "--timeout", "120", "--mode", mode,
               "--header", header(native), "--output", str(output), "--summary-output", str(summary)]
    if mode == "serial":
        command += ["--tries", str(tries), "--warmup", "0"]
    else:
        command += ["--concurrency", str(concurrency), "--duration-seconds", str(duration), "--warmup-seconds", "0"]
    return command, output, summary


def http_post(url: str, body: bytes, headers: dict[str, str] | None = None) -> tuple[int, str]:
    request = urllib.request.Request(url, data=body, headers=headers or {}, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            return response.status, response.read().decode("utf-8", "replace")
    except Exception as exc:
        if hasattr(exc, "code"):
            return exc.code, exc.read().decode("utf-8", "replace")
        raise


def sql(url: str, statement: str) -> dict:
    status, text = http_post(url + "/v1/sql", urllib.parse.urlencode({"sql": statement, "format": "json"}).encode(),
                             {"Content-Type": "application/x-www-form-urlencoded"})
    SQL_AUDIT.append({"statement":statement,"http_status":status,"response":text})
    if status != 200:
        raise RuntimeError(f"SQL HTTP {status}: {text}")
    try:
        result = json.loads(text)
    except ValueError as exc:
        raise RuntimeError(f"SQL response is not JSON: {text[:500]}") from exc
    if not isinstance(result, dict) or result.get("code", 0) not in (0, "0", "SUCCESS", "success") or result.get("error"):
        raise RuntimeError(f"SQL failure: {result}")
    return result


def load_remote_write(tsdg: Path, original: Path, data: Path, url: str, logs: Path) -> dict:
    sys.path.insert(0, str(tsdg))
    from proto.remote_pb2 import WriteRequest
    import snappy
    chunks = data / "chunks"; chunks.mkdir()
    original_files = sorted(original.glob("*.bin"))
    series: dict[tuple, object] = {}
    original_multiset: dict[tuple, list] = {}
    output_multiset: dict[tuple, list] = {}
    metric_label_order = {}
    for path in original_files:
        decoded = WriteRequest.FromString(snappy.decompress(path.read_bytes()))
        for item in decoded.timeseries:
            pairs=[(label.name,label.value) for label in item.labels]
            labels = tuple(sorted(pairs))
            metric=dict(labels).get("__name__")
            if metric is not None:
                metric_label_order.setdefault(metric,[name for name,_ in pairs if name!="__name__"])
            prior=series.get(labels)
            if prior is None:
                series[labels] = item
            else:
                prior.samples.extend(item.samples)
            original_multiset.setdefault(labels,[]).extend((sample.timestamp,struct.pack(">d",sample.value)) for sample in item.samples)
    for item in series.values():
        item.samples.sort(key=lambda sample: sample.timestamp)
        timestamps=[sample.timestamp for sample in item.samples]
        if len(timestamps)!=216 or len(set(timestamps))!=216 or any(b-a!=10000 for a,b in zip(timestamps,timestamps[1:])):
            raise RuntimeError("source unique series must contain 216 distinct contiguous 10-second points")
    points = sum(len(item.samples) for item in series.values())
    if len(series) != 6880 or points != 1486080:
        raise RuntimeError(f"dataset cardinality mismatch: {len(series)} series, {points} points")
    by_metric: dict[str, list] = {}
    for labels,item in series.items():
        metric=dict(labels).get("__name__")
        if not metric or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*",metric): raise RuntimeError("invalid __name__")
        by_metric.setdefault(metric,[]).append((labels,item))
    schemas={}; per_metric={}; chunks_manifest=[]
    import copy
    for metric, rows in sorted(by_metric.items()):
        tags=[]
        tags=[name for name in metric_label_order.get(metric,[]) if name!="__name__"]
        for labels,_ in rows:
            for key,_ in labels:
                if key != "__name__" and key not in tags: tags.append(key)
        if any({k for k,_ in labels if k!="__name__"}!=set(tags) for labels,_ in rows): raise RuntimeError(f"tag schema mismatch: {metric}")
        schemas[metric]=tags
        per_metric[metric]={"tags":tags,"series":len(rows),"samples":sum(len(item.samples) for _,item in rows)}
        cols=["`greptime_timestamp` TIMESTAMP(3) NOT NULL","`greptime_value` DOUBLE NULL"]
        cols.extend(f"`{tag}` STRING NULL"+(" INVERTED INDEX" if tag=="region" or (metric=="node_cpu_seconds_total" and tag=="mode") else "") for tag in tags)
        ddl=f"CREATE TABLE `{metric}` ("+", ".join(cols)+", TIME INDEX (`greptime_timestamp`), PRIMARY KEY ("+", ".join(f"`{tag}`" for tag in tags)+")) ENGINE=mito"
        sql(url,ddl)
        created=sql(url,f"SHOW CREATE TABLE `{metric}`")
        (logs/f"show-create-{metric}.json").write_text(json.dumps(created,indent=2)+"\n")
        buckets=[[] for _ in range(4)]
        for _,item in rows:
            ordered=sorted(item.samples,key=lambda x:x.timestamp)
            if len(ordered)!=216: raise RuntimeError("each time series must have 216 samples")
            for part in range(4):
                clone=copy.deepcopy(item); del clone.samples[:]; clone.samples.extend(ordered[part*54:(part+1)*54]); buckets[part].append(clone)
        for part,items in enumerate(buckets):
            payload=snappy.compress(WriteRequest(timeseries=items).SerializeToString())
            path=chunks/f"{metric}-{part}.bin"; path.write_bytes(payload)
            check=WriteRequest.FromString(snappy.decompress(path.read_bytes()))
            if len(check.timeseries)!=len(items) or sum(len(x.samples) for x in check.timeseries)!=len(items)*54: raise RuntimeError("chunk round-trip failed")
            for actual in check.timeseries:
                labels=tuple(sorted((label.name,label.value) for label in actual.labels))
                output_multiset.setdefault(labels,[]).extend((sample.timestamp,struct.pack(">d",sample.value)) for sample in actual.samples)
            status,body=http_post(url+"/v1/prometheus/write",payload,{"Content-Type":"application/x-protobuf","Content-Encoding":"snappy","X-Prometheus-Remote-Write-Version":"0.1.0"})
            if not 200<=status<300: raise RuntimeError(f"remote write HTTP {status}: {body[:500]}")
            sql(url,f"ADMIN FLUSH_TABLE('`{metric}`')")
            chunks_manifest.append({"path":str(path.relative_to(data)),"sha256":sha256(path),"metric":metric,"chunk":part,"series":len(items),"points":len(items)*54})
    expected={labels:sorted(points) for labels,points in original_multiset.items()}
    actual={labels:sorted(points) for labels,points in output_multiset.items()}
    if expected != actual: raise RuntimeError("re-encoded chunk dataset differs from exact label/timestamp/IEEE-value multiset")
    manifest={"original_files":[{"path":str(x.relative_to(original)),"sha256":sha256(x)} for x in original_files],"chunks":chunks_manifest,"series":len(series),"points":points,"metrics":schemas,"per_metric":per_metric,"exact_multiset_equality":True}
    manifest["dataset_id"]=hashlib.sha256(json.dumps(manifest,sort_keys=True).encode()).hexdigest()
    (logs/"load-proof.json").write_text(json.dumps(manifest,indent=2)+"\n")
    return manifest


def main_run(args, d: dict, root: Path, candidate: dict) -> None:
    logs = root / "logs"; logs.mkdir(parents=True, exist_ok=True)
    run([sys.executable, str(TEST)], log=logs / "adapter-preflight.json")
    run([sys.executable, str(ROOT / "tests/perf/test_native_promql_vmbench_validate.py")], log=logs / "gate-preflight.json")
    (logs / "classification.json").write_text(json.dumps({"classification": "CI smoke" if d["phase"] == "smoke" else "controlled baseline"}, indent=2)+"\n")
    candidate["base_ref_not_measured"] = args.base_ref
    candidate["run_url_provenance"] = args.run_url
    (logs / "candidate-identity.json").write_text(json.dumps(candidate, indent=2)+"\n")
    deps = root / "deps"; o = deps / "o11ybench"; t = deps / "tsdg"
    deps.mkdir(exist_ok=True)
    if subprocess.check_output(["git", "-C", str(o), "rev-parse", "HEAD"], text=True).strip() != O_SHA or subprocess.check_output(["git", "-C", str(t), "rev-parse", "HEAD"], text=True).strip() != T_SHA:
        raise RuntimeError("prepared pinned checkout SHA mismatch")
    venv_python = t / ".venv/bin/python"
    run(["uv", "pip", "freeze", "--python", str(venv_python)], log=logs / "dependencies-freeze.json")
    args.base_file=o/"base.sh"
    (logs / "runtime-python.json").write_text(json.dumps({"python": str(venv_python)}, indent=2)+"\n")
    plan=o/"scripts/o11ybench_plan.py"; planpath=logs/"planner.json"
    (logs/"dataset/original").mkdir(parents=True,exist_ok=True)
    run([str(venv_python),str(plan),"--workload","vmbench-lite","--target","existing-url:name=narthex-same-build,family=greptimedb,url="+DB_URL,"--output-root",str(root/"planned"),"--output-plan",str(planpath),"--data-home",str(logs/"dataset/original"),"--run-classification","local_smoke_only" if d["phase"]=="smoke" else "controlled_server_benchmark","--mode","serial","--mode","concurrent-duration","--concurrency","1,4,8","--serial-tries",str(d["serial_tries"]),"--serial-warmup",str(d["serial_warmup"]),"--duration-seconds",str(d["duration_seconds"]),"--warmup-seconds",str(d["warmup_seconds"]),"--no-report","--no-open-browser"],log=logs/"planner-command.json")
    run([str(venv_python),str(o/"scripts/run_benchmark_plan.py"),"--plan",str(planpath),"--dry-run"],log=logs/"planner-dry-run.json")
    planner=json.loads(planpath.read_text())
    if planner.get("data",{}).get("generate") is not True or planner.get("targets",[{}])[0].get("url")!=DB_URL or planner.get("query",{}).get("concurrencies")!=[1,4,8]:
        raise RuntimeError("dry-run planner does not match fixed vmbench-lite execution contract")
    dataset = logs / "dataset"; data = dataset / "original"; data.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy(); env.update({"TSDG_PREINSTALLED":"1", "TSDG_DIR":str(t), "TSDG_PARALLELISM":"1",
        "DATA_HOME":str(data), "TSDG_CONFIG":str(o / "tsdg-configs/vmbench-lite.yaml"),
        "TSDG_SUMMARY_OUTPUT":str(dataset / "summary.json"), "TSDG_PROFILE":"vmbench-lite"})
    run([str(venv_python), str(o / "scripts/download_data.py")], env=env, log=logs / "generation.json")
    files=sorted(p for p in data.rglob("*") if p.is_file())
    manifest={"files":[{"path":str(p.relative_to(data)),"sha256":sha256(p),"bytes":p.stat().st_size} for p in files]}
    manifest["dataset_id"]=hashlib.sha256(json.dumps(manifest,sort_keys=True).encode()).hexdigest()
    (dataset / "frozen-manifest.json").write_text(json.dumps(manifest,indent=2)+"\n")
    (logs/"run-context.json").write_text(json.dumps({"candidate":candidate,"base_ref_not_measured":args.base_ref,"github_run_url":args.run_url,"o11ybench_sha":O_SHA,"tsdg_sha":T_SHA,"phase":d["phase"],"planner_command":str(logs/"planner-command.json"),"data_home":str(data),"frozen_dataset_id":manifest["dataset_id"]},indent=2)+"\n")
    sys.path.insert(0,str(o/"scripts"))
    import managed_greptimedb
    candidate_bin=Path(args.candidate_bin).resolve()
    config_path=logs/"query-settings.toml"
    config_path.write_text("[query]\nparallelism = 4\nexperimental_native_promql = true\nallow_query_fallback = false\n")
    target={"binary":{"path":str(candidate_bin),"sha256":candidate["sha256"]},"config":{"path":str(config_path),"sha256":sha256(config_path)},"url":DB_URL,"process":{"env":{}}}
    server_root=logs/"server"; server_root.mkdir()
    server=managed_greptimedb.ManagedGreptimeDB(target,server_root)
    try:
        server.start(); server.wait_for_health("http://127.0.0.1:14240/health",90); pid=server.process.pid
        base_url="http://127.0.0.1:14240"
        (logs/"metrics-before.prom").write_text(urllib.request.urlopen(base_url+"/metrics",timeout=10).read().decode())
        for path in ("/v1/health","/ready","/config","/status"):
            (logs/("startup-"+path.strip("/").replace("/","-")+".json")).write_bytes(urllib.request.urlopen(base_url+path,timeout=10).read())
        server.ensure_running(); server.evidence(base_url+"/v1/health",logs/"server-startup-evidence")
        manifest=load_remote_write(t,data,data,base_url,logs)
        (logs/"metrics-after-load.prom").write_text(urllib.request.urlopen(base_url+"/metrics",timeout=10).read().decode())
        health=urllib.request.urlopen(base_url+"/v1/health",timeout=10); (logs/"health-post-load.txt").write_bytes(health.read())
        # Explicitly audit each table's engine/options and visible row totals.
        audits=[]
        where="table_schema='public' AND table_name IN ("+','.join("'"+m+"'" for m in sorted(manifest["metrics"]))+")"
        tables=sql(base_url,"SELECT table_schema,table_name,table_id,engine,create_options FROM information_schema.tables WHERE "+where+" ORDER BY table_name")
        table_rows=tables.get("data",[])
        if len(table_rows)!=11 or any(row.get("engine","").lower()!="mito" for row in table_rows): raise RuntimeError("table inventory is not exactly 11 Mito tables")
        peers=sql(base_url,"SELECT * FROM information_schema.region_peers WHERE "+where+" ORDER BY table_name,region_id").get("data",[])
        if len(peers)!=11 or any(sum(p.get("table_name")==metric for p in peers)!=1 for metric in manifest["metrics"]): raise RuntimeError("every metric must have exactly one actual region")
        region_ids=sorted({row["region_id"] for row in peers})
        infos=sql(base_url,"SELECT * FROM information_schema.region_info WHERE region_id IN ("+','.join(str(i) for i in region_ids)+") ORDER BY region_id").get("data",[])
        if len(infos)!=11: raise RuntimeError("expected exactly 11 region-info rows")
        for info in infos:
            options=info.get("region_options",{})
            if isinstance(options,str): options=json.loads(options)
            if options.get("append_mode") is not False: raise RuntimeError("region append_mode is not false")
        audits=[]
        for metric,tags in manifest["metrics"].items():
            count=sql(base_url,f"SELECT COUNT(*) AS samples FROM `{metric}`").get("data",[])
            series_count=sql(base_url,f"SELECT COUNT(*) AS series FROM (SELECT "+", ".join(f"`{tag}`" for tag in tags)+f" FROM `{metric}` GROUP BY "+", ".join(f"`{tag}`" for tag in tags)+") AS series_keys").get("data",[])
            expected_series=manifest["per_metric"][metric]["series"]
            expected_samples=manifest["per_metric"][metric]["samples"]
            if not count or count[0].get("samples")!=expected_samples or not series_count or series_count[0].get("series")!=expected_series: raise RuntimeError(f"row/series audit mismatch for {metric}")
            audits.append({"metric":metric,"samples":expected_samples,"series":expected_series,"region_ids":[p["region_id"] for p in peers if p["table_name"]==metric]})
        ssts=sql(base_url,"SELECT * FROM information_schema.ssts_manifest WHERE table_id IN ("+','.join(str(t["table_id"]) for t in table_rows)+") ORDER BY table_id,sequence,min_ts").get("data",[])
        if len({p.get("region_id") for p in peers})!=11: raise RuntimeError("expected one distinct region per metric")
        (logs/"table-audit.json").write_text(json.dumps({"tables":table_rows,"peers":peers,"regions":infos,"per_table":audits,"sst_manifest":ssts,"single_region_per_table":True,"append_mode":False},indent=2)+"\n")

        queries=[q for q in (o/"queries-vmbench-lite.txt").read_text().splitlines() if q.strip()]
        if len(queries)!=12: raise RuntimeError("canonical query file must contain 12 queries")
        qroot=logs/"queries"; qroot.mkdir()
        for i,q in enumerate(queries,1):
            (qroot/f"lite-{i:02d}.txt").write_text(q+"\n")
            (qroot/f"lite-{i:02d}.json").write_text(json.dumps({"query_id":f"lite-{i:02d}","original_index":i,"query":q,"sha256":hashlib.sha256(q.encode()).hexdigest()},indent=2)+"\n")
        shutil.copyfile(o/"queries-vmbench-lite.txt",logs/"queries-vmbench-lite.txt")
        (logs/"queries-vmbench-lite.sha256").write_text(sha256(o/"queries-vmbench-lite.txt")+"\n")
        qualification=logs/"qualification"
        gatecmd=[str(venv_python),str(GATE),"--query-file",str(o/"queries-vmbench-lite.txt"),"--url",target["url"],"--start",d["start"],"--end",d["end"],"--step",d["step"],"--output-dir",str(qualification),"--expected-partitions",str(d["scan_partitions"]),"--rtol",str(d["rtol"]),"--atol",str(d["atol"])]
        run(gatecmd,log=logs/"qualification-command.json")
        qr=json.loads((qualification/"qualification.json").read_text())
        if len(qr.get("records",[]))!=12 or not all(r.get("qualified") for r in qr["records"]): raise RuntimeError("correctness/plan qualification failed")
        a,b=split_tries(d["serial_tries"]); measurements=[]
        for mode,concurrency in [("serial",1)]+[("concurrent-duration",c) for c in d["concurrencies"]]:
            for qi in range(1,13):
                for block in range(2):
                    for lane in lane_order(qi-1,block):
                        native=lane=="native"; folder=logs/"timing"/mode/f"c{concurrency}"/f"lite-{qi:02d}"/f"block-{block+1}"/lane; folder.mkdir(parents=True,exist_ok=True)
                        order=lane_order(qi-1,block)
                        warmtries=d["serial_warmup"] if mode=="serial" else None
                        warmduration=d["warmup_seconds"] if mode!="serial" else None
                        server.ensure_running()
                        server.evidence(base_url+"/v1/health",folder/"server-before-warmup")
                        (folder/"metrics-before.prom").write_text(urllib.request.urlopen(base_url+"/metrics",timeout=10).read().decode())
                        cmd,wout,wsummary=query_commands(venv_python,o/"scripts/query_bench.py",qroot/f"lite-{qi:02d}.txt",folder,f"lite-{qi:02d}",mode,native,args,concurrency,warmtries,warmduration,True)
                        run(cmd,env=os.environ.copy(),log=folder/"warmup-command.json")
                        query_text=queries[qi-1]; counts=qr["records"][qi-1].get("labels_series_point_counts",{}).get("native" if native else "legacy",{})
                        warm_rows=validate_client_result(wsummary,wout,query_text,native,d["serial_warmup"] if mode=="serial" else None,counts,False,d["start"],d["end"])
                        tries=(a if block==0 else b) if mode=="serial" else None; duration=d["duration_seconds"]/2 if mode!="serial" else None
                        cmd,out,summary=query_commands(venv_python,o/"scripts/query_bench.py",qroot/f"lite-{qi:02d}.txt",folder,f"lite-{qi:02d}",mode,native,args,concurrency,tries,duration,False)
                        env=os.environ.copy(); env["GREPTIME_ADAPTER_LANE"]=lane
                        measurement=execute_client(cmd,env,folder,"measurement",pid)
                        if measurement["returncode"] or not out.is_file() or not summary.is_file(): raise RuntimeError(f"measurement failed: {measurement}")
                        rows=validate_client_result(summary,out,query_text,native,tries,counts,False,d["start"],d["end"])
                        server.ensure_running(); server.evidence(base_url+"/v1/health",folder/"server-after-measurement")
                        (folder/"metrics-after.prom").write_text(urllib.request.urlopen(base_url+"/metrics",timeout=10).read().decode())
                        record={"query_id":f"lite-{qi:02d}","mode":mode,"concurrency":concurrency,"block":block+1,"lane":lane,"counterbalanced_order":order,"pid":pid,"binary_sha256":candidate["sha256"],"dataset_id":manifest["dataset_id"],"external_warmup_rows":len(warm_rows),"summary":str(summary),"jsonl":str(out),"configured_duration_seconds":duration,"configured_serial_tries":tries,"sample_scope":"owned PID CPU/RSS across client envelope including idle; excludes warmup/load; RSS is sampled peak, not HWM",**measurement}
                        record["command"] = cmd
                        record["explicit_environment"] = {"GREPTIME_ADAPTER_LANE":lane}
                        record["server_effective_config"] = str(server_root/"effective-config.toml")
                        (folder/"measurement-record.json").write_text(json.dumps(record,indent=2)+"\n")
                        measurements.append(record)
        (logs/"measurements.json").write_text(json.dumps(measurements,indent=2)+"\n")
        rows=[]
        for qi,record in enumerate(qr["records"],1):
            group=[m for m in measurements if m["query_id"]==f"lite-{qi:02d}"]
            lanes={}
            for lane in ("legacy","native"):
                lane_blocks=[m for m in group if m["lane"]==lane and m["mode"]=="serial"]
                lat=[]
                for block in lane_blocks:
                    for line in Path(block["jsonl"]).read_text().splitlines():
                        item=json.loads(line)
                        if item.get("status")=="success" and not item.get("warmup"):
                            lat.append(float(item["latency_ms"]))
                rss_values=[item["rss_peak_mib"] for item in lane_blocks]
                if not rss_values or any(value is None for value in rss_values): raise RuntimeError("missing server RSS samples in serial summary")
                lanes[lane]={"latencies_ms":lat,"p50_ms":percentile(lat,50),"p95_ms":percentile(lat,95),"cpu_seconds":sum(item["cpu_seconds"] for item in lane_blocks),"rss_peak_mib":max(rss_values)}
            eligible=record.get("eligible_for_native_statistics") is True
            concurrent={}
            for c in d["concurrencies"]:
                concurrent[str(c)]={}
                for lane in ("legacy","native"):
                    items=[m for m in group if m["mode"]=="concurrent-duration" and m["concurrency"]==c and m["lane"]==lane]
                    latencies=[]; successes=0
                    for item in items:
                        for line in Path(item["jsonl"]).read_text().splitlines():
                            entry=json.loads(line)
                            if entry.get("status")=="success" and not entry.get("warmup"):
                                successes+=1; latencies.append(float(entry["latency_ms"]))
                    configured_seconds=d["duration_seconds"] * sum(1 for item in items if item["mode"]=="concurrent-duration") / 2
                    concurrent[str(c)][lane]={"successes":successes,"configured_seconds":configured_seconds,"wall_seconds":sum(item["wall_seconds"] for item in items),"qps":successes/configured_seconds if configured_seconds else None,"p50_ms":percentile(latencies,50),"p95_ms":percentile(latencies,95),"cpu_seconds":sum(item["cpu_seconds"] for item in items),"rss_peak_mib":max(item["rss_peak_mib"] for item in items) if items and all(item["rss_peak_mib"] is not None for item in items) else None}
            rows.append({"query_id":record["query_id"],"query":record["query"],"route":record.get("route"),"correctness":record.get("correctness"),"native_eligible":eligible,"fused":record.get("has_fused"),"finalize":record.get("has_finalize"),"observed_partitions":record.get("observed_partitions"),"full_plan_paths":record.get("full_plan_paths"),"serial":lanes,"speedup":(lanes["legacy"]["p50_ms"]/lanes["native"]["p50_ms"] if eligible and lanes["native"]["p50_ms"] else None),"concurrent":concurrent})
        (logs/"summary.json").write_text(json.dumps({"rows":rows,"candidate":candidate,"base_ref":args.base_ref,"dataset_id":manifest["dataset_id"]},indent=2)+"\n")
        table=[("SMOKE_NOT_BASELINE: these measurements validate the harness; they do not establish an optimization claim." if d["phase"]=="smoke" else "Controlled baseline: report measurements without an optimization claim."),"| Query | Expression | Route | Correct | Native eligible | Partitions | Finalize | Serial Legacy p50/p95 ms | Serial Native p50/p95 ms | CPU Legacy/Native s | RSS peak Legacy/Native MiB | Speedup |","|---|---|---|---|---|---:|---|---:|---:|---:|---:|---:|"]
        for row in rows:
            l=row["serial"]["legacy"]; n=row["serial"]["native"]
            expression=row["query"].replace("|","\\|")
            table.append(f"| {row['query_id']} | `{expression}` | {row['route']} | {row['correctness']} | {row['native_eligible']} | {row['observed_partitions']} | {row['finalize']} | {l['p50_ms']}/{l['p95_ms']} | {n['p50_ms']}/{n['p95_ms']} | {l['cpu_seconds']}/{n['cpu_seconds']} | {l['rss_peak_mib']}/{n['rss_peak_mib']} | {row['speedup']} |")
        table.extend(["", "## Concurrent duration results", "", "QPS uses successful requests divided by configured measured seconds; tails are p95 latency in milliseconds.", "", "| Query | Concurrency | Legacy QPS / p95 ms | Native QPS / p95 ms | Legacy/Native CPU s | RSS peak MiB |", "|---|---:|---:|---:|---:|---:|"])
        for row in rows:
            for c in d["concurrencies"]:
                pair=row["concurrent"][str(c)]; legacy=pair["legacy"]; native=pair["native"]
                table.append(f"| {row['query_id']} | {c} | {legacy['qps']} / {legacy['p95_ms']} | {native['qps']} / {native['p95_ms']} | {legacy['cpu_seconds']}/{native['cpu_seconds']} | {legacy['rss_peak_mib']}/{native['rss_peak_mib']} |")
        (logs/"summary.md").write_text("\n".join(table)+"\n")
        (root/"query-regression-report.json").write_text(json.dumps({"case_path":"tests/perf/native_promql_vmbench_lite/benchmark.toml","case":{"name":"native_promql_vmbench_lite"},"status":"ok","query_mode":"same-build Legacy/Native; see logs/summary.md","classification":"CI smoke" if d["phase"]=="smoke" else "controlled baseline","targets":[{"name":"candidate standalone (same binary, both lanes)","status":"measured","metrics":{"queries":12,"qualified_native_queries":sum(row["native_eligible"] for row in rows),"candidate_ref":args.candidate_ref,"base_ref_not_measured":args.base_ref}}],"thresholds":[]},indent=2)+"\n")
    finally:
        (logs/"sql-requests.jsonl").write_text("".join(json.dumps(item)+"\n" for item in SQL_AUDIT))
        cleanup=server.stop() if server.process is not None else {"status":"not-started"}
        (logs/"server-final.json").write_text(json.dumps({"cleanup":cleanup,"pid":server.process.pid if server.process else None,"exit_code":server.process.poll() if server.process else None},indent=2)+"\n")
        if cleanup.get("status")!="pass":
            (logs/"cleanup-failure.json").write_text(json.dumps(cleanup,indent=2)+"\n")
            (root/"query-regression-report.json").write_text(json.dumps({"case_path":"tests/perf/native_promql_vmbench_lite/benchmark.toml","case":{"name":"native_promql_vmbench_lite"},"status":"failed","query_mode":"same-build Legacy/Native; see logs/","targets":[{"name":"candidate standalone (same binary, both lanes)","status":"failed"}],"thresholds":[]},indent=2)+"\n")
            raise RuntimeError(f"managed server cleanup failed: {cleanup}")


def main() -> int:
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--descriptor",required=True); parser.add_argument("--candidate-bin",required=True)
    parser.add_argument("--candidate-ref",required=True); parser.add_argument("--base-ref",required=True)
    parser.add_argument("--work-dir",required=True); parser.add_argument("--run-url",required=True)
    parser.add_argument("--prepared-dependencies",action="store_true",help=argparse.SUPPRESS)
    args=parser.parse_args(); root=Path(args.work_dir).resolve(); root.mkdir(parents=True,exist_ok=True)
    logs=root/"logs"; logs.mkdir(exist_ok=True)
    try:
        d=validate_descriptor(Path(args.descriptor))
        if not SHA_RE.fullmatch(args.candidate_ref) or not SHA_RE.fullmatch(args.base_ref):
            raise ValueError("candidate-ref and base-ref must be full 40-character lowercase SHAs")
        candidate=version_identity(Path(args.candidate_bin).resolve(),args.candidate_ref)
        candidate["base_ref_not_measured"]=args.base_ref
        candidate["run_url_provenance"]=args.run_url
        (logs/"candidate-identity.json").write_text(json.dumps(candidate,indent=2)+"\n")
        if not args.prepared_dependencies:
            deps=root/"deps"; deps.mkdir(exist_ok=True)
            t=deps/"tsdg"
            git_checkout(T_REPO,T_SHA,t)
            run(["uv","sync","--locked","--python","3.13.1"],cwd=t,log=logs/"tsdg-sync-bootstrap.json")
            o=deps/"o11ybench"; git_checkout(O_REPO,O_SHA,o)
            if subprocess.check_output(["git","-C",str(o),"rev-parse","HEAD"],text=True).strip()!=O_SHA or subprocess.check_output(["git","-C",str(t),"rev-parse","HEAD"],text=True).strip()!=T_SHA:
                raise RuntimeError("pinned tool checkout verification failed")
            run(["uv","pip","install","--python",str(t/".venv/bin/python"),"-r",str(o/"scripts/requirements.txt")],log=logs/"requirements-bootstrap.json")
            command=[str(t/".venv/bin/python"),str(HERE),"--descriptor",args.descriptor,"--candidate-bin",args.candidate_bin,"--candidate-ref",args.candidate_ref,"--base-ref",args.base_ref,"--work-dir",args.work_dir,"--run-url",args.run_url,"--prepared-dependencies"]
            return subprocess.run(command).returncode
        args.url=DB_URL; args.start="2025-03-21T09:50:00Z"; args.end="2025-03-21T10:20:00Z"; args.step="30s"
        main_run(args,d,root,candidate); return 0
    except BaseException as error:
        (logs/"failure.json").write_text(json.dumps({"error":str(error),"type":type(error).__name__},indent=2)+"\n")
        report={"case_path":"tests/perf/native_promql_vmbench_lite/benchmark.toml","case":{"name":"native_promql_vmbench_lite"},"status":"failed","query_mode":"same-build Legacy/Native; see logs/","targets":[{"name":"candidate standalone (same binary, both lanes)","status":"failed"}],"thresholds":[]}
        (root/"query-regression-report.json").write_text(json.dumps(report,indent=2)+"\n")
        print(f"adapter failed: {error}",file=sys.stderr); return 1

if __name__ == "__main__":
    raise SystemExit(main())
