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

"""Base-vs-candidate query performance regression runner."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import shutil
import socket
import statistics
import subprocess
import sys
import tempfile
import time
import tomllib
import unittest
from unittest.mock import patch
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any


FICLONE = 0x40049409


@dataclass(frozen=True)
class RunTarget:
    name: str
    binary: Path
    work_dir: Path
    data_dir: Path
    fixture_dir: Path
    report_path: Path
    http_port: int
    grpc_port: int
    mysql_port: int
    postgres_port: int
    metasrv_rpc_port: int
    metasrv_http_port: int
    datanode_rpc_port: int
    datanode_http_port: int
    datanode_data_dir: Path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run a query perf case against base and candidate binaries.")
    p.add_argument("--case", required=True, type=Path)
    p.add_argument("--base-bin", required=True, type=Path)
    p.add_argument("--candidate-bin", required=True, type=Path)
    p.add_argument("--fixture-generator", type=Path)
    p.add_argument("--remote-write-generator", type=Path, help="deprecated: use --fixture-generator query_perf_fixture")
    p.add_argument("--storage-inspector", type=Path, help="deprecated: use --fixture-generator query_perf_fixture")
    p.add_argument("--work-dir", required=True, type=Path)
    p.add_argument("--fixture-cache-dir", type=Path, help="persistent directory for generated fixtures, keyed by case content")
    p.add_argument("--reuse-fixture", action="store_true")
    p.add_argument("--allow-large-fixture", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--fixture-only", action="store_true", help="old smoke mode: generate/materialize fixture only")
    p.add_argument("--reuse-work-dir", action="store_true", help="allow non-empty base/candidate work dirs")
    p.add_argument("--http-timeout", type=float, default=120.0, help="HTTP SQL timeout seconds")
    return p.parse_args()


def load_case(path: Path) -> dict[str, Any]:
    with path.open("rb") as f:
        return tomllib.load(f)


def load_normalized_case(case_path: Path, fixture_generator: Path | None) -> dict[str, Any]:
    if fixture_generator is None:
        raise ValueError("--fixture-generator query_perf_fixture is required for Rust-owned case planning")
    result = run_command([str(fixture_generator), "plan", "--case", str(case_path)])
    if result["returncode"] != 0:
        raise RuntimeError(f"query_perf_fixture plan failed: {result['stderr'][:2000]}")
    plan = json.loads(result["stdout"])
    return {"case": load_case(case_path).get("case", {}), "scenario": plan["scenario"], "schema_version": plan.get("schema_version")}


def require_binary(path: Path, name: str, *, dry_run: bool) -> None:
    if dry_run:
        return
    if not path.exists():
        raise FileNotFoundError(f"{name} binary does not exist: {path}")
    if not os.access(path, os.X_OK):
        raise PermissionError(f"{name} binary is not executable: {path}")


def allocate_ports(n: int) -> list[int]:
    socks = []
    try:
        for _ in range(n):
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind(("127.0.0.1", 0))
            socks.append(s)
        return [s.getsockname()[1] for s in socks]
    finally:
        for s in socks:
            s.close()


def make_target(name: str, binary: Path, root: Path, ports: list[int], fixture_dir: Path | None = None) -> RunTarget:
    work_dir = root / name
    return RunTarget(
        name,
        binary,
        work_dir,
        work_dir / "cluster_data",
        fixture_dir or root / "fixture",
        work_dir / "report.json",
        ports[4],
        ports[5],
        ports[6],
        ports[7],
        ports[0],
        ports[1],
        ports[2],
        ports[3],
        work_dir / "datanode-0" / "data",
    )


def run_command(cmd: list[str], cwd: Path | None = None) -> dict[str, Any]:
    started = time.monotonic()
    proc = subprocess.run(cmd, cwd=cwd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
    return {"cmd": cmd, "cwd": str(cwd) if cwd else None, "returncode": proc.returncode, "elapsed_seconds": time.monotonic() - started, "stdout": proc.stdout, "stderr": proc.stderr}


def sql_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def column_sql(col: dict[str, Any]) -> str:
    return f"{sql_ident(col['name'])} {col['type']}"


def scenario(case: dict[str, Any]) -> dict[str, Any]:
    value = case.get("scenario")
    if not isinstance(value, dict):
        raise ValueError("case requires [scenario] with kind = 'direct_readable_sst' or 'prom_remote_write_then_query'")
    kind = value.get("kind")
    if kind not in ("direct_readable_sst", "prom_remote_write_then_query"):
        raise ValueError(f"unsupported scenario kind {kind!r}; supported: 'direct_readable_sst', 'prom_remote_write_then_query'")
    return value


def case_tables(case: dict[str, Any]) -> list[dict[str, Any]]:
    value = scenario(case)
    if value.get("kind") == "prom_remote_write_then_query":
        remote = value["remote_write"]
        metric = remote["metric"]
        database = remote["database"]
        return [{"database": database, "name": metric, "engine": "metric", "validate_show_create_engine": False}]
    tables = value.get("tables") or []
    if not tables or value.get("layout", {}).get("regions") != 1:
        raise ValueError("runner supports one or more tables and exactly one region per table")
    pairs = [(table.get("database"), table.get("name")) for table in tables]
    if len(set(pairs)) != len(pairs):
        raise ValueError("duplicate (database, name) table entries are not supported")
    names = [table.get("name") for table in tables]
    if len(set(names)) != len(names):
        raise ValueError("duplicate table names are not supported because fixture generator --table selects by name")
    return list(tables)


def fixture_subdir(table: dict[str, Any], index: int) -> str:
    raw = f"{index:02d}_{table['database']}_{table['name']}"
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", raw).strip("._-")
    return safe or f"table_{index:02d}"


def safe_path_component(raw: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", raw).strip("._-")
    return safe or "query_perf_case"


def fixture_root(work_root: Path, case_path: Path, case: dict[str, Any], fixture_cache_dir: Path | None) -> Path:
    if fixture_cache_dir is None:
        return work_root / "fixture"
    case_name = case.get("case", {}).get("name") or case_path.parent.name or case_path.stem
    fixture_config = dict(scenario(case))
    fixture_config.pop("queries", None)
    digest = hashlib.sha256(json.dumps(fixture_config, sort_keys=True).encode()).hexdigest()[:16]
    return fixture_cache_dir.resolve() / f"{safe_path_component(case_name)}-{digest}"


def table_fixture_dir(root: Path, tables: list[dict[str, Any]], table: dict[str, Any], index: int) -> Path:
    return root if len(tables) == 1 else root / fixture_subdir(table, index)


def create_table_sql(table: dict[str, Any]) -> str:
    cols = ",\n  ".join(column_sql(c) for c in table["columns"])
    pk = ", ".join(sql_ident(c) for c in table.get("primary_key", []))
    opts = []
    if "append_mode" in table:
        opts.append(("append_mode", str(table["append_mode"]).lower()))
    if table.get("sst_format"):
        opts.append(("sst_format", table["sst_format"]))
    with_sql = ""
    if opts:
        with_sql = "\nWITH (" + ", ".join(f"'{k}'='{v}'" for k, v in opts) + ")"
    return f"CREATE TABLE {sql_ident(table['name'])} (\n  {cols},\n  TIME INDEX ({sql_ident(table['time_index'])}),\n  PRIMARY KEY ({pk})\n) ENGINE=mito{with_sql};"


def planned_queries(case: dict[str, Any]) -> list[dict[str, Any]]:
    queries = scenario(case).get("queries", [])
    if isinstance(queries, dict):
        return [{"name": name, **value} for name, value in queries.items()]
    return list(queries)


def http_post_sql(port: int, sql: str, db: str, timeout: float) -> dict[str, Any]:
    data = urllib.parse.urlencode({"sql": sql, "db": db, "format": "json"}).encode()
    req = urllib.request.Request(f"http://127.0.0.1:{port}/v1/sql", data=data, method="POST")
    started = time.monotonic()
    elapsed_ms = (time.monotonic() - started) * 1000.0
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode()
            status = resp.status
        elapsed_ms = (time.monotonic() - started) * 1000.0
        try:
            body: Any = json.loads(raw)
        except json.JSONDecodeError:
            body = {"raw": raw}
        ok = status < 400 and not response_has_error(body)
        return {"ok": ok, "status": status, "latency_ms": elapsed_ms, "response": body, "sql": sql}
    except urllib.error.HTTPError as e:
        elapsed_ms = (time.monotonic() - started) * 1000.0
        raw = e.read().decode(errors="replace")
        try:
            body = json.loads(raw)
        except json.JSONDecodeError:
            body = {"raw": raw}
        return {
            "ok": False,
            "status": e.code,
            "latency_ms": elapsed_ms,
            "response": body,
            "error": repr(e),
            "sql": sql,
        }
    except Exception as e:  # noqa: BLE001 - report HTTP/query failures as JSON
        elapsed_ms = (time.monotonic() - started) * 1000.0
        return {"ok": False, "status": None, "latency_ms": elapsed_ms, "error": repr(e), "sql": sql}


def response_has_error(body: Any) -> bool:
    """Detect top-level GreptimeDB HTTP error envelopes without inspecting rows.

    Query outputs may legitimately contain columns named `code` or `error`, so
    this intentionally avoids recursive checks through result rows.
    """
    if isinstance(body, dict):
        if body.get("error") or body.get("err_msg") or body.get("error_msg"):
            return True
        if "error_code" in body and str(body.get("error_code", "")).lower() not in ("", "0", "success"):
            return True
        if "code" in body and "output" not in body and str(body.get("code", "")).lower() not in ("", "0", "success"):
            return True
    return False


def wait_health(port: int, timeout_s: float = 60.0) -> None:
    deadline = time.monotonic() + timeout_s
    last = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=2) as r:
                if r.status < 500:
                    return
        except Exception as e:  # noqa: BLE001 - diagnostic loop
            last = e
        time.sleep(0.5)
    raise TimeoutError(f"health check timed out on port {port}: {last}")


class DistributedCluster:
    """Local metasrv + one datanode + frontend cluster for query-mode runs."""

    def __init__(self, target: RunTarget):
        self.target = target
        self.procs: dict[str, subprocess.Popen[bytes]] = {}

    def component_report(self) -> dict[str, Any]:
        return {
            "metasrv": {
                "grpc": f"127.0.0.1:{self.target.metasrv_rpc_port}",
                "http": f"127.0.0.1:{self.target.metasrv_http_port}",
                "logs": str(self.target.work_dir / "logs" / "metasrv"),
            },
            "datanode_0": {
                "node_id": 0,
                "grpc": f"127.0.0.1:{self.target.datanode_rpc_port}",
                "http": f"127.0.0.1:{self.target.datanode_http_port}",
                "data_home": str(self.target.datanode_data_dir),
                "logs": str(self.target.work_dir / "logs" / "datanode-0"),
            },
            "frontend": {
                "http": f"127.0.0.1:{self.target.http_port}",
                "grpc": f"127.0.0.1:{self.target.grpc_port}",
                "mysql": f"127.0.0.1:{self.target.mysql_port}",
                "postgres": f"127.0.0.1:{self.target.postgres_port}",
                "logs": str(self.target.work_dir / "logs" / "frontend"),
            },
        }

    def _spawn(self, name: str, args: list[str]) -> None:
        logs = self.target.work_dir / "logs" / name
        logs.mkdir(parents=True, exist_ok=True)
        with (logs / "stdout.log").open("ab") as out, (logs / "stderr.log").open("ab") as err:
            self.procs[name] = subprocess.Popen(args, stdout=out, stderr=err)

    def _ensure_metasrv_alive(self) -> None:
        proc = self.procs.get("metasrv")
        if proc is None or proc.poll() is not None:
            raise RuntimeError("metasrv exited; memory-store metadata is no longer valid")

    def start_metasrv(self) -> None:
        log_dir = self.target.work_dir / "logs" / "metasrv"
        self._spawn(
            "metasrv",
            [
                str(self.target.binary),
                "metasrv",
                "start",
                "--grpc-bind-addr",
                f"127.0.0.1:{self.target.metasrv_rpc_port}",
                "--grpc-server-addr",
                f"127.0.0.1:{self.target.metasrv_rpc_port}",
                "--http-addr",
                f"127.0.0.1:{self.target.metasrv_http_port}",
                "--backend",
                "memory-store",
                "--enable-region-failover",
                "false",
                "--log-dir",
                str(log_dir),
            ],
        )
        wait_health(self.target.metasrv_http_port)

    def start_datanode(self) -> None:
        self._ensure_metasrv_alive()
        log_dir = self.target.work_dir / "logs" / "datanode-0"
        self.target.datanode_data_dir.mkdir(parents=True, exist_ok=True)
        self._spawn(
            "datanode",
            [
                str(self.target.binary),
                "datanode",
                "start",
                "--grpc-bind-addr",
                f"127.0.0.1:{self.target.datanode_rpc_port}",
                "--grpc-server-addr",
                f"127.0.0.1:{self.target.datanode_rpc_port}",
                "--http-addr",
                f"127.0.0.1:{self.target.datanode_http_port}",
                "--data-home",
                str(self.target.datanode_data_dir),
                "--log-dir",
                str(log_dir),
                "--node-id",
                "0",
                "--metasrv-addrs",
                f"127.0.0.1:{self.target.metasrv_rpc_port}",
            ],
        )
        wait_health(self.target.datanode_http_port)

    def start_frontend(self, config_file: Path | None = None) -> None:
        self._ensure_metasrv_alive()
        log_dir = self.target.work_dir / "logs" / "frontend"
        cmd = [
                str(self.target.binary),
                "frontend",
                "start",
        ]
        if config_file is not None:
            cmd += ["--config-file", str(config_file)]
        cmd += [
                "--metasrv-addrs",
                f"127.0.0.1:{self.target.metasrv_rpc_port}",
                "--http-addr",
                f"127.0.0.1:{self.target.http_port}",
                "--grpc-bind-addr",
                f"127.0.0.1:{self.target.grpc_port}",
                "--grpc-server-addr",
                f"127.0.0.1:{self.target.grpc_port}",
                "--mysql-addr",
                f"127.0.0.1:{self.target.mysql_port}",
                "--postgres-addr",
                f"127.0.0.1:{self.target.postgres_port}",
                "--log-dir",
                str(log_dir),
        ]
        self._spawn("frontend", cmd)
        wait_health(self.target.http_port)

    def start_all(self, frontend_config: Path | None = None) -> None:
        self.start_metasrv()
        self.start_datanode()
        self.start_frontend(frontend_config)

    def stop_component(self, name: str) -> None:
        proc = self.procs.pop(name, None)
        if proc is None:
            return
        proc.terminate()
        try:
            proc.wait(timeout=20)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=20)

    def stop_all(self) -> None:
        for name in ("frontend", "datanode", "metasrv"):
            self.stop_component(name)

    def restart_frontend(self) -> None:
        self.stop_component("frontend")
        self.start_frontend()


def extract_rows(body: Any) -> list[Any]:
    """Extract rows from GreptimeDB HTTP JSON in a tolerant way."""
    rows: list[Any] = []
    if isinstance(body, dict):
        for key in ("data", "rows", "records", "output"):
            if key in body:
                value = body[key]
                if key in ("data", "rows") and isinstance(value, list):
                    rows.extend(value)
                else:
                    rows.extend(extract_rows(value))
    elif isinstance(body, list):
        if body and all(not isinstance(v, (dict, list)) for v in body):
            rows.append(body)
        else:
            for item in body:
                rows.extend(extract_rows(item))
    return rows


def row_value(row: Any, idx: int, name: str) -> Any:
    if isinstance(row, dict):
        for key in (name, name.upper(), name.lower()):
            if key in row:
                return row[key]
        return None
    if isinstance(row, list):
        return row[idx]
    return row


def discover_region_via_frontend(target: RunTarget, table: dict[str, Any], http_timeout: float) -> dict[str, Any]:
    schema = table["database"]
    table_name = table["name"].replace("'", "''")
    schema_name = schema.replace("'", "''")
    table_sql = f"SELECT table_id FROM information_schema.tables WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'"
    table_result = http_post_sql(target.http_port, table_sql, schema, http_timeout)
    if not table_result["ok"]:
        raise RuntimeError(f"table_id discovery failed: {table_result}")
    table_rows = extract_rows(table_result.get("response"))
    if len(table_rows) != 1:
        raise RuntimeError(f"expected one information_schema.tables row, got {len(table_rows)}: {table_result}")
    table_id = int(row_value(table_rows[0], 0, "table_id"))

    region_sql = f"SELECT region_id, peer_id, peer_addr, is_leader, status FROM information_schema.region_peers WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'"
    region_result = http_post_sql(target.http_port, region_sql, schema, http_timeout)
    if not region_result["ok"]:
        raise RuntimeError(f"region_peers discovery failed: {region_result}")
    region_rows = extract_rows(region_result.get("response"))
    if len(region_rows) != 1:
        raise RuntimeError(f"expected one information_schema.region_peers row, got {len(region_rows)}: {region_result}")
    row = region_rows[0]
    region_id = int(row_value(row, 0, "region_id"))
    peer_id = int(row_value(row, 1, "peer_id"))
    peer_addr = row_value(row, 2, "peer_addr")
    is_leader = row_value(row, 3, "is_leader")
    status = row_value(row, 4, "status")
    if str(is_leader).lower() not in ("yes", "true"):
        raise RuntimeError(f"expected leader region peer, got is_leader={is_leader}: {region_result}")
    if str(status).upper() != "ALIVE":
        raise RuntimeError(f"expected ALIVE region peer, got status={status}: {region_result}")
    if peer_id != 0:
        raise RuntimeError(f"expected region leader on datanode peer_id=0, got {peer_id}")
    computed_table_id = region_id >> 32
    region_seq = region_id & 0xFFFFFFFF
    if computed_table_id != table_id:
        raise RuntimeError(f"table_id mismatch: tables={table_id}, region_id-derived={computed_table_id}")
    table_dir = f"data/greptime/{schema}/{table_id}/"
    region_dir = f"data/greptime/{schema}/{table_id}/{table_id}_{region_seq:010}"
    return {
        "catalog": "greptime",
        "schema": schema,
        "table_id": table_id,
        "region_id": region_id,
        "region_seq": region_seq,
        "table_dir": table_dir,
        "region_dir": region_dir,
        "peer_id": peer_id,
        "peer_addr": peer_addr,
        "is_leader": is_leader,
        "status": status,
        "discovery_queries": {"table": table_result, "region": region_result},
    }


def assert_fixture_summary(summary: dict[str, Any], *, region_id: int | None, table_dir: str | None, region_dir: str | None = None, table_name: str | None = None, database: str | None = None) -> None:
    if table_name is not None and summary.get("table") != table_name:
        raise RuntimeError(f"fixture table mismatch: {summary.get('table')} != {table_name}")
    if database is not None and summary.get("database") != database:
        raise RuntimeError(f"fixture database mismatch: {summary.get('database')} != {database}")
    if region_id is not None and int(summary["region_id"]) != region_id:
        raise RuntimeError(f"fixture region_id mismatch: {summary['region_id']} != {region_id}")
    if table_dir is not None and summary["table_dir"] != table_dir:
        raise RuntimeError(f"fixture table_dir mismatch: {summary['table_dir']} != {table_dir}")
    if region_dir is not None and summary["region_dir"].strip("/") != region_dir.strip("/"):
        raise RuntimeError(f"fixture region_dir mismatch: {summary['region_dir']} != {region_dir}")


def generate_fixture(generator: Path | None, case_path: Path, fixture_dir: Path, *, dry_run: bool, reuse_fixture: bool, allow_large_fixture: bool, table_name: str | None = None, database: str | None = None, region_id: int | None = None, table_dir: str | None = None, region_dir: str | None = None) -> dict[str, Any]:
    if generator is None:
        return {"status": "skipped", "reason": "no fixture generator provided"}
    summary_path = fixture_dir / "summary.json"
    reuse_miss: str | None = None
    if reuse_fixture and summary_path.exists():
        summary = json.loads(summary_path.read_text())
        try:
            assert_fixture_summary(summary, region_id=region_id, table_dir=table_dir, region_dir=region_dir, table_name=table_name, database=database)
            return {"status": "reused", "fixture_dir": str(fixture_dir), "summary": summary}
        except RuntimeError as e:
            reuse_miss = str(e)
            shutil.rmtree(fixture_dir)
    if fixture_dir.exists() and not reuse_fixture:
        shutil.rmtree(fixture_dir)
    fixture_dir.mkdir(parents=True, exist_ok=True)
    cmd = [str(generator), "direct-sst", "--case", str(case_path), "--out-dir", str(fixture_dir)]
    if table_name is not None:
        cmd += ["--table", table_name]
    if region_id is not None:
        cmd += ["--region-id", str(region_id)]
    if table_dir is not None:
        cmd += ["--table-dir", table_dir]
    if allow_large_fixture:
        cmd.append("--allow-large")
    if dry_run:
        return {"status": "dry-run", "cmd": cmd}
    result = run_command(cmd)
    result["status"] = "ok" if result["returncode"] == 0 else "failed"
    if result["returncode"] != 0:
        raise RuntimeError(f"fixture generator failed: {result['stderr'][:2000]}")
    summary = json.loads(summary_path.read_text())
    assert_fixture_summary(summary, region_id=region_id, table_dir=table_dir, region_dir=region_dir, table_name=table_name, database=database)
    result["summary"] = summary
    if reuse_miss is not None:
        result["reuse_miss"] = reuse_miss
    return result


def copy_stats() -> dict[str, int]:
    return {"files": 0, "dirs": 0, "reflinked": 0, "hardlinked": 0, "copied": 0}


def merge_copy_stats(left: dict[str, int], right: dict[str, int]) -> None:
    for key, value in right.items():
        left[key] = left.get(key, 0) + value


def reflink_file(src: Path, dst: Path) -> bool:
    try:
        dst.parent.mkdir(parents=True, exist_ok=True)
        sfd = os.open(src, os.O_RDONLY)
        try:
            dfd = os.open(dst, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, src.stat().st_mode & 0o777)
            try:
                fcntl.ioctl(dfd, FICLONE, sfd)
                shutil.copystat(src, dst, follow_symlinks=True)
                return True
            finally:
                os.close(dfd)
        finally:
            os.close(sfd)
    except OSError:
        try:
            dst.unlink()
        except OSError:
            pass
        return False


def materialize_file(src: Path, dst: Path, *, allow_hardlink: bool) -> str:
    if reflink_file(src, dst):
        return "reflinked"
    if allow_hardlink:
        try:
            dst.parent.mkdir(parents=True, exist_ok=True)
            try:
                dst.unlink()
            except FileNotFoundError:
                pass
            os.link(src, dst)
            return "hardlinked"
        except OSError:
            pass
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    return "copied"


def copy_tree_contents(src: Path, dst: Path, *, allow_hardlinks: bool = True) -> dict[str, int]:
    if not src.exists():
        raise FileNotFoundError(f"fixture path does not exist: {src}")
    stats = copy_stats()
    dst.mkdir(parents=True, exist_ok=True)
    for child in src.iterdir():
        target = dst / child.name
        if child.is_dir():
            stats["dirs"] += 1
            merge_copy_stats(stats, copy_tree_contents(child, target, allow_hardlinks=allow_hardlinks))
        else:
            mode = materialize_file(child, target, allow_hardlink=allow_hardlinks)
            stats["files"] += 1
            stats[mode] += 1
    return stats


def materialize_fixture(target: RunTarget, *, dry_run: bool, preserve_state: bool, expected_region_dir: str | None = None, fixture_dir: Path | None = None, reset_data: bool = True) -> dict[str, Any]:
    fixture_dir = fixture_dir or target.fixture_dir
    summary_path = fixture_dir / "summary.json"
    materialize_root = target.datanode_data_dir if preserve_state else target.data_dir
    if dry_run:
        return {"status": "dry-run", "summary_path": str(summary_path), "data_dir": str(materialize_root)}
    summary = json.loads(summary_path.read_text())
    object_store_dir = fixture_dir / "object-store"
    manifest_dir = fixture_dir / "manifest"
    region_dir = summary["region_dir"].strip("/")
    target_region_dir = materialize_root / region_dir
    data_root = materialize_root.resolve()
    resolved_region_dir = target_region_dir.resolve(strict=False)
    if data_root != resolved_region_dir and data_root not in resolved_region_dir.parents:
        raise RuntimeError(f"unsafe fixture region_dir escapes data_dir: {region_dir}")
    if expected_region_dir is not None and region_dir != expected_region_dir.strip("/"):
        raise RuntimeError(f"fixture region_dir {region_dir} does not equal discovered {expected_region_dir}")
    target_manifest_dir = target_region_dir / "manifest"
    if not preserve_state and reset_data and materialize_root.exists():
        shutil.rmtree(materialize_root)
    materialize_root.mkdir(parents=True, exist_ok=True)
    if preserve_state and target_region_dir.exists():
        shutil.rmtree(target_region_dir)
    object_stats: dict[str, int]
    if preserve_state:
        fixture_region_dir = object_store_dir / region_dir
        object_stats = copy_tree_contents(fixture_region_dir, target_region_dir, allow_hardlinks=True)
    else:
        object_stats = copy_tree_contents(object_store_dir, materialize_root, allow_hardlinks=True)
    if target_manifest_dir.exists():
        shutil.rmtree(target_manifest_dir)
    manifest_stats = copy_tree_contents(manifest_dir, target_manifest_dir, allow_hardlinks=False)
    return {"status": "ok", "summary_path": str(summary_path), "data_dir": str(materialize_root), "region_dir": region_dir, "manifest_dir": str(target_manifest_dir), "preserve_state": preserve_state, "copy_stats": {"object_store": object_stats, "manifest": manifest_stats}}


def response_text(body: Any) -> str:
    return json.dumps(body, sort_keys=True) if not isinstance(body, str) else body


def validate_show_create(result: dict[str, Any], table: dict[str, Any]) -> list[str]:
    text = response_text(result.get("response", {})).lower()
    errors = []
    if table["name"].lower() not in text:
        errors.append("SHOW CREATE output does not contain table name")
    if table.get("validate_show_create_engine", True) and ("engine" not in text or "mito" not in text):
        errors.append("SHOW CREATE output does not mention ENGINE=mito")
    if "append_mode" in table and "append_mode" not in text:
        errors.append("SHOW CREATE output does not mention append_mode")
    if table.get("sst_format") and "sst_format" not in text:
        errors.append("SHOW CREATE output does not mention sst_format")
    return errors


def run_queries(target: RunTarget, case: dict[str, Any], tables: list[dict[str, Any]], http_timeout: float) -> dict[str, Any]:
    table = tables[0]
    db = table["database"]
    queries = planned_queries(case)
    if not queries:
        queries = [{"name": "count_all", "kind": "sql", "query": f"SELECT count(*) FROM {sql_ident(table['name'])}", "warmup": 0, "iterations": 1}]
    validations = []
    validation_errors = []
    for table in tables:
        for sql in [f"SHOW CREATE TABLE {sql_ident(table['name'])}"]:
            result = http_post_sql(target.http_port, sql, table["database"], http_timeout)
            if not result["ok"]:
                validation_errors.append({"sql": sql, "error": result.get("error"), "response": result.get("response")})
            else:
                for error in validate_show_create(result, table):
                    validation_errors.append({"sql": sql, "error": error, "response": result.get("response")})
            validations.append(result)
    if queries:
        result = http_post_sql(target.http_port, queries[0]["query"], db, http_timeout)
        if not result["ok"]:
            validation_errors.append({"sql": queries[0]["query"], "error": result.get("error"), "response": result.get("response")})
        validations.append(result)
    measurements = []
    for q in queries:
        for _ in range(int(q.get("warmup", 0))):
            warmup = http_post_sql(target.http_port, q["query"], db, http_timeout)
            if not warmup["ok"]:
                validation_errors.append({"sql": q["query"], "phase": "warmup", "error": warmup.get("error"), "response": warmup.get("response")})
        samples = []
        for _ in range(int(q.get("iterations", 1))):
            result = http_post_sql(target.http_port, q["query"], db, http_timeout)
            result["execution_time_ms"] = extract_execution_time(result.get("response"))
            samples.append(result)
        good_lats = [s["latency_ms"] for s in samples if s["ok"]]
        measurements.append({"name": q.get("name"), "kind": q.get("kind"), "iterations": len(samples), "samples": samples, "latency_ms_median": statistics.median(good_lats) if good_lats else None, "latency_ms_p95": percentile(good_lats, 95) if good_lats else None, "status": "ok" if len(good_lats) == len(samples) else "failed"})
    return {"validation": validations, "validation_errors": validation_errors, "measurements": measurements, "status": "failed" if validation_errors or any(m["status"] == "failed" for m in measurements) else "ok"}


def extract_execution_time(body: Any) -> Any:
    if isinstance(body, dict):
        for key in ("execution_time_ms", "execution_time", "elapsed"):
            if key in body:
                return body[key]
        for value in body.values():
            found = extract_execution_time(value)
            if found is not None:
                return found
    if isinstance(body, list):
        for value in body:
            found = extract_execution_time(value)
            if found is not None:
                return found
    return None


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, round((pct / 100.0) * (len(ordered) - 1))))
    return ordered[idx]


def enforce_thresholds(case: dict[str, Any], base: dict[str, Any], candidate: dict[str, Any]) -> list[dict[str, Any]]:
    results = []
    base_by_name = {m["name"]: m for m in base.get("measurements", [])}
    for cm in candidate.get("measurements", []):
        bm = base_by_name.get(cm["name"])
        qcfg = next((q for q in planned_queries(case) if q.get("name") == cm["name"]), {})
        th = qcfg.get("thresholds") or {}
        max_reg = th.get("max_candidate_latency_regression_pct")
        if max_reg is not None and not bm:
            results.append({"query": cm["name"], "threshold": "max_candidate_latency_regression_pct", "status": "failed", "reason": "missing base measurement"})
        elif max_reg is not None and bm and bm.get("latency_ms_median") in (None, 0):
            results.append({"query": cm["name"], "threshold": "max_candidate_latency_regression_pct", "status": "failed", "reason": "base median latency is missing or zero", "base_latency_ms_median": bm.get("latency_ms_median")})
        elif max_reg is not None and bm and cm.get("latency_ms_median") is None:
            results.append({"query": cm["name"], "threshold": "max_candidate_latency_regression_pct", "status": "failed", "reason": "missing candidate measurement"})
        elif bm and max_reg is not None:
            ratio = (cm["latency_ms_median"] - bm["latency_ms_median"]) / bm["latency_ms_median"] * 100.0
            results.append({"query": cm["name"], "threshold": "max_candidate_latency_regression_pct", "status": "passed" if ratio <= max_reg else "failed", "actual_pct": ratio, "limit_pct": max_reg})
        for key in th:
            if key != "max_candidate_latency_regression_pct":
                results.append({"query": cm["name"], "threshold": key, "status": "failed", "reason": "unsupported threshold"})
    return results


def storage_config(remote: dict[str, Any]) -> dict[str, Any] | None:
    value = remote["storage"]
    if not isinstance(value, dict):
        return None
    if value["inspect"] is False:
        return None
    return value


def run_storage_inspection(helper: Path | None, target: RunTarget, storage: dict[str, Any], *, dry_run: bool) -> dict[str, Any]:
    if helper is None:
        if not dry_run:
            raise ValueError("--fixture-generator query_perf_fixture is required when storage inspection is enabled")
        helper = Path("query_perf_fixture")
    root = target.datanode_data_dir
    if storage.get("root_suffix"):
        root = root / str(storage["root_suffix"])
    cmd = [
        str(helper),
        "inspect-footer",
        "--root", str(root),
        "--column", str(storage["column"]),
    ]
    if storage["include_metadata_files"]:
        cmd.append("--include-metadata-files")
    if dry_run:
        return {"status": "dry-run", "cmd": cmd, "root": str(root)}
    result = run_command(cmd)
    result["status"] = "ok" if result["returncode"] == 0 else "failed"
    if result["returncode"] != 0:
        raise RuntimeError(f"storage inspector failed for {target.name}: {result['stderr'][:2000]}")
    try:
        result["summary"] = json.loads(result["stdout"])
    except json.JSONDecodeError:
        result["summary_parse_error"] = result["stdout"]
        result["status"] = "failed"
    return result


def storage_summary(inspection: dict[str, Any]) -> dict[str, Any]:
    summary = inspection.get("summary")
    if isinstance(summary, dict):
        nested = summary.get("summary")
        if isinstance(nested, dict):
            return nested
    return {}


def enforce_storage_thresholds(storage: dict[str, Any] | None, base: dict[str, Any] | None, candidate: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not storage:
        return []
    results: list[dict[str, Any]] = []
    target_summaries = [("base", storage_summary(base or {})), ("candidate", storage_summary(candidate or {}))]
    cand = target_summaries[1][1]
    base_summary = storage_summary(base or {})

    def add_abs(name: str, field: str) -> None:
        limit = storage.get(name)
        if limit is None:
            return
        for target_name, summary in target_summaries:
            actual = summary.get(field)
            ok = actual is not None and float(actual) <= float(limit)
            results.append({"target": target_name, "threshold": name, "status": "passed" if ok else "failed", "actual": actual, "limit": limit})

    def add_min(name: str, field: str) -> None:
        limit = storage[name]
        if limit is None:
            return
        for target_name, summary in target_summaries:
            actual = summary.get(field)
            ok = actual is not None and int(actual) >= int(limit)
            results.append({"target": target_name, "threshold": name, "status": "passed" if ok else "failed", "actual": actual, "limit": limit})

    def add_cmp(name: str, field: str) -> None:
        limit = storage.get(name)
        if limit is None:
            return
        base_value = base_summary.get(field)
        candidate_value = cand.get(field)
        if base_value in (None, 0) or candidate_value is None:
            results.append({"threshold": name, "status": "failed", "reason": "missing or zero base/candidate value", "base": base_value, "candidate": candidate_value})
            return
        actual = (float(candidate_value) - float(base_value)) / float(base_value) * 100.0
        results.append({"threshold": name, "status": "passed" if actual <= float(limit) else "failed", "actual_pct": actual, "limit_pct": limit, "base": base_value, "candidate": candidate_value})

    add_min("min_files", "file_count")
    add_min("min_files_with_column", "files_with_column")
    add_abs("max_total_file_size_bytes", "total_file_size")
    add_abs("max_column_compressed_size_bytes", "column_compressed_size")
    add_abs("max_column_uncompressed_size_bytes", "column_uncompressed_size")
    for target_name, summary in target_summaries:
        encodings = set(summary.get("unique_encodings") or [])
        for required in storage["require_encodings"]:
            results.append({"target": target_name, "threshold": "require_encodings", "encoding": required, "status": "passed" if required in encodings else "failed"})
        for forbidden in storage["forbid_encodings"]:
            results.append({"target": target_name, "threshold": "forbid_encodings", "encoding": forbidden, "status": "passed" if forbidden not in encodings else "failed"})
    add_cmp("max_candidate_total_file_size_regression_pct", "total_file_size")
    add_cmp("max_candidate_column_compressed_size_regression_pct", "column_compressed_size")
    add_cmp("max_candidate_column_uncompressed_size_regression_pct", "column_uncompressed_size")
    return results


def planned_storage_thresholds(storage: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not storage:
        return []
    return list(storage["planned_thresholds"])


REGION_DIR_RE = re.compile(r"^(\d+)_(\d{10})$")


def inspection_report(inspection: dict[str, Any] | None) -> dict[str, Any]:
    if not inspection:
        return {}
    report = inspection.get("summary")
    return report if isinstance(report, dict) else {}


def inspected_relative_path(target: RunTarget, inspection: dict[str, Any], relative_path: str) -> Path:
    report = inspection_report(inspection or {})
    root_text = report.get("root") or inspection.get("root")
    if not root_text:
        return Path(relative_path)
    root = Path(root_text).resolve(strict=False)
    data_home = target.datanode_data_dir.resolve(strict=False)
    try:
        root_suffix = root.relative_to(data_home)
    except ValueError as e:
        raise RuntimeError(f"storage inspection root {root} is not under datanode data home {data_home}") from e
    return root_suffix / relative_path


def parse_sst_bench_target(target: RunTarget, inspection: dict[str, Any], file: dict[str, Any], *, dry_run: bool) -> dict[str, str] | None:
    relative_path = str(file.get("relative_path") or "")
    if dry_run and (not relative_path or "<" in relative_path):
        return {
            "relative_path": relative_path or "<table-dir>/<table-id>_<region-seq>/data/<file-id>.parquet",
            "table_dir": "<table-dir>/",
            "region_id": "<table-id>:<region-seq>",
            "path_type": "data",
            "file_id": "<file-id>",
        }
    if not relative_path:
        return None
    full_relative = inspected_relative_path(target, inspection, relative_path)
    parts = full_relative.parts
    if not parts or not parts[-1].endswith(".parquet"):
        return None
    region_index = next((idx for idx, part in enumerate(parts) if REGION_DIR_RE.match(part)), None)
    if region_index is None or region_index == 0:
        if dry_run:
            return {
                "relative_path": relative_path,
                "table_dir": "<table-dir>/",
                "region_id": "<table-id>:<region-seq>",
                "path_type": "data",
                "file_id": Path(parts[-1]).stem,
            }
        return None
    region_match = REGION_DIR_RE.match(parts[region_index])
    assert region_match is not None
    file_index = len(parts) - 1
    path_type = "bare"
    if region_index + 1 < file_index and parts[region_index + 1] in ("data", "metadata"):
        path_type = parts[region_index + 1]
    if path_type == "metadata":
        return None
    table_dir = "/".join(parts[:region_index]).rstrip("/") + "/"
    return {
        "relative_path": str(full_relative),
        "table_dir": table_dir,
        "region_id": f"{int(region_match.group(1))}:{int(region_match.group(2))}",
        "path_type": path_type,
        "file_id": Path(parts[-1]).stem,
    }


ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
PARQUET_ITERATION_RE = re.compile(r"^\s*Iteration\s+(?P<iteration>\d+):\s+(?P<rows>\d+)\s+rows,\s+(?P<columns>\d+)\s+columns,\s+(?P<batches>\d+)\s+record batches\s+in\s+(?P<duration>\S+)")
SCAN_ITERATION_RE = re.compile(r"^\s*\[iter\s+(?P<iteration>\d+)\]\s+(?P<rows>\d+)\s+rows\s+in\s+(?P<duration>\S+)\s+\((?P<partitions>\d+)\s+partitions\),")


def duration_to_millis(value: str) -> float:
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)(ns|µs|us|ms|s)", value)
    if not match:
        raise ValueError(f"unsupported benchmark duration {value!r}")
    scale = {"ns": 1e-6, "µs": 1e-3, "us": 1e-3, "ms": 1.0, "s": 1000.0}[match.group(2)]
    return float(match.group(1)) * scale


def parse_bench_iterations(stdout: str, mode: str, expected_iterations: int, expected_rows: int | None = None) -> list[dict[str, Any]]:
    regex = PARQUET_ITERATION_RE if mode == "parquet" else SCAN_ITERATION_RE
    lines = ANSI_ESCAPE_RE.sub("", stdout).splitlines()
    candidates = [line for line in lines if ("Iteration" in line if mode == "parquet" else "[iter" in line)]
    samples = []
    for line in candidates:
        match = regex.match(line)
        if not match:
            raise ValueError(f"malformed {mode} benchmark iteration line: {line!r}")
        groups = match.groupdict()
        samples.append({"iteration": int(groups["iteration"]), "rows": int(groups["rows"]), "duration": groups["duration"], "duration_ms": duration_to_millis(groups["duration"]), "line": line})
    if len(samples) != expected_iterations:
        raise ValueError(f"{mode} benchmark emitted {len(samples)} iteration samples; expected {expected_iterations}")
    iterations = [sample["iteration"] for sample in samples]
    if iterations != list(range(1, expected_iterations + 1)):
        raise ValueError(f"{mode} benchmark iterations are missing, duplicated, or out of order: {iterations}")
    rows = {sample["rows"] for sample in samples}
    if len(rows) != 1:
        raise ValueError(f"{mode} benchmark row-count mismatch across iterations: {sorted(rows)}")
    if expected_rows is not None and rows != {expected_rows}:
        raise ValueError(f"{mode} benchmark rows {sorted(rows)} do not equal expected {expected_rows}")
    return samples


def summarize_iteration_samples(samples: list[dict[str, Any]]) -> dict[str, Any]:
    if len(samples) < 2:
        raise ValueError("benchmark requires at least two internal iterations to discard the warmup")
    kept = samples[1:]
    return {"raw_samples": samples, "kept_samples": kept, "median_ms": statistics.median(sample["duration_ms"] for sample in kept), "row_count": samples[0]["rows"]}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def artifact_layout_evidence(inspection: dict[str, Any] | None, *, dry_run: bool) -> dict[str, Any]:
    if dry_run:
        return {"status": "planned", "files": [], "stability": {"status": "planned", "stable": None}}
    inspection = inspection or {}
    report = inspection_report(inspection)
    root = Path(str(report.get("root") or inspection.get("root") or ""))
    layout = [dict(item) for item in report.get("files") or []]
    layout.sort(key=lambda item: str(item.get("relative_path") or ""))
    layout_hash = hashlib.sha256(json.dumps(layout, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    hashed_files = []
    for item in layout:
        relative_path = str(item.get("relative_path") or "")
        path = root / relative_path
        if not relative_path or not path.is_file():
            raise RuntimeError(f"inspected SST is unavailable for artifact hashing: {path}")
        hashed_files.append({"relative_path": relative_path, "content_hash": sha256_file(path), "layout": item})
    artifact_hash = hashlib.sha256(json.dumps([(item["relative_path"], item["content_hash"]) for item in hashed_files], separators=(",", ":")).encode()).hexdigest()
    return {"status": "observed", "layout_hash": layout_hash, "artifact_hash": artifact_hash, "files": hashed_files}


def named_projection_schedule(read_bench: dict[str, Any]) -> list[dict[str, Any]]:
    projections = read_bench.get("projections") or []
    if projections:
        return list(projections)
    return [{"name": "legacy", "columns": list(read_bench["projection"])}]


def b4_enabled(read_bench: dict[str, Any] | None) -> bool:
    return bool(read_bench and ((read_bench.get("projections") or []) or int(read_bench.get("rounds", 1)) != 1))


def paired_target_order(targets: list[RunTarget], round_index: int) -> list[RunTarget]:
    return list(targets if round_index % 2 == 0 else reversed(targets))


LOGICAL_DIGEST_PREFIX = "LOGICAL_ROW_DIGEST_JSON="


def logical_digest_command(bench_binary: Path, target: RunTarget, inspection: dict[str, Any], config: dict[str, Any]) -> list[str]:
    files = inspection_report(inspection).get("files") or []
    parsed = next((parse_sst_bench_target(target, inspection, item, dry_run=False) for item in files if item.get("relative_path", "").endswith(".parquet")), None)
    if parsed is None:
        if inspection.get("status") == "dry-run":
            parsed = {"region_id": "<table-id>:<region-seq>", "table_dir": "<table-dir>/", "path_type": "data"}
        else:
            raise RuntimeError("logical stream digest requires a frozen data SST")
    cmd = [str(bench_binary), "datanode", "scanbench", "--config", str(target.work_dir / "read_bench" / "bench.toml"), "--region-id", parsed["region_id"], "--table-dir", parsed["table_dir"], "--path-type", parsed["path_type"], "--scanner", "series", "--parallelism", "1", "--iterations", "1", "--logical-row-digest"]
    for label in config["labels"]:
        cmd.extend(["--digest-label", str(label)])
    cmd.extend(["--digest-timestamp-column", str(config["timestamp_column"]), "--digest-value-column", str(config["value_column"])])
    return cmd


def parse_logical_digest(stdout: str, config: dict[str, Any], expected_rows: int) -> dict[str, Any]:
    lines = [line[len(LOGICAL_DIGEST_PREFIX):] for line in ANSI_ESCAPE_RE.sub("", stdout).splitlines() if line.startswith(LOGICAL_DIGEST_PREFIX)]
    if len(lines) != 1:
        raise ValueError(f"expected exactly one {LOGICAL_DIGEST_PREFIX} line, got {len(lines)}")
    try:
        report = json.loads(lines[0])
    except json.JSONDecodeError as e:
        raise ValueError(f"invalid logical digest JSON: {e}") from e
    required = {"status": "ok", "schema_version": 1, "algorithm": "sha256", "canonicalization": "logical-metric-row-v1", "scanner": "series", "parallelism": 1}
    for key, value in required.items():
        if report.get(key) != value:
            raise ValueError(f"logical digest {key} mismatch: {report.get(key)!r} != {value!r}")
    labels = report.get("labels")
    if not isinstance(labels, list) or len(labels) != len(config["labels"]) or not all(isinstance(item, dict) for item in labels) or [item.get("name") for item in labels] != config["labels"] or any(item.get("type") != "string" or not isinstance(item.get("column_id"), int) for item in labels):
        raise ValueError(f"logical digest labels mismatch: {labels!r} != {config['labels']!r}")
    timestamp = report.get("timestamp")
    if not isinstance(timestamp, dict) or timestamp.get("name") != config["timestamp_column"] or timestamp.get("unit") != "millisecond" or not isinstance(timestamp.get("column_id"), int):
        raise ValueError(f"logical digest timestamp metadata mismatch: {timestamp!r}")
    value = report.get("value")
    if not isinstance(value, dict) or value.get("name") != config["value_column"] or value.get("type") != "float64" or value.get("encoding") != "ieee754-bits" or not isinstance(value.get("column_id"), int):
        raise ValueError(f"logical digest value metadata mismatch: {value!r}")
    if report.get("rows") != expected_rows:
        raise ValueError(f"logical digest rows {report.get('rows')!r} != expected {expected_rows}")
    if not isinstance(report.get("digest"), str) or not re.fullmatch(r"[0-9a-f]{64}", report["digest"]):
        raise ValueError("logical digest must be lowercase 64-hex sha256")
    return report


def run_logical_digests(bench_binary: Path, targets: list[RunTarget], inspections: dict[str, dict[str, Any]], planned_rows: int, config: dict[str, Any], *, dry_run: bool) -> dict[str, Any]:
    reports: dict[str, Any] = {}
    for target in targets:
        cmd = logical_digest_command(bench_binary, target, inspections[target.name], config)
        expected_rows = planned_rows
        evidence: dict[str, Any] = {"target": target.name, "cmd": cmd, "expected_rows": expected_rows}
        if dry_run:
            reports[target.name] = {**evidence, "status": "planned", "report": None}
            continue
        bench_dir = target.work_dir / "read_bench"
        bench_dir.mkdir(parents=True, exist_ok=True)
        (bench_dir / "bench.toml").write_text(f'[storage]\ndata_home = "{target.datanode_data_dir}"\ntype = "File"\n\n[[region_engine]]\n[region_engine.mito]\n')
        result = run_command(cmd)
        evidence.update(result)
        if result["returncode"] != 0:
            reports[target.name] = {**evidence, "status": "failed", "error": "logical digest command failed", "report": None}
            continue
        try:
            reports[target.name] = {**evidence, "status": "observed", "report": parse_logical_digest(result["stdout"], config, int(expected_rows))}
        except ValueError as e:
            reports[target.name] = {**evidence, "status": "failed", "error": str(e), "report": None}
    return reports


def logical_digest_equality(reports: dict[str, Any], targets: list[RunTarget]) -> dict[str, Any]:
    left, right = (reports[target.name] for target in targets)
    if left["status"] != "observed" or right["status"] != "observed":
        return {"status": "failed", "reports": reports, "checks": {"row_count_equal": False, "expected_rows_equal": False, "canonicalization_equal": False, "digest_equal": False}}
    a, b = left["report"], right["report"]
    checks = {"row_count_equal": a["rows"] == b["rows"], "expected_rows_equal": left["expected_rows"] == right["expected_rows"], "canonicalization_equal": all(a.get(key) == b.get(key) for key in ("schema_version", "algorithm", "canonicalization", "scanner", "parallelism", "labels", "timestamp", "value")), "digest_equal": a["digest"] == b["digest"]}
    return {"status": "ok" if all(checks.values()) else "failed", "reports": reports, "checks": checks}


def aggregate_query_rounds(rounds: list[dict[str, Any]]) -> dict[str, Any]:
    by_name: dict[str, list[dict[str, Any]]] = {}
    failed = False
    for round_data in rounds:
        if round_data["result"]["status"] != "ok":
            failed = True
        for measurement in round_data["result"].get("measurements", []):
            by_name.setdefault(str(measurement["name"]), []).append(measurement)
            failed = failed or measurement.get("status") != "ok"
    measurements = []
    for name, samples in by_name.items():
        medians = [sample["latency_ms_median"] for sample in samples if sample.get("latency_ms_median") is not None]
        measurements.append({"name": name, "status": "ok" if len(medians) == len(samples) else "failed", "sample_unit": "round_median", "round_medians_ms": medians, "latency_ms_median": statistics.median(medians) if medians else None, "latency_ms_p95": percentile(medians, 95) if medians else None, "rounds": samples})
    return {"status": "failed" if failed or any(m["status"] == "failed" for m in measurements) else "ok", "measurements": measurements}


def build_b4_commands(bench_binary: Path, target: RunTarget, read_bench: dict[str, Any], storage_inspection: dict[str, Any], projection: dict[str, Any], *, dry_run: bool, expected_region_rows: int | None = None) -> list[dict[str, Any]]:
    files = inspection_report(storage_inspection).get("files") or []
    ssts = [item for item in files if item.get("relative_path", "").endswith(".parquet") and item.get("columns")]
    if dry_run and not ssts:
        ssts = [{"relative_path": "<landed-sst>.parquet"}]
    if read_bench["max_files"] is not None:
        ssts = ssts[: int(read_bench["max_files"])]
    bench_targets = [{**parsed, "expected_rows": item.get("num_rows")} for item in ssts if (parsed := parse_sst_bench_target(target, storage_inspection, item, dry_run=dry_run)) is not None]
    if not bench_targets:
        raise RuntimeError("no inspected data SST files available for paired read_bench")
    bench_dir = target.work_dir / "read_bench"
    config_path = bench_dir / "bench.toml"
    projection_name = str(projection["name"])
    scan_config_path = bench_dir / f"{projection_name}.scan.json"
    projection_columns = projection.get("columns")
    scan_config = {} if projection_columns is None else {"projection_names": projection_columns}
    common = {"config_path": str(config_path), "scan_config_path": str(scan_config_path), "scan_config": scan_config, "projection": projection_name, "projection_columns": projection_columns}
    specs = []
    iterations = str(int(read_bench["iterations"]))
    if read_bench["parquetbench"]:
        for bench_target in bench_targets:
            cmd = [str(bench_binary), "datanode", "parquetbench", "--config", str(config_path), "--region-id", bench_target["region_id"], "--table-dir", bench_target["table_dir"], "--file-id", bench_target["file_id"], "--scan-config", str(scan_config_path), "--path-type", bench_target["path_type"], "--iterations", iterations, "--reader", str(read_bench["parquet_reader"])]
            expected_rows = bench_target.get("expected_rows")
            if not dry_run and expected_rows is None:
                raise RuntimeError(f"inspected SST lacks num_rows for parquet benchmark: {bench_target.get('relative_path')}")
            specs.append({**common, **bench_target, "mode": "parquet", "expected_rows": int(expected_rows) if expected_rows is not None else None, "cmd": cmd})
    regions: dict[tuple[str, str, str], list[str]] = {}
    if read_bench["scanbench"]:
        for bench_target in bench_targets:
            regions.setdefault((bench_target["table_dir"], bench_target["region_id"], bench_target["path_type"]), []).append(bench_target["relative_path"])
    for (table_dir, region_id, path_type), region_files in regions.items():
        cmd = [str(bench_binary), "datanode", "scanbench", "--config", str(config_path), "--region-id", region_id, "--table-dir", table_dir, "--scan-config", str(scan_config_path), "--path-type", path_type, "--scanner", str(read_bench["scan_scanner"]), "--parallelism", str(int(read_bench["parallelism"])), "--iterations", iterations]
        specs.append({**common, "mode": "sequential", "table_dir": table_dir, "region_id": region_id, "path_type": path_type, "files": region_files, "expected_rows": expected_region_rows, "cmd": cmd})
    return specs


def run_paired_read_bench(bench_binary: Path, targets: list[RunTarget], read_bench: dict[str, Any], inspections: dict[str, dict[str, Any]], visibility_rows: dict[str, int | None], *, dry_run: bool) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    projections = named_projection_schedule(read_bench)
    commands = {target.name: [spec for projection in projections for spec in build_b4_commands(bench_binary, target, read_bench, inspections[target.name], projection, dry_run=dry_run, expected_region_rows=visibility_rows.get(target.name))] for target in targets}
    results = {target.name: {"status": "planned" if dry_run else "ok", "rounds": [], "commands": commands[target.name]} for target in targets}
    schedule = []
    for round_index in range(int(read_bench["rounds"])):
        order = paired_target_order(targets, round_index)
        round_schedule = {"round": round_index + 1, "target_order": [target.name for target in order], "invocations": []}
        schedule.append(round_schedule)
        for target in order:
            round_runs = []
            for spec in commands[target.name]:
                run = {**spec, "round": round_index + 1, "target": target.name, "status": "planned" if dry_run else "running"}
                round_schedule["invocations"].append({"target": target.name, "projection": spec["projection"], "mode": spec["mode"], "cmd": spec["cmd"]})
                if not dry_run:
                    bench_dir = Path(spec["config_path"]).parent
                    bench_dir.mkdir(parents=True, exist_ok=True)
                    Path(spec["config_path"]).write_text(f'[storage]\ndata_home = "{target.datanode_data_dir}"\ntype = "File"\n\n[[region_engine]]\n[region_engine.mito]\n')
                    Path(spec["scan_config_path"]).write_text(json.dumps(spec["scan_config"], indent=2) + "\n")
                    command_result = run_command(spec["cmd"])
                    run.update(command_result)
                    if command_result["returncode"] != 0:
                        run["status"] = "failed"
                    else:
                        try:
                            run.update(summarize_iteration_samples(parse_bench_iterations(command_result["stdout"], str(spec["mode"]), int(read_bench["iterations"]), spec.get("expected_rows"))))
                            run["status"] = "ok"
                        except ValueError as e:
                            run["status"] = "failed"
                            run["parse_error"] = str(e)
                round_runs.append(run)
            results[target.name]["rounds"].append({"round": round_index + 1, "target_order": [item.name for item in order], "runs": round_runs, "medians": [{"projection": run["projection"], "mode": run["mode"], "median_ms": run.get("median_ms"), "row_count": run.get("row_count")} for run in round_runs]})
    if not dry_run:
        for result in results.values():
            result["status"] = "ok" if all(run["status"] == "ok" for round_data in result["rounds"] for run in round_data["runs"]) else "failed"
    return results, schedule


def run_read_bench(bench_binary: Path, target: RunTarget, read_bench: dict[str, Any] | None, storage_inspection: dict[str, Any] | None, *, dry_run: bool) -> dict[str, Any]:
    if not read_bench or read_bench["enabled"] is False:
        return {"status": "skipped", "reason": "read_bench disabled"}
    run_parquetbench = read_bench["parquetbench"]
    run_scanbench = read_bench["scanbench"]
    if not run_parquetbench and not run_scanbench:
        return {"status": "skipped", "reason": "parquetbench and scanbench disabled"}
    if storage_inspection is None:
        raise ValueError("read_bench requires storage inspection")
    bench_dir = target.work_dir / "read_bench"
    config_toml = bench_dir / "bench.toml"
    scan_json = bench_dir / "scan.json"
    files = inspection_report(storage_inspection).get("files") or []
    ssts = [f for f in files if f.get("relative_path", "").endswith(".parquet") and f.get("columns")]
    if dry_run and not ssts:
        ssts = [{"relative_path": "<landed-sst>.parquet"}]
    if read_bench["max_files"] is not None:
        ssts = ssts[: int(read_bench["max_files"])]
    bench_targets = [parsed for f in ssts if (parsed := parse_sst_bench_target(target, storage_inspection, f, dry_run=dry_run)) is not None]
    if not bench_targets:
        return {"status": "failed", "reason": "no inspected data SST files available for read_bench"}
    config_text = f'[storage]\ndata_home = "{target.datanode_data_dir}"\ntype = "File"\n\n[[region_engine]]\n[region_engine.mito]\n'
    commands = []
    parquet_runs = []
    scan_runs = []
    iterations = str(int(read_bench["iterations"]))
    if run_parquetbench:
        for bench_target in bench_targets:
            cmd = [str(bench_binary), "datanode", "parquetbench", "--config", str(config_toml), "--region-id", bench_target["region_id"], "--table-dir", bench_target["table_dir"], "--file-id", bench_target["file_id"], "--scan-config", str(scan_json), "--path-type", bench_target["path_type"], "--iterations", iterations, "--reader", str(read_bench["parquet_reader"])]
            commands.append(cmd)
            parquet_runs.append({**bench_target, "cmd": cmd, "status": "dry-run" if dry_run else "planned"})
    regions: dict[tuple[str, str, str], list[str]] = {}
    if run_scanbench:
        for bench_target in bench_targets:
            regions.setdefault((bench_target["table_dir"], bench_target["region_id"], bench_target["path_type"]), []).append(bench_target["relative_path"])
    for (table_dir, region_id, path_type), region_files in regions.items():
        cmd = [str(bench_binary), "datanode", "scanbench", "--config", str(config_toml), "--region-id", region_id, "--table-dir", table_dir, "--scan-config", str(scan_json), "--path-type", path_type, "--scanner", str(read_bench["scan_scanner"]), "--parallelism", str(int(read_bench["parallelism"])), "--iterations", iterations]
        commands.append(cmd)
        scan_runs.append({"table_dir": table_dir, "region_id": region_id, "path_type": path_type, "files": region_files, "cmd": cmd, "status": "dry-run" if dry_run else "planned"})
    if dry_run:
        return {"status": "dry-run", "config_path": str(config_toml), "scan_config_path": str(scan_json), "commands": commands, "parquetbench": parquet_runs, "scanbench": scan_runs}
    bench_dir.mkdir(parents=True, exist_ok=True)
    config_toml.write_text(config_text)
    scan_json.write_text(json.dumps({"projection_names": read_bench["projection"]}, indent=2) + "\n")
    avg_re = re.compile(r"Average duration[^0-9]*([0-9.]+)\s*ms", re.I)
    for runs in (parquet_runs, scan_runs):
        for run in runs:
            result = run_command(run["cmd"])
            run.update(result)
            run["status"] = "ok" if result["returncode"] == 0 else "failed"
            m = avg_re.search(result.get("stdout", ""))
            if m:
                run["average_ms"] = float(m.group(1))
    def median(runs: list[dict[str, Any]]) -> float | None:
        vals = [r["average_ms"] for r in runs if "average_ms" in r]
        return statistics.median(vals) if vals else None
    return {"status": "ok" if all(r.get("status") == "ok" for r in parquet_runs + scan_runs) else "failed", "config_path": str(config_toml), "scan_config_path": str(scan_json), "parquetbench": parquet_runs, "scanbench": scan_runs, "aggregate": {"parquetbench_median_average_ms": median(parquet_runs), "scanbench_median_average_ms": median(scan_runs)}}


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def require_fresh_work_dirs(targets: list[RunTarget], *, reuse_work_dir: bool, dry_run: bool, fixture_only: bool) -> None:
    if dry_run or reuse_work_dir or fixture_only:
        return
    for target in targets:
        if target.work_dir.exists() and any(target.work_dir.iterdir()):
            raise RuntimeError(f"target work_dir exists and is non-empty: {target.work_dir}; pass --reuse-work-dir to override")


def write_frontend_prom_config(target: RunTarget, remote: dict[str, Any]) -> Path:
    prom = remote["prom_store"]
    path = target.work_dir / "frontend-prom-store.toml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "[prom_store]\n"
        "enable = true\n"
        "with_metric_engine = true\n"
        f"pending_rows_flush_interval = {json.dumps(str(prom['pending_rows_flush_interval']))}\n"
        f"max_batch_rows = {int(prom['max_batch_rows'])}\n"
        f"max_concurrent_flushes = {int(prom['max_concurrent_flushes'])}\n"
        f"worker_channel_capacity = {int(prom['worker_channel_capacity'])}\n"
        f"max_inflight_requests = {int(prom['max_inflight_requests'])}\n"
    )
    return path


def merge_physical_table_options(setup: dict[str, Any], target_name: str) -> dict[str, str]:
    """Apply a target overlay without allowing it to remove common options."""
    common = setup.get("options") or {}
    overlays = setup.get("target_options") or {}
    overlay = overlays.get(target_name) or {}
    return {str(key): str(value) for key, value in sorted({**common, **overlay}.items())}


def create_physical_table_sql(table_name: str, setup: dict[str, Any], options: dict[str, str]) -> str:
    columns = ", ".join(f"{sql_ident(str(column['name']))} {column['type']}" for column in setup["columns"])
    option_sql = ""
    if options:
        option_sql = " WITH (" + ", ".join(f"{sql_string(key)}={sql_string(value)}" for key, value in sorted(options.items())) + ")"
    return f"CREATE TABLE {sql_ident(table_name)} ({columns}, TIME INDEX ({sql_ident(str(setup['time_index']))})) ENGINE={setup['engine']}{option_sql};"


def extract_show_create_statement(result: dict[str, Any]) -> str | None:
    def visit(value: Any) -> str | None:
        if isinstance(value, dict):
            for key, nested in value.items():
                if "create" in key.lower() and "table" in key.lower() and isinstance(nested, str):
                    return nested
            for nested in value.values():
                found = visit(nested)
                if found:
                    return found
        elif isinstance(value, list):
            for nested in value:
                found = visit(nested)
                if found:
                    return found
        elif isinstance(value, str) and re.search(r"\bCREATE\s+TABLE\b", value, re.I):
            return value
        return None
    return visit(result.get("response"))


def _unquote_sql_value(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"`":
        quote = value[0]
        return value[1:-1].replace(quote * 2, quote)
    return value


def verify_show_create_setup(statement: str | None, engine: str, options: dict[str, str]) -> dict[str, Any]:
    if not statement:
        return {"status": "failed", "reason": "SHOW CREATE response did not contain a CREATE TABLE statement"}
    engine_match = re.search(r"\bENGINE\s*=\s*([`\"']?[A-Za-z_][A-Za-z0-9_]*[`\"']?)", statement, re.I)
    if not engine_match or _unquote_sql_value(engine_match.group(1)).lower() != engine.lower():
        return {"status": "failed", "reason": f"SHOW CREATE engine does not equal {engine}", "statement": statement}
    missing = []
    for key, expected in sorted(options.items()):
        matches = re.finditer(rf'''(?i)(?<![A-Za-z0-9_])[`"']?{re.escape(key)}[`"']?\s*=\s*('(?:''|[^'])*'|"(?:""|[^"])*"|`(?:``|[^`])*`|[^,\)\s]+)''', statement)
        values = [_unquote_sql_value(match.group(1)) for match in matches]
        if expected not in values:
            missing.append({"option": key, "expected": expected, "observed": values})
    if missing:
        return {"status": "failed", "reason": "SHOW CREATE options differ", "missing_or_wrong": missing, "statement": statement}
    return {"status": "observed", "statement": statement, "engine": engine, "options": options}


def setup_physical_table(target: RunTarget, remote: dict[str, Any], http_timeout: float, *, dry_run: bool) -> dict[str, Any] | None:
    setup = remote.get("physical_table_setup")
    if not isinstance(setup, dict):
        return None
    options = merge_physical_table_options(setup, target.name)
    ddl = create_physical_table_sql(str(remote["physical_table"]), setup, options)
    show_sql = f"SHOW CREATE TABLE {sql_ident(str(remote['physical_table']))}"
    evidence: dict[str, Any] = {
        "target": target.name,
        "ddl": ddl,
        "resolved_options": options,
        "show_create_sql": show_sql,
    }
    if dry_run:
        return {**evidence, "status": "planned", "show_create_response": None, "parsed_create_statement": None, "verification": {"status": "planned"}}
    ddl_result = http_post_sql(target.http_port, ddl, str(remote["database"]), http_timeout)
    evidence["ddl_result"] = ddl_result
    if not ddl_result.get("ok"):
        return {**evidence, "status": "failed", "show_create_response": None, "parsed_create_statement": None, "verification": {"status": "failed"}, "error": f"physical table DDL failed: {ddl_result}"}
    show_result = http_post_sql(target.http_port, show_sql, str(remote["database"]), http_timeout)
    statement = extract_show_create_statement(show_result)
    verification = verify_show_create_setup(statement, str(setup["engine"]), options)
    evidence.update({"status": "observed", "show_create_response": show_result, "parsed_create_statement": statement, "verification": verification})
    if not show_result.get("ok") or verification["status"] == "failed":
        evidence["status"] = "failed"
        evidence["error"] = f"physical table SHOW CREATE verification failed: {show_result}"
    return evidence


def run_remote_write(generator: Path | None, target: RunTarget, remote: dict[str, Any], *, dry_run: bool) -> dict[str, Any]:
    if generator is None:
        if not dry_run:
            raise ValueError("--fixture-generator query_perf_fixture is required for prom_remote_write_then_query")
        generator = Path("query_perf_fixture")
    metric = remote["metric"]
    physical_table = remote["physical_table"]
    cmd = [
        str(generator),
        "prom-remote-write",
        "--endpoint", f"http://127.0.0.1:{target.http_port}/v1/prometheus/write",
        "--database", remote["database"],
        "--metric", metric,
        "--physical-table", physical_table,
        "--series-count", str(int(remote["series_count"])),
        "--samples-per-series", str(int(remote["samples_per_series"])),
        "--start-unix-millis", str(int(remote["start_unix_millis"])),
        "--step-millis", str(int(remote["step_millis"])),
        "--chunk-series-count", str(int(remote["chunk_series_count"])),
        "--timeout-seconds", str(int(remote["timeout_seconds"])),
    ]
    value = remote["value"]
    cmd.extend(["--value-pattern", str(value["pattern"])])
    cmd.extend(["--value-base", str(value["base"])])
    cmd.extend(["--value-step", str(value["step"])])
    cmd.extend(["--value-cardinality", str(int(value["cardinality"]))])
    cmd.extend(["--value-seed", str(int(value["seed"]))])
    cmd.extend(["--value-run-length", str(int(value["run_length"]))])
    cmd.extend(["--value-stall-every", str(int(value["stall_every"]))])
    cmd.extend(["--value-stall-length", str(int(value["stall_length"]))])
    cmd.extend(["--value-mixed-every", str(int(value["mixed_every"]))])
    cmd.extend(["--value-series-mix-every", str(int(value["series_mix_every"]))])
    cmd.extend(["--value-gauge-residue", str(int(value["gauge_residue"]))])
    cmd.extend(["--value-fractional-step", str(value["fractional_step"])])
    if "sample_offset" in remote:
        cmd.extend(["--value-sample-offset", str(int(remote["sample_offset"]))])
    if "total_samples_per_series" in remote:
        cmd.extend(["--value-total-samples-per-series", str(int(remote["total_samples_per_series"]))])
    if dry_run:
        return {"status": "dry-run", "cmd": cmd}
    result = run_command(cmd)
    result["status"] = "ok" if result["returncode"] == 0 else "failed"
    if result["returncode"] != 0:
        raise RuntimeError(f"remote-write generator failed for {target.name}: {result['stderr'][:2000]}")
    try:
        result["summary"] = json.loads(result["stdout"])
    except json.JSONDecodeError:
        result["summary_parse_error"] = result["stdout"]
    return result


def summarize_remote_write_chunks(chunks: list[dict[str, Any]]) -> dict[str, Any]:
    summary = {"rows": 0, "samples_written": 0, "batches": 0, "elapsed_seconds": 0.0}
    for chunk in chunks:
        chunk_summary = chunk.get("summary") or {}
        summary["rows"] += int(chunk_summary.get("rows", 0))
        summary["samples_written"] += int(chunk_summary.get("samples_written", chunk_summary.get("rows", 0)))
        summary["batches"] += int(chunk_summary.get("batches", 0))
        summary["elapsed_seconds"] += float(chunk_summary.get("elapsed_seconds", chunk.get("elapsed_seconds", 0.0)))
    return summary


def flush_remote_physical_table(target: RunTarget, db: str, physical_table: str, args: argparse.Namespace, *, dry_run: bool, reason: str, chunk_index: int | None = None) -> dict[str, Any]:
    if dry_run:
        return {"status": "dry-run", "physical_table": physical_table, "reason": reason, "chunk_index": chunk_index}
    result = http_post_sql(target.http_port, f"ADMIN FLUSH_TABLE({sql_string(physical_table)})", db, args.http_timeout)
    result["physical_table"] = physical_table
    result["reason"] = reason
    result["chunk_index"] = chunk_index
    return result


def run_remote_write_ingestion(generator: Path | None, target: RunTarget, remote: dict[str, Any], args: argparse.Namespace, *, dry_run: bool) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    sample_chunk_size = remote["sample_chunk_size"]
    db = remote["database"]
    physical_table = remote["physical_table"]
    if sample_chunk_size is None:
        rw = run_remote_write(generator, target, remote, dry_run=dry_run)
        flush = flush_remote_physical_table(target, db, physical_table, args, dry_run=dry_run, reason="final")
        return rw, [flush]

    total_samples = int(remote["samples_per_series"])
    chunk_samples = int(sample_chunk_size)
    if chunk_samples <= 0:
        raise ValueError("scenario.remote_write.sample_chunk_size must be positive")
    flush_every = int(remote["flush_every_sample_chunks"])
    if flush_every <= 0:
        raise ValueError("scenario.remote_write.flush_every_sample_chunks must be positive")
    start = int(remote["start_unix_millis"])
    step = int(remote["step_millis"])
    chunks: list[dict[str, Any]] = []
    flushes: list[dict[str, Any]] = []
    chunk_index = 0
    last_flushed_chunk = 0
    for offset in range(0, total_samples, chunk_samples):
        current = min(chunk_samples, total_samples - offset)
        chunk_index += 1
        chunk_remote = dict(remote)
        chunk_remote["samples_per_series"] = current
        chunk_remote["start_unix_millis"] = start + offset * step
        chunk_remote["sample_offset"] = offset
        chunk_remote["total_samples_per_series"] = total_samples
        result = run_remote_write(generator, target, chunk_remote, dry_run=dry_run)
        result["sample_offset"] = offset
        result["samples_per_series"] = current
        result["chunk_index"] = chunk_index
        chunks.append(result)
        if chunk_index % flush_every == 0:
            flushes.append(flush_remote_physical_table(target, db, physical_table, args, dry_run=dry_run, reason="periodic", chunk_index=chunk_index))
            last_flushed_chunk = chunk_index
    if last_flushed_chunk != chunk_index:
        flushes.append(flush_remote_physical_table(target, db, physical_table, args, dry_run=dry_run, reason="final", chunk_index=chunk_index))
    aggregate = summarize_remote_write_chunks(chunks)
    if dry_run:
        series_count = int(remote["series_count"])
        chunk_series_count = int(remote["chunk_series_count"])
        aggregate["rows"] = series_count * total_samples
        aggregate["samples_written"] = aggregate["rows"]
        aggregate["batches"] = chunk_index * ((series_count + chunk_series_count - 1) // chunk_series_count)
    return {
        "status": "dry-run" if dry_run else ("ok" if all(chunk.get("status") == "ok" for chunk in chunks) else "failed"),
        "mode": "sample-chunked",
        "sample_chunk_size": chunk_samples,
        "flush_every_sample_chunks": flush_every,
        "chunks": chunks,
        "aggregate": aggregate,
    }, flushes


def expected_remote_write_rows(remote: dict[str, Any]) -> int:
    return int(remote["series_count"]) * int(remote["samples_per_series"])


def extract_count_value(result: dict[str, Any]) -> int | None:
    body = result.get("response")
    if not isinstance(body, dict):
        return None
    data = body.get("data")
    if not isinstance(data, list) or not data:
        return None
    row = data[0]
    if not isinstance(row, dict):
        return None
    for key, value in row.items():
        if key.lower() == "count(*)" or key.lower().startswith("count("):
            try:
                return int(value)
            except (TypeError, ValueError):
                return None
    return None


def poll_expected_count(target: RunTarget, table_name: str, db: str, expected_rows: int, timeout_s: float, http_timeout: float) -> dict[str, Any]:
    sql = f"SELECT count(*) FROM {sql_ident(table_name)}"
    deadline = time.monotonic() + timeout_s
    attempts = 0
    last: dict[str, Any] | None = None
    while True:
        attempts += 1
        result = http_post_sql(target.http_port, sql, db, http_timeout)
        observed_rows = extract_count_value(result)
        result["expected_rows"] = expected_rows
        result["observed_rows"] = observed_rows
        result["attempts"] = attempts
        result["row_count_ok"] = result.get("ok") and observed_rows == expected_rows
        if result["row_count_ok"]:
            return result
        last = result
        if time.monotonic() >= deadline:
            break
        time.sleep(0.5)
    assert last is not None
    last["ok"] = False
    last["row_count_ok"] = False
    last["error"] = f"expected {expected_rows} rows but observed {last.get('observed_rows')} after {attempts} attempts"
    return last


def run_remote_write_scenario(args: argparse.Namespace, case: dict[str, Any], case_path: Path, targets: list[RunTarget], report: dict[str, Any]) -> None:
    remote = scenario(case)["remote_write"]
    tables = case_tables(case)
    if getattr(args, "reuse_work_dir", False) and (isinstance(remote.get("physical_table_setup"), dict) or b4_enabled(remote.get("read_bench"))):
        report["setup_failure"] = {"status": "failed", "phase": "preparation", "reason": "--reuse-work-dir is not allowed with physical_table_setup or paired B4 benchmarking"}
        raise ValueError(report["setup_failure"]["reason"])
    if args.fixture_only:
        raise ValueError("--fixture-only is not supported for prom_remote_write_then_query; use --dry-run for planning")
    helper = args.fixture_generator or args.remote_write_generator
    if helper is not None:
        require_binary(helper, "query_perf_fixture", dry_run=args.dry_run)
    elif not args.dry_run:
        raise ValueError("--fixture-generator is required for prom_remote_write_then_query")
    storage = storage_config(remote)
    if storage and helper is None and not args.dry_run:
        raise ValueError("--fixture-generator is required when scenario.remote_write.storage.inspect is enabled")
    clusters: list[DistributedCluster] = []
    target_results: dict[str, dict[str, Any]] = {}
    storage_results = []
    use_b4 = b4_enabled(remote.get("read_bench"))
    inspections_by_target: dict[str, dict[str, Any]] = {}
    try:
        for target in targets:
            target.work_dir.mkdir(parents=True, exist_ok=True)
            config_path = write_frontend_prom_config(target, remote)
            cluster = DistributedCluster(target)
            clusters.append(cluster)
            if not args.dry_run:
                cluster.start_all(config_path)
            db = remote["database"]
            create_database = {"status": "dry-run", "database": db}
            if not args.dry_run:
                create_database = http_post_sql(target.http_port, f"CREATE DATABASE IF NOT EXISTS {sql_ident(db)}", "public", args.http_timeout)
            tr: dict[str, Any] = {"name": target.name, "binary": str(target.binary), "work_dir": str(target.work_dir), "components": cluster.component_report(), "frontend_config": str(config_path), "create_database": create_database, "validation_errors": [], "phase_order": ["create_database"]}
            report["targets"].append(tr)
            physical_setup = setup_physical_table(target, remote, args.http_timeout, dry_run=args.dry_run)
            if physical_setup is not None:
                tr["physical_table_setup"] = physical_setup
                tr["phase_order"].extend(["physical_table_ddl", "show_create"])
                if physical_setup.get("status") == "failed":
                    tr.update({"remote_write": {"status": "skipped", "reason": "physical_table_setup failed"}, "read_bench": {"status": "skipped"}, "status": "failed"})
                    tr["validation_errors"].append({"phase": "physical_table_setup", "evidence": physical_setup})
                    write_json(target.report_path, tr)
                    continue
            rw, flushes = run_remote_write_ingestion(helper, target, remote, args, dry_run=args.dry_run)
            tr["phase_order"].extend(["remote_write", "flush"])
            visibility = {"status": "dry-run"}
            query_result = {"validation": [], "validation_errors": [], "measurements": [], "status": "planned"}
            if not args.dry_run:
                visibility = poll_expected_count(target, tables[0]["name"], db, expected_remote_write_rows(remote), float(remote["visibility_timeout_seconds"]), args.http_timeout)
                if not use_b4:
                    query_result = run_queries(target, case, tables, args.http_timeout)
                cluster.stop_component("datanode")
            tr["phase_order"].append("visibility")
            storage_inspection = None
            if storage:
                storage_inspection = run_storage_inspection(helper or args.storage_inspector, target, storage, dry_run=args.dry_run)
                storage_results.append(storage_inspection)
            tr["phase_order"].append("storage_inspection")
            if use_b4:
                if storage_inspection is None:
                    raise ValueError("named projections or outer rounds require storage inspection")
                inspections_by_target[target.name] = storage_inspection
                read_bench_result = {"status": "planned", "phase": "awaiting_paired_measurement"}
                tr["phase_order"].append("artifact_freeze")
            else:
                read_bench_result = run_read_bench(args.candidate_bin, target, remote["read_bench"], storage_inspection, dry_run=args.dry_run)
            tr.update({"remote_write": rw, "flushes": flushes, "flush": flushes[-1] if flushes else None, "visibility": visibility, **query_result})
            if storage_inspection is not None:
                tr["storage_inspection"] = storage_inspection
            if use_b4:
                tr["artifact"] = {"before_measurement": artifact_layout_evidence(storage_inspection, dry_run=args.dry_run)}
            tr["read_bench"] = read_bench_result
            target_results[target.name] = query_result
        if use_b4 and not any(tr.get("status") == "failed" for tr in report["targets"]):
            assert storage is not None
            digest_config = remote.get("logical_stream_verification")
            planned_rows = int(remote["series_count"]) * int(remote["samples_per_series"])
            visibility_failures = [] if args.dry_run else [tr for tr in report["targets"] if tr["visibility"].get("ok") is not True or tr["visibility"].get("row_count_ok") is not True or not isinstance(tr["visibility"].get("observed_rows"), int) or tr["visibility"]["observed_rows"] != planned_rows]
            if visibility_failures:
                for tr in report["targets"]:
                    tr["status"] = "failed"
                    tr.setdefault("validation_errors", []).append({"phase": "planned_row_visibility", "planned_rows": planned_rows, "visibility": tr["visibility"]})
                report["logical_artifact_equality"] = {"status": "failed", "reason": "planned row visibility gate failed", "planned_rows": planned_rows}
            if isinstance(digest_config, dict) and not visibility_failures:
                digest_reports = run_logical_digests(args.candidate_bin, targets, inspections_by_target, planned_rows, digest_config, dry_run=args.dry_run)
                equality = {"status": "planned", "reports": digest_reports, "checks": {"row_count_equal": None, "expected_rows_equal": None, "canonicalization_equal": None, "digest_equal": None}} if args.dry_run else logical_digest_equality(digest_reports, targets)
                report["logical_artifact_equality"] = equality
                for tr in report["targets"]:
                    tr["logical_stream_digest"] = digest_reports[tr["name"]]
                    tr["phase_order"].append("logical_stream_digest")
                if equality["status"] == "failed":
                    for tr in report["targets"]:
                        tr["status"] = "failed"
                        tr.setdefault("validation_errors", []).append({"phase": "logical_artifact_equality", "evidence": equality})
            if not any(tr.get("status") == "failed" for tr in report["targets"]):
                query_schedule = []
                round_results: dict[str, list[dict[str, Any]]] = {target.name: [] for target in targets}
                for round_index in range(int(remote["read_bench"]["rounds"])):
                    order = paired_target_order(targets, round_index)
                    query_schedule.append({"round": round_index + 1, "target_order": [target.name for target in order]})
                    for position, target in enumerate(order, start=1):
                        tr = next(item for item in report["targets"] if item["name"] == target.name)
                        if args.dry_run:
                            result = {"validation": [], "validation_errors": [], "measurements": [], "status": "planned"}
                        else:
                            cluster = next(cluster for cluster in clusters if cluster.target.name == target.name)
                            cluster.start_datanode()
                            result = run_queries(target, case, tables, args.http_timeout)
                            cluster.stop_component("datanode")
                        round_results[target.name].append({"round": round_index + 1, "order_position": position, "result": result})
                report["query_round_schedule"] = {"status": "planned" if args.dry_run else "observed", "rounds": query_schedule}
                for target, tr in zip(targets, report["targets"]):
                    tr["query_rounds"] = round_results[target.name]
                    query_result = aggregate_query_rounds(round_results[target.name]) if not args.dry_run else {"validation": [], "validation_errors": [], "measurements": [], "status": "planned"}
                    tr.update(query_result)
                    target_results[target.name] = query_result
                    tr["phase_order"].append("query")
                    if not args.dry_run:
                        refreshed = run_storage_inspection(helper or args.storage_inspector, target, storage, dry_run=False)
                        after_tql = artifact_layout_evidence(refreshed, dry_run=False)
                        before = tr["artifact"]["before_measurement"]
                        stable = before["layout_hash"] == after_tql["layout_hash"] and before["artifact_hash"] == after_tql["artifact_hash"] and before["files"] == after_tql["files"]
                        tr["artifact"]["after_tql"] = {**after_tql, "stability": {"status": "observed", "stable": stable}}
                        if not stable:
                            tr["status"] = "failed"
                            tr.setdefault("validation_errors", []).append({"phase": "post_tql_artifact_stability", "before": before, "after": after_tql})
                    tr["phase_order"].append("post_tql_stability")
            if any(tr.get("status") == "failed" for tr in report["targets"]):
                paired_results, schedule = ({target.name: {"status": "skipped", "reason": "post-TQL mutation, logical digest, or query phase failed"} for target in targets}, [])
            else:
                paired_results, schedule = run_paired_read_bench(args.candidate_bin, targets, remote["read_bench"], inspections_by_target, {tr["name"]: tr["visibility"].get("observed_rows") for tr in report["targets"]}, dry_run=args.dry_run)
            report["read_bench_schedule"] = {"status": "planned" if args.dry_run else "observed", "artifact_phase": [target.name for target in targets], "rounds": schedule}
            for target, tr in zip(targets, report["targets"]):
                tr["read_bench"] = paired_results[target.name]
                tr["phase_order"].append("paired_read_bench")
                after = artifact_layout_evidence(None, dry_run=True) if args.dry_run else artifact_layout_evidence(run_storage_inspection(helper or args.storage_inspector, target, storage, dry_run=False), dry_run=False)
                tr["artifact"]["after_measurement"] = after
                before = tr["artifact"]["before_measurement"]
                stable = None if args.dry_run else (before["layout_hash"] == after["layout_hash"] and before["artifact_hash"] == after["artifact_hash"] and before["files"] == after["files"])
                tr["artifact"]["stability"] = {"status": "planned" if args.dry_run else "observed", "stable": stable}
                tr["phase_order"].append("final_stability")
                if stable is False:
                    tr["status"] = "failed"
                    if tr["read_bench"].get("status") != "skipped":
                        tr["read_bench"]["status"] = "failed"
                    tr.setdefault("validation_errors", []).append({"phase": "artifact_stability", "before": before, "after": after})
        for target, tr in zip(targets, report["targets"]):
            if tr.get("status") == "failed" and "flushes" not in tr:
                write_json(target.report_path, tr)
                continue
            flushes_ok = all(flush.get("ok") for flush in tr["flushes"])
            storage_inspection = tr.get("storage_inspection")
            storage_ok = storage_inspection is None or args.dry_run or storage_inspection.get("status") == "ok"
            read_bench_ok = args.dry_run or tr["read_bench"].get("status") in ("ok", "skipped")
            setup_ok = tr.get("physical_table_setup", {}).get("status") != "failed"
            remote_checks_ok = args.dry_run or (setup_ok and tr["create_database"].get("ok") and flushes_ok and tr["visibility"].get("ok") and tr["visibility"].get("row_count_ok") and storage_ok and read_bench_ok)
            if not remote_checks_ok:
                tr.setdefault("validation_errors", []).append({"phase": "remote_write_visibility_storage", "physical_setup_ok": setup_ok, "create_database_ok": tr["create_database"].get("ok"), "flushes_ok": flushes_ok, "visibility_ok": tr["visibility"].get("ok"), "row_count_ok": tr["visibility"].get("row_count_ok"), "storage_ok": storage_ok, "read_bench_ok": read_bench_ok, "create_database": tr["create_database"], "flushes": tr["flushes"], "visibility": tr["visibility"], "read_bench": tr["read_bench"]})
            tr["status"] = "planned" if args.dry_run else ("measured" if remote_checks_ok and tr.get("status") == "ok" else "failed")
            write_json(target.report_path, tr)
        if not args.dry_run:
            report["thresholds"] = enforce_thresholds(case, target_results.get(targets[0].name, {}), target_results.get(targets[1].name, {})) + enforce_storage_thresholds(storage, storage_results[0] if storage_results else None, storage_results[1] if len(storage_results) > 1 else None)
        elif storage:
            report["thresholds"] = planned_storage_thresholds(storage)
        report["status"] = "planned" if args.dry_run else ("failed" if any(t["status"] == "failed" for t in report["thresholds"]) or any(t.get("status") == "failed" for t in report["targets"]) else "ok")
    finally:
        for cluster in reversed(clusters):
            cluster.stop_all()


def main() -> int:
    args = parse_args()
    case_path = args.case.resolve()
    if args.fixture_generator is not None:
        require_binary(args.fixture_generator, "fixture generator", dry_run=False)
    case = load_normalized_case(case_path, args.fixture_generator)
    scenario_config = scenario(case)
    scenario_kind = scenario_config.get("kind")
    tables = case_tables(case)
    work_root = args.work_dir.resolve()
    work_root.mkdir(parents=True, exist_ok=True)
    require_binary(args.base_bin, "base", dry_run=args.dry_run or args.fixture_only)
    require_binary(args.candidate_bin, "candidate", dry_run=args.dry_run or args.fixture_only)
    if scenario_kind == "direct_readable_sst" and not args.dry_run and args.fixture_generator is None:
        raise ValueError("--fixture-generator is required unless --dry-run is set")
    ports = allocate_ports(16)
    targets = [make_target("base", args.base_bin.resolve(), work_root, ports[:8]), make_target("candidate", args.candidate_bin.resolve(), work_root, ports[8:])]
    require_fresh_work_dirs(targets, reuse_work_dir=args.reuse_work_dir, dry_run=args.dry_run, fixture_only=args.fixture_only)
    fixture_dir = fixture_root(work_root, case_path, case, args.fixture_cache_dir)
    reuse_fixture = args.reuse_fixture or args.fixture_cache_dir is not None
    report: dict[str, Any] = {"case_path": str(case_path), "case": case.get("case", {}), "scenario": scenario_config, "queries": planned_queries(case), "dry_run": args.dry_run, "fixture_only": args.fixture_only, "query_mode": "fixture-only" if args.fixture_only else "distributed", "reuse_work_dir": args.reuse_work_dir, "reuse_fixture": reuse_fixture, "fixture_cache_dir": str(args.fixture_cache_dir.resolve()) if args.fixture_cache_dir is not None else None, "fixture_dir": str(fixture_dir), "http_timeout": args.http_timeout, "targets": [], "thresholds": [], "status": "planned" if args.dry_run else "running"}

    if scenario_kind == "prom_remote_write_then_query":
        try:
            run_remote_write_scenario(args, case, case_path, targets, report)
        except Exception as e:  # noqa: BLE001 - write machine-readable failure report
            report["status"] = "failed"
            report["error"] = repr(e)
        write_json(work_root / "query-regression-report.json", report)
        print(json.dumps(report, indent=2, sort_keys=True))
        return 1 if report["status"] == "failed" else 0

    if args.fixture_only or args.dry_run:
        generations = []
        for table_idx, table in enumerate(tables):
            per_table_fixture_dir = table_fixture_dir(fixture_dir, tables, table, table_idx)
            generations.append(generate_fixture(args.fixture_generator, case_path, per_table_fixture_dir, dry_run=args.dry_run, reuse_fixture=reuse_fixture, allow_large_fixture=args.allow_large_fixture, table_name=table["name"] if len(tables) > 1 else None, database=table["database"] if len(tables) > 1 else None))
        report["fixture_generation"] = generations[0] if len(generations) == 1 else generations
        for target in targets:
            target.work_dir.mkdir(parents=True, exist_ok=True)
            mats = []
            for table_idx, table in enumerate(tables):
                per_table_fixture_dir = table_fixture_dir(fixture_dir, tables, table, table_idx)
                mats.append(materialize_fixture(target, dry_run=args.dry_run, preserve_state=False, fixture_dir=per_table_fixture_dir, reset_data=not mats))
            tr = {"name": target.name, "binary": str(target.binary), "work_dir": str(target.work_dir), "data_dir": str(target.data_dir), "fixture_dir": str(fixture_dir), "fixture_materialization": mats[0] if len(mats) == 1 else mats, "measurements": [], "status": "planned" if args.dry_run else "fixture-ready"}
            write_json(target.report_path, tr)
            report["targets"].append(tr)
        report["status"] = "planned" if args.dry_run else "fixture-ready"
        write_json(work_root / "query-regression-report.json", report)
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0

    clusters: list[DistributedCluster] = []
    try:
        discovered = []
        for target in targets:
            target.work_dir.mkdir(parents=True, exist_ok=True)
            cluster = DistributedCluster(target)
            clusters.append(cluster)
            cluster.start_all()
            create_results = []
            metas = []
            for table in tables:
                create_result = http_post_sql(target.http_port, create_table_sql(table), table["database"], args.http_timeout)
                if not create_result["ok"]:
                    raise RuntimeError(f"CREATE TABLE {table['name']} failed for {target.name}: {create_result}")
                create_results.append({"table": table["name"], "result": create_result})
                metas.append({"table": table["name"], **discover_region_via_frontend(target, table, args.http_timeout)})
            cluster.stop_component("datanode")
            cluster._ensure_metasrv_alive()
            discovered.append(metas)
            report["targets"].append({"name": target.name, "binary": str(target.binary), "work_dir": str(target.work_dir), "data_dir": str(target.data_dir), "datanode_data_home": str(target.datanode_data_dir), "components": cluster.component_report(), "create_table": create_results[0]["result"] if len(create_results) == 1 else create_results, "discovered": metas[0] if len(metas) == 1 else metas, "discovered_tables": metas})

        if len(discovered[0]) != len(discovered[1]) or any(a["table"] != b["table"] or a["region_id"] != b["region_id"] or a["table_dir"] != b["table_dir"] for a, b in zip(discovered[0], discovered[1])):
            raise RuntimeError(f"base/candidate metadata mismatch: {discovered}")
        generations = []
        for table_idx, meta in enumerate(discovered[0]):
            per_table_fixture_dir = table_fixture_dir(fixture_dir, tables, tables[table_idx], table_idx)
            generations.append(generate_fixture(args.fixture_generator, case_path, per_table_fixture_dir, dry_run=False, reuse_fixture=reuse_fixture, allow_large_fixture=args.allow_large_fixture, table_name=meta["table"] if len(tables) > 1 else None, database=meta["schema"] if len(tables) > 1 else None, region_id=meta["region_id"], table_dir=meta["table_dir"], region_dir=meta["region_dir"]))
        report["fixture_generation"] = generations[0] if len(generations) == 1 else generations

        target_results = []
        for idx, target in enumerate(targets):
            cluster = clusters[idx]
            materialize = []
            for table_idx, meta in enumerate(discovered[idx]):
                per_table_fixture_dir = table_fixture_dir(fixture_dir, tables, tables[table_idx], table_idx)
                materialize.append(materialize_fixture(target, dry_run=False, preserve_state=True, expected_region_dir=meta["region_dir"], fixture_dir=per_table_fixture_dir))
            cluster.start_datanode()
            query_result = run_queries(target, case, tables, args.http_timeout)
            if query_result["status"] == "failed":
                cluster.restart_frontend()
                retry_result = run_queries(target, case, tables, args.http_timeout)
                query_result["frontend_restart_retry"] = retry_result
                if retry_result["status"] == "ok":
                    query_result = retry_result
            report["targets"][idx]["fixture_dir"] = str(fixture_dir)
            report["targets"][idx]["fixture_materialization"] = materialize[0] if len(materialize) == 1 else materialize
            report["targets"][idx].update(query_result)
            report["targets"][idx]["status"] = "measured" if query_result["status"] == "ok" else "failed"
            write_json(target.report_path, report["targets"][idx])
            target_results.append(query_result)

        report["thresholds"] = enforce_thresholds(case, target_results[0], target_results[1])
        report["status"] = "failed" if any(t["status"] == "failed" for t in report["thresholds"]) or any(r["status"] == "failed" for r in target_results) else "ok"
    except Exception as e:  # noqa: BLE001 - write machine-readable failure report
        report["status"] = "failed"
        report["error"] = repr(e)
    finally:
        for cluster in reversed(clusters):
            cluster.stop_all()
    write_json(work_root / "query-regression-report.json", report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 1 if report["status"] == "failed" else 0


if __name__ == "__main__":
    sys.exit(main())


class QueryRegressionRunnerUnitTests(unittest.TestCase):
    def test_logical_digest_command_and_validation(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            target = make_target("base", Path("/bin/true"), Path(tmpdir), list(range(10000, 10008)))
            inspection = {"status": "dry-run"}
            config = {"labels": ["host", "instance"], "timestamp_column": "ts", "value_column": "value"}
            cmd = logical_digest_command(Path("/bin/true"), target, inspection, config)
            self.assertEqual([cmd[cmd.index("--digest-label") + 1], cmd[cmd.index("--digest-label", cmd.index("--digest-label") + 1) + 1]], ["host", "instance"])
            self.assertNotIn("--scan-config", cmd)
            payload = {"status": "ok", "schema_version": 1, "algorithm": "sha256", "canonicalization": "logical-metric-row-v1", "scanner": "series", "parallelism": 1, "labels": [{"name": "host", "column_id": 2, "type": "string"}, {"name": "instance", "column_id": 3, "type": "string"}], "timestamp": {"name": "ts", "column_id": 0, "unit": "millisecond"}, "value": {"name": "value", "column_id": 1, "type": "float64", "encoding": "ieee754-bits"}, "rows": 2, "digest": "a" * 64}
            self.assertEqual(parse_logical_digest(LOGICAL_DIGEST_PREFIX + json.dumps(payload), config, 2)["digest"], "a" * 64)
            with self.assertRaises(ValueError):
                parse_logical_digest("", config, 2)
            with self.assertRaises(ValueError):
                parse_logical_digest((LOGICAL_DIGEST_PREFIX + json.dumps(payload)) * 2, config, 2)
            invalid = json.loads(json.dumps(payload))
            invalid["labels"][0]["type"] = "binary"
            with self.assertRaises(ValueError):
                parse_logical_digest(LOGICAL_DIGEST_PREFIX + json.dumps(invalid), config, 2)
            invalid = json.loads(json.dumps(payload))
            invalid["timestamp"]["unit"] = "nanosecond"
            with self.assertRaises(ValueError):
                parse_logical_digest(LOGICAL_DIGEST_PREFIX + json.dumps(invalid), config, 2)

    def test_finalized_b4_query_results_drive_thresholds(self) -> None:
        case = {"scenario": {"kind": "prom_remote_write_then_query", "queries": [{"name": "q", "thresholds": {"max_candidate_latency_regression_pct": 10}}]}}
        base = {"measurements": [{"name": "q", "latency_ms_median": 10.0}]}
        candidate = {"measurements": [{"name": "q", "latency_ms_median": 12.0}]}
        threshold = enforce_thresholds(case, base, candidate)
        self.assertEqual(threshold[0]["status"], "failed")
        candidate["measurements"][0]["latency_ms_median"] = 11.0
        self.assertEqual(enforce_thresholds(case, base, candidate)[0]["status"], "passed")

    def test_option_merge_and_deterministic_ddl(self) -> None:
        setup = {
            "columns": [{"name": "host", "type": "STRING"}, {"name": "ts", "type": "TIMESTAMP(3)"}],
            "time_index": "ts",
            "engine": "mito",
            "options": {"z_option": "common", "a_option": "keep"},
            "target_options": {"candidate": {"z_option": "override"}},
        }
        options = merge_physical_table_options(setup, "candidate")
        self.assertEqual(options, {"a_option": "keep", "z_option": "override"})
        ddl = create_physical_table_sql("physical", setup, options)
        self.assertIn('CREATE TABLE "physical"', ddl)
        self.assertLess(ddl.index("'a_option'"), ddl.index("'z_option'"))
        self.assertNotIn("IF NOT EXISTS", ddl)

    def test_show_create_requires_exact_engine_and_options(self) -> None:
        statement = 'CREATE TABLE "physical" ("ts" TIMESTAMP TIME INDEX ("ts")) ENGINE=MITO WITH ("compression" = "zstd", \'append_mode\'=\'false\')'
        verified = verify_show_create_setup(statement, "mito", {"append_mode": "false", "compression": "zstd"})
        self.assertEqual(verified["status"], "observed")
        self.assertEqual(verify_show_create_setup(statement, "mito", {"compression": "plain"})["status"], "failed")
        self.assertEqual(verify_show_create_setup(statement, "metric", {})["status"], "failed")

    def test_named_projection_null_and_explicit_command_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            target = make_target("base", Path("/bin/true"), Path(tmpdir), list(range(10000, 10008)))
            read_bench = {"iterations": 8, "max_files": None, "parquetbench": True, "scanbench": True, "parquet_reader": "direct", "scan_scanner": "seq", "parallelism": 1, "projection": ["legacy"], "projections": [{"name": "all", "columns": None}, {"name": "value", "columns": ["greptime_value"]}], "rounds": 2}
            inspection = {"status": "dry-run"}
            all_specs = build_b4_commands(Path("/bin/true"), target, read_bench, inspection, read_bench["projections"][0], dry_run=True)
            value_specs = build_b4_commands(Path("/bin/true"), target, read_bench, inspection, read_bench["projections"][1], dry_run=True)
            self.assertTrue(all(spec["scan_config"] == {} for spec in all_specs))
            self.assertTrue(all(spec["scan_config"] == {"projection_names": ["greptime_value"]} for spec in value_specs))
            self.assertFalse(any("projection_names" in spec["cmd"] for spec in all_specs))

    def test_parses_current_parquet_and_scan_iteration_lines(self) -> None:
        parquet = "\n".join([f"  Iteration {index}: 42 rows, 3 columns, 1 record batches in {index}.0ms (42/s, 1 B/s)" for index in range(1, 4)])
        scan = "\n".join([f"  [iter {index}] 42 rows in {index}.0ms (1 partitions), array_mem_size: 1 B, estimated_size: 1 B" for index in range(1, 4)])
        self.assertEqual(parse_bench_iterations(parquet, "parquet", 3)[2]["duration_ms"], 3.0)
        self.assertEqual(parse_bench_iterations(scan, "sequential", 3)[1]["rows"], 42)
        with self.assertRaises(ValueError):
            parse_bench_iterations("  Iteration 1: malformed", "parquet", 1)
        with self.assertRaises(ValueError):
            parse_bench_iterations(parquet, "parquet", 3, expected_rows=99)
        with self.assertRaises(ValueError):
            parse_bench_iterations(scan, "sequential", 3, expected_rows=0)

    def test_artifact_content_hash_detects_same_size_mutation_and_setup_failures(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            sst = root / "one.parquet"
            sst.write_bytes(b"abcd")
            inspection = {"summary": {"root": str(root), "files": [{"relative_path": "one.parquet", "num_rows": 4}]}}
            before = artifact_layout_evidence(inspection, dry_run=False)
            sst.write_bytes(b"wxyz")
            after = artifact_layout_evidence(inspection, dry_run=False)
            self.assertEqual(before["layout_hash"], after["layout_hash"])
            self.assertNotEqual(before["artifact_hash"], after["artifact_hash"])
            target = make_target("base", Path("/bin/true"), root, list(range(10000, 10008)))
            remote = {"database": "public", "physical_table": "p", "physical_table_setup": {"columns": [{"name": "ts", "type": "TIMESTAMP"}], "time_index": "ts", "engine": "mito", "options": {"quoted": "a'b", "empty": ""}, "target_options": {}}}
            with patch(__name__ + ".http_post_sql", return_value={"ok": False, "response": {"error": "no"}}):
                evidence = setup_physical_table(target, remote, 1.0, dry_run=False)
                assert evidence is not None
                self.assertEqual(evidence["status"], "failed")
            self.assertIn("a''b", create_physical_table_sql("p", remote["physical_table_setup"], merge_physical_table_options(remote["physical_table_setup"], "base")))

    def test_warmup_discard_median_and_alternating_order(self) -> None:
        samples = [{"iteration": index, "rows": 9, "duration_ms": value} for index, value in enumerate([99.0, 1.0, 3.0, 2.0], start=1)]
        summary = summarize_iteration_samples(samples)
        self.assertEqual([sample["duration_ms"] for sample in summary["kept_samples"]], [1.0, 3.0, 2.0])
        self.assertEqual(summary["median_ms"], 2.0)
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            targets = [make_target("base", Path("/bin/true"), root, list(range(10000, 10008))), make_target("candidate", Path("/bin/true"), root, list(range(10008, 10016)))]
            self.assertEqual([target.name for target in paired_target_order(targets, 0)], ["base", "candidate"])
            self.assertEqual([target.name for target in paired_target_order(targets, 1)], ["candidate", "base"])

    def test_dry_run_b4_phase_order_and_legacy_path(self) -> None:
        remote = {"database": "public", "metric": "metric", "physical_table": "physical", "physical_table_setup": {"columns": [{"name": "ts", "type": "TIMESTAMP"}], "time_index": "ts", "engine": "mito", "options": {"compression": "zstd"}, "target_options": {}}, "read_bench": {"enabled": True, "iterations": 2, "rounds": 2, "projection": [], "projections": [{"name": "all", "columns": None}], "max_files": None, "parquetbench": True, "scanbench": True, "parquet_reader": "direct", "scan_scanner": "seq", "parallelism": 1}, "storage": {"inspect": True, "column": "greptime_value", "include_metadata_files": False, "root_suffix": None, "planned_thresholds": []}, "prom_store": {"pending_rows_flush_interval": "1s", "max_batch_rows": 1, "max_concurrent_flushes": 1, "worker_channel_capacity": 1, "max_inflight_requests": 1}, "series_count": 1, "samples_per_series": 1, "sample_chunk_size": None, "visibility_timeout_seconds": 1}
        case = {"scenario": {"kind": "prom_remote_write_then_query", "remote_write": remote, "queries": []}}
        args = argparse.Namespace(fixture_only=False, fixture_generator=Path("/bin/true"), remote_write_generator=None, storage_inspector=None, candidate_bin=Path("/bin/true"), dry_run=True, http_timeout=1.0)
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            targets = [make_target("base", Path("/bin/true"), root, list(range(10000, 10008))), make_target("candidate", Path("/bin/true"), root, list(range(10008, 10016)))]
            report: dict[str, Any] = {"targets": [], "thresholds": []}
            with patch(__name__ + ".run_remote_write_ingestion", return_value=({"status": "dry-run"}, [{"status": "dry-run"}])):
                run_remote_write_scenario(args, case, root / "case.toml", targets, report)
            self.assertEqual(report["read_bench_schedule"]["artifact_phase"], ["base", "candidate"])
            self.assertEqual(report["read_bench_schedule"]["rounds"][1]["target_order"], ["candidate", "base"])
            self.assertEqual(report["targets"][0]["physical_table_setup"]["status"], "planned")
            self.assertLess(report["targets"][0]["phase_order"].index("show_create"), report["targets"][0]["phase_order"].index("remote_write"))
        self.assertFalse(b4_enabled({"rounds": 1, "projections": []}))
