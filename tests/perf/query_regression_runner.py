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
import json
import os
import re
import shutil
import socket
import statistics
import subprocess
import sys
import time
import tomllib
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any


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
    p.add_argument("--work-dir", required=True, type=Path)
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


def column_sql(col: dict[str, Any]) -> str:
    return f"{sql_ident(col['name'])} {col['type']}"


def case_tables(case: dict[str, Any]) -> list[dict[str, Any]]:
    tables = case.get("tables") or []
    if not tables or case.get("layout", {}).get("regions") != 1:
        raise ValueError("runner supports one or more tables and exactly one region per table")
    pairs = [(table.get("database"), table.get("name")) for table in tables]
    if len(set(pairs)) != len(pairs):
        raise ValueError("duplicate (database, name) table entries are not supported")
    names = [table.get("name") for table in tables]
    if len(set(names)) != len(names):
        raise ValueError("duplicate table names are not supported because fixture generator --table selects by name")
    return list(tables)


def workload_kind(case: dict[str, Any]) -> str:
    workload = case.get("workload")
    if not isinstance(workload, dict):
        raise ValueError("case requires [workload] with kind = 'direct_readable_sst'")
    kind = workload.get("kind")
    if kind != "direct_readable_sst":
        raise ValueError(f"unsupported workload kind {kind!r}; only 'direct_readable_sst' is supported")
    if not isinstance(workload.get("direct_readable_sst"), dict):
        raise ValueError("direct_readable_sst workload requires [workload.direct_readable_sst]")
    return kind


def fixture_subdir(table: dict[str, Any], index: int) -> str:
    raw = f"{index:02d}_{table['database']}_{table['name']}"
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", raw).strip("._-")
    return safe or f"table_{index:02d}"


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
    queries = case.get("queries", [])
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

    def start_frontend(self) -> None:
        self._ensure_metasrv_alive()
        log_dir = self.target.work_dir / "logs" / "frontend"
        self._spawn(
            "frontend",
            [
                str(self.target.binary),
                "frontend",
                "start",
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
            ],
        )
        wait_health(self.target.http_port)

    def start_all(self) -> None:
        self.start_metasrv()
        self.start_datanode()
        self.start_frontend()

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
    if reuse_fixture and summary_path.exists():
        summary = json.loads(summary_path.read_text())
        assert_fixture_summary(summary, region_id=region_id, table_dir=table_dir, region_dir=region_dir, table_name=table_name, database=database)
        return {"status": "reused", "fixture_dir": str(fixture_dir), "summary": summary}
    if fixture_dir.exists() and not reuse_fixture:
        shutil.rmtree(fixture_dir)
    fixture_dir.mkdir(parents=True, exist_ok=True)
    cmd = [str(generator), "--case", str(case_path), "--out-dir", str(fixture_dir)]
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
    return result


def copy_tree_contents(src: Path, dst: Path) -> None:
    if not src.exists():
        raise FileNotFoundError(f"fixture path does not exist: {src}")
    dst.mkdir(parents=True, exist_ok=True)
    for child in src.iterdir():
        target = dst / child.name
        if child.is_dir():
            shutil.copytree(child, target, dirs_exist_ok=True)
        else:
            shutil.copy2(child, target)


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
    if preserve_state:
        fixture_region_dir = object_store_dir / region_dir
        copy_tree_contents(fixture_region_dir, target_region_dir)
    else:
        copy_tree_contents(object_store_dir, materialize_root)
    if target_manifest_dir.exists():
        shutil.rmtree(target_manifest_dir)
    copy_tree_contents(manifest_dir, target_manifest_dir)
    return {"status": "ok", "summary_path": str(summary_path), "data_dir": str(materialize_root), "region_dir": region_dir, "manifest_dir": str(target_manifest_dir), "preserve_state": preserve_state}


def response_text(body: Any) -> str:
    return json.dumps(body, sort_keys=True) if not isinstance(body, str) else body


def validate_show_create(result: dict[str, Any], table: dict[str, Any]) -> list[str]:
    text = response_text(result.get("response", {})).lower()
    errors = []
    if table["name"].lower() not in text:
        errors.append("SHOW CREATE output does not contain table name")
    if "engine" not in text or "mito" not in text:
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
                results.append({"query": cm["name"], "threshold": key, "status": "not_enforced", "reason": "runner does not yet extract server scan metrics"})
    return results


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def require_fresh_work_dirs(targets: list[RunTarget], *, reuse_work_dir: bool, dry_run: bool, fixture_only: bool) -> None:
    if dry_run or reuse_work_dir or fixture_only:
        return
    for target in targets:
        if target.work_dir.exists() and any(target.work_dir.iterdir()):
            raise RuntimeError(f"target work_dir exists and is non-empty: {target.work_dir}; pass --reuse-work-dir to override")


def main() -> int:
    args = parse_args()
    case_path = args.case.resolve()
    case = load_case(case_path)
    workload_kind(case)
    tables = case_tables(case)
    work_root = args.work_dir.resolve()
    work_root.mkdir(parents=True, exist_ok=True)
    require_binary(args.base_bin, "base", dry_run=args.dry_run or args.fixture_only)
    require_binary(args.candidate_bin, "candidate", dry_run=args.dry_run or args.fixture_only)
    if not args.dry_run and args.fixture_generator is None:
        raise ValueError("--fixture-generator is required unless --dry-run is set")
    if args.fixture_generator is not None:
        require_binary(args.fixture_generator, "fixture generator", dry_run=args.dry_run)

    ports = allocate_ports(16)
    targets = [make_target("base", args.base_bin.resolve(), work_root, ports[:8]), make_target("candidate", args.candidate_bin.resolve(), work_root, ports[8:])]
    require_fresh_work_dirs(targets, reuse_work_dir=args.reuse_work_dir, dry_run=args.dry_run, fixture_only=args.fixture_only)
    fixture_dir = work_root / "fixture"
    report: dict[str, Any] = {"case_path": str(case_path), "case": case.get("case", {}), "fixture": case.get("fixture", {}), "workload": case["workload"], "queries": planned_queries(case), "dry_run": args.dry_run, "fixture_only": args.fixture_only, "query_mode": "fixture-only" if args.fixture_only else "distributed", "reuse_work_dir": args.reuse_work_dir, "http_timeout": args.http_timeout, "targets": [], "thresholds": [], "status": "planned" if args.dry_run else "running"}

    if args.fixture_only or args.dry_run:
        generations = []
        for table_idx, table in enumerate(tables):
            per_table_fixture_dir = table_fixture_dir(fixture_dir, tables, table, table_idx)
            generations.append(generate_fixture(args.fixture_generator, case_path, per_table_fixture_dir, dry_run=args.dry_run, reuse_fixture=args.reuse_fixture, allow_large_fixture=args.allow_large_fixture, table_name=table["name"] if len(tables) > 1 else None, database=table["database"] if len(tables) > 1 else None))
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
            generations.append(generate_fixture(args.fixture_generator, case_path, per_table_fixture_dir, dry_run=False, reuse_fixture=args.reuse_fixture, allow_large_fixture=args.allow_large_fixture, table_name=meta["table"] if len(tables) > 1 else None, database=meta["schema"] if len(tables) > 1 else None, region_id=meta["region_id"], table_dir=meta["table_dir"], region_dir=meta["region_dir"]))
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
