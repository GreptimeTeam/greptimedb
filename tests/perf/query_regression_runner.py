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
import time
import tomllib
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
    p.add_argument("--remote-write-generator", type=Path, help="prom_remote_write_fixture helper binary")
    p.add_argument("--storage-inspector", type=Path, help="parquet_footer_inspector helper binary")
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
        remote = value.get("remote_write") or {}
        metric = remote.get("metric") or remote.get("metric_name")
        database = remote.get("database", "public")
        if not metric:
            raise ValueError("remote-write scenario requires scenario.remote_write.metric")
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
    value = remote.get("storage")
    if not isinstance(value, dict):
        return None
    if value.get("inspect", True) is False:
        return None
    return value


def run_storage_inspection(inspector: Path | None, target: RunTarget, storage: dict[str, Any], *, dry_run: bool) -> dict[str, Any]:
    if inspector is None:
        if not dry_run:
            raise ValueError("--storage-inspector is required when scenario.remote_write.storage.inspect is enabled")
        inspector = Path("parquet_footer_inspector")
    root = target.datanode_data_dir
    if storage.get("root_suffix"):
        root = root / str(storage["root_suffix"])
    cmd = [
        str(inspector),
        "--root", str(root),
        "--column", str(storage.get("column", "greptime_value")),
    ]
    if storage.get("include_metadata_files", False):
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
        limit = storage.get(name, 1 if name in ("min_files", "min_files_with_column") else None)
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
        for required in storage.get("require_encodings", []):
            results.append({"target": target_name, "threshold": "require_encodings", "encoding": required, "status": "passed" if required in encodings else "failed"})
        for forbidden in storage.get("forbid_encodings", []):
            results.append({"target": target_name, "threshold": "forbid_encodings", "encoding": forbidden, "status": "passed" if forbidden not in encodings else "failed"})
    add_cmp("max_candidate_total_file_size_regression_pct", "total_file_size")
    add_cmp("max_candidate_column_compressed_size_regression_pct", "column_compressed_size")
    add_cmp("max_candidate_column_uncompressed_size_regression_pct", "column_uncompressed_size")
    return results


def planned_storage_thresholds(storage: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not storage:
        return []
    keys = [
        "min_files",
        "min_files_with_column",
        "require_encodings",
        "forbid_encodings",
        "max_total_file_size_bytes",
        "max_column_compressed_size_bytes",
        "max_column_uncompressed_size_bytes",
        "max_candidate_total_file_size_regression_pct",
        "max_candidate_column_compressed_size_regression_pct",
        "max_candidate_column_uncompressed_size_regression_pct",
    ]
    planned = []
    for key in keys:
        if key in storage or key in ("min_files", "min_files_with_column"):
            planned.append({"threshold": key, "status": "planned", "value": storage.get(key, 1)})
    return planned


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
    prom = remote.get("prom_store") or {}
    path = target.work_dir / "frontend-prom-store.toml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "[prom_store]\n"
        "enable = true\n"
        "with_metric_engine = true\n"
        f"pending_rows_flush_interval = {json.dumps(str(prom.get('pending_rows_flush_interval', '1s')))}\n"
        f"max_batch_rows = {int(prom.get('max_batch_rows', 100000))}\n"
        f"max_concurrent_flushes = {int(prom.get('max_concurrent_flushes', 256))}\n"
        f"worker_channel_capacity = {int(prom.get('worker_channel_capacity', 65526))}\n"
        f"max_inflight_requests = {int(prom.get('max_inflight_requests', 3000))}\n"
    )
    return path


def run_remote_write(generator: Path | None, target: RunTarget, remote: dict[str, Any], *, dry_run: bool) -> dict[str, Any]:
    if generator is None:
        if not dry_run:
            raise ValueError("--remote-write-generator is required for prom_remote_write_then_query unless --dry-run is set")
        generator = Path("prom_remote_write_fixture")
    metric = remote.get("metric") or remote.get("metric_name")
    physical_table = remote.get("physical_table", "greptime_physical_table")
    cmd = [
        str(generator),
        "--endpoint", f"http://127.0.0.1:{target.http_port}/v1/prometheus/write",
        "--database", remote.get("database", "public"),
        "--metric", metric,
        "--physical-table", physical_table,
        "--series-count", str(int(remote.get("series_count", 8))),
        "--samples-per-series", str(int(remote.get("samples_per_series", 30))),
        "--start-unix-millis", str(int(remote.get("start_unix_millis", 1_704_067_200_000))),
        "--step-millis", str(int(remote.get("step_millis", 15_000))),
        "--chunk-series-count", str(int(remote.get("chunk_series_count", remote.get("batch_size", 8)))),
        "--timeout-seconds", str(int(remote.get("timeout_seconds", 60))),
    ]
    value = remote.get("value")
    if isinstance(value, dict):
        if "pattern" in value:
            cmd.extend(["--value-pattern", str(value["pattern"])])
        if "base" in value:
            cmd.extend(["--value-base", str(value["base"])])
        if "step" in value:
            cmd.extend(["--value-step", str(value["step"])])
        if "cardinality" in value:
            cmd.extend(["--value-cardinality", str(int(value["cardinality"]))])
        if "seed" in value:
            cmd.extend(["--value-seed", str(int(value["seed"]))])
        if "run_length" in value:
            cmd.extend(["--value-run-length", str(int(value["run_length"]))])
        if "stall_every" in value:
            cmd.extend(["--value-stall-every", str(int(value["stall_every"]))])
        if "stall_length" in value:
            cmd.extend(["--value-stall-length", str(int(value["stall_length"]))])
        if "mixed_every" in value:
            cmd.extend(["--value-mixed-every", str(int(value["mixed_every"]))])
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
    sample_chunk_size = remote.get("sample_chunk_size")
    db = remote.get("database", "public")
    physical_table = remote.get("physical_table", "greptime_physical_table")
    if sample_chunk_size is None:
        rw = run_remote_write(generator, target, remote, dry_run=dry_run)
        flush = flush_remote_physical_table(target, db, physical_table, args, dry_run=dry_run, reason="final")
        return rw, [flush]

    total_samples = int(remote.get("samples_per_series", 30))
    chunk_samples = int(sample_chunk_size)
    if chunk_samples <= 0:
        raise ValueError("scenario.remote_write.sample_chunk_size must be positive")
    flush_every = int(remote.get("flush_every_sample_chunks", 1))
    if flush_every <= 0:
        raise ValueError("scenario.remote_write.flush_every_sample_chunks must be positive")
    start = int(remote.get("start_unix_millis", 1_704_067_200_000))
    step = int(remote.get("step_millis", 15_000))
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
        series_count = int(remote.get("series_count", 8))
        chunk_series_count = int(remote.get("chunk_series_count", remote.get("batch_size", 8)))
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
    return int(remote.get("series_count", 8)) * int(remote.get("samples_per_series", 30))


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
    remote = scenario(case).get("remote_write") or {}
    tables = case_tables(case)
    if args.fixture_only:
        raise ValueError("--fixture-only is not supported for prom_remote_write_then_query; use --dry-run for planning")
    if args.remote_write_generator is not None:
        require_binary(args.remote_write_generator, "remote-write generator", dry_run=args.dry_run)
    elif not args.dry_run:
        raise ValueError("--remote-write-generator is required for prom_remote_write_then_query")
    storage = storage_config(remote)
    if storage and args.storage_inspector is not None:
        require_binary(args.storage_inspector, "storage inspector", dry_run=args.dry_run)
    elif storage and not args.dry_run:
        raise ValueError("--storage-inspector is required when scenario.remote_write.storage.inspect is enabled")
    clusters: list[DistributedCluster] = []
    target_results = []
    storage_results = []
    try:
        for target in targets:
            target.work_dir.mkdir(parents=True, exist_ok=True)
            config_path = write_frontend_prom_config(target, remote)
            cluster = DistributedCluster(target)
            clusters.append(cluster)
            if not args.dry_run:
                cluster.start_all(config_path)
            db = remote.get("database", "public")
            create_database = {"status": "dry-run", "database": db}
            if not args.dry_run:
                create_database = http_post_sql(target.http_port, f"CREATE DATABASE IF NOT EXISTS {sql_ident(db)}", "public", args.http_timeout)
            rw, flushes = run_remote_write_ingestion(args.remote_write_generator, target, remote, args, dry_run=args.dry_run)
            visibility = {"status": "dry-run"}
            query_result = {"validation": [], "validation_errors": [], "measurements": [], "status": "planned"}
            if not args.dry_run:
                visibility = poll_expected_count(target, tables[0]["name"], db, expected_remote_write_rows(remote), float(remote.get("visibility_timeout_seconds", 30)), args.http_timeout)
            storage_inspection = None
            if storage:
                storage_inspection = run_storage_inspection(args.storage_inspector, target, storage, dry_run=args.dry_run)
                storage_results.append(storage_inspection)
            if not args.dry_run:
                query_result = run_queries(target, case, tables, args.http_timeout)
            tr = {"name": target.name, "binary": str(target.binary), "work_dir": str(target.work_dir), "components": cluster.component_report(), "frontend_config": str(config_path), "create_database": create_database, "remote_write": rw, "flushes": flushes, "flush": flushes[-1] if flushes else None, "visibility": visibility, **query_result}
            if storage_inspection is not None:
                tr["storage_inspection"] = storage_inspection
            flushes_ok = all(flush.get("ok") for flush in flushes)
            storage_ok = storage_inspection is None or args.dry_run or storage_inspection.get("status") == "ok"
            remote_checks_ok = args.dry_run or (create_database.get("ok") and flushes_ok and visibility.get("ok") and visibility.get("row_count_ok") and storage_ok)
            if not remote_checks_ok:
                tr.setdefault("validation_errors", []).append({"phase": "remote_write_visibility_storage", "create_database_ok": create_database.get("ok"), "flushes_ok": flushes_ok, "visibility_ok": visibility.get("ok"), "row_count_ok": visibility.get("row_count_ok"), "storage_ok": storage_ok, "create_database": create_database, "flushes": flushes, "visibility": visibility})
            tr["status"] = "planned" if args.dry_run else ("measured" if remote_checks_ok and query_result["status"] == "ok" else "failed")
            write_json(target.report_path, tr)
            report["targets"].append(tr)
            target_results.append(query_result)
        if not args.dry_run:
            report["thresholds"] = enforce_thresholds(case, target_results[0], target_results[1]) + enforce_storage_thresholds(storage, storage_results[0] if storage_results else None, storage_results[1] if len(storage_results) > 1 else None)
        elif storage:
            report["thresholds"] = planned_storage_thresholds(storage)
        report["status"] = "planned" if args.dry_run else ("failed" if any(t["status"] == "failed" for t in report["thresholds"]) or any(t.get("status") == "failed" for t in report["targets"]) else "ok")
    finally:
        for cluster in reversed(clusters):
            cluster.stop_all()


def main() -> int:
    args = parse_args()
    case_path = args.case.resolve()
    case = load_case(case_path)
    scenario_config = scenario(case)
    scenario_kind = scenario_config.get("kind")
    tables = case_tables(case)
    work_root = args.work_dir.resolve()
    work_root.mkdir(parents=True, exist_ok=True)
    require_binary(args.base_bin, "base", dry_run=args.dry_run or args.fixture_only)
    require_binary(args.candidate_bin, "candidate", dry_run=args.dry_run or args.fixture_only)
    if scenario_kind == "direct_readable_sst" and not args.dry_run and args.fixture_generator is None:
        raise ValueError("--fixture-generator is required unless --dry-run is set")
    if args.fixture_generator is not None:
        require_binary(args.fixture_generator, "fixture generator", dry_run=args.dry_run)

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
