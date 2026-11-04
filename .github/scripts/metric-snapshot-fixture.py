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

"""Deterministic wide, heterogeneous Metric tables for snapshot benchmarks."""

import argparse
import base64
import concurrent.futures
import hashlib
import http.client
import json
import os
from pathlib import Path
import threading
import time
import urllib.parse

GROUPS = 10
TAG_POOL = 298
ROWS = 32
DATABASE = "metrics"


class Client:
    def __init__(self, addr):
        self.addr = addr
        self.connections = threading.local()
        self.headers = {"Content-Type": "application/x-www-form-urlencoded"}
        if auth := os.environ.get("GREPTIME_BENCH_AUTH"):
            self.headers["Authorization"] = "Basic " + base64.b64encode(
                auth.encode()
            ).decode()

    def sql(self, query, database=DATABASE, batch=False):
        connection = getattr(self.connections, "connection", None)
        if connection is None:
            connection = http.client.HTTPConnection(self.addr, timeout=600)
            self.connections.connection = connection
        body = urllib.parse.urlencode({"db": database, "sql": query})
        try:
            connection.request(
                "POST", "/v1/ddl/logical-tables" if batch else "/v1/sql",
                body, self.headers,
            )
            response = connection.getresponse()
            raw = response.read()
        except (OSError, http.client.HTTPException):
            connection.close()
            self.connections.connection = None
            raise
        data = json.loads(raw)
        if response.status != 200 or "error" in data:
            raise RuntimeError(f"SQL failed (HTTP {response.status}): {data}")
        return data["output"]

    def values(self, query, database=DATABASE):
        return self.sql(query, database)[0]["records"]["rows"]


def tags(index, per_group):
    return sorted((index % per_group * 7 + offset) % TAG_POOL for offset in range(10))


def column_names(tag_ids):
    return ["ts", *[f"tag_{tag:03d}" for tag in tag_ids], "val"]


def columns(tag_ids):
    names = [f"tag_{tag:03d}" for tag in tag_ids]
    return (
        "ts TIMESTAMP TIME INDEX, "
        + ", ".join(name + " STRING" for name in names)
        + ", val DOUBLE, PRIMARY KEY(" + ",".join(names) + ")"
    )


def table_ddl(index, per_group):
    return (
        f"CREATE TABLE m{index:06d} ({columns(tags(index, per_group))}) "
        f"ENGINE=metric WITH(on_physical_table='phy_{index // per_group:02d}')"
    )


def row(index, row_index, per_group):
    values = []
    for tag in tags(index, per_group):
        if (row_index + tag) % 29 == 0:
            value = None
        elif (row_index + tag) % 17 == 0:
            value = ""
        elif tag % 10 == 0:
            value = f"instance-{index:06d}-{row_index % 4}"
        elif tag % 10 == 1:
            value = "区域-" + str(row_index % 3)
        elif tag % 10 == 2:
            value = "label-" + "x" * 48 + "-" + str(row_index % 2)
        else:
            value = f"tag{tag:03d}-v{row_index % (2 + tag % 7)}"
        values.append(value)
    return [row_index, *values, None if row_index % 7 == 0 else index % 97 + row_index / 4]


def sql_value(value):
    if value is None:
        return "NULL"
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    return str(value)


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def update_digest(digest, value):
    digest.update((canonical(value) + "\n").encode())


def progress(phase, completed, total):
    if completed == total or completed % 4096 == 0:
        print(f"{phase}: {completed}/{total}", flush=True)


def prepare(client, per_group):
    stages = {}
    started = time.monotonic()
    client.sql(f"CREATE DATABASE {DATABASE}", "public")
    for group in range(GROUPS):
        client.sql(
            f"CREATE TABLE phy_{group:02d} ({columns(range(TAG_POOL))}) "
            "ENGINE=metric WITH(physical_metric_table='')"
        )
    total = GROUPS * per_group
    for group in range(GROUPS):
        for base in range(group * per_group, (group + 1) * per_group, 128):
            end = min((group + 1) * per_group, base + 128)
            client.sql(";".join(table_ddl(i, per_group) for i in range(base, end)), batch=True)
            progress("create", end, total)
    stages["create_seconds"] = time.monotonic() - started

    def insert(index):
        rows = ",".join(
            "(" + ",".join(sql_value(v) for v in row(index, j, per_group)) + ")"
            for j in range(ROWS)
        )
        names = ",".join(column_names(tags(index, per_group)))
        client.sql(f"INSERT INTO m{index:06d} ({names}) VALUES {rows}")

    started = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        for base in range(0, total, 128):
            end = min(total, base + 128)
            list(pool.map(insert, range(base, end)))
            progress("insert", end, total)
    stages["insert_seconds"] = time.monotonic() - started
    started = time.monotonic()
    for group in range(GROUPS):
        client.sql(f"ADMIN FLUSH_TABLE('{DATABASE}.phy_{group:02d}')")
    stages["flush_seconds"] = time.monotonic() - started
    return stages


def check_records(index, per_group, records):
    names = column_names(tags(index, per_group))
    types = ["TimestampMillisecond", *["String"] * 10, "Float64"]
    expected_columns = [dict(name=name, data_type=kind) for name, kind in zip(names, types)]
    actual_columns = records["schema"]["column_schemas"]
    if sorted(actual_columns, key=lambda c: c["name"]) != sorted(
        expected_columns, key=lambda c: c["name"]
    ):
        raise RuntimeError(f"logical schema mismatch: m{index:06d}")
    # Metric tables canonicalize column order; SELECT * still checks every column.
    positions = [names.index(column["name"]) for column in actual_columns]
    expected_rows = [
        [values[position] for position in positions]
        for values in (row(index, j, per_group) for j in range(ROWS))
    ]
    # Canonical JSON also distinguishes integers, floats, booleans, and NULL.
    if canonical(records["rows"]) != canonical(expected_rows):
        raise RuntimeError(f"typed values/NULL mismatch: m{index:06d}")


def validate(client, per_group):
    total = GROUPS * per_group
    expected_names = {f"m{i:06d}" for i in range(total)} | {
        f"phy_{group:02d}" for group in range(GROUPS)
    }
    table_rows = client.values(
        "SELECT table_name, table_id FROM information_schema.tables "
        f"WHERE table_schema='{DATABASE}' ORDER BY table_name"
    )
    table_ids = dict(table_rows)
    if len(table_rows) != len(expected_names) or set(table_ids) != expected_names:
        raise RuntimeError("table membership mismatch")
    if len(set(table_ids.values())) != len(table_ids):
        raise RuntimeError("duplicate table IDs")
    digests = {name: hashlib.sha256() for name in ("schema", "data", "ddl")}

    def check_ddl(names):
        outputs = client.sql(";".join(f"SHOW CREATE TABLE {name}" for name in names))
        if len(outputs) != len(names):
            raise RuntimeError("incomplete SHOW CREATE TABLE response")
        for name, output in zip(names, outputs):
            records = output["records"]["rows"]
            if len(records) != 1 or records[0][0] != name or not records[0][1]:
                raise RuntimeError(f"invalid SHOW CREATE TABLE response: {name}")
            update_digest(digests["ddl"], records)

    for base in range(0, total, 64):
        end = min(total, base + 64)
        names = [f"m{i:06d}" for i in range(base, end)]
        outputs = client.sql(";".join(f"SELECT * FROM {name} ORDER BY ts" for name in names))
        if len(outputs) != len(names):
            raise RuntimeError("incomplete SELECT response")
        for index, output in enumerate(outputs, base):
            records = output["records"]
            check_records(index, per_group, records)
            update_digest(digests["schema"], [f"m{index:06d}", records["schema"]])
            update_digest(digests["data"], [f"m{index:06d}", records["rows"]])
        check_ddl(names)
        progress("validate", end, total)
    check_ddl([f"phy_{group:02d}" for group in range(GROUPS)])
    return {**{name + "_sha256": digest.hexdigest() for name, digest in digests.items()},
            "table_ids": table_ids}


def compare_source(result, source):
    for field in ("fixture", "schema_sha256", "data_sha256", "ddl_sha256"):
        if result[field] != source[field]:
            raise RuntimeError(f"source/target mismatch: {field}")
    source_ids, target_ids = source["table_ids"], result["table_ids"]
    if source_ids.keys() != target_ids.keys():
        raise RuntimeError("source/target table membership mismatch")
    changed = sum(source_ids[name] != target_ids[name] for name in source_ids)
    if changed != len(source_ids):
        raise RuntimeError(f"table IDs not all remapped: {changed}/{len(source_ids)}")
    result["remapped_table_ids"] = changed


def placeholders(client):
    database = "metric_bench_guard"
    client.sql(f"CREATE DATABASE {database}", "public")
    for index in range(17):
        client.sql(f"CREATE TABLE placeholder_{index} (ts TIMESTAMP TIME INDEX)", database)
    return {"placeholder_tables": 17}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "validate", "placeholders"))
    parser.add_argument("--addr", default="127.0.0.1:4000")
    parser.add_argument("--tables-per-group", type=int, default=10000)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--source", type=Path, help="Source validation JSON, for target comparison")
    args = parser.parse_args()
    if not 50 <= args.tables_per_group <= 10000:
        parser.error("--tables-per-group must be between 50 and 10000")
    if args.source is not None and args.action != "validate":
        parser.error("--source only applies to validate")
    coverage = {tag for i in range(args.tables_per_group) for tag in tags(i, args.tables_per_group)}
    if coverage != set(range(TAG_POOL)):
        parser.error("fixture does not cover all 298 tags")
    client = Client(args.addr)
    started = time.monotonic()
    result = {"fixture": {"groups": GROUPS, "tables_per_group": args.tables_per_group,
                          "rows_per_table": ROWS, "tag_pool": TAG_POOL}}
    if args.action == "placeholders":
        result.update(placeholders(client))
    elif args.action == "prepare":
        result.update(prepare(client, args.tables_per_group))
    else:
        result.update(validate(client, args.tables_per_group))
    result["wall_seconds"] = time.monotonic() - started
    if args.source is not None:
        compare_source(result, json.loads(args.source.read_text()))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({key: value for key, value in result.items() if key != "table_ids"}))


if __name__ == "__main__":
    main()
