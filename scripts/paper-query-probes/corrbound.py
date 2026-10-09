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

"""Bounded CorrBound applicability probe against an existing GreptimeDB endpoint."""

import argparse
import json
import math
import os
import statistics
import re
import sys
import urllib.error
import urllib.request
import urllib.parse


HOSTS = 16
FACT_ROWS = 256  # 16 rows per host; 256 total.
DIM_ROWS = 64  # 4 zones per host; 64 total.
BASE = "2024-01-01 00:00:00"


def sql_quote(value):
    return "'" + value.replace("'", "''") + "'"


class Client:
    def __init__(self, url):
        self.url = url.rstrip("/") + "/v1/sql"

    def query(self, sql):
        body = urllib.parse.urlencode({"sql": sql}).encode()
        request = urllib.request.Request(
            self.url, body, {"Content-Type": "application/x-www-form-urlencoded"}, method="POST"
        )
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                data = json.loads(response.read())
        except urllib.error.HTTPError as error:
            raise RuntimeError(f"HTTP {error.code} for SQL {sql!r}: {error.read().decode(errors='replace')}") from error
        except urllib.error.URLError as error:
            raise RuntimeError(f"cannot reach {self.url}: {error}") from error
        if isinstance(data, dict) and data.get("code", 0) not in (0, "0", None):
            raise RuntimeError(f"SQL failed ({data.get('code')}): {data.get('error') or data}")
        output = data.get("output", []) if isinstance(data, dict) else []
        for item in output:
            if isinstance(item, dict) and item.get("code", 0) not in (0, "0", None):
                raise RuntimeError(f"SQL failed ({item.get('code')}): {item.get('error') or item}")
        return data


def setup(client):
    suffix = f"{os.getpid()}_{os.urandom(3).hex()}"
    fact = f"paper_corrbound_fact_{suffix}"
    dim = f"paper_corrbound_dim_{suffix}"
    client.query("CREATE DATABASE IF NOT EXISTS public")
    client.query(f"CREATE TABLE {fact} (host STRING, instance STRING, metric_value DOUBLE, ts TIMESTAMP(9), TIME INDEX (ts), PRIMARY KEY (host, instance)) ENGINE=mito WITH (append_mode='true')")
    client.query(f"CREATE TABLE {dim} (host STRING, area STRING, weight DOUBLE, ts TIMESTAMP(3), TIME INDEX (ts), PRIMARY KEY (host, area)) ENGINE=mito WITH (append_mode='true')")
    values = []
    fact_rows = []
    for i in range(FACT_ROWS):
        host, instance = i % HOSTS, i // HOSTS
        value = float(1 + (i * 37) % 499)
        fact_rows.append({"host": f"host{host:02d}", "instance": f"instance{instance:02d}",
                          "metric_value": value, "time_ms": i})
        ts = f"{BASE}.{i:03d}+00:00"
        values.append(f"({sql_quote(f'host{host:02d}')}, {sql_quote(f'instance{instance:02d}')}, {value}, {sql_quote(ts)})")
    for start in range(0, len(values), 128):
        client.query(f"INSERT INTO {fact} (host, instance, metric_value, ts) VALUES " + ",".join(values[start:start + 128]))
    values = []
    dim_rows = []
    for i in range(DIM_ROWS):
        host, zone = i // 4, i % 4
        weight = float(1 + (i * 13) % 97) / 10.0
        dim_rows.append({"host": f"host{host:02d}", "area": f"zone{zone}", "weight": weight, "time_ms": i})
        ts = f"{BASE}.{i:03d}+00:00"
        values.append(f"({sql_quote(f'host{host:02d}')}, {sql_quote(f'zone{zone}')}, {weight}, {sql_quote(ts)})")
    client.query(f"INSERT INTO {dim} (host, area, weight, ts) VALUES " + ",".join(values))
    setup_evidence = {}
    for table in (fact, dim):
        setup_evidence[table] = {
            "flush": client.query(f"ADMIN FLUSH_TABLE('{table}')"),
            "show_create": client.query(f"SHOW CREATE TABLE {table}"),
        }
    return fact, dim, fact_rows, dim_rows, setup_evidence


def make_query(time_end, zone_filter, fact, dim):
    zone = f" AND d.area = {sql_quote(zone_filter)}" if zone_filter else ""
    return (
        "SELECT f.host, d.area, AVG(f.metric_value * d.weight) AS score "
        f"FROM {fact} f JOIN {dim} d ON f.host = d.host "
        f"WHERE f.ts >= {sql_quote(BASE + '+00:00')}::TIMESTAMP "
        f"AND f.ts < {sql_quote(time_end)}::TIMESTAMP "
        f"AND d.ts >= {sql_quote(BASE + '+00:00')}::TIMESTAMP "
        f"AND d.ts < {sql_quote('2024-01-01 00:00:01+00:00')}::TIMESTAMP{zone} "
        "GROUP BY f.host, d.area ORDER BY score DESC, f.host ASC, d.area ASC LIMIT 10"
    )


def result_map(data):
    records = []
    for item in data.get("output", []) if isinstance(data, dict) else []:
        block = item.get("records") if isinstance(item, dict) else None
        if not isinstance(block, dict):
            continue
        cols = block.get("schema", {}).get("column_schemas", [])
        names = [col.get("name") for col in cols]
        for row in block.get("rows", []):
            records.append(dict(zip(names, row)) if names else row)
    return records


def equivalent_rows(left, right):
    if len(left) != len(right):
        return False
    for left_row, right_row in zip(left, right):
        if left_row.keys() != right_row.keys():
            return False
        for key in left_row:
            a, b = left_row[key], right_row[key]
            if isinstance(a, (float, int)) and isinstance(b, (float, int)):
                if not math.isclose(a, b, rel_tol=1e-12, abs_tol=1e-12):
                    return False
            elif a != b:
                return False
    return True


def plan_text(data):
    return json.dumps(data, sort_keys=True, ensure_ascii=False)


def row_stats(text):
    return re.findall(r"(?i)(?:Rows|row_count)\s*[=:]\s*(Exact\([^)]*\)|Inexact\([^)]*\)|Absent|\d+)", text)


def physical_plan_stats(data):
    plans = []
    for item in data.get("output", []) if isinstance(data, dict) else []:
        block = item.get("records") if isinstance(item, dict) else None
        if not isinstance(block, dict):
            continue
        for row in block.get("rows", []):
            if len(row) >= 2 and isinstance(row[0], str) and "physical_plan" in row[0]:
                plans.append({"label": row[0], "row_markers": row_stats(row[1]), "plan": row[1]})
    return plans


def analyzed_join_metrics(data):
    lines = []
    for item in data.get("output", []) if isinstance(data, dict) else []:
        block = item.get("records") if isinstance(item, dict) else None
        if not isinstance(block, dict):
            continue
        for row in block.get("rows", []):
            if len(row) >= 3 and isinstance(row[2], str):
                lines.extend(row[2].splitlines())
    for index, line in enumerate(lines):
        if "HashJoinExec:" in line:
            indentation = len(line) - len(line.lstrip())
            child_lines = []
            for child in lines[index + 1:]:
                child_indent = len(child) - len(child.lstrip())
                if child_indent <= indentation:
                    break
                if not child_lines or child_indent == child_lines[0][0]:
                    child_lines.append((child_indent, child.strip()))
            return {"hash_join_line": line.strip(), "direct_input_children": [child for _, child in child_lines],
                    "build_input_rows": re.search(r"build_input_rows:\s*(\d+)", line).group(1) if re.search(r"build_input_rows:\s*(\d+)", line) else None,
                    "build_mem_used": re.search(r"build_mem_used:\s*([^,]+)", line).group(1).strip() if re.search(r"build_mem_used:\s*([^,]+)", line) else None}
    return None


def oracle(fact_rows, dim_rows, end_ms, zone_filter):
    dimensions = [row for row in dim_rows if row["time_ms"] < 1000 and (zone_filter is None or row["area"] == zone_filter)]
    grouped = {}
    for fact in fact_rows:
        if fact["time_ms"] >= end_ms:
            continue
        for dim in dimensions:
            if fact["host"] != dim["host"]:
                continue
            key = (fact["host"], dim["area"])
            grouped.setdefault(key, []).append(fact["metric_value"] * dim["weight"])
    rows = [{"host": host, "area": area, "score": statistics.mean(scores)}
            for (host, area), scores in grouped.items()]
    rows.sort(key=lambda row: (-row["score"], row["host"], row["area"]))
    return rows[:10]


def run(args):
    os.makedirs(args.out, exist_ok=True)
    client = Client(args.url)
    fact, dim, fact_rows, dim_rows, setup_evidence = setup(client)
    cases = [
        ("wide_all", "2024-01-01 00:00:01+00:00", 1000, None),
        ("narrow_all", "2024-01-01 00:00:00.016+00:00", 16, None),
        ("wide_rare_zone", "2024-01-01 00:00:01+00:00", 1000, "zone0"),
    ]
    report = {
        "endpoint": args.url,
        "fixture": {"fact_rows": FACT_ROWS, "dimension_rows": DIM_ROWS, "hosts": HOSTS,
                    "fact_rows_per_host": FACT_ROWS // HOSTS, "dimension_rows_per_host": DIM_ROWS // HOSTS,
                    "synthetic": True, "claim_scope": "applicability only; not production frequency or performance"},
        "setup": setup_evidence,
        "cases": [],
    }
    for name, time_end, end_ms, zone in cases:
        sql = make_query(time_end, zone, fact, dim)
        expected = oracle(fact_rows, dim_rows, end_ms, zone)
        reverse = sql.replace(f"FROM {fact} f JOIN {dim} d", f"FROM {dim} d JOIN {fact} f")
        case = {"name": name, "time_end": time_end, "time_end_ms_from_base": end_ms, "zone_filter": zone,
                "expected_rows": expected, "orientations": []}
        outputs = []
        for orientation, query in (("canonical", sql), ("reversed", reverse)):
            explained = client.query("EXPLAIN VERBOSE " + query)
            analyzed = client.query("EXPLAIN ANALYZE VERBOSE " + query)
            result = client.query(query)
            text = plan_text(explained)
            outputs.append(result_map(result))
            case["orientations"].append({
                "name": orientation,
                "estimated_rows_markers": row_stats(text),
                "physical_plan_stats": physical_plan_stats(explained),
                "explain_verbose": explained,
                "explain_analyze_verbose": analyzed,
                "runtime_hash_join": analyzed_join_metrics(analyzed),
                "result_rows": result_map(result),
                "result_row_count": len(result_map(result)),
            })
        case["same_ordered_results"] = equivalent_rows(outputs[0], outputs[1])
        case["canonical_matches_oracle"] = equivalent_rows(outputs[0], expected)
        case["reversed_matches_oracle"] = equivalent_rows(outputs[1], expected)
        case["interpretation"] = "orientation alternative only if EXPLAIN output shows distinct physical build-child sides; plan text retained verbatim"
        report["cases"].append(case)
    path = os.path.join(args.out, "corrbound.json")
    with open(path, "w", encoding="utf-8") as output:
        json.dump(report, output, indent=2, sort_keys=True)
        output.write("\n")
    print(json.dumps({"evidence": path, "cases": [{"name": c["name"], "expected_row_count": len(c["expected_rows"]), "canonical_row_count": c["orientations"][0]["result_row_count"], "canonical_matches_oracle": c["canonical_matches_oracle"], "reversed_matches_oracle": c["reversed_matches_oracle"], "build_input_rows": [o["runtime_hash_join"]["build_input_rows"] if o["runtime_hash_join"] else None for o in c["orientations"]]} for c in report["cases"]]}, indent=2))
    if not all(case["same_ordered_results"] and case["canonical_matches_oracle"] and case["reversed_matches_oracle"] for case in report["cases"]):
        raise RuntimeError("query orientation results differ or disagree with independent fixture oracle")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True, help="GreptimeDB HTTP endpoint root")
    parser.add_argument("--out", required=True, help="directory for evidence JSON")
    args = parser.parse_args()
    try:
        run(args)
    except Exception as error:
        print(f"corrbound probe failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
