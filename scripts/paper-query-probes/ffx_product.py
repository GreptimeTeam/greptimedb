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

"""Untimed product correctness and EXPLAIN ANALYZE evidence for weightedAVG.

Requires Python 3.11 or newer for the standard-library TOML parser.
"""

import argparse
import json
import math
import os
import secrets
import sys
import tomllib
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


def request(url, sql):
    endpoint = url.rstrip("/") + "/v1/sql?db=public"
    body = urllib.parse.urlencode({"sql": sql}).encode()
    req = urllib.request.Request(endpoint, data=body,
                                 headers={"Content-Type": "application/x-www-form-urlencoded"})
    try:
        with urllib.request.urlopen(req, timeout=120) as response:
            if response.status != 200:
                raise RuntimeError(f"HTTP {response.status}: {response.read().decode(errors='replace')}")
            payload = json.loads(response.read())
    except urllib.error.HTTPError as error:
        raise RuntimeError(f"HTTP {error.code} for SQL {sql}: {error.read().decode(errors='replace')}") from error
    if payload.get("code", 0) != 0:
        raise RuntimeError(f"SQL failed: {sql}\n{payload}")
    output = payload.get("output")
    if not isinstance(output, list) or len(output) != 1 or not isinstance(output[0], dict):
        raise RuntimeError(f"Unexpected SQL response envelope: {payload}")
    if output[0].get("code", 0) != 0 or output[0].get("error"):
        raise RuntimeError(f"SQL item failed: {sql}\n{output[0]}")
    if "affectedrows" in output[0]:
        return payload
    records = output[0].get("records")
    if not isinstance(records, dict) or not isinstance(records.get("schema"), dict):
        raise RuntimeError(f"Missing records/schema in SQL response: {payload}")
    return payload


def result(payload):
    records = payload["output"][0]["records"]
    columns = records["schema"].get("column_schemas")
    rows = records.get("rows")
    if not isinstance(columns, list) or not isinstance(rows, list):
        raise RuntimeError(f"Malformed SQL records: {records}")
    names = [column.get("name") for column in columns]
    if any(not isinstance(name, str) for name in names) or len(names) != len(set(names)):
        raise RuntimeError(f"Missing or duplicate result columns: {names}")
    if any(not isinstance(row, list) or len(row) != len(names) for row in rows):
        raise RuntimeError(f"Malformed result rows for columns {names}")
    total_rows = records.get("total_rows")
    if total_rows is not None and total_rows != len(rows):
        raise RuntimeError(f"Incomplete result: total_rows={total_rows}, returned={len(rows)}")
    return names, rows


def close(a, b):
    if a is None or b is None:
        return a is b
    try:
        a, b = float(a), float(b)
    except (TypeError, ValueError):
        return False
    return math.isfinite(a) and math.isfinite(b) and math.isclose(a, b, rel_tol=1e-12, abs_tol=1e-12)


def compare_full(raw_payload, factor_payload, oracle, label):
    names, rows = result(raw_payload)
    factor_names, factor_rows = result(factor_payload)
    required = ("host", "area", "pairs", "valid", "total", "score")
    maps = []
    for columns, data in ((names, rows), (factor_names, factor_rows)):
        indexes = {column.lower(): i for i, column in enumerate(columns)}
        if any(column not in indexes for column in required):
            raise RuntimeError(f"{label}: required full-result columns absent: {columns}")
        entries = {}
        previous = None
        for row in data:
            key = (row[indexes["host"]], row[indexes["area"]])
            order_key = tuple((value is None, "" if value is None else value) for value in key)
            if key in entries:
                raise AssertionError(f"{label}: duplicate group key {key}")
            if previous is not None and order_key < previous:
                raise AssertionError(f"{label}: group output not canonical: {previous}, {order_key}")
            previous = order_key
            entries[key] = tuple(row[indexes[name]] for name in required[2:])
        maps.append(entries)
    expected = oracle
    if expected is not None:
        for flavor, actual in zip(("raw", "factor"), maps):
            if actual.keys() != expected.keys():
                raise AssertionError(f"{label}: {flavor} group keys differ from oracle")
            for key, wanted in expected.items():
                got = actual[key]
                if got[:2] != wanted[:2] or not close(got[2], wanted[2]) or not close(got[3], wanted[3]):
                    raise AssertionError(f"{label} {flavor} {key}: {got} != oracle {wanted}")
    if maps[0].keys() != maps[1].keys():
        raise AssertionError(f"{label}: raw/factor group keys differ")
    if expected is None:
        for key in maps[0]:
            left, right = maps[0][key], maps[1][key]
            if left[:2] != right[:2] or not close(left[2], right[2]) or not close(left[3], right[3]):
                raise AssertionError(f"{label} raw/factor {key}: {left} != {right}")
    oracle_rows = None if expected is None else [
        {"host": key[0], "area": key[1], "pairs": value[0], "valid": value[1],
         "total": value[2], "score": value[3]} for key, value in expected.items()]
    return {"name": label, "comparison": "passed", "oracle": oracle_rows,
            "raw": raw_payload, "factor": factor_payload}


def corner_rows():
    # (host, instance, metric, timestamp-seconds); all rows are explicit oracle inputs.
    facts = [("h1", "i1", -2.5, 0), ("h1", "i2", 1.25, 1),
             ("h1", "i3", -2.5, 2), ("h2", "i4", None, 3),
             ("h3", "i5", 4.0, 4), (None, "i6", 3.0, 5)]
    dims = [("h1", "a", 0.5, 0), ("h1", "a", 2.0, 1),
            ("h1", "a", 0.5, 2), ("h2", "a", None, 3),
            ("h1", None, -1.25, 4), ("h4", "a", 3.0, 5),
            (None, "a", 2.0, 6)]
    return facts, dims


def oracle(facts, dims, predicate):
    groups = {}
    for host, _, value, ftime in facts:
        for dhost, area, weight, dtime in dims:
            if host is None or host != dhost or not predicate(host, area, ftime, dtime):
                continue
            entry = groups.setdefault((host, area), [0, 0, 0.0])
            entry[0] += 1
            if value is not None and weight is not None:
                entry[1] += 1
                entry[2] += value * weight
    return {key: (pairs, valid, total if valid else None,
                  total / valid if valid else None)
            for key, (pairs, valid, total) in groups.items()}


def insert_sql(table, rows, columns):
    def val(value):
        if value is None:
            return "NULL"
        if isinstance(value, str):
            return "'" + value.replace("'", "''") + "'"
        return str(value)
    values = ",".join("(" + ",".join(val(item) for item in row) + ")" for row in rows)
    return f"INSERT INTO {table} ({','.join(columns)}) VALUES {values}"


def run_corner(url):
    suffix = f"{os.getpid()}_{secrets.token_hex(5)}"
    fact_table, dim_table = f"ffx_probe_fact_{suffix}", f"ffx_probe_dim_{suffix}"
    facts, dims = corner_rows()
    fact_columns = ("host", "instance", "metric_value", "ts")
    dim_columns = ("host", "area", "weight", "ts")
    created = []
    try:
        request(url, f"CREATE TABLE {fact_table} (host STRING, instance STRING, metric_value DOUBLE, ts TIMESTAMP(9) TIME INDEX, PRIMARY KEY(host, instance)) ENGINE=mito WITH (append_mode = 'true')")
        created.append(fact_table)
        request(url, f"CREATE TABLE {dim_table} (host STRING, area STRING, weight DOUBLE, ts TIMESTAMP(9) TIME INDEX, PRIMARY KEY(host, area)) ENGINE=mito WITH (append_mode = 'true')")
        created.append(dim_table)
        def timestamp(seconds):
            return f"2024-01-01 00:00:{seconds:02}"
        request(url, insert_sql(fact_table, [(h, i, v, timestamp(t)) for h, i, v, t in facts], fact_columns))
        request(url, insert_sql(dim_table, [(h, a, w, timestamp(t)) for h, a, w, t in dims], dim_columns))
        tests = {
            "wide": (lambda h, a, ft, dt: True, "f.ts >= TIMESTAMP '2024-01-01 00:00:00'", "d.ts >= TIMESTAMP '2024-01-01 00:00:00'"),
            "narrow": (lambda h, a, ft, dt: ft >= 1, "f.ts >= TIMESTAMP '2024-01-01 00:00:01'", "d.ts >= TIMESTAMP '2024-01-01 00:00:00'"),
            "rare": (lambda h, a, ft, dt: a == "a", "f.ts >= TIMESTAMP '2024-01-01 00:00:00'", "d.ts >= TIMESTAMP '2024-01-01 00:00:00' AND d.area = 'a'"),
            "empty": (lambda h, a, ft, dt: False, "f.ts >= TIMESTAMP '2030-01-01 00:00:00'", "d.ts >= TIMESTAMP '2030-01-01 00:00:00'"),
        }
        evidence = []
        top10s = []
        for label, (accept, fp, dp) in tests.items():
            where = f"{fp} AND {dp}"
            raw_sql = (f"SELECT f.host,d.area,COUNT(*) AS pairs,COUNT(f.metric_value*d.weight) AS valid,"
                       f"SUM(f.metric_value*d.weight) AS total,AVG(f.metric_value*d.weight) AS score "
                       f"FROM {fact_table} f JOIN {dim_table} d ON f.host=d.host WHERE {where} "
                       "GROUP BY f.host,d.area ORDER BY f.host NULLS LAST,d.area NULLS LAST")
            fact_pred, dim_pred = fp.replace("f.ts", "ts"), dp.replace("d.ts", "ts").replace("d.area", "area")
            factor_sql = (f"WITH ff AS (SELECT host,SUM(metric_value) AS fm,COUNT(metric_value) AS fn,COUNT(*) AS fc "
                          f"FROM {fact_table} WHERE {fact_pred} GROUP BY host), "
                          f"dd AS (SELECT host,area,SUM(weight) AS dw,COUNT(weight) AS dn,COUNT(*) AS dc "
                          f"FROM {dim_table} WHERE {dim_pred} GROUP BY host,area) "
                          "SELECT ff.host,dd.area,ff.fc*dd.dc AS pairs,ff.fn*dd.dn AS valid,ff.fm*dd.dw AS total,"
                          "CASE WHEN ff.fn=0 OR dd.dn=0 THEN NULL ELSE ff.fm*dd.dw/"
                          "(CAST(ff.fn AS DOUBLE)*CAST(dd.dn AS DOUBLE)) END AS score "
                          "FROM ff JOIN dd ON ff.host=dd.host ORDER BY ff.host NULLS LAST,dd.area NULLS LAST")
            raw_payload, factor_payload = request(url, raw_sql), request(url, factor_sql)
            full_oracle = oracle(facts, dims, accept)
            evidence.append(compare_full(raw_payload, factor_payload, full_oracle, label))
            top_raw = raw_sql.replace("ORDER BY f.host NULLS LAST,d.area NULLS LAST", "ORDER BY score DESC NULLS LAST,f.host NULLS LAST,d.area NULLS LAST") + " LIMIT 10"
            top_factor = factor_sql.replace("ORDER BY ff.host NULLS LAST,dd.area NULLS LAST", "ORDER BY score DESC NULLS LAST,ff.host NULLS LAST,dd.area NULLS LAST") + " LIMIT 10"
            top10s.append(compare_top(request(url, top_raw), request(url, top_factor), f"corner/{label}", list(full_oracle.items())))
        wide_raw = (f"EXPLAIN ANALYZE VERBOSE SELECT f.host,d.area,AVG(f.metric_value*d.weight) "
                    f"FROM {fact_table} f JOIN {dim_table} d ON f.host=d.host "
                    "WHERE f.ts >= TIMESTAMP '2024-01-01 00:00:00' "
                    "AND f.ts < TIMESTAMP '2024-01-01 00:00:06' "
                    "AND d.ts >= TIMESTAMP '2024-01-01 00:00:00' "
                    "AND d.ts < TIMESTAMP '2024-01-01 00:00:07' GROUP BY f.host,d.area")
        wide_factor = (f"EXPLAIN ANALYZE VERBOSE WITH ff AS (SELECT host,SUM(metric_value) fm,COUNT(metric_value) fn "
                       f"FROM {fact_table} WHERE ts >= TIMESTAMP '2024-01-01 00:00:00' "
                       "AND ts < TIMESTAMP '2024-01-01 00:00:06' GROUP BY host), "
                       f"dd AS (SELECT host,area,SUM(weight) dw,COUNT(weight) dn FROM {dim_table} "
                       "WHERE ts >= TIMESTAMP '2024-01-01 00:00:00' AND ts < TIMESTAMP '2024-01-01 00:00:07' "
                       "GROUP BY host,area) SELECT ff.host,dd.area,ff.fm*dd.dw/"
                       "(CAST(ff.fn AS DOUBLE)*CAST(dd.dn AS DOUBLE)) FROM ff JOIN dd ON ff.host=dd.host")
        explain = {"raw": request(url, wide_raw), "factor": request(url, wide_factor)}
        for flavor, payload in explain.items():
            names, _ = result(payload)
            if "plan" not in [name.lower() for name in names]:
                raise RuntimeError(f"EXPLAIN ANALYZE {flavor} returned no plan column: {names}")
        return {"checks": evidence, "top10": top10s, "explain_analyze": explain, "tables": [fact_table, dim_table]}
    finally:
        for table in reversed(created):
            request(url, f"DROP TABLE {table}")


def load_case_queries(case_root):
    cases = []
    for path in sorted(case_root.glob("paper_ffx_*/case.toml")):
        case = tomllib.loads(path.read_text())
        queries = {q["name"]: q["query"] for q in case["scenario"]["queries"]}
        if len(queries) != len(case["scenario"]["queries"]):
            raise RuntimeError(f"Duplicate query name in {path}")
        cases.append((case["case"]["name"], queries))
    if not cases:
        raise RuntimeError(f"No paper_ffx cases found under {case_root}")
    return cases

def run_existing(url, case_root):
    evidence = []
    for case_name, queries in load_case_queries(case_root):
        filters = sorted(name[len("raw_full_"):] for name in queries if name.startswith("raw_full_"))
        if not filters:
            raise RuntimeError(f"{case_name}: no raw_full_* queries found")
        case_evidence = {"case": case_name, "checks": []}
        for filt in filters:
            required = (f"factor_full_{filt}", f"raw_{filt}", f"factor_{filt}")
            missing = [name for name in required if name not in queries]
            if missing:
                raise RuntimeError(f"{case_name}/{filt}: missing paired queries {missing}")
            raw_full, factor_full = request(url, queries[f"raw_full_{filt}"]), request(url, queries[f"factor_full_{filt}"])
            check = compare_full(raw_full, factor_full, None, f"{case_name}/{filt}")
            names, rows = result(raw_full)
            indexes = {name.lower(): i for i, name in enumerate(names)}
            groups = {}
            for row in rows:
                key = (row[indexes["host"]], row[indexes["area"]])
                if key in groups:
                    raise AssertionError(f"{case_name}/{filt}: duplicate full-result key {key}")
                groups[key] = (row[indexes["pairs"]], row[indexes["valid"]],
                               row[indexes["total"]], row[indexes["score"]])
            check["top10"] = compare_top(request(url, queries[f"raw_{filt}"]),
                                          request(url, queries[f"factor_{filt}"]),
                                          f"{case_name}/{filt}", list(groups.items()))
            case_evidence["checks"].append(check)
        case_evidence["explain_analyze"] = {
            name: request(url, queries[name])
            for name in ("diagnostic_explain_raw_wide", "diagnostic_explain_factor_wide")
        }
        for flavor, payload in case_evidence["explain_analyze"].items():
            names, _ = result(payload)
            if "plan" not in [name.lower() for name in names]:
                raise RuntimeError(f"EXPLAIN ANALYZE {flavor} returned no plan column: {names}")
        evidence.append(case_evidence)
    return evidence

def compare_top(raw, factor, label, expected_rows):
    def fields(payload):
        names, rows = result(payload)
        indexes = {name.lower(): i for i, name in enumerate(names)}
        if not all(name in indexes for name in ("host", "area", "score")):
            raise RuntimeError(f"{label}: invalid top schema {names}")
        return [(row[indexes["host"]], row[indexes["area"]], row[indexes["score"]]) for row in rows]

    left, right = fields(raw), fields(factor)
    expected = sorted(expected_rows, key=lambda item: (
        item[1][3] is None, -(float(item[1][3]) if item[1][3] is not None else 0.0),
        item[0][0] is None, item[0][0] or "", item[0][1] is None, item[0][1] or ""))[:10]
    wanted = [(key[0], key[1], values[3]) for key, values in expected]
    if len(left) != len(wanted) or len(right) != len(wanted):
        raise AssertionError(f"{label}: expected {len(wanted)} top rows, got raw={len(left)} factor={len(right)}")
    for flavor, actual in (("raw", left), ("factor", right)):
        for index, (got, want) in enumerate(zip(actual, wanted)):
            if got[:2] != want[:2] or not close(got[2], want[2]):
                raise AssertionError(f"{label}: {flavor} top row {index} {got} != expected {want}")
            if index and got[2] is not None and actual[index - 1][2] is not None:
                previous = actual[index - 1]
                if float(got[2]) > float(previous[2]):
                    raise AssertionError(f"{label}: {flavor} scores are not descending")
                if float(got[2]) == float(previous[2]) and got[:2] < previous[:2]:
                    raise AssertionError(f"{label}: {flavor} tie keys are not ascending")
    return {"comparison": "passed", "expected_rows": wanted, "raw": raw, "factor": factor}

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True, help="GreptimeDB HTTP base URL")
    parser.add_argument("--out", required=True, type=Path, help="JSON evidence output file")
    parser.add_argument("--existing-cases", action="store_true", help="probe already-seeded direct-SST tables")
    parser.add_argument("--case-root", type=Path, default=Path("tests/perf/query_cases"))
    args = parser.parse_args()
    evidence = {"endpoint": args.url, "timing": False}
    if args.existing_cases:
        evidence["checks"] = run_existing(args.url, args.case_root)
    else:
        evidence.update(run_corner(args.url))
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(evidence, indent=2, allow_nan=False) + "\n")
    print(f"Wrote correctness/EXPLAIN evidence to {args.out}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"probe failed: {exc}", file=sys.stderr)
        raise
