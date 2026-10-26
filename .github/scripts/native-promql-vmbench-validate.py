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

"""Correctness and observed-plan gate for canonical PromQL VMBench queries."""

import argparse
import datetime as dt
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

EXPECTED_NATIVE_IDS = {1, 2, 3, 4, 7, 10, 11, 12}


def request(url, params, native, timeout):
    data = urllib.parse.urlencode(params).encode()
    hint = "true" if native else "false"
    req = urllib.request.Request(
        url,
        data=data,
        headers={"X-Greptime-Hints": f"query.experimental_native_promql={hint}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            status, raw = response.status, response.read()
    except urllib.error.HTTPError as error:
        status, raw = error.code, error.read()
    except Exception as error:
        return {"http_status": None, "transport_error": str(error), "raw": ""}, None
    text = raw.decode("utf-8", "replace")
    try:
        body = json.loads(text)
    except (ValueError, TypeError):
        body = None
    return {"http_status": status, "raw": text, "json": body}, body


def response_error(response, kind):
    if response.get("transport_error"):
        return response["transport_error"]
    body = response.get("json")
    if response.get("http_status") != 200:
        return f"HTTP status {response.get('http_status')}"
    if not isinstance(body, dict):
        return "response body is not a JSON object"
    if kind == "prom":
        if body.get("status") != "success":
            return f"API status is not success: {body.get('status')!r}; error={body.get('error')!r}"
        data = body.get("data")
        if not isinstance(data, dict) or data.get("resultType") not in ("matrix", "vector", "scalar"):
            return "missing/invalid Prometheus data.resultType"
    else:
        for key in ("error", "err_msg", "error_msg"):
            if body.get(key):
                return f"SQL response {key}: {body[key]}"
        code = body.get("code", body.get("error_code", 0))
        if str(code).lower() not in ("0", "success", "none"):
            return f"SQL response error code: {code!r}"
        if "output" not in body and "data" not in body:
            return "SQL response has neither output nor data"
    return None


def _timestamp(value, path):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{path}: expected numeric timestamp")
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"{path}: expected finite numeric timestamp")
    try:
        return Decimal(str(value))
    except InvalidOperation as error:
        raise ValueError(f"{path}: invalid numeric timestamp") from error


def _labels(metric, path):
    if not isinstance(metric, dict):
        raise ValueError(f"{path}: expected labels object")
    for key, value in metric.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise ValueError(f"{path}: label names and values must be strings")
    return json.dumps(metric, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _point(point, path):
    if not isinstance(point, list) or len(point) != 2:
        raise ValueError(f"{path}: expected [timestamp, value]")
    timestamp = _timestamp(point[0], path + "[0]")
    if not isinstance(point[1], str):
        raise ValueError(f"{path}[1]: expected Prometheus numeric string")
    try:
        number = float(point[1])
    except ValueError as error:
        raise ValueError(f"{path}[1]: malformed number {point[1]!r}") from error
    return timestamp, number


def normalize_prom(body):
    if not isinstance(body, dict) or not isinstance(body.get("data"), dict):
        raise ValueError("data: expected object")
    data = body["data"]
    kind, result = data.get("resultType"), data.get("result")
    if kind == "matrix":
        if not isinstance(result, list):
            raise ValueError("data.result: expected matrix list")
        series = []
        for i, row in enumerate(result):
            if not isinstance(row, dict) or "values" not in row:
                raise ValueError(f"data.result[{i}]: malformed matrix series")
            labels = _labels(row.get("metric"), f"data.result[{i}].metric")
            if not isinstance(row["values"], list):
                raise ValueError(f"data.result[{i}].values: expected list")
            points = [
                _point(point, f"data.result[{i}].values[{j}]")
                for j, point in enumerate(row["values"])
            ]
            if not points:
                raise ValueError(f"data.result[{i}].values: empty series")
            series.append((labels, points))
        return kind, sorted(series, key=lambda item: (item[0], repr(item[1])))
    if kind == "vector":
        if not isinstance(result, list):
            raise ValueError("data.result: expected vector list")
        series = []
        for i, row in enumerate(result):
            if not isinstance(row, dict) or "value" not in row:
                raise ValueError(f"data.result[{i}]: malformed vector sample")
            labels = _labels(row.get("metric"), f"data.result[{i}].metric")
            series.append((labels, [_point(row["value"], f"data.result[{i}].value")]))
        return kind, sorted(series, key=lambda item: (item[0], repr(item[1])))
    if kind == "scalar":
        return kind, _point(result, "data.result")
    raise ValueError(f"unsupported resultType {kind!r}")


def _number_equal(a, b, rtol, atol):
    if math.isnan(a) or math.isnan(b):
        return math.isnan(a) and math.isnan(b)
    if math.isinf(a) or math.isinf(b):
        return a == b
    return math.isclose(a, b, rel_tol=rtol, abs_tol=atol)


def _compare_point(left, right, path, rtol, atol):
    if left[0] != right[0]:
        return False, f"{path}[0] timestamp: {left[0]} != {right[0]}"
    if not _number_equal(left[1], right[1], rtol, atol):
        return False, f"{path}[1] value: {left[1]!r} != {right[1]!r}"
    return True, None


def compare_prom(legacy, native, rtol, atol):
    try:
        left, right = normalize_prom(legacy), normalize_prom(native)
    except (ValueError, KeyError, TypeError) as error:
        return False, str(error)
    if left[0] != right[0]:
        return False, f"data.resultType: {left[0]!r} != {right[0]!r}"
    if left[0] == "scalar":
        return _compare_point(left[1], right[1], "data.result", rtol, atol)
    if len(left[1]) != len(right[1]):
        return False, f"data.result series count: {len(left[1])} != {len(right[1])}"
    for i, (a, b) in enumerate(zip(left[1], right[1])):
        if a[0] != b[0]:
            return False, f"data.result[{i}].metric: {a[0]} != {b[0]}"
        if len(a[1]) != len(b[1]):
            return False, f"data.result[{i}].values count: {len(a[1])} != {len(b[1])}"
        for j, (point_a, point_b) in enumerate(zip(a[1], b[1])):
            equal, detail = _compare_point(
                point_a, point_b, f"data.result[{i}].values[{j}]", rtol, atol
            )
            if not equal:
                return equal, detail
    return True, None


def plan_text(body):
    """Read plan-bearing strings only from SQL response output records."""
    output = body.get("output") if isinstance(body, dict) else None
    cells = []
    if not isinstance(output, list):
        return ""
    for record in output:
        if not isinstance(record, dict):
            continue
        records = record.get("records")
        rows = records.get("rows") if isinstance(records, dict) else None
        if not isinstance(rows, list):
            continue
        for row in rows:
            if isinstance(row, list):
                cells.extend(cell for cell in row if isinstance(cell, str))
    return "\n".join(dict.fromkeys(cell for cell in cells if cell.strip()))


def inspect_plan(text):
    exec_lines = [
        line for line in text.splitlines()
        if re.search(r"\b[A-Za-z][A-Za-z0-9]*Exec\b", line)
    ]
    recognized = bool(exec_lines)
    fused = any("FusedSeriesExec" in line for line in exec_lines)
    finalize = any("AggregateFinalizeExec" in line for line in exec_lines)
    scan_lines = [
        line for line in text.splitlines()
        if re.search(r"\b(?:SeriesScanExec|SeriesScan|SeqScan):", line)
    ]
    scan_counts = []
    scan_evidence = []
    for line in scan_lines:
        match = re.search(r"(?:SeriesScanExec|SeriesScan|SeqScan):[^\n]*?partitions=(\d+)", line)
        if match is None:
            match = re.search(r'"partition_count":\{"count":(\d+),', line)
        count = int(match.group(1)) if match else None
        scan_counts.append(count)
        scan_evidence.append({"line": line, "partition_count": count})
    partitions = scan_counts[0] if len(scan_counts) == 1 else None
    return recognized, fused, finalize, partitions, scan_evidence, len(scan_lines)


def plan_qualification_errors(index, route, native_plan, expected_partitions):
    errors = []
    if route is None:
        errors.append("native plan route is unknown")
    if not native_plan[0]:
        errors.append("native SQL response has no recognized physical plan")
    if route == "native":
        if native_plan[5] != 1:
            errors.append("native plan must contain exactly one SeriesScan or SeqScan node")
        if native_plan[3] is None:
            errors.append("native scan partition count is unknown")
        elif expected_partitions is not None and native_plan[3] != expected_partitions:
            errors.append(
                f"native scan partition count {native_plan[3]} != expected {expected_partitions}"
            )
        if index in {1, 2, 3, 4, 10, 11, 12} and not native_plan[2]:
            errors.append("native aggregate plan is missing AggregateFinalizeExec")
    return errors


def epoch(value):
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("RFC3339 timestamp must include timezone")
    return parsed.timestamp()


def validate_args(args, parser):
    if not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error("--timeout must be finite and positive")
    if args.expected_partitions is not None and args.expected_partitions <= 0:
        parser.error("--expected-partitions must be positive")
    for name, value in (("--rtol", args.rtol), ("--atol", args.atol)):
        if not math.isfinite(value) or value < 0:
            parser.error(f"{name} must be finite and nonnegative")
    try:
        start, end = epoch(args.start), epoch(args.end)
    except (ValueError, OverflowError) as error:
        parser.error(f"invalid RFC3339 start/end: {error}")
    if start > end:
        parser.error("--start must not be after --end")
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)(ms|s|m|h|d|w|y)", args.step)
    if not match or float(match.group(1)) <= 0:
        parser.error("--step must be a positive duration such as 30s")
    return start, end


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--query-file", required=True)
    parser.add_argument("--url", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--step", default="30s")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--expected-partitions", type=int, default=4)
    parser.add_argument("--rtol", type=float, default=1e-10)
    parser.add_argument("--atol", type=float, default=1e-12)
    args = parser.parse_args()
    start, end = validate_args(args, parser)
    output_dir = Path(args.output_dir)
    if output_dir.exists() and any(output_dir.iterdir()):
        parser.error("output-dir must be fresh/empty")
    output_dir.mkdir(parents=True, exist_ok=True)
    query_path = Path(args.query_file)
    queries = [line.strip() for line in query_path.read_text().splitlines() if line.strip()]
    if len(queries) != 12:
        parser.error(f"query file must contain exactly 12 nonblank queries, got {len(queries)}")

    records = []
    for index, query in enumerate(queries, 1):
        query_id = f"lite-{index:02d}"
        directory = output_dir / query_id
        directory.mkdir()
        record = {
            "query_id": query_id,
            "query": query,
            "query_sha256": hashlib.sha256(query.encode()).hexdigest(),
            "route": None,
            "correctness": False,
            "error": None,
            "qualified": False,
            "qualification_error": None,
            "request_errors": [],
            "labels_series_point_counts": {},
            "has_fused": False,
            "has_finalize": False,
            "observed_partitions": None,
            "scan_evidence": {},
            "scan_node_count": {},
            "parallel_scan": False,
            "full_plan_paths": {},
            "expected_route_forecast": "native" if index in EXPECTED_NATIVE_IDS else "fallback",
            "expected_vs_actual_route": None,
            "eligible_for_native_statistics": False,
        }
        prom_bodies = {}
        prom_errors = []
        for route, native in (("legacy", False), ("native", True)):
            response, body = request(
                args.url,
                {"query": query, "start": args.start, "end": args.end, "step": args.step},
                native,
                args.timeout,
            )
            (directory / f"{route}-prometheus.json").write_text(
                json.dumps(response, indent=2, ensure_ascii=False) + "\n"
            )
            prom_bodies[route] = body
            error = response_error(response, "prom")
            if error:
                detail = f"{route} Prometheus: {error}"
                prom_errors.append(detail)
                record["request_errors"].append(detail)

        plans = {}
        for route, native in (("legacy", False), ("native", True)):
            sql = f"TQL ANALYZE VERBOSE ({start:.6f}, {end:.6f}, '{args.step}') {query}"
            url = urllib.parse.urlsplit(args.url)
            sql_url = urllib.parse.urlunsplit((url.scheme, url.netloc, "/v1/sql", "", ""))
            response, body = request(
                sql_url, {"sql": sql, "format": "greptimedb_v1"}, native, args.timeout
            )
            (directory / f"{route}-sql.json").write_text(
                json.dumps(response, indent=2, ensure_ascii=False) + "\n"
            )
            error = response_error(response, "sql")
            text = plan_text(body)
            plan_path = directory / f"{route}-plan.txt"
            plan_path.write_text(text + "\n")
            record["full_plan_paths"][route] = str(plan_path.relative_to(output_dir))
            plans[route] = text
            parsed_plan = inspect_plan(text)
            record["scan_evidence"][route] = parsed_plan[4]
            record["scan_node_count"][route] = parsed_plan[5]
            if error:
                record["request_errors"].append(f"{route} TQL ANALYZE: {error}")
            elif not inspect_plan(text)[0]:
                record["request_errors"].append(
                    f"{route} TQL ANALYZE: no recognized physical Exec plan in output records"
                )

        native_plan = inspect_plan(plans["native"])
        record["has_fused"] = native_plan[1]
        record["has_finalize"] = native_plan[2]
        record["observed_partitions"] = native_plan[3]
        record["scan_evidence"]["native"] = native_plan[4]
        record["scan_node_count"]["native"] = native_plan[5]
        if native_plan[0]:
            record["route"] = "native" if record["has_fused"] else "fallback"
        record["expected_vs_actual_route"] = {
            "expected": record["expected_route_forecast"],
            "actual": record["route"],
        }

        valid_results = {}
        for route, body in prom_bodies.items():
            response = {"http_status": 200 if body is not None else None, "json": body}
            error = response_error(response, "prom")
            if error:
                continue
            try:
                kind, normalized = normalize_prom(body)
                count = len(normalized) if kind != "scalar" else 1
                points = (
                    sum(len(series[1]) for series in normalized)
                    if kind != "scalar"
                    else 1
                )
                if count == 0 or points == 0:
                    raise ValueError("data.result: expected nonempty result")
                record["labels_series_point_counts"][route] = {
                    "series": count,
                    "points": points,
                }
                valid_results[route] = body
            except (ValueError, KeyError, TypeError) as error:
                record["request_errors"].append(f"{route} Prometheus result: {error}")

        if len(valid_results) == 2:
            record["correctness"], record["error"] = compare_prom(
                valid_results["legacy"], valid_results["native"], args.rtol, args.atol
            )
        if prom_errors:
            record["correctness"] = False
            record["error"] = "; ".join(prom_errors)
        elif len(valid_results) != 2:
            record["correctness"] = False
            record["error"] = "both routes must return valid nonempty results"

        qualification_errors = plan_qualification_errors(
            index, record["route"], native_plan, args.expected_partitions
        )
        record["parallel_scan"] = (
            record["route"] == "native"
            and native_plan[5] == 1
            and native_plan[3] is not None
            and native_plan[3] > 1
        )
        if record["request_errors"]:
            qualification_errors.extend(record["request_errors"])
        record["qualified"] = record["correctness"] and not qualification_errors
        record["qualification_error"] = "; ".join(qualification_errors) or None
        record["eligible_for_native_statistics"] = (
            record["qualified"] and record["route"] == "native"
        )
        records.append(record)

    report = {
        "schema_version": 1,
        "url": args.url,
        "start": args.start,
        "end": args.end,
        "step": args.step,
        "timeout": args.timeout,
        "expected_partitions": args.expected_partitions,
        "rtol": args.rtol,
        "atol": args.atol,
        "query_file_sha256": hashlib.sha256(query_path.read_bytes()).hexdigest(),
        "records": records,
    }
    (output_dir / "qualification.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=False) + "\n"
    )
    table = ["| Query | Route | Correctness | Plan |", "|---|---|---|---|"]
    for record in records:
        state = "PASS" if record["correctness"] else f"FAIL: {record['error']}"
        table.append(
            f"| {record['query_id']} | {record['route']} | {state} "
            f"(qualified: {record['qualified']}) | "
            f"`{record['full_plan_paths'].get('native', '')}` |"
        )
    (output_dir / "qualification.md").write_text("\n".join(table) + "\n")
    return 1 if any(not record["qualified"] for record in records) else 0


if __name__ == "__main__":
    sys.exit(main())
