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

"""Compares GreptimeDB's answers with the pinned Prometheus engine's, case by case.

The Prometheus side comes from `oracle`, which evaluates the same case file with the real engine.
This script loads the case's series into a running GreptimeDB over SQL, asks the same queries over
the Prometheus-compatible HTTP API, normalizes both answers the same way, and reports every
difference with its labels, timestamp and value.

Normalization is limited to things that carry no meaning: label order, series order, and float
formatting. It never drops the metric name, never treats NaN as a missing point, and never turns a
missing series into an empty one - the three ways a comparison can be passed without the two engines
agreeing.
"""

import argparse
import json
import math
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

# A float this close is the same number written twice: both sides return float64, and `rate` divides
# in a different order on each.
RELATIVE_TOLERANCE = 1e-9

# The request hint that turns the native PromQL route on for one statement.
NATIVE_HINT = "query.experimental_native_promql"


def sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def json_number(value) -> str:
    """A sample value as a SQL literal that keeps NaN and the infinities."""
    if isinstance(value, str):
        return {
            "NaN": "'NaN'::DOUBLE",
            "+Inf": "'+Inf'::DOUBLE",
            "-Inf": "'-Inf'::DOUBLE",
        }.get(value, value)
    return repr(float(value))


def post_sql(sql_url: str, statement: str, native: bool) -> dict:
    # `/v1/sql` accepts only form-encoded statements.
    body = urllib.parse.urlencode({"sql": statement}).encode()
    request = urllib.request.Request(sql_url, data=body, method="POST")
    request.add_header("Content-Type", "application/x-www-form-urlencoded")
    if native:
        request.add_header("x-greptime-hints", f"{NATIVE_HINT}=true")
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            payload = json.load(response)
    except urllib.error.HTTPError as error:
        raise SystemExit(
            f"GreptimeDB refused {statement!r}: HTTP {error.code} {response_error(error)}"
        ) from error
    except urllib.error.URLError as error:
        raise SystemExit(f"cannot reach GreptimeDB at {sql_url}: {error}") from error
    if payload.get("error"):
        raise SystemExit(f"sql failed: {payload['error']}\n  statement: {statement}")
    return payload


def create_table_and_load(sql_url: str, case: dict, native: bool) -> list[str]:
    """Creates one table per metric name and inserts the case's samples.

    A table is named exactly after its metric: a PromQL selector reads the table name as the metric
    name, so a prefixed table could not answer the case's own query. Each table is dropped first, so
    cases can share an instance without one case's data answering another's query.
    """
    by_metric: dict[str, list[dict]] = {}
    for series in case["series"]:
        metric = series["labels"].get("__name__")
        if not metric:
            raise SystemExit(f"case {case['name']!r}: every series needs a __name__ label")
        by_metric.setdefault(metric, []).append(series)

    label_columns = sorted(
        {name for series in case["series"] for name in series["labels"] if name != "__name__"}
    )
    created = []
    for metric, series_list in by_metric.items():
        # A label is a tag in GreptimeDB only when it is part of the primary key. Without this the
        # string columns become value fields, and a PromQL aggregation then tries to sum a string.
        column_defs = "".join(f", {name} STRING" for name in label_columns)
        primary_key = f", PRIMARY KEY({', '.join(label_columns)})" if label_columns else ""
        statements = [
            f"DROP TABLE IF EXISTS {metric}",
            f"CREATE TABLE {metric} (ts TIMESTAMP TIME INDEX{column_defs}, val DOUBLE"
            f"{primary_key})",
        ]

        values = []
        for series in series_list:
            labels = series["labels"]
            for point in series["samples"]:
                label_values = "".join(
                    ", " + (sql_quote(labels[name]) if name in labels else "NULL")
                    for name in label_columns
                )
                values.append(f"({point['t']}{label_values}, {json_number(point['v'])})")
        if values:
            columns = ", ".join(["ts", *label_columns, "val"])
            statements.append(f"INSERT INTO {metric} ({columns}) VALUES " + ", ".join(values))

        for statement in statements:
            post_sql(sql_url, statement, native)
        created.append(metric)
    return created


def query_prometheus(api_url: str, query: dict, native: bool) -> dict:
    params = {"query": query["expr"]}
    if query["kind"] == "instant":
        params["time"] = f"{query['time'] / 1000:.3f}"
        path = "/v1/prometheus/api/v1/query"
    else:
        params["start"] = f"{query['start'] / 1000:.3f}"
        params["end"] = f"{query['end'] / 1000:.3f}"
        params["step"] = f"{query['step'] / 1000:.3f}"
        path = "/v1/prometheus/api/v1/query_range"

    request = urllib.request.Request(f"{api_url}{path}?{urllib.parse.urlencode(params)}")
    if native:
        request.add_header("x-greptime-hints", f"{NATIVE_HINT}=true")
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        return {"status": "error", "error": response_error(error)}
    except urllib.error.URLError as error:
        return {"status": "error", "error": f"cannot reach {api_url}: {error}"}


def response_error(error) -> str:
    try:
        return json.load(error).get("error", str(error))
    except Exception:
        return str(error)


def millis(seconds) -> int:
    return int(round(float(seconds) * 1000))


def sort_series(series: list[dict]) -> list[dict]:
    for item in series:
        item["points"].sort(key=lambda p: p["t"])
        item["labels"] = dict(sorted(item["labels"].items()))
    return sorted(series, key=lambda item: json.dumps(item["labels"], sort_keys=True))


def normalize_greptime(payload: dict, kind: str) -> dict:
    """GreptimeDB's answer in the oracle's shape."""
    if payload.get("status") != "success":
        return {"error": payload.get("error", "unknown error")}

    data = payload["data"]
    result_type = data.get("resultType")
    result = data.get("result")

    if result_type == "scalar":
        return {"type": "scalar", "scalar": float(result[1])}

    series = []
    if isinstance(result, list):
        for item in result:
            labels = {k: v for k, v in (item.get("metric") or {}).items() if v != ""}
            if kind == "instant":
                points = [{"t": millis(item["value"][0]), "v": float(item["value"][1])}]
            else:
                points = [{"t": millis(t), "v": float(v)} for t, v in item.get("values") or []]
            series.append({"labels": labels, "points": points})

    return {"type": "vector" if kind == "instant" else "matrix", "series": sort_series(series)}


def close_enough(left: float, right: float) -> bool:
    if math.isnan(left) and math.isnan(right):
        return True
    if math.isinf(left) or math.isinf(right):
        return left == right
    if left == right:
        return True
    return abs(left - right) <= RELATIVE_TOLERANCE * max(abs(left), abs(right))


# The one difference this harness accepts, and only in the label set: Prometheus drops `__name__`
# when a function changes what the samples mean, GreptimeDB keeps it. Values, timestamps, missing
# points and extra series are never excused. See tests/narthex-compat/KNOWN-DIFFERENCES.md.
FUNCTIONS_WITHOUT_METRIC_NAME = (
    "rate(",
    "irate(",
    "increase(",
    "delta(",
    "idelta(",
    "deriv(",
    "predict_linear(",
)


def only_metric_name_differs(
    expr: str, expected: list[dict], actual: list[dict]
) -> bool:
    """Whether the answers match once `__name__` is set aside.

    Prometheus drops `__name__` when a function changes what the samples mean, GreptimeDB keeps it,
    so one side names the series and the other does not. The comparison is rerun over label sets
    without that label: if it then agrees, the only difference was the name. Values, timestamps,
    missing points and any other extra series still fail.
    """
    if not any(expr.strip().startswith(fn) for fn in FUNCTIONS_WITHOUT_METRIC_NAME):
        return False

    def strip(series: list[dict]) -> list[dict]:
        stripped = []
        for item in series:
            labels = {k: v for k, v in item["labels"].items() if k != "__name__"}
            stripped.append({"labels": labels, "points": item["points"]})
        return sort_series(stripped)

    before = compare_series(expected, actual)
    if not before:
        return False
    return not compare_series(strip(expected), strip(actual))


def compare_series(expected: list[dict], actual: list[dict]) -> list[str]:
    """Every difference, as a readable line. An empty list means the answers agree."""
    problems = []
    exp = {json.dumps(s["labels"], sort_keys=True): s for s in expected}
    act = {json.dumps(s["labels"], sort_keys=True): s for s in actual}

    for key in sorted(set(exp) - set(act)):
        problems.append(f"missing series {key}")
    for key in sorted(set(act) - set(exp)):
        problems.append(f"unexpected series {key}")

    for key in sorted(set(exp) & set(act)):
        exp_points = {p["t"]: p["v"] for p in exp[key]["points"]}
        act_points = {p["t"]: p["v"] for p in act[key]["points"]}
        for t in sorted(set(exp_points) - set(act_points)):
            problems.append(f"{key} missing point at t={t} (prometheus={exp_points[t]})")
        for t in sorted(set(act_points) - set(exp_points)):
            problems.append(f"{key} unexpected point at t={t} (greptimedb={act_points[t]})")
        for t in sorted(set(exp_points) & set(act_points)):
            if not close_enough(exp_points[t], act_points[t]):
                problems.append(
                    f"{key} t={t}: prometheus={exp_points[t]!r} greptimedb={act_points[t]!r}"
                )
    return problems


def run_case(args, case_path: Path) -> list[dict]:
    case = json.loads(case_path.read_text())

    oracle_out = subprocess.run(
        [args.oracle, "-cases", str(case_path)],
        capture_output=True,
        text=True,
        check=False,
    )
    if oracle_out.returncode != 0:
        raise SystemExit(f"oracle failed on {case_path}: {oracle_out.stderr}")
    expected = {a["name"]: a for a in json.loads(oracle_out.stdout)}

    tables = create_table_and_load(args.sql_url, case, args.native)
    print(f"{case['name']}: loaded {', '.join(tables)}")

    results = []
    for query in case["queries"]:
        answer = expected[query["name"]]
        payload = query_prometheus(args.prom_url, query, args.native)
        actual = normalize_greptime(payload, query["kind"])

        entry = {
            "case": case["name"],
            "query": query["name"],
            "expr": query["expr"],
            "kind": query["kind"],
            "prometheus_type": answer.get("type"),
            "greptimedb_type": actual.get("type"),
            "prometheus_error": answer.get("error"),
            "greptimedb_error": actual.get("error"),
            "problems": [],
        }

        if query.get("expect_error"):
            # Both sides must refuse it. Comparing values here would compare two errors.
            if not answer.get("error"):
                entry["problems"].append("prometheus accepted a query this case expects to fail")
            if not actual.get("error"):
                entry["problems"].append("greptimedb accepted a query this case expects to fail")
            entry["status"] = "failed" if entry["problems"] else "ok"
            results.append(entry)
            continue

        if answer.get("error") or actual.get("error"):
            if answer.get("error") and actual.get("error"):
                # Both refused. The messages need not match; the refusal itself is the contract.
                entry["status"] = "ok"
            else:
                entry["status"] = "failed"
                entry["problems"].append("only one side refused the query")
            results.append(entry)
            continue

        if answer.get("type") != actual.get("type"):
            entry["problems"].append(
                f"result type differs: prometheus={answer.get('type')}"
                f" greptimedb={actual.get('type')}"
            )

        if answer.get("type") == "scalar":
            if not close_enough(answer["scalar"], actual["scalar"]):
                entry["problems"].append(
                    f"scalar differs: prometheus={answer['scalar']!r}"
                    f" greptimedb={actual['scalar']!r}"
                )
        else:
            entry["problems"].extend(
                compare_series(answer.get("series", []), actual.get("series", []))
            )

        if entry["problems"] and only_metric_name_differs(
            query["expr"], answer.get("series", []), actual.get("series", [])
        ):
            entry["status"] = "known-difference"
        else:
            entry["status"] = "failed" if entry["problems"] else "ok"
        results.append(entry)
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cases", required=True, help="a case file, or a directory of them"
    )
    parser.add_argument("--oracle", required=True, help="path to the built Prometheus oracle")
    parser.add_argument("--sql-url", default="http://127.0.0.1:14000/v1/sql")
    parser.add_argument("--prom-url", default="http://127.0.0.1:14000")
    parser.add_argument(
        "--native",
        action="store_true",
        help="send the native PromQL hint; the comparison is identical either way",
    )
    parser.add_argument("--report", help="write the JSON report here")
    args = parser.parse_args()

    cases_path = Path(args.cases)
    case_files = (
        sorted(cases_path.glob("*.json")) if cases_path.is_dir() else [cases_path]
    )
    if not case_files:
        print(f"error: no case file under {cases_path}", file=sys.stderr)
        return 2

    all_results = []
    for case_file in case_files:
        all_results.extend(run_case(args, case_file))

    failed = [r for r in all_results if r["status"] == "failed"]
    known = [r for r in all_results if r["status"] == "known-difference"]
    passed = len(all_results) - len(failed) - len(known)
    print(
        f"\n{passed}/{len(all_results)} queries agree across {len(case_files)} case file(s)"
        f" ({len(known)} known difference(s), {len(failed)} failure(s))"
    )
    for entry in known:
        print(f"\n  KNOWN [{entry['case']}] {entry['query']}  ({entry['expr']})")
        for problem in entry["problems"]:
            print(f"    {problem}")
    for entry in failed:
        print(f"\n  FAIL [{entry['case']}] {entry['query']}  ({entry['expr']})")
        if entry["prometheus_error"] or entry["greptimedb_error"]:
            print(f"    prometheus: {entry['prometheus_error']}")
            print(f"    greptimedb: {entry['greptimedb_error']}")
        for problem in entry["problems"]:
            print(f"    {problem}")

    if args.report:
        Path(args.report).write_text(json.dumps(all_results, indent=2) + "\n")

    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
