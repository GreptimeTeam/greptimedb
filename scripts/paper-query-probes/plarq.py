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

"""Probe PostgreSQL prepared-statement reuse against simple-query execution."""

import argparse
import ctypes
import hashlib
import json
import os
import re
import sys
import urllib.error
import urllib.request
import urllib.parse


CASES = (("wide", 0, 4), ("rare_dim", 0, 1), ("narrow_fact", 2032, 4), ("empty", 2048, 4))
REPETITIONS = 2
QUERY_TEMPLATE = (
    "SELECT f.host, COUNT(*) AS n, SUM(f.value + d.weight) AS s "
    "FROM {fact} AS f JOIN {dim} AS d ON f.host = d.host "
    'WHERE f."value" >= CAST($1 AS BIGINT) AND d.weight <= CAST($2 AS BIGINT) '
    "GROUP BY f.host ORDER BY f.host"
)


class ProbeError(RuntimeError):
    pass


def http_sql(base_url, sql):
    payload = urllib.parse.urlencode({"sql": sql}).encode("utf-8")
    request = urllib.request.Request(
        base_url.rstrip("/") + "/v1/sql?db=public",
        data=payload,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", "replace")
        raise ProbeError("HTTP %d for %s: %s" % (error.code, sql[:120], detail[:1000])) from error
    except (urllib.error.URLError, TimeoutError) as error:
        raise ProbeError("HTTP SQL request failed: " + str(error)) from error
    try:
        decoded = json.loads(body)
    except json.JSONDecodeError as error:
        raise ProbeError("HTTP SQL returned non-JSON: " + body[:500]) from error
    if isinstance(decoded, dict) and decoded.get("code", 0) not in (0, "0", None):
        raise ProbeError("HTTP SQL error: " + json.dumps(decoded, sort_keys=True))
    for item in decoded.get("output", []) if isinstance(decoded, dict) else []:
        if isinstance(item, dict) and item.get("code", 0) not in (0, "0", None):
            raise ProbeError("HTTP SQL error: " + json.dumps(item, sort_keys=True))
    return decoded


def metric_snapshot(base_url):
    request = urllib.request.Request(base_url.rstrip("/") + "/metrics")
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            text = response.read().decode("utf-8")
    except (urllib.error.URLError, TimeoutError) as error:
        raise ProbeError("metrics request failed: " + str(error)) from error
    wanted = re.compile(r"^(greptime_query_stage_elapsed_(?:count|sum)|greptime_servers_postgres_prepared_count)(?:\{([^}]*)\})?\s+([0-9.eE+-]+)\s*$")
    result = {}
    for line in text.splitlines():
        match = wanted.match(line)
        if match:
            result[match.group(1) + ("{" + (match.group(2) or "") + "}" if match.group(2) else "")] = float(match.group(3))
    return result


def metric_deltas(before, after):
    keys = sorted(set(before) | set(after))
    return {key: after.get(key, 0.0) - before.get(key, 0.0) for key in keys}


def expected_rows(threshold, weight_limit):
    # Fixture generation is deterministic: each host has 128 facts and four dimensions.
    grouped = {}
    for row_id in range(2048):
        host = "h%02d" % (row_id % 16)
        value = row_id
        if value < threshold:
            continue
        for weight in range(1, 5):
            if weight <= weight_limit:
                item = grouped.setdefault(host, [0, 0])
                item[0] += 1
                item[1] += value + weight
    return [[host, values[0], values[1]] for host, values in sorted(grouped.items())]


def libpq_library():
    candidates = []
    try:
        import psycopg2
        import glob
        package_dir = os.path.dirname(psycopg2.__file__)
        candidates.extend(glob.glob(os.path.join(package_dir, "..", "psycopg2_binary.libs", "libpq*.so*")))
    except ImportError as error:
        raise ProbeError("existing psycopg2 installation is unavailable") from error
    for path in sorted(candidates):
        if os.path.exists(path):
            return ctypes.CDLL(path)
    raise ProbeError("could not locate psycopg2's bundled libpq library")


def configure_libpq(lib):
    signatures = {
        "PQprepare": ([ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_int, ctypes.POINTER(ctypes.c_uint)], ctypes.c_void_p),
        "PQexecPrepared": ([ctypes.c_void_p, ctypes.c_char_p, ctypes.c_int, ctypes.POINTER(ctypes.c_char_p), ctypes.POINTER(ctypes.c_int), ctypes.POINTER(ctypes.c_int), ctypes.c_int], ctypes.c_void_p),
        "PQexec": ([ctypes.c_void_p, ctypes.c_char_p], ctypes.c_void_p),
        "PQresultStatus": ([ctypes.c_void_p], ctypes.c_int),
        "PQresultErrorMessage": ([ctypes.c_void_p], ctypes.c_char_p),
        "PQclear": ([ctypes.c_void_p], None),
        "PQntuples": ([ctypes.c_void_p], ctypes.c_int),
        "PQnfields": ([ctypes.c_void_p], ctypes.c_int),
        "PQgetvalue": ([ctypes.c_void_p, ctypes.c_int, ctypes.c_int], ctypes.c_void_p),
        "PQgetisnull": ([ctypes.c_void_p, ctypes.c_int, ctypes.c_int], ctypes.c_int),
    }
    for name, (args, result) in signatures.items():
        function = getattr(lib, name)
        function.argtypes = args
        function.restype = result


def checked_result(lib, result, expected_status, label):
    if not result:
        raise ProbeError(label + ": libpq returned a null result")
    status = lib.PQresultStatus(result)
    if status != expected_status:
        message = lib.PQresultErrorMessage(result)
        raise ProbeError(label + ": status %d: %s" % (status, message.decode("utf-8", "replace") if message else "unknown error"))


def pg_text_rows(lib, result):
    rows = []
    for row in range(lib.PQntuples(result)):
        values = []
        for column in range(lib.PQnfields(result)):
            if lib.PQgetisnull(result, row, column):
                values.append(None)
            else:
                pointer = lib.PQgetvalue(result, row, column)
                values.append(ctypes.string_at(pointer).decode("utf-8"))
        rows.append(values)
    return rows


def explain_plans(base_url, fact, dim):
    plans = {}
    for name, threshold, weight_limit in CASES:
        sql = "EXPLAIN VERBOSE " + QUERY_TEMPLATE.format(fact=fact, dim=dim).replace("$1", str(threshold)).replace("$2", str(weight_limit))
        plans[name] = http_rows(http_sql(base_url, sql))
    return plans


def http_rows(data):
    rows = []
    for item in data.get("output", []) if isinstance(data, dict) else []:
        records = item.get("records") if isinstance(item, dict) else None
        if isinstance(records, dict):
            rows.extend(records.get("rows", []))
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True, help="GreptimeDB HTTP base URL")
    parser.add_argument("--pg-port", type=int, required=True, help="local PostgreSQL protocol port")
    parser.add_argument("--out", required=True, help="evidence output directory")
    args = parser.parse_args()
    if args.pg_port < 1 or args.pg_port > 65535:
        parser.error("--pg-port must be a valid TCP port")
    os.makedirs(args.out, exist_ok=True)
    suffix = "%d_%s" % (os.getpid(), os.urandom(4).hex())
    fact, dim = "paper_plarq_fact_" + suffix, "paper_plarq_dim_" + suffix
    query = QUERY_TEMPLATE.format(fact=fact, dim=dim)
    evidence = {"fixture": {"fact": fact, "dimension": dim, "fact_rows": 2048, "dimension_rows": 64}, "cases": {}}
    try:
        http_sql(args.url, "CREATE DATABASE IF NOT EXISTS public")
        http_sql(args.url, 'CREATE TABLE %s (ts TIMESTAMP TIME INDEX, host STRING, "value" BIGINT) WITH (append_mode = \'true\')' % fact)
        http_sql(args.url, 'CREATE TABLE %s (ts TIMESTAMP TIME INDEX, host STRING, weight BIGINT) WITH (append_mode = \'true\')' % dim)
        for start in range(0, 2048, 256):
            rows = []
            for row_id in range(start, min(start + 256, 2048)):
                host = "h%02d" % (row_id % 16)
                rows.append("(TIMESTAMP '2024-01-01 00:00:00' + INTERVAL '%d' SECOND, '%s', %d)" % (row_id, host, row_id))
            http_sql(args.url, "INSERT INTO %s VALUES %s" % (fact, ",".join(rows)))
        dim_rows = []
        for host_id in range(16):
            for weight in range(1, 5):
                dim_rows.append("(TIMESTAMP '2024-01-01 00:00:00' + INTERVAL '%d' SECOND, 'h%02d', %d)" % (host_id * 4 + weight, host_id, weight))
        http_sql(args.url, "INSERT INTO %s VALUES %s" % (dim, ",".join(dim_rows)))
        setup_evidence = {}
        for table in (fact, dim):
            setup_evidence[table] = {
                "flush": http_sql(args.url, "ADMIN FLUSH_TABLE('%s')" % table),
                "show_create": http_sql(args.url, "SHOW CREATE TABLE %s" % table),
            }
        evidence["setup"] = setup_evidence

        plans = explain_plans(args.url, fact, dim)
        evidence["explain_verbose"] = plans
        plan_hashes = {}
        for name, rows in plans.items():
            selected = [row for row in rows if row[0] in ("physical_plan", "physical_plan_with_stats")]
            evidence["cases"].setdefault(name, {"parameters": list(next(case[1:] for case in CASES if case[0] == name))})
            evidence["cases"][name]["explain_physical_plan"] = selected
            evidence["cases"][name]["physical_plan_sha256"] = hashlib.sha256(json.dumps(selected, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
            plan_hashes[name] = evidence["cases"][name]["physical_plan_sha256"]
        evidence["physical_plan_sha256_by_parameters"] = plan_hashes
        evidence["physical_plan_parameter_variants"] = len(set(plan_hashes.values()))
        try:
            import psycopg2
        except ImportError as error:
            raise ProbeError("existing psycopg2 driver is unavailable") from error
        connection = psycopg2.connect(host="127.0.0.1", port=args.pg_port, user="greptime", dbname="public", sslmode="disable", connect_timeout=5)
        try:
            pointer = getattr(connection, "pgconn_ptr", None)
            evidence["pgconn_ptr_supported"] = pointer is not None
            if pointer is None:
                raise ProbeError("installed psycopg2 does not expose pgconn_ptr; refusing a second libpq connection")
            lib = libpq_library()
            configure_libpq(lib)
            evidence["same_connection"] = True
            prepare_before = metric_snapshot(args.url)
            result = lib.PQprepare(pointer, b"paper_plarq_reuse", query.encode(), 0, None)
            try:
                checked_result(lib, result, 1, "PQprepare")
            finally:
                if result:
                    lib.PQclear(result)
            prepare_after = metric_snapshot(args.url)
            evidence["prepare_metric_delta"] = metric_deltas(prepare_before, prepare_after)

            def execute_prepared(case):
                name, threshold, weight_limit = case
                params = (ctypes.c_char_p * 2)(str(threshold).encode(), str(weight_limit).encode())
                lengths = (ctypes.c_int * 2)(0, 0)
                formats = (ctypes.c_int * 2)(0, 0)
                result = lib.PQexecPrepared(pointer, b"paper_plarq_reuse", 2, params, lengths, formats, 0)
                try:
                    checked_result(lib, result, 2, "PQexecPrepared " + name)
                    rows = pg_text_rows(lib, result)
                finally:
                    if result:
                        lib.PQclear(result)
                expected = expected_rows(threshold, weight_limit)
                normalized = [[row[0], int(row[1]), int(row[2])] for row in rows]
                if normalized != expected:
                    raise ProbeError("prepared result does not match Python oracle for " + name)
                case_evidence = evidence["cases"].setdefault(name, {"parameters": [threshold, weight_limit]})
                case_evidence["expected_rows"] = expected
                case_evidence.setdefault("prepared_actual_rows", []).append(normalized)

            evidence["client_prepares"] = 1
            evidence["prepared_client_executions"] = REPETITIONS * len(CASES)
            before = metric_snapshot(args.url)
            for index in range(REPETITIONS):
                for case in CASES:
                    execute_prepared(case)
            after = metric_snapshot(args.url)
            evidence["prepared_execution_metrics"] = {"before": before, "after": after, "delta": metric_deltas(before, after)}

            before = metric_snapshot(args.url)
            simple_results = {}
            for index in range(REPETITIONS):
                for name, threshold, weight_limit in CASES:
                    sql = query.replace("$1", str(threshold)).replace("$2", str(weight_limit))
                    result = lib.PQexec(pointer, sql.encode())
                    try:
                        checked_result(lib, result, 2, "PQexec simple " + name)
                        rows = pg_text_rows(lib, result)
                    finally:
                        if result:
                            lib.PQclear(result)
                    normalized = [[row[0], int(row[1]), int(row[2])] for row in rows]
                    if normalized != expected_rows(threshold, weight_limit):
                        raise ProbeError("simple result does not match Python oracle for " + name)
                    simple_results.setdefault(name, []).append(normalized)
            after = metric_snapshot(args.url)
            evidence["simple_client_executions"] = REPETITIONS * len(CASES)
            evidence["simple_execution_metrics"] = {"before": before, "after": after, "delta": metric_deltas(before, after)}
            evidence["simple_results"] = simple_results
            evidence["metric_scope_note"] = "query-stage histograms are process-global and may include frontend plus remote-region observations; deltas are observations, not per-client execution counts"
        finally:
            connection.close()
        output_path = os.path.join(args.out, "plarq-" + suffix + ".json")
        with open(output_path, "w", encoding="utf-8") as output:
            json.dump(evidence, output, indent=2, sort_keys=True)
            output.write("\n")
        print(json.dumps({"evidence": output_path, "fixture": evidence["fixture"], "client_prepares": evidence["client_prepares"], "prepared_client_executions": evidence["prepared_client_executions"], "simple_client_executions": evidence["simple_client_executions"], "prepared_stage_count_deltas": {key: value for key, value in evidence["prepared_execution_metrics"]["delta"].items() if "count" in key}, "simple_stage_count_deltas": {key: value for key, value in evidence["simple_execution_metrics"]["delta"].items() if "count" in key}}, sort_keys=True))
        return 0
    except Exception as error:
        print("plarq failed: " + str(error), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
