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

"""Untimed three-way correlation fixture diagnostic for an existing cluster."""

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request

BASE = "2024-01-01 00:00:00"
NAMES = ("r", "s_aligned", "s_anti", "t")
COLUMNS = ("r_key", "r_val", "s_key", "s_val", "t_key", "t_val")


class ProbeError(RuntimeError):
    pass


def sql_ident(value):
    return '"' + value.replace('"', '""') + '"'


def literal(value):
    if value is None:
        return "NULL"
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    return str(value)


def query(url, statement):
    endpoint = url.rstrip("/") + "/v1/sql?db=public"
    request = urllib.request.Request(
        endpoint,
        urllib.parse.urlencode({"sql": statement}).encode(),
        {"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            raw = response.read().decode("utf-8")
    except urllib.error.HTTPError as error:
        raise ProbeError("HTTP %s for %r: %s" % (error.code, statement, error.read().decode("utf-8", "replace"))) from error
    except (urllib.error.URLError, TimeoutError, UnicodeError) as error:
        raise ProbeError("request failed for %r: %s" % (statement, error)) from error
    try:
        data = json.loads(raw)
    except ValueError as error:
        raise ProbeError("non-JSON response for %r: %s" % (statement, raw)) from error
    if not isinstance(data, dict) or data.get("code") not in (None, 0, "0") or data.get("error"):
        raise ProbeError("SQL envelope error for %r: %r" % (statement, data))
    output = data.get("output")
    if not isinstance(output, list) or len(output) != 1 or not isinstance(output[0], dict):
        raise ProbeError("expected exactly one output for %r: %r" % (statement, data))
    item = output[0]
    if item.get("code") not in (None, 0, "0") or item.get("error"):
        raise ProbeError("SQL result error for %r: %r" % (statement, item))
    is_read = statement.lstrip().upper().startswith(("SELECT", "EXPLAIN", "ADMIN"))
    records = item.get("records")
    if not is_read:
        affected = item.get("affectedrows")
        if records is not None or not isinstance(affected, int) or isinstance(affected, bool) or affected < 0:
            raise ProbeError("invalid mutation affectedrows/records for %r: %r" % (statement, item))
        return {"sql": statement, "response": data, "affected_rows": affected}
    if records is None:
        raise ProbeError("missing records for %r: %r" % (statement, item))
    if not isinstance(records, dict) or not isinstance(records.get("schema"), dict):
        raise ProbeError("missing records/schema for %r: %r" % (statement, data))
    schema = records["schema"].get("column_schemas")
    rows, total = records.get("rows"), records.get("total_rows")
    if not isinstance(schema, list) or not isinstance(rows, list):
        raise ProbeError("missing schema/rows for %r: %r" % (statement, data))
    names = [col.get("name") for col in schema if isinstance(col, dict)]
    if len(names) != len(schema) or any(not isinstance(n, str) or not n for n in names) or len(set(names)) != len(names):
        raise ProbeError("invalid/duplicate schema columns for %r: %r" % (statement, schema))
    if any(not isinstance(row, list) or len(row) != len(names) for row in rows):
        raise ProbeError("malformed or truncated rows for %r: %r" % (statement, records))
    if not isinstance(total, int) or total != len(rows):
        raise ProbeError("missing/mismatched total_rows for %r: %r" % (statement, records))
    if not rows:
        raise ProbeError("empty result for %r" % statement)
    return {"sql": statement, "response": data, "columns": names, "rows": rows,
            "total_rows": total}


def source_rows(name):
    rows = []
    if name in ("r", "s_aligned", "s_anti"):
        hot = 0 if name != "s_anti" else 1
        for key in range(16):
            count = 68 if key == hot else 4
            for i in range(count):
                rows.append((key, None if i % 5 == 0 else i % 9))
        rows.append((None, None))
    else:
        rows = [(key, None if key % 17 == 0 else key % 11) for key in range(2048)]
        rows.append((None, None))
    return rows


def canonical(row):
    return tuple((value is None, value if value is not None else 0) for value in row)


def oracle(anti):
    r, s, t = source_rows("r"), source_rows("s_anti" if anti else "s_aligned"), source_rows("t")
    result = [(rk, rv, sk, sv, tk, tv) for rk, rv in r for sk, sv in s
              if rk is not None and rk == sk for tk, tv in t if rk == tk]
    return sorted(result, key=canonical)


def run(args):
    suffix = "%x_%s" % (os.getpid(), os.urandom(4).hex())
    tables = {key: "paper_cb3_%s_%s" % (key, suffix) for key in NAMES}
    created, evidence = [], {"status": "running", "tables": tables,
        "performance_claim": False, "query_shape": "not forced fixed tree",
        "topology_limitation": "Multiple regions do not establish multiple datanodes; a single-datanode run is not multi-node.",
        "corrbound_provider": False,
        "ordinary_stats_consumption": "unknown until actual plan inspected",
        "fixture": {"R_rows": 129, "S_rows": 129, "T_rows": 2049,
                    "aligned_expected_rows": 4864, "anti_expected_rows": 768},
        "setup": [], "sources": {}, "regions": {}, "queries": {}}
    try:
        for key in NAMES:
            table = tables[key]
            statement = (f"CREATE TABLE {sql_ident(table)} (k INT, v INT, ts TIMESTAMP(9) TIME INDEX, "
                "PRIMARY KEY(k)) PARTITION ON COLUMNS (k) (k < 8, k >= 8) "
                "ENGINE=mito WITH (append_mode='true')")
            evidence["setup"].append({"sql": statement, "status": "attempted"})
            try:
                response = query(args.url, statement)
            except Exception as error:
                evidence["setup"][-1]["error"] = str(error)
                raise
            created.append(table)
            evidence["setup"][-1].update({"status": "created", "response": response["response"]})
        for name in NAMES:
            values = []
            for index, (key, value) in enumerate(source_rows(name)):
                ts = "2024-01-01 %02d:%02d:%02d.%09d+00:00" % (index // 3600, index // 60 % 60, index % 60, 0)
                values.append("(%s,%s,%s)" % (literal(key), literal(value), literal(ts)))
            for start in range(0, len(values), 128):
                evidence["setup"].append(query(args.url, "INSERT INTO %s (k,v,ts) VALUES %s" %
                    (sql_ident(tables[name]), ",".join(values[start:start + 128]))))
        for name in NAMES:
            table = tables[name]
            evidence["setup"].append(query(args.url, "ADMIN FLUSH_TABLE('%s')" % table))
            statement = ("SELECT COUNT(*) AS n, COUNT(k) AS nonnull_k, COUNT(DISTINCT k) AS ndv, "
                         "MIN(k) AS min_k, MAX(k) AS max_k FROM %s" % sql_ident(table))
            actual = query(args.url, statement)
            frequency = query(args.url, "SELECT k AS k, COUNT(*) AS n FROM %s GROUP BY k ORDER BY k NULLS LAST" % sql_ident(table))
            evidence["sources"][name] = {"summary": actual, "frequency": frequency}
            expected = source_rows(name)
            if actual["columns"] != ["n", "nonnull_k", "ndv", "min_k", "max_k"]:
                raise ProbeError("unexpected source-summary schema for " + name)
            if actual["rows"] != [[len(expected), len(expected) - 1, 16 if name != "t" else 2048, 0, 15 if name != "t" else 2047]]:
                raise ProbeError("source marginal summary mismatch for " + name)
            frequencies = {}
            for key, _ in expected:
                frequencies[key] = frequencies.get(key, 0) + 1
            if frequency["columns"] != ["k", "n"]:
                raise ProbeError("unexpected frequency schema for " + name)
            observed = {}
            for key, count in frequency["rows"]:
                if key in observed:
                    raise ProbeError("duplicate key frequency row for " + name)
                observed[key] = count
            if observed != frequencies:
                raise ProbeError("source key frequencies mismatch for " + name)
            region_sql = ("SELECT * FROM information_schema.region_peers WHERE table_name = '%s' AND table_schema = 'public'" % table)
            region = query(args.url, region_sql)
            evidence["regions"][name] = region
            cols = region["columns"]
            if not {"region_id", "is_leader", "status"}.issubset(cols):
                raise ProbeError("region_peers lacks region_id/is_leader/status columns")
            live_leaders = [row for row in region["rows"]
                            if row[cols.index("is_leader")] == "Yes" and row[cols.index("status")] == "ALIVE"]
            ids = {row[cols.index("region_id")] for row in live_leaders}
            peer_ids = sorted({row[cols.index("peer_id")] for row in live_leaders}) if "peer_id" in cols else []
            evidence["regions"][name]["live_leader_region_ids"] = sorted(ids)
            evidence["regions"][name]["live_leader_peer_ids"] = peer_ids
            if len(ids) < 2:
                raise ProbeError("fewer than two distinct live leader regions for " + name)
        for variant, sname in (("aligned", "s_aligned"), ("anti", "s_anti")):
            for orientation in ("rs_then_t", "t_then_sr"):
                a, b, c = (sql_ident(tables[x]) for x in ("r", sname, "t"))
                if orientation == "rs_then_t":
                    from_sql = f"({a} r JOIN {b} s ON r.k=s.k) JOIN {c} t ON s.k=t.k"
                else:
                    from_sql = f"{c} t JOIN ({b} s JOIN {a} r ON s.k=r.k) ON s.k=t.k"
                predicates = " AND ".join(
                    f"{alias}.ts >= '{BASE}+00:00'::TIMESTAMP AND "
                    f"{alias}.ts < '2024-01-01 01:00:00+00:00'::TIMESTAMP"
                    for alias in ("r", "s", "t")
                )
                sql = ("SELECT r.k AS r_key, r.v AS r_val, s.k AS s_key, s.v AS s_val, "
                       f"t.k AS t_key, t.v AS t_val FROM {from_sql} WHERE {predicates} "
                       "ORDER BY r_key NULLS LAST, r_val NULLS LAST, s_key NULLS LAST, "
                       "s_val NULLS LAST, t_key NULLS LAST, t_val NULLS LAST")
                expected = oracle(variant == "anti")
                if len(expected) != (4864 if variant == "aligned" else 768):
                    raise ProbeError("internal oracle cardinality mismatch")
                result = query(args.url, sql)
                if result["columns"] != list(COLUMNS):
                    raise ProbeError("unexpected six-column schema: %r" % result["columns"])
                if any(value is not None and (not isinstance(value, int) or isinstance(value, bool))
                       for row in result["rows"] for value in row):
                    raise ProbeError("join result contains a non-integer/non-NULL value")
                if result["rows"] != [list(row) for row in expected]:
                    raise ProbeError("full six-column result differs from nested-loop oracle: %s/%s" % (variant, orientation))
                entry = {"sql": result["sql"], "full_response": result["response"],
                         "oracle_rows": [list(row) for row in expected], "oracle_pass": True}
                for explain in ("EXPLAIN VERBOSE " + sql, "EXPLAIN ANALYZE " + sql):
                    entry.setdefault("explain", []).append(query(args.url, explain))
                evidence["queries"][variant + "_" + orientation] = entry
        evidence["status"] = "success"
    except Exception as error:
        evidence["status"], evidence["failure"] = "failure", str(error)
        raise
    finally:
        evidence["cleanup"] = []
        for table in reversed(created):
            try:
                evidence["cleanup"].append(query(args.url, "DROP TABLE IF EXISTS " + sql_ident(table)))
            except Exception as error:
                evidence["cleanup"].append({"table": table, "error": str(error)})
        cleanup_failed = any("error" in item for item in evidence["cleanup"])
        if evidence["status"] == "success" and cleanup_failed:
            evidence["status"] = "cleanup_failure"
        try:
            os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
            with open(args.out, "w", encoding="utf-8") as output:
                json.dump(evidence, output, indent=2, sort_keys=True)
                output.write("\n")
            print("evidence: " + args.out, file=sys.stderr)
        except OSError as error:
            print("cannot write evidence %s: %s" % (args.out, error), file=sys.stderr)
            if evidence["status"] not in ("failure", "cleanup_failure"):
                raise
        if evidence["status"] == "cleanup_failure":
            raise ProbeError("one or more owned tables could not be dropped")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("--out", required=True, help="path for the evidence JSON file")
    args = parser.parse_args()
    try:
        run(args)
    except Exception as error:
        print("corrbound-three-way failed: " + str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
