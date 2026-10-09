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

"""Bounded residual-evaluation diagnostic for LIKE and regex predicates."""

import argparse
import hashlib
import json
import os
import re
import sys
import urllib.error
import urllib.request
import urllib.parse

ROWS = 2048
PAGE = 128
PATTERNS = {
    "like": ("message LIKE '%timeout%'", lambda value: value is not None and "%timeout%"[1:-1] in value),
    "literal_regex_operator": ("message ~ 'timeout'", lambda value: value is not None and "timeout" in value),
    "regex_operator": ("message ~ 'timeout.*upstream'", lambda value: value is not None and re.search(r"timeout.*upstream", value) is not None),
    "regex_function": ("regexp_like(message, 'timeout.*upstream')", lambda value: value is not None and re.search(r"timeout.*upstream", value) is not None),
}


class ProbeError(RuntimeError):
    pass


def unwrap(obj):
    """Find a result record table in the v1/sql response; preserve raw if unknown."""
    if isinstance(obj, dict):
        if obj.get("code") not in (None, 0, "0"):
            raise ProbeError("SQL API returned code %r: %s" % (obj.get("code"), obj.get("error", obj)))
        if "error" in obj and obj["error"]:
            raise ProbeError("SQL API error: %s" % obj["error"])
        if "records" in obj and isinstance(obj["records"], dict):
            return obj["records"]
        for key in ("output", "results", "result"):
            if key in obj:
                found = unwrap(obj[key])
                if found is not None:
                    return found
        for value in obj.values():
            found = unwrap(value)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for value in obj:
            found = unwrap(value)
            if found is not None:
                return found
    return None


def sql(url, statement):
    endpoint = url.rstrip("/") + "/v1/sql?db=public"
    payload = urllib.parse.urlencode({"sql": statement}).encode()
    request = urllib.request.Request(endpoint, data=payload, headers={"Content-Type": "application/x-www-form-urlencoded"}, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace")
        raise ProbeError("HTTP %s for %s: %s" % (exc.code, statement[:100], detail[:1000]))
    except (urllib.error.URLError, TimeoutError) as exc:
        raise ProbeError("HTTP request failed for %s: %s" % (statement[:100], exc))
    try:
        obj = json.loads(raw)
    except ValueError:
        raise ProbeError("Non-JSON response for %s: %s" % (statement[:100], raw[:1000]))
    # Multi-statement admin responses may encode an error in a nested result.
    records = unwrap(obj)
    return {"statement": statement, "response": obj, "records": records}


def rows_of(result):
    records = result["records"]
    if records is None:
        return []
    return records.get("rows", [])


def explain_scan_metrics(explain_result):
    """Extract scan operator JSON metrics while retaining the unmodified plan elsewhere."""
    result = []
    for row in rows_of(explain_result):
        if not row or not isinstance(row[-1], str):
            continue
        text = row[-1]
        for line in text.splitlines():
            if "UnorderedScan:" not in line and "SeqScan:" not in line:
                continue
            start = line.find("{")
            if start < 0:
                continue
            try:
                parsed, _ = json.JSONDecoder().raw_decode(line[start:])
            except ValueError:
                continue
            for partition in parsed.get("metrics_per_partition", []):
                metrics = partition.get("metrics", {})
                inverted = metrics.get("inverted_index_apply_metrics")
                result.append({
                    "partition": partition.get("partition"),
                    "num_rows": metrics.get("num_rows"),
                    "rows_before_filter": metrics.get("rows_before_filter"),
                    "prefilter_filtered_rows": metrics.get("fetch_metrics", {}).get("prefilter_filtered_rows"),
                    "index_apply_present": inverted is not None,
                    "inverted_index_apply_metrics": inverted,
                })
    return result


def sql_ident(name):
    return '"' + name.replace('"', '""') + '"'


def make_values(i):
    # Unique-ish long messages, with a small predictable positive subset and edge cases.
    if i == 0:
        message = None
    elif i == 1:
        message = ""
    elif i == 2:
        message = "timeout.*upstream literal; not a regex match"
    elif i == 3:
        message = "timeout\nupstream with newline"
    elif i == 4:
        message = "Unicode timeout upstream café ☕ punctuation !?"
    elif i % 100 == 0:
        message = "request timeout while waiting for upstream " + ("x%04d " % i) * 34
    else:
        message = "request completed without issue id=%04d " % i + ("ordinary-payload-%04d " % i) * 10
    service = "api" if i % 5 else "worker"
    return i, i, service, message


def lit(value):
    if value is None:
        return "NULL"
    return "'" + value.replace("'", "''") + "'"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:33720")
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    os.makedirs(args.out, exist_ok=True)
    suffix = "%x" % os.getpid()
    base = "paper_rei_" + suffix + "_" + hashlib.sha256(os.urandom(16)).hexdigest()[:8]
    control, indexed = base + "_control", base + "_indexed"
    evidence = {"scope": "bounded local diagnostic; no timing/performance claim", "rows_inserted": ROWS,
                "tables": {"control": control, "indexed": indexed}, "queries": {}, "setup": []}
    statements = []
    for table in (control, indexed):
        statements.append('CREATE TABLE %s ("id" INT, ts TIMESTAMP(3) TIME INDEX, "service" STRING, "message" STRING) ENGINE=mito WITH (append_mode = \'true\')' % sql_ident(table))
    for statement in statements:
        evidence["setup"].append(sql(args.url, statement))
    evidence["setup"].append(sql(args.url, "ALTER TABLE %s MODIFY COLUMN message SET INVERTED INDEX" % sql_ident(indexed)))
    for start in range(0, ROWS, PAGE):
        values = [make_values(i) for i in range(start, min(ROWS, start + PAGE))]
        for table in (control, indexed):
            tuples = ",".join("(%d, %d, %s, %s)" % (i, i, lit(service), lit(message)) for i, _, service, message in values)
            evidence["setup"].append(sql(args.url, 'INSERT INTO %s ("id", ts, service, message) VALUES %s' % (sql_ident(table), tuples)))
    for table in (control, indexed):
        evidence["setup"].append(sql(args.url, "ADMIN FLUSH_TABLE('%s')" % table))
    for table in (control, indexed):
        evidence["setup"].append(sql(args.url, "SHOW CREATE TABLE %s" % sql_ident(table)))
        evidence["setup"].append(sql(args.url, "SHOW INDEX FROM %s" % sql_ident(table)))

    # Narrow predicates allow ordinary time/field pruning; broad query is the residual counterexample.
    scopes = {
        "broad": "",
        "narrow": " WHERE ts >= '1970-01-01 00:00:00' AND ts < '1970-01-01 00:00:02' AND service = 'api'",
    }
    for label, predicate in PATTERNS.items():
        for scope, where_extra in scopes.items():
            pair = {}
            for role, table in (("control", control), ("indexed", indexed)):
                where = " WHERE " + predicate[0] + (" AND " + where_extra[7:] if where_extra else "")
                query = 'SELECT "id" FROM %s%s ORDER BY "id"' % (sql_ident(table), where)
                count_query = "SELECT count(*) FROM %s%s" % (sql_ident(table), where)
                explain = sql(args.url, "EXPLAIN VERBOSE " + query)
                analyze = sql(args.url, "EXPLAIN ANALYZE VERBOSE " + query)
                actual = sql(args.url, query)
                count = sql(args.url, count_query)
                pair[role] = {"query": actual, "count": count, "explain_verbose": explain,
                              "explain_analyze_verbose": analyze,
                              "scan_metrics": explain_scan_metrics(analyze),
                              "ids": [row[0] for row in rows_of(actual) if row],
                              "count_rows": rows_of(count)}
            expected = [i for i in range(ROWS)
                        if predicate[1](make_values(i)[3]) and
                        (scope == "broad" or (i < 2000 and i % 5 != 0))]
            for role in ("control", "indexed"):
                got = pair[role]["ids"]
                if got != expected:
                    raise ProbeError("%s/%s/%s ordered IDs differ from Python oracle (got=%d expected=%d)" % (label, scope, role, len(got), len(expected)))
            for role in ("control", "indexed"):
                count_rows = pair[role]["count_rows"]
                if len(count_rows) != 1 or int(count_rows[0][0]) != len(expected):
                    raise ProbeError("%s/%s/%s count differs from oracle" % (label, scope, role))
            if pair["control"]["ids"] != pair["indexed"]["ids"]:
                raise ProbeError("indexed and control results differ for %s/%s" % (label, scope))
            if label == "literal_regex_operator":
                for role in ("control", "indexed"):
                    like_ids = evidence["queries"]["like/" + scope][role]["ids"]
                    if pair[role]["ids"] != like_ids:
                        raise ProbeError("literal regex and LIKE results differ for %s/%s" % (scope, role))
            evidence["queries"][label + "/" + scope] = {"predicate": predicate[0], "scope": scope,
                "expected_count": len(expected), "control": pair["control"], "indexed": pair["indexed"]}

    encoded = json.dumps(evidence, ensure_ascii=False, indent=2)
    path = os.path.join(args.out, "summary.json")
    with open(path, "w", encoding="utf-8") as output:
        output.write(encoded + "\n")
    print(json.dumps({"status": "ok", "summary": path, "tables": evidence["tables"],
                      "rows_inserted": ROWS, "query_cases": len(evidence["queries"]),
                      "counts": {key: value["expected_count"] for key, value in evidence["queries"].items()}}, indent=2))


if __name__ == "__main__":
    try:
        main()
    except ProbeError as exc:
        print("rei: ERROR: %s" % exc, file=sys.stderr)
        sys.exit(1)
