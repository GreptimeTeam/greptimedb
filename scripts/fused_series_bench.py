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

"""End-to-end benchmark driver for the fused native-PromQL range-function path.

Reproduces the measurements in `docs/fused-series-report.md`: one seeded table, one
standalone server per arm, the same four query shapes run against each arm, and the
per-iteration timings plus the fused-hit counts written to JSON.

Three subcommands, so seeding, measuring and checking stay separable:

    scripts/fused_series_bench.py seed --data-home DIR
    scripts/fused_series_bench.py run --data-home DIR --bin PATH --arm off \
        --out FILE --log FILE --results DIR
    scripts/fused_series_bench.py compare DIR/cC-r1-L-C.json DIR/cC-r1-F-C.json

`seed` writes the fixture once. `run` starts one server over that data directory, runs
every case, and writes a JSON record; with `--results` it also writes the complete
correctness-pass result of each case, which `compare` then checks across arms, field by
field and outside the timing loop. An arm is one `(binary, flag)` pair; the driver does
not build binaries, so the caller decides which commit each arm is, and a crossed order
(L->N->F, N->F->L, F->L->N) is just three invocations per arm.

## Fixture, exactly as measured

The values below are what `docs/fused-series-report.md` reports; changing any of them
invalidates a comparison against that report. They are what the seeded
`~/.cache/fused-scale/data-seed` directory holds, and `seed` refuses to add to a
directory that already holds a different number of rows.

| Property | Value |
| --- | --- |
| Table | `vmbench`, `ENGINE=mito` |
| Tags (primary key, in order) | `host`, `instance`, `job`, `l1` |
| Field | `metric_value` DOUBLE |
| Time index | `ts` TIMESTAMP(3) |
| Series | 400: `host=h0..h399`, `instance=i0..i3` (100 hosts each), `job=j`, `l1=l` |
| Samples per series | 2 880 |
| Sample interval | 15 s |
| Seed anchor `START_MS` | `1700000000000` ms (no sample sits here) |
| First / last sample | `1700000015000` / `1700043200000` ms |
| Span | 43 185 s (11 h 59 m 45 s) |
| Sample k (0-based) | at `FIRST_SAMPLE_MS + k * 15000` |
| Value | `(k + 1) * 15 * (host + 1)`, strictly rising |
| Counter resets | **0** (verified: every step is +15 x (host + 1)) |
| NULL values | **0** |
| Rows | 400 x 2 880 = 1 152 000 |
| Storage partitions | 20 (mito default, measured on the seeded directory) |

A strictly rising value is a deliberate choice: a `rate` over it needs no counter
correction, so any cost that `rate` shows is the fixed per-window work plus whatever the
kernel spends looking for resets that are not there. That is exactly the cost this work
removes, and it is why a regressing shape cannot be blamed on reset arithmetic. A fixture
with resets is covered by the unit and integration tests instead, where the expected value
can be computed by hand.

## Two ranges, named apart

The **data range** (`FIRST_SAMPLE_MS .. LAST_SAMPLE_MS`) and the **query range**
(`QUERY_START_S .. QUERY_END_S`, in seconds, which is what `TQL EVAL` takes) are
different things and one interval apart on purpose:

- the first row is at `START_MS + 1 interval`, not at `START_MS`, so the fixture keeps
  `SAMPLE_COUNT` samples per series *inside* the query range;
- the query starts at `START_MS` (one interval before the first row), so the first grid
  point of every case has nothing behind its window and returns NaN, which the response
  drops. That is how the recorded measurements were taken, so it is kept.

`seed` and `run` both read `count(*)`, `min(ts)` and `max(ts)` back from the server and
fail unless they equal `ROW_COUNT`, `FIRST_SAMPLE_MS` and `LAST_SAMPLE_MS`.

## Cases

Every case is `TQL EVAL (start, end, '<step>') sum by (instance) (<expr>)` over the
seeded span, with the window given as a PromQL range in **seconds**.

| Name | Span | Step | Window | Grid points | Purpose |
| --- | --- | --- | --- | ---: | --- |
| `S` | 30 min | 1 s | 5 min | 1 801 | fine step, narrow window, short span |
| `G` | 2 h | 1 s | 5 min | 7 201 | fine step, narrow window, long span |
| `W` | 2 h | 1 s | 1 h | 7 201 | fine step, wide window |
| `C` | 2 h | 1 min | 5 min | 121 | the original business shape |
| `control` | 2 h | 1 s | 5 min | 7 201 | `max_over_time`, which never fuses |

`S` and `G` differ only in span, so the pair separates "more grid points" from "more
samples per window"; `G` and `W` differ only in window, so the pair isolates the window
width. `C` is the shape the original query actually has.

## What the record holds

Per case, per iteration: wall-clock milliseconds, output row count, response bytes, and
the number of `Fused series selection succeeded` lines the server logged **during that
iteration**, read from the byte offset the log had when the query started. A correctness
pass runs before the warmup, its result is what `--results` saves, and its hit count is
recorded apart from the warmup and timed ones.

Every execution — correctness, every warmup, every timed iteration — is checked against an
explicit expected hit count derived from the case definition: `fuses: true` means exactly
one selection on a flag-on arm and zero on a flag-off arm; `fuses: false` means zero
always. A mismatch stops the run with the case, the phase, the expected count and the
observed one. Counting only the correctness pass let a warmup or a timed iteration quietly
fall back to the ordinary plan while the timings still looked plausible.

## Comparing two arms

`--results DIR` makes `run` save the complete correctness-pass result of each case as
`<record stem>-<case>.json`: the column schema, every row, the label tuple, the timestamp
and the value. Only the correctness pass is saved, never a timed iteration, so the saved
file is never a fragment of a measurement. `compare` then reads the saved results of one
case for two or more arms and checks, in this order, reporting the first difference:

1. schema: column names and data types, in order;
2. labels: the full label tuple of every row;
3. timestamps: the `ts` of every row;
4. row count (an explicit check of its own — two lists of different length are never
   zipped, so a missing row can never hide behind a matching prefix);
5. values: `abs(a - b) <= 1e-9 * max(1, abs(b))`, with NaN matching NaN, infinities
   having to match by sign, and finite-vs-non-finite treated as a difference.

Which column is which is read from the saved schema rather than assumed: a
`Timestamp*` column is the timestamp, a numeric column is a value, and every other
column is a label. A result with no timestamp or no value column is refused, because
comparing it would check nothing.

## Traps this driver encodes

- `TQL EVAL (start, end, step)` takes its start and end in **seconds**, like the PromQL
  API, while the timestamps it returns are in **milliseconds** and `INSERT` literals are
  milliseconds. Passing milliseconds to `EVAL` asks for a range about 50 000 years in the
  future: every query returns zero rows with a valid schema, and `SELECT count(*)` still
  looks healthy. See `QUERY_START_S`, which is the only place the conversion happens.
- `region`, `zone` and `mode` are SQL reserved words and cannot be column names.
- `POST /v1/sql` needs form encoding; a JSON body is rejected with 415.
- Two servers need disjoint ports (`--http-addr`, `--grpc-bind-addr`, `--mysql-addr`,
  `--postgres-addr`), or they collide on the defaults. The defaults below sit inside the
  15000-15199 band this repository's benchmark runs are allowed to bind; pass `--http`
  and friends to move them.
"""

import argparse
import json
import math
import os
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

TABLE = "vmbench"
TAG_COLUMNS = ["host", "instance", "job", "l1"]
FIELD = "metric_value"
TIME = "ts"

# The seeded fixture, exactly as measured.
HOSTS = 400
SAMPLE_COUNT = 2_880
SAMPLE_INTERVAL_SECS = 15
# The seed anchor. It is *not* the first sample: sample k (0-based) is written at
# `FIRST_SAMPLE_MS + k * SAMPLE_INTERVAL_MS`, so the anchor is exactly one interval
# before the first row.
START_MS = 1_700_000_000_000

SAMPLE_INTERVAL_MS = SAMPLE_INTERVAL_SECS * 1000
ROW_COUNT = HOSTS * SAMPLE_COUNT

# The data range, in milliseconds: where the seeded rows actually sit.
FIRST_SAMPLE_MS = START_MS + SAMPLE_INTERVAL_MS
LAST_SAMPLE_MS = START_MS + SAMPLE_COUNT * SAMPLE_INTERVAL_MS

# The query range, in seconds, which is what `TQL EVAL` takes. It deliberately starts at
# the anchor rather than at the first sample: the first grid point therefore has no row
# inside its window and returns NaN, which is how the recorded measurements were taken.
# `QUERY_END_S` is the end of the longest case; every case's own end comes from
# `case_window()`. Naming the two ranges apart is the point: `START_MS` is a millisecond
# data anchor and must never be handed to `EVAL`, and `LAST_SAMPLE_MS` is a millisecond
# data bound that must never be mistaken for the grid end.
QUERY_START_S = START_MS // 1000

# The measured cases. `step` and the window are PromQL arguments, so they are seconds.
CASES = [
    {
        "name": "S",
        "expr": "rate({table}[5m])",
        "span_minutes": 30,
        "step": "1s",
        "window_seconds": 300,
        "fuses": True,
    },
    {
        "name": "G",
        "expr": "rate({table}[5m])",
        "span_minutes": 120,
        "step": "1s",
        "window_seconds": 300,
        "fuses": True,
    },
    {
        "name": "W",
        "expr": "rate({table}[1h])",
        "span_minutes": 120,
        "step": "1s",
        "window_seconds": 3_600,
        "fuses": True,
    },
    {
        "name": "C",
        "expr": "rate({table}[5m])",
        "span_minutes": 120,
        "step": "1m",
        "window_seconds": 300,
        "fuses": True,
    },
    {
        "name": "control",
        "expr": "max_over_time({table}[5m])",
        "span_minutes": 120,
        "step": "1s",
        "window_seconds": 300,
        "fuses": False,
    },
]

MAX_SPAN_MINUTES = max(case["span_minutes"] for case in CASES)
QUERY_END_S = QUERY_START_S + MAX_SPAN_MINUTES * 60

# The types `compare` treats as values; every other non-timestamp column is a label.
NUMERIC_TYPES = {
    "Float64",
    "Float32",
    "Int64",
    "Int32",
    "Int16",
    "Int8",
    "UInt64",
    "UInt32",
    "UInt16",
    "UInt8",
}


def log(message):
    print(message, flush=True)


def check_ranges():
    """Fails unless the range constants still describe the measured fixture.

    Cheap, runs before any server starts, and catches the one edit that would silently
    invalidate a comparison: mixing up the data range and the query range.
    """
    if (FIRST_SAMPLE_MS, LAST_SAMPLE_MS) != (1_700_000_015_000, 1_700_043_200_000):
        raise SystemExit(
            f"the data range is {FIRST_SAMPLE_MS}..{LAST_SAMPLE_MS} ms, but the measured "
            "fixture's first and last samples are 1700000015000..1700043200000 ms"
        )
    if LAST_SAMPLE_MS - FIRST_SAMPLE_MS != (SAMPLE_COUNT - 1) * SAMPLE_INTERVAL_MS:
        raise SystemExit(
            f"the data range spans {LAST_SAMPLE_MS - FIRST_SAMPLE_MS} ms, not the "
            f"{(SAMPLE_COUNT - 1) * SAMPLE_INTERVAL_MS} ms of {SAMPLE_COUNT} samples at "
            f"{SAMPLE_INTERVAL_MS} ms"
        )
    if QUERY_START_S * 1000 != START_MS:
        raise SystemExit(
            f"the query range starts at {QUERY_START_S}s, which is not the seed anchor "
            f"{START_MS} ms rounded down to seconds"
        )
    if QUERY_END_S * 1000 > LAST_SAMPLE_MS:
        raise SystemExit(
            f"the query range ends at {QUERY_END_S}s, after the data range's last sample "
            f"{LAST_SAMPLE_MS} ms; a case would ask for timestamps the fixture does not have"
        )
    for case in CASES:
        _, end = case_window(case)
        if end > QUERY_END_S:
            raise SystemExit(
                f"case {case['name']} ends at {end}s, past QUERY_END_S ({QUERY_END_S}s)"
            )


def check_fixture(where, count, first_ts, last_ts):
    """Fails unless the server's own count/min/max equal the fixture constants."""
    if count != ROW_COUNT:
        raise SystemExit(
            f"{where}: {TABLE} holds {count} rows, but the fixture has "
            f"{ROW_COUNT} ({HOSTS} series x {SAMPLE_COUNT} samples)"
        )
    if first_ts != FIRST_SAMPLE_MS or last_ts != LAST_SAMPLE_MS:
        raise SystemExit(
            f"{where}: {TABLE} spans {first_ts}..{last_ts} ms, but the fixture's first and "
            f"last samples are {FIRST_SAMPLE_MS}..{LAST_SAMPLE_MS} ms"
        )


def expected_hits(case, arm):
    """How many fused selections one execution of `case` must log on `arm`."""
    if not case["fuses"]:
        return 0
    return 1 if arm == "on" else 0


def check_hits(case, arm, phase, actual):
    """Fails unless one execution logged exactly the expected number of selections."""
    expected = expected_hits(case, arm)
    if actual != expected:
        raise SystemExit(
            f"case {case['name']} phase {phase}: expected {expected} fused selection(s) on "
            f"the flag-{arm} arm, the server logged {actual}"
        )
    return expected


class Server:
    """One standalone server over a data directory, on its own port set."""

    def __init__(self, binary, data_home, http, grpc, mysql, postgres, native, log_path):
        self.binary = binary
        self.data_home = data_home
        self.native = native
        self.http = http
        self.grpc = grpc
        self.mysql = mysql
        self.postgres = postgres
        self.log_path = log_path
        self.process = None
        self.log_file = None
        # Where the log was when the query started, so hits can be counted per query.
        self.log_offset = 0

    def __enter__(self):
        config = os.path.join(self.data_home, "..", f"fused-bench-arm-{int(self.native)}.toml")
        with open(config, "w") as handle:
            handle.write(
                'mode = "standalone"\n\n'
                "[logging]\n"
                'level = "debug"\n\n'
                "[query]\n"
                f"experimental_native_promql = {str(self.native).lower()}\n"
            )
        self.log_file = open(self.log_path, "w")
        self.process = subprocess.Popen(
            [
                self.binary,
                "standalone",
                "start",
                "--config-file",
                config,
                "--data-home",
                self.data_home,
                "--http-addr",
                f"127.0.0.1:{self.http}",
                "--grpc-bind-addr",
                f"127.0.0.1:{self.grpc}",
                "--mysql-addr",
                f"127.0.0.1:{self.mysql}",
                "--postgres-addr",
                f"127.0.0.1:{self.postgres}",
            ],
            stdout=self.log_file,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
        )
        deadline = time.time() + 600
        while time.time() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(f"the server exited early; see {self.log_path}")
            try:
                self.sql("SELECT 1", timeout=5)
                return self
            except Exception:
                time.sleep(1)
        raise RuntimeError(f"the server did not become ready; see {self.log_path}")

    def __exit__(self, *exc):
        if self.process is not None and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=120)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        if self.log_file is not None:
            self.log_file.close()

    def sql(self, statement, timeout=3_600):
        body = urllib.parse.urlencode({"sql": statement}).encode()
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.http}/v1/sql",
            data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
        return json.loads(raw.decode()), len(raw)

    def rows(self, statement, timeout=3_600):
        payload, _ = self.sql(statement, timeout=timeout)
        return flatten(payload)[1]

    def cpu_seconds(self):
        """Total CPU seconds this server has used on every thread so far.

        Wall-clock alone cannot separate "does less work" from "does the same work with
        more parallelism", and the two arms differ in exactly that way. Reading the
        process counters makes the distinction measurable instead of arguable.
        """
        with open(f"/proc/{self.process.pid}/stat") as handle:
            # The comm field is parenthesised and may contain spaces, so split after it.
            fields = handle.read().rsplit(") ", 1)[1].split()
        ticks = fields[11:13]  # utime, stime
        return sum(int(tick) for tick in ticks) / os.sysconf("SC_CLK_TCK")

    def begin_query(self):
        """Records where the log ends, so the next query's hits can be counted alone."""
        self.log_offset = os.path.getsize(self.log_path) if os.path.exists(self.log_path) else 0

    def hits_since(self):
        """The number of fused selections logged since `begin_query`."""
        if not os.path.exists(self.log_path):
            return 0
        with open(self.log_path, "rb") as handle:
            handle.seek(self.log_offset)
            tail = handle.read()
        return tail.count(b"Fused series selection succeeded")


def flatten(payload):
    """`(schema, rows)` of a SQL response, with every block folded into one list.

    A TQL response is a single block; folding keeps a multi-block response comparable
    instead of silently comparing only the first block.
    """
    schema = None
    rows = []
    for block in payload.get("output", []):
        records = block.get("records")
        if not records:
            continue
        columns = records["schema"]["column_schemas"]
        if schema is None:
            schema = columns
        elif columns != schema:
            raise SystemExit(
                "the response holds blocks with different schemas, so its rows cannot be "
                "compared as one result"
            )
        rows.extend(records["rows"])
    return schema or [], rows


def create_table(server):
    columns = ", ".join(f'"{tag}" STRING NULL' for tag in TAG_COLUMNS)
    primary_key = ", ".join(f'"{tag}"' for tag in TAG_COLUMNS)
    server.sql(
        f'CREATE TABLE IF NOT EXISTS "{TABLE}" ('
        f"{columns}, "
        f'"{FIELD}" DOUBLE NULL, '
        f'"{TIME}" TIMESTAMP(3) NOT NULL, '
        f'TIME INDEX ("{TIME}"), '
        f"PRIMARY KEY ({primary_key})"
        ") ENGINE=mito"
    )


def seed(data_home, binary, http, grpc, mysql, postgres):
    """Writes the fixture once; safe to re-run because it checks the row count first."""
    check_ranges()
    os.makedirs(data_home, exist_ok=True)
    log_path = os.path.join(data_home, "..", "fused-bench-seed.log")
    with Server(binary, data_home, http, grpc, mysql, postgres, False, log_path) as server:
        create_table(server)
        count, first_ts, last_ts = server.rows(
            f'SELECT count(*), min("{TIME}"), max("{TIME}") FROM "{TABLE}"'
        )[0]
        if count == ROW_COUNT:
            check_fixture("seed", count, first_ts, last_ts)
            log(f"the fixture is already seeded: {count} rows at {first_ts}..{last_ts} ms")
            return
        if count:
            raise SystemExit(
                f"{TABLE} holds {count} rows, not the {ROW_COUNT} of the fixture; use a "
                "fresh data directory instead of mixing shapes"
            )
        log(f"seeding {ROW_COUNT} rows into {data_home}")
        # One INSERT per host keeps each statement small enough for the HTTP endpoint while
        # writing every sample of that series in a single batch.
        for host in range(HOSTS):
            started = time.time()
            values = []
            for index in range(SAMPLE_COUNT):
                timestamp = FIRST_SAMPLE_MS + index * SAMPLE_INTERVAL_MS
                # Strictly rising, and different per host so a wrongly grouped series cannot hide.
                value = (index + 1) * SAMPLE_INTERVAL_SECS * (host + 1)
                values.append(
                    f"('h{host}', 'i{host % 4}', 'j', 'l', {value}, {timestamp})"
                )
            server.sql(
                f'INSERT INTO "{TABLE}" '
                f'("host", "instance", "job", "l1", "{FIELD}", "{TIME}") VALUES '
                + ", ".join(values),
                # A single 2 880-row insert of a series is the slowest statement here.
                timeout=3_600,
            )
            if host % 25 == 0 or host == HOSTS - 1:
                log(f"  {host + 1}/{HOSTS} series ({time.time() - started:.1f}s each)")
        count, first_ts, last_ts = server.rows(
            f'SELECT count(*), min("{TIME}"), max("{TIME}") FROM "{TABLE}"'
        )[0]
        check_fixture("seed", count, first_ts, last_ts)
        log(f"seeded {count} rows at {first_ts}..{last_ts} ms")


def case_window(case):
    """The `(start, end)` of one case in seconds, which is what `TQL EVAL` takes."""
    return QUERY_START_S, QUERY_START_S + case["span_minutes"] * 60


def tql(case, aggregate=True):
    expr = case["expr"].format(table=TABLE)
    if aggregate:
        expr = f"sum by (instance) ({expr})"
    start, end = case_window(case)
    return f"TQL EVAL ({start}, {end}, '{case['step']}') {expr}"


def save_result(directory, stem, case, arm, commit, statement, schema, rows):
    """Saves one correctness-pass result, never overwriting an earlier one."""
    os.makedirs(directory, exist_ok=True)
    base = os.path.join(directory, f"{stem}-{case['name']}")
    path = f"{base}.json"
    suffix = 2
    while os.path.exists(path):
        path = f"{base}-{suffix}.json"
        suffix += 1
    with open(path, "w") as handle:
        json.dump(
            {
                "case": case["name"],
                "arm": arm,
                "commit": commit,
                "tql": statement,
                "schema": schema,
                "row_count": len(rows),
                "rows": rows,
            },
            handle,
        )
    return path


def value_mismatch(left, right):
    """`None` if the two value cells match, otherwise a short reason."""
    if left is None or right is None:
        return None if left is None and right is None else f"{left!r} vs {right!r} (one is NULL)"
    if isinstance(left, bool) or isinstance(right, bool):
        return None if left == right else f"{left!r} vs {right!r}"
    if not isinstance(left, (int, float)) or not isinstance(right, (int, float)):
        return None if left == right else f"{left!r} vs {right!r}"
    left_nan, right_nan = math.isnan(left), math.isnan(right)
    if left_nan or right_nan:
        if left_nan and right_nan:
            return None
        return f"{left!r} vs {right!r} (only one side is NaN)"
    left_inf, right_inf = math.isinf(left), math.isinf(right)
    if left_inf or right_inf:
        if not (left_inf and right_inf):
            return f"{left!r} vs {right!r} (one side is infinite, the other finite)"
        if (left > 0) != (right > 0):
            return f"{left!r} vs {right!r} (opposite infinities)"
        return None
    tolerance = 1e-9 * max(1.0, abs(right))
    if abs(left - right) > tolerance:
        return (
            f"{left!r} vs {right!r} (off by {abs(left - right):.3e}, "
            f"tolerance {tolerance:.3e})"
        )
    return None


def classify(schema):
    """Which columns are labels, which are the timestamp, which are values."""
    labels, times, values = [], [], []
    for index, column in enumerate(schema):
        if column["data_type"].startswith("Timestamp"):
            times.append(index)
        elif column["data_type"] in NUMERIC_TYPES:
            values.append(index)
        else:
            labels.append(index)
    return labels, times, values


def describe(result):
    """An arm's identity: both flag-on arms share `arm: on`, so name the file too."""
    return f"{result['stem']} [flag {result['arm']}, {result.get('commit', 'unknown')}]"


def first_difference(left_rows, right_rows, columns):
    """Index of the first row whose `columns` cells differ, or `None`.

    Only the common prefix is looked at: two lists of different length are never zipped,
    so the length difference is left to its own check, which reports it explicitly.
    """
    for index in range(min(len(left_rows), len(right_rows))):
        left = tuple(left_rows[index][column] for column in columns)
        right = tuple(right_rows[index][column] for column in columns)
        if left != right:
            return index, left, right
    return None


def compare_pair(baseline, other):
    """Checks `other` against `baseline`, in order, stopping at the first difference."""
    case = baseline["case"]
    left, right = describe(baseline), describe(other)

    # 1. schema: names and data types, in order.
    left_schema, right_schema = baseline["schema"], other["schema"]
    if len(left_schema) != len(right_schema):
        raise SystemExit(
            f"case {case}: schema differs: {left} has {len(left_schema)} columns, "
            f"{right} has {len(right_schema)}"
        )
    for index, (left_column, right_column) in enumerate(zip(left_schema, right_schema)):
        if left_column != right_column:
            raise SystemExit(
                f"case {case}: schema differs at column {index}: {left} has "
                f"{left_column['name']!r} {left_column['data_type']}, {right} has "
                f"{right_column['name']!r} {right_column['data_type']}"
            )
    labels, times, values = classify(left_schema)
    if not times or not values:
        raise SystemExit(
            f"case {case}: the result has no timestamp or no value column "
            f"({[column['data_type'] for column in left_schema]}); nothing to compare"
        )

    # 2. labels: the full label tuple of every row.
    diff = first_difference(baseline["rows"], other["rows"], labels)
    if diff:
        index, left_cells, right_cells = diff
        raise SystemExit(
            f"case {case}: labels differ at row {index}: {left} has {left_cells}, "
            f"{right} has {right_cells}"
        )

    # 3. timestamps.
    diff = first_difference(baseline["rows"], other["rows"], times)
    if diff:
        index, left_cells, right_cells = diff
        raise SystemExit(
            f"case {case}: timestamps differ at row {index}: {left} has {left_cells}, "
            f"{right} has {right_cells}"
        )

    # 4. row count, as its own failure rather than a truncated zip.
    if len(baseline["rows"]) != len(other["rows"]):
        raise SystemExit(
            f"case {case}: row count differs: {left} has {len(baseline['rows'])} rows, "
            f"{right} has {len(other['rows'])}"
        )

    # 5. values.
    for index, left_row in enumerate(baseline["rows"]):
        right_row = other["rows"][index]
        for column in values:
            reason = value_mismatch(left_row[column], right_row[column])
            if reason:
                where = ", ".join(
                    f"{left_schema[label]['name']}={left_row[label]!r}" for label in labels
                )
                stamp = ", ".join(
                    f"{left_schema[stamp_column]['name']}={left_row[stamp_column]}"
                    for stamp_column in times
                )
                raise SystemExit(
                    f"case {case}: value differs at row {index} ({where}; {stamp}) in column "
                    f"{left_schema[column]['name']!r}: {left} has {reason}"
                )
    return labels, times, values


def compare(args):
    """Checks the saved results of one case across arms."""
    results = []
    for path in args.results:
        with open(path) as handle:
            result = json.load(handle)
        for key in ("case", "arm", "schema", "rows"):
            if key not in result:
                raise SystemExit(
                    f"{path} is not a result file written by `run --results`: no {key!r}"
                )
        stem = os.path.basename(path)
        result["stem"] = stem[: -len(".json")] if stem.endswith(".json") else stem
        results.append(result)

    cases = sorted({result["case"] for result in results})
    if len(cases) != 1:
        raise SystemExit(f"the result files mix cases: {', '.join(cases)}")
    if args.case and args.case != cases[0]:
        raise SystemExit(f"asked to compare case {args.case}, but the files hold case {cases[0]}")
    statements = {result.get("tql") for result in results}
    if len(statements) != 1:
        raise SystemExit(
            "the result files hold different queries, so comparing them would be meaningless:\n  "
            + "\n  ".join(sorted(statements))
        )

    baseline = results[0]
    arms = ", ".join(describe(result) for result in results)
    log(f"case {cases[0]}: {len(results)} arm(s) {arms}, {len(baseline['rows'])} rows")
    for other in results[1:]:
        labels, times, values = compare_pair(baseline, other)
        log(
            f"  identical to {describe(baseline)}: {len(baseline['schema'])} columns "
            f"({len(labels)} label(s), {len(times)} timestamp(s), {len(values)} value(s)), "
            f"{len(baseline['rows'])} rows"
        )
    log(
        f"ok: case {cases[0]} is identical on {len(results)} arm(s): "
        f"{', '.join(result['stem'] for result in results)}"
    )


def run(args):
    """Runs every case against one arm and writes the JSON record."""
    check_ranges()
    with Server(
        args.bin,
        args.data_home,
        args.http,
        args.grpc,
        args.mysql,
        args.postgres,
        args.arm == "on",
        args.log,
    ) as server:
        # The fixture is checked before anything is timed, so no measurement can come from
        # a table that is not the one the record claims.
        count, first_ts, last_ts = server.rows(
            f'SELECT count(*), min("{TIME}"), max("{TIME}") FROM "{TABLE}"'
        )[0]
        check_fixture("run", count, first_ts, last_ts)
        log(
            f"fixture: {count} rows, {first_ts}..{last_ts} ms "
            f"(expected {ROW_COUNT} rows, {FIRST_SAMPLE_MS}..{LAST_SAMPLE_MS} ms)"
        )

        record = {
            "arm": args.arm,
            "bin": args.bin,
            "commit": args.commit,
            "build_profile": args.profile,
            "data_home": os.path.abspath(args.data_home),
            "fixture": {
                "table": TABLE,
                "tags": TAG_COLUMNS,
                "series": HOSTS,
                "samples_per_series": SAMPLE_COUNT,
                "sample_interval_seconds": SAMPLE_INTERVAL_SECS,
                "rows": ROW_COUNT,
                "start_ms": START_MS,
                "first_sample_ms": FIRST_SAMPLE_MS,
                "last_sample_ms": LAST_SAMPLE_MS,
                "query_start_s": QUERY_START_S,
                "observed_rows": count,
                "first_ts_ms": first_ts,
                "last_ts_ms": last_ts,
                "nulls": 0,
                "counter_resets": 0,
            },
            "warmup": args.warmup,
            "iterations": args.iterations,
            "cases": [],
        }

        wanted: set[str] = (
            {name.strip() for name in args.cases.split(",") if name.strip()}
            if args.cases
            else set()
        )
        for case in [c for c in CASES if not wanted or c["name"] in wanted]:
            statement = tql(case)
            query_start_s, query_end_s = case_window(case)
            # Correctness first, and its hits recorded apart from the timed ones. Its
            # result is the only one saved: a timed iteration must never be compared,
            # because a fragment of a measurement is not a result.
            server.begin_query()
            correctness_payload, _ = server.sql(statement)
            correctness_hits = server.hits_since()
            expected = check_hits(case, args.arm, "correctness", correctness_hits)
            schema, correctness_rows = flatten(correctness_payload)
            log(
                f"{case['name']}: {len(correctness_rows)} rows, {correctness_hits} hits "
                f"(correctness, expected {expected})"
            )
            if args.results:
                saved = save_result(
                    args.results,
                    args.results_stem,
                    case,
                    args.arm,
                    args.commit,
                    statement,
                    schema,
                    correctness_rows,
                )
                log(f"  saved the correctness result to {saved}")

            # Warmup, then the timed runs, each with its own hit count. The three kinds of hit
            # are kept apart so a total can never be mistaken for a per-iteration count, and
            # every single execution is held to the same expected count.
            warmup_hits = []
            for warmup in range(args.warmup):
                server.begin_query()
                server.sql(statement)
                hits = server.hits_since()
                check_hits(case, args.arm, f"warmup {warmup + 1}/{args.warmup}", hits)
                warmup_hits.append(hits)
            timed_hits = []
            for iteration in range(args.iterations):
                server.begin_query()
                cpu_before = server.cpu_seconds()
                started = time.perf_counter()
                payload, size = server.sql(statement)
                elapsed_ms = (time.perf_counter() - started) * 1000.0
                cpu_ms = (server.cpu_seconds() - cpu_before) * 1000.0
                hits = server.hits_since()
                check_hits(case, args.arm, f"iteration {iteration + 1}/{args.iterations}", hits)
                rows = sum(
                    len(block["records"]["rows"])
                    for block in payload.get("output", [])
                    if block.get("records")
                )
                timed_hits.append(hits)
                record["cases"].append(
                    {
                        "load1": os.getloadavg()[0],
                        "name": case["name"],
                        "expr": case["expr"],
                        "step": case["step"],
                        "span_minutes": case["span_minutes"],
                        "window_seconds": case["window_seconds"],
                        "tql": statement,
                        "query_start_s": query_start_s,
                        "query_end_s": query_end_s,
                        "rows": rows,
                        "bytes": size,
                        "ms": elapsed_ms,
                        "cpu_ms": cpu_ms,
                        "hits": hits,
                        "expected_hits": expected,
                    }
                )

            timed = [c for c in record["cases"] if c["name"] == case["name"]]
            record.setdefault("hits", []).append(
                {
                    "name": case["name"],
                    "fuses": case["fuses"],
                    "expected": expected,
                    "correctness": correctness_hits,
                    "warmup": warmup_hits,
                    "timed": timed_hits,
                }
            )
            log(
                f"  {case['name']}: {len(timed)} timed runs, {timed[0]['rows']} rows, "
                f"hits correctness={correctness_hits} warmup={sorted(set(warmup_hits))} "
                f"timed={sorted(set(timed_hits))} (expected {expected}), "
                f"min {min(c['ms'] for c in timed):.1f} ms"
            )

        if args.arm == "off" and any(c["hits"] for c in record["cases"]):
            raise SystemExit("the flag-off arm logged fused selections; it is not the baseline")

        with open(args.out, "w") as handle:
            json.dump(record, handle, indent=2)
        log(f"wrote {args.out}")


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--data-home", required=True, help="the standalone data directory")
    common.add_argument("--bin", required=True, help="the greptime binary to run")
    common.add_argument("--http", type=int, default=15090)
    common.add_argument("--grpc", type=int, default=15092)
    common.add_argument("--mysql", type=int, default=15094)
    common.add_argument("--postgres", type=int, default=15096)

    seed_parser = sub.add_parser("seed", parents=[common], help="write the fixture once")
    seed_parser.set_defaults(func=lambda a: seed(a.data_home, a.bin, a.http, a.grpc, a.mysql, a.postgres))

    run_parser = sub.add_parser("run", parents=[common], help="measure one arm")
    run_parser.add_argument("--arm", choices=["off", "on"], required=True)
    run_parser.add_argument("--out", required=True, help="the JSON record to write")
    run_parser.add_argument("--log", required=True, help="the server log to read hits from")
    run_parser.add_argument("--commit", default="unknown", help="the commit this binary is")
    run_parser.add_argument("--profile", default="nightly", help="the Cargo profile it was built with")
    run_parser.add_argument("--warmup", type=int, default=2)
    run_parser.add_argument("--iterations", type=int, default=9)
    run_parser.add_argument(
        "--cases",
        default="",
        help="comma-separated case names to run; empty runs every case",
    )
    run_parser.add_argument(
        "--results",
        default="",
        help="directory for the per-case correctness results `compare` reads",
    )
    run_parser.add_argument(
        "--results-stem",
        default="",
        help="prefix for the saved result names; defaults to the --out file name",
    )
    run_parser.set_defaults(func=run)

    compare_parser = sub.add_parser(
        "compare", help="check saved results of one case across arms"
    )
    compare_parser.add_argument(
        "results", nargs="+", help="the result files written by `run --results`"
    )
    compare_parser.add_argument("--case", default="", help="the case the files must hold")
    compare_parser.set_defaults(func=compare)

    args = parser.parse_args()
    if args.command == "run":
        if not args.results_stem:
            args.results_stem = os.path.basename(args.out)
            if args.results_stem.endswith(".json"):
                args.results_stem = args.results_stem[: -len(".json")]
    if args.command == "compare" and len(args.results) < 2:
        raise SystemExit("compare needs at least two result files, one per arm")
    args.func(args)


if __name__ == "__main__":
    sys.exit(main())
