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

"""Classifies how a case's `.result` differs between two trees.

A native run is expected to change how a result is *presented*: the native route names a computed
column after its own tree instead of after the expression, and it orders columns and rows itself.
Those changes keep every row, so they are reviewable in bulk. A changed set of rows is a different
answer, and each one has to be read.

The classification is deliberately narrow:

- `presentation-only`: every table holds the same multiset of rows once the column order is set
  aside and numerically equal cells (`31` and `31.0`, `NaN` and `NaN`) count as equal. Headers are
  compared too, so a renamed column on its own lands here.
- `plan-text`: only `EXPLAIN`/`ANALYZE` output changed. A native plan is a different tree, so its
  rows are expected to differ, and they are not an answer.
- `different-rows`: a table of values differs. Printed with the cells that appear on one side only.

Nothing here decides whether a difference is acceptable - it says which differences need a reader.
"""

import argparse
import json
import math
import re
import sys
from pathlib import Path

# A printed table row: `| a | b | c |`.
ROW = re.compile(r"^\s*\|(.*)\|\s*$")


def tables(text: str) -> list[list[tuple[str, ...]]]:
    """Every table in `text`, as its rows (header included)."""
    out = []
    rows: list[tuple[str, ...]] = []
    in_table = False
    for line in text.splitlines():
        stripped = line.rstrip()
        if not stripped.startswith(("|", "+")):
            if in_table and rows:
                out.append(rows)
            rows = []
            in_table = False
            continue
        in_table = True
        match = ROW.match(stripped)
        if match:
            cells = tuple(cell.strip() for cell in match.group(1).split("|"))
            rows.append(cells)
    if in_table and rows:
        out.append(rows)
    return out


def cell(text: str) -> tuple[str, object]:
    """A cell as a value, so `31` and `31.0` - and `NaN` and `NaN` - compare equal."""
    try:
        value = float(text)
    except ValueError:
        return ("text", text)
    return ("number", 0.0 if math.isnan(value) else value)


def row_values(row: tuple[str, ...]) -> tuple:
    """A row as the multiset of its cells, which sets the column order aside."""
    return tuple(sorted(cell(c) for c in row))


def is_plan_table(table: list[tuple[str, ...]]) -> bool:
    """Whether the table is `EXPLAIN`/`ANALYZE` output: headers `plan`, `plan_type`, `plan_`, ..."""
    return bool(table) and bool(table[0]) and any(name.startswith("plan") for name in table[0])


def compare(baseline: str, candidate: str) -> dict:
    baseline_tables = tables(baseline)
    candidate_tables = tables(candidate)

    # Align tables by position; a differing count is itself a difference to report.
    problems = []
    if len(baseline_tables) != len(candidate_tables):
        problems.append(
            f"table count differs: baseline={len(baseline_tables)}"
            f" candidate={len(candidate_tables)}"
        )

    plan_changed = False
    for index, (old, new) in enumerate(zip(baseline_tables, candidate_tables)):
        if len(old) == len(new) and sorted(map(row_values, old[1:])) == sorted(
            map(row_values, new[1:])
        ):
            continue
        if is_plan_table(old):
            plan_changed = True
            continue
        old_rows = sorted(row_values(row) for row in old[1:])
        new_rows = sorted(row_values(row) for row in new[1:])
        problems.append(
            {
                "table": index,
                "baseline_header": old[0] if old else (),
                "candidate_header": new[0] if new else (),
                "only_in_baseline": [row for row in old_rows if row not in new_rows][:6],
                "only_in_candidate": [row for row in new_rows if row not in old_rows][:6],
                "data_rows": {"baseline": len(old) - 1, "candidate": len(new) - 1},
            }
        )

    if problems:
        classification = "different-rows"
    elif plan_changed:
        classification = "plan-text"
    else:
        classification = "presentation-only"
    return {"classification": classification, "problems": problems}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--baseline-root",
        required=True,
        help="a checkout at the base revision, whose committed `.result` files are the baseline",
    )
    parser.add_argument(
        "--candidate-root",
        default=".",
        help="the tree holding the run's `.result` files (default: the current directory)",
    )
    parser.add_argument(
        "--case-dir",
        default="tests/cases/standalone",
        help="the cases inside both roots; the standalone suite is the one that runs natively",
    )
    parser.add_argument("--report", help="write the JSON report here")
    parser.add_argument(
        "--fail-on-different",
        action="store_true",
        help="exit non-zero when any case has different rows",
    )
    args = parser.parse_args()

    baseline_root = Path(args.baseline_root) / args.case_dir
    candidate_root = Path(args.candidate_root) / args.case_dir
    results = []
    counts = {"presentation-only": 0, "plan-text": 0, "different-rows": 0, "missing": 0}
    for sql in sorted(candidate_root.rglob("*.sql")):
        case = sql.relative_to(candidate_root)
        candidate = sql.with_suffix(".result")
        baseline = (baseline_root / case).with_suffix(".result")
        if not baseline.is_file() or not candidate.is_file():
            counts["missing"] += 1
            continue
        outcome = compare(
            baseline.read_text(encoding="utf-8", errors="replace"),
            candidate.read_text(encoding="utf-8", errors="replace"),
        )
        counts[outcome["classification"]] += 1
        outcome["case"] = str(case)
        results.append(outcome)

    for outcome in results:
        if outcome["classification"] != "different-rows":
            continue
        print(f"\n=== {outcome['case']} ===")
        for problem in outcome["problems"]:
            if isinstance(problem, str):
                print(f"  {problem}")
                continue
            print(f"  table {problem['table']}: {problem['data_rows']}")
            for row in problem["only_in_baseline"]:
                print(f"    only baseline:  {row}")
            for row in problem["only_in_candidate"]:
                print(f"    only candidate: {row}")

    print(
        f"\npresentation-only={counts['presentation-only']}"
        f" plan-text={counts['plan-text']}"
        f" different-rows={counts['different-rows']}"
        f" skipped={counts['missing']}"
    )
    if args.report:
        Path(args.report).write_text(json.dumps(results, indent=2) + "\n")

    if args.fail_on_different and counts["different-rows"]:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
