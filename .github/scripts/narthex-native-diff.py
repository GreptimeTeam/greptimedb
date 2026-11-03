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

"""Classifies the difference between a case's `.result` and its `.native.result`.

A native run is expected to change how a result is *presented*: the native route names a computed
column after its own tree instead of after the expression, and it orders columns and rows itself.
Those changes keep every row, so they are reviewable in bulk. A changed set of rows is a different
answer, and each one has to be looked at.

The classification is deliberately narrow:

- `presentation-only`: the two files hold the same multiset of table rows once the header and the
  column order are set aside. Names are compared too, so a renamed header alone is presentation.
- `different-rows`: the multisets differ. Printed with the rows that appear on one side only.

Nothing here decides whether a difference is acceptable - it says which differences need a reader.
"""

import argparse
import json
import re
import sys
from pathlib import Path

SEPARATOR = re.compile(r"^\s*[+|].*[+]\s*$|^\s*\+[-+]+\+\s*$")
# A printed table row: `| a | b | c |`.
ROW = re.compile(r"^\s*\|(.*)\|\s*$")


def tables(text: str) -> list[list[tuple[str, ...]]]:
    """Every table in `text`, as its data rows (header and separator dropped)."""
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
        elif stripped.startswith("+"):
            # A separator ends the header; the rows after it are data.
            continue
    if in_table and rows:
        out.append(rows)
    return out


def data_rows(table: list[tuple[str, ...]]) -> list[tuple[str, ...]]:
    """A table's data rows: everything except its header row."""
    return table[1:] if table else []


def compare(legacy: str, native: str) -> dict:
    legacy_tables = tables(legacy)
    native_tables = tables(native)

    # Align tables by position; a differing count is itself a difference to report.
    problems = []
    if len(legacy_tables) != len(native_tables):
        problems.append(
            f"table count differs: legacy={len(legacy_tables)} native={len(native_tables)}"
        )

    for index, (old, new) in enumerate(zip(legacy_tables, native_tables)):
        old_rows = sorted(data_rows(old))
        new_rows = sorted(data_rows(new))
        if old_rows == new_rows:
            continue
        only_old = [r for r in old_rows if r not in new_rows]
        only_new = [r for r in new_rows if r not in old_rows]
        problems.append(
            {
                "table": index,
                "legacy_header": old[0] if old else (),
                "native_header": new[0] if new else (),
                "only_in_legacy": only_old[:6],
                "only_in_native": only_new[:6],
                "counts": {"legacy": len(old_rows), "native": len(new_rows)},
            }
        )

    return {
        "classification": "presentation-only" if not problems else "different-rows",
        "problems": problems,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case-dir", default="tests/cases")
    parser.add_argument("--result-extension", default="native.result")
    parser.add_argument(
        "--native-root",
        help="directory holding the run's outputs when they are not beside the cases",
    )
    parser.add_argument(
        "--legacy-root",
        help="directory holding the committed `.result` files, when the run's tree holds only"
        " the statements and the native outputs",
    )
    parser.add_argument("--report", help="write the JSON report here")
    parser.add_argument(
        "--fail-on-different",
        action="store_true",
        help="exit non-zero when any case has different rows",
    )
    args = parser.parse_args()

    case_dir = Path(args.case_dir)
    results = []
    counts = {"presentation-only": 0, "different-rows": 0, "missing": 0}
    for sql in sorted(case_dir.rglob("*.sql")):
        native = sql.with_suffix(f".{args.result_extension}")
        if args.native_root:
            native = Path(args.native_root) / sql.with_suffix(f".{args.result_extension}").relative_to(case_dir)
        legacy = sql.with_suffix(".result")
        if args.legacy_root:
            legacy = Path(args.legacy_root) / sql.with_suffix(".result").relative_to(case_dir)
        if not native.is_file() or not legacy.is_file():
            counts["missing"] += 1
            continue
        outcome = compare(
            legacy.read_text(encoding="utf-8", errors="replace"),
            native.read_text(encoding="utf-8", errors="replace"),
        )
        counts[outcome["classification"]] += 1
        outcome["case"] = str(sql)
        results.append(outcome)

    for outcome in results:
        if outcome["classification"] == "different-rows":
            print(f"\n=== {outcome['case']} ===")
            for problem in outcome["problems"]:
                if isinstance(problem, str):
                    print(f"  {problem}")
                    continue
                print(f"  table {problem['table']}: {problem['counts']}")
                for row in problem["only_in_legacy"]:
                    print(f"    only legacy: {row}")
                for row in problem["only_in_native"]:
                    print(f"    only native: {row}")

    print(
        f"\npresentation-only={counts['presentation-only']}"
        f" different-rows={counts['different-rows']} skipped={counts['missing']}"
    )
    if args.report:
        Path(args.report).write_text(json.dumps(results, indent=2) + "\n")

    if args.fail_on_different and counts["different-rows"]:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
