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

"""Checks, per statement, which route actually answered a sqlness run.

A run that sets the native switch still falls back for a statement the native planner declines, and a
`.native.result` that passed says nothing about which route produced it. This reads the run's own
output and classifies each PromQL statement by the plan text it reports:

- `native`: the statement's plan holds `FusedSeries` (the fused native node);
- `legacy`: it holds a legacy evaluator (`PromInstantManipulateExec`, `PromRangeManipulateExec`, ...)
  and no fused node.

The classification is text from `TQL EXPLAIN`/`TQL ANALYZE`, which is the only externally visible
evidence that a statement used the native node; an ordinary PromQL statement reports no route marker
at all, and this tool reports that as `unreported` rather than guessing.

The input is a run log or a sqlness `.native.result`. Both quote plan text, so the same extraction
serves either.
"""

import argparse
import re
import sys
from pathlib import Path

FUSED = re.compile(r"FusedSeries(?:Exec)?\b")
# Every legacy evaluator that answers a PromQL statement in this tree.
LEGACY = re.compile(
    r"\b(?:PromInstantManipulateExec|PromRangeManipulateExec|PromSeriesDivideExec"
    r"|PromSeriesNormalizeExec|PromAggregateExec)\b"
)
# A statement line, followed by its output. The result file quotes the statement verbatim and the
# table below it holds the plan; a run log quotes both in the same order.
STATEMENT_LINE = re.compile(r"^\s*TQL\s+(?:EVAL|EXPLAIN|ANALYZE|ANALYSE)\b", re.IGNORECASE)


def blocks(text: str) -> list[tuple[str, str]]:
    """`(statement, its output)` for every PromQL statement in the text.

    A statement's output is everything up to the next statement, or up to the run summary that
    follows the last one.
    """
    current: str | None = None
    body: list[str] = []
    found: list[tuple[str, str]] = []
    for line in text.splitlines():
        if STATEMENT_LINE.match(line.strip()):
            if current is not None:
                found.append((current, "\n".join(body)))
            current = line.strip()
            body = []
            continue
        if current is not None:
            body.append(line)
    if current is not None:
        found.append((current, "\n".join(body)))
    return found


def classify(text: str) -> list[tuple[str, str]]:
    """`(statement, route)` for every statement whose output reports a plan."""
    results: list[tuple[str, str]] = []
    for statement, output in blocks(text):
        if FUSED.search(output):
            results.append((statement, "native"))
        elif LEGACY.search(output):
            results.append((statement, "legacy"))
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="run log or .native.result")
    parser.add_argument(
        "--require-native",
        action="store_true",
        help="fail when any reported statement fell back to the legacy route",
    )
    parser.add_argument(
        "--expect",
        help="a path filter; only statements quoting this text are required to be native",
    )
    args = parser.parse_args()

    text = Path(args.input).read_text(encoding="utf-8", errors="replace")
    results = classify(text)
    if not results:
        print(
            f"{args.input}: no statement reported its route."
            " A run only reports one through TQL EXPLAIN or TQL ANALYZE.",
            file=sys.stderr,
        )
        return 1

    counts: dict[str, int] = {}
    for statement, route in results:
        counts[route] = counts.get(route, 0) + 1
        print(f"{route:8} {statement[:160]}")
    print("summary: " + ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))

    if not args.require_native:
        return 0

    required = [(s, r) for s, r in results if not args.expect or args.expect in s]
    if not required:
        print(
            f"error: --require-native matched no statement (filter {args.expect!r})",
            file=sys.stderr,
        )
        return 1
    fell_back = [(s, r) for s, r in required if r != "native"]
    if fell_back:
        print(
            f"\nerror: {len(fell_back)} statement(s) required to be native fell back:",
            file=sys.stderr,
        )
        for statement, route in fell_back:
            print(f"  {route}: {statement[:160]}", file=sys.stderr)
        return 1
    print(f"\nall {len(required)} required statement(s) answered natively")
    return 0


if __name__ == "__main__":
    sys.exit(main())
