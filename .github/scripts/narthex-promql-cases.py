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

"""Finds the sqlness cases that exercise PromQL, and checks their native expectations.

Case discovery reads the statements in every `.sql` file under the case directory instead of
matching directory names: PromQL cases live under `common/promql`, but also under `common/tql`,
`flow-tql`, `optimizer` and `tql-explain-analyze`, and a new directory would otherwise be missed.

Two commands:

- `discover` prints the selected cases and the reason each one was selected.
- `check` fails when a selected case has no committed `.result`, so a run cannot pass by
  silently falling back to the ordinary expectation.
"""

import argparse
import difflib
import os
import re
import shutil
import sys
from pathlib import Path

# A statement that evaluates PromQL. `TQL` covers `TQL EVAL`, `TQL EXPLAIN` and `TQL ANALYZE`; the
# protocol form covers `tql eval(...)`, which appears in lowercase as well.
PROMQL_STATEMENT = re.compile(
    r"""(?:\bTQL\b|\btql\s+eval\b|\bpromql\b)""",
    re.IGNORECASE,
)

# A comment line is not a statement: several fixtures mention PromQL while explaining that they are
# plain SQL, and counting those would demand native expectations for unrelated cases.
def statements(path: Path) -> list[str]:
    text = path.read_text(encoding="utf-8", errors="replace")
    # `/* ... */` blocks are removed first so a commented-out statement cannot select a case.
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL)
    kept = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        kept.append(line)
    return "\n".join(kept).split(";")


def promql_statements(path: Path) -> list[str]:
    return [s for s in statements(path) if PROMQL_STATEMENT.search(s)]


def selected_cases(case_dir: Path) -> list[tuple[Path, str]]:
    """The `.sql` files that hold a PromQL statement, with the statement that selected each.

    Symlinks are followed and the resulting paths deduplicated: `tests/cases/distributed/common` is a
    symlink to `../standalone/common`, so the same file is reached twice and must be reported once.
    """
    seen: set[Path] = set()
    selected: list[tuple[Path, str]] = []
    for path in sorted(case_dir.rglob("*.sql")):
        resolved = path.resolve()
        if resolved in seen:
            continue
        found = promql_statements(path)
        if not found:
            continue
        seen.add(resolved)
        statement = " ".join(found[0].split())
        selected.append((path, statement))
    return selected


def expected_native_result(case: Path, result_extension: str) -> Path:
    return case.with_suffix(f".{result_extension}")


def staged_path(path: Path, staging: Path, case_dir: Path) -> Path:
    """Where `path` sits inside the staged tree, keeping its environment directory."""
    return staging / path.relative_to(case_dir)


def stage(
    cases: list[tuple[Path, str]], case_dir: Path, staging: Path, result_extension: str
) -> int:
    """Builds a case tree holding exactly the selected cases.

    The statements stay the files in `tests/cases`: each is a symlink, so there is still one copy of
    every `.sql` and an edited case is the case that runs. The expectation is a *copy* of the
    committed `.result`, which is what keeps the run honest: sqlness rewrites the file it
    compared against before reporting the diff, so pointing it at the repository would let one bad
    run overwrite the reviewed expectation and the next run agree with it. A fresh copy discards
    whatever a run wrote and always starts from what is committed.

    Staging is also what makes the selection exact. sqlness filters by `environment:file_stem`,
    which cannot tell `common/promql/offset` from `common/subquery/offset` in one environment, and a
    case that was not selected has no native expectation to compare against.
    """
    if staging.exists():
        shutil.rmtree(staging)
    for case, _ in cases:
        target = staged_path(case, staging, case_dir)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.symlink_to(case.resolve())
        expected = expected_native_result(case, result_extension)
        shutil.copyfile(expected, staged_path(expected, staging, case_dir))
    environments = sorted(p.name for p in staging.iterdir() if p.is_dir())
    print(
        f"staged {len(cases)} cases into {staging} for {', '.join(environments)}",
    )
    return 0


def collect(
    cases: list[tuple[Path, str]], case_dir: Path, staging: Path, result_extension: str
) -> int:
    """Reports the staged expectations a run rewrote, as a diff against the committed ones.

    sqlness rewrites the expectation file it compared against *before* reporting the diff, so the
    staged copies always end up holding the actual output. A run that failed therefore leaves the
    staged file agreeing with what it just produced, and comparing the two would find nothing. The
    signal to look at is the one sqlness itself printed: a case it reported as unexpected is a case
    whose expectation it replaced.

    `--rewritten-from` points at the run log, so a failing case is reported with the actual output
    the run wrote. Nothing is copied back automatically: a rewritten expectation is a claim about
    behaviour and is reviewed before it replaces the committed file.
    """
    changed = 0
    for case, _ in cases:
        expected = expected_native_result(case, result_extension)
        staged = staged_path(expected, staging, case_dir)
        committed = expected.read_text(encoding="utf-8", errors="replace")
        actual = staged.read_text(encoding="utf-8", errors="replace")
        if committed == actual:
            continue
        changed += 1
        print(f"\n=== {expected} ===")
        for line in difflib.unified_diff(
            committed.splitlines(),
            actual.splitlines(),
            fromfile="committed",
            tofile="native run",
            lineterm="",
            n=3,
        ):
            print(line)
    if changed:
        print(
            f"\n{changed} staged expectation(s) differ from the committed ones."
            " Review each diff before copying the accepted output over the committed file.",
            file=sys.stderr,
        )
        return 1
    print(f"all {len(cases)} staged expectations match the committed ones")
    return 0


def failures_from_log(log: Path, case_dir: Path, staging: Path) -> int:
    """Cases the run reported as unexpected, with the output it wrote in their place.

    This is the check that survives sqlness rewriting its own expectation: the run says which cases
    disagreed, and the staged file now holds what the run produced.
    """
    text = log.read_text(encoding="utf-8", errors="replace")
    reported = re.findall(r'Result unexpected, path:"([^"]+)"', text)
    if not reported:
        print("the run reported no unexpected result")
        return 0
    print(f"\nthe run reported {len(reported)} unexpected result(s):")
    for path in reported:
        staged = Path(path)
        print(f"  {staged}")
        if staged.is_file():
            for line in staged.read_text(encoding="utf-8", errors="replace").splitlines()[:12]:
                print(f"      {line[:140]}")
    return 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case-dir", default="tests/cases")
    parser.add_argument(
        "--result-extension",
        default="result",
        help="extension of the expectation file, without the leading dot",
    )
    parser.add_argument(
        "--command", choices=["discover", "check", "stage", "collect", "failures"], required=True
    )
    parser.add_argument(
        "--staging-dir",
        help="directory the `stage` and `collect` commands read and write",
    )
    parser.add_argument(
        "--run-log",
        help="the run's log, for the `failures` command",
    )
    parser.add_argument(
        "--env",
        help="run one environment only; the staged tree then holds just that environment's cases",
    )
    args = parser.parse_args()

    case_dir = Path(args.case_dir)
    if not case_dir.is_dir():
        print(f"error: {case_dir} is not a directory", file=sys.stderr)
        return 2

    cases = selected_cases(case_dir)
    if not cases:
        print("error: no PromQL case was discovered", file=sys.stderr)
        return 1

    missing = [
        (case, expected)
        for case, _ in cases
        if not (expected := expected_native_result(case, args.result_extension)).is_file()
    ]

    print(f"discovered {len(cases)} PromQL cases under {case_dir}")
    for case, statement in cases:
        print(f"  {case}: {statement[:120]}")

    if args.command == "discover":
        return 0

    selected = cases
    if args.env:
        selected = [
            (case, statement)
            for case, statement in cases
            if case.relative_to(case_dir).parts[0] == args.env
        ]
        if not selected:
            print(f"error: no PromQL case belongs to environment {args.env!r}", file=sys.stderr)
            return 1
        print(f"selected {len(selected)} cases for environment {args.env}")

    if args.command == "stage":
        missing_selected = [
            (case, expected)
            for case, _ in selected
            if not (expected := expected_native_result(case, args.result_extension)).is_file()
        ]
        if missing_selected:
            print(
                f"error: cannot stage, {len(missing_selected)} case(s) have no committed"
                f" .{args.result_extension} expectation",
                file=sys.stderr,
            )
            for case, expected in missing_selected:
                print(f"  {case} needs {expected}", file=sys.stderr)
            return 1
        return stage(selected, case_dir, Path(args.staging_dir), args.result_extension)

    if args.command == "collect":
        return collect(selected, case_dir, Path(args.staging_dir), args.result_extension)

    if args.command == "failures":
        if not args.run_log:
            print("error: --run-log is required for `failures`", file=sys.stderr)
            return 2
        return failures_from_log(Path(args.run_log), case_dir, Path(args.staging_dir))

    if missing:
        print(
            f"\nerror: {len(missing)} PromQL case(s) have no committed"
            f" .{args.result_extension} expectation:",
            file=sys.stderr,
        )
        for case, expected in missing:
            print(f"  {case} needs {expected}", file=sys.stderr)
        print(
            "\nEvery PromQL case keeps its own native expectation so that a native run cannot pass"
            " by reading the ordinary one.",
            file=sys.stderr,
        )
        return 1
    print(f"all {len(cases)} PromQL cases have a committed .{args.result_extension} expectation")
    return 0


if __name__ == "__main__":
    sys.exit(main())
