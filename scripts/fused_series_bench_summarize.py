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

"""Summarises `fused_series_bench.py` records into the report tables.

Every record file is named `<case>-r<round>-<arm>.json` and holds one case, so the
file name is the index. Nothing is discarded: the per-arm table prints the median,
minimum and maximum over all rounds and all timed iterations, the per-round table
prints each round's own median and its L/F and N/F ratio next to the pooled ones so a
lone lucky round cannot pass for a stable gain, and the JSON records keep every
individual timing.

    scripts/fused_series_bench_summarize.py RECORD_DIR
"""

import glob
import json
import os
import statistics
import sys

ARMS = ["L", "N", "F"]
ARM_LABEL = {
    "L": "flag off",
    "N": "flag on, rescan",
    "F": "flag on, indexed",
}


def median(values):
    return statistics.median(values) if values else float("nan")


def median_of(entries, field="ms"):
    """The median `field` of a group of timed iterations, or `None` when there is none.

    `cpu_ms` is the better primary statistic on a shared machine: wall clock carries the
    host load, while the process CPU the query cost does not. Records written before CPU
    accounting existed have no `cpu_ms`, and those groups report `None`.
    """
    if not entries:
        return None
    values = [entry[field] for entry in entries if field in entry]
    return median(values) if values else None


def ratio(numerator, denominator):
    """`numerator / denominator`, or `None` when either side is missing or zero."""
    if numerator is None or denominator is None or not denominator:
        return None
    return numerator / denominator


def fmt_ms(value):
    return "-" if value is None else f"{value:.1f}"


def fmt_ratio(value):
    return "-" if value is None else f"{value:.2f}"


def round_key(round_name):
    """`r2` sorts before `r10`: numeric rounds sort as numbers, anything else as text."""
    return (0, int(round_name), "") if round_name.isdigit() else (1, 0, round_name)


def load(directory):
    """`{case: {arm: {round: [entry, ...]}}}`, plus every recorded host load."""
    collected = {}
    loads = []
    for path in sorted(glob.glob(os.path.join(directory, "*-r*-*.json"))):
        name = os.path.basename(path)[: -len(".json")]
        # The round and the arm come from the file name; the case names come from the
        # record itself, which is the only authority on them.
        _, rest = name.rsplit("-r", 1)
        round_name, arm = rest.split("-", 1)
        with open(path) as handle:
            record = json.load(handle)
        for case in sorted({entry["name"] for entry in record["cases"]}):
            entries = [entry for entry in record["cases"] if entry["name"] == case]
            collected.setdefault(case, {}).setdefault(arm, {}).setdefault(round_name, []).extend(
                entries
            )
            loads.extend(entry["load1"] for entry in entries if "load1" in entry)
    return collected, loads


def rounds_of(arms):
    """Every round any arm has a record for, in round order."""
    names = {round_name for rounds in arms.values() for round_name in rounds}
    return sorted(names, key=round_key)


PER_ROUND_HEADER = (
    f"{'case':<8}{'round':<8}{'L off':>9}{'N rescan':>10}{'F indexed':>11}"
    f"{'L/F':>7}{'N/F':>7}"
)


def report_per_round(case, arms, field="ms", title=None):
    """One row per round plus a pooled row, and a line on how stable the ratios are."""
    if title:
        print(title)
    per_round_ratios = {arm_pair: [] for arm_pair in ("L/F", "N/F")}
    for round_name in rounds_of(arms):
        medians = {
            arm: median_of(arms.get(arm, {}).get(round_name, []), field) for arm in ARMS
        }
        left_fixed = ratio(medians["L"], medians["F"])
        rescan_fixed = ratio(medians["N"], medians["F"])
        if left_fixed is not None:
            per_round_ratios["L/F"].append(left_fixed)
        if rescan_fixed is not None:
            per_round_ratios["N/F"].append(rescan_fixed)
        print(
            f"{case:<8}{'r' + str(round_name):<8}"
            f"{fmt_ms(medians['L']):>9}{fmt_ms(medians['N']):>10}{fmt_ms(medians['F']):>11}"
            f"{fmt_ratio(left_fixed):>7}{fmt_ratio(rescan_fixed):>7}"
        )

    pooled = {
        arm: median_of(
            [
                entry
                for round_name in rounds_of(arms)
                for entry in arms.get(arm, {}).get(round_name, [])
            ],
            field,
        )
        for arm in ARMS
    }
    pooled_left, pooled_rescan = ratio(pooled["L"], pooled["F"]), ratio(pooled["N"], pooled["F"])
    print(
        f"{case:<8}{'pooled':<8}"
        f"{fmt_ms(pooled['L']):>9}{fmt_ms(pooled['N']):>10}{fmt_ms(pooled['F']):>11}"
        f"{fmt_ratio(pooled_left):>7}{fmt_ratio(pooled_rescan):>7}"
    )

    spread = []
    for pair, values, pooled_value in (
        ("L/F", per_round_ratios["L/F"], pooled_left),
        ("N/F", per_round_ratios["N/F"], pooled_rescan),
    ):
        if values:
            spread.append(
                f"{pair} {min(values):.2f}..{max(values):.2f} over {len(values)} round(s), "
                f"pooled {fmt_ratio(pooled_value)}"
            )
    if spread:
        print(f"  {case}: " + "; ".join(spread))
    print()


def main():
    collected, loads = load(sys.argv[1])
    if not collected:
        raise SystemExit(f"no records in {sys.argv[1]}")

    print(f"{len(collected)} case(s): {', '.join(collected)}")
    if loads:
        print(f"host load average during the runs: min {min(loads):.1f}, "
              f"median {median(loads):.1f}, max {max(loads):.1f}")
    print()

    print("## Per-arm timings, all rounds and timed iterations\n")
    header = (f"{'case':<8}{'arm':<4}{'mode':<20}{'runs':>5}{'rows':>8}{'hits':>6}"
              f"{'median':>9}{'min':>8}{'max':>8}")
    print(header)
    print("-" * len(header))
    medians = {}
    for case, arms in collected.items():
        for arm in ARMS:
            entries = [entry for rounds in arms.get(arm, {}).values() for entry in rounds]
            if not entries:
                continue
            times = [entry["ms"] for entry in entries]
            medians[(case, arm)] = median(times)
            print(
                f"{case:<8}{arm:<4}{ARM_LABEL[arm]:<20}{len(times):>5}"
                f"{'/'.join(map(str, sorted({e['rows'] for e in entries}))):>8}"
                f"{'/'.join(map(str, sorted({e['hits'] for e in entries}))):>6}"
                f"{median(times):>9.1f}{min(times):>8.1f}{max(times):>8.1f}"
            )
        print()

    print("## Ratios: ordinary time / fused time, so above 1 means fusion is faster\n")
    header = (f"{'case':<8}{'L off':>9}{'N rescan':>10}{'F indexed':>11}"
              f"{'L/N':>7}{'L/F':>7}{'N/F':>7}{'N->F':>8}")
    print(header)
    print("-" * len(header))
    for case in collected:
        left, rescan, fixed = (medians.get((case, arm)) for arm in ARMS)
        if None in (left, rescan, fixed):
            continue
        print(
            f"{case:<8}{left:>9.1f}{rescan:>10.1f}{fixed:>11.1f}"
            f"{left / rescan:>7.2f}{left / fixed:>7.2f}{rescan / fixed:>7.2f}"
            f"{100 * (rescan - fixed) / rescan:>7.0f}%"
        )
    print()

    print(
        "## Per-round medians and ratios: ordinary time / fused time, so above 1 means "
        "fusion is faster\n"
    )
    print(PER_ROUND_HEADER)
    print("-" * len(PER_ROUND_HEADER))
    for case, arms in collected.items():
        report_per_round(case, arms)

    # The same table on process CPU. Wall clock on a shared host carries the load average
    # printed above; the CPU the query cost does not, so a ratio that holds here and not in
    # the wall table is load noise rather than a property of the plan.
    if any("cpu_ms" in entry for arms in collected.values() for rounds in arms.values()
           for entries in rounds.values() for entry in entries):
        print(
            "## Per-round CPU per query and ratios: ordinary CPU / fused CPU, so above 1 "
            "means fusion burns less\n"
        )
        print(PER_ROUND_HEADER)
        print("-" * len(PER_ROUND_HEADER))
        for case, arms in collected.items():
            report_per_round(case, arms, field="cpu_ms")


if __name__ == "__main__":
    main()
