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

"""Summarises a two-arm `fused_series_bench_pair.sh` run into the tables of section 4.6.

This is the P/F companion to `fused_series_bench_summarize.py`, which is tied to the
three-arm L/N/F sweep of section 4.3. Both arms here are flag-on, so the arms are named
rather than inferred: **P** is the earlier commit and **F** the later one, and every ratio
is computed as `F / P` explicitly. An earlier version of this script derived the ratio from
the position of an arm in a sorted list while labelling the column "F / P", which computed
`P / F`; naming both arms in the arithmetic removes the possibility.

Ratios are reported per paired round *and* pooled, because unchanged code drifts several
percent between invocations on a shared host. A pooled ratio that no single round
reproduces is drift, not an effect.

    scripts/fused_series_bench_pair_summarize.py RECORD_DIR
"""

import glob
import json
import os
import statistics
import sys

# The arm that is the baseline, and the arm that is the change. Named, never positional.
BASELINE = "P"
CHANGE = "F"


def median(values):
    return statistics.median(values) if values else float("nan")


def load(pattern):
    """Reads every `<case>-r<round>-<arm>.json` under `pattern` into a dict."""
    records = {}
    for path in sorted(glob.glob(pattern)):
        with open(path) as handle:
            record = json.load(handle)
        # c<case>-r<round>-<arm>.json -- the case name may itself contain no hyphen.
        stem = os.path.basename(path)[1:-len(".json")]
        case, rest = stem.split("-r", 1)
        round_number, arm = rest.split("-", 1)
        records[(case, int(round_number), arm)] = {
            "ms": [entry["ms"] for entry in record["cases"]],
            "cpu_ms": [entry["cpu_ms"] for entry in record["cases"]],
            "rows": [entry["rows"] for entry in record["cases"]],
            "hits": [entry["hits"] for entry in record["cases"]],
            "expected": [entry["expected_hits"] for entry in record["cases"]],
            "commit": record.get("commit", "?"),
            # "off" or "on": which native flag the arm ran with. Printed, because the same
            # summariser serves two experiments -- two commits (both on) and one commit
            # against the ordinary plan (off against on).
            "flag": record.get("arm", "?"),
        }
    return records


def main():
    if len(sys.argv) != 2:
        raise SystemExit(__doc__)
    run = sys.argv[1]
    records = load(os.path.join(run, "c*.json"))
    if not records:
        raise SystemExit(f"no records under {run}")

    cases = sorted({case for case, _, _ in records})
    rounds = sorted({rnd for _, rnd, _ in records})

    print(f"run: {run}")
    print(f"rounds: {', '.join(map(str, rounds))}")
    print()

    flags = {}
    for (case, rnd, arm), record in records.items():
        flags.setdefault(arm, record["flag"])
    print("flags: " + ", ".join(f"{arm}={flag}" for arm, flag in sorted(flags.items())))
    print()

    header = (
        f"{'case':8} {'arm':4} {'n':>3} {'ms med':>9} {'ms min':>9} {'ms max':>9} "
        f"{'cpu med':>9} {'cpu min':>9} {'cpu max':>9} {'rows':>7} {'hits':>5}"
    )
    print(header)
    print("-" * len(header))
    for case in cases:
        for arm in (BASELINE, CHANGE):
            entries = [
                records[(case, rnd, arm)] for rnd in rounds if (case, rnd, arm) in records
            ]
            if not entries:
                continue
            ms = [value for entry in entries for value in entry["ms"]]
            cpu = [value for entry in entries for value in entry["cpu_ms"]]
            rows = sorted({value for entry in entries for value in entry["rows"]})
            hits = sorted({value for entry in entries for value in entry["hits"]})
            expected = {value for entry in entries for value in entry["expected"]}
            # A wrong fusion count means the comparison measured something other than the
            # intended path, so it is flagged rather than left to be noticed.
            mismatch = "" if set(hits) == expected else "  WRONG-HITS"
            print(
                f"{case:8} {arm:4} {len(ms):3} {median(ms):9.1f} {min(ms):9.1f} "
                f"{max(ms):9.1f} {median(cpu):9.1f} {min(cpu):9.1f} {max(cpu):9.1f} "
                f"{'/'.join(map(str, rows)):>7} {','.join(map(str, hits)):>5}{mismatch}"
            )
        print()

    print(f"ratio {CHANGE} / {BASELINE} (below 1.00 means {CHANGE} is cheaper)")
    print(f"{'case':8} {'round':>5} {'ms':>7} {'cpu':>7} {'rows equal':>10}")
    print("-" * 42)
    for case in cases:
        for rnd in rounds:
            if (case, rnd, BASELINE) not in records or (case, rnd, CHANGE) not in records:
                continue
            baseline = records[(case, rnd, BASELINE)]
            change = records[(case, rnd, CHANGE)]
            ms_ratio = median(change["ms"]) / median(baseline["ms"])
            cpu_ratio = median(change["cpu_ms"]) / median(baseline["cpu_ms"])
            rows_same = set(baseline["rows"]) == set(change["rows"])
            print(f"{case:8} {rnd:5} {ms_ratio:7.3f} {cpu_ratio:7.3f} {str(rows_same):>10}")
        pooled = {}
        for arm in (BASELINE, CHANGE):
            entries = [
                records[(case, rnd, arm)] for rnd in rounds if (case, rnd, arm) in records
            ]
            if entries:
                pooled[arm] = (
                    median([value for entry in entries for value in entry["ms"]]),
                    median([value for entry in entries for value in entry["cpu_ms"]]),
                )
        if len(pooled) == 2:
            ms_ratio = pooled[CHANGE][0] / pooled[BASELINE][0]
            cpu_ratio = pooled[CHANGE][1] / pooled[BASELINE][1]
            print(f"{case:8} {'pool':>5} {ms_ratio:7.3f} {cpu_ratio:7.3f}")
        print()


if __name__ == "__main__":
    main()
