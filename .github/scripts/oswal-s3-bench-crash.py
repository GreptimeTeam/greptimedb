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

"""Writer and oracle of the benchmark's crash cells.

Every writer owns the stripe `first + index, first + index + writers, ...` of
batch sequence numbers, inserts `rows` rows per batch and stops at the first
statement that is not acknowledged, so a stripe has at most one attempted but
unacknowledged batch: the one in flight at the kill. Every request is logged
with its status and the time it ended, on the same clock as the kill stamps.
With `--seconds` the writers also stop once that long has passed, and the
command fails unless every writer wrote until then without a failure.
"""

from __future__ import annotations

import argparse
import collections
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


def insert(url: str, table: str, seq: int, rows: int) -> str:
    values = ", ".join(f"({seq}, {seq * rows + i})" for i in range(rows))
    body = urllib.parse.urlencode({"sql": f"INSERT INTO {table} (seq, ts) VALUES {values}"}).encode()
    try:
        with urllib.request.urlopen(urllib.request.Request(url, data=body), timeout=60) as response:
            return str(response.status)
    except urllib.error.HTTPError as error:
        return str(error.code)
    except (OSError, urllib.error.URLError):
        # The connection dropped: the request may or may not have been applied.
        return "000"


def writer(args, index: int, deadline: float, failures: list) -> None:
    directory, seq = Path(args.dir), args.first_seq + index
    with open(directory / f"attempted-{index}.log", "a") as attempted, \
            open(directory / f"requests-{index}.log", "a") as requests, \
            open(directory / f"acked-{index}.log", "a") as acked:
        while time.monotonic() < deadline:
            attempted.write(f"{seq}\n")
            attempted.flush()
            code = insert(args.url, args.table, seq, args.rows)
            requests.write(f"{seq} {code} {time.time_ns()}\n")
            requests.flush()
            if code != "200":
                failures.append(f"writer {index}: batch {seq} ended with {code}")
                break
            acked.write(f"{seq}\n")
            acked.flush()
            seq += args.writers
    (directory / f"exit-{index}.log").write_text(f"{time.time_ns()}\n")


def write(args) -> int:
    os.makedirs(args.dir, exist_ok=True)
    deadline = time.monotonic() + args.seconds if args.seconds else float("inf")
    failures = []

    def run(index):
        try:
            writer(args, index, deadline, failures)
        except Exception as error:
            failures.append(f"writer {index}: {error!r}")
            raise

    threads = [threading.Thread(target=run, args=(i,)) for i in range(args.writers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    # Crash cells end their writers with a kill; timed writers must have run to the deadline.
    if args.seconds and (failures or not any(lines(Path(args.dir) / f"acked-{i}.log") for i in range(args.writers))):
        print("; ".join(failures) or "no batch was acknowledged", file=sys.stderr)
        return 1
    return 0


def lines(path: Path) -> list:
    return [line.split() for line in path.read_text().splitlines() if line.strip()] if path.exists() else []


def check_kill(directory: Path, writers: int, before_ns: int, after_ns: int, window_max_ms: int) -> dict:
    """A dropped connection is in flight when it ended at or after the stamp taken before the kill."""
    errors, in_flight, running = [], 0, 0
    if after_ns - before_ns > window_max_ms * 1_000_000:
        errors.append(f"kill window {after_ns - before_ns}ns exceeds {window_max_ms}ms")
    for index in range(writers):
        exit_path = directory / f"exit-{index}.log"
        if not exit_path.exists():
            errors.append(f"writer {index} left no exit time")
            continue
        running += int(exit_path.read_text()) >= before_ns
        for seq, code, end_ns in lines(directory / f"requests-{index}.log"):
            if code == "200":
                continue
            if code != "000":
                errors.append(f"writer {index}: batch {seq} rejected with HTTP {code}")
            elif int(end_ns) < before_ns:
                errors.append(f"writer {index}: batch {seq} lost its connection before the kill")
            else:
                in_flight += 1
    if running == 0:
        errors.append("no writer was running at the kill")
    return {"kill_window_ns": after_ns - before_ns, "in_flight": in_flight, "writers_at_kill": running,
            "kill_errors": errors}


def verify(run_dir: Path, found: dict, rows: int, enqueued: bool, cycle: int) -> dict:
    """Checks the table, read as batch -> row count, against every cycle's writers so far, and
    counts what the given cycle's writers acknowledged, left and lost on their own."""
    acked, stripes, cycle_attempted, cycle_acked = [], collections.OrderedDict(), set(), set()
    for cycle_dir in sorted(run_dir.glob("cycle-*"), key=lambda p: int(p.name.split("-")[1])):
        current = cycle_dir.name == f"cycle-{cycle}"
        for path in sorted(cycle_dir.glob("attempted-*.log")):
            stripes[(cycle_dir.name, path.name)] = [int(seq) for (seq,) in lines(path)]
            if current:
                cycle_attempted.update(stripes[(cycle_dir.name, path.name)])
        for path in sorted(cycle_dir.glob("acked-*.log")):
            acked += [int(seq) for (seq,) in lines(path)]
            if current:
                cycle_acked.update(int(seq) for (seq,) in lines(path))
    acked_set = set(acked)
    attempted = {seq for stripe in stripes.values() for seq in stripe}
    errors = []
    duplicated = sorted(seq for seq, count in found.items() if count > rows)
    partial = sorted(seq for seq, count in found.items() if count < rows)
    if duplicated:
        errors.append(f"batches present more than once: {duplicated[:20]}")
    if partial:
        errors.append(f"batches present in part: {partial[:20]}")
    missing = sorted(acked_set - set(found))
    if missing and not enqueued:
        errors.append(f"acknowledged but absent: {missing[:20]}")
    unattempted = sorted(set(found) - attempted)
    if unattempted:
        errors.append(f"present but never attempted: {unattempted[:20]}")
    # The unacknowledged tail of each stripe was in flight at a kill.
    in_flight = set()
    for key, stripe in stripes.items():
        prefix = 0
        while prefix < len(stripe) and stripe[prefix] in acked_set:
            prefix += 1
        if any(seq in acked_set for seq in stripe[prefix:]):
            errors.append(f"{key}: acknowledged after a failure")
        in_flight.update(stripe[prefix:])
    extra = sorted(set(found) - acked_set)
    stray = [seq for seq in extra if seq not in in_flight]
    if stray and not enqueued:
        errors.append(f"present, unacknowledged and not in flight at a kill: {stray[:20]}")
    if len(acked) != len(acked_set):
        errors.append("a batch was acknowledged twice")
    present = set(found) & cycle_attempted
    return {"cycle_acked_batches": len(cycle_acked), "cycle_present_batches": len(present),
            "cycle_lost_batches": len(cycle_acked - present), "cycle_lost_rows": len(cycle_acked - present) * rows,
            "cycle_extra_batches": len(present - cycle_acked),
            "total_found_batches": len(found), "total_acked_batches": len(acked_set),
            "total_lost_batches": len(missing), "total_extra_batches": len(extra), "verify_errors": errors}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    w = commands.add_parser("write")
    w.add_argument("--dir", required=True)
    w.add_argument("--url", required=True)
    w.add_argument("--table", default="crash")
    for name in ("writers", "first-seq", "rows"):
        w.add_argument(f"--{name}", type=int, required=True)
    w.add_argument("--seconds", type=float, default=0, help="stop after this long; 0 writes until a failure")
    c = commands.add_parser("check")
    c.add_argument("--run-dir", type=Path, required=True)
    c.add_argument("--cycle", type=int, required=True)
    c.add_argument("--found", type=Path, required=True, help="GreptimeDB JSON output of SELECT seq, count(*)")
    c.add_argument("--mode", choices=("durable", "enqueued"), required=True)
    for name in ("writers", "rows", "before-ns", "after-ns", "window-max-ms"):
        c.add_argument(f"--{name}", type=int, required=True)
    args = parser.parse_args()
    if args.command == "write":
        return write(args)
    rows = json.loads(args.found.read_text())["output"][0]["records"]["rows"]
    result = {"cycle": args.cycle}
    result |= check_kill(args.run_dir / f"cycle-{args.cycle}", args.writers, args.before_ns, args.after_ns,
                         args.window_max_ms)
    result |= verify(args.run_dir, {int(seq): int(count) for seq, count in rows}, args.rows,
                     args.mode == "enqueued", args.cycle)
    result["passed"] = not result["kill_errors"] and not result["verify_errors"]
    print(json.dumps(result))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
