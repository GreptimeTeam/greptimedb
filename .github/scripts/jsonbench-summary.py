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

#!/usr/bin/env python3

import argparse
import ast
import json
import pathlib
import re
import statistics


def read_number(result_dir, patterns):
    for pattern in patterns:
        for path in sorted(result_dir.rglob(pattern)):
            text = path.read_text(encoding="utf-8", errors="replace")
            match = re.search(r"\d+(?:\.\d+)?", text)
            if match:
                return match.group(0)
    return None


def format_gb(value):
    if value is None:
        return "N/A"

    try:
        bytes_size = float(value)
    except ValueError:
        return "N/A"

    return f"{bytes_size / 1000 / 1000 / 1000:.2f} GB"


def format_dataset(choice):
    datasets = {
        "1": "1M",
        "2": "10M",
        "3": "100M",
        "4": "1000M",
    }
    if choice is None:
        return "N/A"
    return datasets.get(choice, f"choice {choice}")


def read_runtime_text(result_dir):
    runtime_files = sorted(result_dir.rglob("*.results_runtime"))
    if runtime_files:
        return "\n".join(
            path.read_text(encoding="utf-8", errors="replace")
            for path in runtime_files
        )

    log_files = sorted(result_dir.rglob("*.log"))
    return "\n".join(
        path.read_text(encoding="utf-8", errors="replace")
        for path in log_files
    )


def parse_query_rows(text):
    rows = []
    query_index = 0
    has_timings = False

    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("Running query:"):
            query_index += 1
            has_timings = False
            continue

        if not (
            query_index > 0
            and not has_timings
            and stripped.startswith("[")
            and stripped.endswith("]")
        ):
            continue

        try:
            timings = ast.literal_eval(stripped)
        except (SyntaxError, ValueError):
            continue

        has_timings = True
        for label, value in (
            ("cold", timings[0] if len(timings) > 0 else None),
            ("hot", timings[1] if len(timings) > 1 else None),
        ):
            if value is not None:
                rows.append((query_index, label, float(value)))

    return rows


def parse_load_data_stats(result_dir):
    values = []
    for path in sorted(result_dir.rglob("*.load_data")):
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            stripped = line.strip()
            if not stripped.startswith("{"):
                continue

            try:
                data = json.loads(stripped)
            except json.JSONDecodeError:
                continue

            value = data.get("execution_time_ms")
            if isinstance(value, (int, float)):
                values.append(value / 1000)

    if not values:
        return {}

    return {
        "Max": max(values),
        "Min": min(values),
        "Avg": sum(values) / len(values),
        "Median": statistics.median(values),
    }


def query_rows_to_map(query_rows):
    queries = {}
    for query_index, label, value in query_rows:
        queries.setdefault(query_index, {})[label] = value
    return queries


def format_duration(value):
    if value is None:
        return "N/A"
    return f"{value:.3f}"


def format_delta(current, last):
    if current is None or last in (None, 0):
        return "N/A"

    percent = (current - last) / last * 100
    if abs(percent) <= 0.1:
        return "0"

    formatted = f"{percent:+.1f}"
    return formatted.rstrip("0").rstrip(".")


def format_insert_table(stats, previous_stats):
    rows = [("Indicator", "Value (s)", "Value Last (%)")]
    for label, key in (
        ("Max", "Max"),
        ("Min", "Min"),
        ("Avg", "Avg"),
        ("Medium", "Median"),
    ):
        rows.append(
            (
                label,
                format_duration(stats.get(key)),
                format_delta(stats.get(key), previous_stats.get(key)),
            )
        )

    widths = [max(len(row[column]) for row in rows) for column in range(3)]
    separator = tuple("-" * width for width in widths)
    rows.insert(1, separator)

    return "\n".join(
        f"| {indicator:<{widths[0]}} | {value:>{widths[1]}} | "
        f"{delta:>{widths[2]}} |"
        for indicator, value, delta in rows
    )


def format_query_table(query_rows, previous_query_rows):
    queries = query_rows_to_map(query_rows)
    previous_queries = query_rows_to_map(previous_query_rows)

    rows = [("Query", "Cold (s)", "Cold Last (%)", "Hot (s)", "Hot Last (%)")]
    rows.extend(
        (
            f"Q{query_index}",
            format_duration(values.get("cold")),
            format_delta(
                values.get("cold"),
                previous_queries.get(query_index, {}).get("cold"),
            ),
            format_duration(values.get("hot")),
            format_delta(
                values.get("hot"),
                previous_queries.get(query_index, {}).get("hot"),
            ),
        )
        for query_index, values in sorted(queries.items())
    )

    widths = [max(len(row[column]) for row in rows) for column in range(5)]
    separator = tuple("-" * width for width in widths)
    rows.insert(1, separator)

    return "\n".join(
        (
            f"| {query:<{widths[0]}} | {cold:>{widths[1]}} | "
            f"{cold_delta:>{widths[2]}} | {hot:>{widths[3]}} | "
            f"{hot_delta:>{widths[4]}} |"
        )
        for query, cold, cold_delta, hot, hot_delta in rows
    )


def build_payload(result_dir, previous_result_dir, result, run_url):
    if result != "success":
        return {"text": f"Nightly JSONBench failed, please check {run_url}."}

    data_size = read_number(result_dir, ["*.total_size", "*.data_size"])
    count = read_number(result_dir, ["*.count"])
    dataset = read_number(result_dir, ["*.dataset"])
    query_rows = parse_query_rows(read_runtime_text(result_dir))
    insert_stats = parse_load_data_stats(result_dir)
    previous_query_rows = []
    previous_insert_stats = {}
    if previous_result_dir and previous_result_dir.exists():
        previous_query_rows = parse_query_rows(read_runtime_text(previous_result_dir))
        previous_insert_stats = parse_load_data_stats(previous_result_dir)

    summary = (
        f"Dataset: {format_dataset(dataset)}\n"
        f"Data size: {format_gb(data_size)}\n"
        f"Count: {count or 'N/A'}"
    )
    insert_table = format_insert_table(insert_stats, previous_insert_stats)
    query_table = format_query_table(query_rows, previous_query_rows)
    text = (
        "Nightly JSONBench has completed successfully.\n"
        f"<{run_url}|Workflow run>\n"
        f"```{summary}\n\nInsert:\n{insert_table}\n\n{query_table}```"
    )
    return {"text": text}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--result-dir", required=True, type=pathlib.Path)
    parser.add_argument("--previous-result-dir", type=pathlib.Path)
    parser.add_argument("--result", required=True)
    parser.add_argument("--run-url", required=True)
    args = parser.parse_args()

    print(
        json.dumps(
            build_payload(
                args.result_dir,
                args.previous_result_dir,
                args.result,
                args.run_url,
            )
        )
    )


if __name__ == "__main__":
    main()
