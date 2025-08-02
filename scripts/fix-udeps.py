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

import json
import os
import re
import sys


def load_udeps_report(report_path):
    try:
        with open(report_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: Report file '{report_path}' not found.")
        return None
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in report file: {e}")
        return None


def extract_unused_dependencies(report):
    if not report or "unused_deps" not in report:
        return {}

    unused_deps = {}
    for package_full_name, deps_info in report["unused_deps"].items():
        package_name = package_full_name.split(" ")[0]

        all_unused = []
        if deps_info.get("normal"):
            all_unused.extend([(dep, "normal") for dep in deps_info["normal"]])
        if deps_info.get("development"):
            all_unused.extend([(dep, "dev") for dep in deps_info["development"]])
        if deps_info.get("build"):
            all_unused.extend([(dep, "build") for dep in deps_info["build"]])

        if all_unused:
            unused_deps[package_name] = {
                "dependencies": all_unused,
                "manifest_path": deps_info.get("manifest_path", "unknown"),
            }

    return unused_deps


def get_section_pattern(dep_type):
    patterns = {
        "normal": [r"\[dependencies\]", r"\[dependencies\..*?\]"],
        "dev": [r"\[dev-dependencies\]", r"\[dev-dependencies\..*?\]"],
        "build": [r"\[build-dependencies\]", r"\[build-dependencies\..*?\]"],
    }
    return patterns.get(dep_type, [])


def remove_dependency_line(content, dep_name, section_start, section_end):
    section_content = content[section_start:section_end]

    dep_patterns = [
        rf"^{re.escape(dep_name)}\s*=.*$",
        rf"^{re.escape(dep_name)}\.workspace\s*=.*$",
    ]

    for pattern in dep_patterns:
        match = re.search(pattern, section_content, re.MULTILINE)
        if match:
            line_start = section_start + match.start()
            line_end = section_start + match.end()

            if line_end < len(content) and content[line_end] == "\n":
                line_end += 1

            return content[:line_start] + content[line_end:], True

    return content, False


def remove_dependency_from_toml(file_path, dep_name, dep_type):
    try:
        with open(file_path, "r") as f:
            content = f.read()

        section_patterns = get_section_pattern(dep_type)
        if not section_patterns:
            return False

        for pattern in section_patterns:
            section_match = re.search(pattern, content, re.IGNORECASE)
            if not section_match:
                continue

            section_start = section_match.end()
            next_section = re.search(r"\n\s*\[", content[section_start:])
            section_end = (
                section_start + next_section.start() if next_section else len(content)
            )

            new_content, removed = remove_dependency_line(
                content, dep_name, section_start, section_end
            )
            if removed:
                with open(file_path, "w") as f:
                    f.write(new_content)
                return True

        return False

    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False


def process_unused_dependencies(unused_deps):
    if not unused_deps:
        print("No unused dependencies found.")
        return

    total_removed = 0
    total_failed = 0

    for package, info in unused_deps.items():
        deps = info["dependencies"]
        manifest_path = info["manifest_path"]

        if not os.path.exists(manifest_path):
            print(f"Manifest file not found: {manifest_path}")
            total_failed += len(deps)
            continue

        for dep, dep_type in deps:
            if remove_dependency_from_toml(manifest_path, dep, dep_type):
                print(f"Removed {dep} from {package}")
                total_removed += 1
            else:
                print(f"Failed to remove {dep} from {package}")
                total_failed += 1

    print(f"Removed {total_removed} dependencies")
    if total_failed > 0:
        print(f"Failed to remove {total_failed} dependencies")


def main():
    if len(sys.argv) > 1:
        report_path = sys.argv[1]
    else:
        report_path = "udeps-report.json"

    report = load_udeps_report(report_path)
    if report is None:
        sys.exit(1)

    unused_deps = extract_unused_dependencies(report)
    process_unused_dependencies(unused_deps)


if __name__ == "__main__":
    main()
