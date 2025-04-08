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

import os
import re
from multiprocessing import Pool


def find_rust_files(directory):
    rust_files = []
    for root, _, files in os.walk(directory):
        # Skip files with "test" in the path
        if "test" in root.lower():
            continue

        for file in files:
            # Skip files with "test" in the filename
            if "test" in file.lower():
                continue

            if file.endswith(".rs"):
                rust_files.append(os.path.join(root, file))
    return rust_files


def check_file_for_super_import(file_path):
    with open(file_path, "r") as file:
        lines = file.readlines()

    violations = []
    for line_number, line in enumerate(lines, 1):
        # Check for "use super::" without leading tab
        if line.startswith("use super::"):
            violations.append((line_number, line.strip()))

    if violations:
        return file_path, violations
    return None


def main():
    rust_files = find_rust_files(".")

    with Pool() as pool:
        results = pool.map(check_file_for_super_import, rust_files)

    # Filter out None results
    violations = [result for result in results if result]

    if violations:
        print("Found 'use super::' without leading tab in the following files:")
        counter = 1
        for file_path, file_violations in violations:
            for line_number, line in file_violations:
                print(f"{counter:>5} {file_path}:{line_number} - {line}")
                counter += 1
        raise SystemExit(1)
    else:
        print("No 'use super::' without leading tab found. All files are compliant.")


if __name__ == "__main__":
    main()
