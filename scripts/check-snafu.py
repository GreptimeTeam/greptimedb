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
    error_files = []
    other_rust_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file == "error.rs":
                error_files.append(os.path.join(root, file))
            elif file.endswith(".rs"):
                other_rust_files.append(os.path.join(root, file))
    return error_files, other_rust_files


def extract_branch_names(file_content):
    pattern = re.compile(r"#\[snafu\(display\([^\)]*\)\)\]\s*(\w+)\s*\{")
    return pattern.findall(file_content)


def check_snafu_in_files(branch_name, rust_files_content):
    branch_name_snafu = f"{branch_name}Snafu"
    for content in rust_files_content.values():
        if branch_name_snafu in content:
            return True
    return False


def main():
    error_files, other_rust_files = find_rust_files(".")
    branch_names = []

    for error_file in error_files:
        with open(error_file, "r") as file:
            branch_names.extend(extract_branch_names(file.read()))

    # Read all rust files into memory once
    rust_files_content = {}
    for rust_file in other_rust_files:
        with open(rust_file, "r") as file:
            rust_files_content[rust_file] = file.read()

    with Pool() as pool:
        results = pool.starmap(
            check_snafu_in_files, [(bn, rust_files_content) for bn in branch_names]
        )
    unused_snafu = [bn for bn, found in zip(branch_names, results) if not found]

    if unused_snafu:
        print("Unused error variants:")
        for name in unused_snafu:
            print(name)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
