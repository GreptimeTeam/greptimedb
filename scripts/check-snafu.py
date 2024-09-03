import os
import re


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


def check_snafu_in_files(branch_name, rust_files):
    branch_name_snafu = f"{branch_name}Snafu"
    for rust_file in rust_files:
        with open(rust_file, "r") as file:
            content = file.read()
            if branch_name_snafu in content:
                return True
    return False


def main():
    error_files, other_rust_files = find_rust_files(".")
    branch_names = []

    for error_file in error_files:
        with open(error_file, "r") as file:
            content = file.read()
            branch_names.extend(extract_branch_names(content))

    unused_snafu = [
        branch_name
        for branch_name in branch_names
        if not check_snafu_in_files(branch_name, other_rust_files)
    ]

    for name in unused_snafu:
        print(name)

    if unused_snafu:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
