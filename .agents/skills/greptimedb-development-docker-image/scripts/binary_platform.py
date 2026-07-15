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

"""Validate that a binary is an executable ELF for a requested Linux platform."""

import argparse
import stat
import struct
import sys
from pathlib import Path
from typing import Optional

PLATFORM_MACHINES = {"linux/amd64": 0x3E, "linux/arm64": 0xB7}


def validate_platform(platform: str) -> int:
    try:
        return PLATFORM_MACHINES[platform]
    except KeyError as error:
        raise ValueError("platform must be linux/amd64 or linux/arm64") from error


def inspect_elf(path: Path) -> int:
    executable_bits = stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH
    if path.is_symlink() or not path.is_file() or not (path.stat().st_mode & executable_bits):
        raise ValueError("binary must be an executable regular file")
    try:
        with path.open("rb") as handle:
            header = handle.read(64)
    except OSError as error:
        raise ValueError(f"cannot read binary: {error}") from error
    if len(header) != 64 or header[:4] != b"\x7fELF":
        raise ValueError("binary is not ELF")
    if header[4] != 2 or header[5] != 1 or header[6] != 1:
        raise ValueError("binary must be a 64-bit little-endian ELF")
    if header[7] not in (0, 3):
        raise ValueError("binary must use the System V or Linux ELF ABI")
    elf_type, machine, version = struct.unpack_from("<HHI", header, 16)
    header_size = struct.unpack_from("<H", header, 52)[0]
    if elf_type not in (2, 3) or version != 1 or header_size != 64:
        raise ValueError("binary has an unsupported ELF executable header")
    return machine


def validate_binary(path: Path, platform: str) -> None:
    expected = validate_platform(platform)
    actual = inspect_elf(path)
    if actual != expected:
        raise ValueError("binary architecture does not match requested platform")


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--platform", required=True)
    args = parser.parse_args(argv)
    try:
        validate_binary(args.binary, args.platform)
    except ValueError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
