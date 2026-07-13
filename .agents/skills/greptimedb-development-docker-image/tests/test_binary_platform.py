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

import importlib
import os
import stat
import struct
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

binary_platform = importlib.import_module("binary_platform")


def elf(machine: int, *, bits: int = 2, endian: int = 1, abi: int = 3, elf_type: int = 3) -> bytes:
    header = bytearray(64)
    header[:4] = b"\x7fELF"
    header[4:8] = bytes((bits, endian, 1, abi))
    struct.pack_into("<H", header, 16, elf_type)
    struct.pack_into("<H" if endian == 1 else ">H", header, 18, machine)
    struct.pack_into("<I", header, 20, 1)
    struct.pack_into("<H", header, 52, 64)
    return bytes(header)


class BinaryPlatformTest(unittest.TestCase):
    def make_binary(self, content: bytes) -> Path:
        path = Path(self.directory.name) / "greptime"
        path.write_bytes(content)
        path.chmod(0o755)
        return path

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.directory.cleanup()

    def test_matching_architectures(self):
        binary_platform.validate_binary(self.make_binary(elf(0x3E)), "linux/amd64")
        binary_platform.validate_binary(self.make_binary(elf(0xB7)), "linux/arm64")

    def test_rejects_bad_executable_elf_and_platform(self):
        path = self.make_binary(elf(0x3E))
        path.chmod(0o644)
        with self.assertRaises(ValueError):
            binary_platform.validate_binary(path, "linux/amd64")
        path = self.make_binary(b"not an elf")
        with self.assertRaises(ValueError):
            binary_platform.validate_binary(path, "linux/amd64")
        path = self.make_binary(elf(0x3E, bits=1))
        with self.assertRaises(ValueError):
            binary_platform.validate_binary(path, "linux/amd64")
        with self.assertRaises(ValueError):
            binary_platform.validate_binary(self.make_binary(elf(0x3E, abi=9)), "linux/amd64")
        with self.assertRaises(ValueError):
            binary_platform.validate_binary(self.make_binary(elf(0x3E, elf_type=1)), "linux/amd64")
        with self.assertRaises(ValueError):
            binary_platform.validate_binary(self.make_binary(b"\x7fELF"), "linux/amd64")
        with self.assertRaises(ValueError):
            binary_platform.validate_binary(self.make_binary(elf(0x3E)), "darwin/amd64")

    def test_rejects_wrong_architecture_and_symlink(self):
        with self.assertRaises(ValueError):
            binary_platform.validate_binary(self.make_binary(elf(0x3E)), "linux/arm64")
        target = self.make_binary(elf(0x3E))
        link = Path(self.directory.name) / "link"
        link.symlink_to(target)
        with self.assertRaises(ValueError):
            binary_platform.validate_binary(link, "linux/amd64")

    def test_cli_success_and_failures(self):
        path = self.make_binary(elf(0x3E))
        assert binary_platform.__file__ is not None
        script = Path(binary_platform.__file__)
        ok = subprocess.run([sys.executable, str(script), "--binary", str(path), "--platform", "linux/amd64"], capture_output=True, text=True)
        self.assertEqual(ok.returncode, 0, ok.stderr)
        bad = subprocess.run([sys.executable, str(script), "--binary", str(path), "--platform", "linux/arm64"], capture_output=True, text=True)
        self.assertEqual(bad.returncode, 2)
        missing = subprocess.run([sys.executable, str(script), "--binary", str(path) + ".missing", "--platform", "linux/amd64"], capture_output=True, text=True)
        self.assertEqual(missing.returncode, 2)


if __name__ == "__main__":
    unittest.main()
