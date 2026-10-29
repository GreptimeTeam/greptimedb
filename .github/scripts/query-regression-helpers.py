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

"""Extract digest-pinned query regression tools from an artifact image."""

import argparse
import hashlib
import json
import os
import re
import subprocess
from pathlib import Path

BINARIES = ("query_perf_fixture", "query_regression_runner")


def resolve_image(value):
    if value == "pinned":
        value = os.environ.get("QUERY_REGRESSION_HELPERS_IMAGE", "")
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9._/:@-]*@sha256:[0-9a-f]{64}", value):
        raise ValueError("helpers_image requires an image@sha256 digest; set QUERY_REGRESSION_HELPERS_IMAGE by publishing a verified image before using 'pinned'")
    return value


def run(*args):
    return subprocess.check_output(args, text=True).strip()


def extract(image, destination, checksums=None):
    expected = None
    if checksums is not None:
        entries = [line.split() for line in checksums.read_text().splitlines()]
        expected = {name: digest for digest, name in entries}
        if set(expected) != set(BINARIES) or len(entries) != len(BINARIES):
            raise ValueError("Checksums must contain exactly the two helper binaries")
    image = resolve_image(image)
    destination.mkdir(parents=True, exist_ok=True)
    run("docker", "pull", image)
    metadata = json.loads(run("docker", "image", "inspect", image))[0]
    revision = (metadata.get("Config", {}).get("Labels") or {}).get("org.opencontainers.image.revision", "")
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("Helper image is missing its source revision label")
    container = run("docker", "create", image, "/query_perf_fixture", "--help")
    try:
        for binary in BINARIES:
            target = destination / binary
            run("docker", "cp", f"{container}:/{binary}", str(target))
            target.chmod(0o755)
            if expected is None:
                run(str(target.resolve()), "--help")
            else:
                with target.open("rb") as file:
                    actual = hashlib.file_digest(file, "sha256").hexdigest()
                if actual != expected[binary]:
                    raise ValueError(f"Published helper checksum mismatch: {binary}")
    finally:
        run("docker", "rm", "--force", "--volumes", container)
    provenance = {"image": image, "image_id": metadata["Id"], "source_revision": revision}
    (destination / "helpers-image.json").write_text(json.dumps(provenance, indent=2) + "\n")
    print(json.dumps(provenance))
    return provenance


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("image")
    parser.add_argument("destination", type=Path)
    parser.add_argument("--checksums", type=Path, help="Verify build checksums instead of executing on this host")
    args = parser.parse_args()
    extract(args.image, args.destination, args.checksums)
