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

"""Collect read-only build context for a GreptimeDB development image."""

import argparse
import json
import os
import platform
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, cast

from next_image_tag import increment_tag
from binary_platform import PLATFORM_MACHINES, inspect_elf
from image_config import image_reference, parse_env_file, validate_values


ENV_KEYS = ("IMAGE_REGISTRY", "IMAGE_REPOSITORY", "IMAGE_TAG")
PROFILE_DIRECTORIES = {"dev": "debug", "test": "debug", "bench": "release"}
PLATFORM_ARCHITECTURES = {
    "x86_64": "linux/amd64",
    "amd64": "linux/amd64",
    "aarch64": "linux/arm64",
    "arm64": "linux/arm64",
}


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--bin", dest="binary", default="greptime")
    parser.add_argument("--profile", default="nightly")
    parser.add_argument("--target", help="Rust target triple for cross-compilation")
    parser.add_argument("--package", help="Cargo package containing the selected binary")
    parser.add_argument("--platform", choices=("linux/amd64", "linux/arm64"))
    parser.add_argument("--engine", choices=("auto", "docker", "podman"), default="auto")
    parser.add_argument("--registry")
    parser.add_argument("--repository")
    parser.add_argument("--tag")
    parser.add_argument(
        "--check-registry-tag",
        action="store_true",
        help="inspect the configured image tag with Docker without modifying it",
    )
    return parser.parse_args()


def native_platform(architecture: str) -> Optional[str]:
    return PLATFORM_ARCHITECTURES.get(architecture.lower())


def parse_env(env_path: Path) -> dict[str, object]:
    values = parse_env_file(env_path)
    missing = [key for key in ("IMAGE_REPOSITORY", "IMAGE_TAG") if not values.get(key)]
    try:
        if not missing:
            validate_values(values)
        error = None
    except ValueError as exc:
        error = str(exc)
    return {"path": str(env_path), "exists": env_path.is_file(), "values": values, "missing": missing, "error": error}


def cargo_metadata(source: Path) -> dict[str, Any]:
    command = ["cargo", "metadata", "--locked", "--format-version=1", "--no-deps"]
    try:
        result = subprocess.run(command, cwd=source, text=True, capture_output=True, check=True)
    except FileNotFoundError as error:
        raise RuntimeError("cargo is not installed or is not on PATH") from error
    except subprocess.CalledProcessError as error:
        detail = error.stderr.strip() or error.stdout.strip()
        raise RuntimeError(f"cargo metadata failed: {detail}") from error
    return json.loads(result.stdout)


def binary_targets(metadata: dict[str, Any]) -> list[dict[str, Any]]:
    targets = []
    for package in metadata["packages"]:
        for target in package["targets"]:
            if "bin" in target["kind"]:
                targets.append({
                    "package": package["name"],
                    "name": target["name"],
                    "path": target["src_path"],
                    "required_features": target.get("required-features", []),
                })
    return sorted(targets, key=lambda target: (target["name"], target["package"]))


def expected_binary(target_dir: Path, binary: str, profile: str, rust_target: Optional[str]) -> Path:
    profile_directory = PROFILE_DIRECTORIES.get(profile, profile)
    output_dir = target_dir / rust_target if rust_target else target_dir
    return output_dir / profile_directory / binary


def engine_status(engine: str) -> dict[str, object]:
    if engine == "docker":
        command = ["docker", "version", "--format", "{{.Server.Os}}/{{.Server.Arch}}"]
    else:
        command = ["podman", "info", "--format", "{{.Host.Os}}/{{.Host.Arch}}"]
    try:
        result = subprocess.run(command, text=True, capture_output=True, check=True)
    except (FileNotFoundError, subprocess.CalledProcessError):
        return {"available": False, "server_platform": None}
    return {"available": True, "server_platform": result.stdout.strip()}


def registry_tag_status(values: dict[str, str], enabled: bool, engine: str, source: str) -> dict[str, object]:
    if not enabled:
        return {"checked": False, "status": "not_checked", "candidate_tag": None}

    try:
        reference = image_reference(values)
    except ValueError:
        return {"checked": False, "status": "not_configured", "candidate_tag": None, "configuration_source": source}
    command = ["docker", "buildx", "imagetools", "inspect", reference] if engine == "docker" else ["podman", "manifest", "inspect", reference]
    try:
        subprocess.run(command, text=True, capture_output=True, check=True)
    except FileNotFoundError:
        return {"checked": True, "status": "unknown", "candidate_tag": None, "reference": reference, "engine": engine, "configuration_source": source}
    except subprocess.CalledProcessError:
        return {"checked": True, "status": "unknown", "candidate_tag": None, "reference": reference, "engine": engine, "configuration_source": source}

    try:
        candidate_tag = increment_tag(values["IMAGE_TAG"])
    except ValueError:
        candidate_tag = None
    return {"checked": True, "status": "exists", "candidate_tag": candidate_tag, "reference": reference, "engine": engine, "configuration_source": source}


def collect_report(args: argparse.Namespace) -> dict[str, object]:
    source = args.source.expanduser().resolve()
    if not (source / "Cargo.toml").is_file():
        raise RuntimeError(f"not a Cargo workspace: {source}")

    metadata = cargo_metadata(source)
    targets = binary_targets(metadata)
    target_dir = Path(metadata["target_directory"])
    expected = expected_binary(target_dir, args.binary, args.profile, args.target)
    host_architecture = platform.machine()
    selected_target = next((target for target in targets if target["name"] == args.binary), None)
    environment = parse_env(source / ".env")
    supplied_values = (args.registry, args.repository, args.tag)
    if any(value is not None for value in supplied_values) and not all(value is not None for value in supplied_values):
        raise RuntimeError("--registry, --repository, and --tag must be supplied together")
    values: dict[str, str] = {"IMAGE_REGISTRY": args.registry or "", "IMAGE_REPOSITORY": args.repository or "", "IMAGE_TAG": args.tag or ""} if all(value is not None for value in supplied_values) else cast(dict[str, str], environment["values"])
    configuration_source = "arguments" if all(value is not None for value in supplied_values) else "environment"
    docker = engine_status("docker")
    podman = engine_status("podman")
    engine = args.engine if args.engine != "auto" else "docker" if docker["available"] else "podman"
    binary_details: dict[str, object] = {"platform": None, "error": None}
    if expected.is_file():
        try:
            binary_details["platform"] = next(name for name, machine in PLATFORM_MACHINES.items() if machine == inspect_elf(expected))
        except ValueError as exc:
            binary_details["error"] = str(exc)
    return {
        "host": {"os": platform.system(), "arch": host_architecture, "native_platform": native_platform(host_architecture)},
        "docker": docker,
        "podman": podman,
        "environment": environment,
        "registry_tag": registry_tag_status(values, args.check_registry_tag, engine, configuration_source),
        "cargo": {"workspace_root": metadata["workspace_root"], "target_dir": str(target_dir), "binaries": targets},
        "selected_binary": {"name": args.binary, "package": args.package, "cargo_target_exists": selected_target is not None, "target": selected_target},
        "expected_binary": {"profile": args.profile, "rust_target": args.target, "path": str(expected), "exists": expected.is_file(), "executable": os.access(expected, os.X_OK), "platform": binary_details["platform"], "validation_error": binary_details["error"], "mtime": datetime.fromtimestamp(expected.stat().st_mtime, timezone.utc).isoformat() if expected.exists() else None, "freshness": "unverified"},
    }


def main() -> int:
    try:
        print(json.dumps(collect_report(parse_arguments()), indent=2, sort_keys=True))
    except RuntimeError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
