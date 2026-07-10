#!/usr/bin/env python3
"""Collect read-only build context for a GreptimeDB development image."""

import argparse
import json
import os
import platform
import subprocess
import sys
from pathlib import Path
from typing import Optional

from next_image_tag import increment_tag


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
    parser.add_argument(
        "--check-registry-tag",
        action="store_true",
        help="inspect the configured image tag with Docker without modifying it",
    )
    return parser.parse_args()


def native_platform(architecture: str) -> Optional[str]:
    return PLATFORM_ARCHITECTURES.get(architecture.lower())


def parse_env(env_path: Path) -> dict[str, object]:
    values: dict[str, str] = {}
    if env_path.is_file():
        for line in env_path.read_text().splitlines():
            key, separator, value = line.partition("=")
            if separator and key in ENV_KEYS:
                values[key] = value
    missing = [key for key in ENV_KEYS if not values.get(key)]
    return {"path": str(env_path), "exists": env_path.is_file(), "values": values, "missing": missing}


def cargo_metadata(source: Path) -> dict[str, object]:
    command = ["cargo", "metadata", "--format-version=1", "--no-deps"]
    try:
        result = subprocess.run(command, cwd=source, text=True, capture_output=True, check=True)
    except FileNotFoundError as error:
        raise RuntimeError("cargo is not installed or is not on PATH") from error
    except subprocess.CalledProcessError as error:
        detail = error.stderr.strip() or error.stdout.strip()
        raise RuntimeError(f"cargo metadata failed: {detail}") from error
    return json.loads(result.stdout)


def binary_targets(metadata: dict[str, object]) -> list[dict[str, object]]:
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


def docker_server_platform() -> dict[str, object]:
    command = ["docker", "version", "--format", "{{.Server.Os}}/{{.Server.Arch}}"]
    try:
        result = subprocess.run(command, text=True, capture_output=True, check=True)
    except (FileNotFoundError, subprocess.CalledProcessError):
        return {"available": False, "server_platform": None}
    return {"available": True, "server_platform": result.stdout.strip()}


def registry_tag_status(environment: dict[str, object], enabled: bool) -> dict[str, object]:
    if not enabled:
        return {"checked": False, "status": "not_checked", "candidate_tag": None}

    values = environment["values"]
    if environment["missing"]:
        return {"checked": False, "status": "not_configured", "candidate_tag": None}

    registry = values["IMAGE_REGISTRY"].rstrip("/")
    repository = values["IMAGE_REPOSITORY"].lstrip("/")
    reference = f"{registry}/{repository}:{values['IMAGE_TAG']}" if registry else f"{repository}:{values['IMAGE_TAG']}"
    command = ["docker", "buildx", "imagetools", "inspect", reference]
    try:
        subprocess.run(command, text=True, capture_output=True, check=True)
    except FileNotFoundError:
        return {"checked": True, "status": "unknown", "candidate_tag": None}
    except subprocess.CalledProcessError:
        return {"checked": True, "status": "unknown", "candidate_tag": None}

    try:
        candidate_tag = increment_tag(values["IMAGE_TAG"])
    except ValueError:
        candidate_tag = None
    return {"checked": True, "status": "exists", "candidate_tag": candidate_tag}


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
    return {
        "host": {"os": platform.system(), "arch": host_architecture, "native_platform": native_platform(host_architecture)},
        "docker": docker_server_platform(),
        "environment": environment,
        "registry_tag": registry_tag_status(environment, args.check_registry_tag),
        "cargo": {"workspace_root": metadata["workspace_root"], "target_dir": str(target_dir), "binaries": targets},
        "selected_binary": {"name": args.binary, "cargo_target_exists": selected_target is not None, "target": selected_target},
        "expected_binary": {"profile": args.profile, "rust_target": args.target, "path": str(expected), "exists": expected.is_file(), "executable": os.access(expected, os.X_OK)},
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
