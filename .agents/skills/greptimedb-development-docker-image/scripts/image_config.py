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

"""Bounded parsing and safe updating of the image settings in a .env file."""

import os
import re
import tempfile
from pathlib import Path
from typing import Mapping

MANAGED_KEYS = ("IMAGE_REGISTRY", "IMAGE_REPOSITORY", "IMAGE_TAG")
_REGISTRY = re.compile(r"[a-z0-9][a-z0-9.-]*(?::[0-9]+)?(?:/[a-z0-9][a-z0-9._-]*)*")
_REPOSITORY = re.compile(r"[a-z0-9][a-z0-9._-]*(?:/[a-z0-9][a-z0-9._-]*)*")
_TAG = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}")


def _value(text: str) -> str:
    text = text.strip()
    if not text:
        return ""
    quote = text[0] if text[0] in "'\"" else None
    if quote:
        end = text.find(quote, 1)
        if end < 0 or text[end + 1 :].strip() and not text[end + 1 :].lstrip().startswith("#"):
            raise ValueError("unmatched quote or trailing value syntax")
        return text[1:end]
    # A comment is inline only when introduced by whitespace.  This keeps
    # valid values such as an image tag containing '#'.
    match = re.search(r"\s+#", text)
    if match:
        text = text[: match.start()].rstrip()
    if any(character in text for character in "'\";$`"):
        raise ValueError("unsupported shell syntax in .env value")
    return text


def parse_env(text: str) -> dict[str, str]:
    """Parse the three managed keys, without validating their values."""
    lines = text.splitlines()
    result: dict[str, str] = {}
    for number, original in enumerate(lines, 1):
        line = original.strip()
        if not line or line.startswith("#"):
            continue
        export = re.match(r"export[ \t]+", line)
        if export:
            line = line[export.end() :].lstrip()
        key, separator, raw = line.partition("=")
        if not separator:
            continue
        key = key.strip()
        if key in MANAGED_KEYS:
            try:
                result[key] = _value(raw)
            except ValueError as error:
                raise ValueError(f"line {number}: {error}") from error
    return result


def parse_env_file(path: Path) -> dict[str, str]:
    return parse_env(path.read_text() if path.exists() else "")


def normalize_values(values: Mapping[str, str]) -> dict[str, str]:
    return {
        "IMAGE_REGISTRY": values.get("IMAGE_REGISTRY", ""),
        "IMAGE_REPOSITORY": values.get("IMAGE_REPOSITORY", ""),
        "IMAGE_TAG": values.get("IMAGE_TAG", ""),
    }


def validate_values(values: Mapping[str, str]) -> None:
    values = normalize_values(values)
    missing = [key for key in ("IMAGE_REPOSITORY", "IMAGE_TAG") if not values.get(key)]
    if missing:
        raise ValueError("missing managed values: " + ", ".join(missing))
    registry = values.get("IMAGE_REGISTRY", "")
    if registry and (
        not _REGISTRY.fullmatch(registry)
    ):
        raise ValueError("invalid registry")
    if registry:
        host = registry.split("/", 1)[0]
        if host != "localhost" and "." not in host and ":" not in host:
            raise ValueError("ambiguous registry; use an FQDN, explicit port, or localhost")
    if not _REPOSITORY.fullmatch(values["IMAGE_REPOSITORY"]):
        raise ValueError("invalid repository")
    if not _TAG.fullmatch(values["IMAGE_TAG"]):
        raise ValueError("invalid tag")
    if any(any(character.isspace() for character in value) for value in values.values()):
        raise ValueError("managed values cannot contain whitespace")


def image_reference(values: Mapping[str, str]) -> str:
    values = normalize_values(values)
    validate_values(values)
    prefix = values.get("IMAGE_REGISTRY", "")
    repository = values["IMAGE_REPOSITORY"]
    return f"{prefix}/{repository}:{values['IMAGE_TAG']}" if prefix else f"{repository}:{values['IMAGE_TAG']}"


def update_env(path: Path, values: Mapping[str, str]) -> bool:
    """Atomically update managed entries; return whether the file changed."""
    values = normalize_values(values)
    validate_values(values)
    if path.is_symlink():
        raise ValueError("refusing to update symlink .env")
    old = path.read_text() if path.exists() else ""
    parse_env(old)  # detect malformed managed syntax before changing anything
    lines = old.splitlines()
    output: list[str] = []
    written: set[str] = set()
    for line in lines:
        stripped = line.strip()
        export = re.match(r"export[ \t]+", stripped)
        candidate = stripped[export.end() :].lstrip() if export else stripped
        key = candidate.split("=", 1)[0].strip() if "=" in candidate else ""
        if key in MANAGED_KEYS:
            if key in written:
                continue
            output.append(f"{key}={values[key]}")
            written.add(key)
        else:
            output.append(line)
    output.extend(f"{key}={values.get(key, '')}" for key in MANAGED_KEYS if key not in written)
    new = "\n".join(output) + "\n"
    if old == new:
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    mode = os.stat(path).st_mode & 0o777 if path.exists() else 0o600
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(fd, mode)
        with os.fdopen(fd, "w") as handle:
            handle.write(new)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return True
