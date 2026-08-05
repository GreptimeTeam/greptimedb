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

"""Keep the two license configs in sync with the enterprise feature gate.

A Rust file reachable only through `#[cfg(feature = "enterprise")] mod ...;` is
governed by the GreptimeDB Enterprise License, so it must be listed in the
`includes` of `licenserc-enterprise.toml` and in the `excludes` of
`licenserc.toml`.

hawkeye cannot catch a miss here: such a file that still carries the Apache-2.0
header passes the default check precisely because it was never excluded from it.
"""

import re
import subprocess
import sys
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
APACHE_CONFIG = "licenserc.toml"
ENTERPRISE_CONFIG = "licenserc-enterprise.toml"
ENTERPRISE_EXCLUDES_START = "# enterprise:start"
ENTERPRISE_EXCLUDES_END = "# enterprise:end"

ATTRIBUTE = re.compile(r"#\[[^\]]*\]")
MOD_DECL = re.compile(
    r"^\s*(?:pub\s*(?:\([^)]*\)\s*)?)?mod\s+([A-Za-z_][A-Za-z0-9_]*)\s*;"
)
MODULE_ROOTS = {"mod.rs", "lib.rs", "main.rs"}
CFG_ATTRIBUTE = re.compile(r"#\[\s*cfg\s*\((.*)\)\s*\]")
ENTERPRISE_FEATURE = re.compile(r'feature\s*=\s*"enterprise"')


def cfg_requires_enterprise(attribute):
    """Whether `attribute` prevents the item from compiling without enterprise."""
    match = CFG_ATTRIBUTE.fullmatch(attribute)
    if not match:
        return False
    predicate = match.group(1).strip()
    if not ENTERPRISE_FEATURE.search(predicate):
        return False
    if ENTERPRISE_FEATURE.fullmatch(predicate):
        return True

    all_match = re.fullmatch(r"all\((.*)\)", predicate)
    if all_match:
        terms = [term.strip() for term in all_match.group(1).split(",")]
        return any(ENTERPRISE_FEATURE.fullmatch(term) for term in terms)

    return False


def parse_mod_decls(source):
    """Yield `(module_name, enterprise_gated)` for each `mod <name>;` declaration.

    Inline `mod <name> { ... }` is skipped: it has no file of its own.
    """
    attributes = []
    for line in source.splitlines():
        rest = line.lstrip()
        while rest.startswith("#["):
            attribute = ATTRIBUTE.match(rest)
            if not attribute:
                break
            attributes.append(attribute.group())
            rest = rest[attribute.end() :].lstrip()

        match = MOD_DECL.match(rest)
        if match:
            gated = any(cfg_requires_enterprise(attr) for attr in attributes)
            yield match.group(1), gated
            attributes = []
            continue

        stripped = rest.strip()
        if stripped and not stripped.startswith("//"):
            attributes = []


def child_module_dir(path):
    """Directory holding the submodules declared by `path`."""
    return path.parent if path.name in MODULE_ROOTS else path.parent / path.stem


def resolve_module(parent, name):
    directory = child_module_dir(parent)
    for candidate in (directory / f"{name}.rs", directory / name / "mod.rs"):
        if candidate.is_file():
            return candidate
    return None


def collect_gated_files(rust_files):
    """Return the files behind every enterprise-gated module, plus unresolved declarations.

    Submodules of a gated module are gated too, so the walk keeps descending.
    """
    pending = [
        (path, name)
        for path in rust_files
        for name, gated in parse_mod_decls(path.read_text(encoding="utf-8"))
        if gated
    ]

    gated_files = set()
    unresolved = []
    while pending:
        parent, name = pending.pop()
        target = resolve_module(parent, name)
        if target is None:
            unresolved.append((parent, name))
            continue
        if target in gated_files:
            continue
        gated_files.add(target)
        source = target.read_text(encoding="utf-8")
        pending.extend((target, child) for child, _ in parse_mod_decls(source))

    return gated_files, unresolved


def tracked_rust_files(repo_root):
    """Rust files under version control.

    Untracked files are left out on purpose: a scratch copy of a gated module
    lying around a working tree would otherwise fail the check. Anything headed
    for a PR is tracked by the time CI sees it.
    """
    result = subprocess.run(
        ["git", "-C", str(repo_root), "ls-files", "*.rs"],
        capture_output=True,
        text=True,
        check=True,
    )
    return [repo_root / line for line in result.stdout.splitlines() if line]


def load_paths(config_path, key):
    with open(config_path, "rb") as config:
        return set(tomllib.load(config).get(key, []))


def load_marked_paths(config_path, start_marker, end_marker):
    """Load a TOML array fragment delimited by comment markers."""
    source = config_path.read_text(encoding="utf-8")
    try:
        fragment = source.split(start_marker, 1)[1].split(end_marker, 1)[0]
    except IndexError as error:
        raise ValueError(
            f"Missing {start_marker!r} or {end_marker!r} in {config_path}"
        ) from error
    return set(tomllib.loads(f"paths = [{fragment}]")["paths"])


def report(title, paths):
    print(f"\n{title}:")
    for path in sorted(paths):
        print(f'    "{path}",')


def main():
    rust_files = tracked_rust_files(REPO_ROOT)
    gated_files, unresolved = collect_gated_files(rust_files)
    gated = {path.relative_to(REPO_ROOT).as_posix() for path in gated_files}

    enterprise_includes = load_paths(REPO_ROOT / ENTERPRISE_CONFIG, "includes")
    enterprise_excludes = load_marked_paths(
        REPO_ROOT / APACHE_CONFIG,
        ENTERPRISE_EXCLUDES_START,
        ENTERPRISE_EXCLUDES_END,
    )
    missing_includes = gated - enterprise_includes
    missing_excludes = gated - enterprise_excludes
    stale_includes = enterprise_includes - gated
    stale_excludes = enterprise_excludes - gated

    if not (
        missing_includes
        or missing_excludes
        or stale_includes
        or stale_excludes
        or unresolved
    ):
        print(f"Enterprise license lists are in sync ({len(gated)} gated files).")
        return

    if missing_includes:
        report(
            f"Enterprise-gated files missing from `includes` in {ENTERPRISE_CONFIG}",
            missing_includes,
        )
    if missing_excludes:
        report(
            f"Enterprise files missing from `excludes` in {APACHE_CONFIG} "
            "(the Apache-2.0 header must not be applied to them)",
            missing_excludes,
        )
    if stale_includes:
        report(
            f"Stale `includes` in {ENTERPRISE_CONFIG} (file is gone, or is no "
            'longer behind `#[cfg(feature = "enterprise")]`)',
            stale_includes,
        )
    if stale_excludes:
        report(
            f"Stale enterprise `excludes` in {APACHE_CONFIG} (file is gone, or "
            "is no longer enterprise-gated)",
            stale_excludes,
        )
    if unresolved:
        print("\nEnterprise-gated modules whose file could not be located:")
        for parent, name in sorted(unresolved):
            print(f"    mod {name}; declared in {parent.relative_to(REPO_ROOT)}")

    print(
        "\nAn enterprise-gated file carries the enterprise header instead of the "
        "Apache-2.0 one. After fixing both lists, run:"
        "\n    hawkeye format --config licenserc-enterprise.toml"
    )
    raise SystemExit(1)


if __name__ == "__main__":
    sys.exit(main())
