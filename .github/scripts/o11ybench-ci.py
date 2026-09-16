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

"""Input, image provenance and reporting helpers; benchmark orchestration stays in YAML."""

import argparse
import json
import os
import re
import subprocess
from pathlib import Path

TARGETS = ("greptimedb", "clickhouse", "victorialogs")
REPOSITORIES = {
    "greptimedb": "greptime/greptimedb",
    "clickhouse": "clickhouse/clickhouse-server",
    "victorialogs": "victoriametrics/victoria-logs",
}


def selected_targets(value):
    selected = value.split(",") if value != "all" else list(TARGETS)
    if (
        not selected
        or any(t not in TARGETS for t in selected)
        or len(set(selected)) != len(selected)
    ):
        raise ValueError(
            "targets must be all or unique comma-separated greptimedb,clickhouse,victorialogs"
        )
    return [t for t in TARGETS if t in selected]


def validate(env):
    targets = selected_targets(env["TARGETS"])
    if env["PROFILE"] not in ("S", "P", "M"):
        raise ValueError("profile must be S (5K), P (10M) or M (100M)")
    if not re.fullmatch(r"[0-9a-f]{40}", env["O11YBENCH_REF"]):
        raise ValueError(
            "o11ybench_ref must be a full commit SHA matching the runtime image"
        )
    if not re.fullmatch(r"[1-9][0-9]*", env["DB_CPUS"]) or not re.fullmatch(
        r"[1-9][0-9]*[mMgG]", env["DB_MEMORY"]
    ):
        raise ValueError("invalid DB CPU or memory limit")
    runtime = env["RUNTIME_IMAGE"]
    if not re.fullmatch(
        r"[a-z0-9.-]+\.cr\.aliyuncs\.com/[a-z0-9_./-]+(?::[\w.-]+|@sha256:[0-9a-f]{64})",
        runtime,
    ):
        raise ValueError(
            "runtime_image must be an explicit Aliyun registry image tag or digest"
        )
    for target in targets:
        if not re.fullmatch(
            r"[\w][\w.-]{0,127}", env[target.upper() + "_TAG"], flags=re.ASCII
        ):
            raise ValueError(f"invalid image tag for {target}")
    return targets


def output(path, values):
    with Path(path).open("a") as f:
        f.writelines(f"{key}={value}\n" for key, value in values.items())


def docker_image(reference):
    subprocess.run(["docker", "pull", reference], check=True)
    return json.loads(
        subprocess.check_output(["docker", "image", "inspect", reference], text=True)
    )[0]


def prepare(env):
    targets = validate(env)
    root = Path(env["ARTIFACT_ROOT"])
    # generate owns creation of this directory and refuses to overwrite a previous run.
    if not root.is_absolute() or root.exists():
        raise ValueError("artifact root must be a new absolute directory")
    root.parent.mkdir(parents=True, exist_ok=True)
    actual = subprocess.check_output(
        ["git", "-C", env["O11YBENCH_DIR"], "rev-parse", "HEAD"], text=True
    ).strip()
    if actual != env["O11YBENCH_REF"]:
        raise ValueError("unexpected o11ybench checkout revision")
    runtime = docker_image(env["RUNTIME_IMAGE"])
    revision = (runtime.get("Config", {}).get("Labels") or {}).get(
        "org.opencontainers.image.revision"
    )
    if revision != actual:
        raise ValueError("runtime image revision must match o11ybench_ref")
    images = {
        "runtime": {
            "requested": env["RUNTIME_IMAGE"],
            "id": runtime["Id"],
            "digests": runtime.get("RepoDigests", []),
        }
    }
    exports = {"RESOLVED_RUNTIME_IMAGE": runtime["Id"]}
    for target in targets:
        reference = REPOSITORIES[target] + ":" + env[target.upper() + "_TAG"]
        image = docker_image(reference)
        images[target] = {
            "requested": reference,
            "id": image["Id"],
            "digests": image.get("RepoDigests", []),
        }
        exports[target.upper() + "_IMAGE"] = image["Id"]
    output(env["GITHUB_ENV"], exports)
    manifest = {
        "o11ybench_sha": actual,
        "targets": targets,
        "profile": env["PROFILE"],
        "images": images,
        "db_cpus": env["DB_CPUS"],
        "db_memory": env["DB_MEMORY"],
        "classification": "local_smoke_only",
        "publishable": False,
    }
    Path(env["MANIFEST_PATH"]).write_text(json.dumps(manifest, indent=2) + "\n")


def summarize(env):
    targets = selected_targets(env["TARGETS"])
    root = Path(env["ARTIFACT_ROOT"])
    if not root.exists():
        raise ValueError("dataset generation did not create the artifact directory")
    lines = [
        "## Agent observability benchmark",
        "",
        "Preflight only; no performance regression threshold.",
        "",
        "| Target | Passed | Requests | P50 ms | P95 ms | P99 ms |",
        "|---|---|---|---|---|---|",
    ]
    results = {}
    for target in targets:
        path = root / f"{target}-1" / f"{target}-1" / "measured-summary.json"
        exit_path = root / f"{target}-1" / "exit-code.txt"
        summary = json.loads(path.read_text()) if path.exists() else {}
        passed = (
            summary.get("passed") is True
            and summary.get("target") == target
            and exit_path.exists()
            and exit_path.read_text().strip() == "0"
        )
        results[target] = {"passed": passed, "summary": str(path.relative_to(root))}
        values = [
            target,
            str(passed),
            *(
                str(summary.get(k, "—"))
                for k in ("successful_requests", "p50_ms", "p95_ms", "p99_ms")
            ),
        ]
        lines.append("| " + " | ".join(values) + " |")
    comparison_passed = None
    if targets == list(TARGETS):
        comparison_path = root / "comparison.json"
        comparison = (
            json.loads(comparison_path.read_text()) if comparison_path.exists() else {}
        )
        comparison_passed = (
            comparison.get("passed") is True
            and comparison.get("cross_target_fingerprints_match") is True
        )
        lines.extend(["", f"Cross-target comparison passed: {comparison_passed}"])
    passed = (
        all(r["passed"] for r in results.values()) and comparison_passed is not False
    )
    (root / "run-summary.json").write_text(
        json.dumps(
            {
                "passed": passed,
                "targets": results,
                "comparison_passed": comparison_passed,
                "publishable": False,
            },
            indent=2,
        )
        + "\n"
    )
    report = "\n".join(lines) + "\n"
    (root / "summary.md").write_text(report)
    with Path(env["GITHUB_STEP_SUMMARY"]).open("a") as f:
        f.write(report)
    if not passed:
        raise ValueError(
            "selected targets or cross-target comparison failed or have no completed result"
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", choices=("validate", "prepare", "summarize"))
    args = parser.parse_args()
    if args.stage == "validate":
        targets = validate(os.environ)
        output(
            os.environ["GITHUB_OUTPUT"], {t: str(t in targets).lower() for t in TARGETS}
        )
    elif args.stage == "prepare":
        prepare(os.environ)
    else:
        summarize(os.environ)


if __name__ == "__main__":
    main()
