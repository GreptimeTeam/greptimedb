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

"""Apply one reviewed, pinned benchmark-only cache-capacity patch before data work."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess

LANES = {
    "agent": ("2f40fb3fdbdc", "scripts/run-agent-observability.sh"),
    "vmbench": ("b43785b56f42", "scripts/run_benchmark_plan.py"),
    "traces": ("77a84e1c8136", "scripts/run_tracesbench_docker.py"),
}


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(command, log):
    result = subprocess.run(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    log.write_text(result.stdout)
    result.check_returncode()
    return result.stdout


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lane", choices=LANES, required=True)
    parser.add_argument("--checkout", type=Path, required=True)
    parser.add_argument("--expected-revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    # A second invocation must not replace evidence from the first attempt.
    output.mkdir(parents=True, exist_ok=False)
    state = {"status": "started", "lane": args.lane,
             "expected_revision": args.expected_revision, "apply_script_sha256": sha(Path(__file__))}
    def save():
        temporary = output / "status.json.tmp"
        temporary.write_text(json.dumps(state, indent=2) + "\n")
        temporary.replace(output / "status.json")
    save()
    try:
        # Probe before modifying the checkout or beginning data generation.
        import tomllib  # noqa: F401
        if shutil.which("patch") is None:
            raise RuntimeError("patch executable is required")
        checkout = args.checkout.resolve(strict=True)
        prefix, launcher = LANES[args.lane]
        if not re.fullmatch(r"[0-9a-f]{40}", args.expected_revision) or not args.expected_revision.startswith(prefix):
            raise ValueError("Runtime revision is not the reviewed lane pin")
        revision = run(["git", "-C", str(checkout), "rev-parse", "HEAD"], output / "revision.txt").strip()
        state["original_revision"] = revision
        if revision != args.expected_revision:
            raise ValueError("Checkout and resolved runtime revision differ")
        dirty = run(["git", "-C", str(checkout), "status", "--porcelain", "--untracked-files=all"], output / "status-before.txt")
        if dirty:
            raise ValueError("Benchmark checkout must be clean, including untracked files")
        patch = Path(__file__).resolve().parents[1] / "benchmark-patches" / (args.lane + ".patch")
        state["patch_sha256"] = sha(patch)
        shutil.copyfile(patch, output / patch.name)
        expected = {launcher, "scripts/cache_capacity_evidence.py"}
        targets = [line[6:] for line in patch.read_text().splitlines() if line.startswith("+++ b/")]
        if set(targets) != expected or len(targets) != len(expected):
            raise ValueError("Patch changes files outside the reviewed lane")
        state["modified_files"] = {name: {"before_sha256": sha(checkout / name) if (checkout / name).exists() else None} for name in sorted(expected)}
        save()
        command = ["patch", "--batch", "--forward", "--fuzz=0", "-p1", "-d", str(checkout), "-i", str(patch)]
        run(command + ["--dry-run"], output / "patch-dry-run.log")
        run(command, output / "patch-apply.log")
        status = run(["git", "-C", str(checkout), "status", "--porcelain", "--untracked-files=all"], output / "status-after.txt")
        actual = {line[3:] for line in status.splitlines()}
        if actual != expected:
            raise ValueError("Unexpected modified files after patch application")
        for name in expected:
            state["modified_files"][name]["after_sha256"] = sha(checkout / name)
        run(["git", "-C", str(checkout), "diff", "--check"], output / "diff-check.txt")
        state["status"] = "passed"
        save()
    except Exception as error:
        state.update(status="failed", error=str(error))
        save()
        raise


if __name__ == "__main__":
    main()
