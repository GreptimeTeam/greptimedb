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

"""Run one packed Metric snapshot smoke, then one full S3 round trip."""

import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import threading
import time
import urllib.request

HERE = Path(__file__).resolve().parent


def command(args, **kwargs):
    result = subprocess.run(args, text=True, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, **kwargs)
    if result.returncode:
        # CLI argv and error output can contain storage credentials.
        raise RuntimeError(f"{args[0]} exited with status {result.returncode}")
    return result.stdout


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def metric_values(text):
    values = {}
    for line in text.splitlines():
        if line and not line.startswith("#"):
            key, value = line.rsplit(" ", 1)
            values[key] = float(value)
    return values


def docker_timestamp(value):
    # Docker emits nanoseconds; Python 3.9 accepts at most microseconds.
    return datetime.fromisoformat(re.sub(r"(\.\d{6})\d+", r"\1", value).replace("Z", "+00:00"))


def host_memory():
    if not Path("/proc/vmstat").exists():
        return {"available": False}
    vm = dict(line.split() for line in Path("/proc/vmstat").read_text().splitlines())
    mem = dict(line.split(":", 1) for line in Path("/proc/meminfo").read_text().splitlines())
    return {"available": True, "pswpin_pages": int(vm["pswpin"]),
            "pswpout_pages": int(vm["pswpout"]), "oom_kill": int(vm.get("oom_kill", 0)),
            **{key + "_kib": int(mem[key].split()[0])
               for key in ("MemTotal", "MemAvailable", "SwapTotal", "SwapFree")}}


def process_usage(pid):
    try:
        stat = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        return {"cpu_seconds": (int(stat[11]) + int(stat[12])) / os.sysconf("SC_CLK_TCK"),
                "rss_bytes": int(stat[21]) * os.sysconf("SC_PAGE_SIZE")}
    except (OSError, IndexError):
        return None


class Benchmark:
    def __init__(self, local_smoke):
        self.local_smoke = local_smoke
        self.image = os.environ["GREPTIMEDB_IMAGE"]
        self.commit = os.environ["EXPECTED_COMMIT"]
        if not re.fullmatch(r"greptime/greptimedb(?:-nightly)?@sha256:[0-9a-f]{64}", self.image):
            raise ValueError("GREPTIMEDB_IMAGE must be an immutable official image digest")
        if not re.fullmatch(r"[0-9a-f]{40}", self.commit):
            raise ValueError("EXPECTED_COMMIT must be a full commit SHA")
        self.per_group = int(os.environ.get("LOGICAL_TABLES_PER_GROUP", "10000"))
        if not 50 <= self.per_group <= 10000:
            raise ValueError("LOGICAL_TABLES_PER_GROUP must be in 50..10000")
        self.prefix = os.environ["RUN_PREFIX"]
        if not re.fullmatch(r"metric-snapshot-bench/[0-9]+-[0-9]+/", self.prefix):
            raise ValueError("RUN_PREFIX must be metric-snapshot-bench/<run>-<attempt>/")
        self.results = Path(os.environ["RESULTS"]).resolve()
        self.data = Path(os.environ["DATA_ROOT"]).resolve()
        self.results.mkdir(parents=True, exist_ok=False)
        self.data.mkdir(parents=True, exist_ok=False)
        self.network = "metric-snapshot-" + self.prefix.split("/")[1]
        self.server = self.network + "-db"
        self.cli_name = self.network + "-cli"
        self.port = int(os.environ.get("BENCH_HTTP_PORT", "19410"))
        self.base_url = f"http://127.0.0.1:{self.port}"
        self.endpoint = os.environ.get("S3_ENDPOINT", "")
        if self.endpoint and not local_smoke:
            raise ValueError("Custom S3 endpoints are restricted to local smoke")
        if not local_smoke and os.environ["S3_REGION"] != "ap-southeast-1":
            raise ValueError("Cloud benchmark requires ap-southeast-1")
        self.containers = set()
        self.network_created = False
        self.phases = []
        self.server_pid = 0
        self.round_dir = None

    def docker(self, *args):
        return command(["docker", *args])

    def metrics(self):
        try:
            with urllib.request.urlopen(self.base_url + "/metrics", timeout=5) as response:
                return response.read().decode()
        except OSError:
            return ""

    def phase(self, name, action):
        print(f"Starting {self.round_dir.name}/{name}", flush=True)
        before = host_memory()
        metrics_before = self.metrics()
        samples = []
        done = threading.Event()

        def sample():
            while not done.is_set():
                samples.append({"time": time.time(), "server": process_usage(self.server_pid),
                                "cli": process_usage(getattr(self, "cli_pid", 0)),
                                "query_memory": {k: v for k, v in metric_values(self.metrics()).items()
                                                 if "query_memory" in k} if self.server_pid else {}})
                done.wait(0.5)

        monitor = threading.Thread(target=sample, daemon=True)
        monitor.start()
        start = time.monotonic()
        outcome = "failed"
        result = None
        try:
            result = action()
            outcome = "passed"
            return result
        finally:
            elapsed = time.monotonic() - start
            done.set()
            monitor.join()
            after = host_memory()
            interfered = before["available"] and after["available"] and (
                after["pswpout_pages"] > before["pswpout_pages"] or after["oom_kill"] > before["oom_kill"])
            if interfered:
                outcome = "resource-invalid"
            record = {"round": self.round_dir.name, "phase": name, "outcome": outcome,
                      "wall_seconds": elapsed, "host_before": before, "host_after": after,
                      "process_samples": samples}
            if isinstance(result, dict):
                record.update(result)
            if before["available"] and after["available"]:
                record["new_swapout_pages"] = after["pswpout_pages"] - before["pswpout_pages"]
                record["new_oom_kills"] = after["oom_kill"] - before["oom_kill"]
            metrics_after = self.metrics()
            pre, post = metric_values(metrics_before), metric_values(metrics_after)
            record["server_counter_deltas"] = {
                key: value - pre.get(key, 0) for key, value in post.items()
                if ("opendal" in key and ("_count{" in key or "_sum{" in key))
                or key.startswith("greptime_servers_http_requests_total{")}
            write_json(self.round_dir / f"{name}-resources.json", record)
            (self.round_dir / f"{name}-before.prom").write_text(metrics_before)
            (self.round_dir / f"{name}-after.prom").write_text(metrics_after)
            self.phases.append({k: v for k, v in record.items() if k != "process_samples"})
            self.summary()
            print(f"Finished {name}: {outcome}, {elapsed:.3f}s", flush=True)
            if interfered:
                raise RuntimeError(f"Resource interference during {name}; evidence retained")

    def fixture(self, operation, per_group, output, source=None):
        args = [sys.executable, str(HERE / "metric-snapshot-fixture.py"), operation,
                "--addr", f"127.0.0.1:{self.port}", "--tables-per-group", str(per_group),
                "--output", str(output)]
        if source:
            args += ["--source", str(source)]
        with (self.round_dir / f"{operation}-{output.stem}.log").open("w") as log:
            status = subprocess.run(args, stdout=log, stderr=subprocess.STDOUT).returncode
        if status:
            raise RuntimeError(f"Fixture {operation} failed; see its artifact log")

    def start_server(self, role):
        root = self.data / self.round_dir.name / role
        root.mkdir(parents=True)
        evidence = self.round_dir / role
        evidence.mkdir()
        config = root / "config.toml"
        config.write_text('''enable_telemetry = false
[http]
addr = "0.0.0.0:4000"
[grpc]
bind_addr = "0.0.0.0:4001"
[mysql]
enable = false
[postgres]
enable = false
[storage]
type = "File"
data_home = "/data/db"
[query]
parallelism = 4
memory_pool_size = "2GiB"
[logging]
dir = "/evidence/logs"
level = "info"
''')
        self.containers.add(self.server)
        self.docker("run", "-d", "--name", self.server, "--network", self.network,
                    "--network-alias", "metric-db", "--user", f"{os.getuid()}:{os.getgid()}",
                    "--add-host", "host.docker.internal:host-gateway",
                    "-p", f"127.0.0.1:{self.port}:4000",
                    "-v", f"{root}:/data", "-v", f"{evidence}:/evidence",
                    self.image, "standalone", "start", "-c", "/data/config.toml")
        self.server_pid = int(self.docker("inspect", "-f", "{{.State.Pid}}", self.server))
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(self.base_url + "/v1/capabilities", timeout=2) as response:
                    caps = json.load(response)
                if any(caps.get(key) != 1 for key in
                       ("metric_packed_export", "metric_packed_import", "metric_batch_ddl")):
                    raise RuntimeError("Required packed snapshot capabilities are absent")
                write_json(evidence / "capabilities.json", caps)
                return
            except OSError:
                if self.docker("inspect", "-f", "{{.State.Running}}", self.server).strip() != "true":
                    break
                time.sleep(1)
        raise RuntimeError("Server did not become ready")

    def stop_server(self, role):
        self.docker("stop", "-t", "60", self.server)
        state = json.loads(self.docker("inspect", "-f", "{{json .State}}", self.server))
        write_json(self.round_dir / role / "exit.json", state)
        (self.round_dir / role / "stdout.log").write_text(self.docker("logs", self.server))
        self.docker("rm", self.server)
        self.containers.remove(self.server)
        self.server_pid = 0
        if state["OOMKilled"] or state["ExitCode"] != 0:
            raise RuntimeError(f"{role} server did not shut down cleanly")

    def cli(self, args, name):
        # Expand credentials only inside the container. Never persist argv or env.
        script = 'exec greptime "$@" --s3 --s3-region "$S3_REGION" --s3-access-key-id "$AWS_ACCESS_KEY_ID" --s3-secret-access-key "$AWS_SECRET_ACCESS_KEY"'
        if self.endpoint:
            args += ["--s3-endpoint", self.endpoint]
        self.containers.add(self.cli_name)
        self.docker("create", "--name", self.cli_name, "--network", self.network,
                    "--user", f"{os.getuid()}:{os.getgid()}",
                    "--add-host", "host.docker.internal:host-gateway",
                    "-e", "AWS_ACCESS_KEY_ID", "-e", "AWS_SECRET_ACCESS_KEY", "-e", "S3_REGION",
                    "-v", f"{self.round_dir}:/evidence", "--entrypoint", "/bin/sh", self.image,
                    "-c", script, "sh", "--log-dir", f"/evidence/{name}-logs", "cli", "data", *args)

        def execute():
            self.docker("start", self.cli_name)
            self.cli_pid = int(self.docker("inspect", "-f", "{{.State.Pid}}", self.cli_name))
            status = int(self.docker("wait", self.cli_name))
            state = json.loads(self.docker("inspect", "-f", "{{json .State}}", self.cli_name))
            (self.round_dir / f"{name}.log").write_text(self.docker("logs", self.cli_name))
            write_json(self.round_dir / f"{name}-exit.json", state)
            if status or state["ExitCode"] or state["OOMKilled"]:
                raise RuntimeError(f"{name} failed; see artifact log")
            return {"cli_wall_seconds": (docker_timestamp(state["FinishedAt"]) -
                                          docker_timestamp(state["StartedAt"])).total_seconds()}

        try:
            self.phase(name, execute)
        finally:
            self.cli_pid = 0
            if not (self.round_dir / f"{name}-exit.json").exists():
                self.save_container(self.cli_name, name + "-failure")
            self.docker("rm", "-f", self.cli_name)
            self.containers.remove(self.cli_name)

    def round(self, label, per_group):
        self.round_dir = self.results / label
        self.round_dir.mkdir()
        self.start_server("source")
        source = self.round_dir / "source-validation.json"
        self.phase("prepare", lambda: self.fixture("prepare", per_group, self.round_dir / "prepare.json"))
        self.phase("source-validation", lambda: self.fixture("validate", per_group, source))
        location = f"s3://{os.environ['S3_BUCKET']}/{self.prefix}{label}"
        self.cli(["export-v2", "create", "--addr", "metric-db:4000", "--to", location,
                  "--schemas", "metrics", "--pack-metric-data", "--parallelism", "4",
                  "--chunk-parallelism", "1", "--start-time", "1970-01-01T00:00:00Z",
                  "--end-time", "1970-01-01T00:00:01Z", "--timeout", "1h",
                  "--no-proxy", "--progress", "always"], "export")
        if not self.local_smoke:
            command(["uv", "run", str(HERE / "metric-snapshot-s3-ops.py"), "inventory",
                     "--output", str(self.round_dir / "s3-inventory.json")])
        self.stop_server("source")
        self.cli(["export-v2", "verify", "--snapshot", location], "verify")
        self.start_server("target")
        self.fixture("placeholders", per_group, self.round_dir / "placeholders.json")
        self.cli(["import-v2", "--addr", "metric-db:4000", "--from", location,
                  "--task-parallelism", "1", "--state-path", "/evidence/import-state.json",
                  "--timeout", "1h", "--no-proxy", "--progress", "always"], "import")
        imported = json.loads((self.round_dir / "import-resources.json").read_text())
        batch_requests = sum(value for key, value in imported["server_counter_deltas"].items()
                             if '/v1/ddl/logical-tables' in key and 'code="200"' in key)
        if batch_requests <= 0:
            raise RuntimeError("Import did not complete any successful batch DDL HTTP requests")
        write_json(self.round_dir / "import-completion.json", {"batch_ddl_requests": batch_requests,
                   "resume_state_removed": not (self.round_dir / "import-state.json").exists()})
        if (self.round_dir / "import-state.json").exists():
            raise RuntimeError("Successful import left its resume state file")
        self.phase("target-validation", lambda: self.fixture(
            "validate", per_group, self.round_dir / "target-validation.json", source))
        self.stop_server("target")

    def summary(self):
        lines = ["# Metric packed snapshot benchmark", "", f"Image: `{self.image}`",
                 f"Source commit: `{self.commit}`", "",
                 "Local smoke only; timings are not AWS results." if self.local_smoke else
                 "AWS S3 in ap-southeast-1; source and target use local File storage.", "",
                 "| Round | Phase | Seconds | Result |", "| --- | --- | ---: | --- |"]
        for phase in self.phases:
            lines.append(f"| {phase['round']} | {phase['phase']} | {phase['wall_seconds']:.3f} | {phase['outcome']} |")
        lines += ["", "Each logical table has 32 rows, 10 overlapping string tags, a timestamp and a DOUBLE.",
                  "Phase durations include Docker start, wait and log collection. cli_wall_seconds records Docker StartedAt to FinishedAt separately; setup, loading and full validation are separate.",
                  "Process CPU/RSS are sampled every 0.5 s on Linux, not exact process peaks. Linux host swap/OOM counters are checked per phase; configured swap is not RAM.",
                  "Raw server Prometheus snapshots and CLI progress/logs are retained. OpenDAL operation counts are not AWS HTTP request counts: multipart requests, retries and CLI operations are not fully covered. Exact AWS request totals are unavailable."]
        (self.results / "summary.md").write_text("\n".join(lines) + "\n")

    def run(self):
        self.docker("pull", self.image)
        version = self.docker("run", "--rm", self.image, "--version")
        (self.results / "version.txt").write_text(version)
        if self.commit not in version or "clean: true" not in version:
            raise RuntimeError("Image version does not match the expected clean source commit")
        for args, required in ((["export-v2", "create"], "--pack-metric-data"),
                               (["import-v2"], "--task-parallelism")):
            help_text = self.docker("run", "--rm", self.image, "cli", "data", *args, "--help")
            (self.results / (args[0] + "-help.txt")).write_text(help_text)
            if required not in help_text:
                raise RuntimeError(f"Image CLI lacks {required}")
        write_json(self.results / "host.json", {"memory": host_memory(), "local_smoke": self.local_smoke,
                    "platform": self.docker("image", "inspect", "-f", "{{.Os}}/{{.Architecture}}", self.image).strip()})
        self.docker("network", "create", self.network)
        self.network_created = True
        self.round("smoke", 50)
        if not self.local_smoke and self.per_group != 50:
            self.round("full", self.per_group)

    def save_container(self, name, label):
        state = subprocess.run(["docker", "inspect", "-f", "{{json .State}}", name],
                               capture_output=True, text=True)
        (self.round_dir / (label + "-state.json")).write_text(state.stdout)
        result = subprocess.run(["docker", "logs", name], capture_output=True, text=True)
        (self.round_dir / (label + ".log")).write_text(result.stdout + result.stderr)

    def cleanup(self):
        for name in self.containers:
            if self.round_dir:
                self.save_container(name, name + "-failure")
            subprocess.run(["docker", "rm", "-f", name], stdout=subprocess.DEVNULL, check=False)
        if self.network_created:
            subprocess.run(["docker", "network", "rm", self.network], stdout=subprocess.DEVNULL, check=False)
        self.summary()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["run"])
    parser.add_argument("--local-smoke", action="store_true")
    args = parser.parse_args()
    bench = Benchmark(args.local_smoke)

    def cancelled(signum, frame):
        raise RuntimeError(f"Received signal {signum}")

    signal.signal(signal.SIGTERM, cancelled)
    try:
        bench.run()
    finally:
        bench.cleanup()


if __name__ == "__main__":
    main()
