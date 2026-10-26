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

"""Offline contract coverage for the fixed Native-vs-Legacy vmbench adapter."""

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from types import SimpleNamespace

SCRIPT = Path(__file__).resolve().parents[2] / ".github/scripts/native-promql-vmbench-lite.py"
DESCRIPTOR = Path(__file__).parent / "native_promql_vmbench_lite/benchmark.toml"
spec = importlib.util.spec_from_file_location("vmbench_adapter", SCRIPT)
adapter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(adapter)


class AdapterContractTest(unittest.TestCase):
    def test_real_toml_descriptor_and_strict_types(self):
        expected = adapter.validate_descriptor(DESCRIPTOR)
        self.assertEqual(expected["phase"], "smoke")
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "descriptor.toml"
            source = DESCRIPTOR.read_text()
            for original, changed in (
                ('serial_tries = 2', 'serial_tries = true'),
                ('concurrencies = [1, 4, 8]', 'concurrencies = [1, true, 8]'),
                ('schema_version = 1', 'schema_version = 2'),
                ('atol = 1e-12', 'atol = -1.0'),
            ):
                path.write_text(source.replace(original, changed))
                with self.subTest(changed=changed), self.assertRaises(ValueError):
                    adapter.validate_descriptor(path)
            path.write_text(source + 'unexpected = 1\n')
            with self.assertRaises(ValueError):
                adapter.validate_descriptor(path)

    def test_gh_run_url_is_provenance_not_database_address(self):
        self.assertEqual(adapter.DB_URL, "http://127.0.0.1:14240/v1/prometheus/api/v1/query_range")
        args = type("Args", (), {"url": adapter.DB_URL, "start": "2025-03-21T09:50:00Z",
                                  "end": "2025-03-21T10:20:00Z", "step": "30s",
                                  "base_file": Path("base.sh"), "external_warmup": 0})()
        with tempfile.TemporaryDirectory() as temp:
            command, _, _ = adapter.query_commands(Path("python"), Path("bench.py"), Path("q.txt"),
                                                   Path(temp), "lite-01", "serial", False, args, 1, 2, None, False)
        self.assertIn(adapter.DB_URL, command)
        self.assertNotIn("github.com", command)
        self.assertEqual(adapter.header(False), "X-Greptime-Hints:query.experimental_native_promql=false")
        self.assertEqual(adapter.header(True), "X-Greptime-Hints:query.experimental_native_promql=true")

    def test_full_clean_binary_identity_required(self):
        expected = "a" * 40
        good = "GreptimeDB\ncommit: " + expected + "\nclean: true\nversion: 1.4.0\n"
        with tempfile.TemporaryDirectory() as temp:
            binary = Path(temp) / "greptime"
            binary.write_bytes(b"executable")
            binary.chmod(0o755)
            for version, ok in ((good, True), (good.replace("clean: true", "clean: false"), False),
                                (good.replace(expected, "b" * 40), False), ("commit " + expected, False)):
                with patch.object(adapter.subprocess, "run", return_value=type("Run", (), {
                    "returncode": 0, "stdout": version, "stderr": ""})()):
                    if ok:
                        self.assertEqual(adapter.version_identity(binary, expected)["clean"], True)
                    else:
                        with self.assertRaises(ValueError):
                            adapter.version_identity(binary, expected)

    def test_counterbalance_and_serial_split(self):
        for query_index in range(12):
            first = adapter.lane_order(query_index, 0)
            second = adapter.lane_order(query_index, 1)
            self.assertEqual(set(first), {"legacy", "native"})
            self.assertEqual(first, list(reversed(second)))
        self.assertEqual(adapter.split_tries(50), (25, 25))
        self.assertEqual(adapter.split_tries(3), (1, 2))
        with self.assertRaises(ValueError):
            adapter.split_tries(1)

    def test_client_output_rejects_non_success_or_wrong_shape(self):
        query = "up"
        row = {"status": "success", "query": query, "start": "start", "end": "end", "step": "30s",
               "latency_ms": 1.0, "request_headers": {"X-Greptime-Hints": "query.experimental_native_promql=false"},
               "series": 2, "points": 61, "warmup": False, "start": "2025-03-21T09:50:00Z",
               "end": "2025-03-21T10:20:00Z"}
        summary = {"query_count": 1, "successful_requests": 1, "measured_requests": 1, "total_requests": 1, "errors": 0, "timeouts": 0, "per_query": [{"success": 1, "errors": 0, "timeouts": 0}]}
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp); sp = root / "summary.json"; op = root / "rows.jsonl"
            sp.write_text(json.dumps(summary)); op.write_text(json.dumps(row) + "\n")
            good = adapter.validate_client_result(sp, op, query, False, 1, {"series": 2, "points": 61})
            self.assertEqual(len(good), 1)
            for mutate in (
                lambda r: r.update(status="error"), lambda r: r.update(points=60),
                lambda r: r.update(warmup=True), lambda r: r.update(latency_ms=float("nan")),
                lambda r: r.update(request_headers={}),
            ):
                broken = dict(row); mutate(broken); op.write_text(json.dumps(broken) + "\n")
                with self.assertRaises(RuntimeError):
                    adapter.validate_client_result(sp, op, query, False, 1, {"series": 2, "points": 61})

    def test_main_run_startup_evidence_uses_existing_parent_and_plans_before_generation(self):
        class StopAfterStartup(Exception):
            pass

        class Managed:
            def __init__(self, target, root):
                self.target, self.root, self.process = target, root, None
                events.append("managed-created")
            def start(self):
                self.root.mkdir(parents=True, exist_ok=True)
                self.process = SimpleNamespace(pid=314, poll=lambda: None)
                events.append("start")
            def wait_for_health(self, url, timeout):
                self.assert_url = url
            def ensure_running(self):
                pass
            def evidence(self, url, directory):
                # Match ManagedGreptimeDB.evidence's exact documented filesystem behavior.
                directory.mkdir(exist_ok=True)
                (directory / "process-state.json").write_text("{}")
                evidence_dirs.append(directory)
            def stop(self):
                events.append("stop")
                return {"status":"pass"}

        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); deps=root/"deps"; o=deps/"o11ybench"; t=deps/"tsdg"
            o.mkdir(parents=True); t.mkdir()
            (root/"logs/dataset/original").mkdir(parents=True)
            events=[]; evidence_dirs=[]; commands=[]
            args=SimpleNamespace(candidate_ref="a"*40,base_ref="b"*40,candidate_bin="candidate",run_url="https://github.com/org/repo/actions/runs/7",base_file=None)
            d=adapter.validate_descriptor(DESCRIPTOR)
            def fake_run(command, **kwargs):
                commands.append(command)
                if str(command[1]).endswith("o11ybench_plan.py"):
                    plan=Path(command[command.index("--output-plan")+1])
                    plan.write_text(json.dumps({"data":{"generate":True},"targets":[{"url":adapter.DB_URL}],"query":{"concurrencies":[1,4,8]}}))
                    events.append("planner")
                elif str(command[1]).endswith("run_benchmark_plan.py"):
                    events.append("dry-run")
                elif str(command[1]).endswith("download_data.py"):
                    events.append("generation")
                    Path(kwargs["env"]["DATA_HOME"],"original.bin").write_bytes(b"dataset")
            def fake_urlopen(url, timeout=10):
                events.append("http:"+str(url))
                return SimpleNamespace(read=lambda: b"ok", __enter__=lambda self:self, __exit__=lambda *args:None)
            fake_manager=SimpleNamespace(ManagedGreptimeDB=Managed)
            def pinned_head(command, **kwargs):
                return adapter.O_SHA if command[2] == str(o) else adapter.T_SHA
            with patch.object(adapter, "run", side_effect=fake_run), \
                 patch.object(adapter.subprocess, "check_output", side_effect=pinned_head), \
                 patch.dict("sys.modules", {"managed_greptimedb":fake_manager}), \
                 patch.object(adapter.urllib.request, "urlopen", side_effect=fake_urlopen), \
                 patch.object(adapter, "load_remote_write", side_effect=StopAfterStartup):
                with self.assertRaises(StopAfterStartup):
                    adapter.main_run(args,d,root,{"sha256":"c"*64})
            self.assertLess(events.index("planner"),events.index("dry-run"))
            self.assertLess(events.index("dry-run"),events.index("generation"))
            planner_command=next(command for command in commands if str(command[1]).endswith("o11ybench_plan.py"))
            self.assertIn("existing-url:name=narthex-same-build,family=greptimedb,url="+adapter.DB_URL,planner_command)
            self.assertEqual(args.run_url,"https://github.com/org/repo/actions/runs/7")
            self.assertEqual(evidence_dirs,[root/"logs/server-startup-evidence"])
            self.assertTrue((root/"logs/server-startup-evidence/process-state.json").is_file())
            self.assertIn("stop",events)

    def test_warmup_calls_are_unmeasured_and_measured_shapes_are_validated(self):
        args=SimpleNamespace(url=adapter.DB_URL,start="2025-03-21T09:50:00Z",end="2025-03-21T10:20:00Z",step="30s",base_file=Path("base.sh"),external_warmup=1)
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            serial,_,_=adapter.query_commands(Path("python"),Path("bench.py"),Path("query.txt"),root,"lite-01","serial",False,args,1,1,None,True)
            concurrent,_,_=adapter.query_commands(Path("python"),Path("bench.py"),Path("query.txt"),root,"lite-01","concurrent-duration",True,args,4,None,1.0,True)
            self.assertEqual(serial[serial.index("--tries")+1],"1")
            self.assertEqual(serial[serial.index("--warmup")+1],"0")
            self.assertEqual(concurrent[concurrent.index("--duration-seconds")+1],"1.0")
            self.assertEqual(concurrent[concurrent.index("--warmup-seconds")+1],"0")
            row={"status":"success","query":"up","start":"2025-03-21T09:50:00Z","end":"2025-03-21T10:20:00Z","step":"30s","latency_ms":1.0,"request_headers":{"X-Greptime-Hints":"query.experimental_native_promql=false"},"series":1,"points":2,"warmup":False}
            summary={"query_count":1,"successful_requests":1,"measured_requests":1,"total_requests":1,"errors":0,"timeouts":0,"per_query":[{"success":1,"errors":0,"timeouts":0}]}
            sp=root/"summary.json"; op=root/"rows.jsonl"; sp.write_text(json.dumps(summary)); op.write_text(json.dumps(row)+"\n")
            self.assertFalse(adapter.validate_client_result(sp,op,"up",False,1,{"series":1,"points":2})[0]["warmup"])
            for key,value in (("successful_requests",0),("measured_requests",0),("total_requests",2)):
                broken=dict(summary); broken[key]=value; sp.write_text(json.dumps(broken))
                with self.assertRaises(RuntimeError): adapter.validate_client_result(sp,op,"up",False,1,{"series":1,"points":2})

    def test_process_cpu_rss_parser_and_sampling_failure_terminates_client(self):
        with patch.object(adapter.Path, "read_text", side_effect=["7 (proc name) S 1 2 3 4 5 6 7 8 9 10 11 12 13 14", "VmRSS: 2048 kB\n"]):
            ticks, rss = adapter.cpu_rss(99)
        self.assertEqual(ticks, 23)
        self.assertEqual(rss, 2)
        class Child:
            def __init__(self, *args, **kwargs): self.returncode = None; self.terminated = False
            def poll(self): return self.returncode
            def terminate(self): self.terminated = True; self.returncode = -15
            def wait(self, timeout=None): return self.returncode
        child = Child()
        with tempfile.TemporaryDirectory() as temp, patch.object(adapter, "cpu_rss", side_effect=[(1, 1.0), RuntimeError("sample failed")]), \
                patch.object(adapter.subprocess, "Popen", return_value=child):
            with self.assertRaisesRegex(RuntimeError, "sample failed"):
                adapter.execute_client(["bench"], {}, Path(temp), "measurement", 9)
        self.assertTrue(child.terminated)


if __name__ == "__main__":
    unittest.main()
