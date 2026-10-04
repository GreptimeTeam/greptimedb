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

import json
import os
import stat
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
CHECK_VERSION_SCRIPT = SCRIPT_DIR / "check-version.sh"


def write_python_executable(path: Path, source: str) -> None:
    path.write_text(
        f"#!{sys.executable}\n{textwrap.dedent(source)}",
        encoding="utf-8",
    )
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


class CheckVersionTest(unittest.TestCase):
    def run_check_version(
        self, current_version: str, latest_version: str
    ) -> tuple[subprocess.CompletedProcess[str], dict[str, str]]:
        with tempfile.TemporaryDirectory(prefix="check-version-test-") as temp_dir:
            temp_path = Path(temp_dir)
            mock_bin = temp_path / "bin"
            mock_bin.mkdir()
            output_path = temp_path / "github-output"
            output_path.touch()

            write_python_executable(
                mock_bin / "curl",
                """
                import os
                import sys

                sys.stdout.write(os.environ["MOCK_CURL_RESPONSE"])
                """,
            )
            write_python_executable(
                mock_bin / "jq",
                """
                import json
                import sys

                payload = json.load(sys.stdin)
                query = sys.argv[-1]
                if query == ".message":
                    value = payload.get("message")
                elif query == ".tag_name":
                    value = payload.get("tag_name")
                else:
                    raise SystemExit(f"unsupported jq query: {query}")
                print("null" if value is None else value)
                """,
            )

            environment = os.environ.copy()
            environment.update(
                {
                    "GITHUB_OUTPUT": str(output_path),
                    "MOCK_CURL_RESPONSE": json.dumps({"tag_name": latest_version}),
                    "PATH": f"{mock_bin}{os.pathsep}{environment['PATH']}",
                }
            )
            completed = subprocess.run(
                [str(CHECK_VERSION_SCRIPT), current_version],
                cwd=SCRIPT_DIR.parent.parent,
                env=environment,
                capture_output=True,
                text=True,
                check=False,
            )
            outputs = dict(
                line.split("=", 1)
                for line in output_path.read_text(encoding="utf-8").splitlines()
                if line
            )

        return completed, outputs

    def assert_version_outputs(
        self,
        name: str,
        current_version: str,
        latest_version: str,
        expected_stable: str,
        expected_latest: str,
    ) -> None:
        completed, outputs = self.run_check_version(current_version, latest_version)
        failure_details = (
            f"{name}: current={current_version}, latest={latest_version}, "
            f"returncode={completed.returncode}, stdout={completed.stdout!r}, "
            f"stderr={completed.stderr!r}, outputs={outputs!r}"
        )
        self.assertEqual(completed.returncode, 0, failure_details)
        self.assertEqual(
            outputs,
            {
                "is-current-version-stable": expected_stable,
                "is-current-version-latest": expected_latest,
            },
            failure_details,
        )

    def test_version_classification(self) -> None:
        cases = [
            ("stable-newer", "v1.2.4", "v1.2.3", "true", "true"),
            ("stable-older", "v1.2.2", "v1.2.3", "true", "false"),
            ("beta-newer-base", "v1.2.4-beta.1", "v1.2.3", "false", "true"),
            ("beta-against-same-stable", "v1.2.3-beta.1", "v1.2.3", "false", "false"),
            ("rc", "v1.2.4-rc.1", "v1.2.3", "false", "true"),
            ("nightly", "v1.2.4-nightly-20250101", "v1.2.3", "false", "true"),
            ("build-suffix", "v1.2.4-build.1", "v1.2.3", "false", "true"),
            ("invalid-version", "v1.2", "v1.2.3", "false", "false"),
        ]

        for case in cases:
            with self.subTest(name=case[0]):
                self.assert_version_outputs(*case)

    def test_empty_input_fails_without_outputs(self) -> None:
        completed, outputs = self.run_check_version("", "v1.2.3")
        failure_details = (
            f"empty-input: returncode={completed.returncode}, stdout={completed.stdout!r}, "
            f"stderr={completed.stderr!r}, outputs={outputs!r}"
        )
        self.assertNotEqual(completed.returncode, 0, failure_details)
        self.assertEqual(outputs, {}, failure_details)


if __name__ == "__main__":
    unittest.main(verbosity=2)
