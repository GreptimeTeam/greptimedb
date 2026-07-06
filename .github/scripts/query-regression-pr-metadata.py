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

"""Write metadata consumed by the trusted query-regression comment workflow."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default=Path("query-regression-pr.json"), type=Path)
    args = parser.parse_args()

    metadata = {
        "pr_number": int(os.environ["PR_NUMBER"]),
        "head_sha": os.environ["HEAD_SHA"],
        "base_sha": os.environ["BASE_SHA"],
        "head_repo": os.environ["HEAD_REPO"],
        "base_repo": os.environ["BASE_REPO"],
        "run_id": int(os.environ["RUN_ID"]),
        "run_attempt": int(os.environ["RUN_ATTEMPT"]),
    }
    args.output.write_text(json.dumps(metadata, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
