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

"""Small shared GitHub runner API and workflow-output helpers."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request


def github_api(token: str, method: str, path: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode("utf-8") if body is not None else None
    request = urllib.request.Request(
        f"https://api.github.com{path}",
        data=data,
        method=method,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            if response.status == 204:
                return {}
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        # GitHub's error body says exactly why (e.g. "Must have admin rights to
        # Repository" for a PAT without the required scope); surface it instead
        # of a bare "HTTP Error 403".
        body = error.read().decode("utf-8", "replace")
        raise SystemExit(
            f"GitHub API {method} {path} failed: HTTP {error.code}: {body}\n"
            "The token comes from the GH_PERSONAL_ACCESS_TOKEN secret; it needs "
            "'repo' scope (classic PAT) or 'Administration: write' on the "
            "repository (fine-grained PAT)."
        ) from error


def create_registration_token(github_token: str, repo: str) -> str:
    response = github_api(
        github_token,
        "POST",
        f"/repos/{repo}/actions/runners/registration-token",
        body={},
    )
    return response["token"]


def find_runner_by_name(github_token: str, repo: str, name: str) -> dict | None:
    page = 1
    while True:
        response = github_api(
            github_token,
            "GET",
            f"/repos/{repo}/actions/runners?per_page=100&page={page}",
        )
        runners = response.get("runners", [])
        for runner in runners:
            if runner.get("name") == name:
                return runner
        if len(runners) < 100:
            return None
        page += 1


def append_github_output(name: str, value: str) -> None:
    output_path = os.environ.get("GITHUB_OUTPUT")
    if output_path:
        with open(output_path, "a", encoding="utf-8") as output:
            output.write(f"{name}={value}\n")


def deregister_runner(token: str, repo: str, runner_name: str) -> bool:
    runner = find_runner_by_name(token, repo, runner_name)
    if runner is None:
        print(f"Runner {runner_name} is not registered", flush=True)
        return True
    try:
        github_api(token, "DELETE", f"/repos/{repo}/actions/runners/{runner['id']}")
        print(f"Deregistered runner {runner_name} (id {runner['id']})", flush=True)
        return True
    except Exception as error:  # noqa: BLE001
        print(f"Failed to deregister runner {runner_name}: {error}", flush=True)
        return False
