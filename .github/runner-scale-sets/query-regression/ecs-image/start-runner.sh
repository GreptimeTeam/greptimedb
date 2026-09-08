#!/bin/bash
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

# Configure and start the ephemeral GitHub Actions runner on an ECS instance.
# Baked into the custom image at /opt/ephemeral-github-runner/ by
# build-ecs-image.py; the per-run environment is written by cloud-init to
# /etc/ephemeral-github-runner.env.
set -euo pipefail

# shellcheck disable=SC1091
source /etc/ephemeral-github-runner.env

export HOME=/home/runner
cd /home/runner

if [[ ! -f /home/runner/.runner ]]; then
  runuser -u runner -- ./config.sh \
    --url "${REPO_URL}" \
    --token "${RUNNER_TOKEN}" \
    --name "${RUNNER_NAME}" \
    --labels "${RUNNER_LABELS}" \
    --ephemeral --unattended --replace --disableupdate
fi

exec runuser -u runner -- ./run.sh
