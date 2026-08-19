#!/usr/bin/env bash
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

# Bootstrap an Ubuntu 24.04 host as a query-regression self-hosted runner.
#
# This is the host-native equivalent of the ECS image contract: it
# installs docker-ce from Docker's official repository, builds the runner
# image from the sibling Dockerfile (the single source of the tool contract),
# materializes the toolchain onto the host, and optionally registers the
# runner and installs its systemd service.
#
# Large state lives under --data-root (a data disk): docker's data-root, the
# runner home, and the rustup/cargo toolchain roots, symlinked back to the
# hard-coded contract paths (/home/runner, /opt/rustup, /opt/cargo).
#
# The script is idempotent: re-running refreshes the toolchain in place, and
# the runner registration (.runner/.credentials) and _work survive. A live
# runner service is stopped first and restarted at the end.
#
# Usage:
#   sudo bash bootstrap-runner-host.sh --repo-dir /path/to/greptimedb
#   sudo RUNNER_TOKEN=<token> bash bootstrap-runner-host.sh --repo-dir . \
#     --register --repo-url https://github.com/<owner>/<repo>
#
# Options:
#   --repo-dir PATH    greptimedb checkout containing the runner Dockerfile (required)
#   --data-root PATH   data disk mount point (default: /data)
#   --uid / --gid N    runner user id (default: 3141; the ECS image contract is 1001)
#   --register         also register the runner and install the systemd service
#   --repo-url URL     repository URL for registration (required with --register)
#   --runner-name N    runner name (default: qreg-host)
#   --labels L         runner labels (default: perf-regression-8-cores)
#
# With --register, provide a fresh registration token via RUNNER_TOKEN.

set -euo pipefail

REPO_DIR=""
DATA_ROOT="/data"
RUNNER_UID="3141"
RUNNER_GID="3141"
REGISTER="false"
REPO_URL=""
RUNNER_NAME="qreg-host"
RUNNER_LABELS="perf-regression-8-cores"
IMAGE_TAG="qreg-runner:manual"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo-dir) REPO_DIR="$2"; shift 2 ;;
    --data-root) DATA_ROOT="$2"; shift 2 ;;
    --uid) RUNNER_UID="$2"; shift 2 ;;
    --gid) RUNNER_GID="$2"; shift 2 ;;
    --register) REGISTER="true"; shift ;;
    --repo-url) REPO_URL="$2"; shift 2 ;;
    --runner-name) RUNNER_NAME="$2"; shift 2 ;;
    --labels) RUNNER_LABELS="$2"; shift 2 ;;
    -h | --help) sed -n '17,40p' "$0"; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; exit 2 ;;
  esac
done

[[ $EUID -eq 0 ]] || { echo "Run as root, e.g. sudo bash $0 ..." >&2; exit 1; }
[[ -n "${REPO_DIR}" ]] || { echo "--repo-dir is required" >&2; exit 2; }
DOCKERFILE="${REPO_DIR}/.github/runner-scale-sets/query-regression/Dockerfile"
[[ -f "${DOCKERFILE}" ]] || { echo "Dockerfile not found at ${DOCKERFILE}" >&2; exit 1; }
if [[ "${REGISTER}" == "true" ]]; then
  [[ -n "${REPO_URL}" ]] || { echo "--repo-url is required with --register" >&2; exit 2; }
  [[ -n "${RUNNER_TOKEN:-}" ]] || { echo "RUNNER_TOKEN env is required with --register" >&2; exit 2; }
fi

step() { printf '\n==> %s\n' "$*"; }

# A re-run refreshes files under the (possibly live) runner service, so stop
# it first and restart it at the end (unless --register re-creates it).
RUNNER_SERVICE="$(systemctl list-units --type=service --all --no-legend 'actions.runner.*' 2>/dev/null | awk '{print $1}' | head -n 1 || true)"
RUNNER_WAS_ACTIVE="false"
if [[ -n "${RUNNER_SERVICE}" ]] && systemctl is-active --quiet "${RUNNER_SERVICE}"; then
  step "Stop running runner service ${RUNNER_SERVICE}"
  systemctl stop "${RUNNER_SERVICE}"
  RUNNER_WAS_ACTIVE="true"
fi

step "Install docker-ce from Docker's official repository"
for pkg in docker.io docker-doc docker-compose docker-compose-v2 podman-docker containerd runc; do
  apt-get remove -y "${pkg}" 2>/dev/null || true
done
apt-get update
apt-get install -y ca-certificates curl
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
chmod a+r /etc/apt/keyrings/docker.asc
# shellcheck disable=SC1091
. /etc/os-release
tee /etc/apt/sources.list.d/docker.sources > /dev/null <<EOF
Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: ${UBUNTU_CODENAME:-$VERSION_CODENAME}
Components: stable
Architectures: $(dpkg --print-architecture)
Signed-By: /etc/apt/keyrings/docker.asc
EOF
apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin

step "Move docker data-root to ${DATA_ROOT}/docker"
systemctl stop docker containerd
mkdir -p "${DATA_ROOT}/docker"
if [[ -f /etc/docker/daemon.json ]] && ! grep -q '"data-root"' /etc/docker/daemon.json; then
  echo "ERROR: /etc/docker/daemon.json exists without data-root; merge manually." >&2
  exit 1
fi
echo "{ \"data-root\": \"${DATA_ROOT}/docker\" }" > /etc/docker/daemon.json
systemctl start containerd docker
docker info | grep "Docker Root Dir"

step "Build the runner image"
docker build --platform linux/amd64 -f "${DOCKERFILE}" -t "${IMAGE_TAG}" "${REPO_DIR}"

step "Materialize the toolchain onto the host"
mkdir -p "${DATA_ROOT}/opt" "${DATA_ROOT}/runner" /opt /home
container="$(docker create "${IMAGE_TAG}")"
trap 'docker rm -f "${container}" >/dev/null 2>&1 || true' EXIT
# The `/.` suffix copies directory *contents*, so re-running over an existing
# materialization refreshes it in place instead of nesting runner/runner.
# Files absent from the image (runner registration, _work) are preserved.
docker cp "${container}:/home/runner/." "${DATA_ROOT}/runner"
docker cp "${container}:/opt/rustup/." "${DATA_ROOT}/opt/rustup"
docker cp "${container}:/opt/cargo/." "${DATA_ROOT}/opt/cargo"
for tool in uv uvx otelgen sccache; do
  docker cp "${container}:/usr/local/bin/${tool}" "/usr/local/bin/${tool}"
done
docker rm "${container}" > /dev/null
trap - EXIT
ln -sfn "${DATA_ROOT}/runner" /home/runner
ln -sfn "${DATA_ROOT}/opt/rustup" /opt/rustup
ln -sfn "${DATA_ROOT}/opt/cargo" /opt/cargo
docker image rm "${IMAGE_TAG}" > /dev/null

step "Install system packages"
apt-get install -y --no-install-recommends \
  build-essential clang cmake git gzip jq libprotobuf-dev libssl-dev mold \
  openssh-client pkg-config protobuf-compiler python3 sudo tar unzip wget xz-utils zip zstd

step "Create runner user (${RUNNER_UID}:${RUNNER_GID}) and environment"
getent group "${RUNNER_GID}" > /dev/null || groupadd -g "${RUNNER_GID}" runner
id -u runner > /dev/null 2>&1 || useradd -u "${RUNNER_UID}" -g "${RUNNER_GID}" -d /home/runner -s /bin/bash runner
chown -R "${RUNNER_UID}:${RUNNER_GID}" "${DATA_ROOT}/runner"
echo 'PATH=/opt/cargo/bin:/usr/local/bin:/usr/bin:/bin' > "${DATA_ROOT}/runner/.env"
chown "${RUNNER_UID}:${RUNNER_GID}" "${DATA_ROOT}/runner/.env"

if [[ "${REGISTER}" == "true" ]]; then
  step "Register runner ${RUNNER_NAME} (labels: ${RUNNER_LABELS})"
  if [[ -n "${RUNNER_SERVICE}" ]]; then
    (cd /home/runner && ./svc.sh uninstall) || true
  fi
  runuser -u runner -- bash -c "cd /home/runner && HOME=/home/runner ./config.sh \
    --url '${REPO_URL}' --token '${RUNNER_TOKEN}' --name '${RUNNER_NAME}' \
    --labels '${RUNNER_LABELS}' --unattended --replace --disableupdate"

  step "Install and start the runner service"
  cd /home/runner
  ./svc.sh install runner
  ./svc.sh start
  ./svc.sh status
else
  step "Skipping registration (pass --register --repo-url ... with RUNNER_TOKEN to enable)"
  if [[ "${RUNNER_WAS_ACTIVE}" == "true" ]]; then
    step "Restart runner service ${RUNNER_SERVICE}"
    systemctl start "${RUNNER_SERVICE}"
    systemctl --no-pager status "${RUNNER_SERVICE}" || true
  fi
fi

step "Done"
echo "Runner home: /home/runner -> ${DATA_ROOT}/runner"
echo "Before each workflow run on this persistent host, clean transient cargo state:"
echo "  sudo rm -f /home/runner/.cargo/.package-cache"
echo "  sudo rm -rf /home/runner/.cargo/.global-cache"
