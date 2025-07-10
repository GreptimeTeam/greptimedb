#!/bin/sh

set -ue

OS_TYPE=
ARCH_TYPE=

# Set the GitHub token to avoid GitHub API rate limit.
# You can run with `GITHUB_TOKEN`:
#  GITHUB_TOKEN=<your_token> ./scripts/install.sh
GITHUB_TOKEN=${GITHUB_TOKEN:-}

VERSION=${1:-latest}
GITHUB_ORG=GreptimeTeam
GITHUB_REPO=greptimedb
BIN=greptime

get_os_type() {
  os_type="$(uname -s)"

  case "$os_type" in
    Darwin)
      OS_TYPE=darwin
      ;;
    Linux)
      OS_TYPE=linux
      ;;
    *)
      echo "Error: Unknown OS type: $os_type"
      exit 1
  esac
}

get_arch_type() {
  arch_type="$(uname -m)"

  case "$arch_type" in
    arm64)
      ARCH_TYPE=arm64
      ;;
    aarch64)
      ARCH_TYPE=arm64
      ;;
    x86_64)
      ARCH_TYPE=amd64
      ;;
    amd64)
      ARCH_TYPE=amd64
      ;;
    *)
      echo "Error: Unknown CPU type: $arch_type"
      exit 1
  esac
}

# Verify SHA256 checksum
verify_sha256() {
  file="$1"
  expected_sha256="$2"

  if command -v sha256sum >/dev/null 2>&1; then
    actual_sha256=$(sha256sum "$file" | cut -d' ' -f1)
  elif command -v shasum >/dev/null 2>&1; then
    actual_sha256=$(shasum -a 256 "$file" | cut -d' ' -f1)
  else
    echo "Warning: No SHA256 verification tool found (sha256sum or shasum). Skipping checksum verification."
    return 0
  fi

  if [ "$actual_sha256" = "$expected_sha256" ]; then
    echo "SHA256 checksum verified successfully."
    return 0
  else
    echo "Error: SHA256 checksum verification failed!"
    echo "Expected: $expected_sha256"
    echo "Actual: $actual_sha256"
    return 1
  fi
}

# Prompt for user confirmation (compatible with different shells)
prompt_confirmation() {
  message="$1"
  printf "%s (y/N): " "$message"

  # Try to read user input, fallback if read fails
  answer=""
  if read answer </dev/tty 2>/dev/null; then
    case "$answer" in
      [Yy]|[Yy][Ee][Ss])
        return 0
        ;;
      *)
        return 1
        ;;
    esac
  else
    echo ""
    echo "Cannot read user input. Defaulting to No."
    return 1
  fi
}

download_artifact() {
  if [ -n "${OS_TYPE}" ] && [ -n "${ARCH_TYPE}" ]; then
    # Use the latest stable released version.
    # GitHub API reference: https://docs.github.com/en/rest/releases/releases?apiVersion=2022-11-28#get-the-latest-release.
    if [ "${VERSION}" = "latest" ]; then
      # To avoid other tools dependency, we choose to use `curl` to get the version metadata and parsed by `sed`.
      VERSION=$(curl -sL \
        -H "Accept: application/vnd.github+json" \
        -H "X-GitHub-Api-Version: 2022-11-28" \
        ${GITHUB_TOKEN:+-H "Authorization: Bearer $GITHUB_TOKEN"} \
        "https://api.github.com/repos/${GITHUB_ORG}/${GITHUB_REPO}/releases/latest" | sed -n 's/.*"tag_name": "\([^"]*\)".*/\1/p')
      if [ -z "${VERSION}" ]; then
        echo "Failed to get the latest stable released version."
        exit 1
        fi
    fi

    echo "Downloading ${BIN}, OS: ${OS_TYPE}, Arch: ${ARCH_TYPE}, Version: ${VERSION}"
    PKG_NAME="${BIN}-${OS_TYPE}-${ARCH_TYPE}-${VERSION}"
    PACKAGE_NAME="${PKG_NAME}.tar.gz"
    SHA256_FILE="${PKG_NAME}.sha256sum"

    if [ -n "${PACKAGE_NAME}" ]; then
      # Check if files already exist and prompt for override
      if [ -f "${PACKAGE_NAME}" ]; then
        echo "File ${PACKAGE_NAME} already exists."
        if prompt_confirmation "Do you want to override it?"; then
          echo "Overriding existing file..."
          rm -f "${PACKAGE_NAME}"
        else
          echo "Skipping download. Using existing file."
        fi
      fi

      if [ -f "${BIN}" ]; then
        echo "Binary ${BIN} already exists."
        if prompt_confirmation "Do you want to override it?"; then
          echo "Will override existing binary..."
          rm -f "${BIN}"
        else
          echo "Installation cancelled."
          exit 0
        fi
      fi

      # Download package if not exists
      if [ ! -f "${PACKAGE_NAME}" ]; then
        echo "Downloading ${PACKAGE_NAME}..."
        # Use curl instead of wget for better compatibility
        if command -v curl >/dev/null 2>&1; then
          if ! curl -L -o "${PACKAGE_NAME}" "https://github.com/${GITHUB_ORG}/${GITHUB_REPO}/releases/download/${VERSION}/${PACKAGE_NAME}"; then
            echo "Error: Failed to download ${PACKAGE_NAME}"
            exit 1
          fi
        elif command -v wget >/dev/null 2>&1; then
          if ! wget -O "${PACKAGE_NAME}" "https://github.com/${GITHUB_ORG}/${GITHUB_REPO}/releases/download/${VERSION}/${PACKAGE_NAME}"; then
            echo "Error: Failed to download ${PACKAGE_NAME}"
            exit 1
          fi
        else
          echo "Error: Neither curl nor wget is available for downloading."
          exit 1
        fi
      fi

      # Download and verify SHA256 checksum
      echo "Downloading SHA256 checksum..."
      sha256_download_success=0
      if command -v curl >/dev/null 2>&1; then
        if curl -L -s -o "${SHA256_FILE}" "https://github.com/${GITHUB_ORG}/${GITHUB_REPO}/releases/download/${VERSION}/${SHA256_FILE}" 2>/dev/null; then
          sha256_download_success=1
        fi
      elif command -v wget >/dev/null 2>&1; then
        if wget -q -O "${SHA256_FILE}" "https://github.com/${GITHUB_ORG}/${GITHUB_REPO}/releases/download/${VERSION}/${SHA256_FILE}" 2>/dev/null; then
          sha256_download_success=1
        fi
      fi

      if [ $sha256_download_success -eq 1 ] && [ -f "${SHA256_FILE}" ]; then
        expected_sha256=$(cat "${SHA256_FILE}" | cut -d' ' -f1)
        if [ -n "$expected_sha256" ]; then
          if ! verify_sha256 "${PACKAGE_NAME}" "${expected_sha256}"; then
            echo "SHA256 verification failed. Removing downloaded file."
            rm -f "${PACKAGE_NAME}" "${SHA256_FILE}"
            exit 1
          fi
        else
          echo "Warning: Could not parse SHA256 checksum from file."
        fi
        rm -f "${SHA256_FILE}"
      else
        echo "Warning: Could not download SHA256 checksum file. Skipping verification."
      fi

      # Extract the binary and clean the rest.
      echo "Extracting ${PACKAGE_NAME}..."
      if ! tar xf "${PACKAGE_NAME}"; then
        echo "Error: Failed to extract ${PACKAGE_NAME}"
        exit 1
      fi

      # Find the binary in the extracted directory
      extracted_dir="${PACKAGE_NAME%.tar.gz}"
      if [ -f "${extracted_dir}/${BIN}" ]; then
        mv "${extracted_dir}/${BIN}" "${PWD}/"
        rm -f "${PACKAGE_NAME}"
        rm -rf "${extracted_dir}"
        chmod +x "${BIN}"
        echo "Installation completed successfully!"
        echo "Run './${BIN} --help' to get started"
      else
        echo "Error: Binary ${BIN} not found in extracted archive"
        rm -f "${PACKAGE_NAME}"
        rm -rf "${extracted_dir}"
        exit 1
      fi
    fi
  fi
}

get_os_type
get_arch_type
download_artifact
