#!/bin/bash

set -euxo pipefail

cd "$(mktemp -d)"
# Fix version to v1.6.6, this is different than the latest version in original install script in
# https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh
base_url="https://github.com/cargo-bins/cargo-binstall/releases/download/v1.6.6/cargo-binstall-"

os="$(uname -s)"
if [ "$os" == "Darwin" ]; then
    url="${base_url}universal-apple-darwin.zip"
    curl -LO --proto '=https' --tlsv1.2 -sSf "$url"
    unzip cargo-binstall-universal-apple-darwin.zip
elif [ "$os" == "Linux" ]; then
    machine="$(uname -m)"
    if [ "$machine" == "armv7l" ]; then
        machine="armv7"
    fi
    target="${machine}-unknown-linux-musl"
    if [ "$machine" == "armv7" ]; then
        target="${target}eabihf"
    fi

    url="${base_url}${target}.tgz"
    curl -L --proto '=https' --tlsv1.2 -sSf "$url" | tar -xvzf -
elif [ "${OS-}" = "Windows_NT" ]; then
    machine="$(uname -m)"
    target="${machine}-pc-windows-msvc"
    url="${base_url}${target}.zip"
    curl -LO --proto '=https' --tlsv1.2 -sSf "$url"
    unzip "cargo-binstall-${target}.zip"
else
    echo "Unsupported OS ${os}"
    exit 1
fi

./cargo-binstall -y --force cargo-binstall

CARGO_HOME="${CARGO_HOME:-$HOME/.cargo}"

if ! [[ ":$PATH:" == *":$CARGO_HOME/bin:"* ]]; then
    if [ -n "${CI:-}" ] && [ -n "${GITHUB_PATH:-}" ]; then
        echo "$CARGO_HOME/bin" >> "$GITHUB_PATH"
    else
        echo
        printf "\033[0;31mYour path is missing %s, you might want to add it.\033[0m\n" "$CARGO_HOME/bin"
        echo
    fi
fi
