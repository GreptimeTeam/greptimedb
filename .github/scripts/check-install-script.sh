#!/bin/sh

set -e

# Get the latest version of github.com/GreptimeTeam/greptimedb
VERSION=$(curl -s https://api.github.com/repos/GreptimeTeam/greptimedb/releases/latest | jq -r '.tag_name')

echo "Downloading the latest version: $VERSION"

# Download the install script
curl -fsSL https://raw.githubusercontent.com/greptimeteam/greptimedb/main/scripts/install.sh | sh -s $VERSION

# Execute the `greptime` command
./greptime --version
