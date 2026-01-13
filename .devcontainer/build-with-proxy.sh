#!/bin/bash

# Script to build devcontainer with proper proxy handling
# This script handles the case where proxy is set to localhost which won't work in Docker

set -e

echo "=== DevContainer Build Script ==="

# Check if proxy is set to localhost/127.0.0.1
if [[ "$HTTP_PROXY" == *"127.0.0.1"* ]] || [[ "$HTTP_PROXY" == *"localhost"* ]]; then
    echo "⚠️  WARNING: Proxy is set to localhost ($HTTP_PROXY)"
    echo "   This won't work inside Docker containers."
    echo ""
    echo "Options:"
    echo "1. Build without proxy (recommended for local development)"
    echo "2. Use host network mode (requires proxy on host to be accessible)"
    echo "3. Specify a different proxy address"
    echo ""
    
    read -p "Choose option (1-3) or press Enter for option 1: " choice
    
    case $choice in
        2)
            echo "Building with host network mode..."
            # Use docker-compose with host network
            docker-compose -f .devcontainer/docker-compose.yml build --network=host
            echo "✅ Build completed with host network mode"
            echo "To start the container, run: docker-compose -f .devcontainer/docker-compose.yml up -d"
            exit 0
            ;;
        3)
            read -p "Enter proxy address (e.g., http://your-proxy:port): " new_proxy
            export HTTP_PROXY="$new_proxy"
            export HTTPS_PROXY="$new_proxy"
            echo "Using proxy: $new_proxy"
            ;;
        *)
            echo "Building without proxy..."
            # Unset proxy variables for this build
            unset HTTP_PROXY HTTPS_PROXY http_proxy https_proxy
            ;;
    esac
fi

# Build using devcontainer CLI
echo "Building devcontainer..."
devcontainer build --workspace-folder .

echo "✅ Build completed!"
echo ""
echo "To start the devcontainer:"
echo "1. In VS Code: Press F1 → 'Dev Containers: Reopen in Container'"
echo "2. Command line: devcontainer open ."
echo ""
echo "To use docker-compose directly:"
echo "docker-compose -f .devcontainer/docker-compose.yml up -d"