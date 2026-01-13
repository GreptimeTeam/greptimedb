#!/bin/bash

echo "🚀 Setting up GreptimeDB development environment..."

# Create necessary directories
echo "📁 Creating necessary directories..."
mkdir -p /workspace/target
mkdir -p /workspace/logs
mkdir -p /workspace/data

# Set up environment variables in bashrc
echo "🌍 Setting up environment variables..."
cat >> ~/.bashrc << 'EOF'

# GreptimeDB development environment
export RUST_BACKTRACE=1
export CARGO_TERM_COLOR=always
export PROTOC=/usr/local/bin/protoc

# Aliases for common commands
alias gtdb='cargo run --bin greptime'
alias gtdb-test='cargo nextest run'
alias gtdb-check='cargo check --all-targets'
alias gtdb-clippy='cargo clippy --all-targets'
alias gtdb-fmt='cargo fmt --all'
alias gtdb-clean='cargo clean'

# Function to run GreptimeDB with common configurations
gtdb-run() {
    local mode=${1:-standalone}
    case $mode in
        standalone)
            cargo run --bin greptime -- standalone start
            ;;
        datanode)
            cargo run --bin greptime -- datanode start
            ;;
        frontend)
            cargo run --bin greptime -- frontend start
            ;;
        metasrv)
            cargo run --bin greptime -- metasrv start
            ;;
        *)
            echo "Usage: gtdb-run [standalone|datanode|frontend|metasrv]"
            return 1
            ;;
    esac
}

# Function to run tests with nextest
gtdb-test-filter() {
    local filter=$1
    if [ -z "$filter" ]; then
        echo "Usage: gtdb-test-filter <test_name_filter>"
        return 1
    fi
    cargo nextest run --filter-expr "test($filter)"
}

EOF

# Source the bashrc for the current session
source ~/.bashrc

# Install Python dependencies for any Python scripts
if [ -f "requirements.txt" ]; then
    echo "🐍 Installing Python dependencies..."
    pip3 install --user -r requirements.txt
fi

# Set up pre-commit hooks if available
if command -v pre-commit &> /dev/null; then
    echo "🔍 Setting up pre-commit hooks..."
    pre-commit install
fi

# Build the project to verify everything works
echo "🔨 Building the project to verify setup..."
cargo check --workspace

echo "✅ GreptimeDB development environment setup complete!"
echo ""
echo "🎯 Quick start commands:"
echo "  gtdb-run standalone  - Run GreptimeDB in standalone mode"
echo "  gtdb-test            - Run all tests with nextest"
echo "  gtdb-check           - Check all targets"
echo "  gtdb-clippy          - Run clippy lints"
echo ""
echo "📚 Available ports:"
echo "  3001 - HTTP API"
echo "  4000 - gRPC"
echo "  4001 - MySQL protocol"
echo "  4002 - PostgreSQL protocol"
echo "  5001 - Meta service"
echo "  5236 - Prometheus remote write"
echo "  9090 - Metrics endpoint"
echo ""
echo "Happy coding! 🦀"