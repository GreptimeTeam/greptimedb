# GreptimeDB DevContainer

This devcontainer configuration provides a complete development environment for GreptimeDB with all necessary dependencies pre-installed.

## Features

### Pre-installed Dependencies
- **Rust toolchain**: nightly-2025-10-01 (matches project requirements)
- **Protocol Buffers**: v29.3 (protoc compiler)
- **Essential tools**: etcd, MinIO client, Node.js
- **Database clients**: PostgreSQL and MySQL clients for testing
- **Development tools**: cargo-nextest, cargo-watch, cargo-expand, cargo-edit, cargo-audit

### VS Code Extensions
- **Rust**: rust-analyzer, Even Better TOML, crates
- **Debugging**: CodeLLDB
- **General**: Error Lens, JSON, YAML, Docker, Kubernetes
- **Python**: Python extension with formatting and linting

### Port Forwarding
The devcontainer automatically forwards these ports:
- **3001**: HTTP API
- **4000**: gRPC
- **4001**: MySQL protocol
- **4002**: PostgreSQL protocol
- **5001**: Meta service
- **5002**: Meta service (HTTP)
- **5236**: Prometheus remote write
- **5432**: PostgreSQL protocol
- **9090**: Metrics endpoint
- **9091**: Jaeger UI

## Quick Start

1. **Open in VS Code**: Use "Remote-Containers: Open Folder in Container..."
2. **Wait for setup**: The post-create script will run automatically
3. **Verify installation**: Run `cargo check --workspace`

## Useful Commands

The devcontainer provides several aliases for common tasks:

```bash
# Run GreptimeDB
gtdb-run standalone    # Run in standalone mode
gtdb-run datanode      # Run as datanode
gtdb-run frontend      # Run as frontend
gtdb-run metasrv       # Run as meta service

# Development
gtdb-test              # Run all tests with nextest
gtdb-check             # Check all targets
gtdb-clippy            # Run clippy lints
gtdb-fmt               # Format code
gtdb-clean             # Clean build artifacts

# Filtered testing
gtdb-test-filter <name>  # Run tests matching name
```

## Building and Running

### Standalone Mode
```bash
cargo run --bin greptime -- standalone start
```

### Distributed Mode
Start individual components:
```bash
# Terminal 1: Meta service
cargo run --bin greptime -- metasrv start

# Terminal 2: Datanode
cargo run --bin greptime -- datanode start

# Terminal 3: Frontend
cargo run --bin greptime -- frontend start
```

## Testing

Run the test suite:
```bash
# All tests
cargo nextest run

# Specific test
cargo nextest run --filter-expr "test(name)"

# Integration tests
cargo test --test integration
```

## Configuration

The devcontainer uses:
- **Base image**: Ubuntu 22.04
- **Rust version**: nightly-2025-10-01 (as specified in rust-toolchain.toml)
- **User**: vscode (non-root)
- **Workspace**: /workspace

## Troubleshooting

### Build Issues
1. Check Rust toolchain: `rustc --version`
2. Verify protoc: `protoc --version`
3. Clean build: `cargo clean && cargo check`

### Container Issues
1. Rebuild container: "Remote-Containers: Rebuild Container"
2. Check logs: View container logs in VS Code
3. Manual setup: Run `.devcontainer/post-create.sh` manually

### Performance
- The container includes build caches in `/workspace/target`
- Use `cargo nextest` for faster test execution
- Enable incremental compilation in dev profile

## Architecture Support

The devcontainer supports both AMD64 and ARM64 architectures:
- Protocol Buffers compiler automatically detects architecture
- All tools are installed with appropriate binaries
- Rust toolchain supports both architectures

## Security

- Runs as non-root user (`vscode`)
- Uses official base images
- Minimal attack surface with only necessary tools
- No sensitive data in container image