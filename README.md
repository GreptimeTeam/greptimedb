# GreptimeDB

[![codecov](https://codecov.io/gh/GrepTimeTeam/greptimedb/branch/develop/graph/badge.svg?token=FITFDI3J3C)](https://codecov.io/gh/GrepTimeTeam/greptimedb)

GreptimeDB: the next-generation hybrid timeseries/analytics processing database in the cloud.

## Getting Started

### Prerequisites
To compile GreptimeDB from source, you'll need the following:
- Rust
- Protobuf
- OpenSSL

#### Rust
The easiest way to install Rust is to use [`rustup`](https://rustup.rs/), which will check our `rust-toolchain` file and install correct Rust version for you.

#### Protobuf
`protoc` is required for compiling `.proto` files. `protobuf` is available from
major package manager on macos and linux distributions.

#### OpenSSL

For Ubuntu:
```bash
sudo apt install libssl-dev
```

For RedHat-based: Fedora, Oracle Linux, etc:
```bash
sudo dnf install openssl-devel
```

For macOS:
```bash
brew install openssl
```

## Usage

```
// Start datanode with default options.
cargo run -- datanode start

OR

// Start datanode with `http-addr` option.
cargo run -- datanode start --http-addr=0.0.0.0:9999

OR

// Start datanode with `log-dir` and `log-level` options.
cargo run -- --log-dir=logs --log-level=debug datanode start
```
