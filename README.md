# GreptimeDB

[![codecov](https://codecov.io/gh/GrepTimeTeam/greptimedb/branch/develop/graph/badge.svg?token=FITFDI3J3C)](https://codecov.io/gh/GrepTimeTeam/greptimedb)

GreptimeDB: the next-generation hybrid timeseries/analytics processing database in the cloud.

## Getting Started
### Prerequisites
To compile GreptimeDB from source, you'll need the following:
- Rust
- C++ toolchain
- cmake
- OpenSSL

#### Rust
The easiest way to install Rust is to use [`rustup`](https://rustup.rs/), which will check our `rust-toolchain` file and install correct Rust version for you.

#### C++ toolchain
The [`prost-build`](https://github.com/tokio-rs/prost/tree/master/prost-build) dependency requires `C++ toolchain` and `cmake` to build its bundled `protoc`. For more info on what the required dependencies are check [`here`](https://github.com/protocolbuffers/protobuf/blob/master/src/README.md).

#### cmake
Follow the instructions for your operating system on the [`cmake`](https://cmake.org/install/) site.

For macOS users, you can also use `homebrew` to install `cmake`.
```bash
brew install cmake
```

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

