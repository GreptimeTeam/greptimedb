<p align="center">
  <picture>
    <source media="(prefers-color-scheme: light)" srcset="https://cdn.jsdelivr.net/gh/GreptimeTeam/greptimedb@develop/docs/logo-text-padding.png">
    <source media="(prefers-color-scheme: dark)" srcset="https://cdn.jsdelivr.net/gh/GreptimeTeam/greptimedb@develop/docs/logo-text-padding-dark.png">
    <img alt="GreptimeDB Logo" src="https://cdn.jsdelivr.net/gh/GreptimeTeam/greptimedb@develop/docs/logo-text-padding.png" width="400px">
  </picture>
</p>


<h3 align="center">
    The next-generation hybrid time-series/analytics processing database in the cloud
</h3>

<p align="center">
    <a href="https://codecov.io/gh/GrepTimeTeam/greptimedb"><img src="https://codecov.io/gh/GrepTimeTeam/greptimedb/branch/develop/graph/badge.svg?token=FITFDI3J3C"></img></a>
    &nbsp;
    <a href="https://github.com/GreptimeTeam/greptimedb/actions/workflows/develop.yml"><img src="https://github.com/GreptimeTeam/greptimedb/actions/workflows/develop.yml/badge.svg" alt="CI"></img></a>
    &nbsp;
    <a href="https://github.com/greptimeTeam/greptimedb/blob/develop/LICENSE"><img src="https://img.shields.io/github/license/greptimeTeam/greptimedb"></a>
</p>

<p align="center">
    <a href="https://twitter.com/greptime"><img src="https://img.shields.io/badge/twitter-follow_us-1d9bf0.svg"></a>
    &nbsp;
    <a href="https://www.linkedin.com/company/greptime/"><img src="https://img.shields.io/badge/linkedin-connect_with_us-0a66c2.svg"></a>
    &nbsp;
    <a href="https://greptime.com/slack"><img src="https://img.shields.io/badge/slack-GreptimeDB-0abd59?logo=slack" alt="slack" /></a>
</p>

> [!WARNING]  
> Our default branch has changed from `develop` to `main` (issue [#3025](https://github.com/GreptimeTeam/greptimedb/issues/3025)). Please update your local repository to use the `main` branch.

## What is GreptimeDB

GreptimeDB is an open-source time-series database with a special focus on
scalability, analytical capabilities and efficiency. It's designed to work on
infrastructure of the cloud era, and users benefit from its elasticity and commodity
storage.

Our core developers have been building time-series data platform
for years. Based on their best-practices, GreptimeDB is born to give you:

- A standalone binary that scales to highly-available distributed cluster, providing a transparent experience for cluster users
- Optimized columnar layout for handling time-series data; compacted, compressed, and stored on various storage backends
- Flexible indexes, tackling high cardinality issues down
- Distributed, parallel query execution, leveraging elastic computing resource
- Native SQL, and Python scripting for advanced analytical scenarios
- Widely adopted database protocols and APIs, native PromQL supports
- Extensible table engine architecture for extensive workloads

## Quick Start

### [GreptimePlay](https://greptime.com/playground)

Try out the features of GreptimeDB right from your browser.

### Build

#### Build from Source

To compile GreptimeDB from source, you'll need:

- C/C++ Toolchain: provides basic tools for compiling and linking. This is
  available as `build-essential` on ubuntu and similar name on other platforms.
- Rust: the easiest way to install Rust is to use
  [`rustup`](https://rustup.rs/), which will check our `rust-toolchain` file and
  install correct Rust version for you.
- Protobuf: `protoc` is required for compiling `.proto` files. `protobuf` is
  available from major package manager on macos and linux distributions. You can
  find an installation instructions [here](https://grpc.io/docs/protoc-installation/).
  **Note that `protoc` version needs to be >= 3.15** because we have used the `optional`
  keyword. You can check it with `protoc --version`.
- python3-dev or python3-devel(Optional feature, only needed if you want to run scripts
  in CPython, and also need to enable `pyo3_backend` feature when compiling(by `cargo run -F pyo3_backend` or add `pyo3_backend` to src/script/Cargo.toml 's `features.default` like `default = ["python", "pyo3_backend]`)): this install a Python shared library required for running Python
  scripting engine(In CPython Mode). This is available as `python3-dev` on
  ubuntu, you can install it with `sudo apt install python3-dev`, or
  `python3-devel` on RPM based distributions (e.g. Fedora, Red Hat, SuSE). Mac's
  `Python3` package should have this shared library by default. More detail for compiling with PyO3 can be found in [PyO3](https://pyo3.rs/v0.18.1/building_and_distribution#configuring-the-python-version)'s documentation.

#### Build with Docker

A docker image with necessary dependencies is provided:

```
docker build --network host -f docker/Dockerfile -t greptimedb .
```

### Run

Start GreptimeDB from source code, in standalone mode:

```
cargo run -- standalone start
```

Or if you built from docker:

```
docker run -p 4002:4002 -v "$(pwd):/tmp/greptimedb" greptime/greptimedb standalone start
```

Please see the online document site for more installation options and [operations info](https://docs.greptime.com/user-guide/operations/overview).

### Get started

Read the [complete getting started guide](https://docs.greptime.com/getting-started/overview) on our [official document site](https://docs.greptime.com/).

To write and query data, GreptimeDB is compatible with multiple [protocols and clients](https://docs.greptime.com/user-guide/clients/overview).

## Resources

### Installation

- [Pre-built Binaries](https://greptime.com/download):
  For Linux and macOS, you can easily download pre-built binaries including official releases and nightly builds that are ready to use.
  In most cases, downloading the version without PyO3 is sufficient. However, if you plan to run scripts in CPython (and use Python packages like NumPy and Pandas), you will need to download the version with PyO3 and install a Python with the same version as the Python in the PyO3 version.
  We recommend using virtualenv for the installation process to manage multiple Python versions.
- [Docker Images](https://hub.docker.com/r/greptime/greptimedb)(**recommended**): pre-built
  Docker images, this is the easiest way to try GreptimeDB. By default it runs CPython script with `pyo3_backend` enabled.
- [`gtctl`](https://github.com/GreptimeTeam/gtctl): the command-line tool for
  Kubernetes deployment

### Documentation

- GreptimeDB [User Guide](https://docs.greptime.com/user-guide/concepts/overview)
- GreptimeDB [Developer
  Guide](https://docs.greptime.com/developer-guide/overview.html)
- GreptimeDB [internal code document](https://greptimedb.rs)

### Dashboard
- [The dashboard UI for GreptimeDB](https://github.com/GreptimeTeam/dashboard)

### SDK

- [GreptimeDB C++ Client](https://github.com/GreptimeTeam/greptimedb-client-cpp)
- [GreptimeDB Erlang Client](https://github.com/GreptimeTeam/greptimedb-client-erl)
- [GreptimeDB Go Client](https://github.com/GreptimeTeam/greptimedb-client-go)
- [GreptimeDB Java Client](https://github.com/GreptimeTeam/greptimedb-client-java)
- [GreptimeDB Python Client](https://github.com/GreptimeTeam/greptimedb-client-py) (WIP)
- [GreptimeDB Rust Client](https://github.com/GreptimeTeam/greptimedb-client-rust)
- [GreptimeDB JavaScript Client](https://github.com/GreptimeTeam/greptime-js-sdk)

## Project Status

This project is in its early stage and under heavy development. We move fast and
break things. Benchmark on development branch may not represent its potential
performance. We release pre-built binaries constantly for functional
evaluation. Do not use it in production at the moment.

For future plans, check out [GreptimeDB roadmap](https://github.com/GreptimeTeam/greptimedb/issues/669).

## Community

Our core team is thrilled to see you participate in any ways you like. When you are stuck, try to
ask for help by filling an issue with a detailed description of what you were trying to do
and what went wrong. If you have any questions or if you would like to get involved in our
community, please check out:

- GreptimeDB Community on [Slack](https://greptime.com/slack)
- GreptimeDB GitHub [Discussions](https://github.com/GreptimeTeam/greptimedb/discussions)
- Greptime official [Website](https://greptime.com)

In addition, you may:

- View our official [Blog](https://greptime.com/blogs/index)
- Connect us with [Linkedin](https://www.linkedin.com/company/greptime/)
- Follow us on [Twitter](https://twitter.com/greptime)

## License

GreptimeDB uses the [Apache 2.0 license][1] to strike a balance between
open contributions and allowing you to use the software however you want.

[1]: <https://github.com/greptimeTeam/greptimedb/blob/develop/LICENSE>

## Contributing

Please refer to [contribution guidelines](CONTRIBUTING.md) for more information.

## Acknowledgement
- GreptimeDB uses [Apache Arrow](https://arrow.apache.org/) as the memory model and [Apache Parquet](https://parquet.apache.org/) as the persistent file format.
- GreptimeDB's query engine is powered by [Apache Arrow DataFusion](https://github.com/apache/arrow-datafusion).
- [Apache OpenDAL (incubating)](https://opendal.apache.org) gives GreptimeDB a very general and elegant data access abstraction layer.
- GreptimeDB's meta service is based on [etcd](https://etcd.io/).
- GreptimeDB uses [RustPython](https://github.com/RustPython/RustPython) for experimental embedded python scripting.
