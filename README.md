<p align="center">
  <picture>
    <source media="(prefers-color-scheme: light)" srcset="https://cdn.jsdelivr.net/gh/GreptimeTeam/greptimedb@main/docs/logo-text-padding.png">
    <source media="(prefers-color-scheme: dark)" srcset="https://cdn.jsdelivr.net/gh/GreptimeTeam/greptimedb@main/docs/logo-text-padding-dark.png">
    <img alt="GreptimeDB Logo" src="https://cdn.jsdelivr.net/gh/GreptimeTeam/greptimedb@main/docs/logo-text-padding.png" width="400px">
  </picture>
</p>

<h1 align="center">Cloud-scale, Fast and Efficient Time Series Database</h1>

<div align="center">
<h3 align="center">
  <a href="https://greptime.com/product/cloud">GreptimeCloud</a> |
  <a href="https://docs.greptime.com/">User guide</a> |
  <a href="https://greptimedb.rs/">API Docs</a> |
  <a href="https://github.com/GreptimeTeam/greptimedb/issues/3412">Roadmap 2024</a>
</h4>

<a href="https://github.com/GreptimeTeam/greptimedb/releases/latest">
<img src="https://img.shields.io/github/v/release/GreptimeTeam/greptimedb.svg" alt="Version"/>
</a>
<a href="https://github.com/GreptimeTeam/greptimedb/releases/latest">
<img src="https://img.shields.io/github/release-date/GreptimeTeam/greptimedb.svg" alt="Releases"/>
</a>
<a href="https://hub.docker.com/r/greptime/greptimedb/">
<img src="https://img.shields.io/docker/pulls/greptime/greptimedb.svg" alt="Docker Pulls"/>
</a>
<a href="https://github.com/GreptimeTeam/greptimedb/actions/workflows/develop.yml">
<img src="https://github.com/GreptimeTeam/greptimedb/actions/workflows/develop.yml/badge.svg" alt="GitHub Actions"/>
</a>
<a href="https://codecov.io/gh/GrepTimeTeam/greptimedb">
<img src="https://codecov.io/gh/GrepTimeTeam/greptimedb/branch/main/graph/badge.svg?token=FITFDI3J3C" alt="Codecov"/>
</a>
<a href="https://github.com/greptimeTeam/greptimedb/blob/main/LICENSE">
<img src="https://img.shields.io/github/license/greptimeTeam/greptimedb" alt="License"/>
</a>

<a href="https://greptime.com/slack">
<img src="https://img.shields.io/badge/slack-GreptimeDB-0abd59?logo=slack&style=for-the-badge" alt="Slack"/>
</a>
<a href="https://twitter.com/greptime">
<img src="https://img.shields.io/badge/twitter-follow_us-1d9bf0.svg?style=for-the-badge" alt="Twitter"/>
</a>
<a href="https://www.linkedin.com/company/greptime/">
<img src="https://img.shields.io/badge/linkedin-connect_with_us-0a66c2.svg?style=for-the-badge" alt="LinkedIn"/>
</a>
</div>

## Introduction

**GreptimeDB** is an open-source time-series database focusing on efficiency, scalability, and analytical capabilities.
Designed to work on infrastructure of the cloud era, GreptimeDB benefits users with its elasticity and commodity storage, offering a fast and cost-effective **alternative to InfluxDB and Prometheus**.

## Why GreptimeDB

Our core developers have been building time-series data platforms for years. Based on our best-practices, GreptimeDB is born to give you:

* **Easy horizontal scaling**

  Seamless scalability from a standalone binary at edge to a robust, highly available distributed cluster in cloud, with a transparent experience for both developers and administrators.

* **Analyzing time-series data**

  Query your time-series data with SQL and PromQL. Use Python scripts to facilitate complex analytical tasks.

* **Cloud-native distributed database**

  Fully open-source distributed cluster architecture that harnesses the power of cloud-native elastic computing resources.

* **Performance and Cost-effective**

  Flexible indexing capabilities and distributed, parallel-processing query engine, tackling high cardinality issues down. Optimized columnar layout for handling time-series data; compacted, compressed, and stored on various storage backends, particularly cloud object storage with 50x cost efficiency.

* **Compatible with InfluxDB, Prometheus and more protocols**

  Widely adopted database protocols and APIs, including MySQL, PostgreSQL, and Prometheus Remote Storage, etc. [Read more](https://docs.greptime.com/user-guide/clients/overview).

## Try GreptimeDB

### 1. [GreptimePlay](https://greptime.com/playground)

Try out the features of GreptimeDB right from your browser.

### 2. [GreptimeCloud](https://console.greptime.cloud/)

Start instantly with a free cluster.

### 3. Docker Image

To install GreptimeDB locally, the recommneded way is via Docker:

```shell
docker pull greptime/greptimedb
```

Start a GreptimeDB container with:

```shell
docker run --rm --name greptime --net=host greptime/greptimedb standalone start
```

Read more about [Installation](https://docs.greptime.com/getting-started/installation/overview) on docs.

## Getting Started

* [Quickstart](https://docs.greptime.com/getting-started/quick-start/overview)
* [Write Data](https://docs.greptime.com/user-guide/clients/overview)
* [Query Data](https://docs.greptime.com/user-guide/query-data/overview)
* [Operations](https://docs.greptime.com/user-guide/operations/overview)

## Build

Check the prerequisite:

* [Rust toolchain](https://www.rust-lang.org/tools/install) (nightly)
* [Protobuf compiler](https://grpc.io/docs/protoc-installation/) (>= 3.15)
* Python toolchain (optional): Required only if built with PyO3 backend. More detail for compiling with PyO3 can be found in its [documentation](https://pyo3.rs/v0.18.1/building_and_distribution#configuring-the-python-version).

Build GreptimeDB binary:

```shell
make
```

Run a standalone server:

```shell
cargo run -- standalone start
```

## Extension

### Dashboard

- [The dashboard UI for GreptimeDB](https://github.com/GreptimeTeam/dashboard)

### SDK

- [GreptimeDB Go Ingester](https://github.com/GreptimeTeam/greptimedb-ingester-go)
- [GreptimeDB Java Ingester](https://github.com/GreptimeTeam/greptimedb-ingester-java)
- [GreptimeDB C++ Ingester](https://github.com/GreptimeTeam/greptimedb-ingester-cpp)
- [GreptimeDB Erlang Ingester](https://github.com/GreptimeTeam/greptimedb-ingester-erl)
- [GreptimeDB Rust Ingester](https://github.com/GreptimeTeam/greptimedb-ingester-rust)
- [GreptimeDB JavaScript Ingester](https://github.com/GreptimeTeam/greptime-ingester-js)

### Grafana Dashboard

Our official Grafana dashboard is available at [grafana](grafana/README.md) directory.

## Project Status

This project is in its early stage and under heavy development. We move fast and
break things. Benchmark on development branch may not represent its potential
performance. We release pre-built binaries constantly for functional
evaluation. Do not use it in production at the moment.

For future plans, check out [GreptimeDB roadmap](https://github.com/GreptimeTeam/greptimedb/issues/3412).

## Community

Our core team is thrilled to see you participate in any ways you like. When you are stuck, try to
ask for help by filling an issue with a detailed description of what you were trying to do
and what went wrong. If you have any questions or if you would like to get involved in our
community, please check out:

- GreptimeDB Community on [Slack](https://greptime.com/slack)
- GreptimeDB [GitHub Discussions forum](https://github.com/GreptimeTeam/greptimedb/discussions)
- Greptime official [website](https://greptime.com)

In addition, you may:

- View our official [Blog](https://greptime.com/blogs/)
- Connect us with [Linkedin](https://www.linkedin.com/company/greptime/)
- Follow us on [Twitter](https://twitter.com/greptime)

## License

GreptimeDB uses the [Apache License 2.0](https://apache.org/licenses/LICENSE-2.0.txt) to strike a balance between
open contributions and allowing you to use the software however you want.

## Contributing

Please refer to [contribution guidelines](CONTRIBUTING.md) and [internal concepts docs](https://docs.greptime.com/contributor-guide/overview.html) for more information.

## Acknowledgement

- GreptimeDB uses [Apache Arrow™](https://arrow.apache.org/) as the memory model and [Apache Parquet™](https://parquet.apache.org/) as the persistent file format.
- GreptimeDB's query engine is powered by [Apache Arrow DataFusion™](https://arrow.apache.org/datafusion/).
- [Apache OpenDAL™](https://opendal.apache.org) gives GreptimeDB a very general and elegant data access abstraction layer.
- GreptimeDB's meta service is based on [etcd](https://etcd.io/).
- GreptimeDB uses [RustPython](https://github.com/RustPython/RustPython) for experimental embedded python scripting.
