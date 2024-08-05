<p align="center">
  <picture>
    <source media="(prefers-color-scheme: light)" srcset="https://cdn.jsdelivr.net/gh/GreptimeTeam/greptimedb@main/docs/logo-text-padding.png">
    <source media="(prefers-color-scheme: dark)" srcset="https://cdn.jsdelivr.net/gh/GreptimeTeam/greptimedb@main/docs/logo-text-padding-dark.png">
    <img alt="GreptimeDB Logo" src="https://cdn.jsdelivr.net/gh/GreptimeTeam/greptimedb@main/docs/logo-text-padding.png" width="400px">
  </picture>
</p>

<h2 align="center">Unified Time Series Database for Metrics, Logs, and Events</h2>

<div align="center">
<h3 align="center">
  <a href="https://greptime.com/product/cloud">GreptimeCloud</a> |
  <a href="https://docs.greptime.com/">User Guide</a> |
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

<br/>

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

**GreptimeDB** is an open-source unified time-series database for **Metrics**, **Logs**, and **Events** (also **Traces** in plan). You can gain real-time insights from Edge to Cloud at any scale.

## Why GreptimeDB

Our core developers have been building time-series data platforms for years. Based on our best-practices, GreptimeDB is born to give you:

* **Unified all kinds of time series**

  GreptimeDB treats all time series as contextual events with timestamp, and thus unifies the processing of metrics, logs, and events. It supports analyzing metrics, logs, and events with SQL and PromQL, and doing streaming with continuous aggregation.

* **Cloud-Edge collaboration**

  GreptimeDB can be deployed on ARM architecture-compatible Android/Linux systems as well as cloud environments from various vendors. Both sides run the same software, providing identical APIs and control planes, so your application can run at the edge or on the cloud without modification, and data synchronization also becomes extremely easy and efficient.

* **Cloud-native distributed database**

  By leveraging object storage (S3 and others), separating compute and storage, scaling stateless compute nodes arbitrarily, GreptimeDB implements seamless scalability. It also supports cross-cloud deployment with a built-in unified data access layer over different object storages.

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

To install GreptimeDB locally, the recommended way is via Docker:

```shell
docker pull greptime/greptimedb
```

Start a GreptimeDB container with:

```shell
docker run --rm --name greptime --net=host greptime/greptimedb standalone start
```

Read more about [Installation](https://docs.greptime.com/getting-started/installation/overview) on docs.

## Getting Started

* [Quickstart](https://docs.greptime.com/getting-started/quick-start)
* [User Guide](https://docs.greptime.com/user-guide/overview)
* [Demos](https://github.com/GreptimeTeam/demo-scene)
* [FAQ](https://docs.greptime.com/faq-and-others/faq)

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
- [GreptimeDB JavaScript Ingester](https://github.com/GreptimeTeam/greptimedb-ingester-js)

### Grafana Dashboard

Our official Grafana dashboard is available at [grafana](grafana/README.md) directory.

## Project Status

The current version has not yet reached the standards for General Availability. 
According to our Greptime 2024 Roadmap, we aim to achieve a production-level version with the release of v1.0 by the end of 2024. [Join Us](https://github.com/GreptimeTeam/greptimedb/issues/3412)

We welcome you to test and use GreptimeDB. Some users have already adopted it in their production environments. If you're interested in trying it out, please use the latest stable release available.

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

Special thanks to all the contributors who have propelled GreptimeDB forward. For a complete list of contributors, please refer to [AUTHOR.md](AUTHOR.md).

- GreptimeDB uses [Apache Arrow™](https://arrow.apache.org/) as the memory model and [Apache Parquet™](https://parquet.apache.org/) as the persistent file format.
- GreptimeDB's query engine is powered by [Apache Arrow DataFusion™](https://arrow.apache.org/datafusion/).
- [Apache OpenDAL™](https://opendal.apache.org) gives GreptimeDB a very general and elegant data access abstraction layer.
- GreptimeDB's meta service is based on [etcd](https://etcd.io/).
- GreptimeDB uses [RustPython](https://github.com/RustPython/RustPython) for experimental embedded python scripting.
