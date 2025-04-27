<p align="center">
  <picture>
    <source media="(prefers-color-scheme: light)" srcset="https://cdn.jsdelivr.net/gh/GreptimeTeam/greptimedb@main/docs/logo-text-padding.png">
    <source media="(prefers-color-scheme: dark)" srcset="https://cdn.jsdelivr.net/gh/GreptimeTeam/greptimedb@main/docs/logo-text-padding-dark.png">
    <img alt="GreptimeDB Logo" src="https://cdn.jsdelivr.net/gh/GreptimeTeam/greptimedb@main/docs/logo-text-padding.png" width="400px">
  </picture>
</p>

<h2 align="center">Real-Time & Cloud-Native Observability  Database<br/>for metrics, logs, and traces</h2>

<div align="center">
<h3 align="center">
  <a href="https://greptime.com/product/cloud">GreptimeCloud</a> |
  <a href="https://docs.greptime.com/">User Guide</a> |
  <a href="https://greptimedb.rs/">API Docs</a> |
  <a href="https://github.com/GreptimeTeam/greptimedb/issues/5446">Roadmap 2025</a>
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

- [Introduction](#introduction)
- [**Features: Why GreptimeDB**](#why-greptimedb)
- [Architecture](https://docs.greptime.com/contributor-guide/overview/#architecture)
- [Try it for free](#try-greptimedb)
- [Getting Started](#getting-started)
- [Project Status](#project-status)
- [Join the community](#community)
  - [Contributing](#contributing)
- [Tools & Extensions](#tools--extensions)
- [License](#license)
- [Acknowledgement](#acknowledgement)

## Introduction

**GreptimeDB** is an open-source, cloud-native, unified & cost-effective observability database for **Metrics**, **Logs**, and **Traces**. You can gain real-time insights from Edge to Cloud at Any Scale.

## News

**[GreptimeDB tops JSONBench's billion-record cold run test!](https://greptime.com/blogs/2025-03-18-jsonbench-greptimedb-performance)**

## Why GreptimeDB

Our core developers have been building observability data platforms for years. Based on our best practices, GreptimeDB was born to give you:

* **Unified Processing of Observability Data**

  A unified database that treats metrics, logs, and traces as timestamped wide events with context, supporting [SQL](https://docs.greptime.com/user-guide/query-data/sql)/[PromQL](https://docs.greptime.com/user-guide/query-data/promql) queries and [stream processing](https://docs.greptime.com/user-guide/flow-computation/overview) to simplify complex data stacks.

* **High Performance and Cost-effective**

   Written in Rust, combines a distributed query engine with [rich indexing](https://docs.greptime.com/user-guide/manage-data/data-index) (inverted, fulltext, skip data, and vector) and optimized columnar storage to deliver sub-second responses on petabyte-scale data and high-cost efficiency.

* **Cloud-native Distributed Database**

  Built for [Kubernetes](https://docs.greptime.com/user-guide/deployments/deploy-on-kubernetes/greptimedb-operator-management). GreptimeDB achieves seamless scalability with its [cloud-native architecture](https://docs.greptime.com/user-guide/concepts/architecture) of separated compute and storage, built on object storage (AWS S3, Azure Blob Storage, etc.) while enabling cross-cloud deployment through a unified data access layer.

* **Developer-Friendly**

  Access standardized SQL/PromQL interfaces through built-in web dashboard, REST API, and MySQL/PostgreSQL protocols. Supports widely adopted data ingestion [protocols](https://docs.greptime.com/user-guide/protocols/overview) for seamless migration and integration.

* **Flexible Deployment Options**

  Deploy GreptimeDB anywhere from ARM-based edge devices to cloud environments with unified APIs and bandwidth-efficient data synchronization. Query edge and cloud data seamlessly through identical APIs. [Learn how to run on Android](https://docs.greptime.com/user-guide/deployments/run-on-android/).

For more detailed info please read  [Why GreptimeDB](https://docs.greptime.com/user-guide/concepts/why-greptimedb).

## Try GreptimeDB

### 1. [Live Demo](https://greptime.com/playground)

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
docker run -p 127.0.0.1:4000-4003:4000-4003 \
  -v "$(pwd)/greptimedb:./greptimedb_data" \
  --name greptime --rm \
  greptime/greptimedb:latest standalone start \
  --http-addr 0.0.0.0:4000 \
  --rpc-bind-addr 0.0.0.0:4001 \
  --mysql-addr 0.0.0.0:4002 \
  --postgres-addr 0.0.0.0:4003
```

Access the dashboard via `http://localhost:4000/dashboard`.

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
* C/C++ building essentials, including `gcc`/`g++`/`autoconf` and glibc library (eg. `libc6-dev` on Ubuntu and `glibc-devel` on Fedora)
* Python toolchain (optional): Required only if using some test scripts.

Build GreptimeDB binary:

```shell
make
```

Run a standalone server:

```shell
cargo run -- standalone start
```

## Tools & Extensions

### Kubernetes

- [GreptimeDB Operator](https://github.com/GrepTimeTeam/greptimedb-operator)

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

Our official Grafana dashboard for monitoring GreptimeDB is available at [grafana](grafana/README.md) directory.

## Project Status

GreptimeDB is currently in Beta. We are targeting GA (General Availability) with v1.0 release by Early 2025.

While in Beta, GreptimeDB is already:

* Being used in production by early adopters
* Actively maintained with regular releases, [about version number](https://docs.greptime.com/nightly/reference/about-greptimedb-version)
* Suitable for testing and evaluation

For production use, we recommend using the latest stable release.

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

## Commercial Support

If you are running GreptimeDB OSS in your organization, we offer additional
enterprise add-ons, installation services, training, and consulting. [Contact
us](https://greptime.com/contactus) and we will reach out to you with more
detail of our commercial license.

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

<img alt="Known Users" src="https://greptime.com/logo/img/users.png"/>