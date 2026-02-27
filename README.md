<p align="center">
  <picture>
    <source media="(prefers-color-scheme: light)" srcset="https://cdn.jsdelivr.net/gh/GreptimeTeam/greptimedb@main/docs/logo-text-padding.png">
    <source media="(prefers-color-scheme: dark)" srcset="https://cdn.jsdelivr.net/gh/GreptimeTeam/greptimedb@main/docs/logo-text-padding-dark.png">
    <img alt="GreptimeDB Logo" src="https://cdn.jsdelivr.net/gh/GreptimeTeam/greptimedb@main/docs/logo-text-padding.png" width="400px">
  </picture>
</p>

<h2 align="center">One database for metrics, logs, and traces<br/>
replacing Prometheus, Loki, and Elasticsearch</h2>

>  The unified OpenTelemetry backend — with SQL + PromQL on object storage.

<div align="center">
<h3 align="center">
  <a href="https://docs.greptime.com/user-guide/overview/">User Guide</a> |
  <a href="https://greptimedb.rs/">API Docs</a> |
  <a href="https://github.com/GreptimeTeam/greptimedb/issues/5446">Roadmap 2025</a>
</h3>

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
- [⭐ Key Features](#features)
- [How GreptimeDB Compares](#how-greptimedb-compares)
- [Architecture](#architecture)
- [Try GreptimeDB](#try-greptimedb)
- [Getting Started](#getting-started)
- [Build From Source](#build-from-source)
- [Tools & Extensions](#tools--extensions)
- [Project Status](#project-status)
- [Community](#community)
- [License](#license)
- [Commercial Support](#commercial-support)
- [Contributing](#contributing)
- [Acknowledgement](#acknowledgement)

## Introduction

**GreptimeDB** is an open-source observability database that handles metrics, logs, and traces in one engine. Use it as the single OpenTelemetry backend — replacing Prometheus, Loki, and Elasticsearch with one database built on object storage. Query with SQL and PromQL, scale without pain, cut costs up to 50x.

## Features

|   Feature  | Description |
| --------- | ----------- |
| Drop-in replacement | [PromQL](https://docs.greptime.com/user-guide/query-data/promql/), [Prometheus remote write](https://docs.greptime.com/user-guide/ingest-data/for-observability/prometheus/), [Jaeger](https://docs.greptime.com/user-guide/query-data/jaeger/), and [OpenTelemetry](https://docs.greptime.com/user-guide/ingest-data/for-observability/opentelemetry/) native. Use as your single backend for all three signals, or migrate one at a time.|
| 50x lower cost | Object storage (S3, GCS, Azure Blob etc.) as [primary storage](https://docs.greptime.com/user-guide/deployments-administration/configuration/#storage-options). Compute-storage separation scales without pain.|
| SQL + PromQL | Monitor with [PromQL](https://docs.greptime.com/user-guide/query-data/promql), analyze with [SQL](https://docs.greptime.com/user-guide/query-data/sql). One database replaces Prometheus + your data warehouse.|
| Sub-second at PB-EB scale | Columnar engine with [fulltext, inverted, and skipping indexes](https://docs.greptime.com/user-guide/manage-data/data-index). Written in Rust.|

  ✅ **Perfect for:**
  * Replacing Prometheus + Loki + Elasticsearch with one database
  * Scaling past Prometheus — high cardinality, long-term storage, no Thanos/Mimir overhead
  * Cutting observability costs with object storage (up to 50x savings on traces, 30% on logs)
  * Edge-to-cloud observability with unified APIs on resource-constrained devices

Learn more in [Why GreptimeDB](https://docs.greptime.com/user-guide/concepts/why-greptimedb) and [Observability 2.0 and the Database for It](https://greptime.com/blogs/2025-04-25-greptimedb-observability2-new-database).

## How GreptimeDB Compares

| Feature | GreptimeDB | Prometheus / Thanos / Mimir | Grafana Loki | Elasticsearch |
|---|---|---|---|---|
| Data types | Metrics, logs, traces | Metrics only | Logs only | Logs, traces |
| Query language | SQL + PromQL | PromQL | LogQL | Query DSL |
| Storage | Native object storage (S3, etc.) | Local disk + object storage (Thanos/Mimir) | Object storage (chunks) | Local disk |
| Scaling | Compute-storage separation, stateless nodes | Federation / Thanos / Mimir — multi-component, ops heavy | Stateless + object storage | Shard-based, ops heavy |
| Cost efficiency | Up to 50x lower storage | High at scale | Moderate | High (inverted index overhead) |
| OpenTelemetry | Native (metrics + logs + traces) | Partial (metrics only) | Partial (logs only) | Via instrumentation |

**Benchmarks:**
* [GreptimeDB tops JSONBench's billion-record cold run test](https://greptime.com/blogs/2025-03-18-jsonbench-greptimedb-performance)
* [TSBS Benchmark](https://github.com/GreptimeTeam/greptimedb/tree/main/docs/benchmarks/tsbs)
* [More benchmark reports](https://docs.greptime.com/user-guide/concepts/features-that-you-concern#how-is-greptimedbs-performance-compared-to-other-solutions)

## Architecture

GreptimeDB can run in two modes:
* **Standalone Mode** - Single binary for development and small deployments
* **Distributed Mode** - Separate components for production scale:
  - Frontend: Query processing and protocol handling
  - Datanode: Data storage and retrieval
  - Metasrv: Metadata management and coordination
  
Read the [architecture](https://docs.greptime.com/contributor-guide/overview/#architecture) document. [DeepWiki](https://deepwiki.com/GreptimeTeam/greptimedb/1-overview) provides an in-depth look at GreptimeDB:
  <img alt="GreptimeDB System Overview" src="docs/architecture.png">

## Try GreptimeDB

```shell
docker pull greptime/greptimedb
```

```shell
docker run -p 127.0.0.1:4000-4003:4000-4003 \
  -v "$(pwd)/greptimedb_data:/greptimedb_data" \
  --name greptime --rm \
  greptime/greptimedb:latest standalone start \
  --http-addr 0.0.0.0:4000 \
  --rpc-bind-addr 0.0.0.0:4001 \
  --mysql-addr 0.0.0.0:4002 \
  --postgres-addr 0.0.0.0:4003
```
Dashboard: [http://localhost:4000/dashboard](http://localhost:4000/dashboard)

Read more in the [full Install Guide](https://docs.greptime.com/getting-started/installation/overview).

**Troubleshooting:**
* Cannot connect to the database? Ensure that ports `4000`, `4001`, `4002`, and `4003` are not blocked by a firewall or used by other services.
* Failed to start? Check the container logs with `docker logs greptime` for further details.

## Getting Started

- [Quickstart](https://docs.greptime.com/getting-started/quick-start)
- [User Guide](https://docs.greptime.com/user-guide/overview)
- [Demo Scenes](https://github.com/GreptimeTeam/demo-scene)
- [FAQ](https://docs.greptime.com/faq-and-others/faq)

## Build From Source

**Prerequisites:**
* [Rust toolchain](https://www.rust-lang.org/tools/install) (nightly)
* [Protobuf compiler](https://grpc.io/docs/protoc-installation/) (>= 3.15)
* C/C++ building essentials, including `gcc`/`g++`/`autoconf` and glibc library (eg. `libc6-dev` on Ubuntu and `glibc-devel` on Fedora)
* Python toolchain (optional): Required only if using some test scripts.

**Build and Run:**
```bash
make
cargo run -- standalone start
```

## Tools & Extensions

- **Kubernetes**: [GreptimeDB Operator](https://github.com/GrepTimeTeam/greptimedb-operator)
- **Helm Charts**: [Greptime Helm Charts](https://github.com/GreptimeTeam/helm-charts)
- **Dashboard**: [Web UI](https://github.com/GreptimeTeam/dashboard)
- **gRPC Ingester**: [Go](https://github.com/GreptimeTeam/greptimedb-ingester-go), [Java](https://github.com/GreptimeTeam/greptimedb-ingester-java), [C++](https://github.com/GreptimeTeam/greptimedb-ingester-cpp), [Erlang](https://github.com/GreptimeTeam/greptimedb-ingester-erl), [Rust](https://github.com/GreptimeTeam/greptimedb-ingester-rust)
- **Grafana Data Source**: [GreptimeDB Grafana data source plugin](https://github.com/GreptimeTeam/greptimedb-grafana-datasource)
- **Grafana Dashboard**: [Official Dashboard for monitoring](https://github.com/GreptimeTeam/greptimedb/blob/main/grafana/README.md)

## Project Status

> **Status:** RC — marching toward v1.0 GA!
> **GA (v1.0):** March 2026

- Deployed in production handling billions of data points daily
- Stable APIs, actively maintained, with regular releases ([version info](https://docs.greptime.com/nightly/reference/about-greptimedb-version))
- Suitable for evaluation and pilot deployments

GreptimeDB v1.0 represents a major milestone toward maturity — marking stable APIs, production readiness, and proven performance.

**Roadmap:** [v1.0 highlights and release plan](https://greptime.com/blogs/2025-11-05-greptimedb-v1-highlights) and [2026 roadmap](https://greptime.com/blogs/2026-02-11-greptimedb-roadmap-2026).

For production use, we recommend using the latest stable release.

If you find this project useful, a ⭐ would mean a lot to us!

[![Star History Chart](https://api.star-history.com/svg?repos=GreptimeTeam/GreptimeDB&type=Date)](https://www.star-history.com/#GreptimeTeam/GreptimeDB&Date)

<img alt="Known Users" src="https://greptime.com/logo/img/users.png"/>

## Community

We invite you to engage and contribute!

- [Slack](https://greptime.com/slack)
- [Discussions](https://github.com/GreptimeTeam/greptimedb/discussions)
- [Official Website](https://greptime.com/)
- [Blog](https://greptime.com/blogs/)
- [LinkedIn](https://www.linkedin.com/company/greptime/)
- [X (Twitter)](https://X.com/greptime)
- [YouTube](https://www.youtube.com/@greptime)

## License

GreptimeDB is licensed under the [Apache License 2.0](https://apache.org/licenses/LICENSE-2.0.txt).

## Commercial Support

Running GreptimeDB in your organization?
We offer enterprise add-ons, services, training, and consulting.
[Contact us](https://greptime.com/contactus) for details.

## Contributing

- Read our [Contribution Guidelines](https://github.com/GreptimeTeam/greptimedb/blob/main/CONTRIBUTING.md).
- Explore [Internal Concepts](https://docs.greptime.com/contributor-guide/overview.html) and [DeepWiki](https://deepwiki.com/GreptimeTeam/greptimedb).
- Pick up a [good first issue](https://github.com/GreptimeTeam/greptimedb/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22) and join the #contributors [Slack](https://greptime.com/slack) channel.

## Acknowledgement

Special thanks to all contributors! See [AUTHORS.md](https://github.com/GreptimeTeam/greptimedb/blob/main/AUTHOR.md).

- Uses [Apache Arrow™](https://arrow.apache.org/) (memory model)
- [Apache Parquet™](https://parquet.apache.org/) (file storage)
- [Apache DataFusion™](https://arrow.apache.org/datafusion/) (query engine)
- [Apache OpenDAL™](https://opendal.apache.org/) (data access abstraction)
