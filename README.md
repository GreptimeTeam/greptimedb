<p align="center">
  <picture>
    <source media="(prefers-color-scheme: light)" srcset="https://cdn.jsdelivr.net/gh/GreptimeTeam/greptimedb@main/docs/logo-text-padding.png">
    <source media="(prefers-color-scheme: dark)" srcset="https://cdn.jsdelivr.net/gh/GreptimeTeam/greptimedb@main/docs/logo-text-padding-dark.png">
    <img alt="GreptimeDB Logo" src="https://cdn.jsdelivr.net/gh/GreptimeTeam/greptimedb@main/docs/logo-text-padding.png" width="400px">
  </picture>
</p>

<h2 align="center">Metrics, logs, and traces.<br/>
One engine, on your infrastructure.</h2>

> A columnar database for metrics, logs, and traces on object storage.
> Apache-2.0 licensed core.

<div align="center">
<h3 align="center">
  <a href="https://docs.greptime.com/user-guide/overview/">User Guide</a> |
  <a href="https://greptimedb.rs/">API Docs</a> |
  <a href="https://github.com/GreptimeTeam/greptimedb/issues/7685">Roadmap 2026</a>
</h3>

<a href="https://github.com/GreptimeTeam/greptimedb/releases/latest">
<img src="https://img.shields.io/github/v/release/GreptimeTeam/greptimedb?filter=!*-*&label=stable&color=brightgreen" alt="Stable"/>
</a>
<a href="https://github.com/GreptimeTeam/greptimedb/releases">
<img src="https://img.shields.io/github/v/release/GreptimeTeam/greptimedb?include_prereleases&filter=!*-*-*&label=canary&color=blueviolet" alt="Canary"/>
</a>
<a href="https://github.com/GreptimeTeam/greptimedb/releases">
<img src="https://img.shields.io/github/v/release/GreptimeTeam/greptimedb?include_prereleases&filter=*-nightly-*&label=nightly&color=orange" alt="Nightly"/>
</a>

<sub><b>stable</b> for production &nbsp;·&nbsp; <b>canary</b> includes pre-releases &nbsp;·&nbsp; <b>nightly</b> is a weekly snapshot of <code>main</code></sub>

<a href="https://hub.docker.com/r/greptime/greptimedb/">
<img src="https://img.shields.io/docker/pulls/greptime/greptimedb.svg" alt="Docker Pulls"/>
</a>
<a href="https://github.com/GreptimeTeam/greptimedb/actions/workflows/integration.yml">
<img src="https://github.com/GreptimeTeam/greptimedb/actions/workflows/integration.yml/badge.svg" alt="GitHub Actions"/>
</a>
<a href="https://codecov.io/gh/GreptimeTeam/greptimedb">
<img src="https://codecov.io/gh/GreptimeTeam/greptimedb/branch/main/graph/badge.svg?token=FITFDI3J3C" alt="Codecov"/>
</a>
<a href="https://github.com/GreptimeTeam/greptimedb/blob/main/LICENSE">
<img src="https://img.shields.io/github/license/GreptimeTeam/greptimedb" alt="License"/>
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
- [Why You Might Use It](#why-you-might-use-it)
- [Overview](#overview)
- [What's Supported](#whats-supported)
- [Compatibility and Migration](#compatibility-and-migration)
- [Limitations and Edition Boundary](#limitations-and-edition-boundary)
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

**GreptimeDB** is an open-source observability database. Metrics, logs, and traces run on one columnar engine over object storage and share one [table model](https://docs.greptime.com/user-guide/concepts/data-model/): tags, timestamp, and fields. When signals carry common identifiers such as service, host, or trace ID, you can correlate them in SQL without moving data between databases.

Ingest through OpenTelemetry, Prometheus Remote Write, Loki Push, or Elasticsearch Bulk. Use SQL across observability data and PromQL for metrics. Migrate ingestion one signal at a time without rebuilding your collectors.

## Why You Might Use It

- You run Prometheus plus Loki or Elasticsearch and want one backend instead of three
- You have outgrown Prometheus on cardinality or retention and don't want the Thanos/Mimir operational surface
- You need long retention on object storage without a separate analytics stack
- You want to query telemetry with SQL, not only a domain query language
- You are storing GenAI or agent telemetry ([OTel GenAI conventions](https://opentelemetry.io/docs/specs/semconv/gen-ai/)) alongside infrastructure signals
- You need the same engine and semantics on resource-constrained devices

Learn more in [Why GreptimeDB](https://docs.greptime.com/user-guide/concepts/why-greptimedb).

## Overview

A quick overview of what GreptimeDB ingests, how it connects to other systems, and what its distributed engine lets you do.

<p align="center">
  <a href="https://github.com/GreptimeTeam/greptimedb/raw/main/docs/overview.png" target="_blank" rel="noopener">
    <img alt="GreptimeDB Overview" src="docs/overview.png" width="900px">
  </a>
</p>

## What's Supported

| | |
|---|---|
| **Ingest** | [OpenTelemetry (OTLP)](https://docs.greptime.com/user-guide/ingest-data/for-observability/opentelemetry/), [Prometheus Remote Write](https://docs.greptime.com/user-guide/ingest-data/for-observability/prometheus/), [Loki Push](https://docs.greptime.com/user-guide/ingest-data/for-observability/loki), [Elasticsearch Bulk](https://docs.greptime.com/user-guide/ingest-data/for-observability/elasticsearch), [InfluxDB line protocol](https://docs.greptime.com/user-guide/protocols/influxdb-line-protocol), [gRPC](https://docs.greptime.com/user-guide/protocols/grpc/) |
| **Query** | [SQL](https://docs.greptime.com/user-guide/query-data/overview/), [PromQL](https://docs.greptime.com/user-guide/query-data/promql/), [Jaeger-compatible trace queries](https://docs.greptime.com/user-guide/query-data/jaeger/), MySQL and PostgreSQL wire protocols |
| **Storage** | S3, GCS, Azure Blob and S3-compatible endpoints as [primary storage](https://docs.greptime.com/user-guide/deployments-administration/configuration/#storage-options), with memory and local-disk caches |
| **Built in** | Retention policies, downsampling, [continuous aggregation](https://docs.greptime.com/user-guide/flow-computation/overview), explicit table partitioning, and [inverted / skipping / fulltext indexes](https://docs.greptime.com/user-guide/manage-data/data-index) |

Compute and storage are disaggregated: object storage holds the data, while memory and local-disk caches keep recent and frequently queried data close to compute.

## Compatibility and Migration

Compatibility is per protocol, and query-side coverage is narrower than ingestion.

| | Compatible | Not compatible |
|---|---|---|
| **Prometheus** | Remote Write ingestion; PromQL queries | Gaps are listed in [PromQL compatibility](https://docs.greptime.com/user-guide/query-data/promql/) |
| **Loki** | Push ingestion; dual-write through Grafana Alloy makes the [cutover gradual](https://greptime.com/blogs/2026-07-23-from-loki-to-greptimedb-dual-write-migration) | LogQL and the rest of the Loki query API |
| **Elasticsearch** | `_bulk` ingestion in the open-source core; [QueryDSL](https://docs.greptime.com/enterprise/elasticsearch-compatible/query/) partially, in Enterprise | Most other Elasticsearch APIs |

**Benchmarks:**
* [GreptimeDB tops JSONBench's billion-record cold run test](https://greptime.com/blogs/2025-03-18-jsonbench-greptimedb-performance)
* [TSBS Benchmark](https://github.com/GreptimeTeam/greptimedb/tree/main/docs/benchmarks/tsbs)
* [More benchmark reports](https://docs.greptime.com/user-guide/concepts/features-that-you-concern#how-is-greptimedbs-performance-compared-to-other-solutions)

## Limitations and Edition Boundary

Cluster deployment, object storage, the Flow engine, and every ingestion protocol listed above are in the Apache-2.0 build. Repartitioning, region migration, and index creation are manual operations there.

Read replicas, workload isolation, and automated repartitioning are **GreptimeDB Enterprise** features, along with enterprise security and governance. The [Enterprise overview](https://docs.greptime.com/enterprise/overview/) has the current list, and [pricing](https://www.greptime.com/pricing#differences) has the edition comparison.

## Architecture

GreptimeDB can run in two modes:
* **Standalone** — single binary for development and small deployments.
* **Distributed** — four components, each independently scalable:
  - **Frontend** — protocol entry (OTel, Prometheus, MySQL/PostgreSQL, gRPC, ingestion APIs for Elasticsearch/InfluxDB/Loki) and the distributed query engine. Stateless, scales horizontally.
  - **Datanode** — region engine with WAL, memtable, SST, cache, compaction, and indexes. Persists data to object storage. Elastic.
  - **Metasrv** — metadata, routing, repartitioning, and security. Backed by a pluggable KV layer (etcd or RDS).
  - **Flownode** (optional) — continuous flow computation (streaming and materialized views).

For deeper coverage, see the [architecture doc](https://docs.greptime.com/contributor-guide/overview/#architecture) or [DeepWiki](https://deepwiki.com/GreptimeTeam/greptimedb/1-overview).

<a href="https://github.com/GreptimeTeam/greptimedb/raw/main/docs/architecture.png" target="_blank" rel="noopener">
  <img alt="GreptimeDB System Overview" src="https://cdn.jsdelivr.net/gh/GreptimeTeam/greptimedb@main/docs/architecture.png">
</a>

## Try GreptimeDB

**For AI agents** — paste this prompt into your agent:

```text
Read https://docs.greptime.com/SKILL.md and follow the instructions
to deploy, configure, ingest, and query GreptimeDB.
```

```shell
docker run -p 127.0.0.1:4000-4003:4000-4003 \
  -v "$(pwd)/greptimedb_data:/greptimedb_data" \
  --name greptime --rm \
  greptime/greptimedb:latest standalone start \
  --http-addr 0.0.0.0:4000 \
  --grpc-bind-addr 0.0.0.0:4001 \
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
* [Rust toolchain](https://www.rust-lang.org/tools/install) — nightly, pinned by [`rust-toolchain.toml`](https://github.com/GreptimeTeam/greptimedb/blob/main/rust-toolchain.toml)
* [Protobuf compiler](https://grpc.io/docs/protoc-installation/) (>= 3.15)
* C/C++ building essentials: `gcc` / `g++` / `autoconf` and the glibc dev package (`libc6-dev` on Ubuntu, `glibc-devel` on Fedora)
* Python toolchain (optional, only for some test scripts)

**Build and run:**
```bash
make                          # build greptime binary
cargo run -- standalone start # start in standalone mode
```

**Common dev commands:**
```bash
make fmt            # format Rust code
make clippy         # lint (fails on warnings)
make test           # unit + integration tests (uses cargo-nextest)
make sqlness-test   # SQL regression tests
```

See the [Contribution Guidelines](CONTRIBUTING.md) for the full developer workflow.

## Tools & Extensions

- **Kubernetes**: [GreptimeDB Operator](https://github.com/GreptimeTeam/greptimedb-operator)
- **Helm Charts**: [Greptime Helm Charts](https://github.com/GreptimeTeam/helm-charts)
- **Dashboard**: [Web UI](https://github.com/GreptimeTeam/dashboard)
- **gRPC Ingester**: [Go](https://github.com/GreptimeTeam/greptimedb-ingester-go), [Java](https://github.com/GreptimeTeam/greptimedb-ingester-java), [C++](https://github.com/GreptimeTeam/greptimedb-ingester-cpp), [Erlang](https://github.com/GreptimeTeam/greptimedb-ingester-erl), [Rust](https://github.com/GreptimeTeam/greptimedb-ingester-rust), [.NET](https://github.com/GreptimeTeam/greptimedb-ingester-dotnet), [TypeScript](https://github.com/GreptimeTeam/greptimedb-ingester-ts)
- **Grafana Data Source**: [GreptimeDB Grafana data source plugin](https://github.com/GreptimeTeam/greptimedb-grafana-datasource)
- **Grafana Dashboard**: [Official Dashboard for monitoring](https://github.com/GreptimeTeam/greptimedb/blob/main/grafana/README.md)

## Project Status

GreptimeDB is generally available, with stable APIs and regular releases. It runs in production at scale — [OceanBase Cloud](https://greptime.com/blogs/2025-07-22-user-case-obcloud-log-management-greptimedb) operates 80+ GreptimeDB clusters managing 300 TB of logs, cutting log storage cost by 60%+ after migrating from Grafana Loki. See more in [case studies](https://greptime.com/blogs/?category=Use%20Case).

Read the [v1.0 highlights](https://greptime.com/blogs/2025-11-05-greptimedb-v1-highlights) and [2026 roadmap](https://greptime.com/blogs/2026-02-11-greptimedb-roadmap-2026), or browse the [version reference](https://docs.greptime.com/nightly/reference/about-greptimedb-version).

If GreptimeDB is useful to you, please star the repo.

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

GreptimeDB is an open-core project. Its core is licensed under the
[Apache License 2.0](https://apache.org/licenses/LICENSE-2.0.txt).

A small set of peripheral, enterprise-only features are gated behind the
`enterprise` Cargo feature (not built by default) and are governed by the
separate [GreptimeDB Enterprise License](LICENSE-ENTERPRISE). Source files under
that license carry an explicit Enterprise License header.

## Commercial Support

Scaling observability on your infrastructure?
[GreptimeDB Enterprise](https://docs.greptime.com/enterprise/overview/) adds the operational,
security, and support layer for production deployments.
[Contact us](https://greptime.com/contactus) for details.

## Contributing

- Read our [Contribution Guidelines](CONTRIBUTING.md).
- Explore [Internal Concepts](https://docs.greptime.com/contributor-guide/overview.html) and [DeepWiki](https://deepwiki.com/GreptimeTeam/greptimedb).
- Pick up a [good first issue](https://github.com/GreptimeTeam/greptimedb/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22) and join the #contributors [Slack](https://greptime.com/slack) channel.

## Acknowledgement

Special thanks to all contributors! See [AUTHOR.md](AUTHOR.md).

- Uses [Apache Arrow™](https://arrow.apache.org/) (memory model)
- [Apache Parquet™](https://parquet.apache.org/) (file storage)
- [Apache DataFusion™](https://datafusion.apache.org/) (query engine)
- [Apache OpenDAL™](https://opendal.apache.org/) (data access abstraction)

---

*All trademarks, logos, and brand names referenced in this README and in the Overview diagram are the property of their respective owners. Their use is for identification purposes only and does not imply endorsement or affiliation.*
