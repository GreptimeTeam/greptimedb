<p align="center">
  <picture>
    <source media="(prefers-color-scheme: light)" srcset="https://cdn.jsdelivr.net/gh/GreptimeTeam/greptimedb@main/docs/logo-text-padding.png">
    <source media="(prefers-color-scheme: dark)" srcset="https://cdn.jsdelivr.net/gh/GreptimeTeam/greptimedb@main/docs/logo-text-padding-dark.png">
    <img alt="GreptimeDB Logo" src="https://cdn.jsdelivr.net/gh/GreptimeTeam/greptimedb@main/docs/logo-text-padding.png" width="400px">
  </picture>
</p>

[![codecov](https://codecov.io/gh/GrepTimeTeam/greptimedb/branch/main/graph/badge.svg?token=FITFDI3J3C)](https://codecov.io/gh/GrepTimeTeam/greptimedb)
[![GitHub Actions](https://github.com/GreptimeTeam/greptimedb/actions/workflows/develop.yml/badge.svg)](https://github.com/GreptimeTeam/greptimedb/actions/workflows/develop.yml)
[![License](https://img.shields.io/github/license/greptimeTeam/greptimedb)](https://github.com/greptimeTeam/greptimedb/blob/main/LICENSE)

<br/>

[![Twitter](https://img.shields.io/badge/twitter-follow_us-1d9bf0.svg?style=for-the-badge)](https://twitter.com/greptime/)
[![LinkedIn](https://img.shields.io/badge/linkedin-connect_with_us-0a66c2.svg?style=for-the-badge)](https://www.linkedin.com/company/greptime/)
[![Slack](https://img.shields.io/badge/slack-GreptimeDB-0abd59?logo=slack&style=for-the-badge)](https://greptime.com/slack)

## What is GreptimeDB

GreptimeDB is an open-source time-series database focusing on efficiency, scalability, and analytical capabilities.
It's designed to work on infrastructure of the cloud era, and users benefit from its elasticity and commodity storage.

Our core developers have been building time-series data platforms for years. Based on their best-practices, GreptimeDB is born to give you:

* **Compatible with InfluxDB, Prometheus and more protocols**: Widely adopted database protocols and APIs, including MySQL, PostgreSQL, and Prometheus Remote Storage, etc. [Read more](https://docs.greptime.com/user-guide/clients/overview).
* **Easy horizontal scaling**: Seamless scalability from a standalone binary at edge to a robust, highly available distributed cluster in cloud, with a transparent experience for both developers and administrators.
* **Analyzing time-series data**: Native SQL and PromQL for queries, and Python scripting to facilitate complex analytical tasks.
* **Cloud-native distributed database**: Fully open-source distributed cluster architecture that harnesses the power of cloud-native elastic computing resources.
* **Performance and Cost-effective**: Flexible indexing capabilities and distributed, parallel-processing query engine, tackling high cardinality issues down. Optimized columnar layout for handling time-series data; compacted, compressed, and stored on various storage backends, particularly cloud object storage with 50x cost efficiency.

## Quickstart with [GreptimePlay](https://greptime.com/playground)

Try out the features of GreptimeDB right from your browser.

## Up & Running

The recommended way to install GreptimeDB is via Docker:

```shell
docker pull greptime/greptimedb
```

Start a GreptimeDB container with:

```shell
docker run -p 4000-4003:4000-4003 \
  -p 4242:4242 -v "$(pwd)/greptimedb:/tmp/greptimedb" \
  --name greptime --rm \
  greptime/greptimedb standalone start \
  --http-addr 0.0.0.0:4000 \
  --rpc-addr 0.0.0.0:4001 \
  --mysql-addr 0.0.0.0:4002 \
  --postgres-addr 0.0.0.0:4003 \
  --opentsdb-addr 0.0.0.0:4242
```

Connect to the server and test:

```shell
curl -X POST -d 'sql=SELECT 42&format=csv' http://localhost:4000/v1/sql
```

You should get a reply as:

```
42
```

Read more on docs:

* [Installation](https://docs.greptime.com/getting-started/installation/overview)
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

## Documentation

- [User guide](https://docs.greptime.com/user-guide/concepts/overview)
- [API docs](https://greptimedb.rs)

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
