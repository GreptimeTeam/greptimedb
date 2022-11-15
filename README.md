<p align="center">
    <img src="/docs/logo-text-padding.png" alt="GreptimeDB Logo" width="400px"></img>
</p>

<h3 align="center">
    The next-generation hybrid timeseries/analytics processing database in the cloud
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
</p>

## What is GreptimeDB

GreptimeDB is an open-source time-series database with a special focus on
scalability, analytical capabilities and efficiency. It's designed to work on
infrastructure of the cloud era, and users benefit from its elasticity and commodity
storage.

Our core developers have been building time-series data platform
for years. Based on their best-practices, GreptimeDB is born to give you:

- A standalone binary that scales to highly-available distributed cluster, providing a transparent experience for cluster users
- Optimized columnar layout for handling time-series data; compacted, compressed, stored on various storage backends
- Flexible index options, tackling high cardinality issues down
- Distributed, parallel query execution, leveraging elastic computing resource
- Native SQL, and Python scripting for advanced analytical scenarios
- Widely adopted database protocols and APIs
- Extensible table engine architecture for extensive workloads

## Quick Start

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
  find an installation instructions
  [here](https://grpc.io/docs/protoc-installation/).

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

For more startup options, greptimedb's **distributed mode** and information
about Kubernetes deployment, check our [docs](https://greptime.com/docs).

### Connect

1. Connect to GreptimeDB via standard [MySQL
   client](https://dev.mysql.com/downloads/mysql/):

   ```
   # The standalone instance listen on port 4002 by default.
   mysql -h 127.0.0.1 -P 4002
   ```

2. Create table:

   ```SQL
   CREATE TABLE monitor (
     host STRING,
     ts TIMESTAMP,
     cpu DOUBLE DEFAULT 0,
     memory DOUBLE,
     TIME INDEX (ts),
     PRIMARY KEY(host)) ENGINE=mito WITH(regions=1);
   ```

3. Insert some data:

   ```SQL
   INSERT INTO monitor(host, cpu, memory, ts) VALUES ('host1', 66.6, 1024, 1660897955000);
   INSERT INTO monitor(host, cpu, memory, ts) VALUES ('host2', 77.7, 2048, 1660897956000);
   INSERT INTO monitor(host, cpu, memory, ts) VALUES ('host3', 88.8, 4096, 1660897957000);
   ```

4. Query the data:

   ```SQL
   mysql> SELECT * FROM monitor;
   +-------+---------------------+------+--------+
   | host  | ts                  | cpu  | memory |
   +-------+---------------------+------+--------+
   | host1 | 2022-08-19 08:32:35 | 66.6 |   1024 |
   | host2 | 2022-08-19 08:32:36 | 77.7 |   2048 |
   | host3 | 2022-08-19 08:32:37 | 88.8 |   4096 |
   +-------+---------------------+------+--------+
   3 rows in set (0.01 sec)
   ```

You can always cleanup test database by removing `/tmp/greptimedb`.

## Resources

- [Pre-built Binaries](https://github.com/GreptimeTeam/greptimedb/releases):
  downloadable pre-built binaries for Linux and MacOS
- [Docker Images](https://hub.docker.com/r/greptime/greptimedb): pre-built
  Docker images
- [`gtctl`](https://github.com/GreptimeTeam/gtctl): the command-line tool for
  Kubernetes deployment
- [GreptimeDB Java
  Client](https://github.com/GreptimeTeam/greptimedb-client-java)

## Project Status

This project is in its early stage and under heavy development. We move fast and
break things. Benchmark on development branch may not represent its potential
performance. We release pre-built binaries constantly for functional
evaluation. Do not use it in production at the moment.

## Community

Our core team is thrilled too see you participate in any ways you like. When you are stuck, try to
ask for help by filling an issue with a detailed description of what you were trying to do
and what went wrong. If you have any questions or if you would like to get involved in our
community, please check out:

- GreptimeDB Community on [Slack](https://greptime.com/slack)
- GreptimeDB GitHub [Discussions](https://github.com/GreptimeTeam/greptimedb/discussions)
- Greptime official [Website](https://greptime.com)

In addition, you may:

- View our official [Blog](https://greptime.com/blog)
- Connect us with [Linkedin](https://www.linkedin.com/company/greptime/)
- Follow us on [Twitter](https://twitter.com/greptime)

## Documentation

- GreptimeDB [User Guide](https://greptime.com/docs/user-guide)
- GreptimeDB [Developer Guide](https://greptime.com/docs/developer-guide)

## License

GreptimeDB uses the [Apache 2.0 license][1] to strike a balance between
open contributions and allowing you to use the software however you want.

[1]: <https://github.com/greptimeTeam/greptimedb/blob/develop/LICENSE>

## Contributing

Please refer to [contribution guidelines](CONTRIBUTING.md) for more information.
