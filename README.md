<br>
<p align="center">
    <img src="/docs/logo-text-padding.png" alt="GreptimeDB Logo">
</p>
</br>

<h3 align="center">
    The next-generation hybrid timeseries/analytics processing database in the cloud
</h3>

<p align="center">
    <a href="https://codecov.io/gh/GrepTimeTeam/greptimedb"><img src="https://codecov.io/gh/GrepTimeTeam/greptimedb/branch/develop/graph/badge.svg?token=FITFDI3J3C"></img></a>
    &nbsp;
    <a href="https://github.com/GreptimeTeam/greptimedb/actions/workflows/develop.yml"><img src="https://github.com/GreptimeTeam/greptimedb/actions/workflows/develop.yml/badge.svg" alt="Continuous integration for developing"></img></a>
    &nbsp;
    <a href="https://github.com/greptimeTeam/greptimedb/blob/master/LICENSE"><img src="https://img.shields.io/github/license/greptimeTeam/greptimedb"></a>
</p>

<p align="center">
    <a href="https://twitter.com/greptime"><img src="https://img.shields.io/badge/twitter-follow_us-1d9bf0.svg"></a>
    &nbsp;
    <a href="https://www.linkedin.com/company/greptime/"><img src="https://img.shields.io/badge/linkedin-connect_with_us-0a66c2.svg"></a>
</p>


## Getting Started

### Prerequisites

To compile GreptimeDB from source, you'll need the following:
- Rust
- Protobuf

#### Rust

The easiest way to install Rust is to use [`rustup`](https://rustup.rs/), which will check our `rust-toolchain` file and install correct Rust version for you.

#### Protobuf

`protoc` is required for compiling `.proto` files. `protobuf` is available from
major package manager on macos and linux distributions. You can find an
installation instructions [here](https://grpc.io/docs/protoc-installation/).

### Build the Docker Image

```
docker build --network host -f docker/Dockerfile -t greptimedb .
```

## Usage

### Start Datanode

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

Start datanode with config file:

```
cargo run -- --log-dir=logs --log-level=debug datanode start -c ./config/datanode.example.toml
```

Start datanode by runing docker container:

```
docker run -p 3000:3000 \
-p 3001:3001 \
-p 3306:3306 \
greptimedb
```

### Start Frontend

Frontend should connect to Datanode, so **Datanode must have been started** at first!

```
// Connects to local Datanode at its default GRPC port: 3001

// Start Frontend with default options.
cargo run -- frontend start

OR

// Start Frontend with `mysql-addr` option.
cargo run -- frontend start --mysql-addr=0.0.0.0:9999

OR

// Start datanode with `log-dir` and `log-level` options.
cargo run -- --log-dir=logs --log-level=debug frontend start
```

Start datanode with config file:

```
cargo run -- --log-dir=logs --log-level=debug frontend start -c ./config/frontend.example.toml
```

### SQL Operations

1. Connecting DB by [mysql client](https://dev.mysql.com/downloads/mysql/):

   ```
   # The datanode listen on port 3306 by default.
   mysql -h 127.0.0.1 -P 3306
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

3. Insert data:

   ```SQL
   INSERT INTO monitor(host, cpu, memory, ts) VALUES ('host1', 66.6, 1024, 1660897955);
   INSERT INTO monitor(host, cpu, memory, ts) VALUES ('host2', 77.7, 2048, 1660897956);
   INSERT INTO monitor(host, cpu, memory, ts) VALUES ('host3', 88.8, 4096, 1660897957);
   ```

4. Query data:

   ```SQL
   mysql> SELECT * FROM monitor;
   +-------+------------+------+--------+
   | host  | ts         | cpu  | memory |
   +-------+------------+------+--------+
   | host1 | 1660897955 | 66.6 |   1024 |
   | host2 | 1660897956 | 77.7 |   2048 |
   | host3 | 1660897957 | 88.8 |   4096 |
   +-------+------------+------+--------+
   3 rows in set (0.01 sec)
   ```
   You can delete your data by removing `/tmp/greptimedb`.

## Contributing

Please refer to [contribution guidelines](CONTRIBUTING.md) for more information.
