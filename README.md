# GreptimeDB

[![codecov](https://codecov.io/gh/GrepTimeTeam/greptimedb/branch/develop/graph/badge.svg?token=FITFDI3J3C)](https://codecov.io/gh/GrepTimeTeam/greptimedb)

GreptimeDB: the next-generation hybrid timeseries/analytics processing database in the cloud.

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

### Start in standalone mode

```
// Start datanode and frontend with default options.
cargo run  -- --log-level=debug standalone start

OR

// Start with `http-addr` option.
cargo run  -- --log-level=debug standalone start --http-addr=0.0.0.0:9999

OR

// Start with `mysql-addr` option.
cargo run  -- --log-level=debug standalone start --mysql-addr=0.0.0.0:9999

OR
// Start datanode with `log-dir` and `log-level` options.
cargo run  -- --log-dir=logs --log-level=debug standalone start --mysql-addr=0.0.0.0:4102

```

Start with config file:

```
cargo run -- --log-dir=logs --log-level=debug standalone start -c ./config/standalone.example.toml
```

Start datanode by running docker container:

```
docker run -p 3000:3000 \
-p 3001:3001 \
-p 3306:3306 \
greptimedb
```

### SQL Operations

1. Connecting DB by [mysql client](https://dev.mysql.com/downloads/mysql/):

   ```
   # The standalone instance listen on port 4002 by default.
   mysql -h 127.0.0.1 -P 4002
   ```

2. Create a database;
```SQL
CREATE DATABASE hello_greptime;
```

2. Create table:

   ```SQL
   CREATE TABLE hello_greptime.monitor (
     host STRING,
     ts TIMESTAMP,
     cpu DOUBLE DEFAULT 0,
     memory DOUBLE,
     TIME INDEX (ts),
     PRIMARY KEY(host)) ENGINE=mito WITH(regions=1);
   ```

3. Insert data:

   ```SQL
   INSERT INTO hello_greptime.monitor(host, cpu, memory, ts) VALUES ('host1', 66.6, 1024, 1660897955000);
   INSERT INTO hello_greptime.monitor(host, cpu, memory, ts) VALUES ('host2', 77.7, 2048, 1660897956000);
   INSERT INTO hello_greptime.monitor(host, cpu, memory, ts) VALUES ('host3', 88.8, 4096, 1660897957000);
   ```

4. Query data:

   ```SQL
   mysql> SELECT * FROM hello_greptime.monitor;
   +-------+---------------------+------+--------+
   | host  | ts                  | cpu  | memory |
   +-------+---------------------+------+--------+
   | host1 | 2022-08-19 08:32:35 | 66.6 |   1024 |
   | host2 | 2022-08-19 08:32:36 | 77.7 |   2048 |
   | host3 | 2022-08-19 08:32:37 | 88.8 |   4096 |
   +-------+---------------------+------+--------+
   3 rows in set (0.01 sec)
   ```
   You can delete your data by removing `/tmp/greptimedb`.

## Contributing

Please refer to [contribution guidelines](CONTRIBUTING.md) for more information.
