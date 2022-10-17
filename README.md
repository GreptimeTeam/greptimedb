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

## Contribute

1. [Install rust](https://www.rust-lang.org/tools/install)
2. [Install `pre-commit`](https://pre-commit.com/#plugins) for run hooks on every commit automatically such as `cargo fmt` etc.

```
$ pip install pre-commit

or

$ brew install pre-commit
$
```

3. Install the git hook scripts:

```
$ pre-commit install
pre-commit installed at .git/hooks/pre-commit

$ pre-commit install --hook-type commit-msg
pre-commit installed at .git/hooks/commit-msg

$ pre-commit install --hook-type pre-push
pre-commit installed at .git/hooks/pre-pus
```

now `pre-commit` will run automatically on `git commit`.

4. Check out branch from `develop` and make your contribution. Follow the [style guide](https://github.com/GreptimeTeam/docs/blob/main/style-guide/zh.md). Create a PR when you are ready, feel free and have fun!
