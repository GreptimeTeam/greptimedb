# GreptimeDB

[![codecov](https://codecov.io/gh/GrepTimeTeam/greptimedb/branch/develop/graph/badge.svg?token=FITFDI3J3C)](https://codecov.io/gh/GrepTimeTeam/greptimedb)

GreptimeDB: the next-generation hybrid timeseries/analytics processing database in the cloud.

## Usage

```
// Start data-node with default options.
cargo run -p greptimedb -- data-node start

OR

// Start data-node with `http-addr` option.
cargo run -p greptimedb -- data-node start --http-addr=0.0.0.0:9999

OR

// Start data-node with `log-dir` and `log-level` options.
cargo run -p greptimedb -- --log-dir=logs --log-level=debug data-node start
```