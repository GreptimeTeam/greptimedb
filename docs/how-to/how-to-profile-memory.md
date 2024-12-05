# Profile memory usage of GreptimeDB

This crate provides an easy approach to dump memory profiling info.

## Prerequisites
### jemalloc
```bash
# for macOS
brew install jemalloc

# for Ubuntu
sudo apt install libjemalloc-dev
```

### [flamegraph](https://github.com/brendangregg/FlameGraph)

```bash
curl https://raw.githubusercontent.com/brendangregg/FlameGraph/master/flamegraph.pl > ./flamegraph.pl
```

## Profiling

Start GreptimeDB instance with environment variables:

```bash
MALLOC_CONF=prof:true ./target/debug/greptime standalone start
```

Dump memory profiling data through HTTP API:

```bash
curl localhost:4000/debug/prof/mem > greptime.hprof
```

You can periodically dump profiling data and compare them to find the delta memory usage.

## Analyze profiling data with flamegraph

To create flamegraph according to dumped profiling data:

```bash
sudo apt install -y libjemalloc-dev

jeprof <path_to_greptime_binary> <profile_data> --collapse | ./flamegraph.pl > mem-prof.svg

jeprof <path_to_greptime_binary> --base <baseline_prof> <profile_data> --collapse | ./flamegraph.pl > output.svg
```
