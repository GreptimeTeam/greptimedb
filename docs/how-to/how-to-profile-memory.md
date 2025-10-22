# Profile memory usage of GreptimeDB

This crate provides an easy approach to dump memory profiling info. A set of ready to use scripts is provided in [docs/how-to/memory-profile-scripts](./memory-profile-scripts/scripts).

## Prerequisites
### jemalloc
jeprof is already compiled in the target directory of GreptimeDB. You can find the binary and use it.
```
# find jeprof binary
find . -name 'jeprof'
# add executable permission
chmod +x <path_to_jeprof>
```
The path is usually under `./target/${PROFILE}/build/tikv-jemalloc-sys-${HASH}/out/build/bin/jeprof`.
The default version of jemalloc installed from the package manager may not have the `--collapsed` option.
You may need to check the whether the `jeprof` version is >= `5.3.0` if you want to install it from the package manager.
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

### Enable memory profiling for greptimedb binary

Start GreptimeDB instance with environment variables:

```bash
# for Linux
MALLOC_CONF=prof:true ./target/debug/greptime standalone start

# for macOS
_RJEM_MALLOC_CONF=prof:true ./target/debug/greptime standalone start
```

### Memory profiling for greptimedb docker image

We have memory profiling enabled and activated by default in our official docker
image.

This behavior is controlled by configuration `enable_heap_profiling`:

```toml
[memory]
# Whether to enable heap profiling activation during startup.
# Default is true.
enable_heap_profiling = true
```

To disable memory profiling, set `enable_heap_profiling` to `false`.

### Memory profiling control

You can control heap profiling activation using the new HTTP APIs:

```bash
# Check current profiling status
curl -X GET localhost:4000/debug/prof/mem/status

# Activate heap profiling (if not already active)
curl -X POST localhost:4000/debug/prof/mem/activate

# Deactivate heap profiling
curl -X POST localhost:4000/debug/prof/mem/deactivate

# Activate gdump feature that dumps memory profiling data every time virtual memory usage exceeds previous maximum value.
curl -X POST localhost:4000/debug/prof/mem/gdump -d 'activate=true'

# Deactivate gdump.
curl -X POST localhost:4000/debug/prof/mem/gdump -d 'activate=false'

# Retrieve current gdump status.
curl -X GET localhost:4000/debug/prof/mem/gdump
```

### Dump memory profiling data

Dump memory profiling data through HTTP API:

```bash
curl -X POST localhost:4000/debug/prof/mem > greptime.hprof
# or output flamegraph directly
curl -X POST "localhost:4000/debug/prof/mem?output=flamegraph" > greptime.svg
# or output pprof format
curl -X POST "localhost:4000/debug/prof/mem?output=proto" > greptime.pprof
```

You can periodically dump profiling data and compare them to find the delta memory usage.

## Analyze profiling data with flamegraph

To create flamegraph according to dumped profiling data:

```bash
sudo apt install -y libjemalloc-dev

jeprof <path_to_greptime_binary> <profile_data> --collapse | ./flamegraph.pl > mem-prof.svg

jeprof <path_to_greptime_binary> --base <baseline_prof> <profile_data> --collapse | ./flamegraph.pl > output.svg
```
