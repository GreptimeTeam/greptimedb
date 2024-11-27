# Profiling CPU

## HTTP API
Sample at 99 Hertz, for 5 seconds, output report in [protobuf format](https://github.com/google/pprof/blob/master/proto/profile.proto).
```bash
curl -X POST -s '0:4000/debug/prof/cpu' > /tmp/pprof.out
```

Then you can use `pprof` command with the protobuf file.
```bash
go tool pprof -top /tmp/pprof.out
```

Sample at 99 Hertz, for 60 seconds, output report in flamegraph format.
```bash
curl -X POST -s '0:4000/debug/prof/cpu?seconds=60&output=flamegraph' > /tmp/pprof.svg
```

Sample at 49 Hertz, for 10 seconds, output report in text format.
```bash
curl -X POST -s '0:4000/debug/prof/cpu?seconds=10&frequency=49&output=text' > /tmp/pprof.txt
```

## Using `perf`

First find the pid of GreptimeDB:

Using `perf record` to profile GreptimeDB, at the sampling frequency of 99 hertz, and a duration of 60 seconds:

```bash
perf record -p <pid> --call-graph dwarf -F 99 -- sleep 60
```

The result will be saved to file `perf.data`.

Then

```bash
perf script --no-inline > perf.out
```

Produce a flame graph out of it:

```bash
git clone https://github.com/brendangregg/FlameGraph

FlameGraph/stackcollapse-perf.pl perf.out > perf.folded

FlameGraph/flamegraph.pl perf.folded > perf.svg
```
