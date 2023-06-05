# Profiling CPU

## Build GreptimeDB with `pprof` feature

```bash
cargo build --features=pprof
```

## HTTP API
Sample at 99 Hertz, for 5 seconds, output report in [protobuf format](https://github.com/google/pprof/blob/master/proto/profile.proto).
```bash
curl -s '0:4000/v1/prof/cpu' > /tmp/pprof.out
```

Then you can use `pprof` command with the protobuf file.
```bash
go tool pprof -top /tmp/pprof.out
```

Sample at 99 Hertz, for 60 seconds, output report in flamegraph format.
```bash
curl -s '0:4000/v1/prof/cpu?seconds=60&output=flamegraph' > /tmp/pprof.svg
```

Sample at 49 Hertz, for 10 seconds, output report in text format.
```bash
curl -s '0:4000/v1/prof/cpu?seconds=10&frequency=49&output=text' > /tmp/pprof.txt
```
