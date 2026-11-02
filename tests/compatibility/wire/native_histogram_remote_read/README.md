# Native histogram sampled Remote Read wire compatibility

`response.pb` is a frozen `prompb.ReadResponse` generated from Prometheus
revision `e471c7708c6b0b13e2050222bbd5a907b36f4113`, independently of
`greptime-proto`. It contains one query result, one series named
`native_histogram_remote_read`, and nine samples at 1000ms intervals.

| Timestamp (ms) | Independent expectation |
| --- | --- |
| 1000 | Integer count `2^53 + 13`, zero count `2^53 + 1`, both bucket sides `[1,3,2]`, wire deltas `[1,2,-1]` across span boundaries, schema 1, reset hint 2 |
| 2000 | Float count 6, zero count 0.5, absolute buckets `[2,3.5]`, schema 2, gauge hint |
| 3000 | Integer custom buckets `[1,2]`, one observation in `(-Inf,1]` |
| 4000 | Float custom buckets `[1,2]`, absolute counts `[1,2.5]`, gauge hint |
| 5000 | Empty integer histogram with explicit count and zero-count oneofs |
| 6000 | Empty float histogram with explicit count and zero-count oneofs |
| 7000 | Integer count 8, buckets `[3,2]`, wire deltas `[3,-1]`, ordinary NaN sum bits `0x7ff8000000000123` |
| 8000 | Empty integer histogram, stale NaN sum bits `0x7ff0000000000002` |
| 9000 | Integer count and zero count `i64::MAX` |

The 420-byte fixture has SHA-256 `f1414bfa6f0821e920a88e325964abfad0269b05b7c0643a84ca6e908729d1eb`.
Generation used Go `go1.27.3 darwin/arm64`, the pinned Prometheus `go.mod` and
`go.sum` (declared Go version `1.26.10`), and its generated `prompb` types.
Direct wire dependencies were `github.com/gogo/protobuf v1.3.2` and
`github.com/golang/snappy v1.0.0`. No local `protoc` or Greptime protobuf
generator is involved.

## Automated regression

From the GreptimeDB checkout:

```shell
cargo nextest run -p servers -E 'test(native_histogram_remote_read_wire_fixture)'
```

This constructs stored arrays from independent Rust inputs, checks fixed
absolute bucket counts, runs the actual record-batch encoder, Snappy-compresses
and protobuf-decodes its output, and compares all fields with the upstream
fixture. NaN payloads are compared by bits. It also checks fixed wire deltas;
converting those deltas back with an upstream helper is not the expected-value
generator. Tests never regenerate the frozen fixture.

## Reproduce and check with Prometheus

Use a scratch module to preserve the Prometheus checkout. Set the two checkout
paths, create a scratch directory, and save the Go program below as `main.go`
in that directory:

```shell
PROMETHEUS_CHECKOUT=/path/to/prometheus
GREPTIMEDB_CHECKOUT=/path/to/greptimedb
NH_SCRATCH=$(mktemp -d)
git -C "$PROMETHEUS_CHECKOUT" rev-parse HEAD
cp "$PROMETHEUS_CHECKOUT/go.mod" "$PROMETHEUS_CHECKOUT/go.sum" "$NH_SCRATCH/"
cd "$NH_SCRATCH"
go mod edit -module=native-histogram-interop
go mod edit -require=github.com/prometheus/prometheus@v0.0.0
go mod edit -replace="github.com/prometheus/prometheus=$PROMETHEUS_CHECKOUT"
# Save the Go program below as main.go here before running these commands.
go run -mod=mod . generate "$NH_SCRATCH/response.pb"
shasum -a 256 "$NH_SCRATCH/response.pb"
cmp "$NH_SCRATCH/response.pb" "$GREPTIMEDB_CHECKOUT/tests/compatibility/wire/native_histogram_remote_read/response.pb"
cd "$GREPTIMEDB_CHECKOUT"
GREPTIME_NATIVE_HISTOGRAM_REMOTE_READ_OUTPUT="$NH_SCRATCH/greptime-response.snappy" \
  cargo nextest run -p servers -E 'test(native_histogram_remote_read_wire_fixture)'
cd "$NH_SCRATCH"
go run -mod=mod . check "$NH_SCRATCH/greptime-response.snappy"
```

The check consumes `storage/remote.FromQueryResult` iterators to exhaustion,
checks both iterator and series-set errors, integer/float types, exact integer
counts, timestamps, spans, bucket placement, custom bounds, reset hints, and
NaN bits. It calls both `AtHistogram` and `AtFloatHistogram` on integer custom
samples, including a single finite-bound bucket. Intentional integer-to-float
conversion is not expected to retain large-integer precision.

Verified on 2026-10-26 against the response emitted by the Rust test:
`PASS: 9 histogram samples, both iterator accessors, no series or iterator errors`.

The tested baseline is the exact revision above, not a claimed minimum
Prometheus release. This Go check is a manual implementation check; it is not
part of the existing Go CI. The Rust regression is the automated guard.

```go
package main

import (
    "fmt"
    "math"
    "os"
    "reflect"

    "github.com/golang/snappy"
    "github.com/prometheus/prometheus/prompb"
    "github.com/prometheus/prometheus/storage/remote"
    "github.com/prometheus/prometheus/tsdb/chunkenc"
)

func expected() *prompb.ReadResponse {
    hs := []prompb.Histogram{
        {Count: &prompb.Histogram_CountInt{CountInt: (1<<53)+13}, ZeroCount: &prompb.Histogram_ZeroCountInt{ZeroCountInt: (1<<53)+1}, Sum: 10, Schema: 1, ZeroThreshold: 0.001, ResetHint: prompb.Histogram_NO,
         PositiveSpans: []prompb.BucketSpan{{Offset: -2, Length: 2}, {Offset: 3, Length: 1}}, PositiveDeltas: []int64{1,2,-1}, NegativeSpans: []prompb.BucketSpan{{Offset: -3, Length: 1}, {Offset: 2, Length: 2}}, NegativeDeltas: []int64{1,2,-1}},
        {Count: &prompb.Histogram_CountFloat{CountFloat: 6}, ZeroCount: &prompb.Histogram_ZeroCountFloat{ZeroCountFloat: 0.5}, Sum: 20, Schema: 2, ZeroThreshold: 0.002, ResetHint: prompb.Histogram_GAUGE,
         PositiveSpans: []prompb.BucketSpan{{Offset: 3, Length: 2}}, PositiveCounts: []float64{2,3.5}},
        {Count: &prompb.Histogram_CountInt{CountInt: 1}, ZeroCount: &prompb.Histogram_ZeroCountInt{}, Sum: 1, Schema: -53, CustomValues: []float64{1,2}, PositiveSpans: []prompb.BucketSpan{{Length: 1}}, PositiveDeltas: []int64{1}},
        {Count: &prompb.Histogram_CountFloat{CountFloat: 3.5}, ZeroCount: &prompb.Histogram_ZeroCountFloat{}, Sum: 4, Schema: -53, CustomValues: []float64{1,2}, PositiveSpans: []prompb.BucketSpan{{Length: 2}}, PositiveCounts: []float64{1,2.5}, ResetHint: prompb.Histogram_GAUGE},
        {Count: &prompb.Histogram_CountInt{}, ZeroCount: &prompb.Histogram_ZeroCountInt{}},
        {Count: &prompb.Histogram_CountFloat{}, ZeroCount: &prompb.Histogram_ZeroCountFloat{}},
        {Count: &prompb.Histogram_CountInt{CountInt: 8}, ZeroCount: &prompb.Histogram_ZeroCountInt{}, Sum: math.Float64frombits(0x7ff8000000000123), PositiveSpans: []prompb.BucketSpan{{Length: 2}}, PositiveDeltas: []int64{3,-1}},
        {Count: &prompb.Histogram_CountInt{}, ZeroCount: &prompb.Histogram_ZeroCountInt{}, Sum: math.Float64frombits(0x7ff0000000000002)},
        {Count: &prompb.Histogram_CountInt{CountInt: math.MaxInt64}, ZeroCount: &prompb.Histogram_ZeroCountInt{ZeroCountInt: math.MaxInt64}},
    }
    for i := range hs { hs[i].Timestamp = int64(i+1)*1000 }
    return &prompb.ReadResponse{Results: []*prompb.QueryResult{{Timeseries: []*prompb.TimeSeries{{Labels: []prompb.Label{{Name:"__name__", Value:"native_histogram_remote_read"}}, Histograms:hs}}}}}
}

func check(ok bool, args ...any) { if !ok { panic(fmt.Sprint(args...)) } }
func main() {
    want := expected()
    if os.Args[1] == "generate" {
        data, err := want.Marshal(); if err != nil { panic(err) }
        if err := os.WriteFile(os.Args[2], data, 0644); err != nil { panic(err) }
        fmt.Printf("generated %d bytes\n", len(data)); return
    }
    data, err := os.ReadFile(os.Args[2]); if err != nil { panic(err) }
    data, err = snappy.Decode(nil, data); if err != nil { panic(err) }
    var got prompb.ReadResponse
    if err = got.Unmarshal(data); err != nil { panic(err) }
    check(len(got.Results)==1 && len(got.Results[0].Timeseries)==1, "result/series count")
    hs := got.Results[0].Timeseries[0].Histograms
    expectedHs := want.Results[0].Timeseries[0].Histograms
    check(len(hs)==len(expectedHs), "histogram count")
    for i := range hs {
        g,w := hs[i], expectedHs[i]
        check(math.Float64bits(g.Sum)==math.Float64bits(w.Sum), "sum bits ", i)
        g.Sum,w.Sum=0,0
        check(reflect.DeepEqual(g,w), "protobuf fields ", i, ": ", g, " expected ", w)
    }
    ss := remote.FromQueryResult(true, got.Results[0])
    samples,series := 0,0
    for ss.Next() {
        series++
        it := ss.At().Iterator(nil)
        for typ := it.Next(); typ != chunkenc.ValNone; typ=it.Next() {
            w := expectedHs[samples]
            if w.IsFloatHistogram() {
                check(typ==chunkenc.ValFloatHistogram, "float type ", samples)
                t,h:=it.AtFloatHistogram(nil)
                check(t==w.Timestamp && h.Count==w.GetCountFloat() && h.ZeroCount==w.GetZeroCountFloat(), "float count/time ", samples)
                check(reflect.DeepEqual(h.PositiveBuckets,w.PositiveCounts), "float buckets ", samples)
                check(reflect.DeepEqual(h.CustomValues,w.CustomValues), "float bounds ", samples)
                check(math.Float64bits(h.Sum)==math.Float64bits(w.Sum), "float sum ", samples)
                check(int32(h.CounterResetHint)==int32(w.ResetHint), "float reset ", samples)
            } else {
                check(typ==chunkenc.ValHistogram, "integer type ", samples)
                t,h:=it.AtHistogram(nil)
                check(t==w.Timestamp && h.Count==w.GetCountInt() && h.ZeroCount==w.GetZeroCountInt(), "integer count/time ", samples)
                check(reflect.DeepEqual(h.PositiveBuckets,w.PositiveDeltas) && reflect.DeepEqual(h.NegativeBuckets,w.NegativeDeltas), "integer deltas ", samples)
                check(reflect.DeepEqual(h.CustomValues,w.CustomValues), "integer bounds ", samples)
                check(math.Float64bits(h.Sum)==math.Float64bits(w.Sum), "integer sum ", samples)
                check(int32(h.CounterResetHint)==int32(w.ResetHint), "integer reset ", samples)
                _,f:=it.AtFloatHistogram(nil)
                check(reflect.DeepEqual(f.CustomValues,w.CustomValues), "converted bounds ", samples)
                if w.Schema == -53 {
                    buckets:=f.PositiveBucketIterator()
                    check(buckets.Next(), "custom bucket missing")
                    b:=buckets.At()
                    check(b.Lower==math.Inf(-1) && b.Upper==1 && b.Count==1, "custom bucket placement: ",b)
                    check(!buckets.Next(), "extra custom bucket")
                }
            }
            _,f:=it.AtFloatHistogram(nil)
            check(f.Schema==w.Schema && f.ZeroThreshold==w.ZeroThreshold, "schema/threshold ",samples)
            check(len(f.PositiveSpans)==len(w.PositiveSpans) && len(f.NegativeSpans)==len(w.NegativeSpans), "span counts ",samples)
            for i,s:=range f.PositiveSpans { check(s.Offset==w.PositiveSpans[i].Offset && s.Length==w.PositiveSpans[i].Length,"positive spans") }
            for i,s:=range f.NegativeSpans { check(s.Offset==w.NegativeSpans[i].Offset && s.Length==w.NegativeSpans[i].Length,"negative spans") }
            samples++
        }
        check(it.Err()==nil,"iterator error: ",it.Err())
    }
    check(ss.Err()==nil,"series error: ",ss.Err())
    check(series==1 && samples==9,"series/sample count ",series,"/",samples)
    fmt.Println("PASS: 9 histogram samples, both iterator accessors, no series or iterator errors")
}
```
