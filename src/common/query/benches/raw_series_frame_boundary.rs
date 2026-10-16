// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Synthetic A/C boundary benchmark for the raw-series frame codec.
//!
//! Arm A (`a_direct_consume`) feeds a fresh lazy raw-series source straight into the shared digest
//! consumer; arm C (`c_frame_roundtrip`) runs the same source through [`pack_raw_series`] and
//! [`unpack_raw_series`] before that same consumer. Both arms share one fixture, one source
//! generator, and one consumer, with no mode enum shim.
//!
//! Setup boundary: the current-thread runtime, each fixture's field schema, the constant explicit
//! metric `raw_bench_metric{host="bench-0"}`, and the shared histogram template are built once
//! outside the timed phase; every `iter_batched` setup only clones those handles into a fresh lazy
//! source. Chunk buffers are generated inside the timed phase, one chunk per poll, and neither
//! input nor output is collected.
//!
//! Scale: the six fixture identities (key length, rows per chunk, field kinds) are retained, but
//! every fixture runs at the fixed [`BENCH_SERIES`] series count: a reusable synthetic boundary
//! measurement, not a numerical replica of the earlier campaign, and not an allocation, RSS, or
//! throughput claim.
//!
//! Run:
//! ```text
//! cargo bench --locked -p common-query --bench raw_series_frame_boundary -- --test
//! cargo bench --locked -p common-query --bench raw_series_frame_boundary
//! cargo bench --locked -p common-query --bench raw_series_frame_boundary -- float3_key128
//! ```

use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use std::hint::black_box;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use common_error::ext::BoxedError;
use common_query::series_frame::{pack_raw_series, unpack_raw_series};
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use datatypes::arrow::array::Float64Array;
use datatypes::arrow::buffer::{Buffer, ScalarBuffer};
use datatypes::series::histogram::{
    BucketSideView, CounterResetHint, HistogramBatch, HistogramBatchBuilder, HistogramView,
};
use datatypes::series::raw::{RawSeriesChunk, RawTimestamps};
use datatypes::series::samples::{FieldSpec, SampleRef, Samples, SeriesFields, SeriesSchema};
use datatypes::series::{Label, MetricName, SeriesOrdering};
use futures_util::{Stream, StreamExt};
use store_api::region_engine::{RawLabelSource, RawSeriesStream, SendableRawSeriesStream};
use tokio::runtime::Runtime;

/// Series per iteration for every fixture: one fixed small scale for all six identities.
const BENCH_SERIES: usize = 256;
/// Rows of the shared histogram template; also the modulus of a sample's template row.
const HISTOGRAM_ROWS: usize = 4;
/// Payload NaN pinned to position 512 of the 1024-row fixture.
const PAYLOAD_NAN_BITS: u64 = 0x7ff8_0000_0000_0042;
/// Per-position digest discriminants for the shared consumer's sample view.
const TAG_MISSING: u8 = 0;
const TAG_FLOAT: u8 = 1;
const TAG_HISTOGRAM: u8 = 2;
/// Field-kind groups, so every fixture identity stays a single table row.
const FLOAT2: &[FieldKind] = &[FieldKind::Float, FieldKind::Float];
const FLOAT3: &[FieldKind] = &[FieldKind::Float, FieldKind::Float, FieldKind::Float];
const LONG_FLOAT: &[FieldKind] = &[FieldKind::Float];
const HISTOGRAM: &[FieldKind] = &[FieldKind::Histogram];
const MIXED: &[FieldKind] = &[FieldKind::Float, FieldKind::Histogram];
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FieldKind {
    Float,
    Histogram,
}

/// Fixture identity: name, key length, rows per chunk, field kinds.
struct Fixture(&'static str, usize, usize, &'static [FieldKind]);
/// The six fixture identities, in run order.
static FIXTURES: &[Fixture] = &[
    Fixture("float2_key16", 16, 2, FLOAT2),
    Fixture("float3_key16", 16, 3, FLOAT3),
    Fixture("float3_key128", 128, 3, FLOAT3),
    Fixture("long_float1024_key16", 16, 1024, LONG_FLOAT),
    Fixture("histogram3_key16", 16, 3, HISTOGRAM),
    Fixture("multifield_float_hist3_key16", 16, 3, MIXED),
];
/// Float column of one chunk field: payload `series * 0.5 + position * 0.25 + field * 8`, missing
/// at every position where `(position * 3 + field + series) % 4 == 3`. The only fixture with more
/// than 512 rows keeps one float field and pins position 511 to negative zero and 512 to the
/// payload NaN; both stay present, so those exact bits must cross the boundary even where the
/// generic formula would mark them missing.
fn float_column(fixture: &Fixture, series: usize, field: usize) -> Vec<Option<f64>> {
    let oversize = fixture.2 > 512 && field == 0;
    (0..fixture.2)
        .map(|position| match position {
            511 if oversize => Some(-0.0),
            512 if oversize => Some(f64::from_bits(PAYLOAD_NAN_BITS)),
            _ if (position * 3 + field + series) % 4 == 3 => None,
            _ => Some(series as f64 * 0.5 + position as f64 * 0.25 + field as f64 * 8.0),
        })
        .collect()
}

/// Histogram reference column of one chunk field: present where `(position * 5 + field + series)
/// % 3 != 2`, referencing template row `(series + position) % HISTOGRAM_ROWS`.
fn histogram_column(fixture: &Fixture, series: usize, field: usize) -> Vec<Option<u32>> {
    (0..fixture.2)
        .map(|position| {
            let row = ((series + position) % HISTOGRAM_ROWS) as u32;
            ((position * 5 + field + series) % 3 != 2).then_some(row)
        })
        .collect()
}

/// One template row: custom bounds, then positive and negative (offsets, lengths, buckets).
type S<T> = &'static [T];
type Row = (S<f64>, S<i32>, S<i32>, S<f64>, S<i32>, S<i32>, S<f64>);
/// Four distinct rows covering every reset hint, start timestamps, custom bounds, and both bucket
/// sides; built once per fixture outside the timed phase.
fn histogram_template() -> Arc<HistogramBatch> {
    // Row order is the reset-hint order: unknown, counter reset, no reset, gauge.
    let rows: [Row; HISTOGRAM_ROWS] = [
        (&[1.5, 4.5], &[1], &[2], &[3.25, 7.5], &[-2], &[1], &[0.75]),
        (&[], &[0], &[1], &[0.5], &[0], &[0], &[]),
        (&[0.25], &[2], &[1], &[9.5], &[1], &[2], &[2.5, 1.25]),
        (&[-1.75], &[0], &[1], &[4.5], &[-1], &[2], &[0.125, 0.5]),
    ];
    let starts = [Some(-123), None, Some(456), None];
    let mut builder = HistogramBatchBuilder::new();
    for (index, (custom, po, pl, pb, no, nl, nb)) in rows.iter().copied().enumerate() {
        builder
            .push(HistogramView {
                schema: index as i32 - 1,
                zero_threshold: 0.125,
                sum: 10.5 + index as f64,
                count: 17.25,
                zero_count: 2.5,
                reset_hint: CounterResetHint::from(index as i32),
                start_timestamp: starts[index],
                custom_values: custom,
                positive: BucketSideView {
                    span_offsets: po,
                    span_lengths: pl,
                    buckets: pb,
                },
                negative: BucketSideView {
                    span_offsets: no,
                    span_lengths: nl,
                    buckets: nb,
                },
            })
            .expect("valid template histogram row");
    }
    builder.finish().expect("valid template histogram table")
}

/// One fixture's shared inputs, built once outside the timed phase.
struct Shared {
    fixture: &'static Fixture,
    schema: Arc<SeriesSchema>,
    metric: MetricName,
    histogram: Arc<HistogramBatch>,
    label_source: RawLabelSource,
}
impl Shared {
    fn new(fixture: &'static Fixture) -> Self {
        assert!(fixture.1 >= 8, "keys reserve the 8-byte ordinal");
        let mut fields = Vec::with_capacity(fixture.3.len());
        for index in 0..fixture.3.len() {
            fields.push(FieldSpec {
                name: Arc::from(format!("f{index}")),
            });
        }
        let tags = vec![Label {
            name: Arc::from("host"),
            value: Arc::from("bench-0"),
        }];
        let metric = MetricName::try_new(Some(Arc::from("raw_bench_metric")), tags)
            .expect("constant bench metric");
        Self {
            fixture,
            schema: Arc::new(SeriesSchema {
                fields: fields.into(),
                eval_grid: None,
            }),
            metric,
            histogram: histogram_template(),
            label_source: RawLabelSource::Explicit,
        }
    }
    /// Creates a fresh lazy source; only these shared handles are cloned here.
    fn source(self: &Arc<Self>) -> SendableRawSeriesStream {
        Box::pin(BenchSource {
            shared: self.clone(),
            emitted: 0,
        })
    }
}

/// Lazy source: one complete chunk per series, generated on demand with fresh buffers.
struct BenchSource {
    shared: Arc<Shared>,
    emitted: usize,
}
impl BenchSource {
    /// Builds one chunk: big-endian ordinal key with deterministic padding, strictly increasing
    /// timestamps, and schema-aligned fields; every chunk is a series' final chunk.
    fn build_chunk(&self, series: usize) -> Result<RawSeriesChunk, BoxedError> {
        let shared = &self.shared;
        let fixture = shared.fixture;
        let Fixture(_, key_len, rows_per_chunk, field_kinds) = fixture;
        let mut key = vec![series as u8; *key_len];
        key[..8].copy_from_slice(&(series as u64).to_be_bytes());
        let times = (0..*rows_per_chunk)
            .map(|position| series as i64 * 1_000_000 + position as i64)
            .collect::<Vec<i64>>();
        let mut fields = Vec::with_capacity(field_kinds.len());
        for (field, kind) in field_kinds.iter().enumerate() {
            fields.push(match kind {
                FieldKind::Float => {
                    Samples::from_float(Float64Array::from(float_column(fixture, series, field)))
                }
                FieldKind::Histogram => Samples::try_from_histogram(
                    histogram_column(fixture, series, field)
                        .into_iter()
                        .collect(),
                    shared.histogram.clone(),
                )
                .map_err(BoxedError::new)?,
            });
        }
        RawSeriesChunk::try_new(
            &shared.schema,
            Buffer::from_vec(key),
            Some(shared.metric.clone()),
            RawTimestamps::try_new(ScalarBuffer::from(times)).map_err(BoxedError::new)?,
            SeriesFields::try_from_vec(fields).map_err(BoxedError::new)?,
            true,
        )
        .map_err(BoxedError::new)
    }
}
impl Stream for BenchSource {
    type Item = Result<RawSeriesChunk, BoxedError>;
    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        if this.emitted >= BENCH_SERIES {
            return Poll::Ready(None);
        }
        let series = this.emitted;
        this.emitted += 1;
        Poll::Ready(Some(this.build_chunk(series)))
    }
}
impl RawSeriesStream for BenchSource {
    fn schema(&self) -> &Arc<SeriesSchema> {
        &self.shared.schema
    }
    fn label_source(&self) -> &RawLabelSource {
        &self.shared.label_source
    }
    fn ordering(&self) -> SeriesOrdering {
        SeriesOrdering::KeyThenTime
    }
}
/// Counters plus payload hash of one consumed stream.
#[derive(Debug, Default, PartialEq, Eq)]
struct Digest {
    hash: u64,
    series: usize,
    positions: usize,
    fields: usize,
    finals: usize,
    negative_zero: usize,
    payload_nan: usize,
}

/// Shared, representation-independent consumer used by both arms: it walks each chunk in order and
/// digests the complete payload through [`SampleRef`] with length prefixes: key, explicit metric
/// name and tags, finality, timestamp length and values, schema field names, and every position's
/// missing marker, exact float bits (including negative zero and payload NaN), or all fourteen
/// histogram members.
async fn consume(
    mut stream: SendableRawSeriesStream,
    schema: &SeriesSchema,
) -> Result<Digest, BoxedError> {
    let mut digest = Digest::default();
    let mut hasher = DefaultHasher::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        digest.series += 1;
        digest.finals += usize::from(chunk.is_last);
        digest.positions += chunk.time.len();
        digest.fields += chunk.fields.len();
        write_bytes(&mut hasher, chunk.primary_key.as_slice());
        match &chunk.metric {
            Some(metric) => {
                hasher.write_u8(1);
                hasher.write_u8(u8::from(metric.name().is_some()));
                metric
                    .name()
                    .iter()
                    .for_each(|name| write_bytes(&mut hasher, name.as_bytes()));
                hasher.write_u64(metric.tags().len() as u64);
                for tag in metric.tags() {
                    write_bytes(&mut hasher, tag.name.as_bytes());
                    write_bytes(&mut hasher, tag.value.as_bytes());
                }
            }
            None => hasher.write_u8(0),
        }
        hasher.write_u8(u8::from(chunk.is_last));
        hasher.write_u64(chunk.time.len() as u64);
        chunk
            .time
            .as_buffer()
            .iter()
            .for_each(|time| hasher.write_i64(*time));
        for (index, field) in chunk.fields.iter().enumerate() {
            write_bytes(&mut hasher, schema.fields[index].name.as_bytes());
            for position in 0..chunk.time.len() {
                match field.sample(position) {
                    SampleRef::Missing => hasher.write_u8(TAG_MISSING),
                    SampleRef::Float(value) => {
                        let bits = value.to_bits();
                        hasher.write_u8(TAG_FLOAT);
                        hasher.write_u64(bits);
                        digest.negative_zero += usize::from(bits == (-0.0_f64).to_bits());
                        digest.payload_nan += usize::from(bits == PAYLOAD_NAN_BITS);
                    }
                    SampleRef::Histogram(view) => {
                        hasher.write_u8(TAG_HISTOGRAM);
                        digest_histogram(&mut hasher, &view);
                    }
                }
            }
        }
    }
    digest.hash = hasher.finish();
    Ok(digest)
}

/// Digests all fourteen logical histogram members, every variable-length member length-prefixed:
/// schema, threshold, sum, count, zero, reset hint, start option, custom values, and both sides'
/// span offsets, span lengths, and buckets.
fn digest_histogram(hasher: &mut DefaultHasher, view: &HistogramView<'_>) {
    hasher.write_i32(view.schema);
    for member in [view.zero_threshold, view.sum, view.count, view.zero_count] {
        hasher.write_u64(member.to_bits());
    }
    hasher.write_i32(i32::from(view.reset_hint));
    let (present, start) = match view.start_timestamp {
        Some(start) => (1u8, start),
        None => (0, 0),
    };
    hasher.write_u8(present);
    hasher.write_i64(start);
    hasher.write_u64(view.custom_values.len() as u64);
    view.custom_values
        .iter()
        .for_each(|v| hasher.write_u64(v.to_bits()));
    for side in [view.positive, view.negative] {
        hasher.write_u64(side.span_offsets.len() as u64);
        side.span_offsets.iter().for_each(|v| hasher.write_i32(*v));
        hasher.write_u64(side.span_lengths.len() as u64);
        side.span_lengths.iter().for_each(|v| hasher.write_i32(*v));
        hasher.write_u64(side.buckets.len() as u64);
        side.buckets
            .iter()
            .for_each(|v| hasher.write_u64(v.to_bits()));
    }
}
fn write_bytes(hasher: &mut DefaultHasher, bytes: &[u8]) {
    hasher.write_u64(bytes.len() as u64);
    hasher.write(bytes);
}
/// Arm A: consume the lazy source directly.
async fn direct_digest(source: SendableRawSeriesStream) -> Digest {
    let schema = source.schema().clone();
    consume(source, &schema).await.expect("direct consume")
}
/// Arm C: pack the same source, unpack the frames, then consume the unpacked stream.
async fn roundtrip_digest(source: SendableRawSeriesStream) -> Digest {
    let schema = source.schema().clone();
    let labels = source.label_source().clone();
    let frames = pack_raw_series(source).expect("pack raw series");
    let unpacked = unpack_raw_series(frames, schema.clone(), labels, SeriesOrdering::KeyThenTime)
        .expect("unpack raw series");
    consume(unpacked, &schema).await.expect("roundtrip consume")
}
/// Correctness gate, run once per fixture before any timing: both arms digest the complete payload
/// through the shared consumer and must agree with each other and with the fixture's expected shape
/// counters, final markers, and exact negative-zero and payload-NaN bits, which stay present even
/// where the null formula would mark those two positions missing.
fn verify(shared: &Arc<Shared>, runtime: &Runtime) {
    let Fixture(name, _, rows_per_chunk, field_kinds) = shared.fixture;
    let direct = runtime.block_on(direct_digest(shared.source()));
    let roundtrip = runtime.block_on(roundtrip_digest(shared.source()));
    assert_eq!(direct.series, BENCH_SERIES, "{name}: series");
    assert_eq!(
        direct.positions,
        BENCH_SERIES * rows_per_chunk,
        "{name}: positions"
    );
    assert_eq!(
        direct.fields,
        BENCH_SERIES * field_kinds.len(),
        "{name}: fields"
    );
    assert_eq!(direct.finals, BENCH_SERIES, "{name}: final markers");
    let specials = usize::from(*name == "long_float1024_key16") * BENCH_SERIES;
    assert_eq!(direct.negative_zero, specials, "{name}: negative zero bits");
    assert_eq!(direct.payload_nan, specials, "{name}: payload NaN bits");
    assert_eq!(direct, roundtrip, "{name}: A/C digest mismatch");
}
fn bench_fixture(c: &mut Criterion, runtime: &Runtime, fixture: &'static Fixture) {
    let shared = Arc::new(Shared::new(fixture));
    verify(&shared, runtime);
    let mut run_a = |source| black_box(runtime.block_on(direct_digest(source)));
    let mut run_c = |source| black_box(runtime.block_on(roundtrip_digest(source)));
    let mut group = c.benchmark_group(format!("raw_series_frame_boundary/{}", fixture.0));
    group.bench_function("a_direct_consume", |b| {
        b.iter_batched(|| shared.source(), &mut run_a, BatchSize::SmallInput);
    });
    group.bench_function("c_frame_roundtrip", |b| {
        b.iter_batched(|| shared.source(), &mut run_c, BatchSize::SmallInput);
    });
    group.finish();
}
fn boundaries(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("current-thread bench runtime");
    for fixture in FIXTURES {
        bench_fixture(c, &runtime, fixture);
    }
}

criterion_group!(benches, boundaries);
criterion_main!(benches);
