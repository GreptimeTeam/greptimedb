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

//! Index build and bloom search benchmarks over synthetic observability data.

use std::collections::{BTreeSet, HashMap};
use std::hint::black_box;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use async_trait::async_trait;
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use futures::{AsyncRead, AsyncReadExt};
use index::bitmap::BitmapType;
use index::bloom_filter::applier::{BloomFilterApplier, InListPredicate};
use index::bloom_filter::creator::BloomFilterCreator;
use index::bloom_filter::reader::BloomFilterReaderImpl;
use index::external_provider::{ExternalTempFileProvider, Reader, Writer};
use index::fulltext_index::Config;
use index::fulltext_index::create::{BloomFilterFulltextIndexCreator, FulltextIndexCreator};
use index::fulltext_index::tokenizer::{Analyzer, EnglishTokenizer};
use index::inverted_index::create::InvertedIndexCreator;
use index::inverted_index::create::sort::external_sort::ExternalSorter;
use index::inverted_index::create::sort_create::SortIndexCreator;
use index::inverted_index::format::writer::InvertedIndexBlobWriter;
use puffin::puffin_manager::{PuffinWriter, PutOptions};
use rand::seq::IndexedRandom;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

const ROWS: usize = 100_000;
const SEGMENT_ROWS: usize = 10240;
const ROW_GROUP_ROWS: usize = 102400;

/// Drains blobs so the bloom fulltext creator can finish without a puffin file.
struct DrainPuffinWriter;

#[async_trait]
impl PuffinWriter for DrainPuffinWriter {
    async fn put_blob<R>(
        &mut self,
        _key: &str,
        raw_data: R,
        _options: PutOptions,
        _properties: HashMap<String, String>,
    ) -> puffin::error::Result<u64>
    where
        R: AsyncRead + Send,
    {
        let mut buf = Vec::new();
        Box::pin(raw_data).read_to_end(&mut buf).await.unwrap();
        Ok(buf.len() as u64)
    }

    async fn put_dir(
        &mut self,
        _key: &str,
        _dir: PathBuf,
        _options: PutOptions,
        _properties: HashMap<String, String>,
    ) -> puffin::error::Result<u64> {
        unreachable!("bloom fulltext index only writes blobs")
    }

    fn set_footer_lz4_compressed(&mut self, _lz4_compressed: bool) {}

    async fn finish(self) -> puffin::error::Result<u64> {
        Ok(0)
    }
}

/// The benchmarks set no memory limit, so nothing is spilled.
struct NoSpill;

#[async_trait]
impl ExternalTempFileProvider for NoSpill {
    async fn create(&self, _: &str, _: &str) -> Result<Writer, index::error::Error> {
        unreachable!("no memory limit is set")
    }

    async fn read_all(&self, _: &str) -> Result<Vec<(String, Reader)>, index::error::Error> {
        Ok(vec![])
    }
}

fn uuid(rng: &mut ChaCha8Rng) -> String {
    let h = format!("{:032x}", rng.random::<u128>());
    format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    )
}

/// Access, auth, timeout and error log lines with request ids, IPs and numbers: most
/// tokens of a segment are distinct.
fn service_logs(rows: usize) -> Vec<String> {
    let mut rng = ChaCha8Rng::seed_from_u64(1);
    let paths = ["users", "orders", "items", "carts", "payments", "sessions"];
    (0..rows)
        .map(|_| {
            let ip = format!(
                "10.{}.{}.{}",
                rng.random_range(0..16),
                rng.random_range(0..256),
                rng.random_range(1..255)
            );
            match rng.random_range(0..10) {
                0..=5 => format!(
                    "INFO GET /api/v1/{}/{}/detail?page={} 200 {}ms request_id={} client={}",
                    paths.choose(&mut rng).unwrap(),
                    rng.random_range(0..200000),
                    rng.random_range(0..50),
                    rng.random_range(1..2000),
                    uuid(&mut rng),
                    ip
                ),
                6..=7 => format!(
                    "WARN connection to {}:{} timed out after {}ms retry={}",
                    ip,
                    rng.random_range(1024..65535),
                    rng.random_range(100..30000),
                    rng.random_range(0..5)
                ),
                _ => format!(
                    "ERROR failed to process message {} from queue orders-{}: deadline exceeded",
                    uuid(&mut rng),
                    rng.random_range(0..64)
                ),
            }
        })
        .collect()
}

/// Java stack traces: long lines where a few frame names repeat many times.
fn stack_trace_logs(rows: usize) -> Vec<String> {
    let mut rng = ChaCha8Rng::seed_from_u64(2);
    let frames = [
        "at org.apache.kafka.clients.consumer.KafkaConsumer.poll(KafkaConsumer.java:1250)",
        "at com.example.orders.OrderService.process(OrderService.java:88)",
        "at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136)",
        "at java.base/java.lang.Thread.run(Thread.java:833)",
    ];
    (0..rows)
        .map(|_| {
            let mut line = format!(
                "ERROR request {} failed: java.lang.IllegalStateException: retry exhausted",
                uuid(&mut rng)
            );
            for _ in 0..40 {
                line.push(' ');
                line.push_str(frames.choose(&mut rng).unwrap());
            }
            line
        })
        .collect()
}

/// Trace ids with temporal locality: each trace spreads over about a hundred rows.
fn trace_ids(rows: usize) -> Vec<String> {
    let mut rng = ChaCha8Rng::seed_from_u64(3);
    let traces = (0..rows / 10)
        .map(|_| format!("{:032x}", rng.random::<u128>()))
        .collect::<Vec<_>>();
    (0..rows)
        .map(|i| {
            let j = (i / 10) as i64 + rng.random_range(-50i64..=50);
            traces[j.clamp(0, traces.len() as i64 - 1) as usize].clone()
        })
        .collect()
}

/// Prometheus-like series sorted by labels; each series has `samples` rows.
fn series(num_series: usize) -> Vec<[String; 6]> {
    let mut rng = ChaCha8Rng::seed_from_u64(4);
    let mut series = (0..num_series)
        .map(|s| {
            let pod = s / 2;
            let ns = pod % 400;
            [
                format!("region-{}", ns % 4),
                format!("az-{}", ns % 12),
                format!("cluster-{}", ns % 40),
                format!("namespace-{ns}"),
                format!("pod-{:05x}-{pod}", rng.random_range(0..0xfffff)),
                format!("container-{}", s % 2),
            ]
        })
        .collect::<Vec<_>>();
    series.sort();
    series
}

fn bloom_creator() -> BloomFilterCreator {
    BloomFilterCreator::new(
        SEGMENT_ROWS,
        0.01,
        Arc::new(NoSpill),
        Arc::new(AtomicUsize::new(0)),
        None,
    )
}

fn bench_bloom_fulltext_build(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("bloom_fulltext_build");
    group.sample_size(10);

    for (name, lines) in [
        ("service_logs", service_logs(ROWS)),
        ("stack_traces", stack_trace_logs(ROWS / 10)),
    ] {
        group.throughput(Throughput::Elements(lines.len() as u64));
        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    BloomFilterFulltextIndexCreator::new(
                        Config::default(),
                        SEGMENT_ROWS,
                        0.01,
                        Arc::new(NoSpill),
                        Arc::new(AtomicUsize::new(0)),
                        None,
                    )
                },
                |mut creator| {
                    let lines = &lines;
                    rt.block_on(async move {
                        for line in lines {
                            creator.push_text(line).await.unwrap();
                        }
                        let written = creator
                            .finish(&mut DrainPuffinWriter, "blob", PutOptions::default())
                            .await
                            .unwrap();
                        black_box(written)
                    })
                },
                BatchSize::LargeInput,
            )
        });
    }
    group.finish();
}

fn bench_bloom_skipping_build(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("bloom_skipping_build");
    group.sample_size(10);

    let trace_ids = trace_ids(ROWS * 10);
    group.throughput(Throughput::Elements(trace_ids.len() as u64));
    group.bench_function("trace_id", |b| {
        b.iter_batched(
            bloom_creator,
            |mut creator| {
                let trace_ids = &trace_ids;
                rt.block_on(async move {
                    for id in trace_ids {
                        creator
                            .push_n_row_elem(1, Some(id.as_bytes()))
                            .await
                            .unwrap();
                    }
                    let mut out = Vec::new();
                    creator.finish(&mut out).await.unwrap();
                    black_box(out)
                })
            },
            BatchSize::LargeInput,
        )
    });

    // A sorted tag pushed as one run per series, as the flat SST path does.
    let series = series(ROWS);
    let samples = 10;
    group.throughput(Throughput::Elements((series.len() * samples) as u64));
    group.bench_function("sorted_tag_runs", |b| {
        b.iter_batched(
            bloom_creator,
            |mut creator| {
                let series = &series;
                rt.block_on(async move {
                    for s in series {
                        creator
                            .push_n_row_elem(samples, Some(s[4].as_bytes()))
                            .await
                            .unwrap();
                    }
                    let mut out = Vec::new();
                    creator.finish(&mut out).await.unwrap();
                    black_box(out)
                })
            },
            BatchSize::LargeInput,
        )
    });
    group.finish();
}

fn bench_inverted_build(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("inverted_build");
    group.sample_size(10);

    let series = series(ROWS);
    let samples = 10;
    let names = ["region", "az", "cluster", "namespace", "pod", "container"];
    group.throughput(Throughput::Elements((series.len() * samples) as u64));
    group.bench_function("six_tags", |b| {
        b.iter_batched(
            || {
                let factory = ExternalSorter::factory(
                    Arc::new(NoSpill),
                    None,
                    Arc::new(AtomicUsize::new(0)),
                    None,
                );
                SortIndexCreator::new(factory, NonZeroUsize::new(1024).unwrap())
            },
            |mut creator| {
                let series = &series;
                rt.block_on(async move {
                    for s in series {
                        for (name, value) in names.iter().zip(s) {
                            let spill =
                                creator.push_with_name_n(name, Some(value.as_bytes()), samples);
                            assert!(!spill, "no memory limit is set");
                        }
                    }
                    let mut out = Vec::new();
                    let mut writer = InvertedIndexBlobWriter::new(&mut out);
                    creator
                        .finish(&mut writer, BitmapType::Roaring)
                        .await
                        .unwrap();
                    black_box(out)
                })
            },
            BatchSize::LargeInput,
        )
    });
    group.finish();
}

#[allow(clippy::single_range_in_vec_init)]
fn bench_bloom_search(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("bloom_search");

    let lines = service_logs(ROWS * 10);
    let analyzer = Analyzer::new(Box::new(EnglishTokenizer), false);
    let blob = rt.block_on(async {
        let mut creator = bloom_creator();
        for line in &lines {
            creator
                .push_row_elems(analyzer.analyze_text(line).unwrap())
                .await
                .unwrap();
        }
        let mut out = Vec::new();
        creator.finish(&mut out).await.unwrap();
        out
    });

    let num_rows = lines.len();
    // Each row group tests every segment's filter against the three probes.
    let predicates = ["timed", "deadline", "nonexistent"]
        .iter()
        .map(|term| InListPredicate {
            list: BTreeSet::from([term.as_bytes().to_vec()]),
        })
        .collect::<Vec<_>>();
    group.bench_function("three_terms_per_row_group", |b| {
        b.iter_batched(
            || {
                let reader = BloomFilterReaderImpl::new(blob.clone());
                rt.block_on(BloomFilterApplier::new(Box::new(reader)))
                    .unwrap()
            },
            |mut applier| {
                rt.block_on(async {
                    let mut matched = 0;
                    for start in (0..num_rows).step_by(ROW_GROUP_ROWS) {
                        let end = (start + ROW_GROUP_ROWS).min(num_rows);
                        matched += applier
                            .search(&predicates, &[start..end], None)
                            .await
                            .unwrap()
                            .len();
                    }
                    black_box(matched)
                })
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_bloom_fulltext_build,
    bench_bloom_skipping_build,
    bench_inverted_build,
    bench_bloom_search
);
criterion_main!(benches);
