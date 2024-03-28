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

use std::intrinsics::unreachable;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use api::v1::{ColumnDataType, ColumnSchema, SemanticType};
use benchmarks::metrics;
use benchmarks::wal::Region;
use clap::{Parser, ValueEnum};
use common_base::readable_size::ReadableSize;
use common_telemetry::info;
use common_wal::config::kafka::common::BackoffConfig;
use common_wal::config::kafka::DatanodeKafkaConfig as KafkaConfig;
use common_wal::config::raft_engine::RaftEngineConfig;
use common_wal::options::{KafkaWalOptions, WalOptions};
use itertools::Itertools;
use log_store::kafka::log_store::{
    KafkaLogStore, APPEND_BATCH_ELAPSED_TOTAL, PRODUCED_ELAPSED_TOTAL,
};
use log_store::raft_engine::log_store::RaftEngineLogStore;
use mito2::wal::Wal;
use prometheus::{Encoder, TextEncoder};
use rand::distributions::{Alphanumeric, DistString};
use rand::rngs::SmallRng;
use rand::SeedableRng;
use rskafka::client::partition::Compression;
use rskafka::client::ClientBuilder;
use store_api::logstore::LogStore;
use store_api::storage::RegionId;
use tokio::sync::Barrier;

#[derive(Clone, ValueEnum, Default, Debug, PartialEq)]
enum WalProvider {
    #[default]
    Kafka,
    RaftEngine,
}

#[derive(Parser)]
struct Args {
    /// There are two modes to run the benchmarker:
    /// - dedicated: regions are separated into several disjoint sets and each set of regions is bound to a dedicated worker.
    /// - steal: regions are not separated and could be handled by any worker.
    #[clap(long, default_value_t = false)]
    dedicated: bool,

    /// The wal provider.
    #[clap(long, value_enum, default_value_t = WalProvider::default())]
    wal_provider: WalProvider,

    /// The advertised addresses of the kafka brokers.
    /// If there're multiple bootstrap brokers, their addresses should be separated by comma, for e.g. "localhost:9092,localhost:9093".
    #[clap(long, short = 'b', default_value = "localhost:9092")]
    bootstrap_brokers: String,

    /// The number of workers each running in a dedicated thread.
    #[clap(long, default_value_t = num_cpus::get() as u32)]
    num_workers: u32,

    /// The number of kafka topics to be created.
    #[clap(long, default_value_t = 32)]
    num_topics: u32,

    /// The number of regions.
    #[clap(long, default_value_t = 1000)]
    num_regions: u32,

    /// The number of times each region is scraped.
    /// The total amount of data written into the wal are identical for both modes.
    /// However, in the steal mode, the amount of data written to the wal in a single write will be relatively large than that in the dedicated mode.
    #[clap(long, default_value_t = 1000)]
    num_scrapes: u32,

    /// The number of rows in each wal entry.
    /// Each time a region is scraped, a wal entry containing will be produced.
    #[clap(long, default_value_t = 5)]
    num_rows: u32,

    /// The column types of the schema for each region.
    /// Currently, three column types are supported:
    /// - i = ColumnDataType::Int64
    /// - f = ColumnDataType::Float64
    /// - s = ColumnDataType::String  
    /// For e.g., "ifs" will be parsed as three columns: i64, f64, and string.
    #[clap(long, default_value = "ifs")]
    col_types: String,

    /// The maximum size of a batch of kafka records.
    /// The default value is 1mb.
    #[clap(long, default_value = "1mb")]
    max_batch_size: ReadableSize,

    /// The minimum latency the kafka client issues a batch of kafka records.
    /// However, a batch of kafka records would be immediately issued if a record cannot be fit into the batch.
    #[clap(long, default_value = "20ms")]
    linger: ReadableSize,

    /// The initial backoff delay of the kafka consumer.
    #[clap(long, default_value = "10ms")]
    backoff_init: ReadableSize,

    /// The maximum backoff delay of the kafka consumer.
    #[clap(long, default_value = "1s")]
    backoff_max: ReadableSize,

    /// The exponential backoff rate of the kafka consumer. The next back off = base * the current backoff.
    #[clap(long, default_value_t = 2)]
    backoff_base: u32,

    /// The deadline of backoff. The backoff ends if the total backoff delay reaches the deadline.
    #[clap(long, default_value = "3s")]
    backoff_deadline: ReadableSize,

    /// The client-side compression algorithm for kafka records.
    #[clap(long, default_value = "zstd")]
    compression: String,

    /// The seed of random number generators.
    #[clap(long, default_value_t = 42)]
    rng_seed: u64,

    /// Skips the read phase, aka. region replay, if set to true.
    #[clap(long, default_value_t = false)]
    skip_read: bool,

    /// Skips the write phase if set to true.
    #[clap(long, default_value_t = false)]
    skip_write: bool,

    /// Randomly generates topic names if set to true.
    /// Useful when you want to run the benchmarker without worrying about the topics created before.
    #[clap(long, default_value_t = false)]
    random_topics: bool,

    /// Logs out the gathered prometheus metrics when the benchmarker ends.
    #[clap(long, default_value_t = false)]
    report_metrics: bool,
}

/// Benchmarker config.
#[derive(Debug, Clone)]
struct Config {
    dedicated: bool,
    wal_provider: WalProvider,
    bootstrap_brokers: Vec<String>,
    num_workers: u32,
    num_topics: u32,
    num_regions: u32,
    num_scrapes: u32,
    num_rows: u32,
    col_types: String,
    max_batch_size: u64,
    linger: u64,
    backoff_init: Duration,
    backoff_max: Duration,
    backoff_base: u32,
    backoff_deadline: Duration,
    compression: Compression,
    rng_seed: u64,
    skip_read: bool,
    skip_write: bool,
    random_topics: bool,
    report_metrics: bool,
}

struct Benchmarker;

impl Benchmarker {
    async fn run_dedicated<S: LogStore>(cfg: &Config, topics: &[String], wal: Arc<Wal<S>>) {
        let chunk_size = (cfg.num_regions as f32 / cfg.num_workers as f32).ceil() as usize;
        let region_chunks = (0..cfg.num_regions)
            .map(|id| {
                build_region(
                    id as u64,
                    topics,
                    &mut SmallRng::seed_from_u64(cfg.rng_seed),
                    cfg,
                )
            })
            .chunks(chunk_size)
            .into_iter()
            .map(|chunk| Arc::new(chunk.collect::<Vec<_>>()))
            .collect::<Vec<_>>();

        let mut write_elapsed = 0;
        let mut read_elapsed = 0;

        if !cfg.skip_write {
            info!("Benchmarking write ...");

            let barrier = Arc::new(Barrier::new(cfg.num_workers as usize));
            let num_scrapes = cfg.num_scrapes;
            let write_start = Instant::now();

            futures::future::join_all((0..cfg.num_workers).map(|i| {
                let barrier = barrier.clone();
                let wal = wal.clone();
                let regions = region_chunks[i as usize].clone();
                tokio::spawn(async move {
                    barrier.wait().await;
                    for _ in 0..num_scrapes {
                        let mut wal_writer = wal.writer();
                        regions
                            .iter()
                            .for_each(|region| region.add_wal_entry(&mut wal_writer));
                        wal_writer.write_to_wal().await.unwrap();
                    }
                })
            }))
            .await;

            write_elapsed = write_start.elapsed().as_millis();
            assert!(write_elapsed > 0);
        }

        if !cfg.skip_read {
            info!("Benchmarking read ...");

            let barrier = Arc::new(Barrier::new(cfg.num_workers as usize));
            let read_start = Instant::now();

            futures::future::join_all((0..cfg.num_workers).map(|i| {
                let barrier = barrier.clone();
                let wal = wal.clone();
                let regions = region_chunks[i as usize].clone();
                tokio::spawn(async move {
                    barrier.wait().await;
                    for region in regions.iter() {
                        region.replay(&wal).await;
                    }
                })
            }))
            .await;

            read_elapsed = read_start.elapsed().as_millis();
            assert!(read_elapsed > 0);
        }

        dump_report(cfg, write_elapsed, read_elapsed);
    }

    async fn run_steal<S: LogStore>(cfg: &Config, topics: &[String], wal: Arc<Wal<S>>) {
        let regions = (0..cfg.num_regions)
            .map(|id| {
                build_region(
                    id as u64,
                    topics,
                    &mut SmallRng::seed_from_u64(cfg.rng_seed),
                    cfg,
                )
            })
            .collect::<Vec<_>>();
        let regions = Arc::new(regions);

        let mut write_elapsed = 0;
        let mut read_elapsed = 0;

        if !cfg.skip_write {
            info!("Benchmarking write ...");

            let barrier = Arc::new(Barrier::new(cfg.num_workers as usize));
            let scrapes = Arc::new(AtomicU32::new(0));
            let num_scrapes = cfg.num_scrapes;

            let write_start = Instant::now();

            futures::future::join_all((0..cfg.num_workers).map(|_| {
                let barrier = barrier.clone();
                let scrapes = scrapes.clone();
                let wal = wal.clone();
                let regions = regions.clone();

                tokio::spawn(async move {
                    barrier.wait().await;
                    while scrapes.fetch_add(1, Ordering::Relaxed) < num_scrapes {
                        let mut wal_writer = wal.writer();
                        regions
                            .iter()
                            .for_each(|region| region.add_wal_entry(&mut wal_writer));
                        wal_writer.write_to_wal().await.unwrap();
                    }
                })
            }))
            .await;

            write_elapsed = write_start.elapsed().as_millis();
            assert!(write_elapsed > 0);
        }

        if !cfg.skip_read {
            info!("Benchmarking read ...");

            let barrier = Arc::new(Barrier::new(cfg.num_workers as usize));
            let next_replay = Arc::new(AtomicUsize::new(0));
            let num_regions = cfg.num_regions;

            let read_start = Instant::now();

            futures::future::join_all((0..cfg.num_workers).map(|_| {
                let barrier = barrier.clone();
                let next_replay = next_replay.clone();
                let wal = wal.clone();
                let regions = regions.clone();

                tokio::spawn(async move {
                    barrier.wait().await;
                    loop {
                        let i = next_replay.fetch_add(1, Ordering::Relaxed);
                        if i >= num_regions as usize {
                            break;
                        }
                        regions[i].replay(&wal).await;
                    }
                })
            }))
            .await;

            read_elapsed = read_start.elapsed().as_millis();
            assert!(read_elapsed > 0);
        }

        dump_report(cfg, write_elapsed, read_elapsed);
    }
}

fn build_region(id: u64, topics: &[String], rng: &mut SmallRng, cfg: &Config) -> Region {
    let (rows_factor, cols_factor) = parse_workload(&cfg.workload);
    let wal_options = match cfg.wal_provider {
        WalProvider::Kafka => {
            assert!(!topics.is_empty());
            WalOptions::Kafka(KafkaWalOptions {
                topic: topics.get(id as usize % topics.len()).cloned().unwrap(),
            })
        }
        WalProvider::RaftEngine => WalOptions::RaftEngine,
    };
    Region::new(
        RegionId::from_u64(id),
        build_schema(cols_factor, rng),
        wal_options,
        rows_factor,
        cfg.rng_seed,
    )
}

fn build_schema(cols_factor: usize, mut rng: &mut SmallRng) -> Vec<ColumnSchema> {
    let ts_col = ColumnSchema {
        column_name: "ts".to_string(),
        datatype: ColumnDataType::TimestampMillisecond as i32,
        semantic_type: SemanticType::Tag as i32,
        datatype_extension: None,
    };
    let mut schema = vec![ts_col];

    for _ in 0..cols_factor {
        let i32_col = ColumnSchema {
            column_name: "i32_".to_string() + &Alphanumeric.sample_string(&mut rng, 5),
            datatype: ColumnDataType::Int32 as i32,
            semantic_type: SemanticType::Field as i32,
            datatype_extension: None,
        };
        let f32_col = ColumnSchema {
            column_name: "f32_".to_string() + &Alphanumeric.sample_string(&mut rng, 5),
            datatype: ColumnDataType::Float32 as i32,
            semantic_type: SemanticType::Field as i32,
            datatype_extension: None,
        };
        let str_col = ColumnSchema {
            column_name: "str_".to_string() + &Alphanumeric.sample_string(&mut rng, 5),
            datatype: ColumnDataType::String as i32,
            semantic_type: SemanticType::Field as i32,
            datatype_extension: None,
        };

        schema.append(&mut vec![i32_col, f32_col, str_col]);
    }

    schema
}

fn dump_report(cfg: &Config, write_elapsed: u128, read_elapsed: u128) {
    let cost_report = format!(
        "write costs: {} ms, read costs: {} ms, append_batch costs: {} ms, produce costs: {} ms",
        write_elapsed,
        read_elapsed,
        APPEND_BATCH_ELAPSED_TOTAL.load(Ordering::Relaxed),
        PRODUCED_ELAPSED_TOTAL.load(Ordering::Relaxed),
    );

    let total_written_bytes = metrics::METRIC_WAL_WRITE_BYTES_TOTAL.get();
    let write_throughput = if write_elapsed > 0 {
        total_written_bytes as f64 / write_elapsed as f64 * 1000.0
    } else {
        0.0
    };
    // This is the effective read throughput from which the read amplification is removed.
    let total_read_bytes = metrics::METRIC_WAL_READ_BYTES_TOTAL.get();
    let read_throughput = if read_elapsed > 0 {
        total_read_bytes as f64 / read_elapsed as f64 * 1000.0
    } else {
        0.0
    };

    let throughput_report = format!(
                "total written bytes: {} bytes, total read bytes: {} bytes, write throuput: {} bytes/s ({} mb/s), read throughput: {} bytes/s ({} mb/s)",
                total_written_bytes,
                total_read_bytes,
                write_throughput.floor() as u128,
                (write_throughput / (1 << 20) as f64).floor() as u128,
                read_throughput.floor() as u128,
                (read_throughput / (1 << 20) as f64).floor() as u128,
            );

    let metrics_report = if cfg.report_metrics {
        let mut buffer = Vec::new();
        let encoder = TextEncoder::new();
        let metrics = prometheus::gather();
        encoder.encode(&metrics, &mut buffer).unwrap();
        String::from_utf8(buffer).unwrap()
    } else {
        String::new()
    };

    info!("Benchmark config: {:?}\n\nBenchmark report:\n\n{cost_report}\n\n{throughput_report}\n\n{metrics_report}", cfg);
}

fn parse_col_types(col_types: &str) -> Vec<ColumnDataType> {
    col_types
        .chars()
        .map(|c| match c {
            'i' => ColumnDataType::Int64,
            'f' => ColumnDataType::Float64,
            's' => ColumnDataType::String,
            other => unreachable!(format!("Cannot parse {other} as a column data type")),
        })
        .collect()
}

fn parse_compression(comp: &str) -> Compression {
    match comp {
        "no" => Compression::NoCompression,
        "gzip" => Compression::Gzip,
        "lz4" => Compression::Lz4,
        "snappy" => Compression::Snappy,
        "zstd" => Compression::Zstd,
        other => unreachable!(format!("Unrecognized compression {other}")),
    }
}

fn main() {
    common_telemetry::init_default_ut_logging();

    let args = Args::parse();
    let cfg = Config {
        dedicated: args.dedicated,
        wal_provider: args.wal_provider,
        bootstrap_brokers: args
            .bootstrap_brokers
            .split(',')
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        num_workers: args.num_workers.min(num_cpus::get() as u32),
        num_topics: args.num_topics,
        num_regions: args.num_regions,
        num_scrapes: args.num_scrapes,
        num_rows: args.num_rows,
        col_types: parse_col_types(&args.col_types),
        max_batch_size: args.max_batch_size,
        linger: args.linger,
        backoff_init: Duration::from_millis(args.backoff_init),
        backoff_max: Duration::from_millis(args.backoff_max),
        backoff_base: args.backoff_base,
        backoff_deadline: Duration::from_millis(args.backoff_deadline),
        compression: parse_compression(&args.compression),
        rng_seed: args.rng_seed,
        skip_read: args.skip_read,
        skip_write: args.skip_write,
        random_topics: args.random_topics,
        report_metrics: args.report_metrics,
    };
    if !cfg.dedicated && cfg.wal_provider == WalProvider::RaftEngine {
        panic!("Benchmarker has to be run in the dedicated mode for raft-engine");
    }
    assert!(
        cfg.num_workers
            .min(cfg.num_topics)
            .min(cfg.num_regions)
            .min(cfg.num_scrapes)
            .min(cfg.max_batch_size as u32)
            .min(cfg.bootstrap_brokers.len() as u32)
            > 0
    );

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            match cfg.wal_provider {
                WalProvider::Kafka => {
                    // Creates topics.
                    let client = ClientBuilder::new(cfg.bootstrap_brokers.clone())
                        .build()
                        .await
                        .unwrap();
                    let ctrl_client = client.controller_client().unwrap();
                    let (topics, tasks): (Vec<_>, Vec<_>) = (0..cfg.num_topics)
                        .map(|i| {
                            let topic = if cfg.random_topics {
                                format!(
                                    "greptime_wal_bench_topic_{}_{}",
                                    uuid::Uuid::new_v4().as_u128(),
                                    i
                                )
                            } else {
                                format!("greptime_wal_bench_topic_{}", i)
                            };
                            let task = ctrl_client.create_topic(
                                topic.clone(),
                                1,
                                cfg.bootstrap_brokers.len() as i16,
                                2000,
                            );
                            (topic, task)
                        })
                        .unzip();
                    // Must ignore errors since we allow topics being created more than once.
                    let _ = futures::future::try_join_all(tasks).await;

                    let kafka_cfg = KafkaConfig {
                        broker_endpoints: cfg.bootstrap_brokers.clone(),
                        max_batch_size: ReadableSize::mb(cfg.max_batch_size),
                        linger: Duration::from_millis(cfg.linger),
                        backoff: BackoffConfig {
                            init: cfg.backoff_init,
                            max: cfg.backoff_max,
                            base: cfg.backoff_base,
                            deadline: Some(cfg.backoff_deadline),
                        },
                        compression: cfg.compression,
                        ..Default::default()
                    };
                    let store = Arc::new(KafkaLogStore::try_new(&kafka_cfg).await.unwrap());
                    let wal = Arc::new(Wal::new(store));

                    match cfg.dedicated {
                        true => Benchmarker::run_dedicated(&cfg, &topics, wal).await,
                        false => Benchmarker::run_steal(&cfg, &topics, wal).await,
                    }
                }
                WalProvider::RaftEngine => {
                    // The benchmarker assumes the raft engine directory exists.
                    let store = RaftEngineLogStore::try_new(
                        "/tmp/greptimedb/raft-engine-wal".to_string(),
                        RaftEngineConfig::default(),
                    )
                    .await
                    .map(Arc::new)
                    .unwrap();
                    let wal = Arc::new(Wal::new(store));
                    Benchmarker::run_dedicated(&cfg, &[], wal).await;
                }
            }
        });
}
