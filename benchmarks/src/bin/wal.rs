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

#![feature(int_roundings)]

use std::fs;
use std::sync::Arc;
use std::time::Instant;

use api::v1::{ColumnDataType, ColumnSchema, SemanticType};
use benchmarks::metrics;
use benchmarks::wal::{Args, Config, Region, WalProvider};
use clap::Parser;
use common_telemetry::info;
use common_wal::config::kafka::common::BackoffConfig;
use common_wal::config::kafka::DatanodeKafkaConfig as KafkaConfig;
use common_wal::config::raft_engine::RaftEngineConfig;
use common_wal::options::{KafkaWalOptions, WalOptions};
use itertools::Itertools;
use log_store::kafka::log_store::KafkaLogStore;
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

async fn run_benchmarker<S: LogStore>(cfg: &Config, topics: &[String], wal: Arc<Wal<S>>) {
    let chunk_size = cfg.num_regions.div_ceil(cfg.num_workers);
    let region_chunks = (0..cfg.num_regions)
        .map(|id| {
            build_region(
                id as u64,
                topics,
                &mut SmallRng::seed_from_u64(cfg.rng_seed),
                cfg,
            )
        })
        .chunks(chunk_size as usize)
        .into_iter()
        .map(|chunk| Arc::new(chunk.collect::<Vec<_>>()))
        .collect::<Vec<_>>();

    let mut write_elapsed = 0;
    let mut read_elapsed = 0;

    if !cfg.skip_write {
        info!("Benchmarking write ...");

        let num_scrapes = cfg.num_scrapes;
        let timer = Instant::now();
        futures::future::join_all((0..cfg.num_workers).map(|i| {
            let wal = wal.clone();
            let regions = region_chunks[i as usize].clone();
            tokio::spawn(async move {
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
        write_elapsed += timer.elapsed().as_millis() as u64;
    }

    if !cfg.skip_read {
        info!("Benchmarking read ...");

        let timer = Instant::now();
        futures::future::join_all((0..cfg.num_workers).map(|i| {
            let wal = wal.clone();
            let regions = region_chunks[i as usize].clone();
            tokio::spawn(async move {
                for region in regions.iter() {
                    region.replay(&wal).await;
                }
            })
        }))
        .await;
        read_elapsed = timer.elapsed().as_millis() as u64;
    }

    dump_report(cfg, write_elapsed, read_elapsed);
}

fn build_region(id: u64, topics: &[String], rng: &mut SmallRng, cfg: &Config) -> Region {
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
        build_schema(&parse_col_types(&cfg.col_types), rng),
        wal_options,
        cfg.num_rows,
        cfg.rng_seed,
    )
}

fn build_schema(col_types: &[ColumnDataType], mut rng: &mut SmallRng) -> Vec<ColumnSchema> {
    col_types
        .iter()
        .map(|col_type| ColumnSchema {
            column_name: Alphanumeric.sample_string(&mut rng, 5),
            datatype: *col_type as i32,
            semantic_type: SemanticType::Field as i32,
            datatype_extension: None,
        })
        .chain(vec![ColumnSchema {
            column_name: "ts".to_string(),
            datatype: ColumnDataType::TimestampMillisecond as i32,
            semantic_type: SemanticType::Tag as i32,
            datatype_extension: None,
        }])
        .collect()
}

fn dump_report(cfg: &Config, write_elapsed: u64, read_elapsed: u64) {
    let cost_report = format!(
        "write costs: {} ms, read costs: {} ms",
        write_elapsed, read_elapsed,
    );

    let total_written_bytes = metrics::METRIC_WAL_WRITE_BYTES_TOTAL.get();
    let write_throughput = if write_elapsed > 0 {
        (total_written_bytes * 1000).div_floor(write_elapsed)
    } else {
        0
    };
    let total_read_bytes = metrics::METRIC_WAL_READ_BYTES_TOTAL.get();
    let read_throughput = if read_elapsed > 0 {
        (total_read_bytes * 1000).div_floor(read_elapsed)
    } else {
        0
    };

    let throughput_report = format!(
        "total written bytes: {} bytes, total read bytes: {} bytes, write throuput: {} bytes/s ({} mb/s), read throughput: {} bytes/s ({} mb/s)",
        total_written_bytes,
        total_read_bytes,
        write_throughput,
        write_throughput.div_floor(1 << 20),
        read_throughput,
        read_throughput.div_floor(1 << 20),
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

    info!(
        r#"
Benchmark config: 
{cfg:?}

Benchmark report:
{cost_report}
{throughput_report}
{metrics_report}"#
    );
}

async fn create_topics(cfg: &Config) -> Vec<String> {
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

    topics
}

fn parse_compression(comp: &str) -> Compression {
    match comp {
        "no" => Compression::NoCompression,
        "gzip" => Compression::Gzip,
        "lz4" => Compression::Lz4,
        "snappy" => Compression::Snappy,
        "zstd" => Compression::Zstd,
        other => unreachable!("Unrecognized compression {other}"),
    }
}

fn parse_col_types(col_types: &str) -> Vec<ColumnDataType> {
    let parts = col_types.split('x').collect::<Vec<_>>();
    assert!(parts.len() <= 2);

    let pattern = parts[0];
    let repeat = parts
        .get(1)
        .map(|r| r.parse::<usize>().unwrap())
        .unwrap_or(1);

    pattern
        .chars()
        .map(|c| match c {
            'i' | 'I' => ColumnDataType::Int64,
            'f' | 'F' => ColumnDataType::Float64,
            's' | 'S' => ColumnDataType::String,
            other => unreachable!("Cannot parse {other} as a column data type"),
        })
        .cycle()
        .take(pattern.len() * repeat)
        .collect()
}

fn main() {
    // Sets the global logging to INFO and suppress loggings from rskafka other than ERROR and upper ones.
    std::env::set_var("UNITTEST_LOG_LEVEL", "info,rskafka=error");
    common_telemetry::init_default_ut_logging();

    let args = Args::parse();
    let cfg = if !args.cfg_file.is_empty() {
        toml::from_str(&fs::read_to_string(&args.cfg_file).unwrap()).unwrap()
    } else {
        Config::from(args)
    };

    // Validates arguments.
    if cfg.num_regions < cfg.num_workers {
        panic!("num_regions must be greater than or equal to num_workers");
    }
    if cfg
        .num_workers
        .min(cfg.num_topics)
        .min(cfg.num_regions)
        .min(cfg.num_scrapes)
        .min(cfg.max_batch_size.as_bytes() as u32)
        .min(cfg.bootstrap_brokers.len() as u32)
        == 0
    {
        panic!("Invalid arguments");
    }

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            match cfg.wal_provider {
                WalProvider::Kafka => {
                    let topics = create_topics(&cfg).await;
                    let kafka_cfg = KafkaConfig {
                        broker_endpoints: cfg.bootstrap_brokers.clone(),
                        max_batch_size: cfg.max_batch_size,
                        linger: cfg.linger,
                        backoff: BackoffConfig {
                            init: cfg.backoff_init,
                            max: cfg.backoff_max,
                            base: cfg.backoff_base,
                            deadline: Some(cfg.backoff_deadline),
                        },
                        compression: parse_compression(&cfg.compression),
                        ..Default::default()
                    };
                    let store = Arc::new(KafkaLogStore::try_new(&kafka_cfg).await.unwrap());
                    let wal = Arc::new(Wal::new(store));
                    run_benchmarker(&cfg, &topics, wal).await;
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
                    run_benchmarker(&cfg, &[], wal).await;
                }
            }
        });
}
