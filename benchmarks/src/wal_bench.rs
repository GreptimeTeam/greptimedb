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

use std::mem::size_of;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use api::v1::value::ValueData;
use api::v1::{ColumnDataType, ColumnSchema, Mutation, OpType, Row, Rows, Value, WalEntry};
use clap::{Parser, ValueEnum};
use common_base::readable_size::ReadableSize;
use common_wal::options::WalOptions;
use futures::StreamExt;
use mito2::wal::{Wal, WalWriter};
use rand::distributions::{Alphanumeric, DistString, Uniform};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use store_api::logstore::namespace::Namespace;
use store_api::logstore::LogStore;
use store_api::storage::RegionId;

use crate::metrics;

/// The wal provider.
#[derive(Clone, ValueEnum, Default, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WalProvider {
    #[default]
    RaftEngine,
    Kafka,
}

#[derive(Parser)]
pub struct Args {
    /// The provided configuration file.
    /// The example configuration file can be found at `greptimedb/benchmarks/config/wal_bench.example.toml`.
    #[clap(long, short = 'c')]
    pub cfg_file: String,

    /// The wal provider.
    #[clap(long, value_enum, default_value_t = WalProvider::default())]
    pub wal_provider: WalProvider,

    /// The advertised addresses of the kafka brokers.
    /// If there're multiple bootstrap brokers, their addresses should be separated by comma, for e.g. "localhost:9092,localhost:9093".
    #[clap(long, short = 'b', default_value = "localhost:9092")]
    pub bootstrap_brokers: String,

    /// The number of workers each running in a dedicated thread.
    #[clap(long, default_value_t = num_cpus::get() as u32)]
    pub num_workers: u32,

    /// The number of kafka topics to be created.
    #[clap(long, default_value_t = 32)]
    pub num_topics: u32,

    /// The number of regions.
    #[clap(long, default_value_t = 1000)]
    pub num_regions: u32,

    /// The number of times each region is scraped.
    #[clap(long, default_value_t = 1000)]
    pub num_scrapes: u32,

    /// The number of rows in each wal entry.
    /// Each time a region is scraped, a wal entry containing will be produced.
    #[clap(long, default_value_t = 5)]
    pub num_rows: u32,

    /// The column types of the schema for each region.
    /// Currently, three column types are supported:
    /// - i = ColumnDataType::Int64
    /// - f = ColumnDataType::Float64
    /// - s = ColumnDataType::String  
    /// For e.g., "ifs" will be parsed as three columns: i64, f64, and string.
    ///
    /// Additionally, a "x" sign can be provided to repeat the column types for a given number of times.
    /// For e.g., "iix2" will be parsed as 4 columns: i64, i64, i64, and i64.
    /// This feature is useful if you want to specify many columns.
    #[clap(long, default_value = "ifs")]
    pub col_types: String,

    /// The maximum size of a batch of kafka records.
    /// The default value is 1mb.
    #[clap(long, default_value = "512KB")]
    pub max_batch_size: ReadableSize,

    /// The minimum latency the kafka client issues a batch of kafka records.
    /// However, a batch of kafka records would be immediately issued if a record cannot be fit into the batch.
    #[clap(long, default_value = "1ms")]
    pub linger: String,

    /// The initial backoff delay of the kafka consumer.
    #[clap(long, default_value = "10ms")]
    pub backoff_init: String,

    /// The maximum backoff delay of the kafka consumer.
    #[clap(long, default_value = "1s")]
    pub backoff_max: String,

    /// The exponential backoff rate of the kafka consumer. The next back off = base * the current backoff.
    #[clap(long, default_value_t = 2)]
    pub backoff_base: u32,

    /// The deadline of backoff. The backoff ends if the total backoff delay reaches the deadline.
    #[clap(long, default_value = "3s")]
    pub backoff_deadline: String,

    /// The client-side compression algorithm for kafka records.
    #[clap(long, default_value = "zstd")]
    pub compression: String,

    /// The seed of random number generators.
    #[clap(long, default_value_t = 42)]
    pub rng_seed: u64,

    /// Skips the read phase, aka. region replay, if set to true.
    #[clap(long, default_value_t = false)]
    pub skip_read: bool,

    /// Skips the write phase if set to true.
    #[clap(long, default_value_t = false)]
    pub skip_write: bool,

    /// Randomly generates topic names if set to true.
    /// Useful when you want to run the benchmarker without worrying about the topics created before.
    #[clap(long, default_value_t = false)]
    pub random_topics: bool,

    /// Logs out the gathered prometheus metrics when the benchmarker ends.
    #[clap(long, default_value_t = false)]
    pub report_metrics: bool,
}

/// Benchmarker config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub wal_provider: WalProvider,
    pub bootstrap_brokers: Vec<String>,
    pub num_workers: u32,
    pub num_topics: u32,
    pub num_regions: u32,
    pub num_scrapes: u32,
    pub num_rows: u32,
    pub col_types: String,
    pub max_batch_size: ReadableSize,
    #[serde(with = "humantime_serde")]
    pub linger: Duration,
    #[serde(with = "humantime_serde")]
    pub backoff_init: Duration,
    #[serde(with = "humantime_serde")]
    pub backoff_max: Duration,
    pub backoff_base: u32,
    #[serde(with = "humantime_serde")]
    pub backoff_deadline: Duration,
    pub compression: String,
    pub rng_seed: u64,
    pub skip_read: bool,
    pub skip_write: bool,
    pub random_topics: bool,
    pub report_metrics: bool,
}

impl From<Args> for Config {
    fn from(args: Args) -> Self {
        let cfg = Self {
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
            col_types: args.col_types,
            max_batch_size: args.max_batch_size,
            linger: humantime::parse_duration(&args.linger).unwrap(),
            backoff_init: humantime::parse_duration(&args.backoff_init).unwrap(),
            backoff_max: humantime::parse_duration(&args.backoff_max).unwrap(),
            backoff_base: args.backoff_base,
            backoff_deadline: humantime::parse_duration(&args.backoff_deadline).unwrap(),
            compression: args.compression,
            rng_seed: args.rng_seed,
            skip_read: args.skip_read,
            skip_write: args.skip_write,
            random_topics: args.random_topics,
            report_metrics: args.report_metrics,
        };

        cfg
    }
}

/// The region used for wal benchmarker.
pub struct Region {
    id: RegionId,
    schema: Vec<ColumnSchema>,
    namespace: Namespace,
    next_sequence: AtomicU64,
    next_entry_id: AtomicU64,
    next_timestamp: AtomicI64,
    rng: Mutex<Option<SmallRng>>,
    num_rows: u32,
}

impl Region {
    /// Creates a new region.
    pub fn new(
        id: RegionId,
        schema: Vec<ColumnSchema>,
        wal_options: WalOptions,
        num_rows: u32,
        rng_seed: u64,
    ) -> Self {
        let namespace = match wal_options {
            WalOptions::RaftEngine => Namespace::raft_engine_namespace(*id),
            WalOptions::Kafka(opts) => Namespace::kafka_namespace(opts.topic),
        };
        Self {
            id,
            schema,
            namespace,
            next_sequence: AtomicU64::new(1),
            next_entry_id: AtomicU64::new(1),
            next_timestamp: AtomicI64::new(1655276557000),
            rng: Mutex::new(Some(SmallRng::seed_from_u64(rng_seed))),
            num_rows,
        }
    }

    /// Scrapes the region and adds the generated entry to wal.
    pub fn add_wal_entry<S: LogStore>(&self, wal_writer: &mut WalWriter<S>) {
        let mutation = Mutation {
            op_type: OpType::Put as i32,
            sequence: self
                .next_sequence
                .fetch_add(self.num_rows as u64, Ordering::Relaxed),
            rows: Some(self.build_rows()),
        };
        let entry = WalEntry {
            mutations: vec![mutation],
        };
        metrics::METRIC_WAL_WRITE_BYTES_TOTAL.inc_by(Self::entry_estimated_size(&entry) as u64);

        wal_writer
            .add_entry(
                self.id,
                self.next_entry_id.fetch_add(1, Ordering::Relaxed),
                &entry,
                &self.namespace,
            )
            .unwrap();
    }

    /// Replays the region.
    pub async fn replay<S: LogStore>(&self, wal: &Arc<Wal<S>>) {
        let mut wal_stream = wal.scan(self.id, 0, &self.namespace).unwrap();
        while let Some(res) = wal_stream.next().await {
            let (_, entry) = res.unwrap();
            metrics::METRIC_WAL_READ_BYTES_TOTAL.inc_by(Self::entry_estimated_size(&entry) as u64);
        }
    }

    /// Computes the estimated size in bytes of the entry.
    pub fn entry_estimated_size(entry: &WalEntry) -> usize {
        let wrapper_size = size_of::<WalEntry>()
            + entry.mutations.capacity() * size_of::<Mutation>()
            + size_of::<Rows>();

        let rows = entry.mutations[0].rows.as_ref().unwrap();

        let schema_size = rows.schema.capacity() * size_of::<ColumnSchema>()
            + rows
                .schema
                .iter()
                .map(|s| s.column_name.capacity())
                .sum::<usize>();
        let values_size = (rows.rows.capacity() * size_of::<Row>())
            + rows
                .rows
                .iter()
                .map(|r| r.values.capacity() * size_of::<Value>())
                .sum::<usize>();

        wrapper_size + schema_size + values_size
    }

    fn build_rows(&self) -> Rows {
        let cols = self
            .schema
            .iter()
            .map(|col_schema| {
                let col_data_type = ColumnDataType::try_from(col_schema.datatype).unwrap();
                self.build_col(&col_data_type, self.num_rows)
            })
            .collect::<Vec<_>>();

        let rows = (0..self.num_rows)
            .map(|i| {
                let values = cols.iter().map(|col| col[i as usize].clone()).collect();
                Row { values }
            })
            .collect();

        Rows {
            schema: self.schema.clone(),
            rows,
        }
    }

    fn build_col(&self, col_data_type: &ColumnDataType, num_rows: u32) -> Vec<Value> {
        let mut rng_guard = self.rng.lock().unwrap();
        let rng = rng_guard.as_mut().unwrap();
        match col_data_type {
            ColumnDataType::TimestampMillisecond => (0..num_rows)
                .map(|_| {
                    let ts = self.next_timestamp.fetch_add(1000, Ordering::Relaxed);
                    Value {
                        value_data: Some(ValueData::TimestampMillisecondValue(ts)),
                    }
                })
                .collect(),
            ColumnDataType::Int64 => (0..num_rows)
                .map(|_| {
                    let v = rng.sample(Uniform::new(0, 10_000));
                    Value {
                        value_data: Some(ValueData::I64Value(v)),
                    }
                })
                .collect(),
            ColumnDataType::Float64 => (0..num_rows)
                .map(|_| {
                    let v = rng.sample(Uniform::new(0.0, 5000.0));
                    Value {
                        value_data: Some(ValueData::F64Value(v)),
                    }
                })
                .collect(),
            ColumnDataType::String => (0..num_rows)
                .map(|_| {
                    let v = Alphanumeric.sample_string(rng, 10);
                    Value {
                        value_data: Some(ValueData::StringValue(v)),
                    }
                })
                .collect(),
            _ => unreachable!(),
        }
    }
}
