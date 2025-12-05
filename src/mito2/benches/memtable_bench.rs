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

use std::sync::Arc;

use api::v1::value::ValueData;
use api::v1::{Row, Rows, SemanticType};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::Column;
use datafusion_expr::{Expr, lit};
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use mito_codec::row_converter::DensePrimaryKeyCodec;
use mito2::memtable::bulk::context::BulkIterContext;
use mito2::memtable::bulk::part::BulkPartConverter;
use mito2::memtable::bulk::part_reader::BulkPartRecordBatchIter;
use mito2::memtable::partition_tree::{PartitionTreeConfig, PartitionTreeMemtable};
use mito2::memtable::time_series::TimeSeriesMemtable;
use mito2::memtable::{KeyValues, Memtable};
use mito2::read::flat_merge::FlatMergeIterator;
use mito2::region::options::MergeMode;
use mito2::sst::{FlatSchemaOptions, to_flat_sst_arrow_schema};
use mito2::test_util::memtable_util::{self, region_metadata_to_row_schema};
use rand::Rng;
use rand::rngs::ThreadRng;
use rand::seq::IndexedRandom;
use store_api::metadata::{
    ColumnMetadata, RegionMetadata, RegionMetadataBuilder, RegionMetadataRef,
};
use store_api::storage::RegionId;
use table::predicate::Predicate;

/// Writes rows.
fn write_rows(c: &mut Criterion) {
    let metadata = Arc::new(memtable_util::metadata_with_primary_key(vec![1, 0], true));
    let timestamps = (0..100).collect::<Vec<_>>();

    // Note that this test only generate one time series.
    let mut group = c.benchmark_group("write");
    group.bench_function("partition_tree", |b| {
        let codec = Arc::new(DensePrimaryKeyCodec::new(&metadata));
        let memtable = PartitionTreeMemtable::new(
            1,
            codec,
            metadata.clone(),
            None,
            &PartitionTreeConfig::default(),
        );
        let kvs =
            memtable_util::build_key_values(&metadata, "hello".to_string(), 42, &timestamps, 1);
        b.iter(|| {
            memtable.write(&kvs).unwrap();
        });
    });
    group.bench_function("time_series", |b| {
        let memtable = TimeSeriesMemtable::new(metadata.clone(), 1, None, true, MergeMode::LastRow);
        let kvs =
            memtable_util::build_key_values(&metadata, "hello".to_string(), 42, &timestamps, 1);
        b.iter(|| {
            memtable.write(&kvs).unwrap();
        });
    });
}

/// Scans all rows.
fn full_scan(c: &mut Criterion) {
    let metadata = Arc::new(cpu_metadata());
    let config = PartitionTreeConfig::default();
    let start_sec = 1710043200;
    let generator = CpuDataGenerator::new(metadata.clone(), 4000, start_sec, start_sec + 3600 * 2);

    let mut group = c.benchmark_group("full_scan");
    group.sample_size(10);
    group.bench_function("partition_tree", |b| {
        let codec = Arc::new(DensePrimaryKeyCodec::new(&metadata));
        let memtable = PartitionTreeMemtable::new(1, codec, metadata.clone(), None, &config);
        for kvs in generator.iter() {
            memtable.write(&kvs).unwrap();
        }

        b.iter(|| {
            let iter = memtable.iter(None, None, None).unwrap();
            for batch in iter {
                let _batch = batch.unwrap();
            }
        });
    });
    group.bench_function("time_series", |b| {
        let memtable = TimeSeriesMemtable::new(metadata.clone(), 1, None, true, MergeMode::LastRow);
        for kvs in generator.iter() {
            memtable.write(&kvs).unwrap();
        }

        b.iter(|| {
            let iter = memtable.iter(None, None, None).unwrap();
            for batch in iter {
                let _batch = batch.unwrap();
            }
        });
    });
}

/// Filters 1 host.
fn filter_1_host(c: &mut Criterion) {
    let metadata = Arc::new(cpu_metadata());
    let config = PartitionTreeConfig::default();
    let start_sec = 1710043200;
    let generator = CpuDataGenerator::new(metadata.clone(), 4000, start_sec, start_sec + 3600 * 2);

    let mut group = c.benchmark_group("filter_1_host");
    group.sample_size(10);
    group.bench_function("partition_tree", |b| {
        let codec = Arc::new(DensePrimaryKeyCodec::new(&metadata));
        let memtable = PartitionTreeMemtable::new(1, codec, metadata.clone(), None, &config);
        for kvs in generator.iter() {
            memtable.write(&kvs).unwrap();
        }
        let predicate = generator.random_host_filter();

        b.iter(|| {
            let iter = memtable.iter(None, Some(predicate.clone()), None).unwrap();
            for batch in iter {
                let _batch = batch.unwrap();
            }
        });
    });
    group.bench_function("time_series", |b| {
        let memtable = TimeSeriesMemtable::new(metadata.clone(), 1, None, true, MergeMode::LastRow);
        for kvs in generator.iter() {
            memtable.write(&kvs).unwrap();
        }
        let predicate = generator.random_host_filter();

        b.iter(|| {
            let iter = memtable.iter(None, Some(predicate.clone()), None).unwrap();
            for batch in iter {
                let _batch = batch.unwrap();
            }
        });
    });
}

struct Host {
    hostname: String,
    region: String,
    datacenter: String,
    rack: String,
    os: String,
    arch: String,
    team: String,
    service: String,
    service_version: String,
    service_environment: String,
}

impl Host {
    fn random_with_id(id: usize) -> Host {
        let mut rng = rand::rng();
        let region = format!("ap-southeast-{}", rng.random_range(0..10));
        let datacenter = format!(
            "{}{}",
            region,
            ['a', 'b', 'c', 'd', 'e'].choose(&mut rng).unwrap()
        );
        Host {
            hostname: format!("host_{id}"),
            region,
            datacenter,
            rack: rng.random_range(0..100).to_string(),
            os: "Ubuntu16.04LTS".to_string(),
            arch: "x86".to_string(),
            team: "CHI".to_string(),
            service: rng.random_range(0..100).to_string(),
            service_version: rng.random_range(0..10).to_string(),
            service_environment: "test".to_string(),
        }
    }

    fn fill_values(&self, values: &mut Vec<api::v1::Value>) {
        let tags = [
            api::v1::Value {
                value_data: Some(ValueData::StringValue(self.hostname.clone())),
            },
            api::v1::Value {
                value_data: Some(ValueData::StringValue(self.region.clone())),
            },
            api::v1::Value {
                value_data: Some(ValueData::StringValue(self.datacenter.clone())),
            },
            api::v1::Value {
                value_data: Some(ValueData::StringValue(self.rack.clone())),
            },
            api::v1::Value {
                value_data: Some(ValueData::StringValue(self.os.clone())),
            },
            api::v1::Value {
                value_data: Some(ValueData::StringValue(self.arch.clone())),
            },
            api::v1::Value {
                value_data: Some(ValueData::StringValue(self.team.clone())),
            },
            api::v1::Value {
                value_data: Some(ValueData::StringValue(self.service.clone())),
            },
            api::v1::Value {
                value_data: Some(ValueData::StringValue(self.service_version.clone())),
            },
            api::v1::Value {
                value_data: Some(ValueData::StringValue(self.service_environment.clone())),
            },
        ];
        for tag in tags {
            values.push(tag);
        }
    }
}

struct CpuDataGenerator {
    metadata: RegionMetadataRef,
    column_schemas: Vec<api::v1::ColumnSchema>,
    hosts: Vec<Host>,
    start_sec: i64,
    end_sec: i64,
}

impl CpuDataGenerator {
    fn new(metadata: RegionMetadataRef, num_hosts: usize, start_sec: i64, end_sec: i64) -> Self {
        let column_schemas = region_metadata_to_row_schema(&metadata);
        Self {
            metadata,
            column_schemas,
            hosts: Self::generate_hosts(num_hosts),
            start_sec,
            end_sec,
        }
    }

    fn iter(&self) -> impl Iterator<Item = KeyValues> + '_ {
        // point per 10s.
        (self.start_sec..self.end_sec)
            .step_by(10)
            .enumerate()
            .map(|(seq, ts)| self.build_key_values(seq, ts))
    }

    fn build_key_values(&self, seq: usize, current_sec: i64) -> KeyValues {
        let rows = self
            .hosts
            .iter()
            .map(|host| {
                let mut rng = rand::rng();
                let mut values = Vec::with_capacity(21);
                values.push(api::v1::Value {
                    value_data: Some(ValueData::TimestampMillisecondValue(current_sec * 1000)),
                });
                host.fill_values(&mut values);
                for _ in 0..10 {
                    values.push(api::v1::Value {
                        value_data: Some(ValueData::F64Value(Self::random_f64(&mut rng))),
                    });
                }
                Row { values }
            })
            .collect();
        let mutation = api::v1::Mutation {
            op_type: api::v1::OpType::Put as i32,
            sequence: seq as u64,
            rows: Some(Rows {
                schema: self.column_schemas.clone(),
                rows,
            }),
            write_hint: None,
        };

        KeyValues::new(&self.metadata, mutation).unwrap()
    }

    fn random_host_filter(&self) -> Predicate {
        let host = self.random_hostname();
        let expr = Expr::Column(Column::from_name("hostname")).eq(lit(host));
        Predicate::new(vec![expr])
    }

    fn random_hostname(&self) -> String {
        let mut rng = rand::rng();
        self.hosts.choose(&mut rng).unwrap().hostname.clone()
    }

    fn random_f64(rng: &mut ThreadRng) -> f64 {
        let base: u32 = rng.random_range(30..95);
        base as f64
    }

    fn generate_hosts(num_hosts: usize) -> Vec<Host> {
        (0..num_hosts).map(Host::random_with_id).collect()
    }
}

/// Creates a metadata for TSBS cpu-like table.
fn cpu_metadata() -> RegionMetadata {
    let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
    builder.push_column_metadata(ColumnMetadata {
        column_schema: ColumnSchema::new(
            "ts",
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        ),
        semantic_type: SemanticType::Timestamp,
        column_id: 0,
    });
    let mut column_id = 1;
    let tags = [
        "hostname",
        "region",
        "datacenter",
        "rack",
        "os",
        "arch",
        "team",
        "service",
        "service_version",
        "service_environment",
    ];
    for tag in tags {
        builder.push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(tag, ConcreteDataType::string_datatype(), true),
            semantic_type: SemanticType::Tag,
            column_id,
        });
        column_id += 1;
    }
    let fields = [
        "usage_user",
        "usage_system",
        "usage_idle",
        "usage_nice",
        "usage_iowait",
        "usage_irq",
        "usage_softirq",
        "usage_steal",
        "usage_guest",
        "usage_guest_nice",
    ];
    for field in fields {
        builder.push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(field, ConcreteDataType::float64_datatype(), true),
            semantic_type: SemanticType::Field,
            column_id,
        });
        column_id += 1;
    }
    builder.primary_key(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    builder.build().unwrap()
}

fn bulk_part_converter(c: &mut Criterion) {
    let metadata = Arc::new(cpu_metadata());
    let start_sec = 1710043200;

    let mut group = c.benchmark_group("bulk_part_converter");

    for &rows in &[1024, 2048, 4096, 8192] {
        // Benchmark without storing primary key columns (baseline)
        group.bench_with_input(format!("{}_rows_no_pk_columns", rows), &rows, |b, &rows| {
            b.iter(|| {
                let generator =
                    CpuDataGenerator::new(metadata.clone(), rows, start_sec, start_sec + 1);
                let codec = Arc::new(DensePrimaryKeyCodec::new(&metadata));
                let schema = to_flat_sst_arrow_schema(
                    &metadata,
                    &FlatSchemaOptions {
                        raw_pk_columns: false,
                        string_pk_use_dict: false,
                    },
                );
                let mut converter = BulkPartConverter::new(&metadata, schema, rows, codec, false);

                if let Some(kvs) = generator.iter().next() {
                    converter.append_key_values(&kvs).unwrap();
                }

                let _bulk_part = converter.convert().unwrap();
            });
        });

        // Benchmark with storing primary key columns
        group.bench_with_input(
            format!("{}_rows_with_pk_columns", rows),
            &rows,
            |b, &rows| {
                b.iter(|| {
                    let generator =
                        CpuDataGenerator::new(metadata.clone(), rows, start_sec, start_sec + 1);
                    let codec = Arc::new(DensePrimaryKeyCodec::new(&metadata));
                    let schema = to_flat_sst_arrow_schema(
                        &metadata,
                        &FlatSchemaOptions {
                            raw_pk_columns: true,
                            string_pk_use_dict: true,
                        },
                    );
                    let mut converter =
                        BulkPartConverter::new(&metadata, schema, rows, codec, true);

                    if let Some(kvs) = generator.iter().next() {
                        converter.append_key_values(&kvs).unwrap();
                    }

                    let _bulk_part = converter.convert().unwrap();
                });
            },
        );
    }
}

fn flat_merge_iterator_bench(c: &mut Criterion) {
    let metadata = Arc::new(cpu_metadata());
    let schema = to_flat_sst_arrow_schema(&metadata, &FlatSchemaOptions::default());
    let start_sec = 1710043200;

    let mut group = c.benchmark_group("flat_merge_iterator");
    group.sample_size(10);

    for &num_parts in &[8, 16, 32, 64, 128, 256, 512] {
        // Pre-create BulkParts with different timestamps but same hosts (1024)
        let mut bulk_parts = Vec::with_capacity(num_parts);
        let codec = Arc::new(DensePrimaryKeyCodec::new(&metadata));

        for part_idx in 0..num_parts {
            let generator = CpuDataGenerator::new(
                metadata.clone(),
                1024,                             // 1024 hosts per part
                start_sec + part_idx as i64 * 10, // Different timestamps for each part
                start_sec + part_idx as i64 * 10 + 1,
            );

            let mut converter =
                BulkPartConverter::new(&metadata, schema.clone(), 1024, codec.clone(), true);
            if let Some(kvs) = generator.iter().next() {
                converter.append_key_values(&kvs).unwrap();
            }
            let bulk_part = converter.convert().unwrap();
            bulk_parts.push(bulk_part);
        }

        // Pre-create BulkIterContext
        let context = Arc::new(
            BulkIterContext::new(
                metadata.clone(),
                None, // No projection
                None, // No predicate
                false,
            )
            .unwrap(),
        );

        group.bench_with_input(
            format!("{}_parts_1024_hosts", num_parts),
            &num_parts,
            |b, _| {
                b.iter(|| {
                    // Create iterators from BulkParts
                    let mut iters = Vec::with_capacity(num_parts);
                    for bulk_part in &bulk_parts {
                        let iter = BulkPartRecordBatchIter::new(
                            bulk_part.batch.clone(),
                            context.clone(),
                            None, // No sequence filter
                            1024, // 1024 hosts per part
                            None, // No mem_scan_metrics
                        );
                        iters.push(Box::new(iter) as _);
                    }

                    // Create and consume FlatMergeIterator
                    let merge_iter = FlatMergeIterator::new(schema.clone(), iters, 1024).unwrap();
                    for batch_result in merge_iter {
                        let _batch = batch_result.unwrap();
                    }
                });
            },
        );
    }
}

fn bulk_part_record_batch_iter_filter(c: &mut Criterion) {
    let metadata = Arc::new(cpu_metadata());
    let schema = to_flat_sst_arrow_schema(&metadata, &FlatSchemaOptions::default());
    let start_sec = 1710043200;

    let mut group = c.benchmark_group("bulk_part_record_batch_iter_filter");

    // Pre-create RecordBatch and primary key arrays
    let (record_batch_with_filter, record_batch_no_filter) = {
        let generator = CpuDataGenerator::new(metadata.clone(), 4096, start_sec, start_sec + 1);
        let codec = Arc::new(DensePrimaryKeyCodec::new(&metadata));
        let mut converter = BulkPartConverter::new(&metadata, schema, 4096, codec, true);

        if let Some(kvs) = generator.iter().next() {
            converter.append_key_values(&kvs).unwrap();
        }

        let bulk_part = converter.convert().unwrap();
        let record_batch = bulk_part.batch;

        (record_batch.clone(), record_batch)
    };

    // Pre-create predicate
    let generator = CpuDataGenerator::new(metadata.clone(), 4096, start_sec, start_sec + 1);
    let predicate = generator.random_host_filter();

    // Benchmark with hostname filter using non-encoded primary keys
    group.bench_function("4096_rows_with_hostname_filter", |b| {
        b.iter(|| {
            // Create context for BulkPartRecordBatchIter with predicate
            let context = Arc::new(
                BulkIterContext::new(
                    metadata.clone(),
                    None,                    // No projection
                    Some(predicate.clone()), // With hostname filter
                    false,
                )
                .unwrap(),
            );

            // Create and iterate over BulkPartRecordBatchIter with filter
            let iter = BulkPartRecordBatchIter::new(
                record_batch_with_filter.clone(),
                context,
                None, // No sequence filter
                4096, // 4096 hosts
                None, // No mem_scan_metrics
            );

            // Consume all batches
            for batch_result in iter {
                let _batch = batch_result.unwrap();
            }
        });
    });

    // Benchmark without filter for comparison
    group.bench_function("4096_rows_no_filter", |b| {
        b.iter(|| {
            // Create context for BulkPartRecordBatchIter without predicate
            let context = Arc::new(
                BulkIterContext::new(
                    metadata.clone(),
                    None, // No projection
                    None, // No predicate
                    false,
                )
                .unwrap(),
            );

            // Create and iterate over BulkPartRecordBatchIter
            let iter = BulkPartRecordBatchIter::new(
                record_batch_no_filter.clone(),
                context,
                None, // No sequence filter
                4096, // 4096 hosts
                None, // No mem_scan_metrics
            );

            // Consume all batches
            for batch_result in iter {
                let _batch = batch_result.unwrap();
            }
        });
    });
}

criterion_group!(
    benches,
    write_rows,
    full_scan,
    filter_1_host,
    bulk_part_converter,
    bulk_part_record_batch_iter_filter,
    flat_merge_iterator_bench
);
criterion_main!(benches);
