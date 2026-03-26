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

//! Benchmarks for memtable operations: writes, full scans, filtered scans,
//! bulk part conversion, record batch iteration with filters, and flat merge.
//!
//! Run with:
//! ```sh
//! cargo bench -p mito2 --features test --bench memtable_bench
//! ```

use std::sync::Arc;

use criterion::{Criterion, criterion_group, criterion_main};
use mito_codec::row_converter::DensePrimaryKeyCodec;
use mito2::memtable::bulk::context::BulkIterContext;
use mito2::memtable::bulk::part::BulkPartConverter;
use mito2::memtable::bulk::part_reader::BulkPartBatchIter;
use mito2::memtable::bulk::{BulkMemtable, BulkMemtableConfig};
use mito2::memtable::partition_tree::{PartitionTreeConfig, PartitionTreeMemtable};
use mito2::memtable::time_series::TimeSeriesMemtable;
use mito2::memtable::{IterBuilder, Memtable, RangesOptions};
use mito2::read::flat_merge::FlatMergeIterator;
use mito2::read::scan_region::PredicateGroup;
use mito2::region::options::MergeMode;
use mito2::sst::{FlatSchemaOptions, to_flat_sst_arrow_schema};
use mito2::test_util::bench_util::{CpuDataGenerator, cpu_metadata};
use mito2::test_util::memtable_util;

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
            let iter = memtable
                .ranges(None, RangesOptions::default())
                .unwrap()
                .build(None)
                .unwrap();
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
            let iter = memtable
                .ranges(
                    None,
                    RangesOptions {
                        predicate: PredicateGroup::new(&metadata, predicate.exprs()).unwrap(),
                        ..Default::default()
                    },
                )
                .unwrap()
                .build(None)
                .unwrap();
            for batch in iter {
                let _batch = batch.unwrap();
            }
        });
    });
    group.bench_function("bulk", |b| {
        // Create BulkMemtable
        let memtable = BulkMemtable::new(
            1,
            BulkMemtableConfig::default(),
            metadata.clone(),
            None,  // write_buffer_manager
            None,  // compact_dispatcher
            false, // append_mode
            MergeMode::LastRow,
        );

        // Write data using BulkPartConverter
        let schema = to_flat_sst_arrow_schema(&metadata, &FlatSchemaOptions::default());
        let codec = Arc::new(DensePrimaryKeyCodec::new(&metadata));

        for kvs in generator.iter() {
            let mut converter = BulkPartConverter::new(
                &metadata,
                schema.clone(),
                kvs.num_rows(),
                codec.clone(),
                true, // store_pk_columns
            );
            converter.append_key_values(&kvs).unwrap();
            let bulk_part = converter.convert().unwrap();
            memtable.write_bulk(bulk_part).unwrap();
        }

        // Create predicate for filtering
        let filter_exprs = generator.random_host_filter_exprs();
        let predicate = PredicateGroup::new(&metadata, &filter_exprs).unwrap();

        b.iter(|| {
            let ranges = memtable
                .ranges(
                    None, // No projection
                    RangesOptions::default().with_predicate(predicate.clone()),
                )
                .unwrap();

            for (_range_id, range) in ranges.ranges.iter() {
                let iter = range.build_record_batch_iter(None, None).unwrap();
                for batch in iter {
                    let _batch = batch.unwrap();
                }
            }
        });
    });
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
                        let iter = BulkPartBatchIter::from_single(
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
            // Create context for BulkPartBatchIter with predicate
            let context = Arc::new(
                BulkIterContext::new(
                    metadata.clone(),
                    None,                    // No projection
                    Some(predicate.clone()), // With hostname filter
                    false,
                )
                .unwrap(),
            );

            // Create and iterate over BulkPartBatchIter with filter
            let iter = BulkPartBatchIter::from_single(
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
            // Create context for BulkPartBatchIter without predicate
            let context = Arc::new(
                BulkIterContext::new(
                    metadata.clone(),
                    None, // No projection
                    None, // No predicate
                    false,
                )
                .unwrap(),
            );

            // Create and iterate over BulkPartBatchIter
            let iter = BulkPartBatchIter::from_single(
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
