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

use common_recordbatch::DfRecordBatch;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use datatypes::arrow::array::{
    ArrayRef, BinaryDictionaryBuilder, Int64Array, TimestampMillisecondArray, UInt8Array,
    UInt64Array,
};
use datatypes::arrow::compute::{SortColumn, SortOptions};
use datatypes::arrow::datatypes::{DataType, Field, Schema, TimeUnit, UInt32Type};
use mito_codec::row_converter::DensePrimaryKeyCodec;
use mito2::memtable::bulk::context::BulkIterContext;
use mito2::memtable::bulk::part::{BulkPartConverter, sort_primary_key_record_batch};
use mito2::memtable::bulk::part_reader::BulkPartBatchIter;
use mito2::memtable::bulk::{BulkMemtable, BulkMemtableConfig};
use mito2::memtable::time_series::TimeSeriesMemtable;
use mito2::memtable::{IterBuilder, Memtable, RangesOptions};
use mito2::read::flat_merge::FlatMergeIterator;
use mito2::read::scan_region::PredicateGroup;
use mito2::region::options::MergeMode;
use mito2::sst::{FlatSchemaOptions, to_flat_sst_arrow_schema};
use mito2::test_util::bench_util::{CpuDataGenerator, cpu_metadata};
use mito2::test_util::memtable_util;

const DEFAULT_BATCH_SIZE: usize = 8 * 1024;

fn primary_key_sort_batch(
    series_count: usize,
    samples_per_series: usize,
    sorted_within_series: bool,
) -> DfRecordBatch {
    let num_rows = series_count * samples_per_series;
    let primary_keys = (0..series_count)
        .map(|series| format!("series_{series:08}").into_bytes())
        .collect::<Vec<_>>();
    let mut primary_key_builder = BinaryDictionaryBuilder::<UInt32Type>::new();
    let mut timestamps = Vec::with_capacity(num_rows);
    let mut sequences = Vec::with_capacity(num_rows);

    for series_order in 0..series_count {
        // 17 is coprime to the power-of-two cardinalities used below and gives a deterministic
        // non-lexical series order.
        let series = series_order * 17 % series_count;
        for sample_order in 0..samples_per_series {
            let sample = if sorted_within_series {
                sample_order
            } else {
                sample_order * 37 % samples_per_series
            };
            primary_key_builder
                .append(primary_keys[series].as_slice())
                .unwrap();
            timestamps.push(sample as i64);
            sequences.push((series * samples_per_series + sample_order) as u64);
        }
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(0..num_rows as i64)),
        Arc::new(TimestampMillisecondArray::from(timestamps)),
        Arc::new(primary_key_builder.finish()),
        Arc::new(UInt64Array::from(sequences)),
        Arc::new(UInt8Array::from_value(1, num_rows)),
    ];
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Int64, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new(
            "__primary_key",
            DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Binary)),
            false,
        ),
        Field::new("__sequence", DataType::UInt64, false),
        Field::new("__op_type", DataType::UInt8, false),
    ]));
    DfRecordBatch::try_new(schema, columns).unwrap()
}

fn lexsort_primary_key_record_batch(batch: &DfRecordBatch) -> DfRecordBatch {
    let total_columns = batch.num_columns();
    let sort_columns = vec![
        SortColumn {
            values: batch.column(total_columns - 3).clone(),
            options: Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
        },
        SortColumn {
            values: batch.column(total_columns - 4).clone(),
            options: Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
        },
        SortColumn {
            values: batch.column(total_columns - 2).clone(),
            options: Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
        },
    ];
    let indices = datatypes::arrow::compute::lexsort_to_indices(&sort_columns, None).unwrap();
    datatypes::arrow::compute::take_record_batch(batch, &indices).unwrap()
}

fn primary_key_sort(c: &mut Criterion) {
    let mut group = c.benchmark_group("primary_key_sort");
    group.sample_size(20);

    for (name, series_count, samples_per_series, sorted_within_series) in [
        ("sorted_runs", 512, 16, true),
        ("shuffled_runs", 512, 16, false),
        ("high_cardinality", 8192, 1, true),
    ] {
        let batch = primary_key_sort_batch(series_count, samples_per_series, sorted_within_series);
        group.throughput(Throughput::Elements(batch.num_rows() as u64));
        group.bench_function(format!("{name}/arrow_lexsort"), |b| {
            b.iter(|| lexsort_primary_key_record_batch(&batch));
        });
        group.bench_function(format!("{name}/rank_scatter"), |b| {
            b.iter(|| sort_primary_key_record_batch(&batch).unwrap());
        });
    }
}

/// Writes rows.
fn write_rows(c: &mut Criterion) {
    let metadata = Arc::new(memtable_util::metadata_with_primary_key(vec![1, 0], true));
    let timestamps = (0..100).collect::<Vec<_>>();

    // Note that this test only generate one time series.
    let mut group = c.benchmark_group("write");
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
    let start_sec = 1710043200;
    let generator = CpuDataGenerator::new(metadata.clone(), 4000, start_sec, start_sec + 3600 * 2);

    let mut group = c.benchmark_group("full_scan");
    group.sample_size(10);
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
    let start_sec = 1710043200;
    let generator = CpuDataGenerator::new(metadata.clone(), 4000, start_sec, start_sec + 3600 * 2);

    let mut group = c.benchmark_group("filter_1_host");
    group.sample_size(10);
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
                        ..Default::default()
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
                            ..Default::default()
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
                DEFAULT_BATCH_SIZE,
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
                    DEFAULT_BATCH_SIZE,
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
                    DEFAULT_BATCH_SIZE,
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
    primary_key_sort,
    write_rows,
    full_scan,
    filter_1_host,
    bulk_part_converter,
    bulk_part_record_batch_iter_filter,
    flat_merge_iterator_bench
);
criterion_main!(benches);
