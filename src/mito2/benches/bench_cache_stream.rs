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

//! Benchmarks for `cache_flat_range_stream` overhead.
//!
//! Compares consuming batches from a plain stream vs through the caching wrapper
//! that clones batches for the range cache.
//!
//! Run with:
//! ```sh
//! cargo bench -p mito2 --features test --bench bench_cache_stream
//! ```

use std::collections::VecDeque;
use std::sync::Arc;

use mito2::test_util::bench_util::{CpuDataGenerator, cpu_metadata};
use criterion::{Criterion, criterion_group, criterion_main};
use futures::TryStreamExt;
use mito2::memtable::bulk::context::BulkIterContext;
use mito2::memtable::bulk::part::{BulkPartConverter, BulkPartEncoder};
use mito2::memtable::bulk::part_reader::EncodedBulkPartIter;
use mito2::read::range_cache::bench_cache_flat_range_stream;
use mito2::sst::parquet::DEFAULT_ROW_GROUP_SIZE;
use mito2::sst::{FlatSchemaOptions, to_flat_sst_arrow_schema};
use mito_codec::row_converter::DensePrimaryKeyCodec;

fn cache_flat_range_stream_bench(c: &mut Criterion) {
    let metadata = Arc::new(cpu_metadata());
    let region_id = metadata.region_id;
    let start_sec = 1710043200;
    // 2000 hosts × 51 steps = 102,000 rows ≈ DEFAULT_ROW_GROUP_SIZE
    let num_hosts = 2000;
    let end_sec = start_sec + 510;
    let generator = CpuDataGenerator::new(metadata.clone(), num_hosts, start_sec, end_sec);

    // Build a BulkPart from all the generated data
    let schema = to_flat_sst_arrow_schema(&metadata, &FlatSchemaOptions::default());
    let codec = Arc::new(DensePrimaryKeyCodec::new(&metadata));

    let mut converter = BulkPartConverter::new(
        &metadata,
        schema,
        DEFAULT_ROW_GROUP_SIZE,
        codec,
        true, // store_pk_columns
    );
    for kvs in generator.iter() {
        converter.append_key_values(&kvs).unwrap();
    }
    let bulk_part = converter.convert().unwrap();

    // Encode to parquet
    let encoder = BulkPartEncoder::new(metadata.clone(), DEFAULT_ROW_GROUP_SIZE).unwrap();
    let encoded_part = encoder.encode_part(&bulk_part).unwrap().unwrap();

    // Decode all record batches
    let num_row_groups = encoded_part.metadata().parquet_metadata.num_row_groups();
    let context = Arc::new(
        BulkIterContext::new(
            metadata.clone(),
            None, // No projection
            None, // No predicate
            false,
        )
        .unwrap(),
    );
    let row_groups: VecDeque<usize> = (0..num_row_groups).collect();

    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("cache_flat_range_stream");
    group.sample_size(10);

    group.bench_function("baseline_iter_stream", |b| {
        b.iter(|| {
            rt.block_on(async {
                let iter = EncodedBulkPartIter::try_new(
                    &encoded_part,
                    context.clone(),
                    row_groups.clone(),
                    None,
                    None,
                )
                .unwrap();
                let stream: mito2::read::BoxedRecordBatchStream =
                    Box::pin(futures::stream::iter(iter));
                let mut stream = stream;
                while let Some(_batch) = stream.try_next().await.unwrap() {}
            });
        });
    });

    group.bench_function("cache_flat_range_stream", |b| {
        b.iter(|| {
            rt.block_on(async {
                let iter = EncodedBulkPartIter::try_new(
                    &encoded_part,
                    context.clone(),
                    row_groups.clone(),
                    None,
                    None,
                )
                .unwrap();
                let stream: mito2::read::BoxedRecordBatchStream =
                    Box::pin(futures::stream::iter(iter));
                let mut stream =
                    bench_cache_flat_range_stream(stream, 64 * 1024 * 1024, region_id);
                while let Some(_batch) = stream.try_next().await.unwrap() {}
            });
        });
    });
}

criterion_group!(benches, cache_flat_range_stream_bench);
criterion_main!(benches);
