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
use api::v1::{Mutation, OpType, Row, Rows, SemanticType};
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use mito2::memtable::simple_bulk_memtable::SimpleBulkMemtable;
use mito2::memtable::{KeyValues, Memtable, MemtableRanges, RangesOptions};
use mito2::read;
use mito2::read::Source;
use mito2::read::dedup::DedupReader;
use mito2::read::merge::MergeReaderBuilder;
use mito2::region::options::MergeMode;
use mito2::test_util::column_metadata_to_column_schema;
use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
use store_api::storage::SequenceNumber;

fn new_test_metadata() -> Arc<store_api::metadata::RegionMetadata> {
    let mut builder = RegionMetadataBuilder::new(1.into());
    builder
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            semantic_type: SemanticType::Timestamp,
            column_id: 1,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new("f1", ConcreteDataType::float64_datatype(), true),
            semantic_type: SemanticType::Field,
            column_id: 2,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new("f2", ConcreteDataType::string_datatype(), true),
            semantic_type: SemanticType::Field,
            column_id: 3,
        });
    Arc::new(builder.build().unwrap())
}

fn new_test_memtable(dedup: bool, merge_mode: MergeMode) -> SimpleBulkMemtable {
    SimpleBulkMemtable::new(1, new_test_metadata(), None, dedup, merge_mode)
}

fn build_key_values(
    metadata: &Arc<store_api::metadata::RegionMetadata>,
    sequence: SequenceNumber,
    row_values: &[(i64, f64, String)],
) -> KeyValues {
    let column_schemas: Vec<_> = metadata
        .column_metadatas
        .iter()
        .map(column_metadata_to_column_schema)
        .collect();

    let rows: Vec<_> = row_values
        .iter()
        .map(|(ts, f1, f2)| Row {
            values: vec![
                api::v1::Value {
                    value_data: Some(ValueData::TimestampMillisecondValue(*ts)),
                },
                api::v1::Value {
                    value_data: Some(ValueData::F64Value(*f1)),
                },
                api::v1::Value {
                    value_data: Some(ValueData::StringValue(f2.clone())),
                },
            ],
        })
        .collect();
    let mutation = Mutation {
        op_type: OpType::Put as i32,
        sequence,
        rows: Some(Rows {
            schema: column_schemas,
            rows,
        }),
        write_hint: None,
    };
    KeyValues::new(metadata, mutation).unwrap()
}

fn create_memtable_with_rows(num_batches: usize) -> SimpleBulkMemtable {
    let memtable = new_test_memtable(false, MergeMode::LastRow);
    let batch_size = 10000;
    for i in 0..num_batches {
        let data: Vec<_> = (0..batch_size)
            .map(|j| {
                (
                    (i * batch_size + j) as i64,
                    (i * batch_size + j) as f64,
                    format!("value_{}", i * batch_size + j),
                )
            })
            .collect();

        memtable
            .write(&build_key_values(
                &memtable.region_metadata(),
                i as u64,
                &data,
            ))
            .unwrap();
    }

    memtable
}

async fn flush(mem: &SimpleBulkMemtable) {
    let MemtableRanges { ranges, .. } = mem.ranges(None, RangesOptions::for_flush()).unwrap();

    let mut source = if ranges.len() == 1 {
        let only_range = ranges.into_values().next().unwrap();
        let iter = only_range.build_iter().unwrap();
        Source::Iter(iter)
    } else {
        let sources = ranges
            .into_values()
            .map(|r| r.build_iter().map(Source::Iter))
            .collect::<mito2::error::Result<Vec<_>>>()
            .unwrap();
        let merge_reader = MergeReaderBuilder::from_sources(sources)
            .build()
            .await
            .unwrap();
        let reader = Box::new(DedupReader::new(
            merge_reader,
            read::dedup::LastRow::new(true),
            None,
        ));
        Source::Reader(reader)
    };

    while let Some(b) = source.next_batch().await.unwrap() {
        black_box(b);
    }
}

async fn flush_original(mem: &SimpleBulkMemtable) {
    let iter = mem.iter(None, None, None).unwrap();
    for b in iter {
        black_box(b.unwrap());
    }
}

fn bench_ranges_parallel_vs_sequential(c: &mut Criterion) {
    use criterion::BenchmarkId;

    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("ranges_parallel_vs_sequential");

    // Test different row counts: (num_series, rows_per_series)
    let test_cases = vec![1, 10, 50, 100, 500, 1000];

    for num_batch in test_cases {
        let total_rows_k = num_batch * 10;
        let memtable = create_memtable_with_rows(num_batch);

        group.bench_with_input(
            BenchmarkId::new("flush_by_merge_reader", format!("{}k_rows", total_rows_k)),
            &memtable,
            |b, memtable| b.to_async(&rt).iter(|| async { flush(memtable).await }),
        );

        group.bench_with_input(
            BenchmarkId::new("flush_by_iter", format!("{}k_rows", total_rows_k)),
            &memtable,
            |b, memtable| {
                b.to_async(&rt)
                    .iter(|| async { flush_original(memtable).await })
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_ranges_parallel_vs_sequential,);
criterion_main!(benches);
