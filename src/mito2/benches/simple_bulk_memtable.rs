use std::sync::Arc;

use api::v1::value::ValueData;
use api::v1::{Mutation, OpType, Row, Rows, SemanticType};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use mito2::memtable::simple_bulk_memtable::SimpleBulkMemtable;
use mito2::memtable::{KeyValues, Memtable};
use mito2::read::scan_region::PredicateGroup;
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

fn create_large_memtable() -> SimpleBulkMemtable {
    let memtable = new_test_memtable(false, MergeMode::LastRow);

    // Create many series with different data to simulate real workload
    for i in 0..1000 {
        let data: Vec<_> = (0..100)
            .map(|j| {
                (
                    (i * 100 + j) as i64,
                    (i * 100 + j) as f64,
                    format!("value_{}", i * 100 + j),
                )
            })
            .collect();

        memtable
            .write(&build_key_values(&memtable.region_metadata(), i, &data))
            .unwrap();
    }

    memtable
}

fn bench_ranges_parallel_vs_sequential(c: &mut Criterion) {
    let memtable = create_large_memtable();
    let predicate = PredicateGroup::default();

    let mut group = c.benchmark_group("ranges_parallel_vs_sequential");

    group.bench_function("parallel", |b| {
        b.iter(|| black_box(memtable.ranges(None, predicate.clone(), None).unwrap()))
    });

    group.bench_function("sequential", |b| {
        b.iter(|| {
            black_box(
                memtable
                    .ranges_sequential(None, predicate.clone(), None)
                    .unwrap(),
            )
        })
    });

    group.finish();
}

fn bench_ranges_with_projection(c: &mut Criterion) {
    let memtable = create_large_memtable();
    let predicate = PredicateGroup::default();
    let projection = vec![2]; // Only project f1 column

    let mut group = c.benchmark_group("ranges_with_projection");

    group.bench_function("parallel_with_projection", |b| {
        b.iter(|| {
            black_box(
                memtable
                    .ranges(Some(&projection), predicate.clone(), None)
                    .unwrap(),
            )
        })
    });

    group.bench_function("sequential_with_projection", |b| {
        b.iter(|| {
            black_box(
                memtable
                    .ranges_sequential(Some(&projection), predicate.clone(), None)
                    .unwrap(),
            )
        })
    });

    group.finish();
}

fn bench_ranges_with_sequence_filter(c: &mut Criterion) {
    let memtable = create_large_memtable();
    let predicate = PredicateGroup::default();
    let sequence = Some(500); // Filter to first half of sequences

    let mut group = c.benchmark_group("ranges_with_sequence_filter");

    group.bench_function("parallel_with_sequence_filter", |b| {
        b.iter(|| black_box(memtable.ranges(None, predicate.clone(), sequence).unwrap()))
    });

    group.bench_function("sequential_with_sequence_filter", |b| {
        b.iter(|| {
            black_box(
                memtable
                    .ranges_sequential(None, predicate.clone(), sequence)
                    .unwrap(),
            )
        })
    });

    group.finish();
}

fn bench_memtable_write_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable_write");

    group.bench_function("write_small_batch", |b| {
        b.iter_with_setup(
            || new_test_memtable(false, MergeMode::LastRow),
            |memtable| {
                let data = vec![(1i64, 1.0f64, "test".to_string())];
                let kvs = build_key_values(&memtable.region_metadata(), 0, &data);
                memtable.write(&kvs).unwrap();
            },
        )
    });

    group.bench_function("write_large_batch", |b| {
        b.iter_with_setup(
            || new_test_memtable(false, MergeMode::LastRow),
            |memtable| {
                let data: Vec<_> = (0..1000)
                    .map(|i| (i as i64, i as f64, format!("value_{}", i)))
                    .collect();
                let kvs = build_key_values(&memtable.region_metadata(), 0, &data);
                memtable.write(&kvs).unwrap();
            },
        )
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_ranges_parallel_vs_sequential,
    bench_ranges_with_projection,
    bench_ranges_with_sequence_filter,
    bench_memtable_write_performance
);
criterion_main!(benches);
