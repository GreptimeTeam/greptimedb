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

//! Real Dense index updates, excluding batch generation, creator setup and cleanup.
//! `CARGO_PROFILE_BENCH_DEBUG=0 cargo bench -p mito2 --features testing
//! --bench bench_dense_index_update -- --save-baseline before`
//! Repeat with `--baseline before` after changing the implementation.
//! Set `DENSE_INDEX_ALLOCATIONS=1` to print allocation counts/bytes for one update
//! instead of timing. The current-thread runtime keeps counting scoped to the update.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use api::v1::SemanticType;
use common_test_util::temp_dir::create_temp_dir;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use datatypes::arrow::array::{
    ArrayRef, BinaryArray, BinaryDictionaryBuilder, DictionaryArray, StringDictionaryBuilder,
    TimestampMillisecondArray, UInt8Array, UInt64Array,
};
use datatypes::arrow::datatypes::UInt32Type;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::{ColumnSchema, SkippingIndexOptions};
use datatypes::value::Value;
use mito_codec::row_converter::{DensePrimaryKeyCodec, PrimaryKeyCodecExt};
use mito2::read::{Batch, BatchBuilder};
use mito2::sst::index::Indexer;
use mito2::sst::index::intermediate::IntermediateManager;
use mito2::sst::{FlatSchemaOptions, to_flat_sst_arrow_schema};
use mito2::test_util::bench_util::{BloomFilterIndexer, InvertedIndexer};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder, RegionMetadataRef};
use store_api::storage::{ColumnId, FileId, RegionId};

thread_local! {
    static ALLOCATIONS: Cell<Option<(usize, usize)>> = const { Cell::new(None) };
}

struct CountingAllocator;

fn record_allocation(bytes: usize) {
    ALLOCATIONS.with(|counter| {
        if let Some((count, total)) = counter.get() {
            counter.set(Some((count + 1, total + bytes)));
        }
    });
}

// SAFETY: All allocations are delegated unchanged to System, including layout and ownership.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record_allocation(layout.size());
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record_allocation(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record_allocation(new_size);
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

// Allocation mode is a benchmark report, not a server log.
#[allow(clippy::print_stdout)]
fn report_allocations(name: &str) {
    let (count, bytes) = ALLOCATIONS.replace(None).unwrap();
    println!("{name},allocations={count},bytes={bytes}");
}

const ROWS: usize = 4096;
const SEGMENT_ROWS: usize = 1024;

struct Shape {
    name: String,
    tags: u32,
    indexed_tags: u32,
    inverted: bool,
    bloom: bool,
    rows_per_key: usize,
    cardinality: usize,
    numeric: bool,
    field: bool,
}

fn input(shape: &Shape) -> (RegionMetadataRef, HashSet<ColumnId>, RecordBatch) {
    let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
    let mut inverted_columns = HashSet::new();
    for id in 0..shape.tags + u32::from(shape.field) {
        let field = id == shape.tags;
        let indexed = if shape.field {
            field
        } else {
            id < shape.indexed_tags
        };
        let data_type = if shape.numeric || field {
            ConcreteDataType::uint64_datatype()
        } else {
            ConcreteDataType::string_datatype()
        };
        let mut schema = ColumnSchema::new(format!("col_{id}"), data_type, true);
        if indexed && shape.inverted {
            inverted_columns.insert(id);
        }
        if indexed && shape.bloom {
            schema = schema
                .with_skipping_options(SkippingIndexOptions {
                    granularity: SEGMENT_ROWS as _,
                    ..Default::default()
                })
                .unwrap();
        }
        builder.push_column_metadata(ColumnMetadata {
            column_schema: schema,
            semantic_type: if field {
                SemanticType::Field
            } else {
                SemanticType::Tag
            },
            column_id: id,
        });
    }
    builder
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            semantic_type: SemanticType::Timestamp,
            column_id: shape.tags + 1,
        })
        .primary_key((0..shape.tags).collect())
        .primary_key_encoding(PrimaryKeyEncoding::Dense);
    let metadata = Arc::new(builder.build().unwrap());
    // Last tag is unique per series, so low-cardinality indexed tags still have many PKs.
    let values: Vec<Vec<Value>> = (0..shape.tags)
        .map(|id| {
            (0..ROWS / shape.rows_per_key)
                .map(|series| {
                    let value = if id == shape.tags - 1 {
                        series
                    } else {
                        series % shape.cardinality
                    };
                    if shape.numeric {
                        Value::UInt64(value as u64)
                    } else {
                        Value::from(format!(
                            "tag-{id:03}-value-{value:010}-abcdefghijklmnopqrstuvwxyz"
                        ))
                    }
                })
                .collect()
        })
        .collect();
    let mut columns: Vec<ArrayRef> = values
        .iter()
        .map(|values| {
            if shape.numeric {
                Arc::new(UInt64Array::from_iter_values(
                    (0..ROWS).map(|row| values[row / shape.rows_per_key].as_u64().unwrap()),
                )) as ArrayRef
            } else {
                let mut dict = StringDictionaryBuilder::<UInt32Type>::new();
                for row in 0..ROWS {
                    dict.append(values[row / shape.rows_per_key].as_string().unwrap())
                        .unwrap();
                }
                Arc::new(dict.finish()) as ArrayRef
            }
        })
        .collect();
    if shape.field {
        columns.push(Arc::new(UInt64Array::from_iter_values(
            (0..ROWS).map(|row| (row % shape.cardinality) as u64),
        )));
    }
    let codec = DensePrimaryKeyCodec::new(&metadata);
    let mut keys = BinaryDictionaryBuilder::<UInt32Type>::new();
    for series in 0..ROWS / shape.rows_per_key {
        let key = codec
            .encode(values.iter().map(|column| column[series].as_value_ref()))
            .unwrap();
        for _ in 0..shape.rows_per_key {
            keys.append(&key).unwrap();
        }
    }
    columns.extend([
        Arc::new(TimestampMillisecondArray::from_iter_values(0..ROWS as i64)) as ArrayRef,
        Arc::new(keys.finish()),
        Arc::new(UInt64Array::from(vec![1; ROWS])),
        Arc::new(UInt8Array::from(vec![1; ROWS])),
    ]);
    let schema = to_flat_sst_arrow_schema(
        &metadata,
        &FlatSchemaOptions::from_encoding(PrimaryKeyEncoding::Dense),
    );
    (
        metadata,
        inverted_columns,
        RecordBatch::try_new(schema, columns).unwrap(),
    )
}

fn bench_dense_index_update(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let dir = create_temp_dir("bench_dense_index_update");
    let intermediate = runtime
        .block_on(IntermediateManager::init_fs(dir.path().to_str().unwrap()))
        .unwrap();
    let allocations = std::env::var_os("DENSE_INDEX_ALLOCATIONS").is_some();
    let mut group = c.benchmark_group("dense_index_update");
    group.sample_size(30);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(3));
    group.throughput(Throughput::Elements(ROWS as u64));
    let mut shapes = Vec::new();
    for rows_per_key in [1, 8, 64] {
        for (mode, inverted, bloom) in [
            ("inverted", true, false),
            ("bloom", false, true),
            ("both", true, true),
        ] {
            shapes.push(Shape {
                name: format!("{mode}_string_one_10tags_{rows_per_key}rpk"),
                tags: 10,
                indexed_tags: 1,
                inverted,
                bloom,
                rows_per_key,
                cardinality: ROWS,
                numeric: false,
                field: false,
            });
        }
    }
    for (name, tags, indexed_tags, rows_per_key, cardinality, numeric, field) in [
        ("both_string_all_40tags_1rpk", 40, 40, 1, ROWS, false, false),
        ("both_string_all_40tags_8rpk", 40, 40, 8, ROWS, false, false),
        ("both_string_low_cardinality", 10, 1, 1, 8, false, false),
        ("both_numeric_1rpk", 10, 1, 1, ROWS, true, false),
        ("both_numeric_64rpk", 10, 1, 64, ROWS, true, false),
        ("both_numeric_varying_field", 10, 0, 64, 8, true, true),
    ] {
        shapes.push(Shape {
            name: name.into(),
            tags,
            indexed_tags,
            inverted: true,
            bloom: true,
            rows_per_key,
            cardinality,
            numeric,
            field,
        });
    }
    for shape in shapes {
        let (metadata, inverted_columns, batch) = input(&shape);
        for encoded_only in [false, true] {
            let name = if encoded_only {
                format!("encoded/{}", shape.name)
            } else {
                shape.name.clone()
            };
            let encoded_batches: Vec<Batch> = if encoded_only {
                let pk = batch
                    .column_by_name("__primary_key")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<DictionaryArray<UInt32Type>>()
                    .unwrap();
                let values = pk.values().as_any().downcast_ref::<BinaryArray>().unwrap();
                (0..ROWS)
                    .step_by(shape.rows_per_key)
                    .map(|row| {
                        let mut builder =
                            BatchBuilder::new(values.value(pk.keys().value(row) as usize).to_vec());
                        builder
                            .timestamps_array(
                                batch
                                    .column_by_name("ts")
                                    .unwrap()
                                    .slice(row, shape.rows_per_key),
                            )
                            .unwrap();
                        builder
                            .sequences_array(
                                batch
                                    .column_by_name("__sequence")
                                    .unwrap()
                                    .slice(row, shape.rows_per_key),
                            )
                            .unwrap();
                        builder
                            .op_types_array(
                                batch
                                    .column_by_name("__op_type")
                                    .unwrap()
                                    .slice(row, shape.rows_per_key),
                            )
                            .unwrap();
                        if shape.field {
                            builder
                                .push_field_array(
                                    shape.tags,
                                    batch
                                        .column_by_name(&format!("col_{}", shape.tags))
                                        .unwrap()
                                        .slice(row, shape.rows_per_key),
                                )
                                .unwrap();
                        }
                        builder.build().unwrap()
                    })
                    .collect()
            } else {
                Vec::new()
            };
            let run = |iterations| {
                runtime.block_on(async {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iterations {
                        let file_id = FileId::random();
                        let inverted = shape.inverted.then(|| {
                            InvertedIndexer::new(
                                file_id,
                                &metadata,
                                intermediate.clone(),
                                None,
                                NonZeroUsize::new(SEGMENT_ROWS).unwrap(),
                                inverted_columns.clone(),
                            )
                        });
                        let bloom = if shape.bloom {
                            BloomFilterIndexer::new(file_id, &metadata, intermediate.clone(), None)
                                .unwrap()
                        } else {
                            None
                        };
                        let mut indexer = Indexer::for_bench(&metadata, inverted, bloom);
                        // Fresh caches for each iteration; both creators share each Batch.
                        let mut encoded_batches = encoded_batches.clone();
                        if allocations {
                            ALLOCATIONS.set(Some((0, 0)));
                        }
                        let start = Instant::now();
                        if encoded_only {
                            for batch in &mut encoded_batches {
                                indexer.update(batch).await;
                            }
                        } else {
                            indexer.update_flat(&batch).await;
                        }
                        elapsed += start.elapsed();
                        if allocations {
                            report_allocations(&name);
                        }
                        indexer.abort().await;
                    }
                    elapsed
                })
            };
            if allocations {
                run(1);
            } else {
                group.bench_function(&name, |b| b.iter_custom(&run));
            }
        }
    }
    group.finish();
}

criterion_group!(benches, bench_dense_index_update);
criterion_main!(benches);
