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

//! Tests of the live-handle dynamic inverted-index applier.
//!
//! Covers the phase-1 admission matrix, the NULL guards, the live handle (add,
//! generation change, reset), fail-open on an unknown index, rename and type drift, the
//! differential result check against the parquet-only path, and the measurement of the
//! three arms (static index, parquet prefilter, live-handle index).

use std::collections::HashSet;
use std::time::Instant;

use api::v1::SemanticType;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::expressions::{
    BinaryExpr as PhysicalBinaryExpr, CaseExpr, Column as PhysicalColumn,
    DynamicFilterPhysicalExpr, InListExpr, IsNullExpr, NotExpr, lit, try_cast,
};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_common::ScalarValue;
use datafusion_expr::{Expr, Operator, col};
use datatypes::arrow::array::{
    Array, ArrayRef, BinaryDictionaryBuilder, Int64Array, StringArray, TimestampMillisecondArray,
    UInt8Array, UInt64Array,
};
use datatypes::arrow::datatypes::{
    DataType as ArrowDataType, Field, Schema as ArrowSchema, TimeUnit,
};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use futures::TryStreamExt;
use mito_codec::row_converter::{DensePrimaryKeyCodec, PrimaryKeyCodecExt, SortField};
use parquet::basic::Type as PhysicalType;
use parquet::file::metadata::{
    ColumnChunkMetaData, FileMetaData, ParquetMetaData, RowGroupMetaData,
};
use parquet::file::statistics::Statistics;
use parquet::schema::types::{SchemaDescriptor, Type};
use store_api::metadata::{ColumnMetadata, RegionMetadata, RegionMetadataBuilder};
use store_api::region_engine::{PartitionRange, RegionScanner};
use store_api::region_request::PathType;
use store_api::storage::RegionId;
use table::predicate::Predicate;

use super::*;
use crate::access_layer::{AccessLayer, Metrics, RegionFilePathFactory, WriteType};
use crate::cache::{CacheManager, CacheStrategy};
use crate::config::IndexConfig;
use crate::read::flat_projection::FlatProjectionMapper;
use crate::read::pruner::{PartitionPruner, Pruner};
use crate::read::range_cache::{
    build_candidate_range_cache_key, build_range_cache_key, build_series_range_cache_key,
};
use crate::read::read_columns::ReadColumns;
use crate::read::scan_region::{PredicateGroup, ScanInput, StreamContext};
use crate::read::scan_util::PartitionMetrics;
use crate::read::seq_scan::{FlatReadMode, SeqScan};
use crate::read::series_reader::SeriesRange;
use crate::read::series_scan::{SeriesRangeCachePause, SeriesScan};
use crate::region::options::{IndexOptions, InvertedIndexOptions};
use crate::sst::file::{FileHandle, FileMeta};
use crate::sst::file_purger::NoopFilePurger;
use crate::sst::index::registry::IndexRegistry;
use crate::sst::index::{IndexBuildType, IndexerBuilderImpl};
use crate::sst::parquet::WriteOptions;
use crate::sst::parquet::metadata::MetadataLoader;
use crate::sst::parquet::reader::{
    MetadataCacheMetrics, ParquetReader, ParquetReaderBuilder, ReaderMetrics,
};
use crate::sst::parquet::row_group::ParquetFetchMetricsData;
use crate::sst::parquet::row_selection::RowGroupSelection;
use crate::sst::parquet::writer::ParquetWriter;
use crate::sst::{FlatSchemaOptions, to_flat_sst_arrow_schema};
use crate::test_util::TestEnv;
use crate::test_util::sst_util::{
    new_flat_source_from_record_batches, new_record_batch_with_custom_sequence, sst_region_metadata,
};

const FILE_DIR: &str = "/";
const TAG_COLUMN_ID: ColumnId = 0;
const PAYLOAD_COLUMN_ID: ColumnId = 1;
const TS_COLUMN_ID: ColumnId = 2;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// Builds the prototype schema: one `Int64` tag (the primary key, so the flat format
/// stores it as a raw column), one wide string field, and the time index.
fn test_metadata() -> Arc<RegionMetadata> {
    test_metadata_with_index(true)
}

/// The prototype schema with the tag's inverted index switched off.
fn unindexed_metadata() -> Arc<RegionMetadata> {
    test_metadata_with_index(false)
}

fn test_metadata_with_index(with_index: bool) -> Arc<RegionMetadata> {
    let mut builder = RegionMetadataBuilder::new(RegionId::new(0, 0));
    builder
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "tag".to_string(),
                ConcreteDataType::int64_datatype(),
                false,
            )
            .with_inverted_index(with_index),
            semantic_type: SemanticType::Tag,
            column_id: TAG_COLUMN_ID,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "payload".to_string(),
                ConcreteDataType::string_datatype(),
                true,
            ),
            semantic_type: SemanticType::Field,
            column_id: PAYLOAD_COLUMN_ID,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "ts".to_string(),
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            semantic_type: SemanticType::Timestamp,
            column_id: TS_COLUMN_ID,
        })
        .primary_key(vec![TAG_COLUMN_ID]);
    Arc::new(builder.build().unwrap())
}

/// The prototype schema with a `Float64` tag, to test the type slice.
fn float_tag_metadata() -> Arc<RegionMetadata> {
    let mut builder = RegionMetadataBuilder::new(RegionId::new(0, 0));
    builder
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "tag".to_string(),
                ConcreteDataType::float64_datatype(),
                false,
            )
            .with_inverted_index(true),
            semantic_type: SemanticType::Tag,
            column_id: TAG_COLUMN_ID,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "ts".to_string(),
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            semantic_type: SemanticType::Timestamp,
            column_id: TS_COLUMN_ID,
        })
        .primary_key(vec![TAG_COLUMN_ID]);
    Arc::new(builder.build().unwrap())
}

fn indexed_column_ids(metadata: &RegionMetadata) -> HashSet<ColumnId> {
    metadata.inverted_indexed_column_ids(std::iter::empty())
}

/// Arrow schema used to build physical expressions over the fixture columns.
fn fixture_arrow_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("tag", ArrowDataType::Int64, false),
        Field::new("payload", ArrowDataType::Utf8, true),
        Field::new(
            "ts",
            ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]))
}

fn phys_col_on(schema: &ArrowSchema, name: &str) -> Arc<dyn PhysicalExpr> {
    let index = schema.index_of(name).unwrap();
    Arc::new(PhysicalColumn::new(name, index))
}

fn phys_col(name: &str) -> Arc<dyn PhysicalExpr> {
    phys_col_on(&fixture_arrow_schema(), name)
}

fn phys_in_list_on(name: &str, values: Vec<ScalarValue>, negated: bool) -> Arc<dyn PhysicalExpr> {
    let schema = fixture_arrow_schema();
    let list = values
        .into_iter()
        .map(|value| lit(value) as Arc<dyn PhysicalExpr>)
        .collect::<Vec<_>>();
    Arc::new(InListExpr::try_new(phys_col(name), list, negated, &schema).unwrap())
}

fn phys_in_list(name: &str, values: Vec<ScalarValue>, negated: bool) -> Arc<dyn PhysicalExpr> {
    phys_in_list_on(name, values, negated)
}

fn phys_eq(name: &str, value: ScalarValue) -> Arc<dyn PhysicalExpr> {
    let schema = fixture_arrow_schema();
    let _ = &schema;
    Arc::new(PhysicalBinaryExpr::new(
        phys_col(name),
        Operator::Eq,
        lit(value),
    ))
}

fn phys_cmp(name: &str, op: Operator, value: ScalarValue) -> Arc<dyn PhysicalExpr> {
    let schema = fixture_arrow_schema();
    let _ = &schema;
    Arc::new(PhysicalBinaryExpr::new(phys_col(name), op, lit(value)))
}

fn phys_combine(
    left: Arc<dyn PhysicalExpr>,
    op: Operator,
    right: Arc<dyn PhysicalExpr>,
) -> Arc<dyn PhysicalExpr> {
    let schema = fixture_arrow_schema();
    let _ = &schema;
    Arc::new(PhysicalBinaryExpr::new(left, op, right))
}

fn int64_list(values: &[i64]) -> Vec<ScalarValue> {
    values
        .iter()
        .map(|value| ScalarValue::Int64(Some(*value)))
        .collect()
}

/// Creates a live dynamic filter handle over `expr`.
fn dyn_handle(expr: Arc<dyn PhysicalExpr>) -> Arc<DynamicFilterPhysicalExpr> {
    Arc::new(DynamicFilterPhysicalExpr::new(vec![phys_col("tag")], expr))
}

fn logical_atom(name: &str, values: Vec<ScalarValue>) -> Expr {
    Expr::InList(datafusion_expr::expr::InList {
        expr: Box::new(col(name)),
        list: values
            .into_iter()
            .map(|value| Expr::Literal(value, None))
            .collect(),
        negated: false,
    })
}

// ---------------------------------------------------------------------------
// Admission matrix
// ---------------------------------------------------------------------------

#[test]
fn test_dynamic_index_admission_matrix() {
    let metadata = test_metadata();
    let indexed = indexed_column_ids(&metadata);

    let admit = |exprs: Vec<Arc<dyn PhysicalExpr>>| {
        admit_dynamic_filters(&metadata, &indexed, &exprs).map(|admitted| admitted.column.name)
    };

    // Accepted: a positive IN list and an equality on the indexed Int64 tag.
    assert_eq!(
        Some("tag".to_string()),
        admit(vec![phys_in_list("tag", int64_list(&[1, 2, 3]), false)])
    );
    assert_eq!(
        Some("tag".to_string()),
        admit(vec![phys_eq("tag", ScalarValue::Int64(Some(7)))])
    );
    // Accepted: several top-level conjuncts on the same single column.
    assert_eq!(
        Some("tag".to_string()),
        admit(vec![phys_combine(
            phys_in_list("tag", int64_list(&[1, 2]), false),
            Operator::And,
            phys_eq("tag", ScalarValue::Int64(Some(9))),
        )])
    );

    // Rejected: a negated IN list.
    assert_eq!(
        None,
        admit(vec![phys_in_list("tag", int64_list(&[1, 2]), true)])
    );
    // Rejected: OR of two admitted atoms.
    assert_eq!(
        None,
        admit(vec![phys_combine(
            phys_eq("tag", ScalarValue::Int64(Some(1))),
            Operator::Or,
            phys_eq("tag", ScalarValue::Int64(Some(2))),
        )])
    );
    // Rejected: NOT.
    assert_eq!(
        None,
        admit(vec![Arc::new(NotExpr::new(phys_in_list(
            "tag",
            int64_list(&[1, 2]),
            false
        )))])
    );
    // Rejected: IS NULL / IS NOT NULL.
    assert_eq!(
        None,
        admit(vec![Arc::new(IsNullExpr::new(phys_col("tag")))])
    );
    assert_eq!(
        None,
        admit(vec![Arc::new(
            datafusion::physical_plan::expressions::IsNotNullExpr::new(phys_col("tag"))
        )])
    );
    // Rejected: CASE.
    let case = CaseExpr::try_new(
        None,
        vec![(
            phys_eq("tag", ScalarValue::Int64(Some(1))),
            lit(ScalarValue::Int64(Some(1))),
        )],
        Some(lit(ScalarValue::Int64(Some(0)))),
    )
    .unwrap();
    assert_eq!(None, admit(vec![Arc::new(case)]));
    // Rejected: a function/struct/hash-lookup style wrapper (a cast function here).
    assert_eq!(
        None,
        admit(vec![
            try_cast(
                phys_col("tag"),
                &fixture_arrow_schema(),
                ArrowDataType::Int64
            )
            .unwrap()
        ])
    );
    // Range comparisons are declined by the inverted translator.
    for op in [Operator::Gt, Operator::GtEq, Operator::Lt, Operator::LtEq] {
        assert_eq!(
            None,
            admit(vec![phys_cmp("tag", op, ScalarValue::Int64(Some(5)))])
        );
    }
    // Rejected: two range conjuncts on one column, i.e. what a physical BETWEEN looks like.
    assert_eq!(
        None,
        admit(vec![phys_combine(
            phys_cmp("tag", Operator::GtEq, ScalarValue::Int64(Some(1))),
            Operator::And,
            phys_cmp("tag", Operator::LtEq, ScalarValue::Int64(Some(9))),
        )])
    );
    // Rejected: a mirrored range (`lit < col`) is not the published shape.
    assert_eq!(
        None,
        admit(vec![phys_combine(
            lit(ScalarValue::Int64(Some(1))),
            Operator::Lt,
            phys_col("tag"),
        )])
    );
    // Rejected: NotEq.
    assert_eq!(
        None,
        admit(vec![phys_cmp(
            "tag",
            Operator::NotEq,
            ScalarValue::Int64(Some(1))
        )])
    );
    // Rejected: a non-literal right-hand side.
    assert_eq!(
        None,
        admit(vec![phys_combine(
            phys_col("tag"),
            Operator::Gt,
            phys_col("payload"),
        )])
    );
    assert_eq!(
        None,
        admit(vec![phys_combine(
            phys_col("tag"),
            Operator::Gt,
            phys_combine(
                phys_col("tag"),
                Operator::Plus,
                lit(ScalarValue::Int64(Some(1))),
            ),
        )])
    );
    // Rejected: a NULL range literal.
    assert_eq!(
        None,
        admit(vec![phys_cmp(
            "tag",
            Operator::Gt,
            ScalarValue::Int64(None)
        )])
    );
    // Rejected: a range on a nullable column of the wrong type.
    assert_eq!(
        None,
        admit(vec![phys_cmp(
            "tag",
            Operator::Gt,
            ScalarValue::Float64(Some(1.5))
        )])
    );
    // Rejected: a range on a non-tag column.
    assert_eq!(
        None,
        admit(vec![phys_cmp(
            "payload",
            Operator::Gt,
            ScalarValue::Utf8(Some("a".to_string()))
        )])
    );

    // Rejected: a NULL literal inside the list.
    assert_eq!(
        None,
        admit(vec![phys_in_list(
            "tag",
            vec![ScalarValue::Int64(Some(1)), ScalarValue::Int64(None)],
            false
        )])
    );
    // Rejected: an empty list.
    assert_eq!(None, admit(vec![phys_in_list("tag", vec![], false)]));

    // Rejected: a non-tag column, even when it is inverted indexed.
    assert_eq!(
        None,
        admit(vec![phys_in_list(
            "payload",
            vec![ScalarValue::Utf8(Some("a".to_string()))],
            false
        )])
    );
    // Rejected: a tag without an inverted index.
    let plain = (*metadata).clone();
    assert!(
        admit_dynamic_filters(
            &plain,
            &HashSet::new(),
            &[phys_in_list("tag", int64_list(&[1, 2]), false)]
        )
        .is_none()
    );
    // Rejected: a float tag (outside the type slice).
    let float_metadata = float_tag_metadata();
    let float_schema = ArrowSchema::new(vec![Field::new("tag", ArrowDataType::Float64, false)]);
    let float_expr: Arc<dyn PhysicalExpr> = Arc::new(
        InListExpr::try_new(
            phys_col_on(&float_schema, "tag"),
            vec![lit(ScalarValue::Float64(Some(1.5)))],
            false,
            &float_schema,
        )
        .unwrap(),
    );
    assert!(
        admit_dynamic_filters(
            &float_metadata,
            &indexed_column_ids(&float_metadata),
            &[float_expr]
        )
        .is_none()
    );

    // Rejected: a literal type that does not match the column type exactly. DataFusion
    // rejects such a list before the applier sees it, so the type guard is checked on
    // the logical atoms directly.
    let int_type = ConcreteDataType::int64_datatype();
    assert!(atom_literals_match(
        &logical_atom("tag", vec![ScalarValue::Int64(Some(1))]),
        &int_type
    ));
    assert!(!atom_literals_match(
        &logical_atom("tag", vec![ScalarValue::Utf8(Some("a".to_string()))]),
        &int_type
    ));
    assert!(!atom_literals_match(
        &logical_atom("tag", vec![ScalarValue::Float64(Some(1.5))]),
        &int_type
    ));
    assert!(!atom_literals_match(
        &logical_atom("tag", vec![ScalarValue::Int32(Some(1))]),
        &int_type
    ));
    // No dictionary or view reinterpretation.
    assert!(!atom_literals_match(
        &logical_atom(
            "tag",
            vec![ScalarValue::Dictionary(
                Box::new(ArrowDataType::Int32),
                Box::new(ScalarValue::Utf8(Some("a".to_string())))
            )]
        ),
        &ConcreteDataType::string_datatype()
    ));
    // String columns accept exactly a `Utf8` literal.
    assert!(atom_literals_match(
        &logical_atom("tag", vec![ScalarValue::Utf8(Some("a".to_string()))]),
        &ConcreteDataType::string_datatype()
    ));
    assert!(!atom_literals_match(
        &logical_atom("tag", vec![ScalarValue::Utf8View(Some("a".into()))]),
        &ConcreteDataType::string_datatype()
    ));

    // Rejected: two guarded columns.
    let two_columns = phys_combine(
        phys_in_list("tag", int64_list(&[1, 2]), false),
        Operator::And,
        phys_in_list(
            "payload",
            vec![ScalarValue::Utf8(Some("a".to_string()))],
            false,
        ),
    );
    assert_eq!(None, admit(vec![two_columns]));
}

// ---------------------------------------------------------------------------
// NULL guards
// ---------------------------------------------------------------------------

/// Builds a synthetic parquet file metadata whose first leaf is the tag column.
///
/// `nulls` is the tag's null count; `None` means "the file has no statistics".
fn synthetic_parquet_meta(nulls: Option<u64>) -> ParquetMetaData {
    let names = [
        "tag",
        "payload",
        "ts",
        "__primary_key",
        "__sequence",
        "__op_type",
    ];
    let fields: Vec<std::sync::Arc<Type>> = names
        .iter()
        .map(|name| {
            let physical = if *name == "tag" || *name == "ts" || *name == "__sequence" {
                PhysicalType::INT64
            } else {
                PhysicalType::BYTE_ARRAY
            };
            std::sync::Arc::new(
                Type::primitive_type_builder(name, physical)
                    .build()
                    .unwrap(),
            )
        })
        .collect();
    let schema_descr = std::sync::Arc::new(SchemaDescriptor::new(std::sync::Arc::new(
        Type::group_type_builder("schema")
            .with_fields(fields)
            .build()
            .unwrap(),
    )));
    let chunks: Vec<_> = (0..schema_descr.num_columns())
        .map(|index| {
            let mut builder = ColumnChunkMetaData::builder(schema_descr.column(index));
            if index == 0
                && let Some(nulls) = nulls
            {
                builder = builder.set_statistics(Statistics::int64(
                    Some(1),
                    Some(100),
                    None,
                    Some(nulls),
                    true,
                ));
            }
            builder.build().unwrap()
        })
        .collect();
    let row_group = RowGroupMetaData::builder(schema_descr.clone())
        .set_num_rows(100)
        .set_total_byte_size(0)
        .set_column_metadata(chunks)
        .build()
        .unwrap();
    let file_meta = FileMetaData::new(1, 100, None, None, schema_descr, None);
    ParquetMetaData::new(file_meta, vec![row_group])
}

fn test_read_format(metadata: &Arc<RegionMetadata>) -> FlatReadFormat {
    FlatReadFormat::new(
        metadata.clone(),
        ReadColumns::new([TAG_COLUMN_ID, PAYLOAD_COLUMN_ID, TS_COLUMN_ID]),
        None,
        "test.parquet",
        false,
    )
    .unwrap()
}

#[test]
fn test_dynamic_index_null_guards() {
    let metadata = test_metadata();
    let format = test_read_format(&metadata);

    // A non-nullable tag needs no per-file proof.
    let nonnull = admit_dynamic_filters(
        &metadata,
        &indexed_column_ids(&metadata),
        &[phys_in_list("tag", int64_list(&[1, 2]), false)],
    )
    .unwrap();
    assert!(!nonnull.column.nullable);
    assert!(guards_pass(
        &nonnull.column,
        &metadata,
        &format,
        &synthetic_parquet_meta(None)
    ));

    // An older file can remain nullable even when the current tag is not.
    let mut old_metadata = (*metadata).clone();
    old_metadata.column_metadatas[0].column_schema =
        ColumnSchema::new("tag", ConcreteDataType::int64_datatype(), true)
            .with_inverted_index(true);
    let old_metadata = Arc::new(old_metadata);
    let old_format = test_read_format(&old_metadata);
    assert!(!guards_pass(
        &nonnull.column,
        &old_metadata,
        &old_format,
        &synthetic_parquet_meta(None)
    ));
    assert!(!guards_pass(
        &nonnull.column,
        &old_metadata,
        &old_format,
        &synthetic_parquet_meta(Some(1))
    ));
    assert!(guards_pass(
        &nonnull.column,
        &old_metadata,
        &old_format,
        &synthetic_parquet_meta(Some(0))
    ));

    // A nullable tag with a per-file `null_count == 0` proof is accepted.
    let nullable = GuardedColumn {
        name: "tag".to_string(),
        column_id: TAG_COLUMN_ID,
        data_type: ConcreteDataType::int64_datatype(),
        nullable: true,
    };
    assert!(guards_pass(
        &nullable,
        &metadata,
        &format,
        &synthetic_parquet_meta(Some(0))
    ));
    // A nullable tag with NULLs in the file is declined.
    assert!(!guards_pass(
        &nullable,
        &metadata,
        &format,
        &synthetic_parquet_meta(Some(3))
    ));
    // Unknown statistics are not a proof: declined.
    assert!(!guards_pass(
        &nullable,
        &metadata,
        &format,
        &synthetic_parquet_meta(None)
    ));

    // The raw helper agrees with the guard.
    assert!(file_has_no_nulls(
        &format,
        &synthetic_parquet_meta(Some(0)),
        TAG_COLUMN_ID
    ));
    assert!(!file_has_no_nulls(
        &format,
        &synthetic_parquet_meta(Some(1)),
        TAG_COLUMN_ID
    ));
    assert!(!file_has_no_nulls(
        &format,
        &synthetic_parquet_meta(None),
        TAG_COLUMN_ID
    ));
    // A column this file does not store has no proof either.
    assert!(!file_has_no_nulls(
        &format,
        &synthetic_parquet_meta(Some(0)),
        99
    ));

    // One guarded column stays admitted even with two conjuncts on it.
    assert!(
        admit_dynamic_filters(
            &metadata,
            &indexed_column_ids(&metadata),
            &[phys_combine(
                phys_in_list("tag", int64_list(&[1]), false),
                Operator::And,
                phys_in_list("tag", int64_list(&[2]), false),
            )]
        )
        .is_some()
    );
    // A second guarded child (another column) is declined.
    assert!(
        admit_dynamic_filters(
            &metadata,
            &indexed_column_ids(&metadata),
            &[phys_combine(
                phys_in_list("tag", int64_list(&[1]), false),
                Operator::And,
                phys_eq("payload", ScalarValue::Utf8(Some("a".to_string()))),
            )]
        )
        .is_none()
    );
}

// ---------------------------------------------------------------------------
// File harness
// ---------------------------------------------------------------------------

/// A written SST with everything needed to read it.
struct WrittenFile {
    handle: FileHandle,
    parquet_meta: std::sync::Arc<ParquetMetaData>,
    index_file_size: u64,
}

fn file_path(handle: &FileHandle) -> String {
    crate::sst::location::sst_file_path(FILE_DIR, handle.file_id(), PathType::Bare)
}

fn indexer_builder(
    env: &TestEnv,
    object_store: object_store::ObjectStore,
    path_factory: RegionFilePathFactory,
    metadata: std::sync::Arc<RegionMetadata>,
) -> IndexerBuilderImpl {
    let puffin_manager = env.get_puffin_manager().build(object_store, path_factory);
    IndexerBuilderImpl {
        index_build_inputs: None,
        build_type: IndexBuildType::Flush,
        metadata,
        puffin_manager,
        write_cache_enabled: false,
        intermediate_manager: env.get_intermediate_manager(),
        index_options: IndexOptions {
            inverted_index: InvertedIndexOptions {
                segment_row_count: 1,
                ..Default::default()
            },
        },
        registry: std::sync::Arc::new(IndexRegistry::new(
            Default::default(),
            Default::default(),
            Default::default(),
        )),
    }
}

/// Builds a flat-format batch of the prototype schema.
fn new_batch(start: i64, rows: &[(i64, String)]) -> RecordBatch {
    let metadata = test_metadata();
    let flat_schema = to_flat_sst_arrow_schema(&metadata, &FlatSchemaOptions::default());
    let num_rows = rows.len();
    let tags: Vec<i64> = rows.iter().map(|(tag, _)| *tag).collect();
    let payloads: Vec<String> = rows.iter().map(|(_, payload)| payload.clone()).collect();
    let timestamps: Vec<i64> = (start..start + num_rows as i64).collect();

    let codec = DensePrimaryKeyCodec::with_fields(vec![(
        TAG_COLUMN_ID,
        SortField::new(ConcreteDataType::int64_datatype()),
    )]);
    let mut pk_builder = BinaryDictionaryBuilder::<datatypes::arrow::datatypes::UInt32Type>::new();
    for tag in &tags {
        let pk = codec
            .encode(std::iter::once(datatypes::value::ValueRef::Int64(*tag)))
            .unwrap();
        pk_builder.append(&pk).unwrap();
    }

    let columns: Vec<ArrayRef> = vec![
        std::sync::Arc::new(Int64Array::from(tags)),
        std::sync::Arc::new(StringArray::from(payloads)),
        std::sync::Arc::new(TimestampMillisecondArray::from(timestamps)),
        std::sync::Arc::new(pk_builder.finish()),
        std::sync::Arc::new(UInt64Array::from_value(1, num_rows)),
        std::sync::Arc::new(UInt8Array::from_value(api::v1::OpType::Put as u8, num_rows)),
    ];
    RecordBatch::try_new(flat_schema, columns).unwrap()
}

async fn write_sst(
    env: &TestEnv,
    object_store: object_store::ObjectStore,
    metadata: std::sync::Arc<RegionMetadata>,
    batches: Vec<RecordBatch>,
    row_group_size: usize,
    with_index: bool,
) -> WrittenFile {
    let path_factory = RegionFilePathFactory::new(FILE_DIR.to_string(), PathType::Bare);
    let builder = if with_index {
        indexer_builder(
            env,
            object_store.clone(),
            path_factory.clone(),
            metadata.clone(),
        )
    } else {
        IndexerBuilderImpl {
            index_build_inputs: None,
            build_type: IndexBuildType::Flush,
            metadata: metadata.clone(),
            puffin_manager: env
                .get_puffin_manager()
                .build(object_store.clone(), path_factory.clone()),
            write_cache_enabled: false,
            intermediate_manager: env.get_intermediate_manager(),
            // No index options: this file gets no inverted index.
            index_options: IndexOptions::default(),
            registry: std::sync::Arc::new(IndexRegistry::new(
                Default::default(),
                Default::default(),
                Default::default(),
            )),
        }
    };

    let mut metrics = Metrics::new(WriteType::Flush);
    let mut writer = ParquetWriter::new_with_object_store(
        object_store.clone(),
        metadata.clone(),
        IndexConfig::default(),
        builder,
        path_factory,
        &mut metrics,
    )
    .await;
    let info = writer
        .write_all_flat(
            new_flat_source_from_record_batches(batches),
            None,
            &WriteOptions {
                row_group_size,
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .remove(0);

    if with_index {
        assert!(
            info.index_metadata.file_size > 0,
            "the indexed file must carry an index blob"
        );
    }
    let handle = FileHandle::new(
        FileMeta {
            region_id: metadata.region_id,
            file_id: info.file_id,
            time_range: info.time_range,
            level: 0,
            file_size: info.file_size,
            max_row_group_uncompressed_size: info.max_row_group_uncompressed_size,
            available_indexes: info.index_metadata.build_available_indexes(),
            indexes: info.index_metadata.build_indexes().unwrap(),
            index_file_size: info.index_metadata.file_size,
            index_version: 0,
            num_row_groups: info.num_row_groups,
            num_rows: info.num_rows as u64,
            ..Default::default()
        },
        std::sync::Arc::new(NoopFilePurger),
    );
    let path = file_path(&handle);
    let parquet_meta = MetadataLoader::new(object_store.clone(), &path, info.file_size)
        .load(&mut MetadataCacheMetrics::default())
        .await
        .unwrap();

    WrittenFile {
        handle,
        parquet_meta: std::sync::Arc::new(parquet_meta),
        index_file_size: info.index_metadata.file_size,
    }
}

fn test_cache() -> std::sync::Arc<CacheManager> {
    std::sync::Arc::new(
        CacheManager::builder()
            .index_result_cache_size(1024 * 1024)
            .index_metadata_size(1024 * 1024)
            .index_content_page_size(1024 * 1024)
            .index_content_size(1024 * 1024)
            .puffin_metadata_size(1024 * 1024)
            .build(),
    )
}

fn static_applier(
    env: &TestEnv,
    object_store: object_store::ObjectStore,
    metadata: std::sync::Arc<RegionMetadata>,
    exprs: &[Expr],
    cache: &std::sync::Arc<CacheManager>,
) -> Option<std::sync::Arc<InvertedIndexApplier>> {
    InvertedIndexApplierBuilder::new(
        FILE_DIR.to_string(),
        PathType::Bare,
        object_store,
        &metadata,
        metadata.inverted_indexed_column_ids(std::iter::empty()),
        env.get_puffin_manager(),
    )
    .with_inverted_index_cache(cache.inverted_index_cache().cloned())
    .with_puffin_metadata_cache(cache.puffin_metadata_cache().cloned())
    .build(exprs)
    .unwrap()
    .map(std::sync::Arc::new)
}

fn dynamic_applier(
    env: &TestEnv,
    object_store: object_store::ObjectStore,
    metadata: std::sync::Arc<RegionMetadata>,
    predicate: Predicate,
    cache: &std::sync::Arc<CacheManager>,
) -> std::sync::Arc<DynamicInvertedIndexApplier> {
    std::sync::Arc::new(DynamicInvertedIndexApplier::new(
        DynamicInvertedIndexConfig {
            table_dir: FILE_DIR.to_string(),
            path_type: PathType::Bare,
            store: object_store,
            puffin_manager_factory: env.get_puffin_manager(),
            file_cache: None,
            inverted_index_cache: cache.inverted_index_cache().cloned(),
            puffin_metadata_cache: cache.puffin_metadata_cache().cloned(),
            indexed_column_ids: metadata.inverted_indexed_column_ids(std::iter::empty()),
            output_schema: metadata.schema.arrow_schema().clone(),
            metadata,
            predicate,
        },
    ))
}

/// Number of rows an inverted-index apply output selects.
///
/// `ApplyOutput::total_row_count` is the row count of the whole index, so the selection is
/// reconstructed exactly like the reader does.
fn matched_rows(
    file: &WrittenFile,
    output: index::inverted_index::search::index_apply::ApplyOutput,
) -> usize {
    let row_group_size = file.parquet_meta.row_group(0).num_rows() as usize;
    let num_row_groups = file.parquet_meta.num_row_groups();
    let total_row_count = file.parquet_meta.file_metadata().num_rows() as usize;
    RowGroupSelection::from_inverted_index_apply_output(
        row_group_size,
        num_row_groups,
        total_row_count,
        output,
    )
    .expect("the index row count matches the file")
    .row_count()
}

/// Reads only the guarded tag column over all files.
///
/// `page_size_to_fetch_store` aggregates the prefilter and the projection passes, so this
/// probe isolates the filter-column part of a scan: it projects nothing but the tag.
async fn probe_tag_column_bytes(
    object_store: object_store::ObjectStore,
    metadata: std::sync::Arc<RegionMetadata>,
    files: &[WrittenFile],
    index_applier: Option<std::sync::Arc<InvertedIndexApplier>>,
    predicate: Predicate,
) -> (u64, usize) {
    let cache = test_cache();
    let mut bytes = 0;
    let mut pages = 0;
    for file in files {
        let builder = ParquetReaderBuilder::new(
            FILE_DIR.to_string(),
            PathType::Bare,
            file.handle.clone(),
            object_store.clone(),
        )
        .predicate(Some(predicate.clone()))
        .expected_metadata(Some(metadata.clone()))
        .inverted_index_appliers([index_applier.clone(), None])
        .projection(Some(ReadColumns::new([TAG_COLUMN_ID])))
        .cache(CacheStrategy::EnableAll(cache.clone()));
        let mut metrics = ReaderMetrics::default();
        if let Some((context, selection)) = builder.build_reader_input(&mut metrics).await.unwrap()
        {
            let mut reader = ParquetReader::new(std::sync::Arc::new(context), selection)
                .await
                .unwrap();
            while reader.next_record_batch().await.unwrap().is_some() {}
            let fetch = reader.fetch_metrics_snapshot();
            bytes += fetch.page_size_to_fetch_store;
            pages += fetch.pages_to_fetch_store;
        }
    }
    (bytes, pages)
}

/// What one arm read for one file.
#[derive(Clone)]
struct ArmResult {
    rows: Vec<(i64, i64)>,
    filter_metrics: crate::sst::parquet::reader::ReaderFilterMetrics,
    fetch: ParquetFetchMetricsData,
    elapsed_us: u128,
    index_apply_us: u128,
    index_blob_bytes: u64,
}

/// Reads one file and collects `(ts, tag)` pairs plus the reader counters.
#[allow(clippy::too_many_arguments)]
async fn read_file(
    object_store: object_store::ObjectStore,
    metadata: std::sync::Arc<RegionMetadata>,
    file: &WrittenFile,
    cache: &std::sync::Arc<CacheManager>,
    index_applier: Option<std::sync::Arc<InvertedIndexApplier>>,
    predicate: Option<Predicate>,
    dynamic: Option<std::sync::Arc<DynamicInvertedIndexApplier>>,
    enable_prefilter: bool,
) -> ArmResult {
    let start = Instant::now();
    let builder = ParquetReaderBuilder::new(
        FILE_DIR.to_string(),
        PathType::Bare,
        file.handle.clone(),
        object_store,
    )
    .predicate(predicate)
    .expected_metadata(Some(metadata))
    .inverted_index_appliers([index_applier, None])
    .enable_predicate_prefilter(enable_prefilter)
    .cache(CacheStrategy::EnableAll(cache.clone()))
    .dynamic_inverted_index_applier(dynamic);

    let mut metrics = ReaderMetrics::default();
    // The reader only fills the applier metrics when they are already present.
    metrics.filter_metrics.inverted_index_apply_metrics = Some(Default::default());
    metrics.filter_metrics.dynamic_inverted_index_apply_metrics = Some(Default::default());
    let mut rows = Vec::new();
    let mut fetch = ParquetFetchMetricsData::default();
    if let Some((context, selection)) = builder.build_reader_input(&mut metrics).await.unwrap() {
        let mut reader = ParquetReader::new(std::sync::Arc::new(context), selection)
            .await
            .unwrap();
        while let Some(batch) = reader.next_record_batch().await.unwrap() {
            let ts = batch.column_by_name("ts").unwrap();
            let ts = ts
                .as_any()
                .downcast_ref::<datatypes::arrow::array::TimestampMillisecondArray>()
                .unwrap();
            let tag = batch.column_by_name("tag").unwrap();
            let tag = tag.as_any().downcast_ref::<Int64Array>().unwrap();
            for index in 0..batch.num_rows() {
                rows.push((ts.value(index), tag.value(index)));
            }
        }
        fetch = reader.fetch_metrics_snapshot();
    }
    let elapsed_us = start.elapsed().as_micros();
    let (index_apply_us, index_blob_bytes) = metrics
        .filter_metrics
        .inverted_index_apply_metrics
        .as_ref()
        .map(|m| (m.apply_elapsed.as_micros(), m.blob_read_bytes))
        .unwrap_or((0, 0));
    let dynamic_apply = metrics
        .filter_metrics
        .dynamic_inverted_index_apply_metrics
        .as_ref()
        .map(|m| (m.apply_elapsed.as_micros(), m.blob_read_bytes))
        .unwrap_or((0, 0));

    ArmResult {
        rows,
        filter_metrics: metrics.filter_metrics,
        fetch,
        elapsed_us,
        index_apply_us: index_apply_us + dynamic_apply.0,
        index_blob_bytes: index_blob_bytes + dynamic_apply.1,
    }
}

// ---------------------------------------------------------------------------
// Live handle and fail-open tests
// ---------------------------------------------------------------------------

async fn single_file_env() -> (TestEnv, object_store::ObjectStore, WrittenFile) {
    let mut env = TestEnv::new().await;
    let object_store = env.init_object_store_manager();
    let rows: Vec<(i64, String)> = (0..100).map(|i| (i % 10, format!("p{i}"))).collect();
    let file = write_sst(
        &env,
        object_store.clone(),
        test_metadata(),
        vec![new_batch(0, &rows)],
        100,
        true,
    )
    .await;
    (env, object_store, file)
}

fn cache_test_input(
    env: &TestEnv,
    object_store: object_store::ObjectStore,
    metadata: &Arc<RegionMetadata>,
    file: &WrittenFile,
    cache: Arc<CacheManager>,
) -> ScanInput {
    let access = Arc::new(AccessLayer::new(
        FILE_DIR,
        PathType::Bare,
        object_store,
        env.get_puffin_manager(),
        env.get_intermediate_manager(),
    ));
    let mapper = FlatProjectionMapper::new(metadata, 0..metadata.column_metadatas.len()).unwrap();
    let predicate =
        PredicateGroup::new(metadata, &[col("tag").gt_eq(datafusion_expr::lit(0_i64))]).unwrap();
    ScanInput::builder(access, mapper)
        .with_predicate(predicate)
        .with_files(vec![file.handle.clone()])
        .with_cache(CacheStrategy::EnableAll(cache))
        .with_dynamic_indexed_column_ids(Some(indexed_column_ids(metadata)))
        .build()
}

fn cache_test_context(
    env: &TestEnv,
    object_store: object_store::ObjectStore,
    metadata: &Arc<RegionMetadata>,
    file: &WrittenFile,
    cache: Arc<CacheManager>,
) -> Arc<StreamContext> {
    Arc::new(StreamContext::seq_scan_ctx(cache_test_input(
        env,
        object_store,
        metadata,
        file,
        cache,
    )))
}

fn cache_test_partition(
    context: &Arc<StreamContext>,
    range: &PartitionRange,
) -> Arc<PartitionPruner> {
    let pruner = Arc::new(Pruner::new(context.clone(), 1));
    pruner.add_partition_ranges(std::slice::from_ref(range));
    Arc::new(PartitionPruner::new(pruner, std::slice::from_ref(range)))
}

fn cache_test_metrics(region_id: RegionId) -> PartitionMetrics {
    PartitionMetrics::new(
        region_id,
        0,
        "dynamic_cache_test",
        Instant::now(),
        true,
        &ExecutionPlanMetricsSet::new(),
    )
}

async fn cache_test_read(
    context: &Arc<StreamContext>,
    range: &PartitionRange,
    partition: Arc<PartitionPruner>,
) -> (Vec<RecordBatch>, PartitionMetrics) {
    let metrics = cache_test_metrics(context.input.region_metadata().region_id);
    let (stream, _) = SeqScan::build_flat_partition_range_read(
        context,
        range,
        FlatReadMode::Sequential,
        &metrics,
        partition,
        None,
        None,
    )
    .await
    .unwrap();
    (stream.try_collect().await.unwrap(), metrics)
}

fn cache_test_tags(batches: &[RecordBatch]) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name("tag")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect()
}

#[tokio::test]
async fn test_dynamic_index_range_cache_late_install_and_reset() {
    let (env, object_store, file) = single_file_env().await;
    let metadata = test_metadata();
    let cache = Arc::new(
        CacheManager::builder()
            .range_result_cache_size(1024 * 1024)
            // The cache builder also creates an index cache, which requires a
            // nonzero page size even when index content caching is disabled.
            .index_content_page_size(1024 * 1024)
            .build(),
    );
    let context = cache_test_context(&env, object_store.clone(), &metadata, &file, cache.clone());
    let range = context.partition_ranges()[0];
    let partition = cache_test_partition(&context, &range);
    let series_range = SeriesRange::new(0, 1).unwrap();
    let key = build_range_cache_key(&context, &range, Some(&partition)).unwrap();
    assert!(build_candidate_range_cache_key(&context, &range, &partition).is_some());
    assert!(build_series_range_cache_key(&context, &range, series_range, &partition).is_some());

    // The filter is installed only after static-key admission and before index apply.
    // Rejection, not a recheck at publication, protects prefetched builders too.
    let rejected_before = crate::metrics::DYNAMIC_INDEX_FILTER_TOTAL
        .with_label_values(&["range_cache", "rejected_install"])
        .get();
    let a = dyn_handle(phys_eq("tag", ScalarValue::Int64(Some(1))));
    assert_eq!(vec![false], context.add_dyn_filter_to_predicate(vec![a]));
    assert!(
        crate::metrics::DYNAMIC_INDEX_FILTER_TOTAL
            .with_label_values(&["range_cache", "rejected_install"])
            .get()
            > rejected_before
    );
    assert!(
        context
            .input
            .predicate_group()
            .predicate()
            .unwrap()
            .dyn_filters()
            .is_empty()
    );

    // The shared pruner's background prefetch must also see the static-only
    // predicate, even if it builds the file before the loader requests it.
    let pruner = Arc::new(Pruner::new(context.clone(), 1));
    pruner.add_partition_ranges(std::slice::from_ref(&range));
    let prefetch_metrics = cache_test_metrics(metadata.region_id);
    pruner.get_file_builder_background(
        0,
        context.range_pre_filter_mode(&range),
        Some(prefetch_metrics),
    );
    let partition = Arc::new(PartitionPruner::new(pruner, std::slice::from_ref(&range)));
    let (owner, _) = cache_test_read(&context, &range, partition).await;
    assert_eq!(100, cache_test_tags(&owner).len());
    assert!(cache_test_tags(&owner).contains(&2));
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while context
            .input
            .cache_strategy
            .get_range_result(&key)
            .is_none()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    // A different static-only scan replays the same key and still sees tag 2.
    let follower = cache_test_context(&env, object_store.clone(), &metadata, &file, cache.clone());
    let follower_range = follower.partition_ranges()[0];
    let (replayed, metrics) = cache_test_read(
        &follower,
        &follower_range,
        cache_test_partition(&follower, &follower_range),
    )
    .await;
    assert_eq!(cache_test_tags(&owner), cache_test_tags(&replayed));
    assert!(format!("{metrics:?}").contains("\"range_cache_hit\":"));

    // Reset reopens this execution gate. A fresh pruner sees B, bypasses the
    // static cache, and actually translates B into the inverted index.
    context.input.predicate.clear_dyn_filters();
    let b = dyn_handle(phys_eq("tag", ScalarValue::Int64(Some(2))));
    assert_eq!(vec![true], context.add_dyn_filter_to_predicate(vec![b]));
    let reset_pruner = Arc::new(Pruner::new(context.clone(), 1));
    reset_pruner.add_partition_ranges(std::slice::from_ref(&range));
    let reset_applier = reset_pruner
        .dynamic_inverted_applier_for_test()
        .unwrap()
        .clone();
    let partition = Arc::new(PartitionPruner::new(
        reset_pruner,
        std::slice::from_ref(&range),
    ));
    assert!(build_range_cache_key(&context, &range, Some(&partition)).is_none());
    assert!(build_candidate_range_cache_key(&context, &range, &partition).is_none());
    assert!(build_series_range_cache_key(&context, &range, series_range, &partition).is_none());
    let (reset_rows, metrics) = cache_test_read(&context, &range, partition).await;
    assert_eq!(vec![2; 10], cache_test_tags(&reset_rows));
    let stats = reset_applier.stats();
    assert_eq!(
        1, stats.translations,
        "stats={stats:?}, metrics={metrics:?}"
    );
    assert_eq!(
        1, stats.applied_files,
        "stats={stats:?}, metrics={metrics:?}"
    );

    // Exercise the public scanner reset, not just the predicate helper.
    let input = cache_test_input(&env, object_store, &metadata, &file, cache);
    let mut scanner = SeqScan::new(input);
    let first = scanner
        .build_stream()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        100,
        first.iter().map(|batch| batch.num_rows()).sum::<usize>()
    );
    assert_eq!(
        vec![false],
        scanner.add_dyn_filter_to_predicate(vec![dyn_handle(phys_eq(
            "tag",
            ScalarValue::Int64(Some(1)),
        ))]),
    );
    scanner.reset_state();
    assert_eq!(
        vec![true],
        scanner.add_dyn_filter_to_predicate(vec![dyn_handle(phys_eq(
            "tag",
            ScalarValue::Int64(Some(2)),
        ))]),
    );
    let second = scanner
        .build_stream()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let tags = second
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name("tag")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect::<Vec<_>>();
    assert_eq!(vec![2; 10], tags);
}

/// A detached legacy series loader must remain static across drop and reset.
#[tokio::test]
async fn test_dynamic_index_series_reset_cannot_contaminate_range_cache() {
    let (env, object_store, file) = single_file_env().await;
    let metadata = test_metadata();
    let cache = Arc::new(
        CacheManager::builder()
            .range_result_cache_size(1024 * 1024)
            .index_content_page_size(1024 * 1024)
            .build(),
    );
    let input = cache_test_input(&env, object_store.clone(), &metadata, &file, cache.clone());
    // Memtables retain clones of this same group, rather than a separate gate.
    let memtable_group = input.predicate_group().clone();
    let mut series = SeriesScan::new(input, false);
    assert_eq!("legacy", series.mode());

    let (admitted_tx, admitted_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
    series.pause_range_cache_for_test(SeriesRangeCachePause {
        admitted: admitted_tx,
        resume: resume_rx,
    });
    let stream = series.build_stream().await.unwrap();
    let key = tokio::time::timeout(std::time::Duration::from_secs(5), admitted_rx)
        .await
        .expect("series loader never admitted a static key")
        .expect("series loader exited before cache admission");
    drop(stream);
    series.reset_state();

    let rejected_before = crate::metrics::DYNAMIC_INDEX_FILTER_TOTAL
        .with_label_values(&["series", "rejected_install"])
        .get();
    let b = dyn_handle(phys_eq("tag", ScalarValue::Int64(Some(2))));
    assert_eq!(
        vec![false],
        series.add_dyn_filter_to_predicate(vec![b.clone()])
    );
    assert!(
        crate::metrics::DYNAMIC_INDEX_FILTER_TOTAL
            .with_label_values(&["series", "rejected_install"])
            .get()
            > rejected_before
    );
    assert!(!memtable_group.add_dyn_filters(vec![b.clone()]));
    assert!(memtable_group.predicate().unwrap().dyn_filters().is_empty());
    assert!(
        series
            .input()
            .predicate_group()
            .predicate()
            .unwrap()
            .dyn_filters()
            .is_empty()
    );

    // The old distributor continues independently, drains its range stream and
    // publishes under the key admitted before reset, despite its receiver being gone.
    resume_tx.send(()).unwrap();
    let cached = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if let Some(cached) = series.input().cache_strategy.get_range_result(&key) {
                break cached;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("detached series loader did not publish its static result");
    let cached_rows: Vec<_> = cached
        .cached_batches()
        .map(|(batch, _)| batch.clone())
        .collect();
    let mut cached_tags = cache_test_tags(&cached_rows);
    cached_tags.sort_unstable();
    let mut expected_tags: Vec<_> = (0..100).map(|i| i % 10).collect();
    expected_tags.sort_unstable();
    assert_eq!(
        expected_tags, cached_tags,
        "the old cache entry must remain static"
    );

    let mut follower = SeriesScan::new(
        cache_test_input(&env, object_store.clone(), &metadata, &file, cache.clone()),
        false,
    );
    assert_eq!((0, 0), follower.range_cache_counts_for_test());
    let replayed = follower
        .build_stream()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let replayed: Vec<_> = replayed
        .into_iter()
        .map(|batch| batch.into_df_record_batch())
        .collect();
    let mut replayed_tags = cache_test_tags(&replayed);
    replayed_tags.sort_unstable();
    assert_eq!(expected_tags, replayed_tags);
    assert_eq!(
        (1, 0),
        follower.range_cache_counts_for_test(),
        "the follower must replay the admitted entry rather than run a fresh scan"
    );
    assert_eq!(
        vec![false],
        follower.add_dyn_filter_to_predicate(vec![b.clone()])
    );

    // A separate sequential scanner owns a fresh group and still translates B.
    let mut seq = SeqScan::new(cache_test_input(
        &env,
        object_store,
        &metadata,
        &file,
        cache,
    ));
    let applier = seq
        .pruner_for_test()
        .dynamic_inverted_applier_for_test()
        .unwrap()
        .clone();
    assert_eq!(vec![true], seq.add_dyn_filter_to_predicate(vec![b]));
    let rows = seq
        .build_stream()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let rows: Vec<_> = rows
        .into_iter()
        .map(|batch| batch.into_df_record_batch())
        .collect();
    assert_eq!(vec![2; 10], cache_test_tags(&rows));
    let stats = applier.stats();
    assert_eq!(1, stats.translations, "{stats:?}");
    assert_eq!(1, stats.applied_files, "{stats:?}");
}

#[tokio::test]
async fn test_dynamic_index_noncacheable_late_install_and_gate_race() {
    let (env, object_store, file) = single_file_env().await;
    let metadata = test_metadata();
    let uncached = Arc::new(CacheManager::default());
    let context = cache_test_context(&env, object_store.clone(), &metadata, &file, uncached);
    let range = context.partition_ranges()[0];
    let partition = cache_test_partition(&context, &range);
    assert!(build_range_cache_key(&context, &range, Some(&partition)).is_none());
    assert_eq!(
        vec![true],
        context.add_dyn_filter_to_predicate(vec![dyn_handle(phys_eq(
            "tag",
            ScalarValue::Int64(Some(1)),
        ))])
    );
    let (rows, metrics) = cache_test_read(&context, &range, partition).await;
    assert_eq!(vec![1; 10], cache_test_tags(&rows));
    assert!(format!("{metrics:?}").contains("\"rows_dynamic_inverted_filtered\":"));

    // Competing threads start together: an install winning the mutex prevents a
    // key; admission winning it rejects the install. Existing filters are retained.
    let cache = Arc::new(
        CacheManager::builder()
            .range_result_cache_size(1024 * 1024)
            .build(),
    );
    for _ in 0..64 {
        let context =
            cache_test_context(&env, object_store.clone(), &metadata, &file, cache.clone());
        let range = context.partition_ranges()[0];
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let installer = context.clone();
        let installer_barrier = barrier.clone();
        let install = std::thread::spawn(move || {
            installer_barrier.wait();
            installer.add_dyn_filter_to_predicate(vec![dyn_handle(phys_eq(
                "tag",
                ScalarValue::Int64(Some(1)),
            ))])[0]
        });
        let admission = context.clone();
        let admission_barrier = barrier.clone();
        let key = std::thread::spawn(move || {
            admission_barrier.wait();
            build_range_cache_key(&admission, &range, None).is_some()
        });
        barrier.wait();
        let installed = install.join().unwrap();
        let admitted = key.join().unwrap();
        assert_ne!(installed, admitted);
        assert_eq!(
            installed,
            !context
                .input
                .predicate_group()
                .predicate()
                .unwrap()
                .dyn_filters()
                .is_empty()
        );
    }
}

#[tokio::test]
async fn test_dynamic_index_projected_column_binding_declines() {
    let mut env = TestEnv::new().await;
    let object_store = env.init_object_store_manager();
    let metadata = Arc::new(sst_region_metadata());
    let first = new_record_batch_with_custom_sequence(&["z", "x"], 0, 1, 1);
    let second = new_record_batch_with_custom_sequence(&["x", "z"], 1, 2, 1);
    let file = write_sst(
        &env,
        object_store.clone(),
        metadata.clone(),
        vec![first.clone(), second.clone()],
        2,
        true,
    )
    .await;
    let cache = test_cache();
    let predicate = Predicate::new(vec![]);
    // Display name says tag_0, but index 1 actually refers to tag_1.
    let mismatched: Arc<dyn PhysicalExpr> = Arc::new(PhysicalColumn::new("tag_0", 1));
    let expr: Arc<dyn PhysicalExpr> = Arc::new(PhysicalBinaryExpr::new(
        mismatched.clone(),
        Operator::Eq,
        lit("x"),
    ));
    let handle = Arc::new(DynamicFilterPhysicalExpr::new(vec![mismatched], expr));
    predicate.add_dyn_filters(vec![handle.clone()]);
    let projected_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("tag_0", ArrowDataType::Utf8, true),
        Field::new("tag_1", ArrowDataType::Utf8, true),
    ]));
    let applier = Arc::new(DynamicInvertedIndexApplier::new(
        DynamicInvertedIndexConfig {
            table_dir: FILE_DIR.to_string(),
            path_type: PathType::Bare,
            store: object_store.clone(),
            puffin_manager_factory: env.get_puffin_manager(),
            file_cache: None,
            inverted_index_cache: cache.inverted_index_cache().cloned(),
            puffin_metadata_cache: cache.puffin_metadata_cache().cloned(),
            indexed_column_ids: indexed_column_ids(&metadata),
            metadata: metadata.clone(),
            output_schema: projected_schema.clone(),
            predicate: predicate.clone(),
        },
    ));
    let format = FlatReadFormat::new(
        metadata.clone(),
        ReadColumns::new(metadata.column_metadatas.iter().map(|c| c.column_id)),
        None,
        "test",
        false,
    )
    .unwrap();
    assert!(
        applier
            .apply(
                &format,
                &file.parquet_meta,
                file.handle.index_id(),
                Some(file.index_file_size),
                None
            )
            .await
            .is_none()
    );
    assert_eq!(1, applier.stats().declined_files);
    assert_eq!(0, applier.stats().translations);

    // Physical evaluation retains the row whose second tag is x, despite its
    // first tag being z; a name-based index lookup would lose precisely this row.
    let batch = RecordBatch::try_new(
        projected_schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["z", "x"])),
            Arc::new(StringArray::from(vec!["x", "z"])),
        ],
    )
    .unwrap();
    let mask = handle
        .evaluate(&batch)
        .unwrap()
        .into_array(batch.num_rows())
        .unwrap();
    let selected = datatypes::arrow::compute::filter_record_batch(
        &batch,
        mask.as_any()
            .downcast_ref::<datatypes::arrow::array::BooleanArray>()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        "z",
        selected
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
    );
    assert_eq!(
        "x",
        selected
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
    );
    assert_eq!(1, selected.num_rows());

    // Declared columns that bind correctly do not excuse an invalid column in
    // the publication, and out-of-bounds projected indexes decline as well.
    let valid: Arc<dyn PhysicalExpr> = Arc::new(PhysicalColumn::new("tag_0", 0));
    let wrong: Arc<dyn PhysicalExpr> = Arc::new(PhysicalColumn::new("tag_0", 1));
    let wrong_expr: Arc<dyn PhysicalExpr> =
        Arc::new(PhysicalBinaryExpr::new(wrong, Operator::Eq, lit("x")));
    let wrong_publication = DynamicFilterPhysicalExpr::new(vec![valid], wrong_expr.clone());
    assert!(
        ApprovedDynamicFilter::new(&wrong_publication, &wrong_expr, &projected_schema).is_none()
    );
    let out_of_bounds: Arc<dyn PhysicalExpr> = Arc::new(PhysicalColumn::new("tag_0", 2));
    let out_expr: Arc<dyn PhysicalExpr> = Arc::new(PhysicalBinaryExpr::new(
        out_of_bounds.clone(),
        Operator::Eq,
        lit("x"),
    ));
    let out_handle = DynamicFilterPhysicalExpr::new(vec![out_of_bounds], out_expr.clone());
    assert!(ApprovedDynamicFilter::new(&out_handle, &out_expr, &projected_schema).is_none());

    // Exercise the actual reader as well, isolating the inverted-index path.
    let builder = ParquetReaderBuilder::new(
        FILE_DIR.to_string(),
        PathType::Bare,
        file.handle.clone(),
        object_store,
    )
    .expected_metadata(Some(metadata))
    .dynamic_inverted_index_applier(Some(applier))
    .enable_predicate_prefilter(false);
    let mut metrics = ReaderMetrics::default();
    let (context, selection) = builder
        .build_reader_input(&mut metrics)
        .await
        .unwrap()
        .unwrap();
    let mut reader = ParquetReader::new(Arc::new(context), selection)
        .await
        .unwrap();
    let mut kept = 0;
    while let Some(batch) = reader.next_record_batch().await.unwrap() {
        kept += batch.num_rows();
    }
    assert_eq!(2, kept);
    assert_eq!(0, metrics.filter_metrics.rows_dynamic_inverted_filtered);
}

#[tokio::test]
async fn test_dynamic_index_live_handle() {
    let (env, object_store, file) = single_file_env().await;
    let metadata = test_metadata();
    let cache = test_cache();
    let format = test_read_format(&metadata);

    // The scan's static predicate is empty; the dynamic filter is added later.
    let predicate = Predicate::new(vec![]);
    let applier = dynamic_applier(
        &env,
        object_store.clone(),
        metadata.clone(),
        predicate.clone(),
        &cache,
    );

    // Nothing published yet: keep every row.
    assert!(
        applier
            .apply(
                &format,
                &file.parquet_meta,
                file.handle.index_id(),
                Some(file.index_file_size),
                None
            )
            .await
            .is_none()
    );
    assert_eq!(0, applier.stats().translations);
    assert_eq!(0, applier.stats().applied_files);

    // With no live filters, wiring the applier leaves the static rows unchanged
    // and does not translate or read an index.
    let baseline = read_file(
        object_store.clone(),
        metadata.clone(),
        &file,
        &cache,
        None,
        None,
        None,
        false,
    )
    .await;
    let empty = read_file(
        object_store.clone(),
        metadata.clone(),
        &file,
        &cache,
        None,
        Some(predicate.clone()),
        Some(applier.clone()),
        false,
    )
    .await;
    assert_eq!(baseline.rows, empty.rows);
    assert_eq!(0, applier.stats().translations);
    assert!(
        empty
            .filter_metrics
            .dynamic_inverted_index_apply_metrics
            .as_ref()
            .unwrap()
            .is_empty()
    );

    // A filter added after construction is visible at the next file read.
    let handle = dyn_handle(phys_in_list("tag", int64_list(&[1, 2]), false));
    predicate.add_dyn_filters(vec![handle.clone()]);
    let output = applier
        .apply(
            &format,
            &file.parquet_meta,
            file.handle.index_id(),
            Some(file.index_file_size),
            None,
        )
        .await
        .unwrap_or_else(|| panic!("the dynamic index prunes the file: {:?}", applier.stats()));
    assert_eq!(20, matched_rows(&file, output));
    assert_eq!(1, applier.stats().translations);
    assert_eq!(1, applier.stats().applied_files);

    // Same generation: the translation is reused.
    let generation = applier.generation_for_test();
    let before = applier.stats().translations;
    let output = applier
        .apply(
            &format,
            &file.parquet_meta,
            file.handle.index_id(),
            Some(file.index_file_size),
            None,
        )
        .await
        .expect("same generation still prunes");
    assert_eq!(20, matched_rows(&file, output));
    assert_eq!(before, applier.stats().translations);
    assert_eq!(generation, applier.generation_for_test());

    // A generation change retranslates and prunes with the new expression.
    handle
        .update(phys_in_list("tag", int64_list(&[3]), false))
        .unwrap();
    let output = applier
        .apply(
            &format,
            &file.parquet_meta,
            file.handle.index_id(),
            Some(file.index_file_size),
            None,
        )
        .await
        .expect("the new generation prunes");
    assert_eq!(10, matched_rows(&file, output));
    assert_eq!(before + 1, applier.stats().translations);

    // Appending a new same-generation handle changes the ArcSwap vector identity.
    // The new conjunct must be included rather than reusing the old plan.
    predicate.add_dyn_filters(vec![dyn_handle(phys_in_list(
        "tag",
        int64_list(&[4]),
        false,
    ))]);
    let output = applier
        .apply(
            &format,
            &file.parquet_meta,
            file.handle.index_id(),
            Some(file.index_file_size),
            None,
        )
        .await
        .expect("appended handle is translated");
    assert_eq!(0, matched_rows(&file, output));
    assert_eq!(before + 2, applier.stats().translations);
}

#[tokio::test]
async fn test_dynamic_index_single_flight_and_build_failure() {
    let (env, object_store, file) = single_file_env().await;
    let metadata = test_metadata();
    let cache = test_cache();
    let format = test_read_format(&metadata);
    let predicate = Predicate::new(vec![]);
    let handle = dyn_handle(phys_in_list("tag", int64_list(&[1, 2]), false));
    predicate.add_dyn_filters(vec![handle.clone()]);
    let applier = dynamic_applier(
        &env,
        object_store.clone(),
        metadata.clone(),
        predicate.clone(),
        &cache,
    );
    let run_concurrent = |applier: Arc<DynamicInvertedIndexApplier>| {
        let format = &format;
        let file = &file;
        async move {
            futures::future::join_all((0..16).map(|_| async {
                applier
                    .apply(
                        format,
                        &file.parquet_meta,
                        file.handle.index_id(),
                        Some(file.index_file_size),
                        None,
                    )
                    .await
            }))
            .await
        }
    };
    let results = run_concurrent(applier.clone()).await;
    assert!(
        results
            .into_iter()
            .all(|result| matched_rows(&file, result.unwrap()) == 20)
    );
    assert_eq!(applier.stats().builds, 1);
    assert_eq!(applier.stats().translations, 1);

    // Updating a handle and appending another handle each produces one build,
    // despite the concurrent readers observing the same publication.
    handle
        .update(phys_in_list("tag", int64_list(&[1]), false))
        .unwrap();
    let results = run_concurrent(applier.clone()).await;
    assert!(
        results
            .into_iter()
            .all(|result| matched_rows(&file, result.unwrap()) == 10)
    );
    assert_eq!(applier.stats().builds, 2);
    predicate.add_dyn_filters(vec![dyn_handle(phys_in_list(
        "tag",
        int64_list(&[1]),
        false,
    ))]);
    let results = run_concurrent(applier.clone()).await;
    assert!(
        results
            .into_iter()
            .all(|result| matched_rows(&file, result.unwrap()) == 10)
    );
    assert_eq!(applier.stats().builds, 3);

    // A failed build is shared and keeps the static selection unchanged. A new
    // generation retries once instead of poisoning the whole execution.
    handle
        .update(phys_in_list("tag", int64_list(&[1, 2]), false))
        .unwrap();
    applier
        .counters
        .fail_next_build
        .store(true, Ordering::Relaxed);
    let baseline = read_file(
        object_store.clone(),
        metadata.clone(),
        &file,
        &cache,
        None,
        None,
        None,
        false,
    )
    .await;
    let failed = read_file(
        object_store.clone(),
        metadata.clone(),
        &file,
        &cache,
        None,
        None,
        Some(applier.clone()),
        false,
    )
    .await;
    assert_eq!(baseline.rows, failed.rows);
    let results = run_concurrent(applier.clone()).await;
    assert!(results.into_iter().all(|result| result.is_none()));
    assert_eq!(applier.stats().builds, 4);
    assert_eq!(applier.stats().failed_files, 17);
    handle
        .update(phys_in_list("tag", int64_list(&[1]), false))
        .unwrap();
    let results = run_concurrent(applier.clone()).await;
    assert!(
        results
            .into_iter()
            .all(|result| matched_rows(&file, result.unwrap()) == 10)
    );
    assert_eq!(applier.stats().builds, 5);

    predicate.clear_dyn_filters();
    predicate.add_dyn_filters(vec![dyn_handle(phys_in_list(
        "tag",
        int64_list(&[2]),
        false,
    ))]);
    let results = run_concurrent(applier.clone()).await;
    assert!(results.into_iter().all(|result| result.is_none()));
    assert_eq!(applier.stats().builds, 5);
    let fresh = dynamic_applier(&env, object_store, metadata, predicate, &cache);
    let results = run_concurrent(fresh.clone()).await;
    assert!(
        results
            .into_iter()
            .all(|result| matched_rows(&file, result.unwrap()) == 10)
    );
    assert_eq!(fresh.stats().builds, 1);
}

#[tokio::test]
async fn test_dynamic_index_reset_stops_pruning() {
    let (env, object_store, file) = single_file_env().await;
    let metadata = test_metadata();
    let cache = test_cache();
    let format = test_read_format(&metadata);

    let predicate = Predicate::new(vec![]);
    let handle_a = dyn_handle(phys_in_list("tag", int64_list(&[1, 2]), false));
    predicate.add_dyn_filters(vec![handle_a.clone()]);
    let applier = dynamic_applier(
        &env,
        object_store.clone(),
        metadata.clone(),
        predicate.clone(),
        &cache,
    );

    let output = applier
        .apply(
            &format,
            &file.parquet_meta,
            file.handle.index_id(),
            Some(file.index_file_size),
            None,
        )
        .await
        .expect("the first file prunes");
    assert_eq!(20, matched_rows(&file, output));

    // Clear and re-add before the next observation, using a new handle at the same
    // generation. The old execution must decline rather than reuse A's cached plan.
    predicate.clear_dyn_filters();
    let handle_b = dyn_handle(phys_in_list("tag", int64_list(&[3]), false));
    assert_eq!(
        handle_a.snapshot_generation(),
        handle_b.snapshot_generation()
    );
    predicate.add_dyn_filters(vec![handle_b]);
    assert!(
        applier
            .apply(
                &format,
                &file.parquet_meta,
                file.handle.index_id(),
                Some(file.index_file_size),
                None
            )
            .await
            .is_none()
    );
    assert_eq!(1, applier.stats().reset_disabled_files);
    assert_eq!(1, applier.stats().declined_files);
    assert_eq!(1, applier.stats().translations);

    let static_predicate = Predicate::new(vec![
        col("tag").gt_eq(Expr::Literal(ScalarValue::Int64(Some(5)), None)),
    ]);
    let static_index = static_applier(
        &env,
        object_store.clone(),
        metadata.clone(),
        static_predicate.exprs(),
        &cache,
    );
    let baseline = read_file(
        object_store.clone(),
        metadata.clone(),
        &file,
        &cache,
        static_index.clone(),
        Some(static_predicate.clone()),
        None,
        false,
    )
    .await;
    let after_reset = read_file(
        object_store.clone(),
        metadata.clone(),
        &file,
        &cache,
        static_index,
        Some(static_predicate),
        Some(applier.clone()),
        false,
    )
    .await;
    assert_eq!(baseline.rows, after_reset.rows);
    assert_eq!(2, applier.stats().reset_disabled_files);

    // A new execution owns a fresh applier and therefore translates B.
    let fresh = dynamic_applier(&env, object_store, metadata, predicate, &cache);
    let output = fresh
        .apply(
            &format,
            &file.parquet_meta,
            file.handle.index_id(),
            Some(file.index_file_size),
            None,
        )
        .await
        .expect("fresh execution uses B");
    assert_eq!(10, matched_rows(&file, output));
    assert_eq!(1, fresh.stats().translations);
}

#[tokio::test]
async fn test_dynamic_index_fail_open() {
    let mut env = TestEnv::new().await;
    let object_store = env.init_object_store_manager();
    let metadata = test_metadata();
    let rows: Vec<(i64, String)> = (0..100).map(|i| (i % 10, format!("p{i}"))).collect();
    let batch = new_batch(0, &rows);
    let indexed_file = write_sst(
        &env,
        object_store.clone(),
        metadata.clone(),
        vec![batch.clone()],
        100,
        true,
    )
    .await;
    let unindexed_file = write_sst(
        &env,
        object_store.clone(),
        unindexed_metadata(),
        vec![batch],
        100,
        false,
    )
    .await;
    assert!(!unindexed_file.handle.meta_ref().inverted_index_available());

    let cache = test_cache();
    let predicate = Predicate::new(vec![]);
    predicate.add_dyn_filters(vec![dyn_handle(phys_in_list(
        "tag",
        int64_list(&[1, 2]),
        false,
    ))]);
    let applier = dynamic_applier(
        &env,
        object_store.clone(),
        metadata.clone(),
        predicate,
        &cache,
    );
    let format = test_read_format(&metadata);

    // Unknown index in this file: keep every row, never panic.
    let unindexed_format = test_read_format(&unindexed_metadata());
    assert!(
        applier
            .apply(
                &unindexed_format,
                &unindexed_file.parquet_meta,
                unindexed_file.handle.index_id(),
                Some(unindexed_file.index_file_size),
                None,
            )
            .await
            .is_none()
    );

    // The indexed file still prunes.
    assert!(
        applier
            .apply(
                &format,
                &indexed_file.parquet_meta,
                indexed_file.handle.index_id(),
                Some(indexed_file.index_file_size),
                None,
            )
            .await
            .is_some()
    );

    // A renamed column in the file is declined.
    let mut renamed = (*metadata).clone();
    renamed.column_metadatas[0].column_schema = ColumnSchema::new(
        "tag_old".to_string(),
        ConcreteDataType::int64_datatype(),
        false,
    )
    .with_inverted_index(true);
    let renamed_metadata = std::sync::Arc::new(renamed);
    assert!(
        applier
            .apply(
                &test_read_format(&renamed_metadata),
                &indexed_file.parquet_meta,
                indexed_file.handle.index_id(),
                Some(indexed_file.index_file_size),
                None,
            )
            .await
            .is_none()
    );

    // Type drift between the latest metadata and the file is declined.
    let mut drifted = (*metadata).clone();
    drifted.column_metadatas[0].column_schema =
        ColumnSchema::new("tag".to_string(), ConcreteDataType::int32_datatype(), false)
            .with_inverted_index(true);
    let drifted_metadata = std::sync::Arc::new(drifted);
    assert!(
        applier
            .apply(
                &test_read_format(&drifted_metadata),
                &indexed_file.parquet_meta,
                indexed_file.handle.index_id(),
                Some(indexed_file.index_file_size),
                None,
            )
            .await
            .is_none()
    );
}

// ---------------------------------------------------------------------------
// Measurement
// ---------------------------------------------------------------------------

const MEASURE_ROWS: usize = 4_000;
const MEASURE_FILES: usize = 2;
const MEASURE_KEYS: i64 = 2_000;
const STATIC_KEYS: i64 = 1_000;
const MEASURE_ROW_GROUP_SIZE: usize = 512;
const PAYLOAD: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const N_VALUES: [i64; 4] = [8, 32, 64, 642];

fn measure_rows(spread: bool) -> Vec<(i64, String)> {
    (0..MEASURE_ROWS)
        .map(|i| {
            let tag = if spread {
                // Spread the stored keys over the whole key space.
                (i as i64 * 7 + 13) % MEASURE_KEYS
            } else {
                (i as i64) % MEASURE_KEYS
            };
            (tag, PAYLOAD.to_string())
        })
        .collect()
}

fn static_predicate() -> Predicate {
    Predicate::new(vec![Expr::InList(datafusion_expr::expr::InList {
        expr: Box::new(col("tag")),
        list: (0..STATIC_KEYS)
            .map(|key| Expr::Literal(ScalarValue::Int64(Some(key)), None))
            .collect(),
        negated: false,
    })])
}

/// Runs one measurement arm over all files of a layout.
///
/// `dynamic` is the producer publication: `None` measures the static arm alone.
/// `use_index` attaches the live-handle index applier, otherwise the dynamic filter is only
/// visible to the parquet prefilter pass (the reader drops it there when the prefilter is
/// disabled, which is how the index arm is isolated).
async fn run_arm(
    env: &TestEnv,
    object_store: object_store::ObjectStore,
    metadata: std::sync::Arc<RegionMetadata>,
    files: &[WrittenFile],
    cache: &std::sync::Arc<CacheManager>,
    dynamic: Option<Arc<dyn PhysicalExpr>>,
    use_index: bool,
) -> ArmResult {
    let static_preds = static_predicate();
    let index_applier = static_applier(
        env,
        object_store.clone(),
        metadata.clone(),
        static_preds.exprs(),
        cache,
    );
    // One predicate per arm, i.e. one per scan.
    let predicate = Predicate::new(static_preds.exprs().to_vec());
    if let Some(expr) = &dynamic {
        predicate.add_dyn_filters(vec![dyn_handle(expr.clone())]);
    }
    let applier = if use_index {
        Some(dynamic_applier(
            env,
            object_store.clone(),
            metadata.clone(),
            predicate.clone(),
            cache,
        ))
    } else {
        None
    };

    let mut result = ArmResult {
        rows: Vec::new(),
        filter_metrics: Default::default(),
        fetch: ParquetFetchMetricsData::default(),
        elapsed_us: 0,
        index_apply_us: 0,
        index_blob_bytes: 0,
    };
    for file in files {
        let file_result = read_file(
            object_store.clone(),
            metadata.clone(),
            file,
            cache,
            index_applier.clone(),
            Some(predicate.clone()),
            applier.clone(),
            // The index arm isolates the index path: no parquet prefilter pass.
            !use_index,
        )
        .await;
        result.rows.extend(file_result.rows);
        result
            .filter_metrics
            .merge_from(&file_result.filter_metrics);
        result.fetch.page_size_to_fetch_store += file_result.fetch.page_size_to_fetch_store;
        result.fetch.pages_to_fetch_store += file_result.fetch.pages_to_fetch_store;
        result.fetch.prefilter_filtered_rows += file_result.fetch.prefilter_filtered_rows;
        result.fetch.prefilter_cost += file_result.fetch.prefilter_cost;
        result.fetch.total_fetch_elapsed += file_result.fetch.total_fetch_elapsed;
        result.elapsed_us += file_result.elapsed_us;
        result.index_apply_us += file_result.index_apply_us;
        result.index_blob_bytes += file_result.index_blob_bytes;
    }
    result.rows.sort_unstable();
    result
}

/// One measured dynamic predicate shape.
struct MeasureCase {
    /// Label printed in the table.
    label: String,
    /// The dynamic filter the producer publishes.
    expr: Arc<dyn PhysicalExpr>,
    /// Whether the case is also measured with a cold cache. A cold run re-reads every index
    /// blob per arm.
    cold: bool,
}

/// Builds membership cases for one layout, including a large list.
fn measure_cases() -> Vec<MeasureCase> {
    let mut cases = Vec::new();
    for n in N_VALUES {
        cases.push(MeasureCase {
            label: format!("IN N={n}"),
            expr: phys_in_list("tag", int64_list(&(0..n).collect::<Vec<_>>()), false),
            cold: true,
        });
    }
    cases.push(MeasureCase {
        label: "EQ".to_string(),
        expr: phys_eq("tag", ScalarValue::Int64(Some(7))),
        cold: true,
    });
    cases
}

#[tokio::test]
#[allow(clippy::print_stdout)]
async fn test_dynamic_index_measurement() {
    let mut env = TestEnv::new().await;
    let object_store = env.init_object_store_manager();
    let metadata = test_metadata();

    println!("\n=== live-handle dynamic inverted index measurement ===");
    println!(
        "rows={MEASURE_ROWS} files={MEASURE_FILES} keys={MEASURE_KEYS} static_keys={STATIC_KEYS} row_group_size={MEASURE_ROW_GROUP_SIZE}"
    );
    println!(
        "note: page_size_to_fetch_store is one counter shared by the prefilter pass and the projection pass"
    );
    println!(
        "note: rows_static/rows_dyn are per-step deltas of one selection and are not comparable across arms; the differential result rows are the correctness check"
    );
    println!(
        "note: index-warm measures every case; cold repeats all membership cases, because a cold run re-reads every index blob per arm"
    );

    for spread in [false, true] {
        let layout = if spread { "spread" } else { "contig" };
        let rows = measure_rows(spread);
        let per_file = MEASURE_ROWS / MEASURE_FILES;
        let mut files = Vec::with_capacity(MEASURE_FILES);
        for file_index in 0..MEASURE_FILES {
            let chunk = rows[file_index * per_file..(file_index + 1) * per_file].to_vec();
            files.push(
                write_sst(
                    &env,
                    object_store.clone(),
                    metadata.clone(),
                    vec![new_batch((file_index * per_file) as i64, &chunk)],
                    MEASURE_ROW_GROUP_SIZE,
                    true,
                )
                .await,
            );
        }
        let static_metadata = metadata.clone();

        // Filter-column bytes alone: the same static selection over a tag-only projection.
        let probe_cache = test_cache();
        let probe_static = static_predicate();
        let probe_applier = static_applier(
            &env,
            object_store.clone(),
            static_metadata.clone(),
            probe_static.exprs(),
            &probe_cache,
        );
        let (probe_bytes, probe_pages) = probe_tag_column_bytes(
            object_store.clone(),
            static_metadata.clone(),
            &files,
            probe_applier,
            probe_static,
        )
        .await;
        println!(
            "probe {layout}: tag-only projection with S: pages_store={probe_pages} bytes_store={probe_bytes}"
        );

        let cases = measure_cases();
        for (mode, warm_cache) in [("index-warm", Some(test_cache())), ("cold", None)] {
            let cache = match &warm_cache {
                Some(cache) => cache.clone(),
                None => test_cache(),
            };
            // The static arm does not depend on the dynamic predicate, so it runs once per
            // mode and layout instead of once per case.
            let baseline = run_arm(
                &env,
                object_store.clone(),
                static_metadata.clone(),
                &files,
                &cache,
                None,
                false,
            )
            .await;

            for case in cases
                .iter()
                .filter(|case| warm_cache.is_some() || case.cold)
            {
                // Arm 1: the same predicate, pruned by the parquet prefilter only.
                let prefilter = run_arm(
                    &env,
                    object_store.clone(),
                    static_metadata.clone(),
                    &files,
                    &cache,
                    Some(case.expr.clone()),
                    false,
                )
                .await;
                // Arm 2: the same predicate, pruned by the live-handle inverted index.
                let index = run_arm(
                    &env,
                    object_store.clone(),
                    static_metadata.clone(),
                    &files,
                    &cache,
                    Some(case.expr.clone()),
                    true,
                )
                .await;

                assert!(
                    index
                        .filter_metrics
                        .dynamic_inverted_index_apply_metrics
                        .as_ref()
                        .is_some_and(|m| !m.is_empty()),
                    "{} must be applied by the inverted index",
                    case.label
                );
                // Correctness: the index arm returns exactly the same rows as the
                // parquet-only arm, and the static arm is a superset.
                assert_eq!(
                    prefilter.rows, index.rows,
                    "{mode}/{layout}/{}: the index path returned different rows",
                    case.label
                );
                assert!(
                    baseline.rows.len() >= prefilter.rows.len(),
                    "{mode}/{layout}/{}: S must keep at least the S+D rows",
                    case.label
                );

                let bytes_ratio = ratio(
                    index.fetch.page_size_to_fetch_store,
                    prefilter.fetch.page_size_to_fetch_store,
                );
                let wall_ratio = ratio(index.elapsed_us as u64, prefilter.elapsed_us as u64);
                for (label, result, ratio_text) in [
                    ("S", &baseline, String::from("-")),
                    ("S+D prefilter", &prefilter, String::from("-")),
                    (
                        "S+D index",
                        &index,
                        format!("bytes x{bytes_ratio:.2} wall x{wall_ratio:.2}"),
                    ),
                ] {
                    println!(
                        "{mode:<10} {layout} {:<16} {label:<14} rows={:<7} pages={:<5} bytes={:<9} prefilter_filtered={:<6} rows_static={:<7} rows_dyn={:<7} index_us={:<7} wall_us={:<8} vs_prefilter={ratio_text} index_blob_bytes={}",
                        case.label,
                        result.rows.len(),
                        result.fetch.pages_to_fetch_store,
                        result.fetch.page_size_to_fetch_store,
                        result.fetch.prefilter_filtered_rows,
                        result.filter_metrics.rows_inverted_filtered,
                        result.filter_metrics.rows_dynamic_inverted_filtered,
                        result.index_apply_us,
                        result.elapsed_us,
                        result.index_blob_bytes,
                    );
                }
            }
        }
    }
}

/// Returns `numerator / denominator`, or `0.0` for an empty denominator.
fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        return 0.0;
    }
    numerator as f64 / denominator as f64
}
