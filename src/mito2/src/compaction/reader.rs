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

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow_schema::extension::ExtensionType;
use common_time::Timestamp;
use common_time::range::TimestampRange;
use common_time::timestamp::TimeUnit;
use datafusion_common::ScalarValue;
use datafusion_expr::Expr;
use datatypes::arrow::array::ArrayRef;
use datatypes::arrow::datatypes::{DataType as ArrowDataType, Field, Schema, SchemaRef};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::extension::json::{
    JSON2_REMAINDER_FIELD_NAME, Json2ExtensionType, Json2PhysicalLayout, JsonMetadata,
    is_json2_extension_type,
};
use datatypes::json::{JsonSettings, JsonTypeHint};
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use datatypes::types::json_type::JsonNativeType;
use datatypes::vectors::json::array::JsonArray;
use futures::StreamExt;
use parquet::arrow::parquet_to_arrow_schema;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData};
use snafu::{OptionExt, ResultExt};
use store_api::metadata::RegionMetadataRef;

use crate::access_layer::AccessLayerRef;
use crate::cache::{CacheManagerRef, CacheStrategy};
use crate::error::{
    ConvertValueSnafu, DataTypeMismatchSnafu, InvalidRecordBatchSnafu, NewRecordBatchSnafu, Result,
    TimeRangePredicateOverflowSnafu,
};
use crate::read::FlatSource;
use crate::read::flat_projection::FlatProjectionMapper;
use crate::read::read_columns::ReadColumns;
use crate::read::scan_region::{PredicateGroup, ScanInput};
use crate::read::seq_scan::SeqScan;
use crate::region::options::MergeMode;
use crate::sst::file::FileHandle;
use crate::sst::parquet::reader::MetadataCacheMetrics;

/// Builders to create [BoxedRecordBatchStream] for compaction.
pub(crate) struct CompactionSstReaderBuilder<'a> {
    pub(crate) metadata: RegionMetadataRef,
    pub(crate) sst_layer: AccessLayerRef,
    pub(crate) cache: CacheManagerRef,
    pub(crate) inputs: &'a [FileHandle],
    pub(crate) append_mode: bool,
    pub(crate) filter_deleted: bool,
    pub(crate) time_range: Option<TimestampRange>,
    pub(crate) merge_mode: MergeMode,
}

impl CompactionSstReaderBuilder<'_> {
    /// Build a [FlatSource] that yields Arrow `RecordBatch`s from reading all the input SST files,
    /// for compaction. The schema of the [FlatSource] is unified.
    pub(crate) async fn build_flat_sst_reader(self) -> Result<FlatSource> {
        let parquet_metadata = self.collect_parquet_metadata().await?;
        let targets = json2_targets(&self.metadata, &parquet_metadata)?;
        let scan_input = self.build_scan_input(&parquet_metadata)?;

        let schema = scan_input.mapper.output_schema();
        let schema = rewrite_json2_schema(schema.arrow_schema(), &targets);

        let stream = SeqScan::new(scan_input)
            .build_flat_reader_for_compaction()
            .await?;
        if targets.is_empty() {
            return Ok(FlatSource::new_stream(schema, stream));
        }

        let targets = Arc::new(targets);
        let stream = stream
            .map(move |batch| batch.and_then(|batch| rewrite_json2_batch(batch, targets.as_ref())))
            .boxed();
        Ok(FlatSource::new_stream(schema, stream))
    }

    fn build_scan_input(self, parquet_metadata: &[Arc<ParquetMetaData>]) -> Result<ScanInput> {
        let schema = self.metadata.schema.arrow_schema();
        let batch_size = crate::batch_size::estimate_batch_size(
            parquet_metadata
                .iter()
                .flat_map(|metadata| metadata.row_groups())
                .map(|row_group| {
                    let uncompressed_bytes = row_group
                        .columns()
                        .iter()
                        .map(|column| column.uncompressed_size() as u64)
                        .sum();
                    (row_group.num_rows() as u64, uncompressed_bytes)
                }),
        );

        // Materialize every JSON2 column as complete Binary JSON so source-only explicit paths
        // survive until rewrite_json2_batch demotes them into the target v2 remainder.
        let json_type_hint = schema
            .fields()
            .iter()
            .any(is_json2_extension_type)
            .then(|| {
                schema
                    .fields()
                    .iter()
                    .filter(|&field| is_json2_extension_type(field))
                    .map(|field| (field.name().clone(), JsonNativeType::Variant))
                    .collect::<HashMap<_, _>>()
            });

        let projection = (0..self.metadata.column_metadatas.len()).collect();
        let read_column_ids = self
            .metadata
            .column_metadatas
            .iter()
            .map(|x| x.column_id)
            .collect::<Vec<_>>();
        let json_target_types = json_type_hint
            .as_ref()
            .map(|hint| {
                hint.iter()
                    .filter_map(|(col_name, json_type)| {
                        self.metadata
                            .column_by_name(col_name)
                            .map(|col| (col.column_id, json_type.clone()))
                    })
                    .collect::<BTreeMap<_, _>>()
            })
            .unwrap_or_default();
        let read_columns =
            ReadColumns::new(read_column_ids).with_json_target_types(json_target_types);
        let mapper =
            FlatProjectionMapper::new_with_read_columns(&self.metadata, projection, read_columns)?;

        let mut scan_input = ScanInput::new(self.sst_layer, mapper)
            .with_files(self.inputs.to_vec())
            .with_compaction(true)
            .with_batch_size(batch_size)
            .with_append_mode(self.append_mode)
            // We use special cache strategy for compaction.
            .with_cache(CacheStrategy::Compaction(self.cache))
            .with_filter_deleted(self.filter_deleted)
            // We ignore file not found error during compaction.
            .with_ignore_file_not_found(true)
            .with_merge_mode(self.merge_mode);

        // This serves as a workaround of https://github.com/GreptimeTeam/greptimedb/issues/3944
        // by converting time ranges into predicate.
        if let Some(time_range) = self.time_range {
            scan_input =
                scan_input.with_predicate(time_range_to_predicate(time_range, &self.metadata)?);
        }

        Ok(scan_input)
    }

    async fn collect_parquet_metadata(&self) -> Result<Vec<Arc<ParquetMetaData>>> {
        let mut metadata = Vec::with_capacity(self.inputs.len());

        for file_handle in self.inputs {
            let file_path =
                file_handle.file_path(self.sst_layer.table_dir(), self.sst_layer.path_type());
            let file_size = file_handle.meta_ref().file_size;
            let parquet_metadata = match self
                .sst_layer
                .read_sst(file_handle.clone())
                .cache(CacheStrategy::Compaction(self.cache.clone()))
                .read_parquet_metadata(
                    &file_path,
                    file_size,
                    &mut MetadataCacheMetrics::default(),
                    PageIndexPolicy::default(),
                )
                .await
                .map(|x| x.0.parquet_metadata())
            {
                Ok(x) => x,
                Err(e) if e.is_object_not_found() => continue,
                Err(e) => return Err(e),
            };
            metadata.push(parquet_metadata);
        }
        Ok(metadata)
    }
}

/// The target physical layout of one JSON2 column in compaction output.
///
/// A compaction input may contain v1 and v2 SSTs with different physical schemas. This target is
/// derived only from the current region metadata and remains fixed while all input batches are
/// decoded and rewritten. It therefore prevents source-only paths from expanding the output
/// schema without a bound.
pub(crate) struct Json2Target {
    /// The column schema carrying temporary settings for the fixed target layout.
    ///
    /// Selected dynamic paths are represented as temporary type hints and automatic expansion is
    /// disabled, so every output batch is encoded with exactly the same physical schema. The
    /// persisted Arrow field keeps the original region extension metadata.
    schema: ColumnSchema,
    /// The fixed JSON encoding settings used by the target builder.
    ///
    /// The settings are retained separately because source arrays are first decoded into logical
    /// JSON values and then encoded according to the target column policy.
    settings: JsonSettings,
    /// The Arrow data type produced by the target vector builder.
    ///
    /// Compaction uses this type for its output schema and verifies that rewritten arrays keep the
    /// same physical layout across batches.
    data_type: ArrowDataType,
}

/// Fixed JSON2 output layouts keyed by logical column name.
pub(crate) type Json2Targets = HashMap<String, Json2Target>;

#[derive(Clone)]
struct Json2PathStats {
    rows: u64,
    data_type: JsonNativeType,
}

/// Builds the fixed JSON2 output targets for a compaction.
///
/// Type hints from current region metadata are always retained. Existing explicit dynamic paths
/// from all input SST schemas are ranked once to produce a fixed target; paths found only in a v2
/// remainder are deliberately not promoted. [`rewrite_json2_batch`] decodes inputs and rewrites
/// them into these targets. Non-JSON2 columns are omitted from the returned map.
///
/// Returns an error when a JSON2 column has invalid or missing extension metadata, or when its
/// target is not layout v2. Legacy region metadata is upgraded in memory by the region opener
/// before compaction reaches this function.
fn json2_targets(
    metadata: &RegionMetadataRef,
    parquet_metadata: &[Arc<ParquetMetaData>],
) -> Result<Json2Targets> {
    let source_schemas = parquet_metadata
        .iter()
        .map(|metadata| {
            let file = metadata.file_metadata();
            let schema = parquet_to_arrow_schema(file.schema_descr(), file.key_value_metadata())
                .map_err(|error| {
                    InvalidRecordBatchSnafu {
                        reason: format!("failed to read compaction input Arrow schema: {error}"),
                    }
                    .build()
                })?;
            let rows = metadata
                .row_groups()
                .iter()
                .map(|x| x.num_rows() as u64)
                .sum();
            // Nested and list leaf statistics do not map uniformly to logical row validity.
            // Weight an explicit path by its SST row count; its source builder already selected
            // it by non-null frequency within the original write batch.
            Ok((schema, rows))
        })
        .collect::<Result<Vec<_>>>()?;

    json2_targets_from_schemas(metadata, &source_schemas)
}

/// Builds fixed JSON2 output targets from existing physical schemas.
///
/// Each source row count weights all of its existing explicit leaves. Ordinary paths are never
/// discovered from the v2 remainder, so this operation cannot unexpectedly promote opaque data.
pub(crate) fn json2_targets_from_schemas(
    metadata: &RegionMetadataRef,
    source_schemas: &[(Schema, u64)],
) -> Result<Json2Targets> {
    metadata
        .column_metadatas
        .iter()
        .filter(|x| x.column_schema.data_type.is_json2())
        .map(|column| {
            let schema = &column.column_schema;
            let extension = schema
                .extension_type::<Json2ExtensionType>()
                .context(DataTypeMismatchSnafu)?
                .with_context(|| InvalidRecordBatchSnafu {
                    reason: format!(
                        "JSON2 target column '{}' has no extension metadata",
                        schema.name
                    ),
                })?;
            if !extension.metadata().is_version_2() {
                return InvalidRecordBatchSnafu {
                    reason: format!("JSON2 target column '{}' is not layout v2", schema.name),
                }
                .fail();
            }

            let settings = extension.metadata().json_settings();
            let mut stats = BTreeMap::new();
            for (source, rows) in source_schemas {
                let Ok(index) = source.index_of(&schema.name) else {
                    continue;
                };
                collect_json2_path_stats(source.field(index), *rows, settings, &mut stats)?;
            }

            let mut hints = settings.type_hints().to_vec();
            hints.extend(select_dynamic_hints(settings, &stats));
            let settings = JsonSettings::try_new(hints)
                .context(DataTypeMismatchSnafu)?
                .with_max_auto_expanded_paths(0);
            let extension = Json2ExtensionType::new(Arc::new(JsonMetadata::new(settings.clone())));
            let mut schema = schema.clone();
            schema.with_extension_type(&extension);
            let mut builder = schema.create_mutable_vector(0);
            let data_type = builder.to_vector().to_arrow_array().data_type().clone();
            Ok((
                schema.name.clone(),
                Json2Target {
                    schema,
                    settings,
                    data_type,
                },
            ))
        })
        .collect()
}

fn collect_json2_path_stats(
    field: &Field,
    rows: u64,
    settings: &JsonSettings,
    stats: &mut BTreeMap<Vec<String>, Json2PathStats>,
) -> Result<()> {
    let layout = Json2PhysicalLayout::try_from_root(field).context(DataTypeMismatchSnafu)?;
    let ArrowDataType::Struct(fields) = field.data_type() else {
        return InvalidRecordBatchSnafu {
            reason: format!("JSON2 source column '{}' is not a struct", field.name()),
        }
        .fail();
    };
    let mut paths = Vec::new();
    for field in fields {
        if layout.is_version_2() && field.name() == JSON2_REMAINDER_FIELD_NAME {
            continue;
        }
        collect_explicit_leaf_types(field, &mut Vec::new(), &mut paths)?;
    }

    for (path, data_type) in paths {
        if settings.type_hints().iter().any(|x| x.path == path) {
            continue;
        }
        let entry = stats.entry(path).or_insert_with(|| Json2PathStats {
            rows: 0,
            data_type: JsonNativeType::Null,
        });
        entry.rows += rows;
        entry.data_type.merge(&data_type);
    }
    Ok(())
}

fn collect_explicit_leaf_types(
    field: &Field,
    path: &mut Vec<String>,
    paths: &mut Vec<(Vec<String>, JsonNativeType)>,
) -> Result<()> {
    path.push(field.name().clone());
    if let ArrowDataType::Struct(fields) = field.data_type()
        && !fields.is_empty()
    {
        for field in fields {
            collect_explicit_leaf_types(field, path, paths)?;
        }
    } else {
        paths.push((
            path.clone(),
            JsonNativeType::try_from(field.data_type()).context(DataTypeMismatchSnafu)?,
        ));
    }
    path.pop();
    Ok(())
}

fn select_dynamic_hints(
    settings: &JsonSettings,
    stats: &BTreeMap<Vec<String>, Json2PathStats>,
) -> Vec<JsonTypeHint> {
    let mut candidates = stats
        .iter()
        .filter(|(path, stat)| {
            !matches!(
                stat.data_type,
                JsonNativeType::Null | JsonNativeType::Variant
            ) && !stats.keys().any(|other| paths_conflict(path, other))
                && !settings
                    .type_hints()
                    .iter()
                    .any(|hint| paths_conflict(path, &hint.path))
        })
        .collect::<Vec<_>>();
    candidates.sort_unstable_by(|(x_path, x), (y_path, y)| {
        y.rows.cmp(&x.rows).then_with(|| x_path.cmp(y_path))
    });
    candidates
        .into_iter()
        .take(settings.max_auto_expanded_paths() as usize)
        .map(|(path, stat)| JsonTypeHint {
            path: path.clone(),
            data_type: ConcreteDataType::from_arrow_type(&stat.data_type.as_arrow_type()),
            nullable: true,
            default_constraint: None,
            inverted_index: false,
        })
        .collect()
}

fn paths_conflict(x: &[String], y: &[String]) -> bool {
    x != y && (x.starts_with(y) || y.starts_with(x))
}

/// Replaces JSON2 physical field types with their fixed output targets.
pub(crate) fn rewrite_json2_schema(schema: &SchemaRef, targets: &Json2Targets) -> SchemaRef {
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            let Some(target) = targets.get(field.name()) else {
                return field.clone();
            };
            let mut field = Field::clone(field);
            field.set_data_type(target.data_type.clone());
            Arc::new(field)
        })
        .collect::<Vec<_>>();
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

/// Rewrites JSON2 columns in `batch` into fixed output targets.
pub(crate) fn rewrite_json2_batch(
    batch: RecordBatch,
    targets: &Json2Targets,
) -> Result<RecordBatch> {
    let mut fields = Vec::with_capacity(batch.num_columns());
    let mut columns = Vec::with_capacity(batch.num_columns());

    for (field, array) in batch.schema_ref().fields().iter().zip(batch.columns()) {
        let Some(target) = targets.get(field.name()) else {
            fields.push(field.clone());
            columns.push(array.clone());
            continue;
        };

        let mut builder = target.schema.create_mutable_vector(array.len());
        // SST compaction normally materializes JSON2 as Binary before this rewrite, while bulk
        // memtable compaction passes the original v2 Struct. Reconstruct the latter first so its
        // Variant remainder is merged with explicit children instead of decoded as plain JSONB.
        let projected = if matches!(array.data_type(), ArrowDataType::Struct(_))
            && Json2PhysicalLayout::try_from_root(field)
                .context(DataTypeMismatchSnafu)?
                .is_version_2()
        {
            Some(
                JsonArray::from(array)
                    .project_json2(field, &ArrowDataType::Binary)
                    .context(ConvertValueSnafu)?,
            )
        } else {
            None
        };
        let json = JsonArray::from(projected.as_ref().unwrap_or(array));
        for i in 0..array.len() {
            let value = json.try_get_value(i).context(ConvertValueSnafu)?;
            if value.is_null() {
                builder.push_null();
            } else {
                let value = target.settings.encode(value).context(ConvertValueSnafu)?;
                builder
                    .try_push_value_ref(&value.as_value_ref())
                    .context(ConvertValueSnafu)?;
            }
        }
        let array: ArrayRef = builder.to_vector().to_arrow_array();
        debug_assert_eq!(&target.data_type, array.data_type());

        let mut field = Field::clone(field);
        field.set_data_type(target.data_type.clone());
        fields.push(Arc::new(field));
        columns.push(array);
    }

    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        batch.schema_ref().metadata().clone(),
    ));
    RecordBatch::try_new(schema, columns).context(NewRecordBatchSnafu)
}

/// Converts time range to predicates so that rows outside the range will be filtered.
fn time_range_to_predicate(
    range: TimestampRange,
    metadata: &RegionMetadataRef,
) -> Result<PredicateGroup> {
    let ts_col = metadata.time_index_column();

    // safety: time index column's type must be a valid timestamp type.
    let ts_col_unit = ts_col
        .column_schema
        .data_type
        .as_timestamp()
        .unwrap()
        .unit();

    let exprs = match (range.start(), range.end()) {
        (Some(start), Some(end)) => {
            vec![
                datafusion_expr::col(ts_col.column_schema.name.clone())
                    .gt_eq(ts_to_lit(*start, ts_col_unit)?),
                datafusion_expr::col(ts_col.column_schema.name.clone())
                    .lt(ts_to_lit(*end, ts_col_unit)?),
            ]
        }
        (Some(start), None) => {
            vec![
                datafusion_expr::col(ts_col.column_schema.name.clone())
                    .gt_eq(ts_to_lit(*start, ts_col_unit)?),
            ]
        }

        (None, Some(end)) => {
            vec![
                datafusion_expr::col(ts_col.column_schema.name.clone())
                    .lt(ts_to_lit(*end, ts_col_unit)?),
            ]
        }
        (None, None) => {
            return Ok(PredicateGroup::default());
        }
    };

    let predicate = PredicateGroup::new(metadata, &exprs)?;
    Ok(predicate)
}

fn ts_to_lit(ts: Timestamp, ts_col_unit: TimeUnit) -> Result<Expr> {
    let ts = ts
        .convert_to(ts_col_unit)
        .context(TimeRangePredicateOverflowSnafu {
            timestamp: ts,
            unit: ts_col_unit,
        })?;
    let val = ts.value();
    let scalar_value = match ts_col_unit {
        TimeUnit::Second => ScalarValue::TimestampSecond(Some(val), None),
        TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(Some(val), None),
        TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(Some(val), None),
        TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(Some(val), None),
    };
    Ok(datafusion_expr::lit(scalar_value))
}

#[cfg(test)]
mod tests {
    use datatypes::extension::json::{
        JSON2_REMAINDER_FIELD_NAME, Json2PhysicalLayout, JsonMetadata,
    };
    use datatypes::json::JsonTypeHint;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::types::json_type::{JsonNativeType, JsonObjectType};
    use serde_json::json;

    use super::*;

    #[test]
    fn test_select_dynamic_hints_rejects_type_and_prefix_conflicts()
    -> Result<(), Box<dyn std::error::Error>> {
        let settings = JsonSettings::try_new(vec![JsonTypeHint {
            path: vec!["hint".to_string()],
            data_type: ConcreteDataType::string_datatype(),
            nullable: true,
            default_constraint: None,
            inverted_index: false,
        }])?
        .with_max_auto_expanded_paths(2);
        let stats = BTreeMap::from([
            (
                vec!["conflict".to_string()],
                Json2PathStats {
                    rows: 3,
                    data_type: JsonNativeType::Variant,
                },
            ),
            (
                vec!["popular".to_string()],
                Json2PathStats {
                    rows: 1,
                    data_type: JsonNativeType::String,
                },
            ),
            (
                vec!["popular".to_string(), "nested".to_string()],
                Json2PathStats {
                    rows: 2,
                    data_type: JsonNativeType::u64(),
                },
            ),
            (
                vec!["rare".to_string()],
                Json2PathStats {
                    rows: 1,
                    data_type: JsonNativeType::u64(),
                },
            ),
            (
                vec!["tie_a".to_string()],
                Json2PathStats {
                    rows: 2,
                    data_type: JsonNativeType::String,
                },
            ),
            (
                vec!["tie_b".to_string()],
                Json2PathStats {
                    rows: 2,
                    data_type: JsonNativeType::Bool,
                },
            ),
        ]);

        let hints = select_dynamic_hints(&settings, &stats);
        assert_eq!(
            vec![vec!["tie_a".to_string()], vec!["tie_b".to_string()]],
            hints.into_iter().map(|x| x.path).collect::<Vec<_>>()
        );
        Ok(())
    }

    #[test]
    fn test_rewrite_json2_v1_batch_to_target_layout() -> Result<(), Box<dyn std::error::Error>> {
        let settings = JsonSettings::try_new(vec![JsonTypeHint {
            path: vec!["kind".to_string()],
            data_type: ConcreteDataType::string_datatype(),
            nullable: true,
            default_constraint: None,
            inverted_index: false,
        }])?;
        let extension = Json2ExtensionType::new(Arc::new(JsonMetadata::new(settings.clone())));
        let mut column = ColumnSchema::new(
            "j",
            ConcreteDataType::json2(JsonNativeType::Object(JsonObjectType::from([(
                "kind".to_string(),
                JsonNativeType::String,
            )]))),
            true,
        );
        column.with_extension_type(&extension);

        let mut builder = column.create_mutable_vector(0);
        let target = builder.to_vector().to_arrow_array().data_type().clone();
        let targets = HashMap::from([(
            "j".to_string(),
            Json2Target {
                schema: column,
                settings,
                data_type: target.clone(),
            },
        )]);
        let values = [
            json!({"kind": "a", "extra": {"x": 1}}),
            json!({"kind": "b", "dynamic": true}),
        ];
        let source_settings = JsonSettings::default();
        let source_extension =
            Json2ExtensionType::new(Arc::new(JsonMetadata::new_v1(source_settings.clone())));
        let mut source_column = ColumnSchema::new(
            "j",
            ConcreteDataType::json2(JsonNativeType::Object(JsonObjectType::new())),
            true,
        );
        source_column.with_extension_type(&source_extension);
        let mut sources = Vec::with_capacity(values.len());
        for value in &values {
            let mut source_builder = source_column.create_mutable_vector(1);
            let value = source_settings.encode(value.clone())?;
            source_builder.try_push_value_ref(&value.as_value_ref())?;
            let source = source_builder.to_vector().to_arrow_array();
            let source_field = Field::new("j", source.data_type().clone(), true)
                .with_extension_type(source_extension.clone());
            sources.push(
                JsonArray::from(&source).project_json2(&source_field, &ArrowDataType::Binary)?,
            );
        }
        let arrays = sources.iter().map(|x| x.as_ref()).collect::<Vec<_>>();
        let array = datatypes::arrow::compute::concat(&arrays)?;
        let field = Field::new("j", ArrowDataType::Binary, true).with_extension_type(extension);
        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![array])?;

        let batch = rewrite_json2_batch(batch, &targets)?;
        let field = batch.schema_ref().field(0);
        assert!(Json2PhysicalLayout::try_from_root(field)?.is_version_2());
        assert_eq!(&target, field.data_type());
        let projected =
            JsonArray::from(batch.column(0)).project_json2(field, &ArrowDataType::Binary)?;
        let projected = JsonArray::from(&projected);
        for (i, expected) in values.into_iter().enumerate() {
            assert_eq!(expected, projected.try_get_value(i)?);
        }
        Ok(())
    }

    #[test]
    fn test_rewrite_json2_v2_source_to_narrower_target() -> Result<(), Box<dyn std::error::Error>> {
        let target_settings = JsonSettings::try_new(vec![JsonTypeHint {
            path: vec!["kind".to_string()],
            data_type: ConcreteDataType::string_datatype(),
            nullable: true,
            default_constraint: None,
            inverted_index: false,
        }])?;
        let target_extension =
            Json2ExtensionType::new(Arc::new(JsonMetadata::new(target_settings.clone())));
        let mut target_column = ColumnSchema::new(
            "j",
            ConcreteDataType::json2(JsonNativeType::Object(JsonObjectType::new())),
            true,
        );
        target_column.with_extension_type(&target_extension);
        let mut builder = target_column.create_mutable_vector(0);
        let target_type = builder.to_vector().to_arrow_array().data_type().clone();
        let targets = HashMap::from([(
            "j".to_string(),
            Json2Target {
                schema: target_column,
                settings: target_settings,
                data_type: target_type.clone(),
            },
        )]);

        let source_settings = JsonSettings::try_new(vec![
            JsonTypeHint {
                path: vec!["kind".to_string()],
                data_type: ConcreteDataType::string_datatype(),
                nullable: true,
                default_constraint: None,
                inverted_index: false,
            },
            JsonTypeHint {
                path: vec!["source_only".to_string()],
                data_type: ConcreteDataType::int64_datatype(),
                nullable: true,
                default_constraint: None,
                inverted_index: false,
            },
        ])?;
        let source_extension =
            Json2ExtensionType::new(Arc::new(JsonMetadata::new(source_settings.clone())));
        let mut source_column = ColumnSchema::new(
            "j",
            ConcreteDataType::json2(JsonNativeType::Object(JsonObjectType::new())),
            true,
        );
        source_column.with_extension_type(&source_extension);
        let expected = json!({
            "kind": "a",
            "source_only": 7,
            "dynamic": {"nested": true}
        });
        let mut builder = source_column.create_mutable_vector(1);
        let value = source_settings.encode(expected.clone())?;
        builder.try_push_value_ref(&value.as_value_ref())?;
        let source = builder.to_vector().to_arrow_array();
        let source_field =
            Field::new("j", source.data_type().clone(), true).with_extension_type(source_extension);
        let source =
            JsonArray::from(&source).project_json2(&source_field, &ArrowDataType::Binary)?;
        let field = Field::new("j", ArrowDataType::Binary, true)
            .with_extension_type(target_extension.clone());
        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![source])?;

        let batch = rewrite_json2_batch(batch, &targets)?;
        let field = batch.schema_ref().field(0);
        assert!(Json2PhysicalLayout::try_from_root(field)?.is_version_2());
        assert_eq!(&target_type, field.data_type());
        let ArrowDataType::Struct(fields) = field.data_type() else {
            unreachable!()
        };
        assert_eq!(
            vec![JSON2_REMAINDER_FIELD_NAME, "kind"],
            fields.iter().map(|x| x.name().as_str()).collect::<Vec<_>>()
        );

        let projected =
            JsonArray::from(batch.column(0)).project_json2(field, &ArrowDataType::Binary)?;
        assert_eq!(expected, JsonArray::from(&projected).try_get_value(0)?);

        let first_schema = batch.schema();
        let field =
            Field::new("j", ArrowDataType::Binary, true).with_extension_type(target_extension);
        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![projected])?;
        let batch = rewrite_json2_batch(batch, &targets)?;
        assert_eq!(first_schema, batch.schema());

        let field = batch.schema_ref().field(0);
        let projected =
            JsonArray::from(batch.column(0)).project_json2(field, &ArrowDataType::Binary)?;
        assert_eq!(expected, JsonArray::from(&projected).try_get_value(0)?);
        Ok(())
    }
}
