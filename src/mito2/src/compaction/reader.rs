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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use arrow_schema::extension::{EXTENSION_TYPE_METADATA_KEY, ExtensionType};
use common_time::Timestamp;
use common_time::range::TimestampRange;
use common_time::timestamp::TimeUnit;
use datafusion_common::ScalarValue;
use datafusion_expr::Expr;
use datatypes::arrow::datatypes::{DataType as ArrowDataType, Field, Schema, SchemaRef};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::extension::json::{JSON2_REMAINDER_FIELD_NAME, Json2ExtensionType, JsonMetadata};
use datatypes::json::{JSON2_DEFAULT_MAX_AUTO_EXPANDED_PATHS, JsonSettings, JsonTypeHint};
use datatypes::prelude::ConcreteDataType;
use datatypes::types::json_type::JsonNativeType;
use datatypes::vectors::json::array::JsonArray;
use datatypes::vectors::json::json2_physical_data_type;
use parquet::arrow::parquet_to_arrow_schema;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData};
use snafu::{OptionExt, ResultExt, ensure};
use store_api::metadata::RegionMetadataRef;

use crate::access_layer::AccessLayerRef;
use crate::cache::{CacheManagerRef, CacheStrategy};
use crate::error::{
    ConvertValueSnafu, DataTypeMismatchSnafu, InvalidRecordBatchSnafu, NewRecordBatchSnafu, Result,
    TimeRangePredicateOverflowSnafu,
};
use crate::read::FlatSource;
use crate::read::flat_projection::FlatProjectionMapper;
use crate::read::read_columns::{Json2TargetLayout, ReadColumns};
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
        let plans = collect_json2_rewrite_plans_from_parquet(&self.metadata, &parquet_metadata)?;
        let scan_input = self.build_scan_input(&parquet_metadata, &plans)?;

        let schema = scan_input.mapper.output_schema();
        let schema = rewrite_json2_schema(schema.arrow_schema(), &plans);

        let stream = SeqScan::new(scan_input)
            .build_flat_reader_for_compaction()
            .await?;
        Ok(FlatSource::new_stream(schema, stream))
    }

    fn build_scan_input(
        self,
        parquet_metadata: &[Arc<ParquetMetaData>],
        plans: &Json2RewritePlans,
    ) -> Result<ScanInput> {
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

        let projection = (0..self.metadata.column_metadatas.len()).collect();
        let read_column_ids = self
            .metadata
            .column_metadatas
            .iter()
            .map(|x| x.column_id)
            .collect::<Vec<_>>();

        let mut json2_target_layouts = BTreeMap::new();
        for (name, plan) in plans {
            let Some(column) = self.metadata.column_by_name(name) else {
                continue;
            };
            let extension_metadata = column
                .column_schema
                .metadata()
                .get(EXTENSION_TYPE_METADATA_KEY)
                .cloned()
                .with_context(|| InvalidRecordBatchSnafu {
                    reason: format!("JSON2 target column '{name}' has no extension metadata"),
                })?;
            json2_target_layouts.insert(
                column.column_id,
                Json2TargetLayout {
                    data_type: json2_physical_data_type(&plan.target_layout),
                    extension_metadata,
                    target_layout: plan.target_layout.clone(),
                },
            );
        }
        let read_columns =
            ReadColumns::new(read_column_ids).with_json2_target_layouts(json2_target_layouts);
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

/// Plan for rewriting one JSON2 column to a fixed compaction layout.
///
/// A compaction input may contain v1 and v2 SSTs with different physical schemas. This plan is
/// derived only from the current region metadata and remains fixed while all input batches are
/// decoded and rewritten. It therefore prevents source-only paths from expanding the output
/// schema without a bound.
pub(crate) struct Json2RewritePlan {
    /// User-defined settings used to encode logical values.
    logical_settings: JsonSettings,
    /// Fixed settings used to build the target physical layout.
    target_layout: JsonSettings,
}

/// JSON2 rewrite plans keyed by logical column name.
pub(crate) type Json2RewritePlans = HashMap<String, Json2RewritePlan>;

#[derive(Clone)]
struct Json2LeafPathStats {
    rows: u64,
    data_type: JsonNativeType,
    is_type_conflicted: bool,
}

/// Builds the JSON2 rewrite plans for a compaction.
///
/// Type hints from current region metadata are always retained. Existing explicit dynamic paths
/// from all input SST schemas are ranked once to produce a fixed layout; paths found only in a v2
/// remainder are deliberately not promoted. [`rewrite_json2_batch`] decodes inputs and rewrites
/// them according to these plans. Non-JSON2 columns are omitted from the returned map.
///
/// Returns an error when a JSON2 column has invalid or missing extension metadata, or when its
/// output layout is not v2. Legacy region metadata is upgraded in memory by the region opener
/// before compaction reaches this function.
fn collect_json2_rewrite_plans_from_parquet(
    metadata: &RegionMetadataRef,
    parquet_metadata: &[Arc<ParquetMetaData>],
) -> Result<Json2RewritePlans> {
    let schemas = parquet_metadata
        .iter()
        .map(|metadata| {
            let file = metadata.file_metadata();
            let schema = parquet_to_arrow_schema(file.schema_descr(), file.key_value_metadata())
                .map_err(|error| {
                    InvalidRecordBatchSnafu {
                        reason: format!("Failed to read compaction input Arrow schema: {error}"),
                    }
                    .build()
                })?;
            let rows = metadata
                .row_groups()
                .iter()
                .map(|x| x.num_rows())
                .sum::<i64>() as u64;
            Ok((Arc::new(schema), rows))
        })
        .collect::<Result<Vec<_>>>()?;

    collect_json2_rewrite_plans(metadata, &schemas)
}

/// Builds JSON2 rewrite plans from existing physical schemas.
///
/// Each source row count weights all of its existing explicit leaves. Ordinary paths are never
/// discovered from the v2 remainder, so this operation cannot unexpectedly promote opaque data.
pub(crate) fn collect_json2_rewrite_plans(
    metadata: &RegionMetadataRef,
    schemas: &[(SchemaRef, u64)],
) -> Result<Json2RewritePlans> {
    let json2_columns = metadata
        .column_metadatas
        .iter()
        .filter_map(|x| {
            x.column_schema
                .data_type
                .is_json2()
                .then_some(&x.column_schema)
        })
        .collect::<Vec<_>>();

    let mut plans = HashMap::with_capacity(json2_columns.len());
    for column in json2_columns {
        let extension = column
            .extension_type::<Json2ExtensionType>()
            .context(DataTypeMismatchSnafu)?
            .with_context(|| InvalidRecordBatchSnafu {
                reason: format!("JSON2 column '{}' has no extension metadata", column.name),
            })?;
        // Source SSTs may use v1, but current region metadata is copied to the rewritten output.
        // Since the target physical layout is always v2, v1 metadata would produce an
        // inconsistent persisted field. The region opener normally upgraded this metadata already.
        ensure!(
            extension.metadata().is_version_2(),
            InvalidRecordBatchSnafu {
                reason: format!("JSON2 column '{}' is not layout v2", column.name),
            }
        );

        let settings = extension.metadata().json_settings();
        let hint_paths = settings
            .type_hints()
            .iter()
            .map(|hint| hint.path.iter().map(String::as_str).collect::<Vec<_>>())
            .collect::<HashSet<_>>();
        let mut stats = HashMap::new();
        for (schema, rows) in schemas {
            let Some((_, field)) = schema.fields().find(&column.name) else {
                continue;
            };
            collect_json2_path_stats(field, *rows, &hint_paths, &mut stats)?;
        }

        let mut hints = settings.type_hints().to_vec();
        hints.extend(select_dynamic_hints(settings, &hint_paths, &stats));
        let target_layout = JsonSettings::try_new(hints, Some(0)).context(DataTypeMismatchSnafu)?;
        plans.insert(
            column.name.clone(),
            Json2RewritePlan {
                logical_settings: settings.clone(),
                target_layout,
            },
        );
    }
    Ok(plans)
}

fn collect_json2_path_stats<'a>(
    field: &'a Field,
    rows: u64,
    hint_paths: &HashSet<Vec<&str>>,
    stats: &mut HashMap<Vec<&'a str>, Json2LeafPathStats>,
) -> Result<()> {
    let ArrowDataType::Struct(fields) = field.data_type() else {
        return InvalidRecordBatchSnafu {
            reason: format!("JSON2 column '{}' is not a struct", field.name()),
        }
        .fail();
    };
    let mut paths = Vec::new();
    for field in fields {
        if field.name() == JSON2_REMAINDER_FIELD_NAME {
            continue;
        }
        collect_leaf_path_types(field, &mut Vec::new(), &mut paths)?;
    }

    for (path, data_type) in paths {
        if hint_paths.contains(path.as_slice()) {
            continue;
        }
        let Some(stat) = stats.get_mut(&path) else {
            stats.insert(
                path,
                Json2LeafPathStats {
                    rows,
                    data_type,
                    is_type_conflicted: false,
                },
            );
            continue;
        };
        if stat.data_type != data_type {
            stat.is_type_conflicted = true;
        } else {
            stat.rows += rows;
        }
    }
    Ok(())
}

fn collect_leaf_path_types<'a>(
    field: &'a Field,
    path: &mut Vec<&'a str>,
    paths: &mut Vec<(Vec<&'a str>, JsonNativeType)>,
) -> Result<()> {
    path.push(field.name());
    if let ArrowDataType::Struct(fields) = field.data_type()
        && !fields.is_empty()
    {
        for field in fields {
            collect_leaf_path_types(field, path, paths)?;
        }
    } else {
        let json_type =
            JsonNativeType::try_from(field.data_type()).context(DataTypeMismatchSnafu)?;
        paths.push((path.clone(), json_type));
    }
    path.pop();
    Ok(())
}

fn select_dynamic_hints(
    settings: &JsonSettings,
    hint_paths: &HashSet<Vec<&str>>,
    stats: &HashMap<Vec<&str>, Json2LeafPathStats>,
) -> Vec<JsonTypeHint> {
    let all_paths = stats
        .keys()
        .map(Vec::as_slice)
        .chain(hint_paths.iter().map(Vec::as_slice))
        .collect::<HashSet<_>>();
    let has_ancestor_path =
        |path: &[&str]| (1..path.len()).any(|len| all_paths.contains(&path[..len]));

    let prefixes = all_paths
        .iter()
        .copied()
        .flat_map(|path| (1..path.len()).map(|len| &path[..len]))
        .collect::<HashSet<_>>();
    let has_descendant_path = |path: &[&str]| prefixes.contains(path);

    let mut candidates = stats
        .iter()
        .filter(|(path, stat)| {
            !stat.is_type_conflicted
                && stat.data_type.is_primitive()
                && !has_ancestor_path(path)
                && !has_descendant_path(path)
        })
        .collect::<Vec<_>>();
    candidates.sort_unstable_by(|(x_path, x), (y_path, y)| {
        y.rows.cmp(&x.rows).then_with(|| x_path.cmp(y_path))
    });
    candidates
        .into_iter()
        .take(
            settings
                .max_auto_expanded_paths()
                .unwrap_or(JSON2_DEFAULT_MAX_AUTO_EXPANDED_PATHS) as usize,
        )
        .map(|(path, stat)| JsonTypeHint {
            path: path.iter().map(|x| (*x).to_owned()).collect(),
            data_type: ConcreteDataType::from_arrow_type(&stat.data_type.as_arrow_type()),
            nullable: true,
            default_constraint: None,
            inverted_index: false,
        })
        .collect()
}

/// Replaces JSON2 physical field types according to the computed rewrite plans.
pub(crate) fn rewrite_json2_schema(schema: &SchemaRef, plans: &Json2RewritePlans) -> SchemaRef {
    if plans.is_empty() {
        return schema.clone();
    }
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            let Some(plan) = plans.get(field.name()) else {
                return field.clone();
            };
            let mut field = Field::clone(field);
            field.set_data_type(json2_physical_data_type(&plan.target_layout));
            field = field.with_extension_type(Json2ExtensionType::new(Arc::new(
                JsonMetadata::new(plan.logical_settings.clone()),
            )));
            Arc::new(field)
        })
        .collect::<Vec<_>>();
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

/// Rewrites JSON2 columns in `batch` according to the computed plans.
pub(crate) fn rewrite_json2_batch(
    batch: RecordBatch,
    plans: &Json2RewritePlans,
) -> Result<RecordBatch> {
    if plans.is_empty() {
        return Ok(batch);
    }
    let mut fields = Vec::with_capacity(batch.num_columns());
    let mut columns = Vec::with_capacity(batch.num_columns());

    for (field, array) in batch.schema_ref().fields().iter().zip(batch.columns()) {
        let Some(plan) = plans.get(field.name()) else {
            fields.push(field.clone());
            columns.push(array.clone());
            continue;
        };

        let array = JsonArray::from(array)
            .rewrite_to_v2(field, &plan.logical_settings, &plan.target_layout)
            .context(ConvertValueSnafu)?;
        debug_assert_eq!(
            &json2_physical_data_type(&plan.target_layout),
            array.data_type()
        );

        let mut field = Field::clone(field);
        field.set_data_type(array.data_type().clone());
        field = field.with_extension_type(Json2ExtensionType::new(Arc::new(JsonMetadata::new(
            plan.logical_settings.clone(),
        ))));
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
    use datatypes::prelude::{ConcreteDataType, DataType};
    use datatypes::schema::ColumnSchema;
    use datatypes::types::json_type::{JsonNativeType, JsonObjectType};
    use serde_json::json;

    use super::*;

    #[test]
    fn test_select_dynamic_hints_rejects_type_and_prefix_conflicts()
    -> Result<(), Box<dyn std::error::Error>> {
        let settings = JsonSettings::try_new(
            vec![JsonTypeHint {
                path: vec!["hint".to_string()],
                data_type: ConcreteDataType::string_datatype(),
                nullable: true,
                default_constraint: None,
                inverted_index: false,
            }],
            Some(2),
        )?;
        let stat = |rows, data_type, is_type_conflicted| Json2LeafPathStats {
            rows,
            data_type,
            is_type_conflicted,
        };
        let stats = HashMap::from([
            (
                vec!["hint", "nested"],
                stat(10, JsonNativeType::String, false),
            ),
            (vec!["popular"], stat(9, JsonNativeType::String, false)),
            (
                vec!["popular", "nested"],
                stat(8, JsonNativeType::u64(), false),
            ),
            (
                vec!["type_conflicted"],
                stat(7, JsonNativeType::String, true),
            ),
            (
                vec!["array"],
                stat(
                    6,
                    JsonNativeType::Array(Box::new(JsonNativeType::String)),
                    false,
                ),
            ),
            (vec!["variant"], stat(5, JsonNativeType::Variant, false)),
            (vec!["tie_a"], stat(2, JsonNativeType::String, false)),
            (vec!["tie_b"], stat(2, JsonNativeType::Bool, false)),
        ]);

        let hint_paths = settings
            .type_hints()
            .iter()
            .map(|hint| hint.path.iter().map(String::as_str).collect::<Vec<_>>())
            .collect::<HashSet<_>>();
        let hints = select_dynamic_hints(&settings, &hint_paths, &stats);
        assert_eq!(
            vec![vec!["tie_a".to_string()], vec!["tie_b".to_string()]],
            hints.into_iter().map(|x| x.path).collect::<Vec<_>>()
        );
        Ok(())
    }

    #[test]
    fn test_rewrite_json2_v1_batch_to_target_layout() -> Result<(), Box<dyn std::error::Error>> {
        let settings = JsonSettings::try_new(
            vec![JsonTypeHint {
                path: vec!["kind".to_string()],
                data_type: ConcreteDataType::string_datatype(),
                nullable: true,
                default_constraint: None,
                inverted_index: false,
            }],
            Some(0),
        )?;
        let target = json2_physical_data_type(&settings);
        let plans = HashMap::from([(
            "j".to_string(),
            Json2RewritePlan {
                logical_settings: settings.clone(),
                target_layout: settings,
            },
        )]);
        let values = [
            json!({"kind": "a", "extra": {"x": 1}}),
            json!({"kind": "b", "extra": {"x": 2}}),
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
        let mut source_builder = source_column.data_type.create_mutable_vector(values.len());
        for value in &values {
            let value = source_settings.encode(value.clone())?;
            source_builder.try_push_value_ref(&value.as_value_ref())?;
        }
        let source = source_builder.to_vector().to_arrow_array();
        let field =
            Field::new("j", source.data_type().clone(), true).with_extension_type(source_extension);
        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![source])?;

        let batch = rewrite_json2_batch(batch, &plans)?;
        let field = batch.schema_ref().field(0);
        assert!(Json2PhysicalLayout::try_from_root(field)?.is_version_2());
        assert_eq!(&target, field.data_type());
        let projected =
            JsonArray::from(batch.column(0)).project_to_v2(field, &ArrowDataType::Binary)?;
        let projected = JsonArray::from(&projected);
        for (i, expected) in values.into_iter().enumerate() {
            assert_eq!(expected, projected.try_get_value(i)?);
        }
        Ok(())
    }

    #[test]
    fn test_rewrite_json2_v2_source_to_narrower_target() -> Result<(), Box<dyn std::error::Error>> {
        let target_settings = JsonSettings::try_new(
            vec![JsonTypeHint {
                path: vec!["kind".to_string()],
                data_type: ConcreteDataType::string_datatype(),
                nullable: true,
                default_constraint: None,
                inverted_index: false,
            }],
            Some(0),
        )?;
        let target_extension =
            Json2ExtensionType::new(Arc::new(JsonMetadata::new(target_settings.clone())));
        let target_type = json2_physical_data_type(&target_settings);
        let plans = HashMap::from([(
            "j".to_string(),
            Json2RewritePlan {
                logical_settings: target_settings.clone(),
                target_layout: target_settings,
            },
        )]);

        let source_settings = JsonSettings::try_new(
            vec![
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
            ],
            None,
        )?;
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
            JsonArray::from(&source).project_to_v2(&source_field, &ArrowDataType::Binary)?;
        let field = Field::new("j", ArrowDataType::Binary, true)
            .with_extension_type(target_extension.clone());
        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![source])?;

        let batch = rewrite_json2_batch(batch, &plans)?;
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
            JsonArray::from(batch.column(0)).project_to_v2(field, &ArrowDataType::Binary)?;
        assert_eq!(expected, JsonArray::from(&projected).try_get_value(0)?);

        let first_schema = batch.schema();
        let field =
            Field::new("j", ArrowDataType::Binary, true).with_extension_type(target_extension);
        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![projected])?;
        let batch = rewrite_json2_batch(batch, &plans)?;
        assert_eq!(first_schema, batch.schema());

        let field = batch.schema_ref().field(0);
        let projected =
            JsonArray::from(batch.column(0)).project_to_v2(field, &ArrowDataType::Binary)?;
        assert_eq!(expected, JsonArray::from(&projected).try_get_value(0)?);
        Ok(())
    }

    #[test]
    fn test_rewrite_json2_v2_source_to_wider_target() -> Result<(), Box<dyn std::error::Error>> {
        let logical_settings = JsonSettings::try_new(
            vec![JsonTypeHint {
                path: vec!["kind".to_string()],
                data_type: ConcreteDataType::string_datatype(),
                nullable: true,
                default_constraint: None,
                inverted_index: false,
            }],
            Some(0),
        )?;
        let target_layout = JsonSettings::try_new(
            vec![
                JsonTypeHint {
                    path: vec!["kind".to_string()],
                    data_type: ConcreteDataType::string_datatype(),
                    nullable: true,
                    default_constraint: None,
                    inverted_index: false,
                },
                JsonTypeHint {
                    path: vec!["promoted".to_string()],
                    data_type: ConcreteDataType::int64_datatype(),
                    nullable: true,
                    default_constraint: None,
                    inverted_index: false,
                },
            ],
            Some(0),
        )?;
        let target_type = json2_physical_data_type(&target_layout);
        let plans = HashMap::from([(
            "j".to_string(),
            Json2RewritePlan {
                logical_settings: logical_settings.clone(),
                target_layout,
            },
        )]);

        let extension =
            Json2ExtensionType::new(Arc::new(JsonMetadata::new(logical_settings.clone())));
        let mut column = ColumnSchema::new(
            "j",
            ConcreteDataType::json2(JsonNativeType::Object(JsonObjectType::new())),
            true,
        );
        column.with_extension_type(&extension);
        let expected = json!({
            "kind": "a",
            "promoted": 7,
            "dynamic": {"nested": true}
        });
        let mut builder = column.create_mutable_vector(1);
        let value = logical_settings.encode(expected.clone())?;
        builder.try_push_value_ref(&value.as_value_ref())?;
        let source = builder.to_vector().to_arrow_array();
        assert_eq!(
            &json2_physical_data_type(&logical_settings),
            source.data_type()
        );
        let field =
            Field::new("j", source.data_type().clone(), true).with_extension_type(extension);
        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![source])?;

        let batch = rewrite_json2_batch(batch, &plans)?;
        let field = batch.schema_ref().field(0);
        assert_eq!(&target_type, field.data_type());
        let ArrowDataType::Struct(fields) = field.data_type() else {
            unreachable!()
        };
        assert_eq!(
            vec![JSON2_REMAINDER_FIELD_NAME, "kind", "promoted"],
            fields.iter().map(|x| x.name().as_str()).collect::<Vec<_>>()
        );

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<datatypes::arrow::array::StructArray>()
            .unwrap();
        let promoted = array
            .column_by_name("promoted")
            .unwrap()
            .as_any()
            .downcast_ref::<datatypes::arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(7, promoted.value(0));

        let projected =
            JsonArray::from(batch.column(0)).project_to_v2(field, &ArrowDataType::Binary)?;
        assert_eq!(expected, JsonArray::from(&projected).try_get_value(0)?);
        Ok(())
    }
}
