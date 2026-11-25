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

mod memory;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use api::helper::{
    ColumnDataTypeWrapper, pb_value_to_value_ref, proto_value_type, proto_value_type_match,
};
use api::v1::column_data_type_extension::TypeExt;
use api::v1::value::ValueData;
use api::v1::{ColumnDataType, ColumnDataTypeExtension, Rows, SemanticType, Value};
use arrow::array::BinaryBuilder;
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use common_error::ext::BoxedError;
use common_query::prelude::{EXEMPLAR_METRIC_COLUMN, greptime_exemplar_labels, greptime_value};
use common_time::Timestamp;
use datatypes::data_type::ConcreteDataType;
use datatypes::extension::json::align_schema_with_json_array;
use datatypes::value::ValueRef;
use datatypes::vectors::VectorRef;
use mito_codec::row_converter::sparse::SparsePrimaryKeyCodec;
use mito_codec::row_converter::{SortField, TsidGenerator};
use snafu::{OptionExt, ResultExt, ensure};
use store_api::metric_engine_consts::{
    DATA_SCHEMA_TABLE_ID_COLUMN_NAME, DATA_SCHEMA_TSID_COLUMN_NAME,
};
use store_api::storage::TableId;
use store_api::storage::consts::{PRIMARY_KEY_COLUMN_NAME, ReservedColumnId};
use table::metadata::TableInfo;

use crate::error::{self, Result};
use crate::insert::ExemplarBatch;
use crate::insert::exemplar::encode_json2_data_value;

pub(crate) use memory::{
    estimated_exemplar_record_batch_bytes, estimated_record_batch_bytes,
    estimated_sparse_record_batch_bytes,
};

fn validate_row_input_columns<'a>(
    rows: &'a Rows,
    table_info: &TableInfo,
) -> Result<HashMap<&'a str, usize>> {
    let schema = &table_info.meta.schema;
    let mut source_columns = HashMap::with_capacity(rows.schema.len());
    for (index, source) in rows.schema.iter().enumerate() {
        ensure!(
            source_columns
                .insert(source.column_name.as_str(), index)
                .is_none(),
            error::InvalidInsertRequestSnafu {
                reason: format!("Duplicate input column {}", source.column_name),
            }
        );
        let target = schema
            .column_schema_by_name(&source.column_name)
            .with_context(|| error::InvalidInsertRequestSnafu {
                reason: format!("Unknown input column {}", source.column_name),
            })?;
        let data_type =
            ColumnDataTypeWrapper::try_new(source.datatype, source.datatype_extension.clone())
                .map_err(BoxedError::new)
                .context(error::ExternalSnafu)?;
        ensure!(
            ConcreteDataType::from(data_type) == target.data_type,
            error::InvalidInsertRequestSnafu {
                reason: format!("Input datatype differs for column {}", source.column_name),
            }
        );
        let semantic = SemanticType::try_from(source.semantic_type)
            .ok()
            .with_context(|| error::InvalidInsertRequestSnafu {
                reason: format!("Invalid semantic type for column {}", source.column_name),
            })?;
        let is_tag = table_info.meta.primary_key_indices.iter().any(|&index| {
            schema
                .column_schemas()
                .get(index)
                .is_some_and(|column| column.name == source.column_name)
        });
        let expected_semantic = if target.is_time_index() {
            SemanticType::Timestamp
        } else if is_tag {
            SemanticType::Tag
        } else {
            SemanticType::Field
        };
        ensure!(
            semantic == expected_semantic,
            error::InvalidInsertRequestSnafu {
                reason: format!("Input semantics differ for column {}", source.column_name),
            }
        );
    }
    for row in &rows.rows {
        ensure!(
            row.values.len() == rows.schema.len(),
            error::InvalidInsertRequestSnafu {
                reason: format!(
                    "Expected {} values, got {}",
                    rows.schema.len(),
                    row.values.len()
                ),
            }
        );
    }
    Ok(source_columns)
}

fn build_column_vector(
    rows: &Rows,
    column: &datatypes::schema::ColumnSchema,
    source_index: Option<usize>,
) -> Result<VectorRef> {
    if let Some(index) = source_index {
        let mut builder = column.create_mutable_vector(rows.rows.len());
        for row in &rows.rows {
            let value = &row.values[index];
            ensure!(
                value.value_data.is_some() || column.is_nullable(),
                error::InvalidInsertRequestSnafu {
                    reason: format!("Null supplied for non-nullable column {}", column.name),
                }
            );
            ensure!(
                value_matches_type(
                    value,
                    rows.schema[index].datatype,
                    rows.schema[index].datatype_extension.as_ref(),
                ),
                error::InvalidInsertRequestSnafu {
                    reason: format!("Value datatype differs for column {}", column.name),
                }
            );
            builder
                .try_push_value_ref(&pb_value_to_value_ref(
                    value,
                    rows.schema[index].datatype_extension.as_ref(),
                ))
                .map_err(BoxedError::new)
                .context(error::ExternalSnafu)?;
        }
        Ok(builder.to_vector())
    } else {
        column
            .create_default_vector(rows.rows.len())
            .map_err(BoxedError::new)
            .context(error::ExternalSnafu)?
            .with_context(|| error::InvalidInsertRequestSnafu {
                reason: format!("Missing required column {}", column.name),
            })
    }
}

/// Converts prepared rows in target-schema order without Prom-specific renaming.
///
/// Missing columns evaluate their defaults once per conversion. Explicit nulls
/// remain nulls. Semantic types are checked against the table time index and
/// primary-key indices.
///
/// # Panics
///
/// Panics if `rows.rows` is empty. Callers must skip empty writes before conversion.
pub fn rows_to_record_batch(rows: &Rows, table_info: &TableInfo) -> Result<RecordBatch> {
    assert!(!rows.rows.is_empty(), "prepared rows must not be empty");
    let schema = &table_info.meta.schema;
    let source_columns = validate_row_input_columns(rows, table_info)?;

    let mut arrays = Vec::with_capacity(schema.num_columns());
    for column in schema.column_schemas() {
        let vector = build_column_vector(
            rows,
            column,
            source_columns.get(column.name.as_str()).copied(),
        )?;
        arrays.push(vector.to_arrow_array());
    }
    let arrow_schema = align_schema_with_json_array(schema.arrow_schema().clone(), &arrays);
    RecordBatch::try_new(arrow_schema, arrays).context(error::ComputeArrowSnafu)
}

/// Encodes validated sparse rows directly into `[__primary_key, <non-tag columns>]`
/// in catalog `primary_key_indices` order, applying pure defaults and nullability checks.
///
/// # Panics
///
/// Panics if `rows.rows` is empty. Callers must skip empty writes before conversion.
pub fn sparse_rows_to_record_batch(rows: &Rows, table_info: &TableInfo) -> Result<RecordBatch> {
    assert!(!rows.rows.is_empty(), "prepared rows must not be empty");
    let schema = &table_info.meta.schema;
    ensure!(
        table_info.meta.column_ids.len() == schema.num_columns(),
        error::InvalidInsertRequestSnafu {
            reason: format!("Table {} is missing sparse column IDs", table_info.name),
        }
    );
    let source_columns = validate_row_input_columns(rows, table_info)?;
    let pk_set: HashSet<usize> = table_info
        .meta
        .primary_key_indices
        .iter()
        .copied()
        .collect();

    enum PkColumnInput<'a> {
        Supplied {
            column_id: store_api::storage::ColumnId,
            index: usize,
            column: &'a datatypes::schema::ColumnSchema,
            source: &'a api::v1::ColumnSchema,
        },
        Default {
            column_id: store_api::storage::ColumnId,
            value: datatypes::value::Value,
        },
    }

    let mut codec_fields = Vec::with_capacity(table_info.meta.primary_key_indices.len());
    let mut pk_inputs = Vec::with_capacity(table_info.meta.primary_key_indices.len());
    for &index in &table_info.meta.primary_key_indices {
        let column = &schema.column_schemas()[index];
        let raw_column_id = table_info.meta.column_ids[index];
        let column_id = if column.name == DATA_SCHEMA_TABLE_ID_COLUMN_NAME
            || raw_column_id == ReservedColumnId::table_id()
        {
            ensure!(
                column.data_type == ConcreteDataType::uint32_datatype(),
                error::InvalidInsertRequestSnafu {
                    reason: format!("Sparse column {} must be UINT32", column.name),
                }
            );
            ReservedColumnId::table_id()
        } else if column.name == DATA_SCHEMA_TSID_COLUMN_NAME
            || raw_column_id == ReservedColumnId::tsid()
        {
            ensure!(
                column.data_type == ConcreteDataType::uint64_datatype(),
                error::InvalidInsertRequestSnafu {
                    reason: format!("Sparse column {} must be UINT64", column.name),
                }
            );
            ReservedColumnId::tsid()
        } else {
            ensure!(
                column.data_type.is_string(),
                error::InvalidInsertRequestSnafu {
                    reason: format!("Sparse primary key tag {} must be STRING", column.name),
                }
            );
            raw_column_id
        };
        codec_fields.push((column_id, SortField::new(column.data_type.clone())));
        if let Some(&src_idx) = source_columns.get(column.name.as_str()) {
            pk_inputs.push(PkColumnInput::Supplied {
                column_id,
                index: src_idx,
                column,
                source: &rows.schema[src_idx],
            });
        } else {
            let value = column
                .create_default()
                .map_err(BoxedError::new)
                .context(error::ExternalSnafu)?
                .with_context(|| error::InvalidInsertRequestSnafu {
                    reason: format!("Missing required column {}", column.name),
                })?;
            // Omitted NULL tags are absent from sparse keys, so skip per-row null propagation.
            if !value.is_null() {
                pk_inputs.push(PkColumnInput::Default { column_id, value });
            }
        }
    }

    let codec = SparsePrimaryKeyCodec::with_fields(codec_fields);
    let mut pk_builder = BinaryBuilder::with_capacity(rows.rows.len(), rows.rows.len() * 64);
    let mut pk_buf = Vec::with_capacity(128);
    let mut row_refs = Vec::with_capacity(pk_inputs.len());
    for row in &rows.rows {
        row_refs.clear();
        for input in &pk_inputs {
            match input {
                PkColumnInput::Supplied {
                    column_id,
                    index,
                    column,
                    source,
                } => {
                    let value = &row.values[*index];
                    ensure!(
                        value.value_data.is_some() || column.is_nullable(),
                        error::InvalidInsertRequestSnafu {
                            reason: format!(
                                "Null supplied for non-nullable column {}",
                                column.name
                            ),
                        }
                    );
                    ensure!(
                        value_matches_type(
                            value,
                            source.datatype,
                            source.datatype_extension.as_ref()
                        ),
                        error::InvalidInsertRequestSnafu {
                            reason: format!("Value datatype differs for column {}", column.name),
                        }
                    );
                    if value.value_data.is_some() {
                        row_refs.push((
                            *column_id,
                            pb_value_to_value_ref(value, source.datatype_extension.as_ref()),
                        ));
                    }
                }
                PkColumnInput::Default { column_id, value } => {
                    row_refs.push((*column_id, value.as_value_ref()));
                }
            }
        }
        pk_buf.clear();
        codec
            .encode_to_vec(row_refs.iter().cloned(), &mut pk_buf)
            .map_err(BoxedError::new)
            .context(error::ExternalSnafu)?;
        pk_builder.append_value(&pk_buf);
    }

    let arrow_schema = schema.arrow_schema();
    let non_pk_len = schema.num_columns() - pk_set.len();
    let mut fields = Vec::with_capacity(non_pk_len + 1);
    let mut arrays = Vec::with_capacity(non_pk_len + 1);
    fields.push(Arc::new(ArrowField::new(
        PRIMARY_KEY_COLUMN_NAME,
        ArrowDataType::Binary,
        false,
    )));
    arrays.push(Arc::new(pk_builder.finish()) as arrow::array::ArrayRef);

    for (idx, column) in schema.column_schemas().iter().enumerate() {
        if pk_set.contains(&idx) {
            continue;
        }
        let vector = build_column_vector(
            rows,
            column,
            source_columns.get(column.name.as_str()).copied(),
        )?;
        fields.push(arrow_schema.fields()[idx].clone());
        arrays.push(vector.to_arrow_array());
    }

    let target_schema = align_schema_with_json_array(
        Arc::new(ArrowSchema::new_with_metadata(
            fields,
            arrow_schema.metadata().clone(),
        )),
        &arrays,
    );
    RecordBatch::try_new(target_schema, arrays).context(error::ComputeArrowSnafu)
}

/// Encodes a validated [`ExemplarBatch`] into `[__primary_key, greptime_timestamp, greptime_value, greptime_exemplar_labels]`.
///
/// Uses destination `ColumnId`s in catalog `primary_key_indices` order so inherited tag encoding
/// matches Mito compaction and region splits even after `ALTER ... FIRST/AFTER`.
///
/// # Panics
///
/// Panics if `exemplars` is empty. Callers must skip empty writes before conversion.
pub fn exemplars_to_sparse_record_batch(
    exemplars: &ExemplarBatch,
    resolved_table_ids: &ahash::HashMap<String, TableId>,
    table_info: &TableInfo,
) -> Result<RecordBatch> {
    assert!(
        !exemplars.is_empty(),
        "prepared exemplars must not be empty"
    );
    let meta = &table_info.meta;
    let schema = &meta.schema;
    ensure!(
        meta.column_ids.len() == schema.num_columns(),
        error::InvalidInsertRequestSnafu {
            reason: format!("Table {} is missing sparse column IDs", table_info.name),
        }
    );

    let ts_idx = schema
        .timestamp_index()
        .context(error::InvalidInsertRequestSnafu {
            reason: "missing exemplar time index",
        })?;
    let ts_col = &schema.column_schemas()[ts_idx];
    let ts_unit = ts_col
        .data_type
        .as_timestamp()
        .map(|ts| ts.unit())
        .context(error::InvalidInsertRequestSnafu {
            reason: "invalid exemplar time index",
        })?;
    let val_idx = schema.column_index_by_name(greptime_value()).context(
        error::InvalidInsertRequestSnafu {
            reason: "missing exemplar value column",
        },
    )?;
    let val_col = &schema.column_schemas()[val_idx];
    let labels_idx = schema
        .column_index_by_name(greptime_exemplar_labels())
        .context(error::InvalidInsertRequestSnafu {
            reason: format!("missing {} column", greptime_exemplar_labels()),
        })?;
    let labels_col = &schema.column_schemas()[labels_idx];

    let mut metric_col = None;
    let mut active_tags = ahash::AHashMap::with_capacity(exemplars.distinct_tags().len());
    for (pk_rank, &idx) in meta.primary_key_indices.iter().enumerate() {
        let col = &schema.column_schemas()[idx];
        let col_id = meta.column_ids[idx];
        if col.name == DATA_SCHEMA_TABLE_ID_COLUMN_NAME
            || col.name == DATA_SCHEMA_TSID_COLUMN_NAME
            || col_id == ReservedColumnId::table_id()
            || col_id == ReservedColumnId::tsid()
        {
            continue;
        }
        ensure!(
            col.data_type.is_string(),
            error::InvalidInsertRequestSnafu {
                reason: format!("exemplar tag {} must be STRING", col.name),
            }
        );
        if col.name == EXEMPLAR_METRIC_COLUMN {
            metric_col = Some((pk_rank, col_id));
        } else if exemplars.distinct_tags().contains(col.name.as_str()) {
            active_tags.insert(col.name.as_str(), (pk_rank, col_id));
        }
    }
    let Some((metric_rank, metric_col_id)) = metric_col else {
        return error::InvalidInsertRequestSnafu {
            reason: "missing exemplar metric column",
        }
        .fail();
    };
    if active_tags.len() != exemplars.distinct_tags().len() {
        for tag in exemplars.distinct_tags() {
            ensure!(
                active_tags.contains_key(tag.as_str()),
                error::InvalidInsertRequestSnafu {
                    reason: format!(
                        "exemplar destination {} is missing tag {tag}",
                        table_info.name
                    ),
                }
            );
        }
    }

    let len = exemplars.len();
    let codec = SparsePrimaryKeyCodec::schemaless();
    let mut pk_builder = BinaryBuilder::with_capacity(len, len * 64);
    let mut ts_builder = ts_col.create_mutable_vector(len);
    let mut val_builder = val_col.create_mutable_vector(len);
    let mut labels_builder = labels_col.create_mutable_vector(len);
    let mut pk_buf = Vec::with_capacity(128);
    let mut ordered_tags = Vec::with_capacity(exemplars.distinct_tags().len() + 1);
    let mut prev_series: Option<(&str, &BTreeMap<String, String>)> = None;

    for exemplar in exemplars.exemplars() {
        let same_series = prev_series.is_some_and(|(prev_metric, prev_labels)| {
            prev_metric == exemplar.metric.as_ref()
                && std::ptr::eq(prev_labels, &*exemplar.series_labels)
        });
        if !same_series {
            let table_id = resolved_table_ids
                .get(exemplar.metric.as_ref())
                .copied()
                .with_context(|| error::InvalidInsertRequestSnafu {
                    reason: format!(
                        "exemplar source {} table ID was not resolved",
                        exemplar.metric
                    ),
                })?;
            let tsid = TsidGenerator::from_sorted_tags(
                exemplar
                    .series_labels
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_str())),
            );
            ordered_tags.clear();
            ordered_tags.push((metric_rank, metric_col_id, exemplar.metric.as_bytes()));
            for (name, value) in exemplar.series_labels.iter() {
                let &(rank, col_id) = &active_tags[name.as_str()];
                ordered_tags.push((rank, col_id, value.as_bytes()));
            }
            ordered_tags.sort_unstable_by_key(|&(rank, _, _)| rank);
            pk_buf.clear();
            codec
                .encode_internal(table_id, tsid, &mut pk_buf)
                .map_err(BoxedError::new)
                .context(error::ExternalSnafu)?;
            codec
                .encode_raw_tag_value(
                    ordered_tags
                        .iter()
                        .map(|&(_, col_id, bytes)| (col_id, bytes)),
                    &mut pk_buf,
                )
                .map_err(BoxedError::new)
                .context(error::ExternalSnafu)?;
            prev_series = Some((exemplar.metric.as_ref(), &*exemplar.series_labels));
        }
        pk_builder.append_value(&pk_buf);

        let converted_ts = Timestamp::new_nanosecond(exemplar.timestamp)
            .convert_to(ts_unit)
            .with_context(|| error::InvalidInsertRequestSnafu {
                reason: format!(
                    "exemplar timestamp {} overflows when converting to unit {ts_unit:?}",
                    exemplar.timestamp
                ),
            })?;
        ts_builder
            .try_push_value_ref(&ValueRef::Timestamp(converted_ts))
            .map_err(BoxedError::new)
            .context(error::ExternalSnafu)?;
        val_builder
            .try_push_value_ref(&ValueRef::Float64(datatypes::value::OrderedFloat(
                exemplar.value,
            )))
            .map_err(BoxedError::new)
            .context(error::ExternalSnafu)?;
        let labels_val = encode_json2_data_value(&exemplar.labels)?;
        labels_builder
            .try_push_value_ref(&labels_val.as_value_ref())
            .map_err(BoxedError::new)
            .context(error::ExternalSnafu)?;
    }

    let arrow_schema = schema.arrow_schema();
    let fields = vec![
        Arc::new(ArrowField::new(
            PRIMARY_KEY_COLUMN_NAME,
            ArrowDataType::Binary,
            false,
        )),
        arrow_schema.fields()[ts_idx].clone(),
        arrow_schema.fields()[val_idx].clone(),
        arrow_schema.fields()[labels_idx].clone(),
    ];
    let arrays = vec![
        Arc::new(pk_builder.finish()) as arrow::array::ArrayRef,
        ts_builder.to_vector().to_arrow_array(),
        val_builder.to_vector().to_arrow_array(),
        labels_builder.to_vector().to_arrow_array(),
    ];
    let target_schema = align_schema_with_json_array(
        Arc::new(ArrowSchema::new_with_metadata(
            fields,
            arrow_schema.metadata().clone(),
        )),
        &arrays,
    );
    RecordBatch::try_new(target_schema, arrays).context(error::ComputeArrowSnafu)
}

// Validate nested values before the infallible protobuf conversion reads type extensions.
pub(crate) fn value_matches_type(
    value: &Value,
    datatype: i32,
    extension: Option<&ColumnDataTypeExtension>,
) -> bool {
    let Some(value_type) = proto_value_type(value) else {
        return true;
    };
    let Ok(column_type) = ColumnDataType::try_from(datatype) else {
        return false;
    };
    if !proto_value_type_match(column_type, value_type) {
        return false;
    }
    match value.value_data.as_ref() {
        Some(ValueData::ListValue(list)) => {
            let Some(TypeExt::ListType(item)) = extension.and_then(|ext| ext.type_ext.as_ref())
            else {
                return false;
            };
            list.items.iter().all(|value| {
                value_matches_type(value, item.datatype, item.datatype_extension.as_deref())
            })
        }
        Some(ValueData::StructValue(value)) => {
            let Some(TypeExt::StructType(schema)) = extension.and_then(|ext| ext.type_ext.as_ref())
            else {
                return false;
            };
            value.items.len() == schema.fields.len()
                && value
                    .items
                    .iter()
                    .zip(&schema.fields)
                    .all(|(value, field)| {
                        value_matches_type(value, field.datatype, field.datatype_extension.as_ref())
                    })
        }
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread::yield_now;

    use api::helper::to_grpc_value;
    use api::v1::value::ValueData;
    use api::v1::{ColumnDataType, ColumnSchema as ProtoColumnSchema, Row, Value};
    use arrow::array::{Array, Int32Array, StringArray};
    use datatypes::extension::json::{Json2ExtensionType, JsonMetadata};
    use datatypes::json::{JsonSettings, JsonTypeHint};
    use datatypes::schema::{ColumnDefaultConstraint, ColumnSchema, Schema};
    use datatypes::types::json_type::JsonNativeType;
    use datatypes::value::Value as DtValue;
    use table::metadata::{TableInfoBuilder, TableMetaBuilder};

    use crate::req_convert::insert::row_to_batch::*;

    fn source(name: &str, datatype: ColumnDataType) -> ProtoColumnSchema {
        ProtoColumnSchema {
            column_name: name.to_string(),
            datatype: datatype as i32,
            semantic_type: SemanticType::Field as i32,
            ..Default::default()
        }
    }

    fn table_info(schema: Schema) -> TableInfo {
        let next_column_id = schema.num_columns() as u32;
        TableInfoBuilder::default()
            .table_id(1)
            .table_version(0)
            .name("test")
            .meta(
                TableMetaBuilder::empty()
                    .schema(Arc::new(schema))
                    .primary_key_indices(vec![])
                    .next_column_id(next_column_id)
                    .engine("mito")
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap()
    }

    fn fixture() -> (Rows, Schema) {
        let schema = Schema::new(vec![
            ColumnSchema::new("count", ConcreteDataType::int32_datatype(), false),
            ColumnSchema::new("label", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("fallback", ConcreteDataType::int32_datatype(), true)
                .with_default_constraint(Some(ColumnDefaultConstraint::Value(DtValue::Int32(7))))
                .unwrap(),
        ]);
        let rows = Rows {
            schema: vec![
                source("label", ColumnDataType::String),
                source("count", ColumnDataType::Int32),
            ],
            rows: vec![Row {
                values: vec![
                    Value::default(),
                    Value {
                        value_data: Some(ValueData::I32Value(3)),
                    },
                ],
            }],
        };
        (rows, schema)
    }

    fn json_column(metadata: JsonMetadata) -> ColumnSchema {
        let mut column = ColumnSchema::new(
            "json",
            ConcreteDataType::json2(JsonNativeType::object()),
            true,
        );
        column.with_extension_type(&Json2ExtensionType::new(Arc::new(metadata)));
        column
    }

    fn json_rows(column: &ColumnSchema, values: impl IntoIterator<Item = DtValue>) -> Rows {
        let (kind, extension) = ColumnDataTypeWrapper::try_from(column.data_type.clone())
            .unwrap()
            .to_parts();
        let mut input = source(&column.name, kind);
        input.datatype_extension = extension;
        Rows {
            schema: vec![input],
            rows: values
                .into_iter()
                .map(|value| Row {
                    values: vec![to_grpc_value(value)],
                })
                .collect(),
        }
    }

    #[test]
    fn test_conversion_estimate_covers_destination_padding_and_defaults() {
        let (mut rows, schema) = fixture();
        rows.rows[0].values[0] = ValueData::StringValue("x".repeat(10_000)).into();
        rows.rows.resize(1000, rows.rows[0].clone());
        let mut columns = schema.column_schemas().to_vec();
        columns.extend((0..2048).map(|i| {
            ColumnSchema::new(
                format!("tag_{i}"),
                ConcreteDataType::string_datatype(),
                true,
            )
        }));
        columns.push(
            ColumnSchema::new("default", ConcreteDataType::string_datatype(), true)
                .with_default_constraint(Some(ColumnDefaultConstraint::Value(DtValue::String(
                    "default".repeat(1000).into(),
                ))))
                .unwrap(),
        );
        let table = table_info(Schema::new(columns));
        let estimate = estimated_record_batch_bytes(&rows, &table).unwrap();
        let batch = rows_to_record_batch(&rows, &table).unwrap();
        assert!(estimate >= batch.get_array_memory_size());
        assert!(estimate > 4 * 1024 * 1024);
    }

    #[test]
    fn test_conversion_estimate_covers_nested_values_and_defaults() {
        use datatypes::value::ListValue;

        let value = DtValue::List(ListValue::new(
            vec![
                DtValue::String("a".into()),
                DtValue::String("b".repeat(30_000).into()),
            ],
            Arc::new(ConcreteDataType::string_datatype()),
        ));
        let datatype = value.data_type();
        let table = table_info(Schema::new(vec![
            ColumnSchema::new("input", datatype.clone(), true),
            ColumnSchema::new("default", datatype.clone(), true)
                .with_default_constraint(Some(ColumnDefaultConstraint::Value(value.clone())))
                .unwrap(),
        ]));
        let (kind, extension) = ColumnDataTypeWrapper::try_from(datatype)
            .unwrap()
            .to_parts();
        let mut input = source("input", kind);
        input.datatype_extension = extension;
        let rows = Rows {
            schema: vec![input],
            rows: vec![
                Row {
                    values: vec![to_grpc_value(value)]
                };
                10
            ],
        };
        let estimate = estimated_record_batch_bytes(&rows, &table).unwrap();
        let batch = rows_to_record_batch(&rows, &table).unwrap();
        assert!(estimate >= batch.get_array_memory_size());
    }

    #[test]
    fn test_conversion_estimate_covers_json2_expansion() {
        for (legacy, max_paths) in [
            (false, None),
            (false, Some(0)),
            (false, Some(1)),
            (true, None),
        ] {
            let settings = JsonSettings::try_new(
                (0..200)
                    .map(|i| JsonTypeHint {
                        path: vec![format!("hint_{i}")],
                        data_type: ConcreteDataType::string_datatype(),
                        inverted_index: false,
                        skipping_index: None,
                        fulltext_index: None,
                    })
                    .collect(),
                max_paths,
            )
            .unwrap();
            let column = json_column(if legacy {
                JsonMetadata::new_v1(settings.clone())
            } else {
                JsonMetadata::new(settings.clone())
            });
            let rows = json_rows(
                &column,
                (0..200).map(|i| {
                    settings
                        .encode(serde_json::json!({
                            format!("field_{i}"): "x".repeat(i * 100),
                            "nested": [{"name": "y".repeat(i)}, {"other": i}],
                        }))
                        .unwrap()
                }),
            );
            let table = table_info(Schema::new(vec![column]));
            let estimate = estimated_record_batch_bytes(&rows, &table).unwrap();
            let batch = rows_to_record_batch(&rows, &table).unwrap();
            assert!(
                estimate >= batch.get_array_memory_size(),
                "legacy={legacy}, max_paths={max_paths:?}"
            );
        }
    }

    #[test]
    fn test_conversion_estimate_respects_json2_expansion_limit() {
        for (legacy, max_paths) in [
            (false, Some(0)),
            (false, Some(100)),
            (false, None),
            (true, Some(0)),
        ] {
            let settings = JsonSettings::try_new(vec![], max_paths).unwrap();
            let column = json_column(if legacy {
                JsonMetadata::new_v1(settings.clone())
            } else {
                JsonMetadata::new(settings.clone())
            });
            let rows = json_rows(
                &column,
                (0..1000).map(|i| {
                    settings
                        .encode(serde_json::json!({format!("field_{i}"): "a"}))
                        .unwrap()
                }),
            );
            let table = table_info(Schema::new(vec![column]));
            let estimate = estimated_record_batch_bytes(&rows, &table).unwrap();
            let batch = rows_to_record_batch(&rows, &table).unwrap();
            assert!(
                estimate >= batch.get_array_memory_size(),
                "legacy={legacy}, max_paths={max_paths:?}"
            );
            if !legacy {
                // Small batches must fit this quota even when every row has a different key.
                assert!(estimate < 64 * 1024 * 1024, "max_paths={max_paths:?}");
            }
        }
    }

    #[test]
    fn test_conversion_estimate_covers_nested_json2_paths_and_defaults() {
        // One selected array path can contain many fields, with padding for every element.
        let items = (0..256)
            .map(|i| serde_json::json!({format!("field_{i}"): "a"}))
            .collect::<Vec<_>>();
        for use_default in [false, true] {
            let settings =
                JsonSettings::try_new(vec![], Some(if use_default { 0 } else { 1 })).unwrap();
            let value = settings
                .encode(serde_json::json!({"outer": {"inner": {"items": items}}}))
                .unwrap();
            let mut column = json_column(JsonMetadata::new(settings));
            let rows = if use_default {
                column = column
                    .with_default_constraint(Some(ColumnDefaultConstraint::Value(value)))
                    .unwrap();
                Rows {
                    schema: vec![],
                    rows: vec![Row { values: vec![] }; 2],
                }
            } else {
                json_rows(&column, [value.clone(), value])
            };
            let table = table_info(Schema::new(vec![column]));
            let estimate = estimated_record_batch_bytes(&rows, &table).unwrap();
            let batch = rows_to_record_batch(&rows, &table).unwrap();
            assert!(
                estimate >= batch.get_array_memory_size(),
                "use_default={use_default}"
            );
        }
    }

    #[test]
    fn test_conversion_estimate_covers_small_json2_view_buffers() {
        let settings = JsonSettings::default();
        let column = json_column(JsonMetadata::new_v1(settings.clone()));
        let rows = json_rows(
            &column,
            [settings
                .encode(serde_json::json!({"value": "x".repeat(13)}))
                .unwrap()],
        );
        let table = table_info(Schema::new(vec![column]));
        let estimate = estimated_record_batch_bytes(&rows, &table).unwrap();
        let batch = rows_to_record_batch(&rows, &table).unwrap();
        assert!(estimate >= batch.get_array_memory_size());
    }

    #[test]
    fn test_reorder_multifield_and_defaults() {
        let (rows, schema) = fixture();
        let batch = rows_to_record_batch(&rows, &table_info(schema.clone())).unwrap();
        assert_eq!(batch.schema(), schema.arrow_schema().clone());
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            3
        );
        assert!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .is_null(0)
        );
        assert_eq!(
            batch
                .column(2)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            7
        );
    }

    #[test]
    fn test_explicit_null_does_not_use_default() {
        let (mut rows, schema) = fixture();
        rows.schema.push(source("fallback", ColumnDataType::Int32));
        rows.rows[0].values.push(Value::default());
        assert!(
            rows_to_record_batch(&rows, &table_info(schema.clone()))
                .unwrap()
                .column(2)
                .is_null(0)
        );
    }

    #[test]
    fn test_timestamp_precisions() {
        for (datatype, value) in [
            (
                ColumnDataType::TimestampSecond,
                ValueData::TimestampSecondValue(42),
            ),
            (
                ColumnDataType::TimestampMillisecond,
                ValueData::TimestampMillisecondValue(42),
            ),
            (
                ColumnDataType::TimestampMicrosecond,
                ValueData::TimestampMicrosecondValue(42),
            ),
            (
                ColumnDataType::TimestampNanosecond,
                ValueData::TimestampNanosecondValue(42),
            ),
        ] {
            let target_type = ConcreteDataType::from(ColumnDataTypeWrapper::new(datatype, None));
            let schema = Schema::new(vec![
                ColumnSchema::new("ts", target_type, false).with_time_index(true),
            ]);
            let mut column = source("ts", datatype);
            column.semantic_type = SemanticType::Timestamp as i32;
            let rows = Rows {
                schema: vec![column],
                rows: vec![Row {
                    values: vec![Value {
                        value_data: Some(value),
                    }],
                }],
            };
            let batch = rows_to_record_batch(&rows, &table_info(schema.clone())).unwrap();
            assert_eq!(batch.schema(), schema.arrow_schema().clone());
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(batch.column(0).null_count(), 0);
        }
    }

    #[test]
    fn test_json2_expanded_schema() {
        use datatypes::schema::SchemaBuilder;

        let settings = JsonSettings::default();
        let mut column_schema = ColumnSchema::new(
            "data",
            ConcreteDataType::json2(JsonNativeType::object()),
            true,
        );
        column_schema.with_extension_type(&Json2ExtensionType::new(Arc::new(JsonMetadata::new(
            settings.clone(),
        ))));
        let schema = SchemaBuilder::try_from(vec![column_schema])
            .unwrap()
            .add_metadata("test", "metadata")
            .build()
            .unwrap();
        let arrow_schema = schema.arrow_schema().clone();
        let field = arrow_schema.field(0);
        let datatype =
            ColumnDataTypeWrapper::try_from(schema.column_schemas()[0].data_type.clone()).unwrap();
        let (kind, extension) = datatype.to_parts();
        let mut column = source("data", kind);
        column.datatype_extension = extension;
        let rows = Rows {
            schema: vec![column],
            rows: vec![Row {
                values: vec![to_grpc_value(
                    settings.encode(serde_json::json!({"id": 3})).unwrap(),
                )],
            }],
        };
        let batch = rows_to_record_batch(&rows, &table_info(schema)).unwrap();
        // The same arrays fail with the static table schema used before alignment.
        assert!(RecordBatch::try_new(arrow_schema.clone(), batch.columns().to_vec()).is_err());
        let actual_schema = batch.schema();
        assert_eq!(actual_schema.metadata(), arrow_schema.metadata());
        assert_eq!(actual_schema.field(0).metadata(), field.metadata());
        assert_eq!(actual_schema.field(0).name(), field.name());
        assert_eq!(actual_schema.field(0).is_nullable(), field.is_nullable());
        assert_eq!(
            actual_schema.field(0).data_type(),
            batch.column(0).data_type()
        );
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .unwrap();
        assert!(array.column_by_name("id").is_some());
        assert!(array.column_by_name("!__remainder__!").is_some());
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_dynamic_default_is_evaluated_per_conversion() {
        use std::time::{Duration, Instant};

        use arrow::array::TimestampMillisecondArray;

        let (mut rows, base_schema) = fixture();
        let mut columns = base_schema.column_schemas().to_vec();
        columns.push(
            ColumnSchema::new(
                "created",
                ConcreteDataType::timestamp_millisecond_datatype(),
                true,
            )
            .with_default_constraint(Some(ColumnDefaultConstraint::Function(
                "current_timestamp()".to_string(),
            )))
            .unwrap(),
        );
        let schema = Schema::new(columns);
        let timestamp = |batch: &RecordBatch| {
            batch
                .column(3)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .value(0)
        };
        let first = timestamp(&rows_to_record_batch(&rows, &table_info(schema.clone())).unwrap());
        // The default uses wall-clock milliseconds, not Tokio's controllable
        // clock. Poll its observable result with a bound instead of sleeping.
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let next =
                timestamp(&rows_to_record_batch(&rows, &table_info(schema.clone())).unwrap());
            if next != first {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "dynamic default was not refreshed"
            );
            yield_now();
        }
        rows.schema
            .push(source("created", ColumnDataType::TimestampMillisecond));
        rows.rows[0].values.push(Value::default());
        assert!(
            rows_to_record_batch(&rows, &table_info(schema.clone()))
                .unwrap()
                .column(3)
                .is_null(0)
        );
    }

    #[test]
    fn test_tag_and_field_semantics() {
        let (mut rows, schema) = fixture();
        let mut table = table_info(schema);
        table.meta.primary_key_indices = vec![1];
        // The "label" column is a tag in table metadata, not a field.
        assert!(rows_to_record_batch(&rows, &table).is_err());
        rows.schema[0].semantic_type = SemanticType::Tag as i32;
        assert!(rows_to_record_batch(&rows, &table).is_ok());
        // Conversely the count field cannot be submitted as a tag.
        rows.schema[1].semantic_type = SemanticType::Tag as i32;
        assert!(rows_to_record_batch(&rows, &table).is_err());
    }

    #[test]
    fn test_invalid_input() {
        for case in 0..7 {
            let (mut rows, schema) = fixture();
            match case {
                0 => rows.schema[0].column_name = "unknown".to_string(),
                1 => rows.schema[0].column_name = "count".to_string(),
                2 => {
                    rows.rows[0].values.pop();
                }
                3 => rows.schema[1].datatype = ColumnDataType::Float64 as i32,
                4 => rows.rows[0].values[1] = Value::default(),
                5 => rows.schema[1].semantic_type = SemanticType::Timestamp as i32,
                _ => {
                    rows.rows[0].values[1].value_data =
                        Some(ValueData::StringValue("wrong".to_string()))
                }
            }
            assert!(
                rows_to_record_batch(&rows, &table_info(schema.clone())).is_err(),
                "case {case}"
            );
        }
    }

    #[test]
    #[should_panic(expected = "prepared rows must not be empty")]
    fn test_empty_rows_contract() {
        let (mut rows, schema) = fixture();
        rows.rows.clear();
        let _ = rows_to_record_batch(&rows, &table_info(schema));
    }

    #[test]
    fn test_reject_nested_values_for_scalar_column() {
        for value in [
            ValueData::ListValue(api::v1::ListValue { items: vec![] }),
            ValueData::StructValue(api::v1::StructValue { items: vec![] }),
        ] {
            let (mut rows, schema) = fixture();
            rows.rows[0].values[1].value_data = Some(value);
            assert!(matches!(
                rows_to_record_batch(&rows, &table_info(schema)),
                Err(error::Error::InvalidInsertRequest { .. })
            ));
        }
    }

    #[test]
    fn test_nested_value_validation() {
        let int = Value {
            value_data: Some(ValueData::I32Value(1)),
        };
        let list = Value {
            value_data: Some(ValueData::ListValue(api::v1::ListValue {
                items: vec![int.clone()],
            })),
        };
        let structure = Value {
            value_data: Some(ValueData::StructValue(api::v1::StructValue {
                items: vec![int.clone()],
            })),
        };
        let types = [
            (
                ColumnDataTypeWrapper::list_datatype(ColumnDataTypeWrapper::int32_datatype()),
                list.clone(),
            ),
            (
                ColumnDataTypeWrapper::struct_datatype(vec![(
                    "count".to_string(),
                    ColumnDataTypeWrapper::int32_datatype(),
                )]),
                structure.clone(),
            ),
        ];
        for (datatype, valid) in types {
            let (kind, extension) = datatype.to_parts();
            let schema = Schema::new(vec![ColumnSchema::new("nested", datatype.into(), true)]);
            let mut column = source("nested", kind);
            column.datatype_extension = extension;
            let mut rows = Rows {
                schema: vec![column],
                rows: vec![Row {
                    values: vec![valid],
                }],
            };
            let table = table_info(schema);
            assert_eq!(rows_to_record_batch(&rows, &table).unwrap().num_rows(), 1);
            for invalid in [list.clone(), structure.clone()] {
                match rows.rows[0].values[0].value_data.as_mut().unwrap() {
                    ValueData::ListValue(v) => v.items = vec![invalid],
                    ValueData::StructValue(v) => v.items = vec![invalid],
                    _ => unreachable!(),
                }
                assert!(matches!(
                    rows_to_record_batch(&rows, &table),
                    Err(error::Error::InvalidInsertRequest { .. })
                ));
            }
        }
        let datatype = ColumnDataTypeWrapper::list_datatype(ColumnDataTypeWrapper::list_datatype(
            ColumnDataTypeWrapper::int32_datatype(),
        ));
        let (kind, extension) = datatype.to_parts();
        let value = Value {
            value_data: Some(ValueData::ListValue(api::v1::ListValue {
                items: vec![list],
            })),
        };
        assert!(value_matches_type(&value, kind as i32, extension.as_ref()));
        assert!(!value_matches_type(&value, kind as i32, None));
    }

    #[test]
    fn test_sparse_rows_to_record_batch_defaults_nulls_and_pk_order() {
        use arrow::array::{Array, BinaryArray, Float64Array};
        use mito_codec::row_converter::sparse::{SparseOffsetsCache, SparsePrimaryKeyView};

        let schema = Schema::new(vec![
            ColumnSchema::new(
                DATA_SCHEMA_TABLE_ID_COLUMN_NAME,
                ConcreteDataType::uint32_datatype(),
                false,
            ),
            ColumnSchema::new(
                DATA_SCHEMA_TSID_COLUMN_NAME,
                ConcreteDataType::uint64_datatype(),
                false,
            ),
            ColumnSchema::new("tag_b", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("tag_a", ConcreteDataType::string_datatype(), true)
                .with_default_constraint(Some(ColumnDefaultConstraint::Value(DtValue::String(
                    "east".into(),
                ))))
                .unwrap(),
            ColumnSchema::new("tag_req", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new("val", ConcreteDataType::float64_datatype(), true)
                .with_default_constraint(Some(ColumnDefaultConstraint::Value(DtValue::Float64(
                    42.0.into(),
                ))))
                .unwrap(),
        ]);
        let mut info = TableInfoBuilder::default()
            .table_id(7)
            .table_version(0)
            .name("sparse_defaults")
            .meta(
                TableMetaBuilder::empty()
                    .schema(Arc::new(schema))
                    // Non-column-ordered PK (tag_a before tag_b before tag_req).
                    .primary_key_indices(vec![0, 1, 3, 2, 4])
                    .column_ids(vec![
                        ReservedColumnId::table_id(),
                        ReservedColumnId::tsid(),
                        10,
                        11,
                        12,
                        13,
                        14,
                    ])
                    .next_column_id(15)
                    .engine("mito")
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();
        info.meta.options.extra_options.insert(
            store_api::metric_engine_consts::PRIMARY_KEY_ENCODING.to_string(),
            "SpArSe".to_string(),
        );

        let tag_col = |name: &str| ProtoColumnSchema {
            column_name: name.to_string(),
            datatype: ColumnDataType::String as i32,
            semantic_type: SemanticType::Tag as i32,
            ..Default::default()
        };
        let tid_col = ProtoColumnSchema {
            column_name: DATA_SCHEMA_TABLE_ID_COLUMN_NAME.to_string(),
            datatype: ColumnDataType::Uint32 as i32,
            semantic_type: SemanticType::Tag as i32,
            ..Default::default()
        };
        let tsid_col = ProtoColumnSchema {
            column_name: DATA_SCHEMA_TSID_COLUMN_NAME.to_string(),
            datatype: ColumnDataType::Uint64 as i32,
            semantic_type: SemanticType::Tag as i32,
            ..Default::default()
        };
        let ts_col = ProtoColumnSchema {
            column_name: "ts".to_string(),
            datatype: ColumnDataType::TimestampMillisecond as i32,
            semantic_type: SemanticType::Timestamp as i32,
            ..Default::default()
        };

        // Missing required non-nullable tag fails.
        let missing_req = Rows {
            schema: vec![tid_col.clone(), tsid_col.clone(), ts_col.clone()],
            rows: vec![Row {
                values: vec![
                    ValueData::U32Value(1).into(),
                    ValueData::U64Value(2).into(),
                    ValueData::TimestampMillisecondValue(100).into(),
                ],
            }],
        };
        let err = sparse_rows_to_record_batch(&missing_req, &info).unwrap_err();
        assert!(
            err.to_string().contains("Missing required column tag_req"),
            "{err}"
        );

        // Explicit NULL on non-nullable tag fails.
        let null_req = Rows {
            schema: vec![
                tid_col.clone(),
                tsid_col.clone(),
                tag_col("tag_req"),
                ts_col.clone(),
            ],
            rows: vec![Row {
                values: vec![
                    ValueData::U32Value(1).into(),
                    ValueData::U64Value(2).into(),
                    Value::default(),
                    ValueData::TimestampMillisecondValue(100).into(),
                ],
            }],
        };
        let err = sparse_rows_to_record_batch(&null_req, &info).unwrap_err();
        assert!(
            err.to_string()
                .contains("Null supplied for non-nullable column tag_req"),
            "{err}"
        );

        // Row 0 omits tag_a (uses DEFAULT 'east') and val (DEFAULT 42.0), supplies tag_b="".
        let rows_omitted_default = Rows {
            schema: vec![
                tid_col.clone(),
                tsid_col.clone(),
                tag_col("tag_b"),
                tag_col("tag_req"),
                ts_col.clone(),
            ],
            rows: vec![Row {
                values: vec![
                    ValueData::U32Value(11).into(),
                    ValueData::U64Value(22).into(),
                    ValueData::StringValue(String::new()).into(),
                    ValueData::StringValue("req0".into()).into(),
                    ValueData::TimestampMillisecondValue(100).into(),
                ],
            }],
        };
        let batch0 = sparse_rows_to_record_batch(&rows_omitted_default, &info).unwrap();
        assert_eq!(batch0.num_columns(), 3);
        let pk0 = batch0
            .column_by_name(PRIMARY_KEY_COLUMN_NAME)
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let val0 = batch0
            .column_by_name("val")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(val0.value(0), 42.0);
        let codec = SparsePrimaryKeyCodec::schemaless();
        assert_eq!(codec.decode_ids(pk0.value(0)).unwrap(), (11, 22));
        let mut cache = SparseOffsetsCache::new();
        let mut buf = Vec::new();
        let mut view = SparsePrimaryKeyView::new(pk0.value(0), &mut cache).unwrap();
        // Verify tag_a (col 11) precedes tag_b (col 10) matching catalog primary_key_indices.
        let pos_11 = pk0
            .value(0)
            .windows(4)
            .position(|w| w == 11_u32.to_be_bytes())
            .unwrap();
        let pos_10 = pk0
            .value(0)
            .windows(4)
            .position(|w| w == 10_u32.to_be_bytes())
            .unwrap();
        assert!(pos_11 < pos_10);
        assert_eq!(view.label(11, &mut buf).unwrap(), Some("east"));
        assert_eq!(view.label(10, &mut buf).unwrap(), Some(""));
        assert_eq!(view.label(12, &mut buf).unwrap(), Some("req0"));

        // Explicit NULL on DEFAULT 'east' column tag_a stays NULL (omitted from __primary_key).
        let rows_explicit_null = Rows {
            schema: vec![
                tid_col,
                tsid_col,
                tag_col("tag_a"),
                tag_col("tag_b"),
                tag_col("tag_req"),
                ts_col,
            ],
            rows: vec![Row {
                values: vec![
                    ValueData::U32Value(11).into(),
                    ValueData::U64Value(22).into(),
                    Value::default(),
                    Value::default(),
                    ValueData::StringValue("req1".into()).into(),
                    ValueData::TimestampMillisecondValue(200).into(),
                ],
            }],
        };
        let batch1 = sparse_rows_to_record_batch(&rows_explicit_null, &info).unwrap();
        let pk1 = batch1
            .column_by_name(PRIMARY_KEY_COLUMN_NAME)
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let mut view1 = SparsePrimaryKeyView::new(pk1.value(0), &mut cache).unwrap();
        assert_eq!(view1.label(11, &mut buf).unwrap(), None);
        assert_eq!(view1.label(10, &mut buf).unwrap(), None);
        assert_eq!(view1.label(12, &mut buf).unwrap(), Some("req1"));
    }

    #[test]
    fn test_sparse_conversion_estimate_omitted_and_default_tags() {
        let make_table = |with_default: bool| {
            let mut columns = vec![
                ColumnSchema::new(
                    DATA_SCHEMA_TABLE_ID_COLUMN_NAME,
                    ConcreteDataType::uint32_datatype(),
                    false,
                ),
                ColumnSchema::new(
                    DATA_SCHEMA_TSID_COLUMN_NAME,
                    ConcreteDataType::uint64_datatype(),
                    false,
                ),
            ];
            for i in 0..128 {
                let mut col = ColumnSchema::new(
                    format!("tag_{i}"),
                    ConcreteDataType::string_datatype(),
                    true,
                );
                if with_default {
                    col = col
                        .with_default_constraint(Some(ColumnDefaultConstraint::Value(
                            DtValue::String("prod".into()),
                        )))
                        .unwrap();
                }
                columns.push(col);
            }
            columns.push(
                ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                )
                .with_time_index(true),
            );
            columns.push(ColumnSchema::new(
                "val",
                ConcreteDataType::float64_datatype(),
                true,
            ));
            let pk_indices: Vec<usize> = (0..130).collect();
            let column_ids: Vec<u32> = std::iter::once(ReservedColumnId::table_id())
                .chain(std::iter::once(ReservedColumnId::tsid()))
                .chain(10..140)
                .collect();
            let mut info = TableInfoBuilder::default()
                .table_id(9)
                .table_version(0)
                .name("sparse_wide")
                .meta(
                    TableMetaBuilder::empty()
                        .schema(Arc::new(Schema::new(columns)))
                        .primary_key_indices(pk_indices)
                        .column_ids(column_ids)
                        .next_column_id(150)
                        .engine("mito")
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap();
            info.meta.options.extra_options.insert(
                store_api::metric_engine_consts::PRIMARY_KEY_ENCODING.to_string(),
                "sparse".to_string(),
            );
            info
        };

        let make_rows = |include_explicit_nulls: bool| {
            let mut schema = vec![
                ProtoColumnSchema {
                    column_name: DATA_SCHEMA_TABLE_ID_COLUMN_NAME.to_string(),
                    datatype: ColumnDataType::Uint32 as i32,
                    semantic_type: SemanticType::Tag as i32,
                    ..Default::default()
                },
                ProtoColumnSchema {
                    column_name: DATA_SCHEMA_TSID_COLUMN_NAME.to_string(),
                    datatype: ColumnDataType::Uint64 as i32,
                    semantic_type: SemanticType::Tag as i32,
                    ..Default::default()
                },
            ];
            if include_explicit_nulls {
                for i in 0..128 {
                    schema.push(ProtoColumnSchema {
                        column_name: format!("tag_{i}"),
                        datatype: ColumnDataType::String as i32,
                        semantic_type: SemanticType::Tag as i32,
                        ..Default::default()
                    });
                }
            }
            schema.push(ProtoColumnSchema {
                column_name: "ts".to_string(),
                datatype: ColumnDataType::TimestampMillisecond as i32,
                semantic_type: SemanticType::Timestamp as i32,
                ..Default::default()
            });
            schema.push(source("val", ColumnDataType::Float64));

            let rows = (0..1000)
                .map(|i| {
                    let mut values =
                        vec![ValueData::U32Value(1).into(), ValueData::U64Value(i).into()];
                    if include_explicit_nulls {
                        values.extend(std::iter::repeat_n(Value::default(), 128));
                    }
                    values.push(ValueData::TimestampMillisecondValue(i as i64).into());
                    values.push(ValueData::F64Value(i as f64).into());
                    Row { values }
                })
                .collect();
            Rows { schema, rows }
        };

        let nullable_table = make_table(false);
        let omitted_rows = make_rows(false);
        let omitted_estimate =
            estimated_sparse_record_batch_bytes(&omitted_rows, &nullable_table).unwrap();
        let omitted_batch = sparse_rows_to_record_batch(&omitted_rows, &nullable_table).unwrap();
        assert!(omitted_estimate < 512 * 1024, "estimate={omitted_estimate}");
        assert!(omitted_estimate >= omitted_batch.get_array_memory_size());

        let explicit_null_rows = make_rows(true);
        let null_estimate =
            estimated_sparse_record_batch_bytes(&explicit_null_rows, &nullable_table).unwrap();
        let null_batch = sparse_rows_to_record_batch(&explicit_null_rows, &nullable_table).unwrap();
        assert!(null_estimate < 512 * 1024, "estimate={null_estimate}");
        assert!(null_estimate >= null_batch.get_array_memory_size());

        let default_table = make_table(true);
        let default_estimate =
            estimated_sparse_record_batch_bytes(&omitted_rows, &default_table).unwrap();
        let default_batch = sparse_rows_to_record_batch(&omitted_rows, &default_table).unwrap();
        assert!(
            default_estimate > 4 * 1024 * 1024,
            "estimate={default_estimate}"
        );
        assert!(default_estimate >= default_batch.get_array_memory_size());
    }

    #[test]
    fn test_exemplars_to_sparse_record_batch_wide_catalog_and_pk_order() {
        use std::collections::BTreeMap;

        use arrow::array::BinaryArray;
        use common_catalog::consts::METRICS_EXEMPLARS_TABLE;
        use common_query::prelude::greptime_timestamp;
        use mito_codec::row_converter::sparse::{SparseOffsetsCache, SparsePrimaryKeyView};

        use crate::insert::Exemplar;

        let mut labels_col = ColumnSchema::new(
            greptime_exemplar_labels(),
            ConcreteDataType::json2(JsonNativeType::Null),
            true,
        );
        labels_col.with_extension_type(&Json2ExtensionType::new(Arc::new(JsonMetadata::new(
            JsonSettings::new_v2(),
        ))));
        let mut columns = vec![
            ColumnSchema::new(
                DATA_SCHEMA_TABLE_ID_COLUMN_NAME,
                ConcreteDataType::uint32_datatype(),
                false,
            ),
            ColumnSchema::new(
                DATA_SCHEMA_TSID_COLUMN_NAME,
                ConcreteDataType::uint64_datatype(),
                false,
            ),
            ColumnSchema::new(
                EXEMPLAR_METRIC_COLUMN,
                ConcreteDataType::string_datatype(),
                true,
            ),
            ColumnSchema::new("zone", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("job", ConcreteDataType::string_datatype(), true),
        ];
        for i in 0..2048 {
            columns.push(ColumnSchema::new(
                format!("unused_{i}"),
                ConcreteDataType::string_datatype(),
                true,
            ));
        }
        columns.push(
            ColumnSchema::new(
                greptime_timestamp(),
                ConcreteDataType::timestamp_nanosecond_datatype(),
                false,
            )
            .with_time_index(true),
        );
        columns.push(ColumnSchema::new(
            greptime_value(),
            ConcreteDataType::float64_datatype(),
            true,
        ));
        columns.push(labels_col);

        // Non-column-ordered primary key: (table_id, tsid, job, metric, zone, unused...)
        let mut pk_indices = vec![0, 1, 4, 2, 3];
        pk_indices.extend(5..2053);
        let column_ids: Vec<u32> = std::iter::once(ReservedColumnId::table_id())
            .chain(std::iter::once(ReservedColumnId::tsid()))
            .chain(10..10 + (columns.len() as u32 - 2))
            .collect();
        let next_col_id = *column_ids.last().unwrap() + 1;
        let info = TableInfoBuilder::default()
            .table_id(1)
            .table_version(0)
            .name(METRICS_EXEMPLARS_TABLE)
            .meta(
                TableMetaBuilder::empty()
                    .schema(Arc::new(Schema::new(columns)))
                    .primary_key_indices(pk_indices)
                    .column_ids(column_ids)
                    .next_column_id(next_col_id)
                    .engine("mito")
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();

        let shared_series = Arc::new(BTreeMap::from([
            ("job".to_string(), "api".to_string()),
            ("zone".to_string(), "z1".to_string()),
        ]));
        let other_series = Arc::new(BTreeMap::from([("zone".to_string(), "".to_string())]));
        let batch_input = ExemplarBatch::try_new(vec![
            Exemplar {
                metric: "cpu".into(),
                series_labels: shared_series.clone(),
                timestamp: 10,
                value: 1.0,
                labels: BTreeMap::from([("trace_id".into(), "t0".into())]),
            },
            Exemplar {
                metric: "cpu".into(),
                series_labels: shared_series.clone(),
                timestamp: 20,
                value: 2.0,
                labels: BTreeMap::from([("trace_id".into(), "t1".into())]),
            },
            Exemplar {
                metric: "mem".into(),
                series_labels: shared_series,
                timestamp: 30,
                value: 3.0,
                labels: BTreeMap::new(),
            },
            Exemplar {
                metric: "mem".into(),
                series_labels: other_series,
                timestamp: 40,
                value: 4.0,
                labels: BTreeMap::new(),
            },
        ])
        .unwrap();
        let resolved = ahash::HashMap::from_iter([("cpu".into(), 100), ("mem".into(), 200)]);
        let record_batch =
            exemplars_to_sparse_record_batch(&batch_input, &resolved, &info).unwrap();
        let pk_array = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(pk_array.value(0), pk_array.value(1));

        let mut cache = SparseOffsetsCache::new();
        let mut buf = Vec::new();
        // Row 0 encodes job (col 12), metric (col 10), zone (col 11) in catalog PK rank order.
        let pk0 = pk_array.value(0);
        let pos_job = pk0
            .windows(4)
            .position(|w| w == 12_u32.to_be_bytes())
            .unwrap();
        let pos_metric = pk0
            .windows(4)
            .position(|w| w == 10_u32.to_be_bytes())
            .unwrap();
        let pos_zone = pk0
            .windows(4)
            .position(|w| w == 11_u32.to_be_bytes())
            .unwrap();
        assert!(pos_job < pos_metric && pos_metric < pos_zone);

        let mut view2 = SparsePrimaryKeyView::new(pk_array.value(2), &mut cache).unwrap();
        assert_eq!(view2.table_id(), 200);
        assert_eq!(view2.label(10, &mut buf).unwrap(), Some("mem"));
        assert_eq!(view2.label(12, &mut buf).unwrap(), Some("api"));
        assert_eq!(view2.label(11, &mut buf).unwrap(), Some("z1"));

        let mut view3 = SparsePrimaryKeyView::new(pk_array.value(3), &mut cache).unwrap();
        assert_eq!(view3.table_id(), 200);
        assert_eq!(view3.label(10, &mut buf).unwrap(), Some("mem"));
        assert_eq!(view3.label(12, &mut buf).unwrap(), None);
        assert_eq!(view3.label(11, &mut buf).unwrap(), Some(""));
    }

    #[test]
    fn test_exemplar_label_path_estimation() {
        use std::collections::BTreeMap;

        use common_catalog::consts::METRICS_EXEMPLARS_TABLE;
        use common_query::prelude::greptime_timestamp;

        use crate::insert::Exemplar;

        let make_exemplar_table = |settings: JsonSettings| {
            let mut labels_col = ColumnSchema::new(
                greptime_exemplar_labels(),
                ConcreteDataType::json2(JsonNativeType::Null),
                true,
            );
            labels_col.with_extension_type(&Json2ExtensionType::new(Arc::new(JsonMetadata::new(
                settings,
            ))));
            let schema = Schema::new(vec![
                ColumnSchema::new(
                    DATA_SCHEMA_TABLE_ID_COLUMN_NAME,
                    ConcreteDataType::uint32_datatype(),
                    false,
                ),
                ColumnSchema::new(
                    DATA_SCHEMA_TSID_COLUMN_NAME,
                    ConcreteDataType::uint64_datatype(),
                    false,
                ),
                ColumnSchema::new(
                    EXEMPLAR_METRIC_COLUMN,
                    ConcreteDataType::string_datatype(),
                    true,
                ),
                ColumnSchema::new(
                    greptime_timestamp(),
                    ConcreteDataType::timestamp_nanosecond_datatype(),
                    false,
                )
                .with_time_index(true),
                ColumnSchema::new(greptime_value(), ConcreteDataType::float64_datatype(), true),
                labels_col,
            ]);
            TableInfoBuilder::default()
                .table_id(1)
                .table_version(0)
                .name(METRICS_EXEMPLARS_TABLE)
                .meta(
                    TableMetaBuilder::empty()
                        .schema(Arc::new(schema))
                        .primary_key_indices(vec![0, 1, 2])
                        .column_ids(vec![
                            ReservedColumnId::table_id(),
                            ReservedColumnId::tsid(),
                            10,
                            11,
                            12,
                            13,
                        ])
                        .next_column_id(14)
                        .engine("mito")
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap()
        };

        let table = make_exemplar_table(JsonSettings::new_v2());
        let resolved = ahash::HashMap::from_iter([("cpu".into(), 100)]);
        let shared = Arc::new(BTreeMap::new());
        let repeated = ExemplarBatch::try_new(
            (0..1000)
                .map(|i| Exemplar {
                    metric: "cpu".into(),
                    series_labels: shared.clone(),
                    timestamp: i,
                    value: i as f64,
                    labels: BTreeMap::from([("trace_id".into(), format!("trace_{i}"))]),
                })
                .collect(),
        )
        .unwrap();
        let distinct = ExemplarBatch::try_new(
            (0..1000)
                .map(|i| Exemplar {
                    metric: "cpu".into(),
                    series_labels: shared.clone(),
                    timestamp: i,
                    value: i as f64,
                    labels: BTreeMap::from([(format!("label_{i}"), "val".into())]),
                })
                .collect(),
        )
        .unwrap();

        for batch in [&repeated, &distinct] {
            let estimate = estimated_exemplar_record_batch_bytes(batch, &table).unwrap();
            let actual = exemplars_to_sparse_record_batch(batch, &resolved, &table).unwrap();
            assert!(estimate >= actual.get_array_memory_size());
        }
    }

    #[test]
    fn test_sparse_rows_to_record_batch_skips_omitted_null_tags_and_preserves_defaults() {
        use arrow::array::BinaryArray;
        use mito_codec::row_converter::sparse::{SparseOffsetsCache, SparsePrimaryKeyView};

        let build_table = |with_required_tag: bool| {
            let mut columns = vec![
                ColumnSchema::new(
                    DATA_SCHEMA_TABLE_ID_COLUMN_NAME,
                    ConcreteDataType::uint32_datatype(),
                    false,
                ),
                ColumnSchema::new(
                    DATA_SCHEMA_TSID_COLUMN_NAME,
                    ConcreteDataType::uint64_datatype(),
                    false,
                ),
                ColumnSchema::new("host", ConcreteDataType::string_datatype(), true),
                ColumnSchema::new("site", ConcreteDataType::string_datatype(), true)
                    .with_default_constraint(Some(ColumnDefaultConstraint::Value(DtValue::String(
                        "east".into(),
                    ))))
                    .unwrap(),
                ColumnSchema::new("empty_def", ConcreteDataType::string_datatype(), true)
                    .with_default_constraint(Some(ColumnDefaultConstraint::Value(DtValue::String(
                        String::new().into(),
                    ))))
                    .unwrap(),
            ];
            for i in 0..128 {
                columns.push(ColumnSchema::new(
                    format!("opt_{i}"),
                    ConcreteDataType::string_datatype(),
                    true,
                ));
            }
            if with_required_tag {
                columns.push(ColumnSchema::new(
                    "req_tag",
                    ConcreteDataType::string_datatype(),
                    false,
                ));
            }
            columns.push(
                ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                )
                .with_time_index(true),
            );
            columns.push(ColumnSchema::new(
                "val",
                ConcreteDataType::float64_datatype(),
                true,
            ));

            let opt_end = 5 + 128;
            // Non-column-ordered primary key: (__table_id, __tsid, site, host, empty_def, opt_0..opt_127[, req_tag])
            let mut pk_indices = vec![0, 1, 3, 2, 4];
            pk_indices.extend(5..opt_end);
            if with_required_tag {
                pk_indices.push(opt_end);
            }
            let column_ids: Vec<u32> = std::iter::once(ReservedColumnId::table_id())
                .chain(std::iter::once(ReservedColumnId::tsid()))
                .chain(10..10 + (columns.len() as u32 - 2))
                .collect();
            let next_column_id = *column_ids.last().unwrap() + 1;
            let mut info = TableInfoBuilder::default()
                .table_id(15)
                .table_version(0)
                .name("sparse_skipped_nulls")
                .meta(
                    TableMetaBuilder::empty()
                        .schema(Arc::new(Schema::new(columns)))
                        .primary_key_indices(pk_indices)
                        .column_ids(column_ids)
                        .next_column_id(next_column_id)
                        .engine("mito")
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap();
            info.meta.options.extra_options.insert(
                store_api::metric_engine_consts::PRIMARY_KEY_ENCODING.to_string(),
                "sparse".to_string(),
            );
            info
        };

        let table = build_table(false);
        let req_table = build_table(true);

        let tid_col = ProtoColumnSchema {
            column_name: DATA_SCHEMA_TABLE_ID_COLUMN_NAME.to_string(),
            datatype: ColumnDataType::Uint32 as i32,
            semantic_type: SemanticType::Tag as i32,
            ..Default::default()
        };
        let tsid_col = ProtoColumnSchema {
            column_name: DATA_SCHEMA_TSID_COLUMN_NAME.to_string(),
            datatype: ColumnDataType::Uint64 as i32,
            semantic_type: SemanticType::Tag as i32,
            ..Default::default()
        };
        let tag_col = |name: &str| ProtoColumnSchema {
            column_name: name.to_string(),
            datatype: ColumnDataType::String as i32,
            semantic_type: SemanticType::Tag as i32,
            ..Default::default()
        };
        let ts_col = ProtoColumnSchema {
            column_name: "ts".to_string(),
            datatype: ColumnDataType::TimestampMillisecond as i32,
            semantic_type: SemanticType::Timestamp as i32,
            ..Default::default()
        };

        let omitted_rows = Rows {
            schema: vec![
                tid_col.clone(),
                tsid_col.clone(),
                tag_col("host"),
                ts_col.clone(),
                source("val", ColumnDataType::Float64),
            ],
            rows: vec![
                Row {
                    values: vec![
                        ValueData::U32Value(15).into(),
                        ValueData::U64Value(101).into(),
                        ValueData::StringValue(String::new()).into(),
                        ValueData::TimestampMillisecondValue(1).into(),
                        ValueData::F64Value(1.0).into(),
                    ],
                },
                Row {
                    values: vec![
                        ValueData::U32Value(15).into(),
                        ValueData::U64Value(102).into(),
                        Value::default(),
                        ValueData::TimestampMillisecondValue(2).into(),
                        ValueData::F64Value(2.0).into(),
                    ],
                },
            ],
        };

        // Required non-nullable omitted tag still fails even when nullable tags are skipped.
        let err = sparse_rows_to_record_batch(&omitted_rows, &req_table).unwrap_err();
        assert!(
            err.to_string().contains("Missing required column req_tag"),
            "{err}"
        );

        let omitted_batch = sparse_rows_to_record_batch(&omitted_rows, &table).unwrap();

        // Explicit NULLs for opt_0..opt_127 must encode byte-for-byte identically to omitting them.
        let mut explicit_schema = vec![tid_col, tsid_col, tag_col("host")];
        explicit_schema.extend((0..128).map(|i| tag_col(&format!("opt_{i}"))));
        explicit_schema.push(ts_col);
        explicit_schema.push(source("val", ColumnDataType::Float64));
        let explicit_rows = Rows {
            schema: explicit_schema,
            rows: omitted_rows
                .rows
                .iter()
                .map(|row| {
                    let mut values = row.values[..3].to_vec();
                    values.extend(std::iter::repeat_n(Value::default(), 128));
                    values.extend_from_slice(&row.values[3..]);
                    Row { values }
                })
                .collect(),
        };
        let explicit_batch = sparse_rows_to_record_batch(&explicit_rows, &table).unwrap();
        assert_eq!(omitted_batch, explicit_batch);

        let pk_col = omitted_batch
            .column_by_name(PRIMARY_KEY_COLUMN_NAME)
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        // Column IDs: host=10, site=11, empty_def=12, opt_0=13 .. opt_127=140
        let pk0 = pk_col.value(0);
        let pos_site = pk0
            .windows(4)
            .position(|w| w == 11_u32.to_be_bytes())
            .unwrap();
        let pos_host = pk0
            .windows(4)
            .position(|w| w == 10_u32.to_be_bytes())
            .unwrap();
        let pos_empty_def = pk0
            .windows(4)
            .position(|w| w == 12_u32.to_be_bytes())
            .unwrap();
        assert!(pos_site < pos_host && pos_host < pos_empty_def);

        let mut cache = SparseOffsetsCache::new();
        let mut buf = Vec::new();
        let mut view0 = SparsePrimaryKeyView::new(pk0, &mut cache).unwrap();
        assert_eq!(view0.label(11, &mut buf).unwrap(), Some("east"));
        assert_eq!(view0.label(10, &mut buf).unwrap(), Some(""));
        assert_eq!(view0.label(12, &mut buf).unwrap(), Some(""));
        assert_eq!(view0.label(13, &mut buf).unwrap(), None);
        assert_eq!(view0.label(140, &mut buf).unwrap(), None);

        let mut view1 = SparsePrimaryKeyView::new(pk_col.value(1), &mut cache).unwrap();
        assert_eq!(view1.label(11, &mut buf).unwrap(), Some("east"));
        assert_eq!(view1.label(10, &mut buf).unwrap(), None);
        assert_eq!(view1.label(12, &mut buf).unwrap(), Some(""));
        assert_eq!(view1.label(13, &mut buf).unwrap(), None);
    }
}
