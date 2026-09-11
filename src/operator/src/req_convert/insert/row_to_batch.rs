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

use std::collections::HashMap;

use api::helper::{ColumnDataTypeWrapper, pb_value_to_value_ref};
use api::v1::{Rows, SemanticType};
use arrow::record_batch::RecordBatch;
use common_error::ext::BoxedError;
use datatypes::data_type::ConcreteDataType;
use snafu::{OptionExt, ResultExt, ensure};
use table::metadata::TableInfo;

use crate::error::{self, Result};

/// Converts prepared rows in target-schema order without Prom-specific renaming.
///
/// Missing columns evaluate their defaults once per conversion. Explicit nulls
/// remain nulls. Semantic types are checked against the table time index and
/// primary-key indices.
pub fn rows_to_record_batch(rows: &Rows, table_info: &TableInfo) -> Result<RecordBatch> {
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

    let mut arrays = Vec::with_capacity(schema.num_columns());
    for column in schema.column_schemas() {
        let vector = if let Some(&index) = source_columns.get(column.name.as_str()) {
            let mut builder = column.create_mutable_vector(rows.rows.len());
            for row in &rows.rows {
                let value = &row.values[index];
                ensure!(
                    value.value_data.is_some() || column.is_nullable(),
                    error::InvalidInsertRequestSnafu {
                        reason: format!("Null supplied for non-nullable column {}", column.name),
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
            builder.to_vector()
        } else {
            column
                .create_default_vector(rows.rows.len())
                .map_err(BoxedError::new)
                .context(error::ExternalSnafu)?
                .with_context(|| error::InvalidInsertRequestSnafu {
                    reason: format!("Missing required column {}", column.name),
                })?
        };
        arrays.push(vector.to_arrow_array());
    }
    RecordBatch::try_new(schema.arrow_schema().clone(), arrays).context(error::ComputeArrowSnafu)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread::yield_now;

    use api::v1::value::ValueData;
    use api::v1::{ColumnDataType, ColumnSchema as ProtoColumnSchema, Row, Value};
    use arrow::array::{Array, Int32Array, StringArray};
    use datatypes::schema::{ColumnDefaultConstraint, ColumnSchema, Schema};
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
}
