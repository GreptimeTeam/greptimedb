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

//! Prometheus row-level helpers for converting proto `Rows` into Arrow
//! `RecordBatch`es and aligning / normalizing their schemas against
//! existing table schemas in the catalog.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use api::helper::ColumnDataTypeWrapper;
use api::v1::value::ValueData;
use api::v1::{ColumnSchema, Rows, SemanticType};
use arrow::array::{
    ArrayRef, Float64Builder, StringBuilder, TimestampMicrosecondBuilder,
    TimestampMillisecondBuilder, TimestampNanosecondBuilder, TimestampSecondBuilder,
    new_null_array,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use arrow_schema::TimeUnit;
use common_query::prelude::{greptime_timestamp, greptime_value};
use datatypes::data_type::DataType;
use datatypes::prelude::ConcreteDataType;
use snafu::{OptionExt, ResultExt, ensure};

use crate::error;
use crate::error::Result;

/// Extract timestamp, field, and tag column names from a logical region schema.
fn unzip_logical_region_schema(
    target_schema: &ArrowSchema,
) -> Result<(String, String, HashSet<String>)> {
    let mut timestamp_column = None;
    let mut field_column = None;
    let mut tag_columns = HashSet::with_capacity(target_schema.fields.len().saturating_sub(2));
    for field in target_schema.fields() {
        if field.name() == greptime_timestamp() {
            timestamp_column = Some(field.name().clone());
            continue;
        }

        if field.name() == greptime_value() {
            field_column = Some(field.name().clone());
            continue;
        }

        if timestamp_column.is_none() && matches!(field.data_type(), ArrowDataType::Timestamp(_, _))
        {
            timestamp_column = Some(field.name().clone());
            continue;
        }

        if field_column.is_none() && matches!(field.data_type(), ArrowDataType::Float64) {
            field_column = Some(field.name().clone());
            continue;
        }
        tag_columns.insert(field.name().clone());
    }

    let timestamp_column = timestamp_column.with_context(|| error::UnexpectedResultSnafu {
        reason: "Failed to locate timestamp column in target schema".to_string(),
    })?;
    let field_column = field_column.with_context(|| error::UnexpectedResultSnafu {
        reason: "Failed to locate field column in target schema".to_string(),
    })?;

    Ok((timestamp_column, field_column, tag_columns))
}

/// Directly converts proto `Rows` into a `RecordBatch` aligned to the given
/// `target_schema`, handling Prometheus column renaming (timestamp/value),
/// reordering, type casting, and null-filling in a single pass.
pub(crate) fn rows_to_aligned_record_batch(
    rows: &Rows,
    target_schema: &ArrowSchema,
) -> Result<RecordBatch> {
    let row_count = rows.rows.len();
    let column_count = rows.schema.len();

    for (idx, row) in rows.rows.iter().enumerate() {
        ensure!(
            row.values.len() == column_count,
            error::InternalSnafu {
                err_msg: format!(
                    "Column count mismatch in row {}, expected {}, got {}",
                    idx,
                    column_count,
                    row.values.len()
                )
            }
        );
    }

    let (target_ts_name, target_field_name, _target_tags) =
        unzip_logical_region_schema(target_schema)?;

    // Map effective target column name → (source column index, source arrow type).
    // Handles prom renames: Timestamp → target ts name, Float64 → target field name.
    let mut source_map: HashMap<&str, (usize, ArrowDataType)> =
        HashMap::with_capacity(rows.schema.len());

    for (src_idx, col) in rows.schema.iter().enumerate() {
        let wrapper = ColumnDataTypeWrapper::try_new(col.datatype, col.datatype_extension.clone())?;
        let src_arrow_type = ConcreteDataType::from(wrapper).as_arrow_type();

        match &src_arrow_type {
            ArrowDataType::Float64 => {
                source_map.insert(&target_field_name, (src_idx, src_arrow_type));
            }
            ArrowDataType::Timestamp(unit, _) => {
                ensure!(
                    unit == &TimeUnit::Millisecond,
                    error::InvalidPromRemoteRequestSnafu {
                        msg: format!(
                            "Unexpected remote write batch timestamp unit, expect millisecond, got: {}",
                            unit
                        )
                    }
                );
                source_map.insert(&target_ts_name, (src_idx, src_arrow_type));
            }
            ArrowDataType::Utf8 => {
                source_map.insert(&col.column_name, (src_idx, src_arrow_type));
            }
            other => {
                return error::InvalidPromRemoteRequestSnafu {
                    msg: format!(
                        "Unexpected remote write batch field type {}, field name: {}",
                        other, col.column_name
                    ),
                }
                .fail();
            }
        }
    }

    // Build columns in target schema order
    let mut columns = Vec::with_capacity(target_schema.fields().len());
    for target_field in target_schema.fields() {
        if let Some((src_idx, src_arrow_type)) = source_map.get(target_field.name().as_str()) {
            let array = build_arrow_array(
                rows,
                *src_idx,
                &rows.schema[*src_idx].column_name,
                src_arrow_type.clone(),
                row_count,
            )?;
            if array.data_type() != target_field.data_type() {
                columns.push(
                    cast(array.as_ref(), target_field.data_type()).context(error::ArrowSnafu)?,
                );
            } else {
                columns.push(array);
            }
        } else {
            columns.push(new_null_array(target_field.data_type(), row_count));
        }
    }

    let batch = RecordBatch::try_new(Arc::new(target_schema.clone()), columns)
        .context(error::ArrowSnafu)?;
    Ok(batch)
}

/// Identify tag columns in the proto `rows_schema` that are absent from the
/// target region schema, without building an intermediate `RecordBatch`.
pub(crate) fn identify_missing_columns_from_proto(
    rows_schema: &[ColumnSchema],
    target_schema: &ArrowSchema,
) -> Result<Vec<String>> {
    let (_, _, target_tags) = unzip_logical_region_schema(target_schema)?;
    let mut missing = Vec::new();
    for col in rows_schema {
        let wrapper = ColumnDataTypeWrapper::try_new(col.datatype, col.datatype_extension.clone())?;
        let arrow_type = ConcreteDataType::from(wrapper).as_arrow_type();
        if matches!(arrow_type, ArrowDataType::Utf8)
            && !target_tags.contains(&col.column_name)
            && target_schema.column_with_name(&col.column_name).is_none()
        {
            missing.push(col.column_name.clone());
        }
    }
    Ok(missing)
}

/// Build a `Vec<ColumnSchema>` suitable for creating a new Prometheus logical table
/// directly from the proto `rows.schema`, avoiding the round-trip through Arrow schema.
pub fn build_prom_create_table_schema_from_proto(
    rows_schema: &[ColumnSchema],
) -> Result<Vec<ColumnSchema>> {
    rows_schema
        .iter()
        .map(|col| {
            let semantic_type = if col.datatype == api::v1::ColumnDataType::TimestampMillisecond as i32 {
                SemanticType::Timestamp
            } else if col.datatype == api::v1::ColumnDataType::Float64 as i32 {
                SemanticType::Field
            } else {
                // tag columns must be String type
                ensure!(col.datatype == api::v1::ColumnDataType::String as i32, error::InvalidPromRemoteRequestSnafu{
                                        msg: format!(
                        "Failed to build create table schema, tag column '{}' must be String but got datatype {}",
                        col.column_name, col.datatype
                    )
                });
                SemanticType::Tag
            };

            Ok(ColumnSchema {
                column_name: col.column_name.clone(),
                datatype: col.datatype,
                semantic_type: semantic_type as i32,
                datatype_extension: col.datatype_extension.clone(),
                options: None,
            })
        })
        .collect()
}

/// Build a single Arrow array for the given column index from proto `Rows`.
fn build_arrow_array(
    rows: &Rows,
    col_idx: usize,
    column_name: &String,
    column_data_type: arrow::datatypes::DataType,
    row_count: usize,
) -> Result<ArrayRef> {
    macro_rules! build_array {
        ($builder:expr, $( $pattern:pat => $value:expr ),+ $(,)?) => {{
            let mut builder = $builder;
            for row in &rows.rows {
                match row.values[col_idx].value_data.as_ref() {
                    $(Some($pattern) => builder.append_value($value),)+
                    Some(v) => {
                        return error::InvalidPromRemoteRequestSnafu {
                            msg: format!("Unexpected value: {:?}", v),
                        }
                        .fail();
                    }
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish()) as ArrayRef
        }};
    }

    let array: ArrayRef = match column_data_type {
        arrow::datatypes::DataType::Float64 => {
            build_array!(Float64Builder::with_capacity(row_count), ValueData::F64Value(v) => *v)
        }
        arrow::datatypes::DataType::Utf8 => build_array!(
            StringBuilder::with_capacity(row_count, 0),
            ValueData::StringValue(v) => v
        ),
        arrow::datatypes::DataType::Timestamp(u, _) => match u {
            TimeUnit::Second => build_array!(
                TimestampSecondBuilder::with_capacity(row_count),
                ValueData::TimestampSecondValue(v) => *v
            ),
            TimeUnit::Millisecond => build_array!(
                TimestampMillisecondBuilder::with_capacity(row_count),
                ValueData::TimestampMillisecondValue(v) => *v
            ),
            TimeUnit::Microsecond => build_array!(
                TimestampMicrosecondBuilder::with_capacity(row_count),
                ValueData::DatetimeValue(v) => *v,
                ValueData::TimestampMicrosecondValue(v) => *v
            ),
            TimeUnit::Nanosecond => build_array!(
                TimestampNanosecondBuilder::with_capacity(row_count),
                ValueData::TimestampNanosecondValue(v) => *v
            ),
        },
        ty => {
            return error::InvalidPromRemoteRequestSnafu {
                msg: format!(
                    "Unexpected column type {:?}, column name: {}",
                    ty, column_name
                ),
            }
            .fail();
        }
    };

    Ok(array)
}

#[cfg(test)]
mod tests {
    use api::v1::value::ValueData;
    use api::v1::{ColumnDataType, ColumnSchema, Row, Rows, SemanticType, Value};
    use arrow::array::{Array, Float64Array, StringArray, TimestampMillisecondArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};

    use super::{
        build_prom_create_table_schema_from_proto, identify_missing_columns_from_proto,
        rows_to_aligned_record_batch,
    };

    #[test]
    fn test_rows_to_aligned_record_batch_renames_and_reorders() {
        let rows = Rows {
            schema: vec![
                ColumnSchema {
                    column_name: "greptime_timestamp".to_string(),
                    datatype: ColumnDataType::TimestampMillisecond as i32,
                    semantic_type: SemanticType::Timestamp as i32,
                    ..Default::default()
                },
                ColumnSchema {
                    column_name: "host".to_string(),
                    datatype: ColumnDataType::String as i32,
                    semantic_type: SemanticType::Tag as i32,
                    ..Default::default()
                },
                ColumnSchema {
                    column_name: "greptime_value".to_string(),
                    datatype: ColumnDataType::Float64 as i32,
                    semantic_type: SemanticType::Field as i32,
                    ..Default::default()
                },
            ],
            rows: vec![
                Row {
                    values: vec![
                        Value {
                            value_data: Some(ValueData::TimestampMillisecondValue(1000)),
                        },
                        Value {
                            value_data: Some(ValueData::StringValue("h1".to_string())),
                        },
                        Value {
                            value_data: Some(ValueData::F64Value(42.0)),
                        },
                    ],
                },
                Row {
                    values: vec![
                        Value {
                            value_data: Some(ValueData::TimestampMillisecondValue(2000)),
                        },
                        Value {
                            value_data: Some(ValueData::StringValue("h2".to_string())),
                        },
                        Value {
                            value_data: Some(ValueData::F64Value(99.0)),
                        },
                    ],
                },
            ],
        };

        // Target schema has renamed columns and different ordering.
        let target = ArrowSchema::new(vec![
            Field::new(
                "my_ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("host", DataType::Utf8, true),
            Field::new("my_value", DataType::Float64, true),
        ]);

        let batch = rows_to_aligned_record_batch(&rows, &target).unwrap();
        assert_eq!(batch.schema().as_ref(), &target);
        assert_eq!(2, batch.num_rows());
        assert_eq!(3, batch.num_columns());

        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_eq!(ts.value(0), 1000);
        assert_eq!(ts.value(1), 2000);

        let hosts = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(hosts.value(0), "h1");
        assert_eq!(hosts.value(1), "h2");

        let values = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(values.value(0), 42.0);
        assert_eq!(values.value(1), 99.0);
    }

    #[test]
    fn test_rows_to_aligned_record_batch_fills_nulls() {
        let rows = Rows {
            schema: vec![
                ColumnSchema {
                    column_name: "greptime_timestamp".to_string(),
                    datatype: ColumnDataType::TimestampMillisecond as i32,
                    semantic_type: SemanticType::Timestamp as i32,
                    ..Default::default()
                },
                ColumnSchema {
                    column_name: "host".to_string(),
                    datatype: ColumnDataType::String as i32,
                    semantic_type: SemanticType::Tag as i32,
                    ..Default::default()
                },
                ColumnSchema {
                    column_name: "instance".to_string(),
                    datatype: ColumnDataType::String as i32,
                    semantic_type: SemanticType::Tag as i32,
                    ..Default::default()
                },
                ColumnSchema {
                    column_name: "greptime_value".to_string(),
                    datatype: ColumnDataType::Float64 as i32,
                    semantic_type: SemanticType::Field as i32,
                    ..Default::default()
                },
            ],
            rows: vec![Row {
                values: vec![
                    Value {
                        value_data: Some(ValueData::TimestampMillisecondValue(1000)),
                    },
                    Value {
                        value_data: Some(ValueData::StringValue("h1".to_string())),
                    },
                    Value {
                        value_data: Some(ValueData::StringValue("i1".to_string())),
                    },
                    Value {
                        value_data: Some(ValueData::F64Value(1.0)),
                    },
                ],
            }],
        };

        // Target schema has "host" but not "instance"; also has "region" which is missing from source.
        let target = ArrowSchema::new(vec![
            Field::new(
                "my_ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("host", DataType::Utf8, true),
            Field::new("region", DataType::Utf8, true),
            Field::new("my_value", DataType::Float64, true),
        ]);

        let batch = rows_to_aligned_record_batch(&rows, &target).unwrap();
        assert_eq!(batch.schema().as_ref(), &target);
        assert_eq!(1, batch.num_rows());
        assert_eq!(4, batch.num_columns());

        // "region" column should be null-filled.
        let region = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(region.is_null(0));
    }

    #[test]
    fn test_identify_missing_columns_from_proto() {
        let rows_schema = vec![
            ColumnSchema {
                column_name: "greptime_timestamp".to_string(),
                datatype: ColumnDataType::TimestampMillisecond as i32,
                semantic_type: SemanticType::Timestamp as i32,
                ..Default::default()
            },
            ColumnSchema {
                column_name: "host".to_string(),
                datatype: ColumnDataType::String as i32,
                semantic_type: SemanticType::Tag as i32,
                ..Default::default()
            },
            ColumnSchema {
                column_name: "instance".to_string(),
                datatype: ColumnDataType::String as i32,
                semantic_type: SemanticType::Tag as i32,
                ..Default::default()
            },
            ColumnSchema {
                column_name: "greptime_value".to_string(),
                datatype: ColumnDataType::Float64 as i32,
                semantic_type: SemanticType::Field as i32,
                ..Default::default()
            },
        ];

        let target = ArrowSchema::new(vec![
            Field::new(
                "my_ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("host", DataType::Utf8, true),
            Field::new("my_value", DataType::Float64, true),
        ]);

        let missing = identify_missing_columns_from_proto(&rows_schema, &target).unwrap();
        assert_eq!(missing, vec!["instance".to_string()]);
    }

    #[test]
    fn test_build_prom_create_table_schema_from_proto() {
        let rows_schema = vec![
            ColumnSchema {
                column_name: "greptime_timestamp".to_string(),
                datatype: ColumnDataType::TimestampMillisecond as i32,
                semantic_type: SemanticType::Timestamp as i32,
                ..Default::default()
            },
            ColumnSchema {
                column_name: "job".to_string(),
                datatype: ColumnDataType::String as i32,
                semantic_type: SemanticType::Tag as i32,
                ..Default::default()
            },
            ColumnSchema {
                column_name: "greptime_value".to_string(),
                datatype: ColumnDataType::Float64 as i32,
                semantic_type: SemanticType::Field as i32,
                ..Default::default()
            },
        ];

        let schema = build_prom_create_table_schema_from_proto(&rows_schema).unwrap();
        assert_eq!(3, schema.len());

        assert_eq!("greptime_timestamp", schema[0].column_name);
        assert_eq!(SemanticType::Timestamp as i32, schema[0].semantic_type);
        assert_eq!(
            ColumnDataType::TimestampMillisecond as i32,
            schema[0].datatype
        );

        assert_eq!("job", schema[1].column_name);
        assert_eq!(SemanticType::Tag as i32, schema[1].semantic_type);
        assert_eq!(ColumnDataType::String as i32, schema[1].datatype);

        assert_eq!("greptime_value", schema[2].column_name);
        assert_eq!(SemanticType::Field as i32, schema[2].semantic_type);
        assert_eq!(ColumnDataType::Float64 as i32, schema[2].datatype);
    }
}
