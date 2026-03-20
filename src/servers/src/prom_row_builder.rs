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

use std::collections::HashSet;
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
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use arrow_schema::TimeUnit;
use common_query::prelude::{greptime_timestamp, greptime_value};
use datatypes::data_type::DataType;
use datatypes::prelude::ConcreteDataType;
use snafu::{OptionExt, ResultExt, ensure};

use crate::error;
use crate::error::{Error, Result};

/// Normalizes an incoming Prometheus record batch against an existing table schema.
///
/// This performs a single pass over source fields to:
/// - remap Prometheus special columns (`greptime_timestamp`, `greptime_value`) to the
///   target table's effective timestamp/field column names when they differ;
/// - collect columns that are absent in the target schema and must be added as tag columns.
///
/// Returns the normalized record batch plus the list of missing tag column names.
/// A missing column is accepted only when its source type is `Utf8`; otherwise it returns
/// an error because non-string missing columns cannot be safely treated as Prom tags.
pub(crate) fn accommodate_record_batch_for_target_schema(
    record_batch: RecordBatch,
    target_schema: &ArrowSchema,
) -> Result<(RecordBatch, Vec<String>)> {
    let (target_timestamp_col_name, target_field_col_name, target_tags) =
        unzip_logical_region_schema(target_schema)?;

    let incoming_schema = record_batch.schema();
    let mut missing_columns = Vec::new();
    let mut renamed_fields = Vec::with_capacity(incoming_schema.fields().len());
    let mut changed = false;

    for source_field in incoming_schema.fields() {
        match source_field.data_type() {
            ArrowDataType::Float64 => {
                if source_field.name() != target_field_col_name.as_str() {
                    // Field name mismatch
                    changed = true;
                    renamed_fields.push(Arc::new(Field::new(
                        target_field_col_name.clone(),
                        ArrowDataType::Float64,
                        false,
                    )));
                } else {
                    renamed_fields.push(source_field.clone());
                }
            }
            ArrowDataType::Timestamp(unit, _) => {
                ensure!(
                    unit == &TimeUnit::Millisecond,
                    error::InvalidPromRemoteRequestSnafu {
                        msg: format!(
                            "Unexpected remote write batch timestamp unit, expect milliseond, got: {}",
                            unit
                        )
                    }
                );
                if source_field.name() != &target_timestamp_col_name {
                    // Timestamp column name mismatch
                    changed = true;
                    renamed_fields.push(Arc::new(Field::new(
                        target_timestamp_col_name.clone(),
                        ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
                        false,
                    )));
                } else {
                    renamed_fields.push(source_field.clone())
                }
            }
            ArrowDataType::Utf8 => {
                ensure!(
                    source_field.data_type() == &ArrowDataType::Utf8,
                    error::InvalidPromRemoteRequestSnafu {
                        msg: format!(
                            "Failed to align record batch schema, missing column '{}' in target schema must be Utf8 but got {:?}",
                            source_field.name(),
                            source_field.data_type()
                        )
                    }
                );
                if !target_tags.contains(source_field.name()) {
                    missing_columns.push(source_field.name().clone());
                    changed = true;
                }
                renamed_fields.push(source_field.clone());
            }
            other => {
                return error::InvalidPromRemoteRequestSnafu {
                    msg: format!(
                        "Unexpected remote write batch field type {}, field name: {}",
                        other,
                        source_field.name()
                    ),
                }
                .fail();
            }
        }
    }

    if !changed {
        // No need to accommodate columns, simply return the original record batch.
        return Ok((record_batch, missing_columns));
    }

    let (_, columns, _) = record_batch.into_parts();
    let renamed_record_batch =
        RecordBatch::try_new(Arc::new(ArrowSchema::new(renamed_fields)), columns)
            .context(error::ArrowSnafu)?;
    Ok((renamed_record_batch, missing_columns))
}

/// Extract timestamp, field, and tag column names from a logical region schema.
fn unzip_logical_region_schema(
    target_schema: &ArrowSchema,
) -> Result<(String, String, HashSet<String>)> {
    let mut timestamp_column = None;
    let mut field_column = None;
    let mut tag_columns = HashSet::with_capacity(target_schema.fields.len() - 2);
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

/// Build a `Vec<ColumnSchema>` suitable for creating a new Prometheus logical table
/// from an incoming Arrow schema whose columns follow Prom conventions
/// (`greptime_timestamp`, `greptime_value`, plus Utf8 tags).
pub(crate) fn build_prom_create_table_schema(
    source_schema: &ArrowSchema,
) -> Result<Vec<ColumnSchema>> {
    source_schema
        .fields()
        .iter()
        .map(|field| {
            let semantic_type = if field.name() == greptime_timestamp() {
                SemanticType::Timestamp
            } else if field.name() == greptime_value() {
                SemanticType::Field
            } else {
                SemanticType::Tag
            };

            let concrete_type = ConcreteDataType::try_from(field.data_type())?;
            let (datatype, datatype_extension) = ColumnDataTypeWrapper::try_from(concrete_type)?
                .into_parts();

            if semantic_type == SemanticType::Tag && datatype != api::v1::ColumnDataType::String {
                return Err(Error::Internal {
                    err_msg: format!(
                        "Failed to build create table schema, tag column '{}' must be String but got {:?}",
                        field.name(), datatype
                    ),
                });
            }

            Ok(ColumnSchema {
                column_name: field.name().clone(),
                datatype: datatype as i32,
                semantic_type: semantic_type as i32,
                datatype_extension,
                options: None,
            })
        })
        .collect()
}

/// Reorder, cast, and fill missing columns so that `record_batch` conforms to
/// `target_schema`.  Columns present in the target but absent from the source
/// are filled with null arrays.
pub(crate) fn align_record_batch_to_schema(
    record_batch: RecordBatch,
    target_schema: &ArrowSchema,
) -> Result<RecordBatch> {
    let source_schema = record_batch.schema();
    if source_schema.as_ref() == target_schema {
        return Ok(record_batch);
    }

    for source_field in source_schema.fields() {
        ensure!(
            target_schema
                .column_with_name(source_field.name())
                .is_some(),
            error::UnexpectedResultSnafu {
                reason: format!(
                    "Failed to align record batch schema, column '{}' not found in target schema",
                    source_field.name()
                ),
            }
        );
    }

    let row_count = record_batch.num_rows();
    let mut columns = Vec::with_capacity(target_schema.fields().len());
    for target_field in target_schema.fields() {
        let column = if let Some((index, source_field)) =
            source_schema.column_with_name(target_field.name())
        {
            let source_column = record_batch.column(index).clone();
            if source_field.data_type() == target_field.data_type() {
                source_column
            } else {
                cast(source_column.as_ref(), target_field.data_type()).map_err(|err| {
                    Error::Internal {
                        err_msg: format!(
                            "Failed to cast column '{}' to target type {:?}: {}",
                            target_field.name(),
                            target_field.data_type(),
                            err
                        ),
                    }
                })?
            }
        } else {
            new_null_array(target_field.data_type(), row_count)
        };
        columns.push(column);
    }

    RecordBatch::try_new(Arc::new(target_schema.clone()), columns).map_err(|err| Error::Internal {
        err_msg: format!("Failed to build aligned record batch: {}", err),
    })
}

/// Convert a proto `Rows` message into an Arrow `RecordBatch`.
pub(crate) fn rows_to_record_batch(rows: &Rows) -> Result<RecordBatch> {
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

    let mut fields = Vec::with_capacity(column_count);
    let mut columns = Vec::with_capacity(column_count);

    for (idx, column_schema) in rows.schema.iter().enumerate() {
        let datatype_wrapper = ColumnDataTypeWrapper::try_new(
            column_schema.datatype,
            column_schema.datatype_extension.clone(),
        )?;
        let data_type = ConcreteDataType::from(datatype_wrapper);
        fields.push(Field::new(
            column_schema.column_name.clone(),
            data_type.as_arrow_type(),
            true,
        ));
        columns.push(build_arrow_array(
            rows,
            idx,
            &column_schema.column_name,
            data_type.as_arrow_type(),
            row_count,
        )?);
    }

    RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns).context(error::ArrowSnafu)
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
    use std::sync::Arc;

    use api::v1::value::ValueData;
    use api::v1::{ColumnDataType, ColumnSchema, Row, Rows, SemanticType, Value};
    use arrow::array::{
        Array, Float64Array, Int32Array, Int64Array, StringArray, TimestampMillisecondArray,
    };
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
    use arrow::record_batch::RecordBatch;

    use super::{
        accommodate_record_batch_for_target_schema, align_record_batch_to_schema,
        build_prom_create_table_schema, rows_to_record_batch,
    };

    #[test]
    fn test_rows_to_record_batch() {
        let rows = Rows {
            schema: vec![
                ColumnSchema {
                    column_name: "ts".to_string(),
                    datatype: ColumnDataType::TimestampMillisecond as i32,
                    semantic_type: SemanticType::Timestamp as i32,
                    ..Default::default()
                },
                ColumnSchema {
                    column_name: "value".to_string(),
                    datatype: ColumnDataType::Float64 as i32,
                    semantic_type: SemanticType::Field as i32,
                    ..Default::default()
                },
                ColumnSchema {
                    column_name: "host".to_string(),
                    datatype: ColumnDataType::String as i32,
                    semantic_type: SemanticType::Tag as i32,
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
                            value_data: Some(ValueData::F64Value(42.0)),
                        },
                        Value {
                            value_data: Some(ValueData::StringValue("h1".to_string())),
                        },
                    ],
                },
                Row {
                    values: vec![
                        Value {
                            value_data: Some(ValueData::TimestampMillisecondValue(2000)),
                        },
                        Value { value_data: None },
                        Value {
                            value_data: Some(ValueData::StringValue("h2".to_string())),
                        },
                    ],
                },
            ],
        };

        let rb = rows_to_record_batch(&rows).unwrap();
        assert_eq!(2, rb.num_rows());
        assert_eq!(3, rb.num_columns());
    }

    #[test]
    fn test_align_record_batch_to_schema_reorder_and_fill_missing() {
        let source_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("host", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]));
        let source = RecordBatch::try_new(
            source_schema,
            vec![
                Arc::new(StringArray::from(vec!["h1"])),
                Arc::new(Float64Array::from(vec![42.0])),
            ],
        )
        .unwrap();

        let target = ArrowSchema::new(vec![
            Field::new("ts", DataType::Int64, true),
            Field::new("host", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]);

        let aligned = align_record_batch_to_schema(source, &target).unwrap();
        assert_eq!(aligned.schema().as_ref(), &target);
        assert_eq!(1, aligned.num_rows());
        assert_eq!(3, aligned.num_columns());
        let ts = aligned
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(ts.is_null(0));
    }

    #[test]
    fn test_align_record_batch_to_schema_cast_column_type() {
        let source_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "value",
            DataType::Int32,
            true,
        )]));
        let source = RecordBatch::try_new(
            source_schema,
            vec![Arc::new(Int32Array::from(vec![Some(7), None]))],
        )
        .unwrap();

        let target = ArrowSchema::new(vec![Field::new("value", DataType::Int64, true)]);
        let aligned = align_record_batch_to_schema(source, &target).unwrap();
        let value = aligned
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(Some(7), value.iter().next().flatten());
        assert!(value.is_null(1));
    }

    #[test]
    fn test_prepare_record_batch_for_target_schema_collects_missing_tag_columns() {
        let source = ArrowSchema::new(vec![
            Field::new(
                "greptime_timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("host", DataType::Utf8, true),
            Field::new("instance", DataType::Utf8, true),
            Field::new("greptime_value", DataType::Float64, true),
        ]);
        let target = ArrowSchema::new(vec![
            Field::new(
                "my_ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("host", DataType::Utf8, true),
            Field::new("my_value", DataType::Float64, true),
        ]);

        let record_batch = RecordBatch::try_new(
            Arc::new(source),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![Some(1000)])),
                Arc::new(StringArray::from(vec!["h1"])),
                Arc::new(StringArray::from(vec!["i1"])),
                Arc::new(Float64Array::from(vec![1.0])),
            ],
        )
        .unwrap();

        let (_, missing) =
            accommodate_record_batch_for_target_schema(record_batch, &target).unwrap();
        assert_eq!(missing, vec!["instance".to_string()]);
    }

    #[test]
    fn test_prepare_record_batch_for_target_schema_reject_non_utf8_missing_column() {
        let source = ArrowSchema::new(vec![
            Field::new(
                "greptime_timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("code", DataType::Utf8, true),
            Field::new("greptime_value", DataType::Float64, true),
        ]);
        let target = ArrowSchema::new(vec![
            Field::new(
                "my_ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("host", DataType::Utf8, true),
            Field::new("my_value", DataType::Float64, true),
        ]);

        let record_batch = RecordBatch::try_new(
            Arc::new(source),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![Some(1000)])),
                Arc::new(StringArray::from(vec!["1"])),
                Arc::new(Float64Array::from(vec![1.0])),
            ],
        )
        .unwrap();
        let (rb, mut missing) =
            accommodate_record_batch_for_target_schema(record_batch, &target).unwrap();
        assert_eq!(missing.len(), 1);
        assert_eq!(missing.swap_remove(0).as_str(), "code");
        assert_eq!(
            rb.schema()
                .fields
                .iter()
                .find(|f| matches!(f.data_type(), DataType::Timestamp(_, _)))
                .unwrap()
                .name(),
            "my_ts"
        );
        assert_eq!(
            rb.schema()
                .fields
                .iter()
                .find(|f| matches!(f.data_type(), DataType::Float64))
                .unwrap()
                .name(),
            "my_value"
        );
    }

    #[test]
    fn test_build_prom_create_table_schema_from_request_schema() {
        let source = ArrowSchema::new(vec![
            Field::new(
                common_query::prelude::greptime_timestamp(),
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("job", DataType::Utf8, true),
            Field::new(
                common_query::prelude::greptime_value(),
                DataType::Float64,
                true,
            ),
        ]);

        let schema = build_prom_create_table_schema(&source).unwrap();
        assert_eq!(3, schema.len());

        assert_eq!(
            common_query::prelude::greptime_timestamp(),
            schema[0].column_name
        );
        assert_eq!(
            api::v1::SemanticType::Timestamp as i32,
            schema[0].semantic_type
        );
        assert_eq!(
            api::v1::ColumnDataType::TimestampMillisecond as i32,
            schema[0].datatype
        );

        assert_eq!("job", schema[1].column_name);
        assert_eq!(api::v1::SemanticType::Tag as i32, schema[1].semantic_type);
        assert_eq!(api::v1::ColumnDataType::String as i32, schema[1].datatype);

        assert_eq!(
            common_query::prelude::greptime_value(),
            schema[2].column_name
        );
        assert_eq!(api::v1::SemanticType::Field as i32, schema[2].semantic_type);
        assert_eq!(api::v1::ColumnDataType::Float64 as i32, schema[2].datatype);
    }

    #[test]
    fn test_prepare_record_batch_for_target_schema_renames_prom_special_columns() {
        let source_schema = Arc::new(ArrowSchema::new(vec![
            Field::new(
                "greptime_timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("host", DataType::Utf8, true),
            Field::new("greptime_value", DataType::Float64, true),
        ]));
        let source = RecordBatch::try_new(
            source_schema,
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![
                    Some(1000),
                    Some(2000),
                ])),
                Arc::new(StringArray::from(vec!["h1", "h2"])),
                Arc::new(Float64Array::from(vec![Some(1.0), Some(2.0)])),
            ],
        )
        .unwrap();

        let target = ArrowSchema::new(vec![
            Field::new(
                "my_ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("host", DataType::Utf8, true),
            Field::new("my_value", DataType::Float64, true),
        ]);

        let (prepared, missing) =
            accommodate_record_batch_for_target_schema(source, &target).unwrap();
        assert!(missing.is_empty());
        let aligned = align_record_batch_to_schema(prepared, &target).unwrap();

        assert_eq!(aligned.schema().as_ref(), &target);
        assert_eq!(2, aligned.num_rows());
        assert_eq!(3, aligned.num_columns());
    }

    #[test]
    fn test_prepare_record_batch_for_target_schema_requires_timestamp_column() {
        let source_schema = Arc::new(ArrowSchema::new(vec![
            Field::new(
                "greptime_timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("host", DataType::Utf8, true),
            Field::new("greptime_value", DataType::Float64, true),
        ]));
        let source = RecordBatch::try_new(
            source_schema,
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![Some(1000)])),
                Arc::new(StringArray::from(vec!["h1"])),
                Arc::new(Float64Array::from(vec![Some(1.0)])),
            ],
        )
        .unwrap();

        let target = ArrowSchema::new(vec![
            Field::new("host", DataType::Utf8, true),
            Field::new("my_value", DataType::Float64, true),
        ]);

        let err = accommodate_record_batch_for_target_schema(source, &target).unwrap_err();
        assert!(
            err.to_string()
                .contains("Failed to locate timestamp column in target schema")
        );
    }

    #[test]
    fn test_prepare_record_batch_for_target_schema_requires_field_column() {
        let source_schema = Arc::new(ArrowSchema::new(vec![
            Field::new(
                "greptime_timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("host", DataType::Utf8, true),
            Field::new("greptime_value", DataType::Float64, true),
        ]));
        let source = RecordBatch::try_new(
            source_schema,
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![Some(1000)])),
                Arc::new(StringArray::from(vec!["h1"])),
                Arc::new(Float64Array::from(vec![Some(1.0)])),
            ],
        )
        .unwrap();

        let target = ArrowSchema::new(vec![
            Field::new(
                "my_ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("host", DataType::Utf8, true),
        ]);

        let err = accommodate_record_batch_for_target_schema(source, &target).unwrap_err();
        assert!(
            err.to_string()
                .contains("Failed to locate field column in target schema")
        );
    }
}
