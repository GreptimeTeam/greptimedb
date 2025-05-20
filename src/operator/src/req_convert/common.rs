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

pub(crate) mod partitioner;

use std::collections::HashMap;

use api::helper::ColumnDataTypeWrapper;
use api::v1::column_data_type_extension::TypeExt;
use api::v1::column_def::options_from_column_schema;
use api::v1::value::ValueData;
use api::v1::{
    Column, ColumnDataType, ColumnDataTypeExtension, ColumnSchema, JsonTypeExtension, Row,
    RowDeleteRequest, RowInsertRequest, Rows, SemanticType, Value,
};
use common_base::BitVec;
use datatypes::prelude::ConcreteDataType;
use datatypes::vectors::VectorRef;
use snafu::prelude::*;
use snafu::ResultExt;
use table::metadata::TableInfo;

use crate::error::{
    ColumnDataTypeSnafu, ColumnNotFoundSnafu, InvalidInsertRequestSnafu, InvalidJsonFormatSnafu,
    MissingTimeIndexColumnSnafu, Result, UnexpectedSnafu,
};

/// Encodes a string value as JSONB binary data if the value is of `StringValue` type.
fn encode_string_to_jsonb_binary(value_data: ValueData) -> Result<ValueData> {
    if let ValueData::StringValue(json) = &value_data {
        let binary = jsonb::parse_value(json.as_bytes())
            .map_err(|_| InvalidJsonFormatSnafu { json }.build())
            .map(|jsonb| jsonb.to_vec())?;
        Ok(ValueData::BinaryValue(binary))
    } else {
        UnexpectedSnafu {
            violated: "Expected to value data to be a string.",
        }
        .fail()
    }
}

/// Prepares row insertion requests by converting any JSON values to binary JSONB format.
pub fn preprocess_row_insert_requests(requests: &mut Vec<RowInsertRequest>) -> Result<()> {
    for request in requests {
        validate_rows(&request.rows)?;
        prepare_rows(&mut request.rows)?;
    }

    Ok(())
}

/// Prepares row deletion requests by converting any JSON values to binary JSONB format.
pub fn preprocess_row_delete_requests(requests: &mut Vec<RowDeleteRequest>) -> Result<()> {
    for request in requests {
        validate_rows(&request.rows)?;
        prepare_rows(&mut request.rows)?;
    }

    Ok(())
}

fn prepare_rows(rows: &mut Option<Rows>) -> Result<()> {
    if let Some(rows) = rows {
        let indexes = rows
            .schema
            .iter()
            .enumerate()
            .filter_map(|(idx, schema)| {
                if schema.datatype() == ColumnDataType::Json {
                    Some(idx)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        for idx in &indexes {
            let column = &mut rows.schema[*idx];
            column.datatype_extension = Some(ColumnDataTypeExtension {
                type_ext: Some(TypeExt::JsonType(JsonTypeExtension::JsonBinary.into())),
            });
            column.datatype = ColumnDataType::Json.into();
        }

        for idx in &indexes {
            for row in &mut rows.rows {
                if let Some(value_data) = row.values[*idx].value_data.take() {
                    row.values[*idx].value_data = Some(encode_string_to_jsonb_binary(value_data)?);
                }
            }
        }
    }

    Ok(())
}

fn validate_rows(rows: &Option<Rows>) -> Result<()> {
    let Some(rows) = rows else {
        return Ok(());
    };

    for (col_idx, schema) in rows.schema.iter().enumerate() {
        let column_type =
            ColumnDataTypeWrapper::try_new(schema.datatype, schema.datatype_extension)
                .context(ColumnDataTypeSnafu)?
                .into();

        let ConcreteDataType::Vector(d) = column_type else {
            return Ok(());
        };

        for row in &rows.rows {
            let value = &row.values[col_idx].value_data;
            if let Some(data) = value {
                validate_vector_col(data, d.dim)?;
            }
        }
    }

    Ok(())
}

fn validate_vector_col(data: &ValueData, dim: u32) -> Result<()> {
    let data = match data {
        ValueData::BinaryValue(data) => data,
        _ => {
            return InvalidInsertRequestSnafu {
                reason: "Expecting binary data for vector column.".to_string(),
            }
            .fail();
        }
    };

    let expected_len = dim as usize * std::mem::size_of::<f32>();
    if data.len() != expected_len {
        return InvalidInsertRequestSnafu {
            reason: format!(
                "Expecting {} bytes of data for vector column, but got {}.",
                expected_len,
                data.len()
            ),
        }
        .fail();
    }

    Ok(())
}

pub fn columns_to_rows(columns: Vec<Column>, row_count: u32) -> Result<Rows> {
    let row_count = row_count as usize;
    let column_count = columns.len();
    let mut schema = Vec::with_capacity(column_count);
    let mut rows = vec![
        Row {
            values: Vec::with_capacity(column_count)
        };
        row_count
    ];
    for column in columns {
        let column_schema = ColumnSchema {
            column_name: column.column_name.clone(),
            datatype: column.datatype,
            semantic_type: column.semantic_type,
            datatype_extension: column.datatype_extension,
            options: column.options.clone(),
        };
        schema.push(column_schema);

        push_column_to_rows(column, &mut rows)?;
    }

    Ok(Rows { schema, rows })
}

fn push_column_to_rows(column: Column, rows: &mut [Row]) -> Result<()> {
    let null_mask = BitVec::from_vec(column.null_mask);
    let column_type = ColumnDataTypeWrapper::try_new(column.datatype, column.datatype_extension)
        .context(ColumnDataTypeSnafu)?
        .datatype();
    let column_values = column.values.unwrap_or_default();

    macro_rules! push_column_values_match_types {
        ($( ($arm:tt, $value_data_variant:tt, $field_name:tt), )*) => { match column_type { $(

        ColumnDataType::$arm => {
            let row_count = rows.len();
            let actual_row_count = null_mask.count_ones() + column_values.$field_name.len();
            ensure!(
                actual_row_count == row_count,
                InvalidInsertRequestSnafu {
                    reason: format!(
                        "Expecting {} rows of data for column '{}', but got {}.",
                        row_count, column.column_name, actual_row_count
                    ),
                }
            );

            let mut null_mask_iter = null_mask.into_iter();
            let mut values_iter = column_values.$field_name.into_iter();

            for row in rows {
                let value_is_null = null_mask_iter.next();
                if value_is_null == Some(true) {
                    row.values.push(Value { value_data: None });
                } else {
                    // previous check ensures that there is a value for each row
                    let value = values_iter.next().unwrap();
                    row.values.push(Value {
                        value_data: Some(ValueData::$value_data_variant(value)),
                    });
                }
            }
        }

        )* }}
    }

    push_column_values_match_types!(
        (Boolean, BoolValue, bool_values),
        (Int8, I8Value, i8_values),
        (Int16, I16Value, i16_values),
        (Int32, I32Value, i32_values),
        (Int64, I64Value, i64_values),
        (Uint8, U8Value, u8_values),
        (Uint16, U16Value, u16_values),
        (Uint32, U32Value, u32_values),
        (Uint64, U64Value, u64_values),
        (Float32, F32Value, f32_values),
        (Float64, F64Value, f64_values),
        (Binary, BinaryValue, binary_values),
        (String, StringValue, string_values),
        (Json, StringValue, string_values),
        (Date, DateValue, date_values),
        (Datetime, DatetimeValue, datetime_values),
        (
            TimestampSecond,
            TimestampSecondValue,
            timestamp_second_values
        ),
        (
            TimestampMillisecond,
            TimestampMillisecondValue,
            timestamp_millisecond_values
        ),
        (
            TimestampMicrosecond,
            TimestampMicrosecondValue,
            timestamp_microsecond_values
        ),
        (
            TimestampNanosecond,
            TimestampNanosecondValue,
            timestamp_nanosecond_values
        ),
        (TimeSecond, TimeSecondValue, time_second_values),
        (
            TimeMillisecond,
            TimeMillisecondValue,
            time_millisecond_values
        ),
        (
            TimeMicrosecond,
            TimeMicrosecondValue,
            time_microsecond_values
        ),
        (TimeNanosecond, TimeNanosecondValue, time_nanosecond_values),
        (
            IntervalYearMonth,
            IntervalYearMonthValue,
            interval_year_month_values
        ),
        (
            IntervalDayTime,
            IntervalDayTimeValue,
            interval_day_time_values
        ),
        (
            IntervalMonthDayNano,
            IntervalMonthDayNanoValue,
            interval_month_day_nano_values
        ),
        (Decimal128, Decimal128Value, decimal128_values),
        (Vector, BinaryValue, binary_values),
    );

    Ok(())
}

pub fn row_count(columns: &HashMap<String, VectorRef>) -> Result<usize> {
    let mut columns_iter = columns.values();

    let len = columns_iter
        .next()
        .map(|column| column.len())
        .unwrap_or_default();
    ensure!(
        columns_iter.all(|column| column.len() == len),
        InvalidInsertRequestSnafu {
            reason: "The row count of columns is not the same."
        }
    );

    Ok(len)
}

pub fn column_schema(
    table_info: &TableInfo,
    columns: &HashMap<String, VectorRef>,
) -> Result<Vec<ColumnSchema>> {
    columns
        .keys()
        .map(|column_name| {
            let column_schema = table_info
                .meta
                .schema
                .column_schema_by_name(column_name)
                .context(ColumnNotFoundSnafu {
                    msg: format!("unable to find column {column_name} in table schema"),
                })?;

            let (datatype, datatype_extension) =
                ColumnDataTypeWrapper::try_from(column_schema.data_type.clone())
                    .context(ColumnDataTypeSnafu)?
                    .to_parts();

            Ok(ColumnSchema {
                column_name: column_name.clone(),
                datatype: datatype as i32,
                semantic_type: semantic_type(table_info, column_name)?.into(),
                datatype_extension,
                options: options_from_column_schema(column_schema),
            })
        })
        .collect::<Result<Vec<_>>>()
}

fn semantic_type(table_info: &TableInfo, column: &str) -> Result<SemanticType> {
    let table_meta = &table_info.meta;
    let table_schema = &table_meta.schema;

    let time_index_column = &table_schema
        .timestamp_column()
        .with_context(|| table::error::MissingTimeIndexColumnSnafu {
            table_name: table_info.name.to_string(),
        })
        .context(MissingTimeIndexColumnSnafu)?
        .name;

    let semantic_type = if column == time_index_column {
        SemanticType::Timestamp
    } else {
        let column_index = table_schema.column_index_by_name(column);
        let column_index = column_index.context(ColumnNotFoundSnafu {
            msg: format!("unable to find column {column} in table schema"),
        })?;

        if table_meta.primary_key_indices.contains(&column_index) {
            SemanticType::Tag
        } else {
            SemanticType::Field
        }
    };

    Ok(semantic_type)
}

#[cfg(test)]
mod tests {
    use api::v1::column::Values;
    use api::v1::{SemanticType, VectorTypeExtension};
    use common_base::bit_vec::prelude::*;

    use super::*;

    #[test]
    fn test_request_column_to_row() {
        let columns = vec![
            Column {
                column_name: String::from("col1"),
                datatype: ColumnDataType::Int32.into(),
                semantic_type: SemanticType::Field.into(),
                null_mask: bitvec![u8, Lsb0; 1, 0, 1].into_vec(),
                values: Some(Values {
                    i32_values: vec![42],
                    ..Default::default()
                }),
                ..Default::default()
            },
            Column {
                column_name: String::from("col2"),
                datatype: ColumnDataType::String.into(),
                semantic_type: SemanticType::Tag.into(),
                null_mask: vec![],
                values: Some(Values {
                    string_values: vec![
                        String::from("value1"),
                        String::from("value2"),
                        String::from("value3"),
                    ],
                    ..Default::default()
                }),
                ..Default::default()
            },
            Column {
                column_name: String::from("col3"),
                datatype: ColumnDataType::Vector.into(),
                semantic_type: SemanticType::Field.into(),
                null_mask: vec![],
                values: Some(Values {
                    binary_values: vec![vec![0; 4], vec![1; 4], vec![2; 4]],
                    ..Default::default()
                }),
                datatype_extension: Some(ColumnDataTypeExtension {
                    type_ext: Some(TypeExt::VectorType(VectorTypeExtension { dim: 1 })),
                }),
                ..Default::default()
            },
        ];
        let row_count = 3;

        let result = columns_to_rows(columns, row_count);
        let rows = result.unwrap();

        assert_eq!(rows.schema.len(), 3);
        assert_eq!(rows.schema[0].column_name, "col1");
        assert_eq!(rows.schema[0].datatype, ColumnDataType::Int32 as i32);
        assert_eq!(rows.schema[0].semantic_type, SemanticType::Field as i32);
        assert_eq!(rows.schema[1].column_name, "col2");
        assert_eq!(rows.schema[1].datatype, ColumnDataType::String as i32);
        assert_eq!(rows.schema[1].semantic_type, SemanticType::Tag as i32);
        assert_eq!(rows.schema[2].column_name, "col3");
        assert_eq!(rows.schema[2].datatype, ColumnDataType::Vector as i32);
        assert_eq!(rows.schema[2].semantic_type, SemanticType::Field as i32);
        assert_eq!(
            rows.schema[2].datatype_extension,
            Some(ColumnDataTypeExtension {
                type_ext: Some(TypeExt::VectorType(VectorTypeExtension { dim: 1 }))
            })
        );

        assert_eq!(rows.rows.len(), 3);

        assert_eq!(rows.rows[0].values.len(), 3);
        assert_eq!(rows.rows[0].values[0].value_data, None);
        assert_eq!(
            rows.rows[0].values[1].value_data,
            Some(ValueData::StringValue(String::from("value1")))
        );
        assert_eq!(
            rows.rows[0].values[2].value_data,
            Some(ValueData::BinaryValue(vec![0; 4]))
        );

        assert_eq!(rows.rows[1].values.len(), 3);
        assert_eq!(
            rows.rows[1].values[0].value_data,
            Some(ValueData::I32Value(42))
        );
        assert_eq!(
            rows.rows[1].values[1].value_data,
            Some(ValueData::StringValue(String::from("value2")))
        );
        assert_eq!(
            rows.rows[1].values[2].value_data,
            Some(ValueData::BinaryValue(vec![1; 4]))
        );

        assert_eq!(rows.rows[2].values.len(), 3);
        assert_eq!(rows.rows[2].values[0].value_data, None);
        assert_eq!(
            rows.rows[2].values[1].value_data,
            Some(ValueData::StringValue(String::from("value3")))
        );
        assert_eq!(
            rows.rows[2].values[2].value_data,
            Some(ValueData::BinaryValue(vec![2; 4]))
        );

        // wrong type
        let columns = vec![Column {
            column_name: String::from("col1"),
            datatype: ColumnDataType::Int32.into(),
            semantic_type: SemanticType::Field.into(),
            null_mask: bitvec![u8, Lsb0; 1, 0, 1].into_vec(),
            values: Some(Values {
                i8_values: vec![42],
                ..Default::default()
            }),
            ..Default::default()
        }];
        let row_count = 3;
        assert!(columns_to_rows(columns, row_count).is_err());

        // wrong row count
        let columns = vec![Column {
            column_name: String::from("col1"),
            datatype: ColumnDataType::Int32.into(),
            semantic_type: SemanticType::Field.into(),
            null_mask: bitvec![u8, Lsb0; 0, 0, 1].into_vec(),
            values: Some(Values {
                i32_values: vec![42],
                ..Default::default()
            }),
            ..Default::default()
        }];
        let row_count = 3;
        assert!(columns_to_rows(columns, row_count).is_err());

        // wrong row count
        let columns = vec![Column {
            column_name: String::from("col1"),
            datatype: ColumnDataType::Int32.into(),
            semantic_type: SemanticType::Field.into(),
            null_mask: vec![],
            values: Some(Values {
                i32_values: vec![42],
                ..Default::default()
            }),
            ..Default::default()
        }];
        let row_count = 3;
        assert!(columns_to_rows(columns, row_count).is_err());
    }

    #[test]
    fn test_validate_vector_row_success() {
        let data = ValueData::BinaryValue(vec![0; 4]);
        let dim = 1;
        assert!(validate_vector_col(&data, dim).is_ok());

        let data = ValueData::BinaryValue(vec![0; 8]);
        let dim = 2;
        assert!(validate_vector_col(&data, dim).is_ok());

        let data = ValueData::BinaryValue(vec![0; 12]);
        let dim = 3;
        assert!(validate_vector_col(&data, dim).is_ok());
    }

    #[test]
    fn test_validate_vector_row_fail_wrong_type() {
        let data = ValueData::I32Value(42);
        let dim = 1;
        assert!(validate_vector_col(&data, dim).is_err());
    }

    #[test]
    fn test_validate_vector_row_fail_wrong_length() {
        let data = ValueData::BinaryValue(vec![0; 8]);
        let dim = 1;
        assert!(validate_vector_col(&data, dim).is_err());

        let data = ValueData::BinaryValue(vec![0; 4]);
        let dim = 2;
        assert!(validate_vector_col(&data, dim).is_err());
    }
}
