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

use api::helper::ColumnDataTypeWrapper;
use api::v1::value::ValueData;
use api::v1::{
    Column, ColumnDataType, ColumnSchema, InsertRequest, InsertRequests, Row, RowInsertRequest,
    RowInsertRequests, Rows, Value,
};
use common_base::BitVec;
use snafu::prelude::*;
use snafu::ResultExt;

use crate::error::{ColumnDataTypeSnafu, InvalidInsertRequestSnafu, Result};

pub struct ColumnToRow;

impl ColumnToRow {
    pub fn convert(requests: InsertRequests) -> Result<RowInsertRequests> {
        requests
            .inserts
            .into_iter()
            .map(request_column_to_row)
            .collect::<Result<Vec<_>>>()
            .map(|inserts| RowInsertRequests { inserts })
    }
}

fn request_column_to_row(request: InsertRequest) -> Result<RowInsertRequest> {
    let row_count = request.row_count as usize;
    let column_count = request.columns.len();
    let mut schema = Vec::with_capacity(column_count);
    let mut rows = vec![
        Row {
            values: Vec::with_capacity(column_count)
        };
        row_count
    ];
    for column in request.columns {
        let column_schema = ColumnSchema {
            column_name: column.column_name.clone(),
            datatype: column.datatype,
            semantic_type: column.semantic_type,
        };
        schema.push(column_schema);

        push_column_to_rows(column, &mut rows)?;
    }

    Ok(RowInsertRequest {
        table_name: request.table_name,
        rows: Some(Rows { schema, rows }),
        region_number: request.region_number,
    })
}

fn push_column_to_rows(column: Column, rows: &mut [Row]) -> Result<()> {
    let null_mask = BitVec::from_vec(column.null_mask);
    let column_type = ColumnDataTypeWrapper::try_new(column.datatype)
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
        (Date, DateValue, date_values),
        (Datetime, DatetimeValue, datetime_values),
        (TimestampSecond, TsSecondValue, ts_second_values),
        (
            TimestampMillisecond,
            TsMillisecondValue,
            ts_millisecond_values
        ),
        (
            TimestampMicrosecond,
            TsMicrosecondValue,
            ts_microsecond_values
        ),
        (TimestampNanosecond, TsNanosecondValue, ts_nanosecond_values),
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
            IntervalYearMonthValues,
            interval_year_month_values
        ),
        (
            IntervalDayTime,
            IntervalDayTimeValues,
            interval_day_time_values
        ),
        (
            IntervalMonthDayNano,
            IntervalMonthDayNanoValues,
            interval_month_day_nano_values
        ),
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use api::v1::column::Values;
    use api::v1::SemanticType;
    use common_base::bit_vec::prelude::*;

    use super::*;

    #[test]
    fn test_request_column_to_row() {
        let insert_request = InsertRequest {
            table_name: String::from("test_table"),
            row_count: 3,
            region_number: 1,
            columns: vec![
                Column {
                    column_name: String::from("col1"),
                    datatype: ColumnDataType::Int32.into(),
                    semantic_type: SemanticType::Field.into(),
                    null_mask: bitvec![u8, Lsb0; 1, 0, 1].into_vec(),
                    values: Some(Values {
                        i32_values: vec![42],
                        ..Default::default()
                    }),
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
                },
            ],
        };

        let result = request_column_to_row(insert_request);
        let row_insert_request = result.unwrap();
        assert_eq!(row_insert_request.table_name, "test_table");
        assert_eq!(row_insert_request.region_number, 1);
        let rows = row_insert_request.rows.unwrap();

        assert_eq!(rows.schema.len(), 2);
        assert_eq!(rows.schema[0].column_name, "col1");
        assert_eq!(rows.schema[0].datatype, ColumnDataType::Int32 as i32);
        assert_eq!(rows.schema[0].semantic_type, SemanticType::Field as i32);
        assert_eq!(rows.schema[1].column_name, "col2");
        assert_eq!(rows.schema[1].datatype, ColumnDataType::String as i32);
        assert_eq!(rows.schema[1].semantic_type, SemanticType::Tag as i32);

        assert_eq!(rows.rows.len(), 3);

        assert_eq!(rows.rows[0].values.len(), 2);
        assert_eq!(rows.rows[0].values[0].value_data, None);
        assert_eq!(
            rows.rows[0].values[1].value_data,
            Some(ValueData::StringValue(String::from("value1")))
        );

        assert_eq!(rows.rows[1].values.len(), 2);
        assert_eq!(
            rows.rows[1].values[0].value_data,
            Some(ValueData::I32Value(42))
        );
        assert_eq!(
            rows.rows[1].values[1].value_data,
            Some(ValueData::StringValue(String::from("value2")))
        );

        assert_eq!(rows.rows[2].values.len(), 2);
        assert_eq!(rows.rows[2].values[0].value_data, None);
        assert_eq!(
            rows.rows[2].values[1].value_data,
            Some(ValueData::StringValue(String::from("value3")))
        );

        let invalid_request_with_wrong_type = InsertRequest {
            table_name: String::from("test_table"),
            row_count: 3,
            region_number: 1,
            columns: vec![Column {
                column_name: String::from("col1"),
                datatype: ColumnDataType::Int32.into(),
                semantic_type: SemanticType::Field.into(),
                null_mask: bitvec![u8, Lsb0; 1, 0, 1].into_vec(),
                values: Some(Values {
                    i8_values: vec![42],
                    ..Default::default()
                }),
            }],
        };
        assert!(request_column_to_row(invalid_request_with_wrong_type).is_err());

        let invalid_request_with_wrong_row_count = InsertRequest {
            table_name: String::from("test_table"),
            row_count: 3,
            region_number: 1,
            columns: vec![Column {
                column_name: String::from("col1"),
                datatype: ColumnDataType::Int32.into(),
                semantic_type: SemanticType::Field.into(),
                null_mask: bitvec![u8, Lsb0; 0, 0, 1].into_vec(),
                values: Some(Values {
                    i32_values: vec![42],
                    ..Default::default()
                }),
            }],
        };
        assert!(request_column_to_row(invalid_request_with_wrong_row_count).is_err());

        let invalid_request_with_wrong_row_count = InsertRequest {
            table_name: String::from("test_table"),
            row_count: 3,
            region_number: 1,
            columns: vec![Column {
                column_name: String::from("col1"),
                datatype: ColumnDataType::Int32.into(),
                semantic_type: SemanticType::Field.into(),
                null_mask: vec![],
                values: Some(Values {
                    i32_values: vec![42],
                    ..Default::default()
                }),
            }],
        };
        assert!(request_column_to_row(invalid_request_with_wrong_row_count).is_err());
    }
}
