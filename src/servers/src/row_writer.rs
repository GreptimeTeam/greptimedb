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

use api::v1::column_data_type_extension::TypeExt;
use api::v1::value::ValueData;
use api::v1::{
    ColumnDataType, ColumnDataTypeExtension, ColumnSchema, JsonTypeExtension, Row,
    RowInsertRequest, RowInsertRequests, Rows, SemanticType, Value,
};
use common_grpc::precision::Precision;
use common_time::timestamp::TimeUnit;
use common_time::timestamp::TimeUnit::Nanosecond;
use common_time::Timestamp;
use snafu::{ensure, OptionExt, ResultExt};

use crate::error::{
    IncompatibleSchemaSnafu, Result, RowWriterSnafu, TimePrecisionSnafu, TimestampOverflowSnafu,
};

/// The intermediate data structure for building the write request.
/// It constructs the `schema` and `rows` as all input data row
/// parsing is completed.
pub struct TableData {
    schema: Vec<ColumnSchema>,
    rows: Vec<Row>,
    column_indexes: HashMap<String, usize>,
}

impl TableData {
    pub fn new(num_columns: usize, num_rows: usize) -> Self {
        Self {
            schema: Vec::with_capacity(num_columns),
            rows: Vec::with_capacity(num_rows),
            column_indexes: HashMap::with_capacity(num_columns),
        }
    }

    #[inline]
    pub fn num_columns(&self) -> usize {
        self.schema.len()
    }

    #[inline]
    pub fn num_rows(&self) -> usize {
        self.rows.len()
    }

    #[inline]
    pub fn alloc_one_row(&self) -> Vec<Value> {
        vec![Value { value_data: None }; self.num_columns()]
    }

    #[inline]
    pub fn add_row(&mut self, values: Vec<Value>) {
        self.rows.push(Row { values })
    }

    #[allow(dead_code)]
    pub fn columns(&self) -> &Vec<ColumnSchema> {
        &self.schema
    }

    pub fn into_schema_and_rows(self) -> (Vec<ColumnSchema>, Vec<Row>) {
        (self.schema, self.rows)
    }
}

pub struct MultiTableData {
    table_data_map: HashMap<String, TableData>,
}

impl Default for MultiTableData {
    fn default() -> Self {
        Self::new()
    }
}

impl MultiTableData {
    pub fn new() -> Self {
        Self {
            table_data_map: HashMap::new(),
        }
    }

    pub fn get_or_default_table_data(
        &mut self,
        table_name: impl ToString,
        num_columns: usize,
        num_rows: usize,
    ) -> &mut TableData {
        self.table_data_map
            .entry(table_name.to_string())
            .or_insert_with(|| TableData::new(num_columns, num_rows))
    }

    pub fn add_table_data(&mut self, table_name: impl ToString, table_data: TableData) {
        self.table_data_map
            .insert(table_name.to_string(), table_data);
    }

    #[allow(dead_code)]
    pub fn num_tables(&self) -> usize {
        self.table_data_map.len()
    }

    /// Returns the request and number of rows in it.
    pub fn into_row_insert_requests(self) -> (RowInsertRequests, usize) {
        let mut total_rows = 0;
        let inserts = self
            .table_data_map
            .into_iter()
            .map(|(table_name, table_data)| {
                total_rows += table_data.num_rows();
                let num_columns = table_data.num_columns();
                let (schema, mut rows) = table_data.into_schema_and_rows();
                for row in &mut rows {
                    if num_columns > row.values.len() {
                        row.values.resize(num_columns, Value { value_data: None });
                    }
                }

                RowInsertRequest {
                    table_name,
                    rows: Some(Rows { schema, rows }),
                }
            })
            .collect::<Vec<_>>();
        let row_insert_requests = RowInsertRequests { inserts };

        (row_insert_requests, total_rows)
    }
}

/// Write data as tags into the table data.
pub fn write_tags(
    table_data: &mut TableData,
    tags: impl Iterator<Item = (String, String)>,
    one_row: &mut Vec<Value>,
) -> Result<()> {
    let ktv_iter = tags.map(|(k, v)| (k, ColumnDataType::String, ValueData::StringValue(v)));
    write_by_semantic_type(table_data, SemanticType::Tag, ktv_iter, one_row)
}

/// Write data as fields into the table data.
pub fn write_fields(
    table_data: &mut TableData,
    fields: impl Iterator<Item = (String, ColumnDataType, ValueData)>,
    one_row: &mut Vec<Value>,
) -> Result<()> {
    write_by_semantic_type(table_data, SemanticType::Field, fields, one_row)
}

/// Write data as a tag into the table data.
pub fn write_tag(
    table_data: &mut TableData,
    name: impl ToString,
    value: impl ToString,
    one_row: &mut Vec<Value>,
) -> Result<()> {
    write_by_semantic_type(
        table_data,
        SemanticType::Tag,
        std::iter::once((
            name.to_string(),
            ColumnDataType::String,
            ValueData::StringValue(value.to_string()),
        )),
        one_row,
    )
}

/// Write float64 data as a field into the table data.
pub fn write_f64(
    table_data: &mut TableData,
    name: impl ToString,
    value: f64,
    one_row: &mut Vec<Value>,
) -> Result<()> {
    write_fields(
        table_data,
        std::iter::once((
            name.to_string(),
            ColumnDataType::Float64,
            ValueData::F64Value(value),
        )),
        one_row,
    )
}

fn build_json_column_schema(name: impl ToString) -> ColumnSchema {
    ColumnSchema {
        column_name: name.to_string(),
        datatype: ColumnDataType::Binary as i32,
        semantic_type: SemanticType::Field as i32,
        datatype_extension: Some(ColumnDataTypeExtension {
            type_ext: Some(TypeExt::JsonType(JsonTypeExtension::JsonBinary.into())),
        }),
        ..Default::default()
    }
}

pub fn write_json(
    table_data: &mut TableData,
    name: impl ToString,
    value: jsonb::Value,
    one_row: &mut Vec<Value>,
) -> Result<()> {
    write_by_schema(
        table_data,
        std::iter::once((
            build_json_column_schema(name),
            ValueData::BinaryValue(value.to_vec()),
        )),
        one_row,
    )
}

fn write_by_schema(
    table_data: &mut TableData,
    kv_iter: impl Iterator<Item = (ColumnSchema, ValueData)>,
    one_row: &mut Vec<Value>,
) -> Result<()> {
    let TableData {
        schema,
        column_indexes,
        ..
    } = table_data;

    for (column_schema, value) in kv_iter {
        let index = column_indexes.get(&column_schema.column_name);
        if let Some(index) = index {
            check_schema_number(
                column_schema.datatype,
                column_schema.semantic_type,
                &schema[*index],
            )?;
            one_row[*index].value_data = Some(value);
        } else {
            let index = schema.len();
            let key = column_schema.column_name.clone();
            schema.push(column_schema);
            column_indexes.insert(key, index);
            one_row.push(Value {
                value_data: Some(value),
            });
        }
    }

    Ok(())
}

fn write_by_semantic_type(
    table_data: &mut TableData,
    semantic_type: SemanticType,
    ktv_iter: impl Iterator<Item = (String, ColumnDataType, ValueData)>,
    one_row: &mut Vec<Value>,
) -> Result<()> {
    let TableData {
        schema,
        column_indexes,
        ..
    } = table_data;

    for (name, datatype, value) in ktv_iter {
        let index = column_indexes.get(&name);
        if let Some(index) = index {
            check_schema(datatype, semantic_type, &schema[*index])?;
            one_row[*index].value_data = Some(value);
        } else {
            let index = schema.len();
            schema.push(ColumnSchema {
                column_name: name.clone(),
                datatype: datatype as i32,
                semantic_type: semantic_type as i32,
                ..Default::default()
            });
            column_indexes.insert(name, index);
            one_row.push(Value {
                value_data: Some(value),
            });
        }
    }

    Ok(())
}

/// Write timestamp data as milliseconds into the table data.
pub fn write_ts_to_millis(
    table_data: &mut TableData,
    name: impl ToString,
    ts: Option<i64>,
    precision: Precision,
    one_row: &mut Vec<Value>,
) -> Result<()> {
    write_ts_to(
        table_data,
        name,
        ts,
        precision,
        TimestampType::Millis,
        one_row,
    )
}

/// Write timestamp data as nanoseconds into the table data.
pub fn write_ts_to_nanos(
    table_data: &mut TableData,
    name: impl ToString,
    ts: Option<i64>,
    precision: Precision,
    one_row: &mut Vec<Value>,
) -> Result<()> {
    write_ts_to(
        table_data,
        name,
        ts,
        precision,
        TimestampType::Nanos,
        one_row,
    )
}

enum TimestampType {
    Millis,
    Nanos,
}

fn write_ts_to(
    table_data: &mut TableData,
    name: impl ToString,
    ts: Option<i64>,
    precision: Precision,
    ts_type: TimestampType,
    one_row: &mut Vec<Value>,
) -> Result<()> {
    let TableData {
        schema,
        column_indexes,
        ..
    } = table_data;
    let name = name.to_string();

    let ts = match ts {
        Some(timestamp) => match ts_type {
            TimestampType::Millis => precision.to_millis(timestamp),
            TimestampType::Nanos => precision.to_nanos(timestamp),
        }
        .with_context(|| TimestampOverflowSnafu {
            error: format!(
                "timestamp {} overflow with precision {}",
                timestamp, precision
            ),
        })?,
        None => {
            let timestamp = Timestamp::current_time(Nanosecond);
            let unit: TimeUnit = precision.try_into().context(RowWriterSnafu)?;
            let timestamp = timestamp
                .convert_to(unit)
                .with_context(|| TimePrecisionSnafu {
                    name: precision.to_string(),
                })?
                .into();
            match ts_type {
                TimestampType::Millis => precision.to_millis(timestamp),
                TimestampType::Nanos => precision.to_nanos(timestamp),
            }
            .with_context(|| TimestampOverflowSnafu {
                error: format!(
                    "timestamp {} overflow with precision {}",
                    timestamp, precision
                ),
            })?
        }
    };

    let (datatype, ts) = match ts_type {
        TimestampType::Millis => (
            ColumnDataType::TimestampMillisecond,
            ValueData::TimestampMillisecondValue(ts),
        ),
        TimestampType::Nanos => (
            ColumnDataType::TimestampNanosecond,
            ValueData::TimestampNanosecondValue(ts),
        ),
    };

    let index = column_indexes.get(&name);
    if let Some(index) = index {
        check_schema(datatype, SemanticType::Timestamp, &schema[*index])?;
        one_row[*index].value_data = Some(ts);
    } else {
        let index = schema.len();
        schema.push(ColumnSchema {
            column_name: name.clone(),
            datatype: datatype as i32,
            semantic_type: SemanticType::Timestamp as i32,
            ..Default::default()
        });
        column_indexes.insert(name, index);
        one_row.push(ts.into())
    }

    Ok(())
}

fn check_schema(
    datatype: ColumnDataType,
    semantic_type: SemanticType,
    schema: &ColumnSchema,
) -> Result<()> {
    check_schema_number(datatype as i32, semantic_type as i32, schema)
}

fn check_schema_number(datatype: i32, semantic_type: i32, schema: &ColumnSchema) -> Result<()> {
    ensure!(
        schema.datatype == datatype,
        IncompatibleSchemaSnafu {
            column_name: &schema.column_name,
            datatype: "datatype",
            expected: schema.datatype,
            actual: datatype,
        }
    );

    ensure!(
        schema.semantic_type == semantic_type,
        IncompatibleSchemaSnafu {
            column_name: &schema.column_name,
            datatype: "semantic_type",
            expected: schema.semantic_type,
            actual: semantic_type,
        }
    );

    Ok(())
}
