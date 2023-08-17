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

use api::v1::value::ValueData;
use api::v1::{
    ColumnDataType, ColumnSchema, InsertRequest as GrpcInsertRequest, InsertRequests, Row,
    RowInsertRequest, RowInsertRequests, Rows, SemanticType, Value,
};
use common_grpc::writer;
use common_grpc::writer::{LinesWriter, Precision};
use common_time::timestamp::TimeUnit;
use common_time::Timestamp;
use influxdb_line_protocol::{parse_lines, FieldSet, FieldValue, TagSet};
use snafu::{ensure, OptionExt, ResultExt};

use crate::error::{
    Error, IncompatibleSchemaSnafu, InfluxdbLineProtocolSnafu, InfluxdbLinesWriteSnafu,
    TimePrecisionSnafu,
};

pub const INFLUXDB_TIMESTAMP_COLUMN_NAME: &str = "ts";
pub const DEFAULT_TIME_PRECISION: Precision = Precision::Nanosecond;

#[derive(Debug)]
pub struct InfluxdbRequest {
    pub precision: Option<Precision>,
    pub lines: String,
}

type TableName = String;

impl TryFrom<InfluxdbRequest> for InsertRequests {
    type Error = Error;

    fn try_from(value: InfluxdbRequest) -> Result<Self, Self::Error> {
        let mut writers: HashMap<TableName, LinesWriter> = HashMap::new();
        let lines = parse_lines(&value.lines)
            .collect::<influxdb_line_protocol::Result<Vec<_>>>()
            .context(InfluxdbLineProtocolSnafu)?;
        let line_len = lines.len();

        for line in lines {
            let table_name = line.series.measurement;
            let writer = writers
                .entry(table_name.to_string())
                .or_insert_with(|| LinesWriter::with_lines(line_len));

            let tags = line.series.tag_set;
            if let Some(tags) = tags {
                for (k, v) in tags {
                    writer
                        .write_tag(k.as_str(), v.as_str())
                        .context(InfluxdbLinesWriteSnafu)?;
                }
            }

            let fields = line.field_set;
            for (k, v) in fields {
                let column_name = k.as_str();
                match v {
                    FieldValue::I64(value) => {
                        writer
                            .write_i64(column_name, value)
                            .context(InfluxdbLinesWriteSnafu)?;
                    }
                    FieldValue::U64(value) => {
                        writer
                            .write_u64(column_name, value)
                            .context(InfluxdbLinesWriteSnafu)?;
                    }
                    FieldValue::F64(value) => {
                        writer
                            .write_f64(column_name, value)
                            .context(InfluxdbLinesWriteSnafu)?;
                    }
                    FieldValue::String(value) => {
                        writer
                            .write_string(column_name, value.as_str())
                            .context(InfluxdbLinesWriteSnafu)?;
                    }
                    FieldValue::Boolean(value) => {
                        writer
                            .write_bool(column_name, value)
                            .context(InfluxdbLinesWriteSnafu)?;
                    }
                }
            }

            if let Some(timestamp) = line.timestamp {
                let precision = unwrap_or_default_precision(value.precision);
                writer
                    .write_ts(INFLUXDB_TIMESTAMP_COLUMN_NAME, (timestamp, precision))
                    .context(InfluxdbLinesWriteSnafu)?;
            } else {
                let precision = unwrap_or_default_precision(value.precision);
                let timestamp = Timestamp::current_millis();
                let unit = get_time_unit(precision)?;
                let timestamp = timestamp
                    .convert_to(unit)
                    .with_context(|| TimePrecisionSnafu {
                        name: precision.to_string(),
                    })?;
                writer
                    .write_ts(
                        INFLUXDB_TIMESTAMP_COLUMN_NAME,
                        (timestamp.into(), precision),
                    )
                    .context(InfluxdbLinesWriteSnafu)?;
            }
            writer.commit();
        }

        let inserts = writers
            .into_iter()
            .map(|(table_name, writer)| {
                let (columns, row_count) = writer.finish();
                GrpcInsertRequest {
                    table_name,
                    region_number: 0,
                    columns,
                    row_count,
                }
            })
            .collect();
        Ok(InsertRequests { inserts })
    }
}

impl TryFrom<InfluxdbRequest> for RowInsertRequests {
    type Error = Error;

    fn try_from(value: InfluxdbRequest) -> Result<Self, Self::Error> {
        let lines = parse_lines(&value.lines)
            .collect::<influxdb_line_protocol::Result<Vec<_>>>()
            .context(InfluxdbLineProtocolSnafu)?;

        struct TableData<'a> {
            schema: Vec<ColumnSchema>,
            rows: Vec<Row>,
            column_indexes: HashMap<&'a str, usize>,
        }

        let mut table_data_map = HashMap::new();

        for line in &lines {
            let table_name = line.series.measurement.as_str();
            let tags = &line.series.tag_set;
            let fields = &line.field_set;
            let ts = line.timestamp;
            let num_columns = tags.as_ref().map(|x| x.len()).unwrap_or(0) + fields.len() + 1;

            let TableData {
                schema,
                rows,
                column_indexes,
            } = table_data_map
                .entry(table_name)
                .or_insert_with(|| TableData {
                    schema: Vec::with_capacity(num_columns),
                    rows: Vec::new(),
                    column_indexes: HashMap::with_capacity(num_columns),
                });

            let mut one_row = vec![Value { value_data: None }; schema.len()];

            // tags
            parse_tags(tags, column_indexes, schema, &mut one_row)?;
            // fields
            parse_fields(fields, column_indexes, schema, &mut one_row)?;
            // timestamp
            parse_ts(ts, value.precision, column_indexes, schema, &mut one_row)?;

            rows.push(Row { values: one_row });
        }

        let inserts = table_data_map
            .into_iter()
            .map(
                |(
                    table_name,
                    TableData {
                        schema, mut rows, ..
                    },
                )| {
                    let num_columns = schema.len();
                    for row in rows.iter_mut() {
                        if num_columns > row.values.len() {
                            row.values.resize(num_columns, Value { value_data: None });
                        }
                    }

                    RowInsertRequest {
                        table_name: table_name.to_string(),
                        rows: Some(Rows { schema, rows }),
                        ..Default::default()
                    }
                },
            )
            .collect::<Vec<_>>();

        Ok(RowInsertRequests { inserts })
    }
}

fn parse_tags<'a>(
    tags: &'a Option<TagSet>,
    column_indexes: &mut HashMap<&'a str, usize>,
    schema: &mut Vec<ColumnSchema>,
    one_row: &mut Vec<Value>,
) -> Result<(), Error> {
    let Some(tags) = tags else {
        return Ok(());
    };

    for (k, v) in tags {
        let index = column_indexes.entry(k.as_str()).or_insert(schema.len());
        if *index == schema.len() {
            schema.push(ColumnSchema {
                column_name: k.to_string(),
                datatype: ColumnDataType::String as i32,
                semantic_type: SemanticType::Tag as i32,
            });
            one_row.push(to_value(ValueData::StringValue(v.to_string())));
        } else {
            check_schema(ColumnDataType::String, SemanticType::Tag, &schema[*index])?;
            // unwrap is safe here
            one_row.get_mut(*index).unwrap().value_data =
                Some(ValueData::StringValue(v.to_string()));
        }
    }

    Ok(())
}

fn parse_fields<'a>(
    fields: &'a FieldSet,
    column_indexes: &mut HashMap<&'a str, usize>,
    schema: &mut Vec<ColumnSchema>,
    one_row: &mut Vec<Value>,
) -> Result<(), Error> {
    for (k, v) in fields {
        let index = column_indexes.entry(k.as_str()).or_insert(schema.len());
        let (datatype, value) = match v {
            FieldValue::I64(v) => (ColumnDataType::Int64, ValueData::I64Value(*v)),
            FieldValue::U64(v) => (ColumnDataType::Uint64, ValueData::U64Value(*v)),
            FieldValue::F64(v) => (ColumnDataType::Float64, ValueData::F64Value(*v)),
            FieldValue::String(v) => (
                ColumnDataType::String,
                ValueData::StringValue(v.to_string()),
            ),
            FieldValue::Boolean(v) => (ColumnDataType::Boolean, ValueData::BoolValue(*v)),
        };

        if *index == schema.len() {
            schema.push(ColumnSchema {
                column_name: k.to_string(),
                datatype: datatype as i32,
                semantic_type: SemanticType::Field as i32,
            });
            one_row.push(to_value(value));
        } else {
            check_schema(datatype, SemanticType::Field, &schema[*index])?;
            // unwrap is safe here
            one_row.get_mut(*index).unwrap().value_data = Some(value);
        }
    }

    Ok(())
}

fn parse_ts(
    ts: Option<i64>,
    precision: Option<Precision>,
    column_indexes: &mut HashMap<&str, usize>,
    schema: &mut Vec<ColumnSchema>,
    one_row: &mut Vec<Value>,
) -> Result<(), Error> {
    let precision = unwrap_or_default_precision(precision);
    let ts = match ts {
        Some(timestamp) => writer::to_ms_ts(precision, timestamp),
        None => {
            let timestamp = Timestamp::current_millis();
            let unit = get_time_unit(precision)?;
            let timestamp = timestamp
                .convert_to(unit)
                .with_context(|| TimePrecisionSnafu {
                    name: precision.to_string(),
                })?;
            writer::to_ms_ts(precision, timestamp.into())
        }
    };

    let column_name = INFLUXDB_TIMESTAMP_COLUMN_NAME;
    let index = column_indexes.entry(column_name).or_insert(schema.len());
    if *index == schema.len() {
        schema.push(ColumnSchema {
            column_name: column_name.to_string(),
            datatype: ColumnDataType::TimestampMillisecond as i32,
            semantic_type: SemanticType::Timestamp as i32,
        });
        one_row.push(to_value(ValueData::TsMillisecondValue(ts)))
    } else {
        check_schema(
            ColumnDataType::TimestampMillisecond,
            SemanticType::Timestamp,
            &schema[*index],
        )?;
        // unwrap is safe here
        one_row.get_mut(*index).unwrap().value_data = Some(ValueData::TsMillisecondValue(ts));
    }

    Ok(())
}

#[inline]
fn check_schema(
    datatype: ColumnDataType,
    semantic_type: SemanticType,
    schema: &ColumnSchema,
) -> Result<(), Error> {
    ensure!(
        schema.datatype == datatype as i32,
        IncompatibleSchemaSnafu {
            err_msg: format!(
                "column {} datatype incompatible, expected {:?}, found {:?}",
                schema.column_name, schema.datatype, datatype
            )
        }
    );

    ensure!(
        schema.semantic_type == semantic_type as i32,
        IncompatibleSchemaSnafu {
            err_msg: format!(
                "column {} semantic type incompatible, expected {:?}, found {:?}",
                schema.column_name, schema.semantic_type, semantic_type
            )
        }
    );

    Ok(())
}

#[inline]
fn to_value(value: ValueData) -> Value {
    Value {
        value_data: Some(value),
    }
}

#[inline]
fn unwrap_or_default_precision(precision: Option<Precision>) -> Precision {
    if let Some(val) = precision {
        val
    } else {
        DEFAULT_TIME_PRECISION
    }
}

#[inline]
fn get_time_unit(precision: Precision) -> Result<TimeUnit, Error> {
    Ok(match precision {
        Precision::Second => TimeUnit::Second,
        Precision::Millisecond => TimeUnit::Millisecond,
        Precision::Microsecond => TimeUnit::Microsecond,
        Precision::Nanosecond => TimeUnit::Nanosecond,
        _ => {
            return Err(Error::NotSupported {
                feat: format!("convert {precision} into TimeUnit"),
            })
        }
    })
}

#[cfg(test)]
mod tests {
    use api::v1::column::Values;
    use api::v1::{Column, ColumnDataType, SemanticType};
    use common_base::BitVec;

    use super::*;
    use crate::influxdb::InfluxdbRequest;

    #[test]
    fn test_convert_influxdb_lines() {
        let lines = r"
monitor1,host=host1 cpu=66.6,memory=1024 1663840496100023100
monitor1,host=host2 memory=1027 1663840496400340001
monitor2,host=host3 cpu=66.5 1663840496100023102
monitor2,host=host4 cpu=66.3,memory=1029 1663840496400340003";

        let influxdb_req = InfluxdbRequest {
            precision: None,
            lines: lines.to_string(),
        };

        let requests: InsertRequests = influxdb_req.try_into().unwrap();
        assert_eq!(2, requests.inserts.len());

        for request in requests.inserts {
            match &request.table_name[..] {
                "monitor1" => assert_monitor_1(&request.columns),
                "monitor2" => assert_monitor_2(&request.columns),
                _ => panic!(),
            }
        }
    }

    fn assert_monitor_1(columns: &[Column]) {
        assert_eq!(4, columns.len());
        verify_column(
            &columns[0],
            "host",
            ColumnDataType::String,
            SemanticType::Tag,
            Vec::new(),
            Values {
                string_values: vec!["host1".to_string(), "host2".to_string()],
                ..Default::default()
            },
        );

        verify_column(
            &columns[1],
            "cpu",
            ColumnDataType::Float64,
            SemanticType::Field,
            vec![false, true],
            Values {
                f64_values: vec![66.6],
                ..Default::default()
            },
        );

        verify_column(
            &columns[2],
            "memory",
            ColumnDataType::Float64,
            SemanticType::Field,
            Vec::new(),
            Values {
                f64_values: vec![1024.0, 1027.0],
                ..Default::default()
            },
        );

        verify_column(
            &columns[3],
            "ts",
            ColumnDataType::TimestampMillisecond,
            SemanticType::Timestamp,
            Vec::new(),
            Values {
                ts_millisecond_values: vec![1663840496100, 1663840496400],
                ..Default::default()
            },
        );
    }

    fn assert_monitor_2(columns: &[Column]) {
        assert_eq!(4, columns.len());
        verify_column(
            &columns[0],
            "host",
            ColumnDataType::String,
            SemanticType::Tag,
            Vec::new(),
            Values {
                string_values: vec!["host3".to_string(), "host4".to_string()],
                ..Default::default()
            },
        );

        verify_column(
            &columns[1],
            "cpu",
            ColumnDataType::Float64,
            SemanticType::Field,
            Vec::new(),
            Values {
                f64_values: vec![66.5, 66.3],
                ..Default::default()
            },
        );

        verify_column(
            &columns[2],
            "ts",
            ColumnDataType::TimestampMillisecond,
            SemanticType::Timestamp,
            Vec::new(),
            Values {
                ts_millisecond_values: vec![1663840496100, 1663840496400],
                ..Default::default()
            },
        );

        verify_column(
            &columns[3],
            "memory",
            ColumnDataType::Float64,
            SemanticType::Field,
            vec![true, false],
            Values {
                f64_values: vec![1029.0],
                ..Default::default()
            },
        );
    }

    fn verify_column(
        column: &Column,
        name: &str,
        datatype: ColumnDataType,
        semantic_type: SemanticType,
        null_mask: Vec<bool>,
        vals: Values,
    ) {
        assert_eq!(name, column.column_name);
        assert_eq!(datatype as i32, column.datatype);
        assert_eq!(semantic_type as i32, column.semantic_type);
        verify_null_mask(&column.null_mask, null_mask);
        assert_eq!(Some(vals), column.values);
    }

    fn verify_null_mask(data: &[u8], expected: Vec<bool>) {
        let bitvec = BitVec::from_slice(data);
        for (idx, b) in expected.iter().enumerate() {
            assert_eq!(b, bitvec.get(idx).unwrap())
        }
    }
    #[test]
    fn test_convert_influxdb_lines_to_rows() {
        let lines = r"
monitor1,host=host1 cpu=66.6,memory=1024 1663840496100023100
monitor1,host=host2 memory=1027 1663840496400340001
monitor2,host=host3 cpu=66.5 1663840496100023102
monitor2,host=host4 cpu=66.3,memory=1029 1663840496400340003";

        let influxdb_req = InfluxdbRequest {
            precision: None,
            lines: lines.to_string(),
        };

        let requests: RowInsertRequests = influxdb_req.try_into().unwrap();
        assert_eq!(2, requests.inserts.len());

        for request in requests.inserts {
            match &request.table_name[..] {
                "monitor1" => assert_monitor1_rows(&request.rows),
                "monitor2" => assert_monitor2_rows(&request.rows),
                _ => panic!(),
            }
        }
    }

    fn assert_monitor1_rows(rows: &Option<Rows>) {
        let rows = rows.as_ref().unwrap();
        let schema = &rows.schema;
        let rows = &rows.rows;
        assert_eq!(4, schema.len());
        assert_eq!(2, rows.len());

        for (i, column_schema) in schema.iter().enumerate() {
            match &column_schema.column_name[..] {
                "host" => {
                    assert_eq!(ColumnDataType::String as i32, column_schema.datatype);
                    assert_eq!(SemanticType::Tag as i32, column_schema.semantic_type);

                    for (j, row) in rows.iter().enumerate() {
                        let v = row.values[i].value_data.as_ref().unwrap();
                        match j {
                            0 => assert_eq!("host1", extract_string_value(v)),
                            1 => assert_eq!("host2", extract_string_value(v)),
                            _ => panic!(),
                        }
                    }
                }
                "cpu" => {
                    assert_eq!(ColumnDataType::Float64 as i32, column_schema.datatype);
                    assert_eq!(SemanticType::Field as i32, column_schema.semantic_type);

                    for (j, row) in rows.iter().enumerate() {
                        let v = row.values[i].value_data.as_ref();
                        match j {
                            0 => assert_eq!(66.6f64, extract_f64_value(v.as_ref().unwrap())),
                            1 => assert_eq!(None, v),
                            _ => panic!(),
                        }
                    }
                }
                "memory" => {
                    assert_eq!(ColumnDataType::Float64 as i32, column_schema.datatype);
                    assert_eq!(SemanticType::Field as i32, column_schema.semantic_type);

                    for (j, row) in rows.iter().enumerate() {
                        let v = row.values[i].value_data.as_ref();
                        match j {
                            0 => assert_eq!(1024f64, extract_f64_value(v.as_ref().unwrap())),
                            1 => assert_eq!(1027f64, extract_f64_value(v.as_ref().unwrap())),
                            _ => panic!(),
                        }
                    }
                }
                "ts" => {
                    assert_eq!(
                        ColumnDataType::TimestampMillisecond as i32,
                        column_schema.datatype
                    );
                    assert_eq!(SemanticType::Timestamp as i32, column_schema.semantic_type);

                    for (j, row) in rows.iter().enumerate() {
                        let v = row.values[i].value_data.as_ref();
                        match j {
                            0 => assert_eq!(
                                1663840496100023100 / 1_000_000,
                                extract_ts_millis_value(v.as_ref().unwrap())
                            ),
                            1 => assert_eq!(
                                1663840496400340001 / 1_000_000,
                                extract_ts_millis_value(v.as_ref().unwrap())
                            ),
                            _ => panic!(),
                        }
                    }
                }
                _ => panic!(),
            }
        }
    }

    fn assert_monitor2_rows(rows: &Option<Rows>) {
        let rows = rows.as_ref().unwrap();
        let schema = &rows.schema;
        let rows = &rows.rows;
        assert_eq!(4, schema.len());
        assert_eq!(2, rows.len());

        for (i, column_schema) in schema.iter().enumerate() {
            match &column_schema.column_name[..] {
                "host" => {
                    assert_eq!(ColumnDataType::String as i32, column_schema.datatype);
                    assert_eq!(SemanticType::Tag as i32, column_schema.semantic_type);

                    for (j, row) in rows.iter().enumerate() {
                        let v = row.values[i].value_data.as_ref().unwrap();
                        match j {
                            0 => assert_eq!("host3", extract_string_value(v)),
                            1 => assert_eq!("host4", extract_string_value(v)),
                            _ => panic!(),
                        }
                    }
                }
                "cpu" => {
                    assert_eq!(ColumnDataType::Float64 as i32, column_schema.datatype);
                    assert_eq!(SemanticType::Field as i32, column_schema.semantic_type);

                    for (j, row) in rows.iter().enumerate() {
                        let v = row.values[i].value_data.as_ref();
                        match j {
                            0 => assert_eq!(66.5f64, extract_f64_value(v.as_ref().unwrap())),
                            1 => assert_eq!(66.3f64, extract_f64_value(v.as_ref().unwrap())),
                            _ => panic!(),
                        }
                    }
                }
                "memory" => {
                    assert_eq!(ColumnDataType::Float64 as i32, column_schema.datatype);
                    assert_eq!(SemanticType::Field as i32, column_schema.semantic_type);

                    for (j, row) in rows.iter().enumerate() {
                        let v = row.values[i].value_data.as_ref();
                        match j {
                            0 => assert_eq!(None, v),
                            1 => assert_eq!(1029f64, extract_f64_value(v.as_ref().unwrap())),
                            _ => panic!(),
                        }
                    }
                }
                "ts" => {
                    assert_eq!(
                        ColumnDataType::TimestampMillisecond as i32,
                        column_schema.datatype
                    );
                    assert_eq!(SemanticType::Timestamp as i32, column_schema.semantic_type);

                    for (j, row) in rows.iter().enumerate() {
                        let v = row.values[i].value_data.as_ref();
                        match j {
                            0 => assert_eq!(
                                1663840496100023102 / 1_000_000,
                                extract_ts_millis_value(v.as_ref().unwrap())
                            ),
                            1 => assert_eq!(
                                1663840496400340003 / 1_000_000,
                                extract_ts_millis_value(v.as_ref().unwrap())
                            ),
                            _ => panic!(),
                        }
                    }
                }
                _ => panic!(),
            }
        }
    }

    fn extract_string_value(value: &ValueData) -> &str {
        match value {
            ValueData::StringValue(v) => v,
            _ => panic!(),
        }
    }

    fn extract_f64_value(value: &ValueData) -> f64 {
        match value {
            ValueData::F64Value(v) => *v,
            _ => panic!(),
        }
    }

    fn extract_ts_millis_value(value: &ValueData) -> i64 {
        match value {
            ValueData::TsMillisecondValue(v) => *v,
            _ => panic!(),
        }
    }
}
