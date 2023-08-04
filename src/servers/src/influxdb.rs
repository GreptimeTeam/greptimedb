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

use api::v1::value::Value;
use api::v1::{
    ColumnDataType, ColumnSchema, InsertRequest as GrpcInsertRequest, InsertRequests, Row,
    RowInsertRequest, RowInsertRequests, Rows, SemanticType,
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

impl TryFrom<&InfluxdbRequest> for InsertRequests {
    type Error = Error;

    fn try_from(value: &InfluxdbRequest) -> Result<Self, Self::Error> {
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

impl TryFrom<&InfluxdbRequest> for RowInsertRequests {
    type Error = Error;

    fn try_from(value: &InfluxdbRequest) -> Result<Self, Self::Error> {
        let lines = parse_lines(&value.lines)
            .collect::<influxdb_line_protocol::Result<Vec<_>>>()
            .context(InfluxdbLineProtocolSnafu)?;

        let mut table_schemas = HashMap::with_capacity(lines.len());
        let mut table_rows = HashMap::with_capacity(lines.len());

        for line in lines {
            let table_name = line.series.measurement.as_str();
            let tags = line.series.tag_set;
            let fields = line.field_set;
            let timestamp = line.timestamp;
            let len = tags.as_ref().map(|x| x.len()).unwrap_or(0) + fields.len();

            let schema = table_schemas
                .entry(table_name.to_string())
                .or_insert_with(|| HashMap::with_capacity(len));
            let values_list = table_rows
                .entry(table_name.to_string())
                .or_insert_with(Vec::new);

            // tags
            parse_tags(tags, schema, values_list)?;
            // fields
            parse_fields(fields, schema, values_list)?;
            // timestamp
            parse_ts(timestamp, value.precision, schema, values_list)?;
        }

        let mut inserts = Vec::with_capacity(table_schemas.len());
        for (table_name, schema) in table_schemas {
            let Some(table_data) = table_rows
                .remove(&table_name) else {
                continue
            };
            let schema = schema.into_values().collect::<Vec<_>>();
            let rows = table_data
                .into_iter()
                .map(|mut r| Row {
                    values: schema
                        .iter()
                        .map(|col| {
                            r.remove(&col.column_name)
                                .unwrap_or(api::v1::Value { value: None })
                        })
                        .collect::<Vec<_>>(),
                })
                .collect::<Vec<_>>();

            inserts.push(RowInsertRequest {
                table_name,
                rows: Some(Rows { schema, rows }),
                ..Default::default()
            });
        }

        Ok(RowInsertRequests { inserts })
    }
}

fn parse_tags(
    tags: Option<TagSet>,
    schema: &mut HashMap<String, ColumnSchema>,
    values_list: &mut Vec<HashMap<String, api::v1::Value>>,
) -> Result<(), Error> {
    let Some(tags) = tags else { return Ok(()); };
    let mut values = HashMap::with_capacity(tags.len());
    for (k, v) in tags {
        let current = schema.entry(k.to_string()).or_insert_with(|| ColumnSchema {
            column_name: k.to_string(),
            datatype: ColumnDataType::String as i32,
            semantic_type: SemanticType::Tag as i32,
        });

        check_schema(ColumnDataType::String, SemanticType::Tag, current)?;

        let v = wrap_value(Value::StringValue(v.to_string()));
        values.insert(k.to_string(), v);
    }

    values_list.push(values);

    Ok(())
}

fn parse_fields(
    fields: FieldSet,
    schema: &mut HashMap<String, ColumnSchema>,
    values_list: &mut Vec<HashMap<String, api::v1::Value>>,
) -> Result<(), Error> {
    let mut values = HashMap::with_capacity(fields.len());
    for (k, v) in fields {
        let (datatype, value) = match v {
            FieldValue::I64(v) => (ColumnDataType::Int64, wrap_value(Value::I64Value(v))),
            FieldValue::U64(v) => (ColumnDataType::Uint64, wrap_value(Value::U64Value(v))),
            FieldValue::F64(v) => (ColumnDataType::Float64, wrap_value(Value::F64Value(v))),
            FieldValue::String(v) => (
                ColumnDataType::String,
                wrap_value(Value::StringValue(v.to_string())),
            ),
            FieldValue::Boolean(v) => (ColumnDataType::Boolean, wrap_value(Value::BoolValue(v))),
        };

        let current = schema.entry(k.to_string()).or_insert_with(|| ColumnSchema {
            column_name: k.to_string(),
            datatype: datatype as i32,
            semantic_type: SemanticType::Field as i32,
        });

        check_schema(datatype, SemanticType::Field, current)?;

        values.insert(k.to_string(), value);
    }

    values_list.push(values);

    Ok(())
}

fn parse_ts(
    ts: Option<i64>,
    precision: Option<Precision>,
    schema: &mut HashMap<String, ColumnSchema>,
    values_list: &mut Vec<HashMap<String, api::v1::Value>>,
) -> Result<(), Error> {
    let mut values = HashMap::with_capacity(1);
    let column_name = INFLUXDB_TIMESTAMP_COLUMN_NAME;
    let current = schema
        .entry(column_name.to_string())
        .or_insert_with(|| ColumnSchema {
            column_name: column_name.to_string(),
            datatype: ColumnDataType::TimestampMillisecond as i32,
            semantic_type: SemanticType::Timestamp as i32,
        });

    check_schema(
        ColumnDataType::TimestampMillisecond,
        SemanticType::Timestamp,
        current,
    )?;

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

    values.insert(
        column_name.to_string(),
        wrap_value(Value::TsMillisecondValue(ts)),
    );

    values_list.push(values);

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

fn wrap_value(value: Value) -> api::v1::Value {
    api::v1::Value { value: Some(value) }
}

fn unwrap_or_default_precision(precision: Option<Precision>) -> Precision {
    if let Some(val) = precision {
        val
    } else {
        DEFAULT_TIME_PRECISION
    }
}

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

        let influxdb_req = &InfluxdbRequest {
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
}
