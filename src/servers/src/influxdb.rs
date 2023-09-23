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

use api::v1::value::ValueData;
use api::v1::{ColumnDataType, RowInsertRequests};
use common_grpc::writer::Precision;
use influxdb_line_protocol::{parse_lines, FieldValue};
use snafu::ResultExt;

use crate::error::{Error, InfluxdbLineProtocolSnafu};
use crate::row_writer::{self, MultiTableData};

pub const INFLUXDB_TIMESTAMP_COLUMN_NAME: &str = "ts";
pub const DEFAULT_TIME_PRECISION: Precision = Precision::Nanosecond;

#[derive(Debug)]
pub struct InfluxdbRequest {
    pub precision: Option<Precision>,
    pub lines: String,
}

impl TryFrom<InfluxdbRequest> for RowInsertRequests {
    type Error = Error;

    fn try_from(value: InfluxdbRequest) -> Result<Self, Self::Error> {
        let lines = parse_lines(&value.lines)
            .collect::<influxdb_line_protocol::Result<Vec<_>>>()
            .context(InfluxdbLineProtocolSnafu)?;

        let mut multi_table_data = MultiTableData::new();

        for line in &lines {
            let table_name = line.series.measurement.as_str();
            let tags = &line.series.tag_set;
            let fields = &line.field_set;
            let ts = line.timestamp;
            // tags.len + fields.len + timestamp(+1)
            let num_columns = tags.as_ref().map(|x| x.len()).unwrap_or(0) + fields.len() + 1;

            let table_data = multi_table_data.get_or_default_table_data(table_name, num_columns, 0);
            let mut one_row = table_data.alloc_one_row();

            // tags
            if let Some(tags) = tags {
                let kvs = tags.iter().map(|(k, v)| (k.to_string(), v.as_str()));
                row_writer::write_tags(table_data, kvs, &mut one_row)?;
            }

            // fields
            let fields = fields.iter().map(|(k, v)| {
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
                (k.to_string(), datatype, value)
            });
            row_writer::write_fields(table_data, fields, &mut one_row)?;

            // timestamp
            let precision = unwrap_or_default_precision(value.precision);
            row_writer::write_ts_precision(
                table_data,
                INFLUXDB_TIMESTAMP_COLUMN_NAME,
                ts,
                precision,
                &mut one_row,
            )?;

            table_data.add_row(one_row);
        }

        Ok(multi_table_data.into_row_insert_requests().0)
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

#[cfg(test)]
mod tests {
    use api::v1::value::ValueData;
    use api::v1::{ColumnDataType, Rows, SemanticType};

    use super::*;
    use crate::influxdb::InfluxdbRequest;

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
            ValueData::TimestampMillisecondValue(v) => *v,
            _ => panic!(),
        }
    }
}
