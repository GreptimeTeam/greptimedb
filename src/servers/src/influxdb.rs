// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;

use api::v1::InsertRequest as GrpcInsertRequest;
use common_grpc::writer::{LinesWriter, Precision};
use influxdb_line_protocol::{parse_lines, FieldValue};
use snafu::ResultExt;
use table::requests::InsertRequest;

use crate::error::{Error, InfluxdbLineProtocolSnafu, InfluxdbLinesWriteSnafu};
use crate::line_writer::LineWriter;

pub const INFLUXDB_TIMESTAMP_COLUMN_NAME: &str = "ts";
pub const DEFAULT_TIME_PRECISION: Precision = Precision::Nanosecond;

pub struct InfluxdbRequest {
    pub precision: Option<Precision>,
    pub db: String,
    pub lines: String,
}

type TableName = String;

impl TryFrom<&InfluxdbRequest> for Vec<InsertRequest> {
    type Error = Error;

    fn try_from(value: &InfluxdbRequest) -> Result<Self, Self::Error> {
        let lines = parse_lines(&value.lines)
            .collect::<influxdb_line_protocol::Result<Vec<_>>>()
            .context(InfluxdbLineProtocolSnafu)?;
        let line_len = lines.len();
        let mut writers: HashMap<TableName, LineWriter> = HashMap::new();
        let db = &value.db;

        for line in lines {
            let table_name = line.series.measurement;
            let writer = writers
                .entry(table_name.to_string())
                .or_insert_with(|| LineWriter::with_lines(db, table_name, line_len));

            let tags = line.series.tag_set;
            if let Some(tags) = tags {
                for (k, v) in tags {
                    writer.write_tag(k.as_str(), v.as_str());
                }
            }

            let fields = line.field_set;
            for (k, v) in fields {
                let column_name = k.as_str();
                match v {
                    FieldValue::I64(value) => writer.write_i64(column_name, value),
                    FieldValue::U64(value) => writer.write_u64(column_name, value),
                    FieldValue::F64(value) => writer.write_f64(column_name, value),
                    FieldValue::String(value) => writer.write_string(column_name, value.as_str()),
                    FieldValue::Boolean(value) => writer.write_bool(column_name, value),
                }
            }

            if let Some(timestamp) = line.timestamp {
                let precision = if let Some(val) = &value.precision {
                    *val
                } else {
                    DEFAULT_TIME_PRECISION
                };
                writer.write_ts(INFLUXDB_TIMESTAMP_COLUMN_NAME, (timestamp, precision));
            }

            writer.commit();
        }
        Ok(writers.into_values().map(|x| x.finish()).collect())
    }
}

// TODO(fys): will remove in the future.
impl TryFrom<&InfluxdbRequest> for Vec<GrpcInsertRequest> {
    type Error = Error;

    fn try_from(value: &InfluxdbRequest) -> Result<Self, Self::Error> {
        let schema_name = value.db.to_string();

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
                let precision = if let Some(val) = &value.precision {
                    *val
                } else {
                    DEFAULT_TIME_PRECISION
                };
                writer
                    .write_ts(INFLUXDB_TIMESTAMP_COLUMN_NAME, (timestamp, precision))
                    .context(InfluxdbLinesWriteSnafu)?;
            }

            writer.commit();
        }

        Ok(writers
            .into_iter()
            .map(|(table_name, writer)| {
                let (columns, row_count) = writer.finish();
                GrpcInsertRequest {
                    schema_name: schema_name.clone(),
                    table_name,
                    region_number: 0,
                    columns,
                    row_count,
                }
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::column::{SemanticType, Values};
    use api::v1::{Column, ColumnDataType};
    use common_base::BitVec;
    use common_time::timestamp::TimeUnit;
    use common_time::Timestamp;
    use datatypes::value::Value;
    use datatypes::vectors::Vector;
    use table::requests::InsertRequest;

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
            db: "influxdb".to_string(),
            precision: None,
            lines: lines.to_string(),
        };
        let insert_reqs: Vec<InsertRequest> = influxdb_req.try_into().unwrap();

        for insert_req in insert_reqs {
            assert_eq!("influxdb", insert_req.schema_name);
            match &insert_req.table_name[..] {
                "monitor1" => assert_table_1(&insert_req),
                "monitor2" => assert_table_2(&insert_req),
                _ => panic!(),
            }
        }
    }

    // TODO(fys): will remove in the future.
    #[test]
    fn test_convert_influxdb_lines_1() {
        let lines = r"
monitor1,host=host1 cpu=66.6,memory=1024 1663840496100023100
monitor1,host=host2 memory=1027 1663840496400340001
monitor2,host=host3 cpu=66.5 1663840496100023102
monitor2,host=host4 cpu=66.3,memory=1029 1663840496400340003";

        let influxdb_req = &InfluxdbRequest {
            db: "public".to_string(),
            precision: None,
            lines: lines.to_string(),
        };

        let requests: Vec<GrpcInsertRequest> = influxdb_req.try_into().unwrap();
        assert_eq!(2, requests.len());

        for request in requests {
            assert_eq!("public", request.schema_name);
            match &request.table_name[..] {
                "monitor1" => assert_monitor_1(&request.columns),
                "monitor2" => assert_monitor_2(&request.columns),
                _ => panic!(),
            }
        }
    }

    fn assert_table_1(insert_req: &InsertRequest) {
        let table_name = &insert_req.table_name;
        assert_eq!("monitor1", table_name);

        let columns = &insert_req.columns_values;

        let host = columns.get("host").unwrap();
        let expected: Vec<Value> = vec!["host1".into(), "host2".into()];
        assert_vector(&expected, host);

        let cpu = columns.get("cpu").unwrap();
        let expected: Vec<Value> = vec![66.6.into(), Value::Null];
        assert_vector(&expected, cpu);

        let memory = columns.get("memory").unwrap();
        let expected: Vec<Value> = vec![1024.0.into(), 1027.0.into()];
        assert_vector(&expected, memory);

        let ts = columns.get("ts").unwrap();
        let expected: Vec<Value> = vec![
            datatypes::prelude::Value::Timestamp(Timestamp::new(
                1663840496100,
                TimeUnit::Millisecond,
            )),
            datatypes::prelude::Value::Timestamp(Timestamp::new(
                1663840496400,
                TimeUnit::Millisecond,
            )),
        ];
        assert_vector(&expected, ts);
    }

    fn assert_table_2(insert_req: &InsertRequest) {
        let table_name = &insert_req.table_name;
        assert_eq!("monitor2", table_name);

        let columns = &insert_req.columns_values;

        let host = columns.get("host").unwrap();
        let expected: Vec<Value> = vec!["host3".into(), "host4".into()];
        assert_vector(&expected, host);

        let cpu = columns.get("cpu").unwrap();
        let expected: Vec<Value> = vec![66.5.into(), 66.3.into()];
        assert_vector(&expected, cpu);

        let memory = columns.get("memory").unwrap();
        let expected: Vec<Value> = vec![Value::Null, 1029.0.into()];
        assert_vector(&expected, memory);

        let ts = columns.get("ts").unwrap();
        let expected: Vec<Value> = vec![
            datatypes::prelude::Value::Timestamp(Timestamp::new(
                1663840496100,
                TimeUnit::Millisecond,
            )),
            datatypes::prelude::Value::Timestamp(Timestamp::new(
                1663840496400,
                TimeUnit::Millisecond,
            )),
        ];
        assert_vector(&expected, ts);
    }

    fn assert_vector(expected: &[Value], vector: &Arc<dyn Vector>) {
        for (idx, expected) in expected.iter().enumerate() {
            let val = vector.get(idx);
            assert_eq!(*expected, val);
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
