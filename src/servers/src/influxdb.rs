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
    ColumnDataType, InsertRequest as GrpcInsertRequest, InsertRequests, RowInsertRequests,
};
use common_grpc::writer::{LinesWriter, Precision};
use common_time::timestamp::TimeUnit;
use common_time::Timestamp;
use influxdb_line_protocol::{parse_lines, FieldValue};
use snafu::{OptionExt, ResultExt};

use crate::error::{Error, InfluxdbLineProtocolSnafu, InfluxdbLinesWriteSnafu, TimePrecisionSnafu};
use crate::row_writer::{self, MultiTableData};

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
                let unit: TimeUnit = precision.try_into().context(InfluxdbLinesWriteSnafu)?;
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
                let kvs = tags.iter().map(|(k, v)| (k.as_str(), v.as_str()));
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
                (k.as_str(), datatype, value)
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
    use api::v1::column::Values;
    use api::v1::value::ValueData;
    use api::v1::{Column, ColumnDataType, Rows, SemanticType};
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
                timestamp_millisecond_values: vec![1663840496100, 1663840496400],
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
                timestamp_millisecond_values: vec![1663840496100, 1663840496400],
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
            ValueData::TimestampMillisecondValue(v) => *v,
            _ => panic!(),
        }
    }
}
