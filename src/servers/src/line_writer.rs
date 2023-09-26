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

use common_catalog::consts::DEFAULT_CATALOG_NAME;
use common_grpc::writer::{to_ms_ts, Precision};
use common_time::timestamp::TimeUnit::Millisecond;
use common_time::Timestamp;
use datatypes::data_type::DataType;
use datatypes::prelude::ConcreteDataType;
use datatypes::types::{TimestampMillisecondType, TimestampType};
use datatypes::value::Value;
use datatypes::vectors::{MutableVector, VectorRef};
use table::requests::InsertRequest;

type ColumnLen = usize;
type ColumnName = String;

pub struct LineWriter {
    db: String,
    table_name: String,
    expected_rows: usize,
    current_rows: usize,
    columns_builders: HashMap<ColumnName, (Box<dyn MutableVector>, ColumnLen)>,
}

impl LineWriter {
    pub fn with_lines(db: impl Into<String>, table_name: impl Into<String>, lines: usize) -> Self {
        Self {
            db: db.into(),
            table_name: table_name.into(),
            expected_rows: lines,
            current_rows: 0,
            columns_builders: Default::default(),
        }
    }

    pub fn write_ts(&mut self, column_name: &str, value: (i64, Precision)) {
        let (val, precision) = value;
        let datatype =
            ConcreteDataType::Timestamp(TimestampType::Millisecond(TimestampMillisecondType));
        let ts_val = Value::Timestamp(Timestamp::new(to_ms_ts(precision, val), Millisecond));
        self.write(column_name, datatype, ts_val);
    }

    pub fn write_tag(&mut self, column_name: &str, value: &str) {
        self.write(
            column_name,
            ConcreteDataType::string_datatype(),
            Value::String(value.into()),
        );
    }

    pub fn write_u64(&mut self, column_name: &str, value: u64) {
        self.write(
            column_name,
            ConcreteDataType::uint64_datatype(),
            Value::UInt64(value),
        );
    }

    pub fn write_i64(&mut self, column_name: &str, value: i64) {
        self.write(
            column_name,
            ConcreteDataType::int64_datatype(),
            Value::Int64(value),
        );
    }

    pub fn write_f64(&mut self, column_name: &str, value: f64) {
        self.write(
            column_name,
            ConcreteDataType::float64_datatype(),
            Value::Float64(value.into()),
        );
    }

    pub fn write_string(&mut self, column_name: &str, value: &str) {
        self.write(
            column_name,
            ConcreteDataType::string_datatype(),
            Value::String(value.into()),
        );
    }

    pub fn write_bool(&mut self, column_name: &str, value: bool) {
        self.write(
            column_name,
            ConcreteDataType::boolean_datatype(),
            Value::Boolean(value),
        );
    }

    fn write(&mut self, column_name: &str, datatype: ConcreteDataType, value: Value) {
        let or_insert = || {
            let rows = self.current_rows;
            let mut builder = datatype.create_mutable_vector(self.expected_rows);
            (0..rows).for_each(|_| builder.push_null());
            (builder, rows)
        };
        let (builder, column_len) = self
            .columns_builders
            .entry(column_name.to_string())
            .or_insert_with(or_insert);

        builder.push_value_ref(value.as_value_ref());
        *column_len += 1;
    }

    pub fn commit(&mut self) {
        self.current_rows += 1;
        self.columns_builders
            .values_mut()
            .for_each(|(builder, len)| {
                if self.current_rows > *len {
                    builder.push_null()
                }
            });
    }

    pub fn finish(self) -> InsertRequest {
        let columns_values: HashMap<ColumnName, VectorRef> = self
            .columns_builders
            .into_iter()
            .map(|(column_name, (mut builder, _))| (column_name, builder.to_vector()))
            .collect();
        InsertRequest {
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
            schema_name: self.db,
            table_name: self.table_name,
            columns_values,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_catalog::consts::DEFAULT_SCHEMA_NAME;
    use common_time::Timestamp;
    use datatypes::value::Value;
    use datatypes::vectors::Vector;

    use crate::line_writer::{LineWriter, Precision};

    #[test]
    fn test_writer() {
        let mut writer = LineWriter::with_lines(DEFAULT_SCHEMA_NAME, "demo".to_string(), 4);
        writer.write_ts("ts", (1665893727685, Precision::Millisecond));
        writer.write_tag("host", "host-1");
        writer.write_i64("memory", 10_i64);
        writer.commit();

        writer.write_ts("ts", (1665893727686, Precision::Millisecond));
        writer.write_tag("host", "host-2");
        writer.write_tag("region", "region-2");
        writer.write_i64("memory", 9_i64);
        writer.commit();

        writer.write_ts("ts", (1665893727689, Precision::Millisecond));
        writer.write_tag("host", "host-3");
        writer.write_tag("region", "region-3");
        writer.write_i64("cpu", 19_i64);
        writer.commit();

        let insert_request = writer.finish();

        assert_eq!("demo", insert_request.table_name);
        assert_eq!(DEFAULT_SCHEMA_NAME, insert_request.schema_name);

        let columns = insert_request.columns_values;
        assert_eq!(5, columns.len());
        assert!(columns.contains_key("ts"));
        assert!(columns.contains_key("host"));
        assert!(columns.contains_key("memory"));
        assert!(columns.contains_key("region"));
        assert!(columns.contains_key("cpu"));

        let ts = columns.get("ts").unwrap();
        let host = columns.get("host").unwrap();
        let memory = columns.get("memory").unwrap();
        let region = columns.get("region").unwrap();
        let cpu = columns.get("cpu").unwrap();

        let expected: Vec<Value> = vec![
            Value::Timestamp(Timestamp::new_millisecond(1665893727685_i64)),
            Value::Timestamp(Timestamp::new_millisecond(1665893727686_i64)),
            Value::Timestamp(Timestamp::new_millisecond(1665893727689_i64)),
        ];
        assert_vector(&expected, ts);

        let expected: Vec<Value> = vec!["host-1".into(), "host-2".into(), "host-3".into()];
        assert_vector(&expected, host);

        let expected: Vec<Value> = vec![10_i64.into(), 9_i64.into(), Value::Null];
        assert_vector(&expected, memory);

        let expected: Vec<Value> = vec![Value::Null, "region-2".into(), "region-3".into()];
        assert_vector(&expected, region);

        let expected: Vec<Value> = vec![Value::Null, Value::Null, 19_i64.into()];
        assert_vector(&expected, cpu);
    }

    fn assert_vector(expected: &[Value], vector: &Arc<dyn Vector>) {
        for (idx, expected) in expected.iter().enumerate() {
            let val = vector.get(idx);
            assert_eq!(*expected, val);
        }
    }
}
