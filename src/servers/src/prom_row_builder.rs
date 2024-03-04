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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::string::ToString;

use api::prom_store::remote::Sample;
use api::v1::value::ValueData;
use api::v1::{
    ColumnDataType, ColumnSchema, Row, RowInsertRequest, RowInsertRequests, Rows, SemanticType,
    Value,
};
use common_query::prelude::{GREPTIME_TIMESTAMP, GREPTIME_VALUE};

use crate::proto::PromLabel;
use crate::repeated_field::Clear;

#[derive(Default)]
pub(crate) struct TablesBuilder {
    tables: HashMap<String, TableBuilder>,
}

impl Clear for TablesBuilder {
    fn clear(&mut self) {
        self.tables.clear();
    }
}

impl TablesBuilder {
    pub(crate) fn get_or_create_table_builder(&mut self, table_name: String) -> &mut TableBuilder {
        self.tables.entry(table_name).or_default()
    }

    pub(crate) fn as_insert_requests(&mut self) -> (RowInsertRequests, usize) {
        let mut total_rows = 0;
        let inserts = self
            .tables
            .drain()
            .map(|(name, mut table)| {
                total_rows += table.num_rows();
                table.as_row_insert_request(name)
            })
            .collect();
        (RowInsertRequests { inserts }, total_rows)
    }
}

pub(crate) struct TableBuilder {
    schema: Vec<ColumnSchema>,
    rows: Vec<Row>,
    col_indexes: HashMap<String, usize>,
}

impl Default for TableBuilder {
    fn default() -> Self {
        let col_indexes = [
            (GREPTIME_TIMESTAMP.to_string(), 0),
            (GREPTIME_VALUE.to_string(), 1),
        ]
        .into_iter()
        .collect();
        Self {
            schema: vec![
                ColumnSchema {
                    column_name: GREPTIME_TIMESTAMP.to_string(),
                    datatype: ColumnDataType::TimestampMillisecond as i32,
                    semantic_type: SemanticType::Timestamp as i32,
                    datatype_extension: None,
                },
                ColumnSchema {
                    column_name: GREPTIME_VALUE.to_string(),
                    datatype: ColumnDataType::Float64 as i32,
                    semantic_type: SemanticType::Field as i32,
                    datatype_extension: None,
                },
            ],
            rows: vec![],
            col_indexes,
        }
    }
}

impl TableBuilder {
    fn num_rows(&self) -> usize {
        self.rows.len()
    }
    pub(crate) fn add_labels_and_samples(&mut self, labels: &[PromLabel], samples: &[Sample]) {
        let mut row = vec![Value { value_data: None }; self.col_indexes.len()];

        for PromLabel { name, value } in labels {
            let tag_name = unsafe { String::from_utf8_unchecked(name.to_vec()) };
            let tag_value = unsafe { String::from_utf8_unchecked(value.to_vec()) };
            let tag_value = Some(ValueData::StringValue(tag_value));
            let tag_num = self.col_indexes.len();

            match self.col_indexes.entry(tag_name) {
                Entry::Occupied(e) => {
                    row[*e.get()].value_data = tag_value;
                }
                Entry::Vacant(e) => {
                    let column_name = e.key().clone();
                    e.insert(tag_num);
                    self.schema.push(ColumnSchema {
                        column_name,
                        datatype: ColumnDataType::String as i32,
                        semantic_type: SemanticType::Tag as i32,
                        datatype_extension: None,
                    });
                    row.push(Value {
                        value_data: tag_value,
                    });
                }
            }
        }

        if samples.len() == 1 {
            let sample = &samples[0];
            row[0].value_data = Some(ValueData::TimestampMillisecondValue(sample.timestamp));
            row[1].value_data = Some(ValueData::F64Value(sample.value));
            self.rows.push(Row { values: row });
            return;
        }
        for sample in samples {
            row[0].value_data = Some(ValueData::TimestampMillisecondValue(sample.timestamp));
            row[1].value_data = Some(ValueData::F64Value(sample.value));
            self.rows.push(Row {
                values: row.clone(),
            });
        }
    }

    pub(crate) fn as_row_insert_request(&mut self, table_name: String) -> RowInsertRequest {
        let mut rows = std::mem::take(&mut self.rows);
        let schema = std::mem::take(&mut self.schema);
        let col_num = schema.len();
        for row in &mut rows {
            if row.values.len() < col_num {
                row.values.resize(col_num, Value { value_data: None });
            }
        }

        RowInsertRequest {
            table_name,
            rows: Some(Rows { schema, rows }),
        }
    }
}

#[cfg(test)]
mod tests {
    use api::prom_store::remote::Sample;
    use api::v1::value::ValueData;
    use api::v1::Value;
    use bytes::Bytes;

    use crate::prom_row_builder::TableBuilder;
    use crate::proto::PromLabel;
    #[test]
    fn test_table_builder() {
        let mut builder = TableBuilder::default();
        builder.add_labels_and_samples(
            &[
                PromLabel {
                    name: Bytes::from("tag0"),
                    value: Bytes::from("v0"),
                },
                PromLabel {
                    name: Bytes::from("tag1"),
                    value: Bytes::from("v1"),
                },
            ],
            &[Sample {
                value: 0.0,
                timestamp: 0,
            }],
        );

        builder.add_labels_and_samples(
            &[
                PromLabel {
                    name: Bytes::from("tag0"),
                    value: Bytes::from("v0"),
                },
                PromLabel {
                    name: Bytes::from("tag2"),
                    value: Bytes::from("v2"),
                },
            ],
            &[Sample {
                value: 0.1,
                timestamp: 1,
            }],
        );

        let request = builder.as_row_insert_request("test".to_string());
        let rows = request.rows.unwrap().rows;
        assert_eq!(2, rows.len());

        assert_eq!(
            vec![
                Value {
                    value_data: Some(ValueData::TimestampMillisecondValue(0))
                },
                Value {
                    value_data: Some(ValueData::F64Value(0.0))
                },
                Value {
                    value_data: Some(ValueData::StringValue("v0".to_string()))
                },
                Value {
                    value_data: Some(ValueData::StringValue("v1".to_string()))
                },
                Value { value_data: None },
            ],
            rows[0].values
        );

        assert_eq!(
            vec![
                Value {
                    value_data: Some(ValueData::TimestampMillisecondValue(1))
                },
                Value {
                    value_data: Some(ValueData::F64Value(0.1))
                },
                Value {
                    value_data: Some(ValueData::StringValue("v0".to_string()))
                },
                Value { value_data: None },
                Value {
                    value_data: Some(ValueData::StringValue("v2".to_string()))
                },
            ],
            rows[1].values
        );
    }
}
