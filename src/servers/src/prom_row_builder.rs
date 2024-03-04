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
use lazy_static::lazy_static;

use crate::proto::PromLabel;

lazy_static! {
    static ref TIMESTAMP_COLUMN_SCHEMA: ColumnSchema = ColumnSchema {
        column_name: GREPTIME_TIMESTAMP.to_string(),
        datatype: ColumnDataType::TimestampMillisecond as i32,
        semantic_type: SemanticType::Timestamp as i32,
        datatype_extension: None,
    };
    static ref VALUE_COLUMN_SCHEMA: ColumnSchema = ColumnSchema {
        column_name: GREPTIME_VALUE.to_string(),
        datatype: ColumnDataType::Float64 as i32,
        semantic_type: SemanticType::Field as i32,
        datatype_extension: None,
    };
}

#[derive(Default)]
pub(crate) struct TablesBuilder {
    tables: HashMap<String, TableBuilder>,
}

impl TablesBuilder {
    pub(crate) fn get_or_create_table_builder(&mut self, table_name: String) -> &mut TableBuilder {
        self.tables.entry(table_name).or_default()
    }

    pub(crate) fn to_insert_requests(&mut self) -> (RowInsertRequests, usize) {
        let mut total_rows = 0;

        let inserts = self
            .tables
            .iter_mut()
            .map(|(name, table)| {
                total_rows += table.num_rows();
                table.to_row_insert_request(name.clone())
            })
            .collect();
        (RowInsertRequests { inserts }, total_rows)
    }

    pub(crate) fn clear(&mut self) {
        for (_, t) in &mut self.tables {
            t.clear();
        }
    }
}

#[derive(Default)]
pub(crate) struct TableBuilder {
    tag_schemas: Vec<ColumnSchema>,
    tag_rows: Vec<Row>,
    tag_indexes: HashMap<String, usize>,
    samples: Vec<Sample>,
}

impl TableBuilder {
    pub(crate) fn add_labels(&mut self, labels: Vec<PromLabel>) {
        let mut tag_row = vec![Value { value_data: None }; self.tag_indexes.len()];

        for PromLabel { name, value } in labels {
            let tag_name = unsafe { String::from_utf8_unchecked(name.to_vec()) };
            let tag_value = unsafe { String::from_utf8_unchecked(value.to_vec()) };

            let tag_value = Some(ValueData::StringValue(tag_value));
            let tag_num = self.tag_indexes.len();
            match self.tag_indexes.entry(tag_name) {
                Entry::Occupied(e) => {
                    tag_row[*e.get()].value_data = tag_value;
                }
                Entry::Vacant(e) => {
                    let column_name = e.key().clone();
                    e.insert(tag_num);
                    self.tag_schemas.push(ColumnSchema {
                        column_name,
                        datatype: ColumnDataType::String as i32,
                        semantic_type: SemanticType::Tag as i32,
                        datatype_extension: None,
                    });
                    tag_row.push(Value {
                        value_data: tag_value,
                    });
                }
            }
        }

        self.tag_rows.push(Row { values: tag_row });
    }

    pub(crate) fn add_samples(&mut self, samples: Vec<Sample>) {
        self.samples.extend(samples);
    }

    fn num_rows(&self) -> usize {
        self.samples.len()
    }

    pub(crate) fn to_row_insert_request(&mut self, table_name: String) -> RowInsertRequest {
        let tag_num = self.tag_indexes.len();
        let rows_num = self.samples.len();

        let samples = std::mem::take(&mut self.samples);
        let mut tag_rows = std::mem::take(&mut self.tag_rows);

        let mut schema = self.tag_schemas.clone();
        schema.push(VALUE_COLUMN_SCHEMA.clone());
        schema.push(TIMESTAMP_COLUMN_SCHEMA.clone());

        assert_eq!(rows_num, tag_rows.len());

        tag_rows.iter_mut().zip(samples.into_iter()).for_each(
            |(tag_row, Sample { timestamp, value })| {
                tag_row
                    .values
                    .resize(tag_num + 2, Value { value_data: None });
                tag_row.values[tag_num].value_data = Some(ValueData::F64Value(value));
                tag_row.values[tag_num + 1].value_data =
                    Some(ValueData::TimestampMillisecondValue(timestamp));
            },
        );

        RowInsertRequest {
            table_name,
            rows: Some(Rows {
                schema,
                rows: tag_rows,
            }),
        }
    }

    pub fn clear(&mut self) {
        // self.tag_indexes.clear();
        // self.tag_schemas.clear();
        self.samples.clear();
        self.tag_rows.clear();
    }
}

#[cfg(test)]
mod tests {
    use api::prom_store::remote::Sample;
    use bytes::Bytes;

    use crate::prom_row_builder::TableBuilder;
    use crate::proto::PromLabel;

    #[test]
    fn test_table_builder() {
        let mut builder = TableBuilder::default();
        builder.add_labels(vec![
            PromLabel {
                name: Bytes::from("tag0"),
                value: Bytes::from("v0"),
            },
            PromLabel {
                name: Bytes::from("tag1"),
                value: Bytes::from("v1"),
            },
        ]);

        builder.add_samples(vec![Sample {
            value: 0.0,
            timestamp: 0,
        }]);

        builder.add_labels(vec![
            PromLabel {
                name: Bytes::from("tag0"),
                value: Bytes::from("v0"),
            },
            PromLabel {
                name: Bytes::from("tag2"),
                value: Bytes::from("v2"),
            },
        ]);

        builder.add_samples(vec![Sample {
            value: 0.1,
            timestamp: 1,
        }]);

        let request = builder.to_row_insert_request("test".to_string());
        let rows = request.rows.unwrap().rows;
        assert_eq!(2, rows.len());
        for r in rows {
            println!("{:?}", r);
        }
    }
}
