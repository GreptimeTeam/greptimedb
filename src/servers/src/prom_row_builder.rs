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
use std::string::ToString;

use api::prom_store::remote::Sample;
use api::v1::helper::{field_column_schema, time_index_column_schema};
use api::v1::value::ValueData;
use api::v1::{ColumnDataType, ColumnSchema, Row, RowInsertRequest, Rows, SemanticType, Value};
use common_query::prelude::{greptime_timestamp, greptime_value};
use datafusion::parquet::data_type::AsBytes;
use pipeline::{ContextOpt, ContextReq};
use prost::DecodeError;

use crate::http::PromValidationMode;
use crate::proto::PromLabel;
use crate::repeated_field::Clear;

// Prometheus remote write context
#[derive(Debug, Default, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct PromCtx {
    pub schema: Option<String>,
    pub physical_table: Option<String>,
}

/// [TablesBuilder] serves as an intermediate container to build [RowInsertRequests].
#[derive(Default, Debug)]
pub struct TablesBuilder {
    // schema -> table -> table_builder
    pub tables: HashMap<PromCtx, HashMap<String, TableBuilder>>,
}

impl Clear for TablesBuilder {
    fn clear(&mut self) {
        self.tables.clear();
    }
}

impl TablesBuilder {
    /// Gets table builder with given table name. Creates an empty [TableBuilder] if not exist.
    pub(crate) fn get_or_create_table_builder(
        &mut self,
        prom_ctx: PromCtx,
        table_name: String,
        label_num: usize,
        row_num: usize,
    ) -> &mut TableBuilder {
        self.tables
            .entry(prom_ctx)
            .or_default()
            .entry(table_name)
            .or_insert_with(|| TableBuilder::with_capacity(label_num + 2, row_num))
    }

    /// Converts [TablesBuilder] to [RowInsertRequests] and row numbers and clears inner states.
    pub(crate) fn as_insert_requests(&mut self) -> ContextReq {
        self.tables
            .drain()
            .map(|(prom, mut tables)| {
                // create context opt
                let mut opt = ContextOpt::default();
                if let Some(physical_table) = prom.physical_table {
                    opt.set_physical_table(physical_table);
                }
                if let Some(schema) = prom.schema {
                    opt.set_schema(schema);
                }

                // create and set context req
                let mut ctx_req = ContextReq::default();
                let reqs = tables
                    .drain()
                    .map(|(table_name, mut table)| table.as_row_insert_request(table_name));
                ctx_req.add_rows(opt, reqs);

                ctx_req
            })
            .fold(ContextReq::default(), |mut req, reqs| {
                req.merge(reqs);
                req
            })
    }
}

/// Builder for one table.
#[derive(Debug)]
pub struct TableBuilder {
    /// Column schemas.
    schema: Vec<ColumnSchema>,
    /// Rows written.
    rows: Vec<Row>,
    /// Indices of columns inside `schema`.
    col_indexes: HashMap<Vec<u8>, usize>,
}

impl Default for TableBuilder {
    fn default() -> Self {
        Self::with_capacity(2, 0)
    }
}

impl TableBuilder {
    pub(crate) fn with_capacity(cols: usize, rows: usize) -> Self {
        let mut col_indexes = HashMap::with_capacity_and_hasher(cols, Default::default());
        col_indexes.insert(greptime_timestamp().as_bytes().to_owned(), 0);
        col_indexes.insert(greptime_value().as_bytes().to_owned(), 1);

        let mut schema = Vec::with_capacity(cols);
        schema.push(time_index_column_schema(
            greptime_timestamp(),
            ColumnDataType::TimestampMillisecond,
        ));
        schema.push(field_column_schema(
            greptime_value(),
            ColumnDataType::Float64,
        ));

        Self {
            schema,
            rows: Vec::with_capacity(rows),
            col_indexes,
        }
    }

    /// Adds a set of labels and samples to table builder.
    pub(crate) fn add_labels_and_samples(
        &mut self,
        labels: &[PromLabel],
        samples: &[Sample],
        prom_validation_mode: PromValidationMode,
    ) -> Result<(), DecodeError> {
        let mut row = vec![Value { value_data: None }; self.col_indexes.len()];

        for PromLabel { name, value } in labels {
            prom_validation_mode.validate_bytes(name)?;
            let raw_tag_name = name;
            let tag_value = Some(ValueData::StringValue(
                prom_validation_mode.decode_string(value)?,
            ));
            let tag_num = self.col_indexes.len();

            if let Some(e) = self.col_indexes.get_mut(*raw_tag_name) {
                row[*e].value_data = tag_value;
                continue;
            }
            let tag_name = prom_validation_mode.decode_string(*raw_tag_name)?;
            self.schema.push(ColumnSchema {
                column_name: tag_name.clone(),
                datatype: ColumnDataType::String as i32,
                semantic_type: SemanticType::Tag as i32,
                ..Default::default()
            });
            self.col_indexes.insert(tag_name.into_bytes(), tag_num);

            row.push(Value {
                value_data: tag_value,
            });
        }

        if samples.len() == 1 {
            let sample = &samples[0];
            row[0].value_data = Some(ValueData::TimestampMillisecondValue(sample.timestamp));
            row[1].value_data = Some(ValueData::F64Value(sample.value));
            self.rows.push(Row { values: row });
            return Ok(());
        }
        for sample in samples {
            row[0].value_data = Some(ValueData::TimestampMillisecondValue(sample.timestamp));
            row[1].value_data = Some(ValueData::F64Value(sample.value));
            self.rows.push(Row {
                values: row.clone(),
            });
        }

        Ok(())
    }

    /// Converts [TableBuilder] to [RowInsertRequest] and clears buffered data.
    pub fn as_row_insert_request(&mut self, table_name: String) -> RowInsertRequest {
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

    pub fn tags(&self) -> impl Iterator<Item = &String> {
        self.schema
            .iter()
            .filter(|v| v.semantic_type == SemanticType::Tag as i32)
            .map(|c| &c.column_name)
    }
}

#[cfg(test)]
mod tests {
    use api::prom_store::remote::Sample;
    use api::v1::Value;
    use api::v1::value::ValueData;
    use prost::DecodeError;

    use crate::http::PromValidationMode;
    use crate::prom_row_builder::TableBuilder;
    use crate::proto::PromLabel;
    #[test]
    fn test_table_builder() {
        let mut builder = TableBuilder::default();
        let _ = builder.add_labels_and_samples(
            &[
                PromLabel {
                    name: b"tag0",
                    value: b"v0",
                },
                PromLabel {
                    name: b"tag1",
                    value: b"v1",
                },
            ],
            &[Sample {
                value: 0.0,
                timestamp: 0,
            }],
            PromValidationMode::Strict,
        );

        let _ = builder.add_labels_and_samples(
            &[
                PromLabel {
                    name: b"tag0",
                    value: b"v0",
                },
                PromLabel {
                    name: b"tag2",
                    value: b"v2",
                },
            ],
            &[Sample {
                value: 0.1,
                timestamp: 1,
            }],
            PromValidationMode::Strict,
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

        let invalid_utf8_bytes = &[0xFF, 0xFF, 0xFF];

        let res = builder.add_labels_and_samples(
            &[PromLabel {
                name: b"tag0",
                value: invalid_utf8_bytes,
            }],
            &[Sample {
                value: 0.1,
                timestamp: 1,
            }],
            PromValidationMode::Strict,
        );
        assert_eq!(res, Err(DecodeError::new("invalid utf-8")));
    }
}
