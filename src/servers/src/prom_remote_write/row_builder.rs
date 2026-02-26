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

use api::helper::{ColumnDataTypeWrapper, pb_value_to_value_ref};
use api::prom_store::remote::Sample;
use api::v1::helper::{field_column_schema, time_index_column_schema};
use api::v1::value::ValueData;
use api::v1::{ColumnDataType, ColumnSchema, Row, RowInsertRequest, Rows, SemanticType, Value};
use arrow::record_batch::RecordBatch;
use common_query::prelude::{greptime_timestamp, greptime_value};
use datatypes::data_type::DataType;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema as DtColumnSchema, Schema as DtSchema};
use pipeline::{ContextOpt, ContextReq};
use prost::DecodeError;

use crate::error::{Error, Result};
use crate::http::PromValidationMode;
use crate::proto::PromLabel;
use crate::prom_remote_write::PromValidationMode;
use crate::prom_remote_write::types::PromLabel;
use crate::prom_remote_write::validation::validate_label_name;
use crate::repeated_field::Clear;

#[derive(Debug, Default, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct PromCtx {
    pub schema: Option<String>,
    pub physical_table: Option<String>,
}

#[derive(Default, Debug)]
pub struct TablesBuilder<'a> {
    pub tables: HashMap<PromCtx, HashMap<String, TableBuilder<'a>>>,
    pub(crate) raw_data: Vec<u8>,
}

#[derive(Debug)]
pub struct PromRecordBatchGroup {
    pub prom_ctx: PromCtx,
    pub table_batches: Vec<(String, RecordBatch)>,
}

impl<'a> Clear for TablesBuilder<'a> {
    fn clear(&mut self) {
        self.tables.clear();
        self.raw_data.clear();
    }
}

impl<'a> TablesBuilder<'a> {
    pub(crate) fn get_or_create_table_builder(
        &mut self,
        prom_ctx: PromCtx,
        table_name: String,
        label_num: usize,
        row_num: usize,
    ) -> &mut TableBuilder<'a> {
        self.tables
            .entry(prom_ctx)
            .or_default()
            .entry(table_name)
            .or_insert_with(|| TableBuilder::with_capacity(label_num + 2, row_num))
    }

    pub(crate) fn as_insert_requests(&mut self) -> ContextReq {
        self.tables
            .drain()
            .map(|(prom, mut tables)| {
                let mut opt = ContextOpt::default();
                if let Some(physical_table) = prom.physical_table {
                    opt.set_physical_table(physical_table);
                }
                if let Some(schema) = prom.schema {
                    opt.set_schema(schema);
                }

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

    pub fn as_record_batch_groups(&mut self) -> Result<Vec<PromRecordBatchGroup>> {
        self.tables
            .drain()
            .map(|(prom_ctx, mut tables)| {
                let mut table_batches = Vec::with_capacity(tables.len());
                for (table_name, mut table) in tables.drain() {
                    let record_batch = table.take_record_batch()?;
                    if record_batch.num_rows() == 0 {
                        continue;
                    }
                    table_batches.push((table_name, record_batch));
                }

                Ok(PromRecordBatchGroup {
                    prom_ctx,
                    table_batches,
                })
            })
            .collect()
    }

    pub(crate) fn set_raw_data(&mut self, buf: Vec<u8>) {
        self.raw_data = buf;
    }
}

#[derive(Debug)]
pub struct TableBuilder<'a> {
    schema: Vec<ColumnSchema>,
    rows: Vec<Row>,
    col_indexes: HashMap<&'a [u8], usize>,
}

impl<'a> Default for TableBuilder<'a> {
    fn default() -> Self {
        Self::with_capacity(2, 0)
    }
}

impl<'a> TableBuilder<'a> {
    pub(crate) fn with_capacity(cols: usize, rows: usize) -> Self {
        let mut col_indexes = HashMap::with_capacity_and_hasher(cols, Default::default());
        col_indexes.insert(greptime_timestamp().as_bytes(), 0);
        col_indexes.insert(greptime_value().as_bytes(), 1);

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

    pub(crate) fn add_labels_and_samples(
        &mut self,
        labels: &[PromLabel],
        samples: &[Sample],
        prom_validation_mode: PromValidationMode,
    ) -> Result<(), DecodeError> {
        let mut row = vec![Value { value_data: None }; self.col_indexes.len()];

        for PromLabel { name, value } in labels {
            if !validate_label_name(name) {
                return Err(DecodeError::new(format!(
                    "Invalid label name: `{}`",
                    String::from_utf8_lossy(name)
                )));
            }
            let raw_tag_name = *name;
            let tag_value = Some(ValueData::StringValue(
                prom_validation_mode.decode_string(value)?,
            ));
            let tag_num = self.col_indexes.len();

            if let Some(e) = self.col_indexes.get_mut(raw_tag_name) {
                row[*e].value_data = tag_value;
                continue;
            }

            let tag_name = unsafe { std::str::from_utf8_unchecked(raw_tag_name) };
            self.schema.push(ColumnSchema {
                column_name: tag_name.to_owned(),
                datatype: ColumnDataType::String as i32,
                semantic_type: SemanticType::Tag as i32,
                ..Default::default()
            });
            self.col_indexes.insert(raw_tag_name, tag_num);

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

    pub fn as_row_insert_request(&mut self, table_name: String) -> RowInsertRequest {
        let rows = self.take_rows();

        RowInsertRequest {
            table_name,
            rows: Some(rows),
        }
    }

    pub fn take_rows(&mut self) -> Rows {
        let (schema, rows) = self.take_schema_rows();
        Rows { schema, rows }
    }

    pub fn take_record_batch(&mut self) -> Result<RecordBatch> {
        let (schema, rows) = self.take_schema_rows();
        schema_rows_to_record_batch(&schema, &rows)
    }

    fn take_schema_rows(&mut self) -> (Vec<ColumnSchema>, Vec<Row>) {
        let mut rows = std::mem::take(&mut self.rows);
        let schema = std::mem::take(&mut self.schema);
        let col_num = schema.len();
        for row in &mut rows {
            if row.values.len() < col_num {
                row.values.resize(col_num, Value { value_data: None });
            }
        }

        (schema, rows)
    }

    pub fn tags(&self) -> impl Iterator<Item = &String> {
        self.schema
            .iter()
            .filter(|v| v.semantic_type == SemanticType::Tag as i32)
            .map(|c| &c.column_name)
    }
}

fn schema_rows_to_record_batch(schema: &[ColumnSchema], rows: &[Row]) -> Result<RecordBatch> {
    let row_count = rows.len();
    let column_count = schema.len();

    for (idx, row) in rows.iter().enumerate() {
        if row.values.len() != column_count {
            return Err(Error::Internal {
                err_msg: format!(
                    "Column count mismatch in row {}, expected {}, got {}",
                    idx,
                    column_count,
                    row.values.len()
                ),
            });
        }
    }

    let mut vectors = Vec::with_capacity(column_count);
    let mut column_schemas = Vec::with_capacity(column_count);
    for (idx, column_schema) in schema.iter().enumerate() {
        let data_type = ColumnDataTypeWrapper::try_new(
            column_schema.datatype,
            column_schema.datatype_extension.clone(),
        )
        .map_err(|err| Error::InvalidParameter {
            reason: format!(
                "Invalid datatype for column '{}': {}",
                column_schema.column_name, err
            ),
            location: snafu::Location::new(file!(), line!(), column!()),
        })
        .map(ConcreteDataType::from)?;

        let mut mutable = data_type.create_mutable_vector(row_count);
        for row in rows {
            let value_ref =
                pb_value_to_value_ref(&row.values[idx], column_schema.datatype_extension.as_ref());
            mutable
                .try_push_value_ref(&value_ref)
                .map_err(|err| Error::InvalidParameter {
                    reason: format!(
                        "Failed to convert value for column '{}': {}",
                        column_schema.column_name, err
                    ),
                    location: snafu::Location::new(file!(), line!(), column!()),
                })?;
        }

        vectors.push(mutable.to_vector());
        column_schemas.push(DtColumnSchema::new(
            column_schema.column_name.clone(),
            data_type,
            true,
        ));
    }

    let schema = std::sync::Arc::new(DtSchema::new(column_schemas));
    common_recordbatch::RecordBatch::new(schema, vectors)
        .map(|rb| rb.into_df_record_batch())
        .map_err(|err| Error::Internal {
            err_msg: format!("Failed to build record batch from rows: {}", err),
        })
}

#[cfg(test)]
mod tests {
    use api::prom_store::remote::Sample;
    use prost::DecodeError;

    use super::*;

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

    #[test]
    fn test_tables_builder_as_record_batch_groups() {
        let mut tables_builder = TablesBuilder::default();
        let table = tables_builder.get_or_create_table_builder(
            PromCtx::default(),
            "metric".to_string(),
            1,
            1,
        );
        table
            .add_labels_and_samples(
                &[PromLabel {
                    name: Bytes::from("host"),
                    value: Bytes::from("h1"),
                }],
                &[Sample {
                    value: 42.0,
                    timestamp: 1000,
                }],
                PromValidationMode::Strict,
            )
            .unwrap();

        let groups = tables_builder.as_record_batch_groups().unwrap();
        assert_eq!(1, groups.len());
        assert_eq!(1, groups[0].table_batches.len());
        assert_eq!("metric", groups[0].table_batches[0].0);
        assert_eq!(1, groups[0].table_batches[0].1.num_rows());
    }
}
