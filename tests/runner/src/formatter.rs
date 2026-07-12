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

use std::borrow::Cow;
use std::fmt::Display;
use std::sync::Arc;

use client::{Output, OutputData, RecordBatches};
use common_error::ext::ErrorExt;
use datatypes::prelude::ConcreteDataType;
use datatypes::scalars::ScalarVectorBuilder;
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::vectors::{StringVectorBuilder, VectorRef};
use mysql::Row as MySqlRow;
use tokio_postgres::SimpleQueryMessage as PgRow;

use crate::client::MysqlSqlResult;

/// A formatter for errors.
pub struct ErrorFormatter<E: ErrorExt>(E);

impl<E: ErrorExt> From<E> for ErrorFormatter<E> {
    fn from(error: E) -> Self {
        ErrorFormatter(error)
    }
}

impl<E: ErrorExt> Display for ErrorFormatter<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let status_code = self.0.status_code();
        let root_cause = self.0.output_msg();
        write!(
            f,
            "Error: {}({status_code}), {root_cause}",
            status_code as u32
        )
    }
}

/// A formatter for [`Output`].
pub struct OutputFormatter(Output);

impl From<Output> for OutputFormatter {
    fn from(output: Output) -> Self {
        OutputFormatter(output)
    }
}

impl Display for OutputFormatter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0.data {
            OutputData::AffectedRows(rows) => {
                write!(f, "Affected Rows: {rows}")
            }
            OutputData::RecordBatches(recordbatches) => {
                let pretty = recordbatches.pretty_print().map_err(|e| e.to_string());
                match pretty {
                    Ok(s) => write!(f, "{s}"),
                    Err(e) => {
                        write!(f, "Failed to pretty format {recordbatches:?}, error: {e}")
                    }
                }
            }
            OutputData::Stream(_) => unreachable!(),
        }
    }
}

pub struct PostgresqlFormatter(Vec<PgRow>);

impl From<Vec<PgRow>> for PostgresqlFormatter {
    fn from(rows: Vec<PgRow>) -> Self {
        PostgresqlFormatter(rows)
    }
}

impl Display for PostgresqlFormatter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0.is_empty() {
            return f.write_fmt(format_args!("(Empty response)"));
        }

        if let PgRow::CommandComplete(affected_rows) = &self.0[0] {
            return write!(
                f,
                "{}",
                OutputFormatter(Output::new_with_affected_rows(*affected_rows as usize))
            );
        };

        let Some(recordbatches) = build_recordbatches_from_postgres_rows(&self.0) else {
            return Ok(());
        };
        write!(
            f,
            "{}",
            OutputFormatter(Output::new_with_record_batches(recordbatches))
        )
    }
}

fn build_recordbatches_from_postgres_rows(rows: &[PgRow]) -> Option<RecordBatches> {
    // create schema
    let schema = match &rows[0] {
        PgRow::RowDescription(desc) => Arc::new(Schema::new(
            desc.iter()
                .map(|column| {
                    ColumnSchema::new(column.name(), ConcreteDataType::string_datatype(), true)
                })
                .collect(),
        )),
        _ => unreachable!(),
    };
    if schema.num_columns() == 0 {
        return None;
    }

    // convert to string vectors
    let mut columns: Vec<StringVectorBuilder> = (0..schema.num_columns())
        .map(|_| StringVectorBuilder::with_capacity(schema.num_columns()))
        .collect();
    for row in rows.iter().skip(1) {
        if let PgRow::Row(row) = row {
            for (i, column) in columns.iter_mut().enumerate().take(schema.num_columns()) {
                column.push(row.get(i));
            }
        }
    }
    let columns: Vec<VectorRef> = columns
        .into_iter()
        .map(|mut col| Arc::new(col.finish()) as VectorRef)
        .collect();

    // construct recordbatch
    let recordbatches = RecordBatches::try_from_columns(schema, columns)
        .expect("Failed to construct recordbatches from columns. Please check the schema.");
    Some(recordbatches)
}

/// A formatter for [`MysqlSqlResult`].
pub struct MysqlFormatter(MysqlSqlResult);

impl From<MysqlSqlResult> for MysqlFormatter {
    fn from(result: MysqlSqlResult) -> Self {
        MysqlFormatter(result)
    }
}

impl Display for MysqlFormatter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            MysqlSqlResult::AffectedRows(rows) => {
                write!(f, "affected_rows: {rows}")
            }
            MysqlSqlResult::Rows(rows) => {
                if rows.is_empty() {
                    return f.write_fmt(format_args!("(Empty response)"));
                }

                let recordbatches = build_recordbatches_from_mysql_rows(rows);
                write!(
                    f,
                    "{}",
                    OutputFormatter(Output::new_with_record_batches(recordbatches))
                )
            }
        }
    }
}

pub fn build_recordbatches_from_mysql_rows(rows: &[MySqlRow]) -> RecordBatches {
    // create schema
    let head_column = &rows[0];
    let head_binding = head_column.columns();
    let names = head_binding
        .iter()
        .map(|column| column.name_str())
        .collect::<Vec<Cow<str>>>();
    let schema = Arc::new(Schema::new(
        names
            .iter()
            .map(|name| {
                ColumnSchema::new(name.to_string(), ConcreteDataType::string_datatype(), false)
            })
            .collect(),
    ));

    // convert to string vectors
    let mut columns: Vec<StringVectorBuilder> = (0..schema.num_columns())
        .map(|_| StringVectorBuilder::with_capacity(schema.num_columns()))
        .collect();
    for row in rows.iter() {
        for (i, name) in names.iter().enumerate() {
            columns[i].push(row.get::<String, &str>(name).as_deref());
        }
    }
    let columns: Vec<VectorRef> = columns
        .into_iter()
        .map(|mut col| Arc::new(col.finish()) as VectorRef)
        .collect();

    // construct recordbatch
    RecordBatches::try_from_columns(schema, columns)
        .expect("Failed to construct recordbatches from columns. Please check the schema.")
}
