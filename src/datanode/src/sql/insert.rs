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
use catalog::CatalogManagerRef;
use common_query::Output;
use common_recordbatch::RecordBatches;
use datatypes::data_type::DataType;
use datatypes::schema::{ColumnSchema, SchemaRef};
use datatypes::vectors::MutableVector;
use query::parser::QueryStatement;
use session::context::QueryContextRef;
use snafu::{ensure, OptionExt, ResultExt};
use sql::ast::Value as SqlValue;
use sql::statements::insert::Insert;
use sql::statements::statement::Statement;
use sql::statements::{self};
use table::engine::TableReference;
use table::requests::*;

use crate::error::{
    CatalogSnafu, ColumnDefaultValueSnafu, ColumnNoneDefaultValueSnafu, ColumnNotFoundSnafu,
    ColumnValuesNumberMismatchSnafu, ExecuteSqlSnafu, InsertSnafu, MissingInsertBodySnafu,
    ParseSqlSnafu, ParseSqlValueSnafu, Result, TableNotFoundSnafu,
};
use crate::sql::{table_idents_to_full_name, SqlHandler, SqlRequest};

const DEFAULT_PLACEHOLDER_VALUE: &str = "default";

impl SqlHandler {
    pub(crate) async fn insert(&self, req: InsertRequest) -> Result<Output> {
        // FIXME(dennis): table_ref is used in InsertSnafu and the req is consumed
        // in `insert`, so we have to clone catalog_name etc.
        let table_ref = TableReference {
            catalog: &req.catalog_name.to_string(),
            schema: &req.schema_name.to_string(),
            table: &req.table_name.to_string(),
        };

        let table = self.get_table(&table_ref)?;

        let affected_rows = table.insert(req).await.with_context(|_| InsertSnafu {
            table_name: table_ref.to_string(),
        })?;

        Ok(Output::AffectedRows(affected_rows))
    }

    fn build_columns_from_values<'a>(
        table_ref: &'a TableReference<'a>,
        schema: &'a SchemaRef,
        values: Vec<Vec<SqlValue>>,
        columns: &Vec<&String>,
        columns_builders: &mut Vec<(&'a ColumnSchema, Box<dyn MutableVector>)>,
    ) -> Result<()> {
        let columns_num = Self::columns_num(schema, columns);
        let rows_num = values.len();

        // Initialize vectors
        if columns.is_empty() {
            for column_schema in schema.column_schemas() {
                let data_type = &column_schema.data_type;
                columns_builders.push((column_schema, data_type.create_mutable_vector(rows_num)));
            }
        } else {
            for column_name in columns {
                let column_schema =
                    schema.column_schema_by_name(column_name).with_context(|| {
                        ColumnNotFoundSnafu {
                            table_name: table_ref.table,
                            column_name: column_name.to_string(),
                        }
                    })?;
                let data_type = &column_schema.data_type;
                columns_builders.push((column_schema, data_type.create_mutable_vector(rows_num)));
            }
        }

        // Convert rows into columns
        for row in values {
            ensure!(
                row.len() == columns_num,
                ColumnValuesNumberMismatchSnafu {
                    columns: columns_num,
                    values: row.len(),
                }
            );

            for (sql_val, (column_schema, builder)) in row.iter().zip(columns_builders.iter_mut()) {
                add_row_to_vector(column_schema, sql_val, builder)?;
            }
        }

        Ok(())
    }

    // FIXME(dennis): move it to frontend when refactor is done.
    async fn build_columns_from_stmt<'a>(
        &self,
        table_ref: &'a TableReference<'a>,
        schema: &'a SchemaRef,
        stmt: &'a Insert,
        columns: &Vec<&String>,
        columns_builders: &mut Vec<(&'a ColumnSchema, Box<dyn MutableVector>)>,
        query_ctx: QueryContextRef,
    ) -> Result<()> {
        if stmt.is_insert_select() {
            let query = stmt
                .query_body()
                .context(ParseSqlValueSnafu)?
                .context(MissingInsertBodySnafu)?;

            let logical_plan = self
                .query_engine
                .statement_to_plan(
                    QueryStatement::Sql(Statement::Query(Box::new(query))),
                    query_ctx,
                )
                .context(ExecuteSqlSnafu)?;

            let output = self
                .query_engine
                .execute(&logical_plan)
                .await
                .context(ExecuteSqlSnafu)?;

            // TODO(dennis): streaming insert to avoid too much memroy consumption.
            let _batches = match output {
                Output::Stream(s) => RecordBatches::try_collect(s).await.unwrap(),
                Output::RecordBatches(bs) => bs,
                _ => unreachable!(),
            };

            Ok(())
        } else {
            let values = stmt
                .values_body()
                .context(ParseSqlValueSnafu)?
                .context(MissingInsertBodySnafu)?;

            Self::build_columns_from_values(table_ref, schema, values, columns, columns_builders)
        }
    }

    fn columns_num(schema: &SchemaRef, columns: &Vec<&String>) -> usize {
        if columns.is_empty() {
            schema.column_schemas().len()
        } else {
            columns.len()
        }
    }

    pub(crate) async fn insert_to_request(
        &self,
        catalog_manager: CatalogManagerRef,
        stmt: Insert,
        query_ctx: QueryContextRef,
    ) -> Result<SqlRequest> {
        let (catalog_name, schema_name, table_name) =
            table_idents_to_full_name(stmt.table_name(), query_ctx.clone())?;
        let table_ref = TableReference::full(&catalog_name, &schema_name, &table_name);

        let columns = stmt.columns();

        let table = catalog_manager
            .table(&catalog_name, &schema_name, &table_name)
            .context(CatalogSnafu)?
            .context(TableNotFoundSnafu {
                table_name: table_ref.table,
            })?;
        let schema = table.schema();

        let mut columns_builders: Vec<(&ColumnSchema, Box<dyn MutableVector>)> =
            Vec::with_capacity(Self::columns_num(&schema, &columns));
        self.build_columns_from_stmt(
            &table_ref,
            &schema,
            &stmt,
            &columns,
            &mut columns_builders,
            query_ctx,
        )
        .await?;

        Ok(SqlRequest::Insert(InsertRequest {
            catalog_name: table_ref.catalog.to_string(),
            schema_name: table_ref.schema.to_string(),
            table_name: table_ref.table.to_string(),
            columns_values: columns_builders
                .into_iter()
                .map(|(cs, mut b)| (cs.name.to_string(), b.to_vector()))
                .collect(),
            region_number: 0,
        }))
    }
}

fn add_row_to_vector(
    column_schema: &ColumnSchema,
    sql_val: &SqlValue,
    builder: &mut Box<dyn MutableVector>,
) -> Result<()> {
    let value = if replace_default(sql_val) {
        column_schema
            .create_default()
            .context(ColumnDefaultValueSnafu {
                column: column_schema.name.to_string(),
            })?
            .context(ColumnNoneDefaultValueSnafu {
                column: column_schema.name.to_string(),
            })?
    } else {
        statements::sql_value_to_value(&column_schema.name, &column_schema.data_type, sql_val)
            .context(ParseSqlSnafu)?
    };
    builder.push_value_ref(value.as_value_ref()).unwrap();
    Ok(())
}

fn replace_default(sql_val: &SqlValue) -> bool {
    matches!(sql_val, SqlValue::Placeholder(s) if s.to_lowercase() == DEFAULT_PLACEHOLDER_VALUE)
}
