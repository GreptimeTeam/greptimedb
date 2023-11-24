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

use api::helper::{value_to_grpc_value, ColumnDataTypeWrapper};
use api::v1::region::InsertRequests as RegionInsertRequests;
use api::v1::{ColumnSchema as GrpcColumnSchema, Row, Rows, Value as GrpcValue};
use catalog::CatalogManager;
use datatypes::schema::{ColumnSchema, SchemaRef};
use partition::manager::PartitionRuleManager;
use session::context::QueryContext;
use snafu::{ensure, OptionExt, ResultExt};
use sql::statements;
use sql::statements::insert::Insert;
use sqlparser::ast::{ObjectName, Value as SqlValue};
use table::TableRef;

use super::semantic_type;
use crate::error::{
    CatalogSnafu, ColumnDataTypeSnafu, ColumnDefaultValueSnafu, ColumnNoneDefaultValueSnafu,
    ColumnNotFoundSnafu, InvalidSqlSnafu, MissingInsertBodySnafu, ParseSqlSnafu, Result,
    TableNotFoundSnafu,
};
use crate::req_convert::common::partitioner::Partitioner;

const DEFAULT_PLACEHOLDER_VALUE: &str = "default";

pub struct StatementToRegion<'a> {
    catalog_manager: &'a dyn CatalogManager,
    partition_manager: &'a PartitionRuleManager,
    ctx: &'a QueryContext,
}

impl<'a> StatementToRegion<'a> {
    pub fn new(
        catalog_manager: &'a dyn CatalogManager,
        partition_manager: &'a PartitionRuleManager,
        ctx: &'a QueryContext,
    ) -> Self {
        Self {
            catalog_manager,
            partition_manager,
            ctx,
        }
    }

    pub async fn convert(&self, stmt: &Insert) -> Result<RegionInsertRequests> {
        let (catalog, schema, table_name) = self.get_full_name(stmt.table_name())?;
        let table = self.get_table(&catalog, &schema, &table_name).await?;
        let table_schema = table.schema();
        let table_info = table.table_info();

        let column_names = column_names(stmt, &table_schema);
        let column_count = column_names.len();

        let sql_rows = stmt.values_body().context(MissingInsertBodySnafu)?;
        let row_count = sql_rows.len();

        sql_rows.iter().try_for_each(|r| {
            ensure!(
                r.len() == column_count,
                InvalidSqlSnafu {
                    err_msg: format!(
                        "column count mismatch, columns: {}, values: {}",
                        column_count,
                        r.len()
                    )
                }
            );
            Ok(())
        })?;

        let mut schema = Vec::with_capacity(column_count);
        let mut rows = vec![
            Row {
                values: Vec::with_capacity(column_count)
            };
            row_count
        ];

        for (i, column_name) in column_names.into_iter().enumerate() {
            let column_schema = table_schema
                .column_schema_by_name(column_name)
                .with_context(|| ColumnNotFoundSnafu {
                    msg: format!("Column {} not found in table {}", column_name, &table_name),
                })?;

            let (datatype, datatype_extension) =
                ColumnDataTypeWrapper::try_from(column_schema.data_type.clone())
                    .context(ColumnDataTypeSnafu)?
                    .to_parts();
            let semantic_type = semantic_type(&table_info, column_name)?;

            let grpc_column_schema = GrpcColumnSchema {
                column_name: column_name.clone(),
                datatype: datatype.into(),
                semantic_type: semantic_type.into(),
                datatype_extension,
            };
            schema.push(grpc_column_schema);

            for (sql_row, grpc_row) in sql_rows.iter().zip(rows.iter_mut()) {
                let value = sql_value_to_grpc_value(column_schema, &sql_row[i])?;
                grpc_row.values.push(value);
            }
        }

        let requests = Partitioner::new(self.partition_manager)
            .partition_insert_requests(table_info.table_id(), Rows { schema, rows })
            .await?;
        Ok(RegionInsertRequests { requests })
    }

    async fn get_table(&self, catalog: &str, schema: &str, table: &str) -> Result<TableRef> {
        self.catalog_manager
            .table(catalog, schema, table)
            .await
            .context(CatalogSnafu)?
            .with_context(|| TableNotFoundSnafu {
                table_name: common_catalog::format_full_table_name(catalog, schema, table),
            })
    }

    fn get_full_name(&self, obj_name: &ObjectName) -> Result<(String, String, String)> {
        match &obj_name.0[..] {
            [table] => Ok((
                self.ctx.current_catalog().to_owned(),
                self.ctx.current_schema().to_owned(),
                table.value.clone(),
            )),
            [schema, table] => Ok((
                self.ctx.current_catalog().to_owned(),
                schema.value.clone(),
                table.value.clone(),
            )),
            [catalog, schema, table] => Ok((
                catalog.value.clone(),
                schema.value.clone(),
                table.value.clone(),
            )),
            _ => InvalidSqlSnafu {
                err_msg: format!(
                    "expect table name to be <catalog>.<schema>.<table>, <schema>.<table> or <table>, actual: {obj_name}",
                ),
            }.fail(),
        }
    }
}

fn column_names<'a>(stmt: &'a Insert, table_schema: &'a SchemaRef) -> Vec<&'a String> {
    if !stmt.columns().is_empty() {
        stmt.columns()
    } else {
        table_schema
            .column_schemas()
            .iter()
            .map(|column| &column.name)
            .collect()
    }
}

fn sql_value_to_grpc_value(column_schema: &ColumnSchema, sql_val: &SqlValue) -> Result<GrpcValue> {
    let column = &column_schema.name;
    let value = if replace_default(sql_val) {
        let default_value = column_schema
            .create_default()
            .context(ColumnDefaultValueSnafu {
                column: column.clone(),
            })?;

        default_value.context(ColumnNoneDefaultValueSnafu {
            column: column.clone(),
        })?
    } else {
        statements::sql_value_to_value(column, &column_schema.data_type, sql_val)
            .context(ParseSqlSnafu)?
    };

    let grpc_value = value_to_grpc_value(value);
    Ok(grpc_value)
}

fn replace_default(sql_val: &SqlValue) -> bool {
    matches!(sql_val, SqlValue::Placeholder(s) if s.to_lowercase() == DEFAULT_PLACEHOLDER_VALUE)
}
