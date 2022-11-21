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
use std::sync::Arc;

use api::helper::ColumnDataTypeWrapper;
use api::v1::{Column, ColumnDataType, CreateExpr};
use datatypes::schema::ColumnSchema;
use snafu::{ensure, ResultExt};
use sql::statements::create::{CreateTable, TIME_INDEX};
use sql::statements::{column_def_to_schema, table_idents_to_full_name};
use sqlparser::ast::{ColumnDef, TableConstraint};

use crate::error::{
    BuildCreateExprOnInsertionSnafu, ColumnDataTypeSnafu, ConvertColumnDefaultConstraintSnafu,
    InvalidSqlSnafu, ParseSqlSnafu, Result,
};

pub type CreateExprFactoryRef = Arc<dyn CreateExprFactory + Send + Sync>;

#[async_trait::async_trait]
pub trait CreateExprFactory {
    async fn create_expr_by_stmt(&self, stmt: &CreateTable) -> Result<CreateExpr>;

    async fn create_expr_by_columns(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        columns: &[Column],
    ) -> crate::error::Result<CreateExpr>;
}

#[derive(Debug)]
pub struct DefaultCreateExprFactory;

#[async_trait::async_trait]
impl CreateExprFactory for DefaultCreateExprFactory {
    async fn create_expr_by_stmt(&self, stmt: &CreateTable) -> Result<CreateExpr> {
        create_to_expr(None, vec![0], stmt)
    }

    async fn create_expr_by_columns(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        columns: &[Column],
    ) -> Result<CreateExpr> {
        let table_id = None;
        let create_expr = common_insert::build_create_expr_from_insertion(
            catalog_name,
            schema_name,
            table_id,
            table_name,
            columns,
        )
        .context(BuildCreateExprOnInsertionSnafu)?;

        Ok(create_expr)
    }
}

/// Convert `CreateTable` statement to `CreateExpr` gRPC request.
fn create_to_expr(
    table_id: Option<u32>,
    region_ids: Vec<u32>,
    create: &CreateTable,
) -> Result<CreateExpr> {
    let (catalog_name, schema_name, table_name) =
        table_idents_to_full_name(&create.name).context(ParseSqlSnafu)?;

    let time_index = find_time_index(&create.constraints)?;
    let expr = CreateExpr {
        catalog_name: Some(catalog_name),
        schema_name: Some(schema_name),
        table_name,
        desc: None,
        column_defs: columns_to_expr(&create.columns, &time_index)?,
        time_index,
        primary_keys: find_primary_keys(&create.constraints)?,
        create_if_not_exists: create.if_not_exists,
        // TODO(LFC): Fill in other table options.
        table_options: HashMap::from([("engine".to_string(), create.engine.clone())]),
        table_id,
        region_ids,
    };
    Ok(expr)
}

fn find_primary_keys(constraints: &[TableConstraint]) -> Result<Vec<String>> {
    let primary_keys = constraints
        .iter()
        .filter_map(|constraint| match constraint {
            TableConstraint::Unique {
                name: _,
                columns,
                is_primary: true,
            } => Some(columns.iter().map(|ident| ident.value.clone())),
            _ => None,
        })
        .flatten()
        .collect::<Vec<String>>();
    Ok(primary_keys)
}

pub fn find_time_index(constraints: &[TableConstraint]) -> crate::error::Result<String> {
    let time_index = constraints
        .iter()
        .filter_map(|constraint| match constraint {
            TableConstraint::Unique {
                name: Some(name),
                columns,
                is_primary: false,
            } => {
                if name.value == TIME_INDEX {
                    Some(columns.iter().map(|ident| &ident.value))
                } else {
                    None
                }
            }
            _ => None,
        })
        .flatten()
        .collect::<Vec<&String>>();
    ensure!(
        time_index.len() == 1,
        InvalidSqlSnafu {
            err_msg: "must have one and only one TimeIndex columns",
        }
    );
    Ok(time_index.first().unwrap().to_string())
}

fn columns_to_expr(
    column_defs: &[ColumnDef],
    time_index: &str,
) -> crate::error::Result<Vec<api::v1::ColumnDef>> {
    let column_schemas = column_defs
        .iter()
        .map(|c| column_def_to_schema(c, c.name.to_string() == time_index).context(ParseSqlSnafu))
        .collect::<Result<Vec<ColumnSchema>>>()?;

    let column_datatypes = column_schemas
        .iter()
        .map(|c| {
            ColumnDataTypeWrapper::try_from(c.data_type.clone())
                .map(|w| w.datatype())
                .context(ColumnDataTypeSnafu)
        })
        .collect::<Result<Vec<ColumnDataType>>>()?;

    column_schemas
        .iter()
        .zip(column_datatypes.into_iter())
        .map(|(schema, datatype)| {
            Ok(api::v1::ColumnDef {
                name: schema.name.clone(),
                datatype: datatype as i32,
                is_nullable: schema.is_nullable(),
                default_constraint: match schema.default_constraint() {
                    None => None,
                    Some(v) => Some(v.clone().try_into().context(
                        ConvertColumnDefaultConstraintSnafu {
                            column_name: &schema.name,
                        },
                    )?),
                },
            })
        })
        .collect()
}
