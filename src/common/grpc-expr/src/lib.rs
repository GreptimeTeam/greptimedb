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

mod alter;
pub mod delete;
pub mod error;
pub mod insert;

use std::collections::HashSet;

pub use alter::{alter_expr_to_request, create_expr_to_request, create_table_schema};
use api::v1::{Column, ColumnDef, ColumnSchema, CreateTableExpr, SemanticType};
use error::Result;
pub use insert::{build_create_expr_from_insertion, column_to_vector, find_new_columns};
use snafu::{ensure, OptionExt};
use table::metadata::TableId;

use crate::error::{DuplicatedTimestampColumnSnafu, MissingTimestampColumnSnafu};

pub struct ColumnExpr {
    pub column_name: String,
    pub datatype: i32,
    pub semantic_type: i32,
}

impl ColumnExpr {
    #[inline]
    pub fn from_columns(columns: &[Column]) -> Vec<Self> {
        columns.iter().map(Self::from).collect()
    }

    #[inline]
    pub fn from_column_schemas(schemas: &[ColumnSchema]) -> Vec<Self> {
        schemas.iter().map(Self::from).collect()
    }
}

impl From<&Column> for ColumnExpr {
    fn from(column: &Column) -> Self {
        Self {
            column_name: column.column_name.clone(),
            datatype: column.datatype,
            semantic_type: column.semantic_type,
        }
    }
}

impl From<&ColumnSchema> for ColumnExpr {
    fn from(schema: &ColumnSchema) -> Self {
        Self {
            column_name: schema.column_name.clone(),
            datatype: schema.datatype,
            semantic_type: schema.semantic_type,
        }
    }
}

pub fn build_create_table_expr(
    catalog_name: &str,
    schema_name: &str,
    table_id: Option<TableId>,
    table_name: &str,
    column_exprs: Vec<ColumnExpr>,
    engine: &str,
    desc: &str,
) -> Result<CreateTableExpr> {
    let mut new_columns = HashSet::with_capacity(column_exprs.len());
    let mut column_defs = Vec::with_capacity(column_exprs.len());
    let mut primary_keys = Vec::default();
    let mut time_index = None;

    for ColumnExpr {
        column_name,
        datatype,
        semantic_type,
    } in column_exprs
    {
        if new_columns.insert(column_name.clone()) {
            let mut is_nullable = true;
            match semantic_type {
                v if v == SemanticType::Tag as i32 => primary_keys.push(column_name.clone()),
                v if v == SemanticType::Timestamp as i32 => {
                    ensure!(
                        time_index.is_none(),
                        DuplicatedTimestampColumnSnafu {
                            exists: time_index.unwrap(),
                            duplicated: &column_name,
                        }
                    );
                    time_index = Some(column_name.clone());
                    // Timestamp column must not be null.
                    is_nullable = false;
                }
                _ => {}
            }

            let column_def = ColumnDef {
                name: column_name,
                datatype,
                is_nullable,
                default_constraint: vec![],
            };
            column_defs.push(column_def);
        }
    }

    let time_index = time_index.context(MissingTimestampColumnSnafu {
        msg: format!("table is {}", table_name),
    })?;

    let expr = CreateTableExpr {
        catalog_name: catalog_name.to_string(),
        schema_name: schema_name.to_string(),
        table_name: table_name.to_string(),
        desc: desc.to_string(),
        column_defs,
        time_index,
        primary_keys,
        create_if_not_exists: true,
        table_options: Default::default(),
        table_id: table_id.map(|id| api::v1::TableId { id }),
        region_numbers: vec![0], // TODO:(hl): region number should be allocated by frontend
        engine: engine.to_string(),
    };

    Ok(expr)
}
