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

use std::collections::HashSet;

use api::v1::{
    AddColumn, AddColumns, Column, ColumnDataTypeExtension, ColumnDef, ColumnSchema,
    CreateTableExpr, SemanticType,
};
use datatypes::schema::Schema;
use snafu::{ensure, OptionExt};
use table::engine::TableReference;
use table::metadata::TableId;

use crate::error::{
    DuplicatedColumnNameSnafu, DuplicatedTimestampColumnSnafu, MissingTimestampColumnSnafu, Result,
};

pub struct ColumnExpr<'a> {
    pub column_name: &'a str,
    pub datatype: i32,
    pub semantic_type: i32,
    pub datatype_extension: &'a Option<ColumnDataTypeExtension>,
}

impl<'a> ColumnExpr<'a> {
    #[inline]
    pub fn from_columns(columns: &'a [Column]) -> Vec<Self> {
        columns.iter().map(Self::from).collect()
    }

    #[inline]
    pub fn from_column_schemas(schemas: &'a [ColumnSchema]) -> Vec<Self> {
        schemas.iter().map(Self::from).collect()
    }
}

impl<'a> From<&'a Column> for ColumnExpr<'a> {
    fn from(column: &'a Column) -> Self {
        Self {
            column_name: &column.column_name,
            datatype: column.datatype,
            semantic_type: column.semantic_type,
            datatype_extension: &column.datatype_extension,
        }
    }
}

impl<'a> From<&'a ColumnSchema> for ColumnExpr<'a> {
    fn from(schema: &'a ColumnSchema) -> Self {
        Self {
            column_name: &schema.column_name,
            datatype: schema.datatype,
            semantic_type: schema.semantic_type,
            datatype_extension: &schema.datatype_extension,
        }
    }
}

pub fn build_create_table_expr(
    table_id: Option<TableId>,
    table_name: &TableReference<'_>,
    column_exprs: Vec<ColumnExpr>,
    engine: &str,
    desc: &str,
) -> Result<CreateTableExpr> {
    // Check for duplicate names. If found, raise an error.
    //
    // The introduction of hashset incurs additional memory overhead
    // but achieves a time complexity of O(1).
    //
    // The separate iteration over `column_exprs` is because the CPU prefers
    // smaller loops, and avoid cloning String.
    let mut distinct_names = HashSet::with_capacity(column_exprs.len());
    for ColumnExpr { column_name, .. } in &column_exprs {
        ensure!(
            distinct_names.insert(*column_name),
            DuplicatedColumnNameSnafu { name: *column_name }
        );
    }

    let mut column_defs = Vec::with_capacity(column_exprs.len());
    let mut primary_keys = Vec::default();
    let mut time_index = None;

    for ColumnExpr {
        column_name,
        datatype,
        semantic_type,
        datatype_extension,
    } in column_exprs
    {
        let mut is_nullable = true;
        match semantic_type {
            v if v == SemanticType::Tag as i32 => primary_keys.push(column_name.to_string()),
            v if v == SemanticType::Timestamp as i32 => {
                ensure!(
                    time_index.is_none(),
                    DuplicatedTimestampColumnSnafu {
                        exists: time_index.unwrap(),
                        duplicated: column_name,
                    }
                );
                time_index = Some(column_name.to_string());
                // Timestamp column must not be null.
                is_nullable = false;
            }
            _ => {}
        }

        let column_def = ColumnDef {
            name: column_name.to_string(),
            data_type: datatype,
            is_nullable,
            default_constraint: vec![],
            semantic_type,
            comment: String::new(),
            datatype_extension: datatype_extension.clone(),
        };
        column_defs.push(column_def);
    }

    let time_index = time_index.context(MissingTimestampColumnSnafu {
        msg: format!("table is {}", table_name.table),
    })?;

    let expr = CreateTableExpr {
        catalog_name: table_name.catalog.to_string(),
        schema_name: table_name.schema.to_string(),
        table_name: table_name.table.to_string(),
        desc: desc.to_string(),
        column_defs,
        time_index,
        primary_keys,
        create_if_not_exists: true,
        table_options: Default::default(),
        table_id: table_id.map(|id| api::v1::TableId { id }),
        engine: engine.to_string(),
    };

    Ok(expr)
}

pub fn extract_new_columns(
    schema: &Schema,
    column_exprs: Vec<ColumnExpr>,
) -> Result<Option<AddColumns>> {
    let columns_to_add = column_exprs
        .into_iter()
        .filter(|expr| schema.column_schema_by_name(expr.column_name).is_none())
        .map(|expr| {
            let column_def = Some(ColumnDef {
                name: expr.column_name.to_string(),
                data_type: expr.datatype,
                is_nullable: true,
                default_constraint: vec![],
                semantic_type: expr.semantic_type,
                comment: String::new(),
                datatype_extension: expr.datatype_extension.clone(),
            });
            AddColumn {
                column_def,
                location: None,
            }
        })
        .collect::<Vec<_>>();

    if columns_to_add.is_empty() {
        Ok(None)
    } else {
        let mut distinct_names = HashSet::with_capacity(columns_to_add.len());
        for add_column in &columns_to_add {
            let name = add_column.column_def.as_ref().unwrap().name.as_str();
            ensure!(
                distinct_names.insert(name),
                DuplicatedColumnNameSnafu { name }
            );
        }

        Ok(Some(AddColumns {
            add_columns: columns_to_add,
        }))
    }
}
