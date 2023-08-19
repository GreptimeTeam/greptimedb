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

use api::helper::ColumnDataTypeWrapper;
use api::v1::alter_expr::Kind;
use api::v1::{
    AddColumn, AddColumns, AlterExpr, Column, ColumnDataType, CreateTableExpr, DropColumn,
    DropColumns, RenameTable,
};
use common_error::ext::BoxedError;
use common_grpc_expr::util::ColumnExpr;
use datanode::instance::sql::table_idents_to_full_name;
use datatypes::schema::ColumnSchema;
use file_table_engine::table::immutable::ImmutableFileTableOptions;
use query::sql::prepare_immutable_file_table_files_and_schema;
use session::context::QueryContextRef;
use snafu::{ensure, ResultExt};
use sql::ast::{ColumnDef, ColumnOption, TableConstraint};
use sql::statements::alter::{AlterTable, AlterTableOperation};
use sql::statements::create::{CreateExternalTable, CreateTable, TIME_INDEX};
use sql::statements::{column_def_to_schema, sql_column_def_to_grpc_column_def};
use sql::util::to_lowercase_options_map;
use table::engine::TableReference;
use table::requests::{TableOptions, IMMUTABLE_TABLE_META_KEY};

use crate::error::{
    BuildCreateExprOnInsertionSnafu, ColumnDataTypeSnafu, ConvertColumnDefaultConstraintSnafu,
    EncodeJsonSnafu, ExternalSnafu, IllegalPrimaryKeysDefSnafu, InvalidSqlSnafu, NotSupportedSnafu,
    ParseSqlSnafu, PrepareImmutableTableSnafu, Result, UnrecognizedTableOptionSnafu,
};

#[derive(Debug, Copy, Clone)]
pub struct CreateExprFactory;

impl CreateExprFactory {
    pub fn create_table_expr_by_columns(
        &self,
        table_name: &TableReference<'_>,
        columns: &[Column],
        engine: &str,
    ) -> Result<CreateTableExpr> {
        let column_exprs = ColumnExpr::from_columns(columns);
        let create_expr = common_grpc_expr::util::build_create_table_expr(
            None,
            table_name,
            column_exprs,
            engine,
            "Created on insertion",
        )
        .context(BuildCreateExprOnInsertionSnafu)?;

        Ok(create_expr)
    }

    pub fn create_table_expr_by_column_schemas(
        &self,
        table_name: &TableReference<'_>,
        column_schemas: &[api::v1::ColumnSchema],
        engine: &str,
    ) -> Result<CreateTableExpr> {
        let column_exprs = ColumnExpr::from_column_schemas(column_schemas);
        let create_expr = common_grpc_expr::util::build_create_table_expr(
            None,
            table_name,
            column_exprs,
            engine,
            "Created on insertion",
        )
        .context(BuildCreateExprOnInsertionSnafu)?;

        Ok(create_expr)
    }
}

pub(crate) async fn create_external_expr(
    create: CreateExternalTable,
    query_ctx: QueryContextRef,
) -> Result<CreateTableExpr> {
    let (catalog_name, schema_name, table_name) =
        table_idents_to_full_name(&create.name, query_ctx)
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;

    let mut options = create.options;

    let (files, schema) = prepare_immutable_file_table_files_and_schema(&options, &create.columns)
        .await
        .context(PrepareImmutableTableSnafu)?;

    let meta = ImmutableFileTableOptions { files };
    let _ = options.insert(
        IMMUTABLE_TABLE_META_KEY.to_string(),
        serde_json::to_string(&meta).context(EncodeJsonSnafu)?,
    );

    let expr = CreateTableExpr {
        catalog_name,
        schema_name,
        table_name,
        desc: "".to_string(),
        column_defs: column_schemas_to_defs(schema.column_schemas)?,
        time_index: "".to_string(),
        primary_keys: vec![],
        create_if_not_exists: create.if_not_exists,
        table_options: options,
        table_id: None,
        region_numbers: vec![],
        engine: create.engine.to_string(),
    };
    Ok(expr)
}

/// Convert `CreateTable` statement to `CreateExpr` gRPC request.
pub fn create_to_expr(create: &CreateTable, query_ctx: QueryContextRef) -> Result<CreateTableExpr> {
    let (catalog_name, schema_name, table_name) =
        table_idents_to_full_name(&create.name, query_ctx)
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;

    let time_index = find_time_index(&create.constraints)?;
    let table_options = HashMap::from(
        &TableOptions::try_from(&to_lowercase_options_map(&create.options))
            .context(UnrecognizedTableOptionSnafu)?,
    );
    let expr = CreateTableExpr {
        catalog_name,
        schema_name,
        table_name,
        desc: "".to_string(),
        column_defs: columns_to_expr(&create.columns, &time_index)?,
        time_index,
        primary_keys: find_primary_keys(&create.columns, &create.constraints)?,
        create_if_not_exists: create.if_not_exists,
        table_options,
        table_id: None,
        region_numbers: vec![],
        engine: create.engine.to_string(),
    };
    Ok(expr)
}

fn find_primary_keys(
    columns: &[ColumnDef],
    constraints: &[TableConstraint],
) -> Result<Vec<String>> {
    let columns_pk = columns
        .iter()
        .filter_map(|x| {
            if x.options
                .iter()
                .any(|o| matches!(o.option, ColumnOption::Unique { is_primary: true }))
            {
                Some(x.name.value.clone())
            } else {
                None
            }
        })
        .collect::<Vec<String>>();

    ensure!(
        columns_pk.len() <= 1,
        IllegalPrimaryKeysDefSnafu {
            msg: "not allowed to inline multiple primary keys in columns options"
        }
    );

    let constraints_pk = constraints
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

    ensure!(
        columns_pk.is_empty() || constraints_pk.is_empty(),
        IllegalPrimaryKeysDefSnafu {
            msg: "found definitions of primary keys in multiple places"
        }
    );

    let mut primary_keys = Vec::with_capacity(columns_pk.len() + constraints_pk.len());
    primary_keys.extend(columns_pk);
    primary_keys.extend(constraints_pk);
    Ok(primary_keys)
}

pub fn find_time_index(constraints: &[TableConstraint]) -> Result<String> {
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

fn columns_to_expr(column_defs: &[ColumnDef], time_index: &str) -> Result<Vec<api::v1::ColumnDef>> {
    let column_schemas = column_defs
        .iter()
        .map(|c| column_def_to_schema(c, c.name.to_string() == time_index).context(ParseSqlSnafu))
        .collect::<Result<Vec<ColumnSchema>>>()?;
    column_schemas_to_defs(column_schemas)
}

pub(crate) fn column_schemas_to_defs(
    column_schemas: Vec<ColumnSchema>,
) -> Result<Vec<api::v1::ColumnDef>> {
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
        .zip(column_datatypes)
        .map(|(schema, datatype)| {
            Ok(api::v1::ColumnDef {
                name: schema.name.clone(),
                datatype: datatype as i32,
                is_nullable: schema.is_nullable(),
                default_constraint: match schema.default_constraint() {
                    None => vec![],
                    Some(v) => {
                        v.clone()
                            .try_into()
                            .context(ConvertColumnDefaultConstraintSnafu {
                                column_name: &schema.name,
                            })?
                    }
                },
            })
        })
        .collect()
}

pub(crate) fn to_alter_expr(
    alter_table: AlterTable,
    query_ctx: QueryContextRef,
) -> Result<AlterExpr> {
    let (catalog_name, schema_name, table_name) =
        table_idents_to_full_name(alter_table.table_name(), query_ctx)
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;

    let kind = match alter_table.alter_operation() {
        AlterTableOperation::AddConstraint(_) => {
            return NotSupportedSnafu {
                feat: "ADD CONSTRAINT",
            }
            .fail();
        }
        AlterTableOperation::AddColumn {
            column_def,
            location,
        } => Kind::AddColumns(AddColumns {
            add_columns: vec![AddColumn {
                column_def: Some(
                    sql_column_def_to_grpc_column_def(column_def)
                        .map_err(BoxedError::new)
                        .context(ExternalSnafu)?,
                ),
                is_key: false,
                location: location.as_ref().map(From::from),
            }],
        }),
        AlterTableOperation::DropColumn { name } => Kind::DropColumns(DropColumns {
            drop_columns: vec![DropColumn {
                name: name.value.to_string(),
            }],
        }),
        AlterTableOperation::RenameTable { new_table_name } => Kind::RenameTable(RenameTable {
            new_table_name: new_table_name.to_string(),
        }),
    };

    Ok(AlterExpr {
        catalog_name,
        schema_name,
        table_name,
        kind: Some(kind),
        ..Default::default()
    })
}

#[cfg(test)]
mod tests {
    use session::context::QueryContext;
    use sql::dialect::GreptimeDbDialect;
    use sql::parser::ParserContext;
    use sql::statements::statement::Statement;

    use super::*;

    #[test]
    fn test_create_to_expr() {
        let sql = "CREATE TABLE monitor (host STRING,ts TIMESTAMP,TIME INDEX (ts),PRIMARY KEY(host)) ENGINE=mito WITH(regions=1, ttl='3days', write_buffer_size='1024KB');";
        let stmt = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {})
            .unwrap()
            .pop()
            .unwrap();

        let Statement::CreateTable(create_table) = stmt else {
            unreachable!()
        };
        let expr = create_to_expr(&create_table, QueryContext::arc()).unwrap();
        assert_eq!("3days", expr.table_options.get("ttl").unwrap());
        assert_eq!(
            "1.0MiB",
            expr.table_options.get("write_buffer_size").unwrap()
        );
    }
}
