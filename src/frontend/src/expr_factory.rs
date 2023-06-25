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
use std::sync::Arc;

use api::helper::ColumnDataTypeWrapper;
use api::v1::alter_expr::Kind;
use api::v1::{
    AddColumn, AddColumns, AlterExpr, Column, ColumnDataType, CreateTableExpr, DropColumn,
    DropColumns, RenameTable,
};
use common_error::prelude::BoxedError;
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
use table::requests::{TableOptions, IMMUTABLE_TABLE_META_KEY};

use crate::error::{
    self, BuildCreateExprOnInsertionSnafu, ColumnDataTypeSnafu,
    ConvertColumnDefaultConstraintSnafu, ExternalSnafu, IllegalPrimaryKeysDefSnafu,
    InvalidSqlSnafu, ParseSqlSnafu, Result,
};

pub type CreateExprFactoryRef = Arc<dyn CreateExprFactory + Send + Sync>;

#[async_trait::async_trait]
pub trait CreateExprFactory {
    async fn create_expr_by_columns(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        columns: &[Column],
        engine: &str,
    ) -> crate::error::Result<CreateTableExpr>;
}

#[derive(Debug)]
pub struct DefaultCreateExprFactory;

#[async_trait::async_trait]
impl CreateExprFactory for DefaultCreateExprFactory {
    async fn create_expr_by_columns(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        columns: &[Column],
        engine: &str,
    ) -> Result<CreateTableExpr> {
        let table_id = None;
        let create_expr = common_grpc_expr::build_create_expr_from_insertion(
            catalog_name,
            schema_name,
            table_id,
            table_name,
            columns,
            engine,
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
            .context(error::ExternalSnafu)?;

    let mut options = create.options;

    let (files, schema) = prepare_immutable_file_table_files_and_schema(&options, &create.columns)
        .await
        .context(error::PrepareImmutableTableSnafu)?;

    let meta = ImmutableFileTableOptions { files };
    let _ = options.insert(
        IMMUTABLE_TABLE_META_KEY.to_string(),
        serde_json::to_string(&meta).context(error::EncodeJsonSnafu)?,
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
            .context(error::ExternalSnafu)?;

    let time_index = find_time_index(&create.constraints)?;
    let table_options = HashMap::from(
        &TableOptions::try_from(&to_lowercase_options_map(&create.options))
            .context(error::UnrecognizedTableOptionSnafu)?,
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
        .zip(column_datatypes.into_iter())
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
            return error::NotSupportedSnafu {
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

        let Statement::CreateTable(create_table) = stmt else { unreachable!() };
        let expr = create_to_expr(&create_table, Arc::new(QueryContext::default())).unwrap();
        assert_eq!("3days", expr.table_options.get("ttl").unwrap());
        assert_eq!(
            "1.0MiB",
            expr.table_options.get("write_buffer_size").unwrap()
        );
    }
}
