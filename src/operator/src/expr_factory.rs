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
    AddColumn, AddColumns, AlterExpr, Column, ColumnDataType, ColumnDataTypeExtension,
    CreateTableExpr, DropColumn, DropColumns, RenameTable, SemanticType,
};
use common_error::ext::BoxedError;
use common_grpc_expr::util::ColumnExpr;
use datatypes::schema::{ColumnSchema, COMMENT_KEY};
use file_engine::FileOptions;
use query::sql::{
    check_file_to_table_schema_compatibility, file_column_schemas_to_table,
    infer_file_table_schema, prepare_file_table_files,
};
use session::context::QueryContextRef;
use snafu::{ensure, ResultExt};
use sql::ast::{ColumnDef, ColumnOption, TableConstraint};
use sql::statements::alter::{AlterTable, AlterTableOperation};
use sql::statements::create::{CreateExternalTable, CreateTable, TIME_INDEX};
use sql::statements::{column_def_to_schema, sql_column_def_to_grpc_column_def};
use sql::util::to_lowercase_options_map;
use table::engine::TableReference;
use table::requests::{TableOptions, FILE_TABLE_META_KEY};

use crate::error::{
    BuildCreateExprOnInsertionSnafu, ColumnDataTypeSnafu, ConvertColumnDefaultConstraintSnafu,
    EncodeJsonSnafu, ExternalSnafu, IllegalPrimaryKeysDefSnafu, InferFileTableSchemaSnafu,
    InvalidSqlSnafu, NotSupportedSnafu, ParseSqlSnafu, PrepareFileTableSnafu, Result,
    SchemaIncompatibleSnafu, UnrecognizedTableOptionSnafu,
};
use crate::table::table_idents_to_full_name;

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

// When the `CREATE EXTERNAL TABLE` statement is in expanded form, like
// ```sql
// CREATE EXTERNAL TABLE city (
//   host string,
//   ts timestamp,
//   cpu float64,
//   memory float64,
//   TIME INDEX (ts),
//   PRIMARY KEY(host)
// ) WITH (location='/var/data/city.csv', format='csv');
// ```
// The user needs to specify the TIME INDEX column. If there is no suitable
// column in the file to use as TIME INDEX, an additional placeholder column
// needs to be created as the TIME INDEX, and a `DEFAULT <value>` constraint
// should be added.
//
//
// When the `CREATE EXTERNAL TABLE` statement is in inferred form, like
// ```sql
// CREATE EXTERNAL TABLE IF NOT EXISTS city WITH (location='/var/data/city.csv',format='csv');
// ```
// 1. If the TIME INDEX column can be inferred from metadata, use that column
//    as the TIME INDEX. Otherwise,
// 2. If a column named `greptime_timestamp` exists (with the requirement that
//    the column is with type TIMESTAMP, otherwise an error is thrown), use
//    that column as the TIME INDEX. Otherwise,
// 3. Automatically create the `greptime_timestamp` column and add a `DEFAULT 0`
//    constraint.
pub(crate) async fn create_external_expr(
    create: CreateExternalTable,
    query_ctx: QueryContextRef,
) -> Result<CreateTableExpr> {
    let (catalog_name, schema_name, table_name) =
        table_idents_to_full_name(&create.name, query_ctx)
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;

    let mut table_options = create.options;

    let (object_store, files) = prepare_file_table_files(&table_options.map)
        .await
        .context(PrepareFileTableSnafu)?;

    let file_column_schemas = infer_file_table_schema(&object_store, &files, &table_options.map)
        .await
        .context(InferFileTableSchemaSnafu)?
        .column_schemas;

    let (time_index, primary_keys, table_column_schemas) = if !create.columns.is_empty() {
        // expanded form
        let time_index = find_time_index(&create.constraints)?;
        let primary_keys = find_primary_keys(&create.columns, &create.constraints)?;
        let column_schemas = columns_to_column_schemas(&create.columns, &time_index)?;
        (time_index, primary_keys, column_schemas)
    } else {
        // inferred form
        let (column_schemas, time_index) = file_column_schemas_to_table(&file_column_schemas);
        let primary_keys = vec![];
        (time_index, primary_keys, column_schemas)
    };

    check_file_to_table_schema_compatibility(&file_column_schemas, &table_column_schemas)
        .context(SchemaIncompatibleSnafu)?;

    let meta = FileOptions {
        files,
        file_column_schemas,
    };
    table_options.insert(
        FILE_TABLE_META_KEY.to_string(),
        serde_json::to_string(&meta).context(EncodeJsonSnafu)?,
    );

    let column_defs = column_schemas_to_defs(table_column_schemas, &primary_keys)?;
    let expr = CreateTableExpr {
        catalog_name,
        schema_name,
        table_name,
        desc: "".to_string(),
        column_defs,
        time_index,
        primary_keys,
        create_if_not_exists: create.if_not_exists,
        table_options: table_options.map,
        table_id: None,
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

    let primary_keys = find_primary_keys(&create.columns, &create.constraints)?;

    let expr = CreateTableExpr {
        catalog_name,
        schema_name,
        table_name,
        desc: "".to_string(),
        column_defs: columns_to_expr(&create.columns, &time_index, &primary_keys)?,
        time_index,
        primary_keys,
        create_if_not_exists: create.if_not_exists,
        table_options,
        table_id: None,
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

fn columns_to_expr(
    column_defs: &[ColumnDef],
    time_index: &str,
    primary_keys: &[String],
) -> Result<Vec<api::v1::ColumnDef>> {
    let column_schemas = columns_to_column_schemas(column_defs, time_index)?;
    column_schemas_to_defs(column_schemas, primary_keys)
}

fn columns_to_column_schemas(
    column_defs: &[ColumnDef],
    time_index: &str,
) -> Result<Vec<ColumnSchema>> {
    column_defs
        .iter()
        .map(|c| column_def_to_schema(c, c.name.to_string() == time_index).context(ParseSqlSnafu))
        .collect::<Result<Vec<ColumnSchema>>>()
}

pub fn column_schemas_to_defs(
    column_schemas: Vec<ColumnSchema>,
    primary_keys: &[String],
) -> Result<Vec<api::v1::ColumnDef>> {
    let column_datatypes: Vec<(ColumnDataType, Option<ColumnDataTypeExtension>)> = column_schemas
        .iter()
        .map(|c| {
            ColumnDataTypeWrapper::try_from(c.data_type.clone())
                .map(|w| w.to_parts())
                .context(ColumnDataTypeSnafu)
        })
        .collect::<Result<Vec<_>>>()?;

    column_schemas
        .iter()
        .zip(column_datatypes)
        .map(|(schema, datatype)| {
            let semantic_type = if schema.is_time_index() {
                SemanticType::Timestamp
            } else if primary_keys.contains(&schema.name) {
                SemanticType::Tag
            } else {
                SemanticType::Field
            } as i32;
            let comment = schema
                .metadata()
                .get(COMMENT_KEY)
                .cloned()
                .unwrap_or_default();

            Ok(api::v1::ColumnDef {
                name: schema.name.clone(),
                data_type: datatype.0 as i32,
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
                semantic_type,
                comment,
                datatype_extension: datatype.1,
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
