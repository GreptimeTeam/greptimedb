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

use std::collections::{HashMap, HashSet};

use api::helper::ColumnDataTypeWrapper;
use api::v1::alter_expr::Kind;
use api::v1::{
    AddColumn, AddColumns, AlterExpr, ChangeColumnType, ChangeColumnTypes, Column, ColumnDataType,
    ColumnDataTypeExtension, CreateFlowExpr, CreateTableExpr, CreateViewExpr, DropColumn,
    DropColumns, RenameTable, SemanticType, TableName,
};
use common_error::ext::BoxedError;
use common_grpc_expr::util::ColumnExpr;
use common_time::Timezone;
use datafusion::sql::planner::object_name_to_table_reference;
use datatypes::schema::{ColumnSchema, COMMENT_KEY};
use file_engine::FileOptions;
use query::sql::{
    check_file_to_table_schema_compatibility, file_column_schemas_to_table,
    infer_file_table_schema, prepare_file_table_files,
};
use session::context::QueryContextRef;
use session::table_name::table_idents_to_full_name;
use snafu::{ensure, OptionExt, ResultExt};
use sql::ast::{ColumnDef, ColumnOption, TableConstraint};
use sql::statements::alter::{AlterTable, AlterTableOperation};
use sql::statements::create::{
    CreateExternalTable, CreateFlow, CreateTable, CreateView, TIME_INDEX,
};
use sql::statements::{
    column_def_to_schema, sql_column_def_to_grpc_column_def, sql_data_type_to_concrete_data_type,
};
use sql::util::extract_tables_from_query;
use table::requests::{TableOptions, FILE_TABLE_META_KEY};
use table::table_reference::TableReference;

use crate::error::{
    BuildCreateExprOnInsertionSnafu, ColumnDataTypeSnafu, ConvertColumnDefaultConstraintSnafu,
    ConvertIdentifierSnafu, EncodeJsonSnafu, ExternalSnafu, IllegalPrimaryKeysDefSnafu,
    InferFileTableSchemaSnafu, InvalidSqlSnafu, NotSupportedSnafu, ParseSqlSnafu,
    PrepareFileTableSnafu, Result, SchemaIncompatibleSnafu, UnrecognizedTableOptionSnafu,
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
    query_ctx: &QueryContextRef,
) -> Result<CreateTableExpr> {
    let (catalog_name, schema_name, table_name) =
        table_idents_to_full_name(&create.name, query_ctx)
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;

    let mut table_options = create.options.into_map();

    let (object_store, files) = prepare_file_table_files(&table_options)
        .await
        .context(PrepareFileTableSnafu)?;

    let file_column_schemas = infer_file_table_schema(&object_store, &files, &table_options)
        .await
        .context(InferFileTableSchemaSnafu)?
        .column_schemas;

    let (time_index, primary_keys, table_column_schemas) = if !create.columns.is_empty() {
        // expanded form
        let time_index = find_time_index(&create.constraints)?;
        let primary_keys = find_primary_keys(&create.columns, &create.constraints)?;
        let column_schemas =
            columns_to_column_schemas(&create.columns, &time_index, Some(&query_ctx.timezone()))?;
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
        desc: String::default(),
        column_defs,
        time_index,
        primary_keys,
        create_if_not_exists: create.if_not_exists,
        table_options,
        table_id: None,
        engine: create.engine.to_string(),
    };
    Ok(expr)
}

/// Convert `CreateTable` statement to [`CreateTableExpr`] gRPC request.
pub fn create_to_expr(
    create: &CreateTable,
    query_ctx: &QueryContextRef,
) -> Result<CreateTableExpr> {
    let (catalog_name, schema_name, table_name) =
        table_idents_to_full_name(&create.name, query_ctx)
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;

    let time_index = find_time_index(&create.constraints)?;
    let table_options = HashMap::from(
        &TableOptions::try_from_iter(create.options.to_str_map())
            .context(UnrecognizedTableOptionSnafu)?,
    );

    let primary_keys = find_primary_keys(&create.columns, &create.constraints)?;

    let expr = CreateTableExpr {
        catalog_name,
        schema_name,
        table_name,
        desc: String::default(),
        column_defs: columns_to_expr(
            &create.columns,
            &time_index,
            &primary_keys,
            Some(&query_ctx.timezone()),
        )?,
        time_index,
        primary_keys,
        create_if_not_exists: create.if_not_exists,
        table_options,
        table_id: None,
        engine: create.engine.to_string(),
    };

    validate_create_expr(&expr)?;
    Ok(expr)
}

/// Validate the [`CreateTableExpr`] request.
pub fn validate_create_expr(create: &CreateTableExpr) -> Result<()> {
    // construct column list
    let mut column_to_indices = HashMap::with_capacity(create.column_defs.len());
    for (idx, column) in create.column_defs.iter().enumerate() {
        if let Some(indices) = column_to_indices.get(&column.name) {
            return InvalidSqlSnafu {
                err_msg: format!(
                    "column name `{}` is duplicated at index {} and {}",
                    column.name, indices, idx
                ),
            }
            .fail();
        }
        column_to_indices.insert(&column.name, idx);
    }

    // verify time_index exists
    let _ = column_to_indices
        .get(&create.time_index)
        .with_context(|| InvalidSqlSnafu {
            err_msg: format!(
                "column name `{}` is not found in column list",
                create.time_index
            ),
        })?;

    // verify primary_key exists
    for pk in &create.primary_keys {
        let _ = column_to_indices
            .get(&pk)
            .with_context(|| InvalidSqlSnafu {
                err_msg: format!("column name `{}` is not found in column list", pk),
            })?;
    }

    // construct primary_key set
    let mut pk_set = HashSet::new();
    for pk in &create.primary_keys {
        if !pk_set.insert(pk) {
            return InvalidSqlSnafu {
                err_msg: format!("column name `{}` is duplicated in primary keys", pk),
            }
            .fail();
        }
    }

    // verify time index is not primary key
    if pk_set.contains(&create.time_index) {
        return InvalidSqlSnafu {
            err_msg: format!(
                "column name `{}` is both primary key and time index",
                create.time_index
            ),
        }
        .fail();
    }

    Ok(())
}

fn find_primary_keys(
    columns: &[ColumnDef],
    constraints: &[TableConstraint],
) -> Result<Vec<String>> {
    let columns_pk = columns
        .iter()
        .filter_map(|x| {
            if x.options.iter().any(|o| {
                matches!(
                    o.option,
                    ColumnOption::Unique {
                        is_primary: true,
                        ..
                    }
                )
            }) {
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
                ..
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
                ..
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
    timezone: Option<&Timezone>,
) -> Result<Vec<api::v1::ColumnDef>> {
    let column_schemas = columns_to_column_schemas(column_defs, time_index, timezone)?;
    column_schemas_to_defs(column_schemas, primary_keys)
}

fn columns_to_column_schemas(
    column_defs: &[ColumnDef],
    time_index: &str,
    timezone: Option<&Timezone>,
) -> Result<Vec<ColumnSchema>> {
    column_defs
        .iter()
        .map(|c| {
            column_def_to_schema(c, c.name.to_string() == time_index, timezone)
                .context(ParseSqlSnafu)
        })
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
    query_ctx: &QueryContextRef,
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
                    sql_column_def_to_grpc_column_def(column_def, Some(&query_ctx.timezone()))
                        .map_err(BoxedError::new)
                        .context(ExternalSnafu)?,
                ),
                location: location.as_ref().map(From::from),
            }],
        }),
        AlterTableOperation::ChangeColumnType {
            column_name,
            target_type,
        } => {
            let target_type =
                sql_data_type_to_concrete_data_type(target_type).context(ParseSqlSnafu)?;
            let (target_type, target_type_extension) = ColumnDataTypeWrapper::try_from(target_type)
                .map(|w| w.to_parts())
                .context(ColumnDataTypeSnafu)?;
            Kind::ChangeColumnTypes(ChangeColumnTypes {
                change_column_types: vec![ChangeColumnType {
                    column_name: column_name.value.to_string(),
                    target_type: target_type as i32,
                    target_type_extension,
                }],
            })
        }
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

/// Try to cast the `[CreateViewExpr]` statement into gRPC `[CreateViewExpr]`.
pub fn to_create_view_expr(
    stmt: CreateView,
    logical_plan: Vec<u8>,
    query_ctx: QueryContextRef,
) -> Result<CreateViewExpr> {
    let (catalog_name, schema_name, view_name) = table_idents_to_full_name(&stmt.name, &query_ctx)
        .map_err(BoxedError::new)
        .context(ExternalSnafu)?;

    let expr = CreateViewExpr {
        catalog_name,
        schema_name,
        view_name,
        logical_plan,
        create_if_not_exists: stmt.if_not_exists,
        or_replace: stmt.or_replace,
    };

    Ok(expr)
}

pub fn to_create_flow_task_expr(
    create_flow: CreateFlow,
    query_ctx: &QueryContextRef,
) -> Result<CreateFlowExpr> {
    // retrieve sink table name
    let sink_table_ref =
        object_name_to_table_reference(create_flow.sink_table_name.clone().into(), true)
            .with_context(|_| ConvertIdentifierSnafu {
                ident: create_flow.sink_table_name.to_string(),
            })?;
    let catalog = sink_table_ref
        .catalog()
        .unwrap_or(query_ctx.current_catalog())
        .to_string();
    let schema = sink_table_ref
        .schema()
        .unwrap_or(query_ctx.current_schema())
        .to_string();
    let sink_table_name = TableName {
        catalog_name: catalog,
        schema_name: schema,
        table_name: sink_table_ref.table().to_string(),
    };

    let source_table_names = extract_tables_from_query(&create_flow.query)
        .map(|name| {
            let reference = object_name_to_table_reference(name.clone().into(), true)
                .with_context(|_| ConvertIdentifierSnafu {
                    ident: name.to_string(),
                })?;
            let catalog = reference
                .catalog()
                .unwrap_or(query_ctx.current_catalog())
                .to_string();
            let schema = reference
                .schema()
                .unwrap_or(query_ctx.current_schema())
                .to_string();
            let table_name = TableName {
                catalog_name: catalog,
                schema_name: schema,
                table_name: reference.table().to_string(),
            };
            Ok(table_name)
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(CreateFlowExpr {
        catalog_name: query_ctx.current_catalog().to_string(),
        flow_name: create_flow.flow_name.to_string(),
        source_table_names,
        sink_table_name: Some(sink_table_name),
        or_replace: create_flow.or_replace,
        create_if_not_exists: create_flow.if_not_exists,
        expire_when: create_flow
            .expire_when
            .map(|e| e.to_string())
            .unwrap_or_default(),
        comment: create_flow.comment.unwrap_or_default(),
        sql: create_flow.query.to_string(),
        flow_options: HashMap::new(),
    })
}

#[cfg(test)]
mod tests {
    use datatypes::value::Value;
    use session::context::{QueryContext, QueryContextBuilder};
    use sql::dialect::GreptimeDbDialect;
    use sql::parser::{ParseOptions, ParserContext};
    use sql::statements::statement::Statement;
    use store_api::storage::ColumnDefaultConstraint;

    use super::*;

    #[test]
    fn test_create_to_expr() {
        let sql = "CREATE TABLE monitor (host STRING,ts TIMESTAMP,TIME INDEX (ts),PRIMARY KEY(host)) ENGINE=mito WITH(ttl='3days', write_buffer_size='1024KB');";
        let stmt =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap()
                .pop()
                .unwrap();

        let Statement::CreateTable(create_table) = stmt else {
            unreachable!()
        };
        let expr = create_to_expr(&create_table, &QueryContext::arc()).unwrap();
        assert_eq!("3days", expr.table_options.get("ttl").unwrap());
        assert_eq!(
            "1.0MiB",
            expr.table_options.get("write_buffer_size").unwrap()
        );
    }

    #[test]
    fn test_invalid_create_to_expr() {
        let cases = [
            // duplicate column declaration
            "CREATE TABLE monitor (host STRING primary key, ts TIMESTAMP TIME INDEX, some_column text, some_column string);",
            // duplicate primary key
            "CREATE TABLE monitor (host STRING, ts TIMESTAMP TIME INDEX, some_column STRING, PRIMARY KEY (some_column, host, some_column));",
            // time index is primary key
            "CREATE TABLE monitor (host STRING, ts TIMESTAMP TIME INDEX, PRIMARY KEY (host, ts));"
        ];

        for sql in cases {
            let stmt = ParserContext::create_with_dialect(
                sql,
                &GreptimeDbDialect {},
                ParseOptions::default(),
            )
            .unwrap()
            .pop()
            .unwrap();
            let Statement::CreateTable(create_table) = stmt else {
                unreachable!()
            };
            create_to_expr(&create_table, &QueryContext::arc()).unwrap_err();
        }
    }

    #[test]
    fn test_create_to_expr_with_default_timestamp_value() {
        let sql = "CREATE TABLE monitor (v double,ts TIMESTAMP default '2024-01-30T00:01:01',TIME INDEX (ts)) engine=mito;";
        let stmt =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap()
                .pop()
                .unwrap();

        let Statement::CreateTable(create_table) = stmt else {
            unreachable!()
        };

        // query context with system timezone UTC.
        let expr = create_to_expr(&create_table, &QueryContext::arc()).unwrap();
        let ts_column = &expr.column_defs[1];
        let constraint = assert_ts_column(ts_column);
        assert!(
            matches!(constraint, ColumnDefaultConstraint::Value(Value::Timestamp(ts))
                         if ts.to_iso8601_string() == "2024-01-30 00:01:01+0000")
        );

        // query context with timezone `+08:00`
        let ctx = QueryContextBuilder::default()
            .timezone(Timezone::from_tz_string("+08:00").unwrap().into())
            .build()
            .into();
        let expr = create_to_expr(&create_table, &ctx).unwrap();
        let ts_column = &expr.column_defs[1];
        let constraint = assert_ts_column(ts_column);
        assert!(
            matches!(constraint, ColumnDefaultConstraint::Value(Value::Timestamp(ts))
                         if ts.to_iso8601_string() == "2024-01-29 16:01:01+0000")
        );
    }

    fn assert_ts_column(ts_column: &api::v1::ColumnDef) -> ColumnDefaultConstraint {
        assert_eq!("ts", ts_column.name);
        assert_eq!(
            ColumnDataType::TimestampMillisecond as i32,
            ts_column.data_type
        );
        assert!(!ts_column.default_constraint.is_empty());

        ColumnDefaultConstraint::try_from(&ts_column.default_constraint[..]).unwrap()
    }

    #[test]
    fn test_to_alter_expr() {
        let sql = "ALTER TABLE monitor add column ts TIMESTAMP default '2024-01-30T00:01:01';";
        let stmt =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap()
                .pop()
                .unwrap();

        let Statement::Alter(alter_table) = stmt else {
            unreachable!()
        };

        // query context with system timezone UTC.
        let expr = to_alter_expr(alter_table.clone(), &QueryContext::arc()).unwrap();
        let kind = expr.kind.unwrap();

        let Kind::AddColumns(AddColumns { add_columns, .. }) = kind else {
            unreachable!()
        };

        assert_eq!(1, add_columns.len());
        let ts_column = add_columns[0].column_def.clone().unwrap();
        let constraint = assert_ts_column(&ts_column);
        assert!(
            matches!(constraint, ColumnDefaultConstraint::Value(Value::Timestamp(ts))
                         if ts.to_iso8601_string() == "2024-01-30 00:01:01+0000")
        );

        //
        // query context with timezone `+08:00`
        let ctx = QueryContextBuilder::default()
            .timezone(Timezone::from_tz_string("+08:00").unwrap().into())
            .build()
            .into();
        let expr = to_alter_expr(alter_table, &ctx).unwrap();
        let kind = expr.kind.unwrap();

        let Kind::AddColumns(AddColumns { add_columns, .. }) = kind else {
            unreachable!()
        };

        assert_eq!(1, add_columns.len());
        let ts_column = add_columns[0].column_def.clone().unwrap();
        let constraint = assert_ts_column(&ts_column);
        assert!(
            matches!(constraint, ColumnDefaultConstraint::Value(Value::Timestamp(ts))
                         if ts.to_iso8601_string() == "2024-01-29 16:01:01+0000")
        );
    }

    #[test]
    fn test_to_alter_change_column_type_expr() {
        let sql = "ALTER TABLE monitor MODIFY mem_usage STRING;";
        let stmt =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap()
                .pop()
                .unwrap();

        let Statement::Alter(alter_table) = stmt else {
            unreachable!()
        };

        // query context with system timezone UTC.
        let expr = to_alter_expr(alter_table.clone(), &QueryContext::arc()).unwrap();
        let kind = expr.kind.unwrap();

        let Kind::ChangeColumnTypes(ChangeColumnTypes {
            change_column_types,
        }) = kind
        else {
            unreachable!()
        };

        assert_eq!(1, change_column_types.len());
        let change_column_type = &change_column_types[0];

        assert_eq!("mem_usage", change_column_type.column_name);
        assert_eq!(
            ColumnDataType::String as i32,
            change_column_type.target_type
        );
        assert!(change_column_type.target_type_extension.is_none());
    }

    #[test]
    fn test_to_create_view_expr() {
        let sql = "CREATE VIEW test AS SELECT * FROM NUMBERS";
        let stmt =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap()
                .pop()
                .unwrap();

        let Statement::CreateView(stmt) = stmt else {
            unreachable!()
        };

        let logical_plan = vec![1, 2, 3];

        let expr = to_create_view_expr(stmt, logical_plan.clone(), QueryContext::arc()).unwrap();

        assert_eq!("greptime", expr.catalog_name);
        assert_eq!("public", expr.schema_name);
        assert_eq!("test", expr.view_name);
        assert!(!expr.create_if_not_exists);
        assert!(!expr.or_replace);
        assert_eq!(logical_plan, expr.logical_plan);
    }

    #[test]
    fn test_to_create_view_expr_complex() {
        let sql = "CREATE OR REPLACE VIEW IF NOT EXISTS test.test_view AS SELECT * FROM NUMBERS";
        let stmt =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap()
                .pop()
                .unwrap();

        let Statement::CreateView(stmt) = stmt else {
            unreachable!()
        };

        let logical_plan = vec![1, 2, 3];

        let expr = to_create_view_expr(stmt, logical_plan.clone(), QueryContext::arc()).unwrap();

        assert_eq!("greptime", expr.catalog_name);
        assert_eq!("test", expr.schema_name);
        assert_eq!("test_view", expr.view_name);
        assert!(expr.create_if_not_exists);
        assert!(expr.or_replace);
        assert_eq!(logical_plan, expr.logical_plan);
    }
}
