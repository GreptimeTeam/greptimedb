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
use api::v1::alter_database_expr::Kind as AlterDatabaseKind;
use api::v1::alter_table_expr::Kind as AlterTableKind;
use api::v1::{
    set_index, unset_index, AddColumn, AddColumns, AlterDatabaseExpr, AlterTableExpr, Analyzer,
    CreateFlowExpr, CreateTableExpr, CreateViewExpr, DropColumn, DropColumns, ExpireAfter,
    ModifyColumnType, ModifyColumnTypes, RenameTable, SetDatabaseOptions, SetFulltext, SetIndex,
    SetInverted, SetSkipping, SetTableOptions, SkippingIndexType as PbSkippingIndexType, TableName,
    UnsetDatabaseOptions, UnsetFulltext, UnsetIndex, UnsetInverted, UnsetSkipping,
    UnsetTableOptions,
};
use common_error::ext::BoxedError;
use common_grpc_expr::util::ColumnExpr;
use datafusion::sql::planner::object_name_to_table_reference;
use datatypes::schema::{FulltextAnalyzer, Schema, SkippingIndexType};
use file_engine::FileOptions;
use query::sql::{
    check_file_to_table_schema_compatibility, file_column_schemas_to_table,
    infer_file_table_schema, prepare_file_table_files,
};
use session::context::QueryContextRef;
use session::table_name::table_idents_to_full_name;
use snafu::{ensure, ResultExt};
use sql::ast::ObjectName;
use sql::statements::alter::{
    AlterDatabase, AlterDatabaseOperation, AlterTable, AlterTableOperation,
};
use sql::statements::create::{CreateExternalTable, CreateFlow, CreateTable, CreateView};
use sql::statements::{sql_column_def_to_grpc_column_def, sql_data_type_to_concrete_data_type};
pub use sql::util::column_schemas_to_defs;
use sql::util::{
    columns_to_column_schemas, extract_tables_from_query, find_primary_keys, find_time_index,
    is_date_time_type, is_interval_type, validate_create_expr,
};
use table::requests::FILE_TABLE_META_KEY;
use table::table_reference::TableReference;

use crate::error::{
    BuildCreateExprOnInsertionSnafu, ColumnDataTypeSnafu, ConvertIdentifierSnafu, EncodeJsonSnafu,
    ExternalSnafu, FindNewColumnsOnInsertionSnafu, InferFileTableSchemaSnafu, InvalidFlowNameSnafu,
    InvalidSqlSnafu, NotSupportedSnafu, ParseSqlSnafu, PrepareFileTableSnafu, Result,
    SchemaIncompatibleSnafu,
};

pub fn create_to_expr(
    create: &CreateTable,
    query_ctx: &QueryContextRef,
) -> Result<CreateTableExpr> {
    //todo: remove the unwrap
    Ok(sql::util::create_to_expr(
        create,
        query_ctx.current_catalog(),
        query_ctx.current_schema(),
        &query_ctx.timezone(),
    )
    .unwrap())
}

pub fn create_table_expr_by_column_schemas(
    table_name: &TableReference<'_>,
    column_schemas: &[api::v1::ColumnSchema],
    engine: &str,
    desc: Option<&str>,
) -> Result<CreateTableExpr> {
    let column_exprs = ColumnExpr::from_column_schemas(column_schemas);
    let expr = common_grpc_expr::util::build_create_table_expr(
        None,
        table_name,
        column_exprs,
        engine,
        desc.unwrap_or("Created on insertion"),
    )
    .context(BuildCreateExprOnInsertionSnafu)?;

    validate_create_expr(&expr).context(ParseSqlSnafu)?;
    Ok(expr)
}

pub(crate) fn extract_add_columns_expr(
    schema: &Schema,
    column_exprs: Vec<ColumnExpr>,
) -> Result<Option<AddColumns>> {
    let add_columns = common_grpc_expr::util::extract_new_columns(schema, column_exprs)
        .context(FindNewColumnsOnInsertionSnafu)?;
    if let Some(add_columns) = &add_columns {
        validate_add_columns_expr(add_columns)?;
    }
    Ok(add_columns)
}

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
        let time_index = find_time_index(&create.constraints).context(ParseSqlSnafu)?;
        let primary_keys =
            find_primary_keys(&create.columns, &create.constraints).context(ParseSqlSnafu)?;
        let column_schemas =
            columns_to_column_schemas(&create.columns, &time_index, Some(&query_ctx.timezone()))
                .context(ParseSqlSnafu)?;
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

    let column_defs =
        column_schemas_to_defs(table_column_schemas, &primary_keys).context(ParseSqlSnafu)?;
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

fn validate_add_columns_expr(add_columns: &AddColumns) -> Result<()> {
    for add_column in &add_columns.add_columns {
        let Some(column_def) = &add_column.column_def else {
            continue;
        };
        if is_date_time_type(&column_def.data_type()) {
            return InvalidSqlSnafu {
                    err_msg: format!("column name `{}` is datetime type, which is not supported, please use `timestamp` type instead", column_def.name),
                }
                .fail();
        }
        if is_interval_type(&column_def.data_type()) {
            return InvalidSqlSnafu {
                err_msg: format!(
                    "column name `{}` is interval type, which is not supported",
                    column_def.name
                ),
            }
            .fail();
        }
    }
    Ok(())
}

/// Converts a SQL alter table statement into a gRPC alter table expression.
pub(crate) fn to_alter_table_expr(
    alter_table: AlterTable,
    query_ctx: &QueryContextRef,
) -> Result<AlterTableExpr> {
    let (catalog_name, schema_name, table_name) =
        table_idents_to_full_name(alter_table.table_name(), query_ctx)
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;

    let kind = match alter_table.alter_operation {
        AlterTableOperation::AddConstraint(_) => {
            return NotSupportedSnafu {
                feat: "ADD CONSTRAINT",
            }
            .fail();
        }
        AlterTableOperation::AddColumns { add_columns } => AlterTableKind::AddColumns(AddColumns {
            add_columns: add_columns
                .into_iter()
                .map(|add_column| {
                    let column_def = sql_column_def_to_grpc_column_def(
                        &add_column.column_def,
                        Some(&query_ctx.timezone()),
                    )
                    .map_err(BoxedError::new)
                    .context(ExternalSnafu)?;
                    if is_interval_type(&column_def.data_type()) {
                        return NotSupportedSnafu {
                            feat: "Add column with interval type",
                        }
                        .fail();
                    }
                    Ok(AddColumn {
                        column_def: Some(column_def),
                        location: add_column.location.as_ref().map(From::from),
                        add_if_not_exists: add_column.add_if_not_exists,
                    })
                })
                .collect::<Result<Vec<AddColumn>>>()?,
        }),
        AlterTableOperation::ModifyColumnType {
            column_name,
            target_type,
        } => {
            let target_type =
                sql_data_type_to_concrete_data_type(&target_type).context(ParseSqlSnafu)?;
            let (target_type, target_type_extension) = ColumnDataTypeWrapper::try_from(target_type)
                .map(|w| w.to_parts())
                .context(ColumnDataTypeSnafu)?;
            if is_interval_type(&target_type) {
                return NotSupportedSnafu {
                    feat: "Modify column type to interval type",
                }
                .fail();
            }
            AlterTableKind::ModifyColumnTypes(ModifyColumnTypes {
                modify_column_types: vec![ModifyColumnType {
                    column_name: column_name.value,
                    target_type: target_type as i32,
                    target_type_extension,
                }],
            })
        }
        AlterTableOperation::DropColumn { name } => AlterTableKind::DropColumns(DropColumns {
            drop_columns: vec![DropColumn {
                name: name.value.to_string(),
            }],
        }),
        AlterTableOperation::RenameTable { new_table_name } => {
            AlterTableKind::RenameTable(RenameTable {
                new_table_name: new_table_name.to_string(),
            })
        }
        AlterTableOperation::SetTableOptions { options } => {
            AlterTableKind::SetTableOptions(SetTableOptions {
                table_options: options.into_iter().map(Into::into).collect(),
            })
        }
        AlterTableOperation::UnsetTableOptions { keys } => {
            AlterTableKind::UnsetTableOptions(UnsetTableOptions { keys })
        }
        AlterTableOperation::SetIndex { options } => AlterTableKind::SetIndex(match options {
            sql::statements::alter::SetIndexOperation::Fulltext {
                column_name,
                options,
            } => SetIndex {
                options: Some(set_index::Options::Fulltext(SetFulltext {
                    column_name: column_name.value,
                    enable: options.enable,
                    analyzer: match options.analyzer {
                        FulltextAnalyzer::English => Analyzer::English.into(),
                        FulltextAnalyzer::Chinese => Analyzer::Chinese.into(),
                    },
                    case_sensitive: options.case_sensitive,
                })),
            },
            sql::statements::alter::SetIndexOperation::Inverted { column_name } => SetIndex {
                options: Some(set_index::Options::Inverted(SetInverted {
                    column_name: column_name.value,
                })),
            },
            sql::statements::alter::SetIndexOperation::Skipping {
                column_name,
                options,
            } => SetIndex {
                options: Some(set_index::Options::Skipping(SetSkipping {
                    column_name: column_name.value,
                    enable: true,
                    granularity: options.granularity as u64,
                    skipping_index_type: match options.index_type {
                        SkippingIndexType::BloomFilter => PbSkippingIndexType::BloomFilter.into(),
                    },
                })),
            },
        }),
        AlterTableOperation::UnsetIndex { options } => AlterTableKind::UnsetIndex(match options {
            sql::statements::alter::UnsetIndexOperation::Fulltext { column_name } => UnsetIndex {
                options: Some(unset_index::Options::Fulltext(UnsetFulltext {
                    column_name: column_name.value,
                })),
            },
            sql::statements::alter::UnsetIndexOperation::Inverted { column_name } => UnsetIndex {
                options: Some(unset_index::Options::Inverted(UnsetInverted {
                    column_name: column_name.value,
                })),
            },
            sql::statements::alter::UnsetIndexOperation::Skipping { column_name } => UnsetIndex {
                options: Some(unset_index::Options::Skipping(UnsetSkipping {
                    column_name: column_name.value,
                })),
            },
        }),
    };

    Ok(AlterTableExpr {
        catalog_name,
        schema_name,
        table_name,
        kind: Some(kind),
    })
}

/// Try to cast the `[AlterDatabase]` statement into gRPC `[AlterDatabaseExpr]`.
pub fn to_alter_database_expr(
    alter_database: AlterDatabase,
    query_ctx: &QueryContextRef,
) -> Result<AlterDatabaseExpr> {
    let catalog = query_ctx.current_catalog();
    let schema = alter_database.database_name;

    let kind = match alter_database.alter_operation {
        AlterDatabaseOperation::SetDatabaseOption { options } => {
            let options = options.into_iter().map(Into::into).collect();
            AlterDatabaseKind::SetDatabaseOptions(SetDatabaseOptions {
                set_database_options: options,
            })
        }
        AlterDatabaseOperation::UnsetDatabaseOption { keys } => {
            AlterDatabaseKind::UnsetDatabaseOptions(UnsetDatabaseOptions { keys })
        }
    };

    Ok(AlterDatabaseExpr {
        catalog_name: catalog.to_string(),
        schema_name: schema.to_string(),
        kind: Some(kind),
    })
}

/// Try to cast the `[CreateViewExpr]` statement into gRPC `[CreateViewExpr]`.
pub fn to_create_view_expr(
    stmt: CreateView,
    logical_plan: Vec<u8>,
    table_names: Vec<TableName>,
    columns: Vec<String>,
    plan_columns: Vec<String>,
    definition: String,
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
        table_names,
        columns,
        plan_columns,
        definition,
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
        .map(|s| s.to_owned())
        .unwrap_or(query_ctx.current_schema());

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
                .map(|s| s.to_string())
                .unwrap_or(query_ctx.current_schema());

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
        flow_name: sanitize_flow_name(create_flow.flow_name)?,
        source_table_names,
        sink_table_name: Some(sink_table_name),
        or_replace: create_flow.or_replace,
        create_if_not_exists: create_flow.if_not_exists,
        expire_after: create_flow.expire_after.map(|value| ExpireAfter { value }),
        comment: create_flow.comment.unwrap_or_default(),
        sql: create_flow.query.to_string(),
        flow_options: HashMap::new(),
    })
}

/// sanitize the flow name, remove possible quotes
fn sanitize_flow_name(mut flow_name: ObjectName) -> Result<String> {
    ensure!(
        flow_name.0.len() == 1,
        InvalidFlowNameSnafu {
            name: flow_name.to_string(),
        }
    );
    // safety: we've checked flow_name.0 has exactly one element.
    Ok(flow_name.0.swap_remove(0).value)
}

#[cfg(test)]
mod tests {
    use api::v1::{ColumnDataType, SetDatabaseOptions, UnsetDatabaseOptions};
    use common_time::Timezone;
    use datatypes::value::Value;
    use session::context::{QueryContext, QueryContextBuilder};
    use sql::dialect::GreptimeDbDialect;
    use sql::parser::{ParseOptions, ParserContext};
    use sql::statements::statement::Statement;
    use store_api::storage::ColumnDefaultConstraint;

    use super::*;

    #[test]
    fn test_create_flow_expr() {
        let sql = r"
CREATE FLOW `task_2`
SINK TO schema_1.table_1
AS
SELECT max(c1), min(c2) FROM schema_2.table_2;";
        let stmt =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap()
                .pop()
                .unwrap();

        let Statement::CreateFlow(create_flow) = stmt else {
            unreachable!()
        };
        let expr = to_create_flow_task_expr(create_flow, &QueryContext::arc()).unwrap();

        let to_dot_sep =
            |c: TableName| format!("{}.{}.{}", c.catalog_name, c.schema_name, c.table_name);
        assert_eq!("task_2", expr.flow_name);
        assert_eq!("greptime", expr.catalog_name);
        assert_eq!(
            "greptime.schema_1.table_1",
            expr.sink_table_name.map(to_dot_sep).unwrap()
        );
        assert_eq!(1, expr.source_table_names.len());
        assert_eq!(
            "greptime.schema_2.table_2",
            to_dot_sep(expr.source_table_names[0].clone())
        );
        assert_eq!("SELECT max(c1), min(c2) FROM schema_2.table_2", expr.sql);

        let sql = r"
CREATE FLOW abc.`task_2`
SINK TO schema_1.table_1
AS
SELECT max(c1), min(c2) FROM schema_2.table_2;";
        let stmt =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap()
                .pop()
                .unwrap();

        let Statement::CreateFlow(create_flow) = stmt else {
            unreachable!()
        };
        let res = to_create_flow_task_expr(create_flow, &QueryContext::arc());

        assert!(res.is_err());
        assert!(res
            .unwrap_err()
            .to_string()
            .contains("Invalid flow name: abc.`task_2`"));
    }

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
            .timezone(Timezone::from_tz_string("+08:00").unwrap())
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
        let sql = "ALTER DATABASE greptime SET key1='value1', key2='value2';";
        let stmt =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap()
                .pop()
                .unwrap();

        let Statement::AlterDatabase(alter_database) = stmt else {
            unreachable!()
        };

        let expr = to_alter_database_expr(alter_database, &QueryContext::arc()).unwrap();
        let kind = expr.kind.unwrap();

        let AlterDatabaseKind::SetDatabaseOptions(SetDatabaseOptions {
            set_database_options,
        }) = kind
        else {
            unreachable!()
        };

        assert_eq!(2, set_database_options.len());
        assert_eq!("key1", set_database_options[0].key);
        assert_eq!("value1", set_database_options[0].value);
        assert_eq!("key2", set_database_options[1].key);
        assert_eq!("value2", set_database_options[1].value);

        let sql = "ALTER DATABASE greptime UNSET key1, key2;";
        let stmt =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap()
                .pop()
                .unwrap();

        let Statement::AlterDatabase(alter_database) = stmt else {
            unreachable!()
        };

        let expr = to_alter_database_expr(alter_database, &QueryContext::arc()).unwrap();
        let kind = expr.kind.unwrap();

        let AlterDatabaseKind::UnsetDatabaseOptions(UnsetDatabaseOptions { keys }) = kind else {
            unreachable!()
        };

        assert_eq!(2, keys.len());
        assert!(keys.contains(&"key1".to_string()));
        assert!(keys.contains(&"key2".to_string()));

        let sql = "ALTER TABLE monitor add column ts TIMESTAMP default '2024-01-30T00:01:01';";
        let stmt =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap()
                .pop()
                .unwrap();

        let Statement::AlterTable(alter_table) = stmt else {
            unreachable!()
        };

        // query context with system timezone UTC.
        let expr = to_alter_table_expr(alter_table.clone(), &QueryContext::arc()).unwrap();
        let kind = expr.kind.unwrap();

        let AlterTableKind::AddColumns(AddColumns { add_columns, .. }) = kind else {
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
            .timezone(Timezone::from_tz_string("+08:00").unwrap())
            .build()
            .into();
        let expr = to_alter_table_expr(alter_table, &ctx).unwrap();
        let kind = expr.kind.unwrap();

        let AlterTableKind::AddColumns(AddColumns { add_columns, .. }) = kind else {
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
    fn test_to_alter_modify_column_type_expr() {
        let sql = "ALTER TABLE monitor MODIFY COLUMN mem_usage STRING;";
        let stmt =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap()
                .pop()
                .unwrap();

        let Statement::AlterTable(alter_table) = stmt else {
            unreachable!()
        };

        // query context with system timezone UTC.
        let expr = to_alter_table_expr(alter_table.clone(), &QueryContext::arc()).unwrap();
        let kind = expr.kind.unwrap();

        let AlterTableKind::ModifyColumnTypes(ModifyColumnTypes {
            modify_column_types,
        }) = kind
        else {
            unreachable!()
        };

        assert_eq!(1, modify_column_types.len());
        let modify_column_type = &modify_column_types[0];

        assert_eq!("mem_usage", modify_column_type.column_name);
        assert_eq!(
            ColumnDataType::String as i32,
            modify_column_type.target_type
        );
        assert!(modify_column_type.target_type_extension.is_none());
    }

    fn new_test_table_names() -> Vec<TableName> {
        vec![
            TableName {
                catalog_name: "greptime".to_string(),
                schema_name: "public".to_string(),
                table_name: "a_table".to_string(),
            },
            TableName {
                catalog_name: "greptime".to_string(),
                schema_name: "public".to_string(),
                table_name: "b_table".to_string(),
            },
        ]
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
        let table_names = new_test_table_names();
        let columns = vec!["a".to_string()];
        let plan_columns = vec!["number".to_string()];

        let expr = to_create_view_expr(
            stmt,
            logical_plan.clone(),
            table_names.clone(),
            columns.clone(),
            plan_columns.clone(),
            sql.to_string(),
            QueryContext::arc(),
        )
        .unwrap();

        assert_eq!("greptime", expr.catalog_name);
        assert_eq!("public", expr.schema_name);
        assert_eq!("test", expr.view_name);
        assert!(!expr.create_if_not_exists);
        assert!(!expr.or_replace);
        assert_eq!(logical_plan, expr.logical_plan);
        assert_eq!(table_names, expr.table_names);
        assert_eq!(sql, expr.definition);
        assert_eq!(columns, expr.columns);
        assert_eq!(plan_columns, expr.plan_columns);
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
        let table_names = new_test_table_names();
        let columns = vec!["a".to_string()];
        let plan_columns = vec!["number".to_string()];

        let expr = to_create_view_expr(
            stmt,
            logical_plan.clone(),
            table_names.clone(),
            columns.clone(),
            plan_columns.clone(),
            sql.to_string(),
            QueryContext::arc(),
        )
        .unwrap();

        assert_eq!("greptime", expr.catalog_name);
        assert_eq!("test", expr.schema_name);
        assert_eq!("test_view", expr.view_name);
        assert!(expr.create_if_not_exists);
        assert!(expr.or_replace);
        assert_eq!(logical_plan, expr.logical_plan);
        assert_eq!(table_names, expr.table_names);
        assert_eq!(sql, expr.definition);
        assert_eq!(columns, expr.columns);
        assert_eq!(plan_columns, expr.plan_columns);
    }
}
