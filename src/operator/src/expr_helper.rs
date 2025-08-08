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

#[cfg(feature = "enterprise")]
pub mod trigger;

use std::collections::{HashMap, HashSet};

use api::helper::ColumnDataTypeWrapper;
use api::v1::alter_database_expr::Kind as AlterDatabaseKind;
use api::v1::alter_table_expr::Kind as AlterTableKind;
use api::v1::column_def::options_from_column_schema;
use api::v1::{
    set_index, unset_index, AddColumn, AddColumns, AlterDatabaseExpr, AlterTableExpr, Analyzer,
    ColumnDataType, ColumnDataTypeExtension, CreateFlowExpr, CreateTableExpr, CreateViewExpr,
    DropColumn, DropColumns, DropDefaults, ExpireAfter, FulltextBackend as PbFulltextBackend,
    ModifyColumnType, ModifyColumnTypes, RenameTable, SemanticType, SetDatabaseOptions,
    SetDefaults, SetFulltext, SetIndex, SetIndexes, SetInverted, SetSkipping, SetTableOptions,
    SkippingIndexType as PbSkippingIndexType, TableName, UnsetDatabaseOptions, UnsetFulltext,
    UnsetIndex, UnsetIndexes, UnsetInverted, UnsetSkipping, UnsetTableOptions,
};
use common_error::ext::BoxedError;
use common_grpc_expr::util::ColumnExpr;
use common_time::Timezone;
use datafusion::sql::planner::object_name_to_table_reference;
use datatypes::schema::{
    ColumnSchema, FulltextAnalyzer, FulltextBackend, Schema, SkippingIndexType, COMMENT_KEY,
};
use file_engine::FileOptions;
use query::sql::{
    check_file_to_table_schema_compatibility, file_column_schemas_to_table,
    infer_file_table_schema, prepare_file_table_files,
};
use session::context::QueryContextRef;
use session::table_name::table_idents_to_full_name;
use snafu::{ensure, OptionExt, ResultExt};
use sql::ast::{ColumnOption, ObjectName, ObjectNamePartExt};
use sql::statements::alter::{
    AlterDatabase, AlterDatabaseOperation, AlterTable, AlterTableOperation,
};
use sql::statements::create::{
    Column as SqlColumn, CreateExternalTable, CreateFlow, CreateTable, CreateView, TableConstraint,
};
use sql::statements::{
    column_to_schema, sql_column_def_to_grpc_column_def, sql_data_type_to_concrete_data_type,
};
use sql::util::extract_tables_from_query;
use table::requests::{TableOptions, FILE_TABLE_META_KEY};
use table::table_reference::TableReference;
#[cfg(feature = "enterprise")]
pub use trigger::to_create_trigger_task_expr;

use crate::error::{
    BuildCreateExprOnInsertionSnafu, ColumnDataTypeSnafu, ConvertColumnDefaultConstraintSnafu,
    ConvertIdentifierSnafu, EncodeJsonSnafu, ExternalSnafu, FindNewColumnsOnInsertionSnafu,
    IllegalPrimaryKeysDefSnafu, InferFileTableSchemaSnafu, InvalidFlowNameSnafu, InvalidSqlSnafu,
    NotSupportedSnafu, ParseSqlSnafu, PrepareFileTableSnafu, Result, SchemaIncompatibleSnafu,
    UnrecognizedTableOptionSnafu,
};

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

    validate_create_expr(&expr)?;
    Ok(expr)
}

pub fn extract_add_columns_expr(
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

    for column in &create.column_defs {
        // verify do not contain interval type column issue #3235
        if is_interval_type(&column.data_type()) {
            return InvalidSqlSnafu {
                err_msg: format!(
                    "column name `{}` is interval type, which is not supported",
                    column.name
                ),
            }
            .fail();
        }
        // verify do not contain datetime type column issue #5489
        if is_date_time_type(&column.data_type()) {
            return InvalidSqlSnafu {
                err_msg: format!(
                    "column name `{}` is datetime type, which is not supported, please use `timestamp` type instead",
                    column.name
                ),
            }
            .fail();
        }
    }
    Ok(())
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

fn is_date_time_type(data_type: &ColumnDataType) -> bool {
    matches!(data_type, ColumnDataType::Datetime)
}

fn is_interval_type(data_type: &ColumnDataType) -> bool {
    matches!(
        data_type,
        ColumnDataType::IntervalYearMonth
            | ColumnDataType::IntervalDayTime
            | ColumnDataType::IntervalMonthDayNano
    )
}

fn find_primary_keys(
    columns: &[SqlColumn],
    constraints: &[TableConstraint],
) -> Result<Vec<String>> {
    let columns_pk = columns
        .iter()
        .filter_map(|x| {
            if x.options().iter().any(|o| {
                matches!(
                    o.option,
                    ColumnOption::Unique {
                        is_primary: true,
                        ..
                    }
                )
            }) {
                Some(x.name().value.clone())
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
            TableConstraint::PrimaryKey { columns, .. } => {
                Some(columns.iter().map(|ident| ident.value.clone()))
            }
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
            TableConstraint::TimeIndex { column, .. } => Some(&column.value),
            _ => None,
        })
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
    column_defs: &[SqlColumn],
    time_index: &str,
    primary_keys: &[String],
    timezone: Option<&Timezone>,
) -> Result<Vec<api::v1::ColumnDef>> {
    let column_schemas = columns_to_column_schemas(column_defs, time_index, timezone)?;
    column_schemas_to_defs(column_schemas, primary_keys)
}

fn columns_to_column_schemas(
    columns: &[SqlColumn],
    time_index: &str,
    timezone: Option<&Timezone>,
) -> Result<Vec<ColumnSchema>> {
    columns
        .iter()
        .map(|c| column_to_schema(c, time_index, timezone).context(ParseSqlSnafu))
        .collect::<Result<Vec<ColumnSchema>>>()
}

// TODO(weny): refactor this function to use `try_as_column_def`
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
                options: options_from_column_schema(schema),
            })
        })
        .collect()
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
        AlterTableOperation::SetIndex { options } => {
            let option = match options {
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
                        backend: match options.backend {
                            FulltextBackend::Bloom => PbFulltextBackend::Bloom.into(),
                            FulltextBackend::Tantivy => PbFulltextBackend::Tantivy.into(),
                        },
                        granularity: options.granularity as u64,
                        false_positive_rate: options.false_positive_rate(),
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
                        false_positive_rate: options.false_positive_rate(),
                        skipping_index_type: match options.index_type {
                            SkippingIndexType::BloomFilter => {
                                PbSkippingIndexType::BloomFilter.into()
                            }
                        },
                    })),
                },
            };
            AlterTableKind::SetIndexes(SetIndexes {
                set_indexes: vec![option],
            })
        }
        AlterTableOperation::UnsetIndex { options } => {
            let option = match options {
                sql::statements::alter::UnsetIndexOperation::Fulltext { column_name } => {
                    UnsetIndex {
                        options: Some(unset_index::Options::Fulltext(UnsetFulltext {
                            column_name: column_name.value,
                        })),
                    }
                }
                sql::statements::alter::UnsetIndexOperation::Inverted { column_name } => {
                    UnsetIndex {
                        options: Some(unset_index::Options::Inverted(UnsetInverted {
                            column_name: column_name.value,
                        })),
                    }
                }
                sql::statements::alter::UnsetIndexOperation::Skipping { column_name } => {
                    UnsetIndex {
                        options: Some(unset_index::Options::Skipping(UnsetSkipping {
                            column_name: column_name.value,
                        })),
                    }
                }
            };

            AlterTableKind::UnsetIndexes(UnsetIndexes {
                unset_indexes: vec![option],
            })
        }
        AlterTableOperation::DropDefaults { columns } => {
            AlterTableKind::DropDefaults(DropDefaults {
                drop_defaults: columns
                    .into_iter()
                    .map(|col| {
                        let column_name = col.0.to_string();
                        Ok(api::v1::DropDefault { column_name })
                    })
                    .collect::<Result<Vec<_>>>()?,
            })
        }
        AlterTableOperation::SetDefaults { defaults } => AlterTableKind::SetDefaults(SetDefaults {
            set_defaults: defaults
                .into_iter()
                .map(|col| {
                    let column_name = col.column_name.to_string();
                    let default_constraint = serde_json::to_string(&col.default_constraint)
                        .context(EncodeJsonSnafu)?
                        .into_bytes();
                    Ok(api::v1::SetDefault {
                        column_name,
                        default_constraint,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
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
    Ok(flow_name.0.swap_remove(0).to_string_unquoted())
}

#[cfg(test)]
mod tests {
    use api::v1::{SetDatabaseOptions, UnsetDatabaseOptions};
    use datatypes::value::Value;
    use session::context::{QueryContext, QueryContextBuilder};
    use sql::dialect::GreptimeDbDialect;
    use sql::parser::{ParseOptions, ParserContext};
    use sql::statements::statement::Statement;
    use store_api::storage::ColumnDefaultConstraint;

    use super::*;

    #[test]
    fn test_create_flow_tql_expr() {
        let sql = r#"
CREATE FLOW calc_reqs SINK TO cnt_reqs AS
TQL EVAL (0, 15, '5s') count_values("status_code", http_requests);"#;
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
        assert_eq!("calc_reqs", expr.flow_name);
        assert_eq!("greptime", expr.catalog_name);
        assert_eq!(
            "greptime.public.cnt_reqs",
            expr.sink_table_name.map(to_dot_sep).unwrap()
        );
        assert!(expr.source_table_names.is_empty());
        assert_eq!(
            r#"TQL EVAL (0, 15, '5s') count_values("status_code", http_requests)"#,
            expr.sql
        );
    }

    #[test]
    fn test_create_flow_expr() {
        let sql = r"
CREATE FLOW test_distinct_basic SINK TO out_distinct_basic AS
SELECT
    DISTINCT number as dis
FROM
    distinct_basic;";
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
        assert_eq!("test_distinct_basic", expr.flow_name);
        assert_eq!("greptime", expr.catalog_name);
        assert_eq!(
            "greptime.public.out_distinct_basic",
            expr.sink_table_name.map(to_dot_sep).unwrap()
        );
        assert_eq!(1, expr.source_table_names.len());
        assert_eq!(
            "greptime.public.distinct_basic",
            to_dot_sep(expr.source_table_names[0].clone())
        );
        assert_eq!(
            r"SELECT
    DISTINCT number as dis
FROM
    distinct_basic",
            expr.sql
        );

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
