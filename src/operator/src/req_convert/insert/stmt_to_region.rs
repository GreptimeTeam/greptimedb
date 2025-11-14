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

use api::helper::{ColumnDataTypeWrapper, value_to_grpc_value};
use api::v1::alter_table_expr::Kind;
use api::v1::column_def::options_from_column_schema;
use api::v1::region::InsertRequests as RegionInsertRequests;
use api::v1::{
    AlterTableExpr, ColumnSchema as GrpcColumnSchema, ModifyColumnType, ModifyColumnTypes, Row,
    Rows,
};
use catalog::CatalogManager;
use common_time::Timezone;
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::{ColumnSchema, SchemaRef};
use datatypes::types::JsonType;
use datatypes::value::Value;
use partition::manager::PartitionRuleManager;
use session::context::{QueryContext, QueryContextRef};
use snafu::{OptionExt, ResultExt, ensure};
use sql::ast::ObjectNamePartExt;
use sql::statements::insert::Insert;
use sqlparser::ast::{ObjectName, Value as SqlValue};
use table::TableRef;
use table::metadata::TableInfoRef;

use crate::error::{
    CatalogSnafu, ColumnDataTypeSnafu, ColumnDefaultValueSnafu, ColumnNoneDefaultValueSnafu,
    ColumnNotFoundSnafu, ConvertToGrpcValueSnafu, InvalidInsertRequestSnafu, InvalidSqlSnafu,
    MissingInsertBodySnafu, ParseSqlSnafu, Result, SchemaReadOnlySnafu, TableNotFoundSnafu,
};
use crate::insert::InstantAndNormalInsertRequests;
use crate::req_convert::common::partitioner::Partitioner;
use crate::req_convert::insert::semantic_type;
use crate::statement::StatementExecutor;

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

    pub async fn convert(
        &self,
        stmt: &Insert,
        query_ctx: &QueryContextRef,
        statement_executor: &StatementExecutor,
    ) -> Result<(InstantAndNormalInsertRequests, TableInfoRef)> {
        let name = stmt.table_name().context(ParseSqlSnafu)?;
        let (catalog, schema, table_name) = self.get_full_name(name)?;
        let mut table = self.get_table(&catalog, &schema, &table_name).await?;
        let table_schema = table.schema();

        ensure!(
            !common_catalog::consts::is_readonly_schema(&schema),
            SchemaReadOnlySnafu { name: schema }
        );

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

        let mut rows = vec![
            Row {
                values: Vec::with_capacity(column_count)
            };
            row_count
        ];

        let mut updater = JsonColumnTypeUpdater::new(statement_executor);

        for (i, column_name) in column_names.into_iter().enumerate() {
            let column_schema = table_schema
                .column_schema_by_name(column_name)
                .with_context(|| ColumnNotFoundSnafu {
                    msg: format!("Column {} not found in table {}", column_name, &table_name),
                })?;

            for (sql_row, grpc_row) in sql_rows.iter().zip(rows.iter_mut()) {
                let value = sql_value_to_value(
                    column_schema,
                    &sql_row[i],
                    Some(&query_ctx.timezone()),
                    query_ctx.auto_string_to_numeric(),
                )?;

                updater.check(column_schema, &value)?;

                let value = value_to_grpc_value(value).context(ConvertToGrpcValueSnafu)?;
                grpc_row.values.push(value);
            }
        }

        if updater
            .maybe_update(&catalog, &schema, &table_name, query_ctx)
            .await?
        {
            table = self.get_table(&catalog, &schema, &table_name).await?;
        }
        let table_info = table.table_info();

        let mut schema = Vec::with_capacity(column_count);
        for column_schema in table.schema().column_schemas() {
            let (datatype, datatype_extension) =
                ColumnDataTypeWrapper::try_from(column_schema.data_type.clone())
                    .context(ColumnDataTypeSnafu)?
                    .to_parts();

            let column_name = &column_schema.name;
            let semantic_type = semantic_type(&table_info, column_name)?;

            let grpc_column_schema = GrpcColumnSchema {
                column_name: column_name.clone(),
                datatype: datatype.into(),
                semantic_type: semantic_type.into(),
                datatype_extension,
                options: options_from_column_schema(column_schema),
            };
            schema.push(grpc_column_schema);
        }

        let requests = Partitioner::new(self.partition_manager)
            .partition_insert_requests(&table_info, Rows { schema, rows })
            .await?;
        let requests = RegionInsertRequests { requests };
        if table_info.is_ttl_instant_table() {
            Ok((
                InstantAndNormalInsertRequests {
                    normal_requests: Default::default(),
                    instant_requests: requests,
                },
                table_info,
            ))
        } else {
            Ok((
                InstantAndNormalInsertRequests {
                    normal_requests: requests,
                    instant_requests: Default::default(),
                },
                table_info,
            ))
        }
    }

    async fn get_table(&self, catalog: &str, schema: &str, table: &str) -> Result<TableRef> {
        self.catalog_manager
            .table(catalog, schema, table, None)
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
                self.ctx.current_schema(),
                table.to_string_unquoted(),
            )),
            [schema, table] => Ok((
                self.ctx.current_catalog().to_owned(),
                schema.to_string_unquoted(),
                table.to_string_unquoted(),
            )),
            [catalog, schema, table] => Ok((
                catalog.to_string_unquoted(),
                schema.to_string_unquoted(),
                table.to_string_unquoted(),
            )),
            _ => InvalidSqlSnafu {
                err_msg: format!(
                    "expect table name to be <catalog>.<schema>.<table>, <schema>.<table> or <table>, actual: {obj_name}",
                ),
            }.fail(),
        }
    }
}

struct JsonColumnTypeUpdater<'a, 'b> {
    statement_executor: &'a StatementExecutor,
    merged_types: HashMap<&'b str, JsonType>,
    column_schemas: HashMap<&'b str, &'b ColumnSchema>,
}

impl<'a, 'b> JsonColumnTypeUpdater<'a, 'b> {
    fn new(statement_executor: &'a StatementExecutor) -> Self {
        Self {
            statement_executor,
            merged_types: HashMap::new(),
            column_schemas: HashMap::new(),
        }
    }

    fn check(&mut self, column_schema: &'b ColumnSchema, value: &Value) -> Result<()> {
        if !matches!(value, Value::Json(_)) {
            return Ok(());
        }

        if let (ConcreteDataType::Json(column_type), ConcreteDataType::Json(value_type)) =
            (&column_schema.data_type, value.data_type())
        {
            let merged_type = self
                .merged_types
                .entry(&column_schema.name)
                .or_insert_with(|| column_type.clone());
            if merged_type != &value_type && merged_type.merge(&value_type).is_err() {
                return InvalidInsertRequestSnafu {
                    reason: format!(
                        r#"json types are conflicting: "{merged_type}" vs "{value_type}""#
                    ),
                }
                .fail();
            }
            self.column_schemas
                .insert(&column_schema.name, column_schema);
        }
        Ok(())
    }

    async fn maybe_update(
        self,
        catalog: &str,
        schema: &str,
        table: &str,
        query_context: &QueryContextRef,
    ) -> Result<bool> {
        if self.merged_types.is_empty() {
            return Ok(false);
        }

        let mut has_update = false;
        for (column_name, merged_type) in self.merged_types {
            if let Some(column_schema) = self.column_schemas.get(column_name)
                && let ConcreteDataType::Json(column_type) = &column_schema.data_type
                && column_type != &merged_type
            {
                common_telemetry::info!(
                    "updating table {table} column {column_name} json type: {column_type} => {merged_type}"
                );

                let (target_type, target_type_extension) =
                    ColumnDataTypeWrapper::try_from(ConcreteDataType::Json(merged_type))
                        .context(ColumnDataTypeSnafu)?
                        .to_parts();
                let alter_expr = AlterTableExpr {
                    catalog_name: catalog.to_string(),
                    schema_name: schema.to_string(),
                    table_name: table.to_string(),
                    kind: Some(Kind::ModifyColumnTypes(ModifyColumnTypes {
                        modify_column_types: vec![ModifyColumnType {
                            column_name: column_name.to_string(),
                            target_type: target_type as i32,
                            target_type_extension,
                        }],
                    })),
                };
                self.statement_executor
                    .alter_table_inner(alter_expr, query_context.clone())
                    .await?;
                has_update = true;
            }
        }
        Ok(has_update)
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

/// Converts SQL value to gRPC value according to the column schema.
/// If `auto_string_to_numeric` is true, tries to cast the string value to numeric values,
/// and fills the default value if the cast fails.
fn sql_value_to_value(
    column_schema: &ColumnSchema,
    sql_val: &SqlValue,
    timezone: Option<&Timezone>,
    auto_string_to_numeric: bool,
) -> Result<Value> {
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
        common_sql::convert::sql_value_to_value(
            column,
            &column_schema.data_type,
            sql_val,
            timezone,
            None,
            auto_string_to_numeric,
        )
        .context(crate::error::SqlCommonSnafu)?
    };
    validate(&value)?;
    Ok(value)
}

fn validate(value: &Value) -> Result<()> {
    match value {
        Value::Struct(struct_value) => {
            // Parquet restriction: "Parquet does not support writing empty structs".
            ensure!(
                !struct_value.is_empty(),
                InvalidInsertRequestSnafu {
                    reason: "empty structs are not supported, consider adding a dummy field"
                }
            );
            Ok(())
        }
        Value::Json(value) => validate(value.as_ref()),
        _ => Ok(()),
    }
}

fn replace_default(sql_val: &SqlValue) -> bool {
    matches!(sql_val, SqlValue::Placeholder(s) if s.to_lowercase() == DEFAULT_PLACEHOLDER_VALUE)
}
