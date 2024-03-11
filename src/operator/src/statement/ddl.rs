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
use std::sync::Arc;

use api::helper::ColumnDataTypeWrapper;
use api::v1::{column_def, AlterExpr, CreateTableExpr};
use catalog::CatalogManagerRef;
use chrono::Utc;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_catalog::format_full_table_name;
use common_error::ext::BoxedError;
use common_meta::cache_invalidator::Context;
use common_meta::ddl::ExecutorContext;
use common_meta::key::schema_name::{SchemaNameKey, SchemaNameValue};
use common_meta::key::NAME_PATTERN;
use common_meta::rpc::ddl::{DdlTask, SubmitDdlTaskRequest, SubmitDdlTaskResponse};
use common_meta::rpc::router::{Partition, Partition as MetaPartition};
use common_meta::table_name::TableName;
use common_query::Output;
use common_telemetry::{info, tracing};
use common_time::Timezone;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::RawSchema;
use datatypes::value::Value;
use lazy_static::lazy_static;
use partition::expr::{Operand, PartitionExpr, RestrictedOp};
use partition::partition::{PartitionBound, PartitionDef};
use query::sql::create_table_stmt;
use regex::Regex;
use session::context::QueryContextRef;
use session::table_name::table_idents_to_full_name;
use snafu::{ensure, IntoError, OptionExt, ResultExt};
use sql::statements::alter::AlterTable;
use sql::statements::create::{CreateExternalTable, CreateTable, CreateTableLike, Partitions};
use sql::statements::sql_value_to_value;
use sqlparser::ast::{Expr, Ident, Value as ParserValue};
use table::dist_table::DistTable;
use table::metadata::{self, RawTableInfo, RawTableMeta, TableId, TableInfo, TableType};
use table::requests::{AlterKind, AlterTableRequest, TableOptions};
use table::TableRef;

use super::StatementExecutor;
use crate::error::{
    self, AlterExprToRequestSnafu, CatalogSnafu, ColumnDataTypeSnafu, ColumnNotFoundSnafu,
    CreateLogicalTablesSnafu, CreateTableInfoSnafu, CreateTableWithMultiCatalogsSnafu,
    CreateTableWithMultiSchemasSnafu, DeserializePartitionSnafu, EmptyCreateTableExprSnafu,
    InvalidPartitionColumnsSnafu, InvalidPartitionRuleSnafu, InvalidTableNameSnafu,
    ParseSqlValueSnafu, Result, SchemaNotFoundSnafu, TableAlreadyExistsSnafu,
    TableMetadataManagerSnafu, TableNotFoundSnafu, UnrecognizedTableOptionSnafu,
};
use crate::expr_factory;
use crate::statement::show::create_partitions_stmt;

lazy_static! {
    static ref NAME_PATTERN_REG: Regex = Regex::new(&format!("^{NAME_PATTERN}$")).unwrap();
}

impl StatementExecutor {
    pub fn catalog_manager(&self) -> CatalogManagerRef {
        self.catalog_manager.clone()
    }

    #[tracing::instrument(skip_all)]
    pub async fn create_table(&self, stmt: CreateTable, ctx: QueryContextRef) -> Result<TableRef> {
        let create_expr = &mut expr_factory::create_to_expr(&stmt, ctx.clone())?;
        self.create_table_inner(create_expr, stmt.partitions, &ctx)
            .await
    }

    #[tracing::instrument(skip_all)]
    pub async fn create_table_like(
        &self,
        stmt: CreateTableLike,
        ctx: QueryContextRef,
    ) -> Result<TableRef> {
        let (catalog, schema, table) = table_idents_to_full_name(&stmt.source_name, &ctx)
            .map_err(BoxedError::new)
            .context(error::ExternalSnafu)?;
        let table_ref = self
            .catalog_manager
            .table(&catalog, &schema, &table)
            .await
            .context(CatalogSnafu)?
            .context(TableNotFoundSnafu { table_name: &table })?;
        let partitions = self
            .partition_manager
            .find_table_partitions(table_ref.table_info().table_id())
            .await
            .context(error::FindTablePartitionRuleSnafu { table_name: table })?;

        let quote_style = ctx.quote_style();
        let mut create_stmt = create_table_stmt(&table_ref.table_info(), quote_style)
            .context(error::ParseQuerySnafu)?;
        create_stmt.name = stmt.table_name;
        create_stmt.if_not_exists = false;

        let partitions = create_partitions_stmt(partitions)?.and_then(|mut partitions| {
            if !partitions.column_list.is_empty() {
                partitions.set_quote(quote_style);
                Some(partitions)
            } else {
                None
            }
        });

        let create_expr = &mut expr_factory::create_to_expr(&create_stmt, ctx.clone())?;
        self.create_table_inner(create_expr, partitions, &ctx).await
    }

    #[tracing::instrument(skip_all)]
    pub async fn create_external_table(
        &self,
        create_expr: CreateExternalTable,
        ctx: QueryContextRef,
    ) -> Result<TableRef> {
        let create_expr = &mut expr_factory::create_external_expr(create_expr, ctx.clone()).await?;
        self.create_table_inner(create_expr, None, &ctx).await
    }

    #[tracing::instrument(skip_all)]
    pub async fn create_table_inner(
        &self,
        create_table: &mut CreateTableExpr,
        partitions: Option<Partitions>,
        query_ctx: &QueryContextRef,
    ) -> Result<TableRef> {
        let _timer = crate::metrics::DIST_CREATE_TABLE.start_timer();
        let schema = self
            .table_metadata_manager
            .schema_manager()
            .get(SchemaNameKey::new(
                &create_table.catalog_name,
                &create_table.schema_name,
            ))
            .await
            .context(TableMetadataManagerSnafu)?;

        let Some(schema_opts) = schema else {
            return SchemaNotFoundSnafu {
                schema_info: &create_table.schema_name,
            }
            .fail();
        };

        // if table exists.
        if let Some(table) = self
            .catalog_manager
            .table(
                &create_table.catalog_name,
                &create_table.schema_name,
                &create_table.table_name,
            )
            .await
            .context(CatalogSnafu)?
        {
            return if create_table.create_if_not_exists {
                Ok(table)
            } else {
                TableAlreadyExistsSnafu {
                    table: format_full_table_name(
                        &create_table.catalog_name,
                        &create_table.schema_name,
                        &create_table.table_name,
                    ),
                }
                .fail()
            };
        }

        ensure!(
            NAME_PATTERN_REG.is_match(&create_table.table_name),
            InvalidTableNameSnafu {
                table_name: create_table.table_name.clone(),
            }
        );

        let table_name = TableName::new(
            &create_table.catalog_name,
            &create_table.schema_name,
            &create_table.table_name,
        );

        let (partitions, partition_cols) = parse_partitions(create_table, partitions, query_ctx)?;

        validate_partition_columns(create_table, &partition_cols)?;

        let mut table_info = create_table_info(create_table, partition_cols, schema_opts)?;

        let resp = self
            .create_table_procedure(create_table.clone(), partitions, table_info.clone())
            .await?;

        let table_id = resp.table_id.context(error::UnexpectedSnafu {
            violated: "expected table_id",
        })?;
        info!("Successfully created table '{table_name}' with table id {table_id}");

        table_info.ident.table_id = table_id;

        let table_info = Arc::new(table_info.try_into().context(CreateTableInfoSnafu)?);
        create_table.table_id = Some(api::v1::TableId { id: table_id });

        let table = DistTable::table(table_info);

        Ok(table)
    }

    #[tracing::instrument(skip_all)]
    pub async fn create_logical_tables(
        &self,
        create_table_exprs: &[CreateTableExpr],
    ) -> Result<Vec<TableRef>> {
        let _timer = crate::metrics::DIST_CREATE_TABLES.start_timer();
        ensure!(!create_table_exprs.is_empty(), EmptyCreateTableExprSnafu);
        ensure!(
            create_table_exprs
                .windows(2)
                .all(|expr| expr[0].catalog_name == expr[1].catalog_name),
            CreateTableWithMultiCatalogsSnafu {
                catalog_names: create_table_exprs
                    .iter()
                    .map(|x| x.catalog_name.as_str())
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>()
                    .join(",")
                    .to_string()
            }
        );
        let catalog_name = create_table_exprs[0].catalog_name.to_string();

        ensure!(
            create_table_exprs
                .windows(2)
                .all(|expr| expr[0].schema_name == expr[1].schema_name),
            CreateTableWithMultiSchemasSnafu {
                schema_names: create_table_exprs
                    .iter()
                    .map(|x| x.schema_name.as_str())
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>()
                    .join(",")
                    .to_string()
            }
        );
        let schema_name = create_table_exprs[0].schema_name.to_string();

        // Check table names
        for create_table in create_table_exprs {
            ensure!(
                NAME_PATTERN_REG.is_match(&create_table.table_name),
                InvalidTableNameSnafu {
                    table_name: create_table.table_name.clone(),
                }
            );
        }

        let schema = self
            .table_metadata_manager
            .schema_manager()
            .get(SchemaNameKey::new(&catalog_name, &schema_name))
            .await
            .context(TableMetadataManagerSnafu)?
            .context(SchemaNotFoundSnafu {
                schema_info: &schema_name,
            })?;

        let mut raw_tables_info = create_table_exprs
            .iter()
            .map(|create| create_table_info(create, vec![], schema.clone()))
            .collect::<Result<Vec<_>>>()?;
        let tables_data = create_table_exprs
            .iter()
            .cloned()
            .zip(raw_tables_info.iter().cloned())
            .collect::<Vec<_>>();

        let resp = self.create_logical_tables_procedure(tables_data).await?;

        let table_ids = resp.table_ids;
        ensure!(table_ids.len() == raw_tables_info.len(), CreateLogicalTablesSnafu {
            reason: format!("The number of tables is inconsistent with the expected number to be created, expected: {}, actual: {}", raw_tables_info.len(), table_ids.len())
        });
        info!("Successfully created logical tables: {:?}", table_ids);

        for (i, table_info) in raw_tables_info.iter_mut().enumerate() {
            table_info.ident.table_id = table_ids[i];
        }
        let tables_info = raw_tables_info
            .into_iter()
            .map(|x| x.try_into().context(CreateTableInfoSnafu))
            .collect::<Result<Vec<_>>>()?;

        Ok(tables_info
            .into_iter()
            .map(|x| DistTable::table(Arc::new(x)))
            .collect())
    }

    #[tracing::instrument(skip_all)]
    pub async fn drop_table(&self, table_name: TableName, drop_if_exists: bool) -> Result<Output> {
        if let Some(table) = self
            .catalog_manager
            .table(
                &table_name.catalog_name,
                &table_name.schema_name,
                &table_name.table_name,
            )
            .await
            .context(CatalogSnafu)?
        {
            let table_id = table.table_info().table_id();
            self.drop_table_procedure(&table_name, table_id, drop_if_exists)
                .await?;

            // Invalidates local cache ASAP.
            self.cache_invalidator
                .invalidate_table_id(&Context::default(), table_id)
                .await
                .context(error::InvalidateTableCacheSnafu)?;

            self.cache_invalidator
                .invalidate_table_name(&Context::default(), table_name.clone())
                .await
                .context(error::InvalidateTableCacheSnafu)?;

            Ok(Output::new_with_affected_rows(0))
        } else if drop_if_exists {
            // DROP TABLE IF EXISTS meets table not found - ignored
            Ok(Output::new_with_affected_rows(0))
        } else {
            Err(TableNotFoundSnafu {
                table_name: table_name.to_string(),
            }
            .into_error(snafu::NoneError))
        }
    }

    #[tracing::instrument(skip_all)]
    pub async fn truncate_table(&self, table_name: TableName) -> Result<Output> {
        let table = self
            .catalog_manager
            .table(
                &table_name.catalog_name,
                &table_name.schema_name,
                &table_name.table_name,
            )
            .await
            .context(CatalogSnafu)?
            .with_context(|| TableNotFoundSnafu {
                table_name: table_name.to_string(),
            })?;
        let table_id = table.table_info().table_id();
        self.truncate_table_procedure(&table_name, table_id).await?;

        Ok(Output::new_with_affected_rows(0))
    }

    fn verify_alter(
        &self,
        table_id: TableId,
        table_info: Arc<TableInfo>,
        expr: AlterExpr,
    ) -> Result<()> {
        let request: AlterTableRequest = common_grpc_expr::alter_expr_to_request(table_id, expr)
            .context(AlterExprToRequestSnafu)?;

        let AlterTableRequest {
            table_name,
            alter_kind,
            ..
        } = &request;

        if let AlterKind::RenameTable { new_table_name } = alter_kind {
            ensure!(
                NAME_PATTERN_REG.is_match(new_table_name),
                error::UnexpectedSnafu {
                    violated: format!("Invalid table name: {}", new_table_name)
                }
            );
        }

        let _ = table_info
            .meta
            .builder_with_alter_kind(table_name, &request.alter_kind)
            .context(error::TableSnafu)?
            .build()
            .context(error::BuildTableMetaSnafu { table_name })?;

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    pub async fn alter_table(
        &self,
        alter_table: AlterTable,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        let expr = expr_factory::to_alter_expr(alter_table, query_ctx)?;
        self.alter_table_inner(expr).await
    }

    #[tracing::instrument(skip_all)]
    pub async fn alter_table_inner(&self, expr: AlterExpr) -> Result<Output> {
        let catalog_name = if expr.catalog_name.is_empty() {
            DEFAULT_CATALOG_NAME
        } else {
            expr.catalog_name.as_str()
        };

        let schema_name = if expr.schema_name.is_empty() {
            DEFAULT_SCHEMA_NAME
        } else {
            expr.schema_name.as_str()
        };

        let table_name = expr.table_name.as_str();

        let table = self
            .catalog_manager
            .table(catalog_name, schema_name, table_name)
            .await
            .context(CatalogSnafu)?
            .with_context(|| TableNotFoundSnafu {
                table_name: format_full_table_name(catalog_name, schema_name, table_name),
            })?;

        let table_id = table.table_info().ident.table_id;
        self.verify_alter(table_id, table.table_info(), expr.clone())?;

        info!(
            "Table info before alter is {:?}, expr: {:?}",
            table.table_info(),
            expr
        );

        let req = SubmitDdlTaskRequest {
            task: DdlTask::new_alter_table(expr.clone()),
        };

        self.procedure_executor
            .submit_ddl_task(&ExecutorContext::default(), req)
            .await
            .context(error::ExecuteDdlSnafu)?;

        // Invalidates local cache ASAP.
        self.cache_invalidator
            .invalidate_table_id(&Context::default(), table_id)
            .await
            .context(error::InvalidateTableCacheSnafu)?;

        self.cache_invalidator
            .invalidate_table_name(
                &Context::default(),
                TableName::new(catalog_name, schema_name, table_name),
            )
            .await
            .context(error::InvalidateTableCacheSnafu)?;

        Ok(Output::new_with_affected_rows(0))
    }

    async fn create_table_procedure(
        &self,
        create_table: CreateTableExpr,
        partitions: Vec<Partition>,
        table_info: RawTableInfo,
    ) -> Result<SubmitDdlTaskResponse> {
        let partitions = partitions.into_iter().map(Into::into).collect();

        let request = SubmitDdlTaskRequest {
            task: DdlTask::new_create_table(create_table, partitions, table_info),
        };

        self.procedure_executor
            .submit_ddl_task(&ExecutorContext::default(), request)
            .await
            .context(error::ExecuteDdlSnafu)
    }

    async fn create_logical_tables_procedure(
        &self,
        tables_data: Vec<(CreateTableExpr, RawTableInfo)>,
    ) -> Result<SubmitDdlTaskResponse> {
        let request = SubmitDdlTaskRequest {
            task: DdlTask::new_create_logical_tables(tables_data),
        };

        self.procedure_executor
            .submit_ddl_task(&ExecutorContext::default(), request)
            .await
            .context(error::ExecuteDdlSnafu)
    }

    async fn drop_table_procedure(
        &self,
        table_name: &TableName,
        table_id: TableId,
        drop_if_exists: bool,
    ) -> Result<SubmitDdlTaskResponse> {
        let request = SubmitDdlTaskRequest {
            task: DdlTask::new_drop_table(
                table_name.catalog_name.to_string(),
                table_name.schema_name.to_string(),
                table_name.table_name.to_string(),
                table_id,
                drop_if_exists,
            ),
        };

        self.procedure_executor
            .submit_ddl_task(&ExecutorContext::default(), request)
            .await
            .context(error::ExecuteDdlSnafu)
    }

    async fn truncate_table_procedure(
        &self,
        table_name: &TableName,
        table_id: TableId,
    ) -> Result<SubmitDdlTaskResponse> {
        let request = SubmitDdlTaskRequest {
            task: DdlTask::new_truncate_table(
                table_name.catalog_name.to_string(),
                table_name.schema_name.to_string(),
                table_name.table_name.to_string(),
                table_id,
            ),
        };

        self.procedure_executor
            .submit_ddl_task(&ExecutorContext::default(), request)
            .await
            .context(error::ExecuteDdlSnafu)
    }

    #[tracing::instrument(skip_all)]
    pub async fn create_database(
        &self,
        catalog: &str,
        database: &str,
        create_if_not_exists: bool,
    ) -> Result<Output> {
        ensure!(
            NAME_PATTERN_REG.is_match(catalog),
            error::UnexpectedSnafu {
                violated: format!("Invalid catalog name: {}", catalog)
            }
        );

        ensure!(
            NAME_PATTERN_REG.is_match(database),
            error::UnexpectedSnafu {
                violated: format!("Invalid database name: {}", database)
            }
        );

        // TODO(weny): considers executing it in the procedures.
        let schema_key = SchemaNameKey::new(catalog, database);
        let exists = self
            .table_metadata_manager
            .schema_manager()
            .exists(schema_key)
            .await
            .context(TableMetadataManagerSnafu)?;

        if exists {
            return if create_if_not_exists {
                Ok(Output::new_with_affected_rows(1))
            } else {
                error::SchemaExistsSnafu { name: database }.fail()
            };
        }

        self.table_metadata_manager
            .schema_manager()
            .create(schema_key, None, false)
            .await
            .context(TableMetadataManagerSnafu)?;

        Ok(Output::new_with_affected_rows(1))
    }
}

fn validate_partition_columns(
    create_table: &CreateTableExpr,
    partition_cols: &[String],
) -> Result<()> {
    ensure!(
        partition_cols
            .iter()
            .all(|col| &create_table.time_index == col || create_table.primary_keys.contains(col)),
        InvalidPartitionColumnsSnafu {
            table: &create_table.table_name,
            reason: "partition column must belongs to primary keys or equals to time index"
        }
    );
    Ok(())
}

/// Parse partition statement [Partitions] into [MetaPartition] and partition columns.
fn parse_partitions(
    create_table: &CreateTableExpr,
    partitions: Option<Partitions>,
    query_ctx: &QueryContextRef,
) -> Result<(Vec<MetaPartition>, Vec<String>)> {
    // If partitions are not defined by user, use the timestamp column (which has to be existed) as
    // the partition column, and create only one partition.
    let partition_columns = find_partition_columns(&partitions)?;
    let partition_entries =
        find_partition_entries(create_table, &partitions, &partition_columns, query_ctx)?;

    Ok((
        partition_entries
            .into_iter()
            .map(|x| MetaPartition::try_from(PartitionDef::new(partition_columns.clone(), x)))
            .collect::<std::result::Result<_, _>>()
            .context(DeserializePartitionSnafu)?,
        partition_columns,
    ))
}

fn create_table_info(
    create_table: &CreateTableExpr,
    partition_columns: Vec<String>,
    schema_opts: SchemaNameValue,
) -> Result<RawTableInfo> {
    let mut column_schemas = Vec::with_capacity(create_table.column_defs.len());
    let mut column_name_to_index_map = HashMap::new();

    for (idx, column) in create_table.column_defs.iter().enumerate() {
        let schema =
            column_def::try_as_column_schema(column).context(error::InvalidColumnDefSnafu {
                column: &column.name,
            })?;
        let schema = schema.with_time_index(column.name == create_table.time_index);

        column_schemas.push(schema);
        let _ = column_name_to_index_map.insert(column.name.clone(), idx);
    }

    let timestamp_index = column_name_to_index_map
        .get(&create_table.time_index)
        .cloned();

    let raw_schema = RawSchema {
        column_schemas: column_schemas.clone(),
        timestamp_index,
        version: 0,
    };

    let primary_key_indices = create_table
        .primary_keys
        .iter()
        .map(|name| {
            column_name_to_index_map
                .get(name)
                .cloned()
                .context(ColumnNotFoundSnafu { msg: name })
        })
        .collect::<Result<Vec<_>>>()?;

    let partition_key_indices = partition_columns
        .into_iter()
        .map(|col_name| {
            column_name_to_index_map
                .get(&col_name)
                .cloned()
                .context(ColumnNotFoundSnafu { msg: col_name })
        })
        .collect::<Result<Vec<_>>>()?;

    let table_options = TableOptions::try_from(&create_table.table_options)
        .context(UnrecognizedTableOptionSnafu)?;
    let table_options = merge_options(table_options, schema_opts);

    let meta = RawTableMeta {
        schema: raw_schema,
        primary_key_indices,
        value_indices: vec![],
        engine: create_table.engine.clone(),
        next_column_id: column_schemas.len() as u32,
        region_numbers: vec![],
        options: table_options,
        created_on: Utc::now(),
        partition_key_indices,
    };

    let desc = if create_table.desc.is_empty() {
        None
    } else {
        Some(create_table.desc.clone())
    };

    let table_info = RawTableInfo {
        ident: metadata::TableIdent {
            // The table id of distributed table is assigned by Meta, set "0" here as a placeholder.
            table_id: 0,
            version: 0,
        },
        name: create_table.table_name.clone(),
        desc,
        catalog_name: create_table.catalog_name.clone(),
        schema_name: create_table.schema_name.clone(),
        meta,
        table_type: TableType::Base,
    };
    Ok(table_info)
}

fn find_partition_columns(partitions: &Option<Partitions>) -> Result<Vec<String>> {
    let columns = if let Some(partitions) = partitions {
        partitions
            .column_list
            .iter()
            .map(|x| x.value.clone())
            .collect::<Vec<_>>()
    } else {
        vec![]
    };
    Ok(columns)
}

/// Parse [Partitions] into a group of partition entries.
///
/// Returns a list of [PartitionBound], each of which defines a partition.
fn find_partition_entries(
    create_table: &CreateTableExpr,
    partitions: &Option<Partitions>,
    partition_columns: &[String],
    query_ctx: &QueryContextRef,
) -> Result<Vec<Vec<PartitionBound>>> {
    let entries = if let Some(partitions) = partitions {
        // extract concrete data type of partition columns
        let column_defs = partition_columns
            .iter()
            .map(|pc| {
                create_table
                    .column_defs
                    .iter()
                    .find(|c| &c.name == pc)
                    // unwrap is safe here because we have checked that partition columns are defined
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let mut column_name_and_type = HashMap::with_capacity(column_defs.len());
        for column in column_defs {
            let column_name = &column.name;
            let data_type = ConcreteDataType::from(
                ColumnDataTypeWrapper::try_new(column.data_type, column.datatype_extension.clone())
                    .context(ColumnDataTypeSnafu)?,
            );
            column_name_and_type.insert(column_name, data_type);
        }

        // Transform parser expr to partition expr
        let mut partition_exprs = Vec::with_capacity(partitions.exprs.len());
        for partition in &partitions.exprs {
            let partition_expr =
                convert_one_expr(partition, &column_name_and_type, &query_ctx.timezone())?;
            partition_exprs.push(vec![PartitionBound::Expr(partition_expr)]);
        }

        // fallback for no expr
        if partition_exprs.is_empty() {
            partition_exprs.push(vec![PartitionBound::MaxValue]);
        }

        partition_exprs
    } else {
        vec![vec![PartitionBound::MaxValue]]
    };
    Ok(entries)
}

fn convert_one_expr(
    expr: &Expr,
    column_name_and_type: &HashMap<&String, ConcreteDataType>,
    timezone: &Timezone,
) -> Result<PartitionExpr> {
    let Expr::BinaryOp { left, op, right } = expr else {
        return InvalidPartitionRuleSnafu {
            reason: "partition rule must be a binary expression",
        }
        .fail();
    };

    let op =
        RestrictedOp::try_from_parser(&op.clone()).with_context(|| InvalidPartitionRuleSnafu {
            reason: format!("unsupported operator in partition expr {op}"),
        })?;

    // convert leaf node.
    let (lhs, op, rhs) = match (left.as_ref(), right.as_ref()) {
        (Expr::Identifier(ident), Expr::Value(value)) => {
            let (column_name, data_type) = convert_identifier(ident, column_name_and_type)?;
            let value = convert_value(value, data_type, timezone)?;
            (Operand::Column(column_name), op, Operand::Value(value))
        }
        (Expr::Value(value), Expr::Identifier(ident)) => {
            let (column_name, data_type) = convert_identifier(ident, column_name_and_type)?;
            let value = convert_value(value, data_type, timezone)?;
            (Operand::Value(value), op, Operand::Column(column_name))
        }
        (Expr::BinaryOp { .. }, Expr::BinaryOp { .. }) => {
            // sub-expr must against another sub-expr
            let lhs = convert_one_expr(left, column_name_and_type, timezone)?;
            let rhs = convert_one_expr(right, column_name_and_type, timezone)?;
            (Operand::Expr(lhs), op, Operand::Expr(rhs))
        }
        _ => {
            return InvalidPartitionRuleSnafu {
                reason: format!("invalid partition expr {expr}"),
            }
            .fail();
        }
    };

    Ok(PartitionExpr::new(lhs, op, rhs))
}

fn convert_identifier(
    ident: &Ident,
    column_name_and_type: &HashMap<&String, ConcreteDataType>,
) -> Result<(String, ConcreteDataType)> {
    let column_name = ident.value.clone();
    let data_type = column_name_and_type
        .get(&column_name)
        .cloned()
        .with_context(|| ColumnNotFoundSnafu { msg: &column_name })?;
    Ok((column_name, data_type))
}

fn convert_value(
    value: &ParserValue,
    data_type: ConcreteDataType,
    timezone: &Timezone,
) -> Result<Value> {
    sql_value_to_value("<NONAME>", &data_type, value, Some(timezone)).context(ParseSqlValueSnafu)
}

/// Merge table level table options with schema level table options.
fn merge_options(mut table_opts: TableOptions, schema_opts: SchemaNameValue) -> TableOptions {
    table_opts.ttl = table_opts.ttl.or(schema_opts.ttl);
    table_opts
}

#[cfg(test)]
mod test {
    use session::context::{QueryContext, QueryContextBuilder};
    use sql::dialect::GreptimeDbDialect;
    use sql::parser::{ParseOptions, ParserContext};
    use sql::statements::statement::Statement;

    use super::*;
    use crate::expr_factory;

    #[test]
    fn test_name_is_match() {
        assert!(!NAME_PATTERN_REG.is_match("/adaf"));
        assert!(!NAME_PATTERN_REG.is_match("🈲"));
        assert!(NAME_PATTERN_REG.is_match("hello"));
    }

    #[test]
    fn test_validate_partition_columns() {
        let create_table = CreateTableExpr {
            table_name: "my_table".to_string(),
            time_index: "ts".to_string(),
            primary_keys: vec!["a".to_string(), "b".to_string()],
            ..Default::default()
        };

        assert!(validate_partition_columns(&create_table, &[]).is_ok());
        assert!(validate_partition_columns(&create_table, &["ts".to_string()]).is_ok());
        assert!(validate_partition_columns(&create_table, &["a".to_string()]).is_ok());
        assert!(
            validate_partition_columns(&create_table, &["b".to_string(), "a".to_string()]).is_ok()
        );

        assert_eq!(
            validate_partition_columns(&create_table, &["a".to_string(), "c".to_string()])
                .unwrap_err()
                .to_string(),
            "Invalid partition columns when creating table 'my_table', \
            reason: partition column must belongs to primary keys or equals to time index",
        );
    }

    #[tokio::test]
    #[ignore = "TODO(ruihang): WIP new partition rule"]
    async fn test_parse_partitions() {
        common_telemetry::init_default_ut_logging();
        let cases = [
            (
                r"
CREATE TABLE rcx ( a INT, b STRING, c TIMESTAMP, TIME INDEX (c) )
PARTITION ON COLUMNS (b) (
  b < 'hz',
  b >= 'hz' AND b < 'sh',
  b >= 'sh'
)
ENGINE=mito",
                r#"[{"column_list":["b"],"value_list":["{\"Value\":{\"String\":\"hz\"}}"]},{"column_list":["b"],"value_list":["{\"Value\":{\"String\":\"sh\"}}"]},{"column_list":["b"],"value_list":["\"MaxValue\""]}]"#,
            ),
            (
                r"
CREATE TABLE rcx ( a INT, b STRING, c TIMESTAMP, TIME INDEX (c) )
PARTITION BY RANGE COLUMNS (b, a) (
  PARTITION r0 VALUES LESS THAN ('hz', 10),
  b < 'hz' AND a < 10,
  b >= 'hz' AND b < 'sh' AND a >= 10 AND a < 20,
  b >= 'sh' AND a >= 20
)
ENGINE=mito",
                r#"[{"column_list":["b","a"],"value_list":["{\"Value\":{\"String\":\"hz\"}}","{\"Value\":{\"Int32\":10}}"]},{"column_list":["b","a"],"value_list":["{\"Value\":{\"String\":\"sh\"}}","{\"Value\":{\"Int32\":20}}"]},{"column_list":["b","a"],"value_list":["\"MaxValue\"","\"MaxValue\""]}]"#,
            ),
        ];
        let ctx = QueryContextBuilder::default().build();
        for (sql, expected) in cases {
            let result = ParserContext::create_with_dialect(
                sql,
                &GreptimeDbDialect {},
                ParseOptions::default(),
            )
            .unwrap();
            match &result[0] {
                Statement::CreateTable(c) => {
                    let expr = expr_factory::create_to_expr(c, QueryContext::arc()).unwrap();
                    let (partitions, _) =
                        parse_partitions(&expr, c.partitions.clone(), &ctx).unwrap();
                    let json = serde_json::to_string(&partitions).unwrap();
                    assert_eq!(json, expected);
                }
                _ => unreachable!(),
            }
        }
    }
}
