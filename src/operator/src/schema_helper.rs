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

//! Utilities to deal with table schemas.

use std::collections::HashMap;
use std::sync::Arc;

use api::v1::alter_table_expr::Kind;
use api::v1::{AlterTableExpr, ColumnDataType, ColumnSchema, CreateTableExpr, SemanticType};
use catalog::CatalogManagerRef;
use common_catalog::consts::{
    default_engine, is_readonly_schema, DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME,
};
use common_catalog::format_full_table_name;
use common_grpc_expr::util::ColumnExpr;
use common_meta::cache_invalidator::{CacheInvalidatorRef, Context};
use common_meta::ddl::{ExecutorContext, ProcedureExecutorRef};
use common_meta::instruction::CacheIdent;
use common_meta::key::schema_name::SchemaNameKey;
use common_meta::key::TableMetadataManagerRef;
use common_meta::rpc::ddl::{DdlTask, SubmitDdlTaskRequest, SubmitDdlTaskResponse};
use common_meta::rpc::router::Partition;
use common_query::prelude::{GREPTIME_TIMESTAMP, GREPTIME_VALUE};
use common_query::Output;
use common_telemetry::tracing;
use session::context::QueryContextRef;
use snafu::{ensure, OptionExt, ResultExt};
use sql::statements::create::Partitions;
use store_api::metric_engine_consts::{
    LOGICAL_TABLE_METADATA_KEY, METRIC_ENGINE_NAME, PHYSICAL_TABLE_METADATA_KEY,
};
use table::dist_table::DistTable;
use table::metadata::{RawTableInfo, TableId, TableInfo};
use table::table_name::TableName;
use table::table_reference::TableReference;
use table::TableRef;

use crate::error::{
    CatalogSnafu, CreateLogicalTablesSnafu, CreateTableInfoSnafu, EmptyDdlExprSnafu,
    ExecuteDdlSnafu, InvalidPartitionRuleSnafu, InvalidTableNameSnafu, InvalidateTableCacheSnafu,
    Result, SchemaNotFoundSnafu, SchemaReadOnlySnafu, TableAlreadyExistsSnafu,
    TableMetadataManagerSnafu, TableNotFoundSnafu, UnexpectedSnafu,
};
use crate::expr_helper;
use crate::insert::{build_create_table_expr, fill_table_options_for_create, AutoCreateTableType};
use crate::statement::ddl::{create_table_info, parse_partitions, verify_alter, NAME_PATTERN_REG};

/// Helper to query and manipulate (CREATE/ALTER) table schemas.
#[derive(Clone)]
pub struct SchemaHelper {
    catalog_manager: CatalogManagerRef,
    table_metadata_manager: TableMetadataManagerRef,
    procedure_executor: ProcedureExecutorRef,
    cache_invalidator: CacheInvalidatorRef,
}

impl SchemaHelper {
    /// Creates a new [`SchemaHelper`].
    pub fn new(
        catalog_manager: CatalogManagerRef,
        table_metadata_manager: TableMetadataManagerRef,
        procedure_executor: ProcedureExecutorRef,
        cache_invalidator: CacheInvalidatorRef,
    ) -> Self {
        Self {
            catalog_manager,
            table_metadata_manager,
            procedure_executor,
            cache_invalidator,
        }
    }

    /// Gets the table by catalog, schema and table name.
    pub async fn get_table(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<Option<TableRef>> {
        self.catalog_manager
            .table(catalog, schema, table, None)
            .await
            .context(CatalogSnafu)
    }

    // TODO(yingwen): Can we create the physical table with all columns from the prometheus metrics?
    /// Creates a physical table for metric engine.
    ///
    /// If table already exists, do nothing.
    pub async fn create_metric_physical_table(
        &self,
        ctx: &QueryContextRef,
        physical_table: String,
    ) -> Result<()> {
        let catalog_name = ctx.current_catalog();
        let schema_name = ctx.current_schema();

        // check if exist
        if self
            .get_table(catalog_name, &schema_name, &physical_table)
            .await?
            .is_some()
        {
            return Ok(());
        }

        let table_reference = TableReference::full(catalog_name, &schema_name, &physical_table);
        common_telemetry::info!(
            "Physical metric table `{table_reference}` does not exist, try creating table"
        );

        // schema with timestamp and field column
        let default_schema = vec![
            ColumnSchema {
                column_name: GREPTIME_TIMESTAMP.to_string(),
                datatype: ColumnDataType::TimestampMillisecond as _,
                semantic_type: SemanticType::Timestamp as _,
                datatype_extension: None,
                options: None,
            },
            ColumnSchema {
                column_name: GREPTIME_VALUE.to_string(),
                datatype: ColumnDataType::Float64 as _,
                semantic_type: SemanticType::Field as _,
                datatype_extension: None,
                options: None,
            },
        ];
        let create_table_expr =
            &mut build_create_table_expr(&table_reference, &default_schema, default_engine())?;
        create_table_expr.engine = METRIC_ENGINE_NAME.to_string();
        create_table_expr
            .table_options
            .insert(PHYSICAL_TABLE_METADATA_KEY.to_string(), "true".to_string());

        // create physical table.
        // TODO(yingwen): Simplify this function. But remember to start the timer.
        let res = self
            .create_table_by_expr(create_table_expr, None, ctx.clone())
            .await;
        match res {
            Ok(_) => {
                common_telemetry::info!("Successfully created table {table_reference}",);
                Ok(())
            }
            Err(err) => {
                common_telemetry::error!(err; "Failed to create table {table_reference}");
                Err(err)
            }
        }
    }

    /// Creates a table by [CreateTableExpr].
    #[tracing::instrument(skip_all)]
    pub async fn create_table_by_expr(
        &self,
        create_table: &mut CreateTableExpr,
        partitions: Option<Partitions>,
        query_ctx: QueryContextRef,
    ) -> Result<TableRef> {
        ensure!(
            !is_readonly_schema(&create_table.schema_name),
            SchemaReadOnlySnafu {
                name: create_table.schema_name.clone()
            }
        );

        if create_table.engine == METRIC_ENGINE_NAME
            && create_table
                .table_options
                .contains_key(LOGICAL_TABLE_METADATA_KEY)
        {
            // Create logical tables
            ensure!(
                partitions.is_none(),
                InvalidPartitionRuleSnafu {
                    reason: "logical table in metric engine should not have partition rule, it will be inherited from physical table",
                }
            );
            self.create_logical_tables(std::slice::from_ref(create_table), query_ctx)
                .await?
                .into_iter()
                .next()
                .context(UnexpectedSnafu {
                    violated: "expected to create logical tables",
                })
        } else {
            // Create other normal table
            self.create_non_logic_table(create_table, partitions, query_ctx)
                .await
        }
    }

    /// Creates a non-logical table.
    /// - If the schema doesn't exist, returns an error
    /// - If the table already exists:
    ///   - If `create_if_not_exists` is true, returns the existing table
    ///   - If `create_if_not_exists` is false, returns an error
    #[tracing::instrument(skip_all)]
    pub async fn create_non_logic_table(
        &self,
        create_table: &mut CreateTableExpr,
        partitions: Option<Partitions>,
        query_ctx: QueryContextRef,
    ) -> Result<TableRef> {
        let _timer = crate::metrics::DIST_CREATE_TABLE.start_timer();

        // Check if schema exists
        let schema = self
            .table_metadata_manager
            .schema_manager()
            .get(SchemaNameKey::new(
                &create_table.catalog_name,
                &create_table.schema_name,
            ))
            .await
            .context(TableMetadataManagerSnafu)?;
        ensure!(
            schema.is_some(),
            SchemaNotFoundSnafu {
                schema_info: &create_table.schema_name,
            }
        );

        // if table exists.
        if let Some(table) = self
            .catalog_manager
            .table(
                &create_table.catalog_name,
                &create_table.schema_name,
                &create_table.table_name,
                Some(&query_ctx),
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
                table_name: &create_table.table_name,
            }
        );

        let table_name = TableName::new(
            &create_table.catalog_name,
            &create_table.schema_name,
            &create_table.table_name,
        );

        let (partitions, partition_cols) = parse_partitions(create_table, partitions, &query_ctx)?;
        let mut table_info = create_table_info(create_table, partition_cols)?;

        let resp = self
            .create_table_procedure(
                create_table.clone(),
                partitions,
                table_info.clone(),
                query_ctx,
            )
            .await?;

        let table_id = resp.table_ids.into_iter().next().context(UnexpectedSnafu {
            violated: "expected table_id",
        })?;
        common_telemetry::info!(
            "Successfully created table '{table_name}' with table id {table_id}"
        );

        table_info.ident.table_id = table_id;

        let table_info: Arc<TableInfo> =
            Arc::new(table_info.try_into().context(CreateTableInfoSnafu)?);
        create_table.table_id = Some(api::v1::TableId { id: table_id });

        let table = DistTable::table(table_info);

        Ok(table)
    }

    /// Creates logical tables.
    #[tracing::instrument(skip_all)]
    pub async fn create_logical_tables(
        &self,
        create_table_exprs: &[CreateTableExpr],
        query_context: QueryContextRef,
    ) -> Result<Vec<TableRef>> {
        let _timer = crate::metrics::DIST_CREATE_TABLES.start_timer();
        ensure!(
            !create_table_exprs.is_empty(),
            EmptyDdlExprSnafu {
                name: "create logic tables"
            }
        );

        // Check table names
        for create_table in create_table_exprs {
            ensure!(
                NAME_PATTERN_REG.is_match(&create_table.table_name),
                InvalidTableNameSnafu {
                    table_name: &create_table.table_name,
                }
            );
        }

        let mut raw_tables_info = create_table_exprs
            .iter()
            .map(|create| create_table_info(create, vec![]))
            .collect::<Result<Vec<_>>>()?;
        let tables_data = create_table_exprs
            .iter()
            .cloned()
            .zip(raw_tables_info.iter().cloned())
            .collect::<Vec<_>>();

        let resp = self
            .create_logical_tables_procedure(tables_data, query_context)
            .await?;

        let table_ids = resp.table_ids;
        ensure!(table_ids.len() == raw_tables_info.len(), CreateLogicalTablesSnafu {
            reason: format!("The number of tables is inconsistent with the expected number to be created, expected: {}, actual: {}", raw_tables_info.len(), table_ids.len())
        });
        common_telemetry::info!("Successfully created logical tables: {:?}", table_ids);

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

    /// Alters a table by [AlterTableExpr].
    #[tracing::instrument(skip_all)]
    pub async fn alter_table_by_expr(
        &self,
        expr: AlterTableExpr,
        query_context: QueryContextRef,
    ) -> Result<Output> {
        ensure!(
            !is_readonly_schema(&expr.schema_name),
            SchemaReadOnlySnafu {
                name: expr.schema_name.clone()
            }
        );

        let catalog_name = if expr.catalog_name.is_empty() {
            DEFAULT_CATALOG_NAME.to_string()
        } else {
            expr.catalog_name.clone()
        };

        let schema_name = if expr.schema_name.is_empty() {
            DEFAULT_SCHEMA_NAME.to_string()
        } else {
            expr.schema_name.clone()
        };

        let table_name = expr.table_name.clone();

        let table = self
            .catalog_manager
            .table(
                &catalog_name,
                &schema_name,
                &table_name,
                Some(&query_context),
            )
            .await
            .context(CatalogSnafu)?
            .with_context(|| TableNotFoundSnafu {
                table_name: format_full_table_name(&catalog_name, &schema_name, &table_name),
            })?;

        let table_id = table.table_info().ident.table_id;
        let need_alter = verify_alter(table_id, table.table_info(), expr.clone())?;
        if !need_alter {
            return Ok(Output::new_with_affected_rows(0));
        }
        common_telemetry::info!(
            "Table info before alter is {:?}, expr: {:?}",
            table.table_info(),
            expr
        );

        let physical_table_id = self
            .table_metadata_manager
            .table_route_manager()
            .get_physical_table_id(table_id)
            .await
            .context(TableMetadataManagerSnafu)?;

        let (req, invalidate_keys) = if physical_table_id == table_id {
            // This is physical table
            let req = SubmitDdlTaskRequest {
                query_context,
                task: DdlTask::new_alter_table(expr),
            };

            let invalidate_keys = vec![
                CacheIdent::TableId(table_id),
                CacheIdent::TableName(TableName::new(catalog_name, schema_name, table_name)),
            ];

            (req, invalidate_keys)
        } else {
            // This is logical table
            let req = SubmitDdlTaskRequest {
                query_context,
                task: DdlTask::new_alter_logical_tables(vec![expr]),
            };

            let mut invalidate_keys = vec![
                CacheIdent::TableId(physical_table_id),
                CacheIdent::TableId(table_id),
                CacheIdent::TableName(TableName::new(catalog_name, schema_name, table_name)),
            ];

            let physical_table = self
                .table_metadata_manager
                .table_info_manager()
                .get(physical_table_id)
                .await
                .context(TableMetadataManagerSnafu)?
                .map(|x| x.into_inner());
            if let Some(physical_table) = physical_table {
                let physical_table_name = TableName::new(
                    physical_table.table_info.catalog_name,
                    physical_table.table_info.schema_name,
                    physical_table.table_info.name,
                );
                invalidate_keys.push(CacheIdent::TableName(physical_table_name));
            }

            (req, invalidate_keys)
        };

        self.procedure_executor
            .submit_ddl_task(&ExecutorContext::default(), req)
            .await
            .context(ExecuteDdlSnafu)?;

        // Invalidates local cache ASAP.
        self.cache_invalidator
            .invalidate(&Context::default(), &invalidate_keys)
            .await
            .context(InvalidateTableCacheSnafu)?;

        Ok(Output::new_with_affected_rows(0))
    }

    /// Alter logical tables.
    pub async fn alter_logical_tables(
        &self,
        alter_table_exprs: Vec<AlterTableExpr>,
        query_context: QueryContextRef,
    ) -> Result<Output> {
        let _timer = crate::metrics::DIST_ALTER_TABLES.start_timer();
        ensure!(
            !alter_table_exprs.is_empty(),
            EmptyDdlExprSnafu {
                name: "alter logical tables"
            }
        );

        // group by physical table id
        let mut groups: HashMap<TableId, Vec<AlterTableExpr>> = HashMap::new();
        for expr in alter_table_exprs {
            // Get table_id from catalog_manager
            let catalog = if expr.catalog_name.is_empty() {
                query_context.current_catalog()
            } else {
                &expr.catalog_name
            };
            let schema = if expr.schema_name.is_empty() {
                query_context.current_schema()
            } else {
                expr.schema_name.to_string()
            };
            let table_name = &expr.table_name;
            let table = self
                .catalog_manager
                .table(catalog, &schema, table_name, Some(&query_context))
                .await
                .context(CatalogSnafu)?
                .with_context(|| TableNotFoundSnafu {
                    table_name: format_full_table_name(catalog, &schema, table_name),
                })?;
            let table_id = table.table_info().ident.table_id;
            let physical_table_id = self
                .table_metadata_manager
                .table_route_manager()
                .get_physical_table_id(table_id)
                .await
                .context(TableMetadataManagerSnafu)?;
            groups.entry(physical_table_id).or_default().push(expr);
        }

        // Submit procedure for each physical table
        let mut handles = Vec::with_capacity(groups.len());
        for (_physical_table_id, exprs) in groups {
            let fut = self.alter_logical_tables_procedure(exprs, query_context.clone());
            handles.push(fut);
        }
        let _results = futures::future::try_join_all(handles).await?;

        Ok(Output::new_with_affected_rows(0))
    }

    /// Returns the catalog manager.
    pub(crate) fn catalog_manager(&self) -> &CatalogManagerRef {
        &self.catalog_manager
    }

    /// Submits a procedure to create a non-logical table.
    async fn create_table_procedure(
        &self,
        create_table: CreateTableExpr,
        partitions: Vec<Partition>,
        table_info: RawTableInfo,
        query_context: QueryContextRef,
    ) -> Result<SubmitDdlTaskResponse> {
        let partitions = partitions.into_iter().map(Into::into).collect();

        let request = SubmitDdlTaskRequest {
            query_context,
            task: DdlTask::new_create_table(create_table, partitions, table_info),
        };

        self.procedure_executor
            .submit_ddl_task(&ExecutorContext::default(), request)
            .await
            .context(ExecuteDdlSnafu)
    }

    /// Submits a procedure to create logical tables.
    async fn create_logical_tables_procedure(
        &self,
        tables_data: Vec<(CreateTableExpr, RawTableInfo)>,
        query_context: QueryContextRef,
    ) -> Result<SubmitDdlTaskResponse> {
        let request = SubmitDdlTaskRequest {
            query_context,
            task: DdlTask::new_create_logical_tables(tables_data),
        };

        self.procedure_executor
            .submit_ddl_task(&ExecutorContext::default(), request)
            .await
            .context(ExecuteDdlSnafu)
    }

    /// Submits a procedure to alter logical tables.
    async fn alter_logical_tables_procedure(
        &self,
        tables_data: Vec<AlterTableExpr>,
        query_context: QueryContextRef,
    ) -> Result<SubmitDdlTaskResponse> {
        let request = SubmitDdlTaskRequest {
            query_context,
            task: DdlTask::new_alter_logical_tables(tables_data),
        };

        self.procedure_executor
            .submit_ddl_task(&ExecutorContext::default(), request)
            .await
            .context(ExecuteDdlSnafu)
    }
}

/// Schema of a logical table.
struct LogicalSchema {
    /// Name of the logical table.
    name: String,
    /// Schema of columns in the logical table.
    columns: Vec<ColumnSchema>,
}

/// Logical table schemas.
struct LogicalSchemas {
    /// Logical table schemas group by physical table name.
    schemas: HashMap<String, Vec<LogicalSchema>>,
}

/// Creates or alters logical tables to match the provided schemas
/// for prometheus metrics.
async fn ensure_logical_tables_for_metrics(
    helper: &SchemaHelper,
    schemas: &LogicalSchemas,
    query_ctx: &QueryContextRef,
) -> Result<()> {
    // 1. For each physical table, creates it if it doesn't exist.
    for (physical_table_name, _) in &schemas.schemas {
        // Check if the physical table exists and create it if it doesn't
        let physical_table_opt = helper
            .get_table(
                &query_ctx.current_catalog(),
                &query_ctx.current_schema(),
                physical_table_name,
            )
            .await?;

        if physical_table_opt.is_none() {
            // Physical table doesn't exist, create it
            helper
                .create_metric_physical_table(query_ctx, physical_table_name.clone())
                .await?;
        }
    }

    // 2. Collects logical tables that do not exist. (CreateTableExpr)
    let mut tables_to_create: Vec<CreateTableExpr> = Vec::new();

    // 3. Collects alterations (columns to add) for each logical table. (AlterTableExpr)
    let mut tables_to_alter: Vec<AlterTableExpr> = Vec::new();

    let catalog_name = query_ctx.current_catalog();
    let schema_name = query_ctx.current_schema();
    // Process each logical table to determine if it needs to be created or altered
    for (physical_table_name, logical_schemas) in &schemas.schemas {
        for logical_schema in logical_schemas {
            let table_name = &logical_schema.name;

            // Check if the logical table exists
            let table_opt = helper
                .get_table(catalog_name, &schema_name, table_name)
                .await?;

            if let Some(existing_table) = table_opt {
                // Logical table exists, determine if it needs alteration
                let existing_schema = existing_table.schema();
                let column_exprs = ColumnExpr::from_column_schemas(&logical_schema.columns);
                let add_columns =
                    expr_helper::extract_add_columns_expr(&existing_schema, column_exprs)?;
                let Some(add_columns) = add_columns else {
                    continue;
                };

                let alter_expr = AlterTableExpr {
                    catalog_name: catalog_name.to_string(),
                    schema_name: schema_name.clone(),
                    table_name: table_name.to_string(),
                    kind: Some(Kind::AddColumns(add_columns)),
                };
                tables_to_alter.push(alter_expr);
            } else {
                // Logical table doesn't exist, prepare for creation
                // Build a CreateTableExpr from the table reference and columns
                let table_ref = TableReference::full(catalog_name, &schema_name, table_name);
                let mut create_expr = build_create_table_expr(
                    &table_ref,
                    &logical_schema.columns,
                    METRIC_ENGINE_NAME,
                )?;
                create_expr.create_if_not_exists = true;
                let create_type = AutoCreateTableType::Logical(physical_table_name.clone());
                // Fill table options.
                fill_table_options_for_create(
                    &mut create_expr.table_options,
                    &create_type,
                    query_ctx,
                );

                tables_to_create.push(create_expr);
            }
        }
    }

    // 4. Creates logical tables in batch using `create_logical_tables()`.
    if !tables_to_create.is_empty() {
        helper
            .create_logical_tables(&tables_to_create, query_ctx.clone())
            .await?;
    }

    // 5. Alters logical tables in batch using `alter_logical_tables()`.
    if !tables_to_alter.is_empty() {
        helper
            .alter_logical_tables(tables_to_alter, query_ctx.clone())
            .await?;
    }

    Ok(())
}
