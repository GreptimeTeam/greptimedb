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
use api::v1::meta::CreateFlowTask as PbCreateFlowTask;
use api::v1::{
    column_def, AlterDatabaseExpr, AlterTableExpr, CreateFlowExpr, CreateTableExpr, CreateViewExpr,
};
#[cfg(feature = "enterprise")]
use api::v1::{
    meta::CreateTriggerTask as PbCreateTriggerTask, CreateTriggerExpr as PbCreateTriggerExpr,
};
use catalog::CatalogManagerRef;
use chrono::Utc;
use common_catalog::consts::{is_readonly_schema, DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_catalog::{format_full_flow_name, format_full_table_name};
use common_error::ext::BoxedError;
use common_meta::cache_invalidator::Context;
use common_meta::ddl::create_flow::FlowType;
use common_meta::instruction::CacheIdent;
use common_meta::key::schema_name::{SchemaName, SchemaNameKey};
use common_meta::key::NAME_PATTERN;
use common_meta::procedure_executor::ExecutorContext;
#[cfg(feature = "enterprise")]
use common_meta::rpc::ddl::trigger::CreateTriggerTask;
#[cfg(feature = "enterprise")]
use common_meta::rpc::ddl::trigger::DropTriggerTask;
use common_meta::rpc::ddl::{
    CreateFlowTask, DdlTask, DropFlowTask, DropViewTask, SubmitDdlTaskRequest,
    SubmitDdlTaskResponse,
};
use common_query::Output;
use common_sql::convert::sql_value_to_value;
use common_telemetry::{debug, info, tracing, warn};
use common_time::{Timestamp, Timezone};
use datafusion_common::tree_node::TreeNodeVisitor;
use datafusion_expr::LogicalPlan;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{RawSchema, Schema};
use datatypes::value::Value;
use lazy_static::lazy_static;
use partition::expr::{Operand, PartitionExpr, RestrictedOp};
use partition::multi_dim::MultiDimPartitionRule;
use query::parser::QueryStatement;
use query::plan::extract_and_rewrite_full_table_names;
use query::query_engine::DefaultSerializer;
use query::sql::create_table_stmt;
use regex::Regex;
use session::context::QueryContextRef;
use session::table_name::table_idents_to_full_name;
use snafu::{ensure, OptionExt, ResultExt};
use sql::parser::{ParseOptions, ParserContext};
#[cfg(feature = "enterprise")]
use sql::statements::alter::trigger::AlterTrigger;
use sql::statements::alter::{AlterDatabase, AlterTable};
#[cfg(feature = "enterprise")]
use sql::statements::create::trigger::CreateTrigger;
use sql::statements::create::{
    CreateExternalTable, CreateFlow, CreateTable, CreateTableLike, CreateView, Partitions,
};
use sql::statements::statement::Statement;
use sqlparser::ast::{Expr, Ident, UnaryOperator, Value as ParserValue};
use store_api::metric_engine_consts::{LOGICAL_TABLE_METADATA_KEY, METRIC_ENGINE_NAME};
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};
use table::dist_table::DistTable;
use table::metadata::{self, RawTableInfo, RawTableMeta, TableId, TableInfo, TableType};
use table::requests::{AlterKind, AlterTableRequest, TableOptions, COMMENT_KEY};
use table::table_name::TableName;
use table::TableRef;

use crate::error::{
    self, AlterExprToRequestSnafu, BuildDfLogicalPlanSnafu, CatalogSnafu, ColumnDataTypeSnafu,
    ColumnNotFoundSnafu, ConvertSchemaSnafu, CreateLogicalTablesSnafu, CreateTableInfoSnafu,
    EmptyDdlExprSnafu, ExternalSnafu, ExtractTableNamesSnafu, FlowNotFoundSnafu,
    InvalidPartitionRuleSnafu, InvalidPartitionSnafu, InvalidSqlSnafu, InvalidTableNameSnafu,
    InvalidViewNameSnafu, InvalidViewStmtSnafu, PartitionExprToPbSnafu, Result, SchemaInUseSnafu,
    SchemaNotFoundSnafu, SchemaReadOnlySnafu, SubstraitCodecSnafu, TableAlreadyExistsSnafu,
    TableMetadataManagerSnafu, TableNotFoundSnafu, UnrecognizedTableOptionSnafu,
    ViewAlreadyExistsSnafu,
};
use crate::expr_helper;
use crate::statement::show::create_partitions_stmt;
use crate::statement::StatementExecutor;

lazy_static! {
    pub static ref NAME_PATTERN_REG: Regex = Regex::new(&format!("^{NAME_PATTERN}$")).unwrap();
}

impl StatementExecutor {
    pub fn catalog_manager(&self) -> CatalogManagerRef {
        self.catalog_manager.clone()
    }

    #[tracing::instrument(skip_all)]
    pub async fn create_table(&self, stmt: CreateTable, ctx: QueryContextRef) -> Result<TableRef> {
        let (catalog, schema, _table) = table_idents_to_full_name(&stmt.name, &ctx)
            .map_err(BoxedError::new)
            .context(error::ExternalSnafu)?;

        let schema_options = self
            .table_metadata_manager
            .schema_manager()
            .get(SchemaNameKey {
                catalog: &catalog,
                schema: &schema,
            })
            .await
            .context(TableMetadataManagerSnafu)?
            .map(|v| v.into_inner());

        let create_expr = &mut expr_helper::create_to_expr(&stmt, &ctx)?;
        // We don't put ttl into the table options
        // Because it will be used directly while compaction.
        if let Some(schema_options) = schema_options {
            for (key, value) in schema_options.extra_options.iter() {
                create_expr
                    .table_options
                    .entry(key.clone())
                    .or_insert(value.clone());
            }
        }

        self.create_table_inner(create_expr, stmt.partitions, ctx)
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
            .table(&catalog, &schema, &table, Some(&ctx))
            .await
            .context(CatalogSnafu)?
            .context(TableNotFoundSnafu { table_name: &table })?;
        let partitions = self
            .partition_manager
            .find_table_partitions(table_ref.table_info().table_id())
            .await
            .context(error::FindTablePartitionRuleSnafu { table_name: table })?;

        // CREATE TABLE LIKE also inherits database level options.
        let schema_options = self
            .table_metadata_manager
            .schema_manager()
            .get(SchemaNameKey {
                catalog: &catalog,
                schema: &schema,
            })
            .await
            .context(TableMetadataManagerSnafu)?
            .map(|v| v.into_inner());

        let quote_style = ctx.quote_style();
        let mut create_stmt =
            create_table_stmt(&table_ref.table_info(), schema_options, quote_style)
                .context(error::ParseQuerySnafu)?;
        create_stmt.name = stmt.table_name;
        create_stmt.if_not_exists = false;

        let table_info = table_ref.table_info();
        let partitions =
            create_partitions_stmt(&table_info, partitions)?.and_then(|mut partitions| {
                if !partitions.column_list.is_empty() {
                    partitions.set_quote(quote_style);
                    Some(partitions)
                } else {
                    None
                }
            });

        let create_expr = &mut expr_helper::create_to_expr(&create_stmt, &ctx)?;
        self.create_table_inner(create_expr, partitions, ctx).await
    }

    #[tracing::instrument(skip_all)]
    pub async fn create_external_table(
        &self,
        create_expr: CreateExternalTable,
        ctx: QueryContextRef,
    ) -> Result<TableRef> {
        let create_expr = &mut expr_helper::create_external_expr(create_expr, &ctx).await?;
        self.create_table_inner(create_expr, None, ctx).await
    }

    #[tracing::instrument(skip_all)]
    pub async fn create_table_inner(
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
                .context(error::UnexpectedSnafu {
                    violated: "expected to create logical tables",
                })
        } else {
            // Create other normal table
            self.create_non_logic_table(create_table, partitions, query_ctx)
                .await
        }
    }

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

        let table_id = resp
            .table_ids
            .into_iter()
            .next()
            .context(error::UnexpectedSnafu {
                violated: "expected table_id",
            })?;
        info!("Successfully created table '{table_name}' with table id {table_id}");

        table_info.ident.table_id = table_id;

        let table_info: Arc<TableInfo> =
            Arc::new(table_info.try_into().context(CreateTableInfoSnafu)?);
        create_table.table_id = Some(api::v1::TableId { id: table_id });

        let table = DistTable::table(table_info);

        Ok(table)
    }

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

    #[cfg(feature = "enterprise")]
    #[tracing::instrument(skip_all)]
    pub async fn create_trigger(
        &self,
        stmt: CreateTrigger,
        query_context: QueryContextRef,
    ) -> Result<Output> {
        let expr = expr_helper::to_create_trigger_task_expr(stmt, &query_context)?;
        self.create_trigger_inner(expr, query_context).await
    }

    #[cfg(feature = "enterprise")]
    pub async fn create_trigger_inner(
        &self,
        expr: PbCreateTriggerExpr,
        query_context: QueryContextRef,
    ) -> Result<Output> {
        self.create_trigger_procedure(expr, query_context).await?;
        Ok(Output::new_with_affected_rows(0))
    }

    #[cfg(feature = "enterprise")]
    async fn create_trigger_procedure(
        &self,
        expr: PbCreateTriggerExpr,
        query_context: QueryContextRef,
    ) -> Result<SubmitDdlTaskResponse> {
        let task = CreateTriggerTask::try_from(PbCreateTriggerTask {
            create_trigger: Some(expr),
        })
        .context(error::InvalidExprSnafu)?;

        let request = SubmitDdlTaskRequest {
            query_context,
            task: DdlTask::new_create_trigger(task),
        };

        self.procedure_executor
            .submit_ddl_task(&ExecutorContext::default(), request)
            .await
            .context(error::ExecuteDdlSnafu)
    }

    #[tracing::instrument(skip_all)]
    pub async fn create_flow(
        &self,
        stmt: CreateFlow,
        query_context: QueryContextRef,
    ) -> Result<Output> {
        // TODO(ruihang): do some verification
        let expr = expr_helper::to_create_flow_task_expr(stmt, &query_context)?;

        self.create_flow_inner(expr, query_context).await
    }

    pub async fn create_flow_inner(
        &self,
        expr: CreateFlowExpr,
        query_context: QueryContextRef,
    ) -> Result<Output> {
        self.create_flow_procedure(expr, query_context).await?;
        Ok(Output::new_with_affected_rows(0))
    }

    async fn create_flow_procedure(
        &self,
        expr: CreateFlowExpr,
        query_context: QueryContextRef,
    ) -> Result<SubmitDdlTaskResponse> {
        let flow_type = self
            .determine_flow_type(&expr, query_context.clone())
            .await?;
        info!("determined flow={} type: {:#?}", expr.flow_name, flow_type);

        let expr = {
            let mut expr = expr;
            expr.flow_options
                .insert(FlowType::FLOW_TYPE_KEY.to_string(), flow_type.to_string());
            expr
        };

        let task = CreateFlowTask::try_from(PbCreateFlowTask {
            create_flow: Some(expr),
        })
        .context(error::InvalidExprSnafu)?;
        let request = SubmitDdlTaskRequest {
            query_context,
            task: DdlTask::new_create_flow(task),
        };

        self.procedure_executor
            .submit_ddl_task(&ExecutorContext::default(), request)
            .await
            .context(error::ExecuteDdlSnafu)
    }

    /// Determine the flow type based on the SQL query
    ///
    /// If it contains aggregation or distinct, then it is a batch flow, otherwise it is a streaming flow
    async fn determine_flow_type(
        &self,
        expr: &CreateFlowExpr,
        query_ctx: QueryContextRef,
    ) -> Result<FlowType> {
        // first check if source table's ttl is instant, if it is, force streaming mode
        for src_table_name in &expr.source_table_names {
            let table = self
                .catalog_manager()
                .table(
                    &src_table_name.catalog_name,
                    &src_table_name.schema_name,
                    &src_table_name.table_name,
                    Some(&query_ctx),
                )
                .await
                .map_err(BoxedError::new)
                .context(ExternalSnafu)?
                .with_context(|| TableNotFoundSnafu {
                    table_name: format_full_table_name(
                        &src_table_name.catalog_name,
                        &src_table_name.schema_name,
                        &src_table_name.table_name,
                    ),
                })?;

            // instant source table can only be handled by streaming mode
            if table.table_info().meta.options.ttl == Some(common_time::TimeToLive::Instant) {
                warn!(
                    "Source table `{}` for flow `{}`'s ttl=instant, fallback to streaming mode",
                    format_full_table_name(
                        &src_table_name.catalog_name,
                        &src_table_name.schema_name,
                        &src_table_name.table_name
                    ),
                    expr.flow_name
                );
                return Ok(FlowType::Streaming);
            }
        }

        let engine = &self.query_engine;
        let stmts = ParserContext::create_with_dialect(
            &expr.sql,
            query_ctx.sql_dialect(),
            ParseOptions::default(),
        )
        .map_err(BoxedError::new)
        .context(ExternalSnafu)?;

        ensure!(
            stmts.len() == 1,
            InvalidSqlSnafu {
                err_msg: format!("Expect only one statement, found {}", stmts.len())
            }
        );
        let stmt = &stmts[0];

        // support tql parse too
        let plan = match stmt {
            // prom ql is only supported in batching mode
            Statement::Tql(_) => return Ok(FlowType::Batching),
            _ => engine
                .planner()
                .plan(&QueryStatement::Sql(stmt.clone()), query_ctx)
                .await
                .map_err(BoxedError::new)
                .context(ExternalSnafu)?,
        };

        /// Visitor to find aggregation or distinct
        struct FindAggr {
            is_aggr: bool,
        }

        impl TreeNodeVisitor<'_> for FindAggr {
            type Node = LogicalPlan;
            fn f_down(
                &mut self,
                node: &Self::Node,
            ) -> datafusion_common::Result<datafusion_common::tree_node::TreeNodeRecursion>
            {
                match node {
                    LogicalPlan::Aggregate(_) | LogicalPlan::Distinct(_) => {
                        self.is_aggr = true;
                        return Ok(datafusion_common::tree_node::TreeNodeRecursion::Stop);
                    }
                    _ => (),
                }
                Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
            }
        }

        let mut find_aggr = FindAggr { is_aggr: false };

        plan.visit_with_subqueries(&mut find_aggr)
            .context(BuildDfLogicalPlanSnafu)?;
        if find_aggr.is_aggr {
            Ok(FlowType::Batching)
        } else {
            Ok(FlowType::Streaming)
        }
    }

    #[tracing::instrument(skip_all)]
    pub async fn create_view(
        &self,
        create_view: CreateView,
        ctx: QueryContextRef,
    ) -> Result<TableRef> {
        // convert input into logical plan
        let logical_plan = match &*create_view.query {
            Statement::Query(query) => {
                self.plan(
                    &QueryStatement::Sql(Statement::Query(query.clone())),
                    ctx.clone(),
                )
                .await?
            }
            Statement::Tql(query) => self.plan_tql(query.clone(), &ctx).await?,
            _ => {
                return InvalidViewStmtSnafu {}.fail();
            }
        };
        // Save the definition for `show create view`.
        let definition = create_view.to_string();

        // Save the columns in plan, it may changed when the schemas of tables in plan
        // are altered.
        let schema: Schema = logical_plan
            .schema()
            .clone()
            .try_into()
            .context(ConvertSchemaSnafu)?;
        let plan_columns: Vec<_> = schema
            .column_schemas()
            .iter()
            .map(|c| c.name.clone())
            .collect();

        let columns: Vec<_> = create_view
            .columns
            .iter()
            .map(|ident| ident.to_string())
            .collect();

        // Validate columns
        if !columns.is_empty() {
            ensure!(
                columns.len() == plan_columns.len(),
                error::ViewColumnsMismatchSnafu {
                    view_name: create_view.name.to_string(),
                    expected: plan_columns.len(),
                    actual: columns.len(),
                }
            );
        }

        // Extract the table names from the original plan
        // and rewrite them as fully qualified names.
        let (table_names, plan) = extract_and_rewrite_full_table_names(logical_plan, ctx.clone())
            .context(ExtractTableNamesSnafu)?;

        let table_names = table_names.into_iter().map(|t| t.into()).collect();

        // TODO(dennis): we don't save the optimized plan yet,
        // because there are some serialization issue with our own defined plan node (such as `MergeScanLogicalPlan`).
        // When the issues are fixed, we can use the `optimized_plan` instead.
        // let optimized_plan = self.optimize_logical_plan(logical_plan)?.unwrap_df_plan();

        // encode logical plan
        let encoded_plan = DFLogicalSubstraitConvertor
            .encode(&plan, DefaultSerializer)
            .context(SubstraitCodecSnafu)?;

        let expr = expr_helper::to_create_view_expr(
            create_view,
            encoded_plan.to_vec(),
            table_names,
            columns,
            plan_columns,
            definition,
            ctx.clone(),
        )?;

        // TODO(dennis): validate the logical plan
        self.create_view_by_expr(expr, ctx).await
    }

    pub async fn create_view_by_expr(
        &self,
        expr: CreateViewExpr,
        ctx: QueryContextRef,
    ) -> Result<TableRef> {
        ensure! {
            !(expr.create_if_not_exists & expr.or_replace),
            InvalidSqlSnafu {
                err_msg: "syntax error Create Or Replace and If Not Exist cannot be used together",
            }
        };
        let _timer = crate::metrics::DIST_CREATE_VIEW.start_timer();

        let schema_exists = self
            .table_metadata_manager
            .schema_manager()
            .exists(SchemaNameKey::new(&expr.catalog_name, &expr.schema_name))
            .await
            .context(TableMetadataManagerSnafu)?;

        ensure!(
            schema_exists,
            SchemaNotFoundSnafu {
                schema_info: &expr.schema_name,
            }
        );

        // if view or table exists.
        if let Some(table) = self
            .catalog_manager
            .table(
                &expr.catalog_name,
                &expr.schema_name,
                &expr.view_name,
                Some(&ctx),
            )
            .await
            .context(CatalogSnafu)?
        {
            let table_type = table.table_info().table_type;

            match (table_type, expr.create_if_not_exists, expr.or_replace) {
                (TableType::View, true, false) => {
                    return Ok(table);
                }
                (TableType::View, false, false) => {
                    return ViewAlreadyExistsSnafu {
                        name: format_full_table_name(
                            &expr.catalog_name,
                            &expr.schema_name,
                            &expr.view_name,
                        ),
                    }
                    .fail();
                }
                (TableType::View, _, true) => {
                    // Try to replace an exists view
                }
                _ => {
                    return TableAlreadyExistsSnafu {
                        table: format_full_table_name(
                            &expr.catalog_name,
                            &expr.schema_name,
                            &expr.view_name,
                        ),
                    }
                    .fail();
                }
            }
        }

        ensure!(
            NAME_PATTERN_REG.is_match(&expr.view_name),
            InvalidViewNameSnafu {
                name: expr.view_name.clone(),
            }
        );

        let view_name = TableName::new(&expr.catalog_name, &expr.schema_name, &expr.view_name);

        let mut view_info = RawTableInfo {
            ident: metadata::TableIdent {
                // The view id of distributed table is assigned by Meta, set "0" here as a placeholder.
                table_id: 0,
                version: 0,
            },
            name: expr.view_name.clone(),
            desc: None,
            catalog_name: expr.catalog_name.clone(),
            schema_name: expr.schema_name.clone(),
            // The meta doesn't make sense for views, so using a default one.
            meta: RawTableMeta::default(),
            table_type: TableType::View,
        };

        let request = SubmitDdlTaskRequest {
            query_context: ctx,
            task: DdlTask::new_create_view(expr, view_info.clone()),
        };

        let resp = self
            .procedure_executor
            .submit_ddl_task(&ExecutorContext::default(), request)
            .await
            .context(error::ExecuteDdlSnafu)?;

        debug!(
            "Submit creating view '{view_name}' task response: {:?}",
            resp
        );

        let view_id = resp
            .table_ids
            .into_iter()
            .next()
            .context(error::UnexpectedSnafu {
                violated: "expected table_id",
            })?;
        info!("Successfully created view '{view_name}' with view id {view_id}");

        view_info.ident.table_id = view_id;

        let view_info = Arc::new(view_info.try_into().context(CreateTableInfoSnafu)?);

        let table = DistTable::table(view_info);

        // Invalidates local cache ASAP.
        self.cache_invalidator
            .invalidate(
                &Context::default(),
                &[
                    CacheIdent::TableId(view_id),
                    CacheIdent::TableName(view_name.clone()),
                ],
            )
            .await
            .context(error::InvalidateTableCacheSnafu)?;

        Ok(table)
    }

    #[tracing::instrument(skip_all)]
    pub async fn drop_flow(
        &self,
        catalog_name: String,
        flow_name: String,
        drop_if_exists: bool,
        query_context: QueryContextRef,
    ) -> Result<Output> {
        if let Some(flow) = self
            .flow_metadata_manager
            .flow_name_manager()
            .get(&catalog_name, &flow_name)
            .await
            .context(error::TableMetadataManagerSnafu)?
        {
            let flow_id = flow.flow_id();
            let task = DropFlowTask {
                catalog_name,
                flow_name,
                flow_id,
                drop_if_exists,
            };
            self.drop_flow_procedure(task, query_context).await?;

            Ok(Output::new_with_affected_rows(0))
        } else if drop_if_exists {
            Ok(Output::new_with_affected_rows(0))
        } else {
            FlowNotFoundSnafu {
                flow_name: format_full_flow_name(&catalog_name, &flow_name),
            }
            .fail()
        }
    }

    async fn drop_flow_procedure(
        &self,
        expr: DropFlowTask,
        query_context: QueryContextRef,
    ) -> Result<SubmitDdlTaskResponse> {
        let request = SubmitDdlTaskRequest {
            query_context,
            task: DdlTask::new_drop_flow(expr),
        };

        self.procedure_executor
            .submit_ddl_task(&ExecutorContext::default(), request)
            .await
            .context(error::ExecuteDdlSnafu)
    }

    #[cfg(feature = "enterprise")]
    #[tracing::instrument(skip_all)]
    pub(super) async fn drop_trigger(
        &self,
        catalog_name: String,
        trigger_name: String,
        drop_if_exists: bool,
        query_context: QueryContextRef,
    ) -> Result<Output> {
        let task = DropTriggerTask {
            catalog_name,
            trigger_name,
            drop_if_exists,
        };
        self.drop_trigger_procedure(task, query_context).await?;
        Ok(Output::new_with_affected_rows(0))
    }

    #[cfg(feature = "enterprise")]
    async fn drop_trigger_procedure(
        &self,
        expr: DropTriggerTask,
        query_context: QueryContextRef,
    ) -> Result<SubmitDdlTaskResponse> {
        let request = SubmitDdlTaskRequest {
            query_context,
            task: DdlTask::new_drop_trigger(expr),
        };

        self.procedure_executor
            .submit_ddl_task(&ExecutorContext::default(), request)
            .await
            .context(error::ExecuteDdlSnafu)
    }

    /// Drop a view
    #[tracing::instrument(skip_all)]
    pub(crate) async fn drop_view(
        &self,
        catalog: String,
        schema: String,
        view: String,
        drop_if_exists: bool,
        query_context: QueryContextRef,
    ) -> Result<Output> {
        let view_info = if let Some(view) = self
            .catalog_manager
            .table(&catalog, &schema, &view, None)
            .await
            .context(CatalogSnafu)?
        {
            view.table_info()
        } else if drop_if_exists {
            // DROP VIEW IF EXISTS meets view not found - ignored
            return Ok(Output::new_with_affected_rows(0));
        } else {
            return TableNotFoundSnafu {
                table_name: format_full_table_name(&catalog, &schema, &view),
            }
            .fail();
        };

        // Ensure the exists one is view, we can't drop other table types
        ensure!(
            view_info.table_type == TableType::View,
            error::InvalidViewSnafu {
                msg: "not a view",
                view_name: format_full_table_name(&catalog, &schema, &view),
            }
        );

        let view_id = view_info.table_id();

        let task = DropViewTask {
            catalog,
            schema,
            view,
            view_id,
            drop_if_exists,
        };

        self.drop_view_procedure(task, query_context).await?;

        Ok(Output::new_with_affected_rows(0))
    }

    /// Submit [DropViewTask] to procedure executor.
    async fn drop_view_procedure(
        &self,
        expr: DropViewTask,
        query_context: QueryContextRef,
    ) -> Result<SubmitDdlTaskResponse> {
        let request = SubmitDdlTaskRequest {
            query_context,
            task: DdlTask::new_drop_view(expr),
        };

        self.procedure_executor
            .submit_ddl_task(&ExecutorContext::default(), request)
            .await
            .context(error::ExecuteDdlSnafu)
    }

    #[tracing::instrument(skip_all)]
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

    #[tracing::instrument(skip_all)]
    pub async fn drop_table(
        &self,
        table_name: TableName,
        drop_if_exists: bool,
        query_context: QueryContextRef,
    ) -> Result<Output> {
        // Reserved for grpc call
        self.drop_tables(&[table_name], drop_if_exists, query_context)
            .await
    }

    #[tracing::instrument(skip_all)]
    pub async fn drop_tables(
        &self,
        table_names: &[TableName],
        drop_if_exists: bool,
        query_context: QueryContextRef,
    ) -> Result<Output> {
        let mut tables = Vec::with_capacity(table_names.len());
        for table_name in table_names {
            ensure!(
                !is_readonly_schema(&table_name.schema_name),
                SchemaReadOnlySnafu {
                    name: table_name.schema_name.clone()
                }
            );

            if let Some(table) = self
                .catalog_manager
                .table(
                    &table_name.catalog_name,
                    &table_name.schema_name,
                    &table_name.table_name,
                    Some(&query_context),
                )
                .await
                .context(CatalogSnafu)?
            {
                tables.push(table.table_info().table_id());
            } else if drop_if_exists {
                // DROP TABLE IF EXISTS meets table not found - ignored
                continue;
            } else {
                return TableNotFoundSnafu {
                    table_name: table_name.to_string(),
                }
                .fail();
            }
        }

        for (table_name, table_id) in table_names.iter().zip(tables.into_iter()) {
            self.drop_table_procedure(table_name, table_id, drop_if_exists, query_context.clone())
                .await?;

            // Invalidates local cache ASAP.
            self.cache_invalidator
                .invalidate(
                    &Context::default(),
                    &[
                        CacheIdent::TableId(table_id),
                        CacheIdent::TableName(table_name.clone()),
                    ],
                )
                .await
                .context(error::InvalidateTableCacheSnafu)?;
        }
        Ok(Output::new_with_affected_rows(0))
    }

    #[tracing::instrument(skip_all)]
    pub async fn drop_database(
        &self,
        catalog: String,
        schema: String,
        drop_if_exists: bool,
        query_context: QueryContextRef,
    ) -> Result<Output> {
        ensure!(
            !is_readonly_schema(&schema),
            SchemaReadOnlySnafu { name: schema }
        );

        if self
            .catalog_manager
            .schema_exists(&catalog, &schema, None)
            .await
            .context(CatalogSnafu)?
        {
            if schema == query_context.current_schema() {
                SchemaInUseSnafu { name: schema }.fail()
            } else {
                self.drop_database_procedure(catalog, schema, drop_if_exists, query_context)
                    .await?;

                Ok(Output::new_with_affected_rows(0))
            }
        } else if drop_if_exists {
            // DROP TABLE IF EXISTS meets table not found - ignored
            Ok(Output::new_with_affected_rows(0))
        } else {
            SchemaNotFoundSnafu {
                schema_info: schema,
            }
            .fail()
        }
    }

    #[tracing::instrument(skip_all)]
    pub async fn truncate_table(
        &self,
        table_name: TableName,
        time_ranges: Vec<(Timestamp, Timestamp)>,
        query_context: QueryContextRef,
    ) -> Result<Output> {
        ensure!(
            !is_readonly_schema(&table_name.schema_name),
            SchemaReadOnlySnafu {
                name: table_name.schema_name.clone()
            }
        );

        let table = self
            .catalog_manager
            .table(
                &table_name.catalog_name,
                &table_name.schema_name,
                &table_name.table_name,
                Some(&query_context),
            )
            .await
            .context(CatalogSnafu)?
            .with_context(|| TableNotFoundSnafu {
                table_name: table_name.to_string(),
            })?;
        let table_id = table.table_info().table_id();
        self.truncate_table_procedure(&table_name, table_id, time_ranges, query_context)
            .await?;

        Ok(Output::new_with_affected_rows(0))
    }

    #[tracing::instrument(skip_all)]
    pub async fn alter_table(
        &self,
        alter_table: AlterTable,
        query_context: QueryContextRef,
    ) -> Result<Output> {
        let expr = expr_helper::to_alter_table_expr(alter_table, &query_context)?;
        self.alter_table_inner(expr, query_context).await
    }

    #[tracing::instrument(skip_all)]
    pub async fn alter_table_inner(
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
        info!(
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
            .context(error::ExecuteDdlSnafu)?;

        // Invalidates local cache ASAP.
        self.cache_invalidator
            .invalidate(&Context::default(), &invalidate_keys)
            .await
            .context(error::InvalidateTableCacheSnafu)?;

        Ok(Output::new_with_affected_rows(0))
    }

    #[cfg(feature = "enterprise")]
    #[tracing::instrument(skip_all)]
    pub async fn alter_trigger(
        &self,
        _alter_expr: AlterTrigger,
        _query_context: QueryContextRef,
    ) -> Result<Output> {
        crate::error::NotSupportedSnafu {
            feat: "alter trigger",
        }
        .fail()
    }

    #[tracing::instrument(skip_all)]
    pub async fn alter_database(
        &self,
        alter_expr: AlterDatabase,
        query_context: QueryContextRef,
    ) -> Result<Output> {
        let alter_expr = expr_helper::to_alter_database_expr(alter_expr, &query_context)?;
        self.alter_database_inner(alter_expr, query_context).await
    }

    #[tracing::instrument(skip_all)]
    pub async fn alter_database_inner(
        &self,
        alter_expr: AlterDatabaseExpr,
        query_context: QueryContextRef,
    ) -> Result<Output> {
        ensure!(
            !is_readonly_schema(&alter_expr.schema_name),
            SchemaReadOnlySnafu {
                name: query_context.current_schema().clone()
            }
        );

        let exists = self
            .catalog_manager
            .schema_exists(&alter_expr.catalog_name, &alter_expr.schema_name, None)
            .await
            .context(CatalogSnafu)?;
        ensure!(
            exists,
            SchemaNotFoundSnafu {
                schema_info: alter_expr.schema_name,
            }
        );

        let cache_ident = [CacheIdent::SchemaName(SchemaName {
            catalog_name: alter_expr.catalog_name.clone(),
            schema_name: alter_expr.schema_name.clone(),
        })];

        self.alter_database_procedure(alter_expr, query_context)
            .await?;

        // Invalidates local cache ASAP.
        self.cache_invalidator
            .invalidate(&Context::default(), &cache_ident)
            .await
            .context(error::InvalidateTableCacheSnafu)?;

        Ok(Output::new_with_affected_rows(0))
    }

    async fn create_table_procedure(
        &self,
        create_table: CreateTableExpr,
        partitions: Vec<PartitionExpr>,
        table_info: RawTableInfo,
        query_context: QueryContextRef,
    ) -> Result<SubmitDdlTaskResponse> {
        let partitions = partitions
            .into_iter()
            .map(|expr| expr.as_pb_partition().context(PartitionExprToPbSnafu))
            .collect::<Result<Vec<_>>>()?;

        let request = SubmitDdlTaskRequest {
            query_context,
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
        query_context: QueryContextRef,
    ) -> Result<SubmitDdlTaskResponse> {
        let request = SubmitDdlTaskRequest {
            query_context,
            task: DdlTask::new_create_logical_tables(tables_data),
        };

        self.procedure_executor
            .submit_ddl_task(&ExecutorContext::default(), request)
            .await
            .context(error::ExecuteDdlSnafu)
    }

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
            .context(error::ExecuteDdlSnafu)
    }

    async fn drop_table_procedure(
        &self,
        table_name: &TableName,
        table_id: TableId,
        drop_if_exists: bool,
        query_context: QueryContextRef,
    ) -> Result<SubmitDdlTaskResponse> {
        let request = SubmitDdlTaskRequest {
            query_context,
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

    async fn drop_database_procedure(
        &self,
        catalog: String,
        schema: String,
        drop_if_exists: bool,
        query_context: QueryContextRef,
    ) -> Result<SubmitDdlTaskResponse> {
        let request = SubmitDdlTaskRequest {
            query_context,
            task: DdlTask::new_drop_database(catalog, schema, drop_if_exists),
        };

        self.procedure_executor
            .submit_ddl_task(&ExecutorContext::default(), request)
            .await
            .context(error::ExecuteDdlSnafu)
    }

    async fn alter_database_procedure(
        &self,
        alter_expr: AlterDatabaseExpr,
        query_context: QueryContextRef,
    ) -> Result<SubmitDdlTaskResponse> {
        let request = SubmitDdlTaskRequest {
            query_context,
            task: DdlTask::new_alter_database(alter_expr),
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
        time_ranges: Vec<(Timestamp, Timestamp)>,
        query_context: QueryContextRef,
    ) -> Result<SubmitDdlTaskResponse> {
        let request = SubmitDdlTaskRequest {
            query_context,
            task: DdlTask::new_truncate_table(
                table_name.catalog_name.to_string(),
                table_name.schema_name.to_string(),
                table_name.table_name.to_string(),
                table_id,
                time_ranges,
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
        database: &str,
        create_if_not_exists: bool,
        options: HashMap<String, String>,
        query_context: QueryContextRef,
    ) -> Result<Output> {
        let catalog = query_context.current_catalog();
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

        if !self
            .catalog_manager
            .schema_exists(catalog, database, None)
            .await
            .context(CatalogSnafu)?
            && !self.catalog_manager.is_reserved_schema_name(database)
        {
            self.create_database_procedure(
                catalog.to_string(),
                database.to_string(),
                create_if_not_exists,
                options,
                query_context,
            )
            .await?;

            Ok(Output::new_with_affected_rows(1))
        } else if create_if_not_exists {
            Ok(Output::new_with_affected_rows(1))
        } else {
            error::SchemaExistsSnafu { name: database }.fail()
        }
    }

    async fn create_database_procedure(
        &self,
        catalog: String,
        database: String,
        create_if_not_exists: bool,
        options: HashMap<String, String>,
        query_context: QueryContextRef,
    ) -> Result<SubmitDdlTaskResponse> {
        let request = SubmitDdlTaskRequest {
            query_context,
            task: DdlTask::new_create_database(catalog, database, create_if_not_exists, options),
        };

        self.procedure_executor
            .submit_ddl_task(&ExecutorContext::default(), request)
            .await
            .context(error::ExecuteDdlSnafu)
    }
}

/// Parse partition statement [Partitions] into [PartitionExpr] and partition columns.
pub fn parse_partitions(
    create_table: &CreateTableExpr,
    partitions: Option<Partitions>,
    query_ctx: &QueryContextRef,
) -> Result<(Vec<PartitionExpr>, Vec<String>)> {
    // If partitions are not defined by user, use the timestamp column (which has to be existed) as
    // the partition column, and create only one partition.
    let partition_columns = find_partition_columns(&partitions)?;
    let partition_exprs =
        find_partition_entries(create_table, &partitions, &partition_columns, query_ctx)?;

    // Validates partition
    let exprs = partition_exprs.clone();
    MultiDimPartitionRule::try_new(partition_columns.clone(), vec![], exprs, true)
        .context(InvalidPartitionSnafu)?;

    Ok((partition_exprs, partition_columns))
}

/// Verifies an alter and returns whether it is necessary to perform the alter.
///
/// # Returns
///
/// Returns true if the alter need to be porformed; otherwise, it returns false.
pub fn verify_alter(
    table_id: TableId,
    table_info: Arc<TableInfo>,
    expr: AlterTableExpr,
) -> Result<bool> {
    let request: AlterTableRequest =
        common_grpc_expr::alter_expr_to_request(table_id, expr, Some(&table_info.meta))
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
    } else if let AlterKind::AddColumns { columns } = alter_kind {
        // If all the columns are marked as add_if_not_exists and they already exist in the table,
        // there is no need to perform the alter.
        let column_names: HashSet<_> = table_info
            .meta
            .schema
            .column_schemas()
            .iter()
            .map(|schema| &schema.name)
            .collect();
        if columns.iter().all(|column| {
            column_names.contains(&column.column_schema.name) && column.add_if_not_exists
        }) {
            return Ok(false);
        }
    }

    let _ = table_info
        .meta
        .builder_with_alter_kind(table_name, &request.alter_kind)
        .context(error::TableSnafu)?
        .build()
        .context(error::BuildTableMetaSnafu { table_name })?;

    Ok(true)
}

pub fn create_table_info(
    create_table: &CreateTableExpr,
    partition_columns: Vec<String>,
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

    let table_options = TableOptions::try_from_iter(&create_table.table_options)
        .context(UnrecognizedTableOptionSnafu)?;

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
        column_ids: vec![],
    };

    let desc = if create_table.desc.is_empty() {
        create_table.table_options.get(COMMENT_KEY).cloned()
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
/// Returns a list of [PartitionExpr], each of which defines a partition.
fn find_partition_entries(
    create_table: &CreateTableExpr,
    partitions: &Option<Partitions>,
    partition_columns: &[String],
    query_ctx: &QueryContextRef,
) -> Result<Vec<PartitionExpr>> {
    let Some(partitions) = partitions else {
        return Ok(vec![]);
    };

    // extract concrete data type of partition columns
    let column_name_and_type = partition_columns
        .iter()
        .map(|pc| {
            let column = create_table
                .column_defs
                .iter()
                .find(|c| &c.name == pc)
                // unwrap is safe here because we have checked that partition columns are defined
                .unwrap();
            let column_name = &column.name;
            let data_type = ConcreteDataType::from(
                ColumnDataTypeWrapper::try_new(column.data_type, column.datatype_extension)
                    .context(ColumnDataTypeSnafu)?,
            );
            Ok((column_name, data_type))
        })
        .collect::<Result<HashMap<_, _>>>()?;

    // Transform parser expr to partition expr
    let mut partition_exprs = Vec::with_capacity(partitions.exprs.len());
    for partition in &partitions.exprs {
        let partition_expr =
            convert_one_expr(partition, &column_name_and_type, &query_ctx.timezone())?;
        partition_exprs.push(partition_expr);
    }

    Ok(partition_exprs)
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
        // col, val
        (Expr::Identifier(ident), Expr::Value(value)) => {
            let (column_name, data_type) = convert_identifier(ident, column_name_and_type)?;
            let value = convert_value(&value.value, data_type, timezone, None)?;
            (Operand::Column(column_name), op, Operand::Value(value))
        }
        (Expr::Identifier(ident), Expr::UnaryOp { op: unary_op, expr })
            if let Expr::Value(v) = &**expr =>
        {
            let (column_name, data_type) = convert_identifier(ident, column_name_and_type)?;
            let value = convert_value(&v.value, data_type, timezone, Some(*unary_op))?;
            (Operand::Column(column_name), op, Operand::Value(value))
        }
        // val, col
        (Expr::Value(value), Expr::Identifier(ident)) => {
            let (column_name, data_type) = convert_identifier(ident, column_name_and_type)?;
            let value = convert_value(&value.value, data_type, timezone, None)?;
            (Operand::Value(value), op, Operand::Column(column_name))
        }
        (Expr::UnaryOp { op: unary_op, expr }, Expr::Identifier(ident))
            if let Expr::Value(v) = &**expr =>
        {
            let (column_name, data_type) = convert_identifier(ident, column_name_and_type)?;
            let value = convert_value(&v.value, data_type, timezone, Some(*unary_op))?;
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
    unary_op: Option<UnaryOperator>,
) -> Result<Value> {
    sql_value_to_value(
        "<NONAME>",
        &data_type,
        value,
        Some(timezone),
        unary_op,
        false,
    )
    .context(error::SqlCommonSnafu)
}

#[cfg(test)]
mod test {
    use session::context::{QueryContext, QueryContextBuilder};
    use sql::dialect::GreptimeDbDialect;
    use sql::parser::{ParseOptions, ParserContext};
    use sql::statements::statement::Statement;

    use super::*;
    use crate::expr_helper;

    #[test]
    fn test_name_is_match() {
        assert!(!NAME_PATTERN_REG.is_match("/adaf"));
        assert!(!NAME_PATTERN_REG.is_match("🈲"));
        assert!(NAME_PATTERN_REG.is_match("hello"));
        assert!(NAME_PATTERN_REG.is_match("test@"));
        assert!(!NAME_PATTERN_REG.is_match("@test"));
        assert!(NAME_PATTERN_REG.is_match("test#"));
        assert!(!NAME_PATTERN_REG.is_match("#test"));
        assert!(!NAME_PATTERN_REG.is_match("@"));
        assert!(!NAME_PATTERN_REG.is_match("#"));
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
        let ctx = QueryContextBuilder::default().build().into();
        for (sql, expected) in cases {
            let result = ParserContext::create_with_dialect(
                sql,
                &GreptimeDbDialect {},
                ParseOptions::default(),
            )
            .unwrap();
            match &result[0] {
                Statement::CreateTable(c) => {
                    let expr = expr_helper::create_to_expr(c, &QueryContext::arc()).unwrap();
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
