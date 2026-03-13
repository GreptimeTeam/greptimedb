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

use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use api::helper::from_pb_time_ranges;
use api::v1::ddl_request::{Expr as DdlExpr, Expr};
use api::v1::greptime_request::Request;
use api::v1::query_request::Query;
use api::v1::{
    DeleteRequests, DropFlowExpr, InsertIntoPlan, InsertRequests, RowDeleteRequests,
    RowInsertRequests,
};
use async_stream::try_stream;
use async_trait::async_trait;
use auth::{PermissionChecker, PermissionCheckerRef, PermissionReq};
use common_base::AffectedRows;
use common_error::ext::BoxedError;
use common_grpc::flight::do_put::DoPutResponse;
use common_query::Output;
use common_query::logical_plan::add_insert_to_logical_plan;
use common_telemetry::tracing::{self};
use datafusion::datasource::DefaultTableSource;
use futures::Stream;
use futures::stream::StreamExt;
use query::parser::PromQuery;
use servers::error as server_error;
use servers::http::prom_store::PHYSICAL_TABLE_PARAM;
use servers::interceptor::{GrpcQueryInterceptor, GrpcQueryInterceptorRef};
use servers::query_handler::grpc::GrpcQueryHandler;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt, ensure};
use table::TableRef;
use table::table::adapter::DfTableProviderAdapter;
use table::table_name::TableName;

use crate::error::{
    CatalogSnafu, DataFusionSnafu, Error, ExternalSnafu, IncompleteGrpcRequestSnafu,
    NotSupportedSnafu, PermissionSnafu, PlanStatementSnafu, Result,
    SubstraitDecodeLogicalPlanSnafu, TableNotFoundSnafu, TableOperationSnafu,
};
use crate::instance::{Instance, attach_timer};
use crate::metrics::{
    GRPC_HANDLE_PLAN_ELAPSED, GRPC_HANDLE_PROMQL_ELAPSED, GRPC_HANDLE_SQL_ELAPSED,
};

#[async_trait]
impl GrpcQueryHandler for Instance {
    async fn do_query(
        &self,
        request: Request,
        ctx: QueryContextRef,
    ) -> server_error::Result<Output> {
        let result: Result<Output> = async {
            let interceptor_ref = self.plugins.get::<GrpcQueryInterceptorRef<Error>>();
            let interceptor = interceptor_ref.as_ref();
            interceptor.pre_execute(&request, ctx.clone())?;

            self.plugins
                .get::<PermissionCheckerRef>()
                .as_ref()
                .check_permission(ctx.current_user(), PermissionReq::GrpcRequest(&request))
                .context(PermissionSnafu)?;

            let output = match request {
                Request::Inserts(requests) => self.handle_inserts(requests, ctx.clone()).await?,
                Request::RowInserts(requests) => match ctx.extension(PHYSICAL_TABLE_PARAM) {
                    Some(physical_table) => {
                        self.handle_metric_row_inserts(
                            requests,
                            ctx.clone(),
                            physical_table.to_string(),
                        )
                        .await?
                    }
                    None => {
                        self.handle_row_inserts(requests, ctx.clone(), false, false)
                            .await?
                    }
                },
                Request::Deletes(requests) => self.handle_deletes(requests, ctx.clone()).await?,
                Request::RowDeletes(requests) => self.handle_row_deletes(requests, ctx.clone()).await?,
                Request::Query(query_request) => {
                    let query = query_request.query.context(IncompleteGrpcRequestSnafu {
                        err_msg: "Missing field 'QueryRequest.query'",
                    })?;
                    match query {
                        Query::Sql(sql) => {
                            let timer = GRPC_HANDLE_SQL_ELAPSED.start_timer();
                            let mut result = self.do_query_inner(&sql, ctx.clone()).await;
                            ensure!(
                                result.len() == 1,
                                NotSupportedSnafu {
                                    feat: "execute multiple statements in SQL query string through GRPC interface"
                                }
                            );
                            let output = result.remove(0)?;
                            attach_timer(output, timer)
                        }
                        Query::LogicalPlan(plan) => {
                            // this path is useful internally when flownode needs to execute a logical plan through gRPC interface
                            let timer = GRPC_HANDLE_PLAN_ELAPSED.start_timer();

                            // use dummy catalog to provide table
                            let plan_decoder = self
                                .query_engine()
                                .engine_context(ctx.clone())
                                .new_plan_decoder()
                                .context(PlanStatementSnafu)?;

                            let dummy_catalog_list =
                                Arc::new(catalog::table_source::dummy_catalog::DummyCatalogList::new(
                                    self.catalog_manager().clone(),
                                ));

                            let logical_plan = plan_decoder
                                .decode(bytes::Bytes::from(plan), dummy_catalog_list, true)
                                .await
                                .context(SubstraitDecodeLogicalPlanSnafu)?;
                            let output =
                                self.do_exec_plan_inner(None, logical_plan, ctx.clone()).await?;

                            attach_timer(output, timer)
                        }
                        Query::InsertIntoPlan(insert) => {
                            self.handle_insert_plan(insert, ctx.clone()).await?
                        }
                        Query::PromRangeQuery(promql) => {
                            let timer = GRPC_HANDLE_PROMQL_ELAPSED.start_timer();
                            let prom_query = PromQuery {
                                query: promql.query,
                                start: promql.start,
                                end: promql.end,
                                step: promql.step,
                                lookback: promql.lookback,
                                alias: None,
                            };
                            let mut result =
                                self.do_promql_query_inner(&prom_query, ctx.clone()).await;
                            ensure!(
                                result.len() == 1,
                                NotSupportedSnafu {
                                    feat: "execute multiple statements in PromQL query string through GRPC interface"
                                }
                            );
                            let output = result.remove(0)?;
                            attach_timer(output, timer)
                        }
                    }
                }
                Request::Ddl(request) => {
                    let mut expr = request.expr.context(IncompleteGrpcRequestSnafu {
                        err_msg: "'expr' is absent in DDL request",
                    })?;

                    fill_catalog_and_schema_from_context(&mut expr, &ctx);

                    match expr {
                        DdlExpr::CreateTable(mut expr) => {
                            let _ = self
                                .statement_executor
                                .create_table_inner(&mut expr, None, ctx.clone())
                                .await?;
                            Output::new_with_affected_rows(0)
                        }
                        DdlExpr::AlterDatabase(expr) => {
                            let _ = self
                                .statement_executor
                                .alter_database_inner(expr, ctx.clone())
                                .await?;
                            Output::new_with_affected_rows(0)
                        }
                        DdlExpr::AlterTable(expr) => {
                            self.statement_executor
                                .alter_table_inner(expr, ctx.clone())
                                .await?
                        }
                        DdlExpr::CreateDatabase(expr) => {
                            self.statement_executor
                                .create_database(
                                    &expr.schema_name,
                                    expr.create_if_not_exists,
                                    expr.options,
                                    ctx.clone(),
                                )
                                .await?
                        }
                        DdlExpr::DropTable(expr) => {
                            let table_name =
                                TableName::new(&expr.catalog_name, &expr.schema_name, &expr.table_name);
                            self.statement_executor
                                .drop_table(table_name, expr.drop_if_exists, ctx.clone())
                                .await?
                        }
                        DdlExpr::TruncateTable(expr) => {
                            let table_name =
                                TableName::new(&expr.catalog_name, &expr.schema_name, &expr.table_name);
                            let time_ranges = from_pb_time_ranges(expr.time_ranges.unwrap_or_default())
                                .map_err(BoxedError::new)
                                .context(ExternalSnafu)?;
                            self.statement_executor
                                .truncate_table(table_name, time_ranges, ctx.clone())
                                .await?
                        }
                        DdlExpr::CreateFlow(expr) => {
                            self.statement_executor
                                .create_flow_inner(expr, ctx.clone())
                                .await?
                        }
                        DdlExpr::DropFlow(DropFlowExpr {
                            catalog_name,
                            flow_name,
                            drop_if_exists,
                            ..
                        }) => {
                            self.statement_executor
                                .drop_flow(catalog_name, flow_name, drop_if_exists, ctx.clone())
                                .await?
                        }
                        DdlExpr::CreateView(expr) => {
                            let _ = self
                                .statement_executor
                                .create_view_by_expr(expr, ctx.clone())
                                .await?;

                            Output::new_with_affected_rows(0)
                        }
                        DdlExpr::DropView(_) => {
                            todo!("implemented in the following PR")
                        }
                        DdlExpr::CommentOn(expr) => {
                            self.statement_executor
                                .comment_by_expr(expr, ctx.clone())
                                .await?
                        }
                    }
                }
            };

            let output = interceptor.post_execute(output, ctx)?;
            Ok(output)
        }
        .await;

        result
            .map_err(BoxedError::new)
            .context(server_error::ExecuteGrpcQuerySnafu)
    }

    async fn put_record_batch(
        &self,
        request: servers::grpc::flight::PutRecordBatchRequest,
        table_ref: &mut Option<TableRef>,
        ctx: QueryContextRef,
    ) -> server_error::Result<AffectedRows> {
        let result: Result<AffectedRows> = async {
            let table = if let Some(table) = table_ref {
                table.clone()
            } else {
                let table = self
                    .catalog_manager()
                    .table(
                        &request.table_name.catalog_name,
                        &request.table_name.schema_name,
                        &request.table_name.table_name,
                        None,
                    )
                    .await
                    .context(CatalogSnafu)?
                    .with_context(|| TableNotFoundSnafu {
                        table_name: request.table_name.to_string(),
                    })?;
                *table_ref = Some(table.clone());
                table
            };

            let interceptor_ref = self.plugins.get::<GrpcQueryInterceptorRef<Error>>();
            let interceptor = interceptor_ref.as_ref();
            interceptor.pre_bulk_insert(table.clone(), ctx.clone())?;

            self.plugins
                .get::<PermissionCheckerRef>()
                .as_ref()
                .check_permission(ctx.current_user(), PermissionReq::BulkInsert)
                .context(PermissionSnafu)?;

            // do we check limit for bulk insert?

            self.inserter
                .handle_bulk_insert(
                    table,
                    request.flight_data,
                    request.record_batch,
                    request.schema_bytes,
                )
                .await
                .context(TableOperationSnafu)
        }
        .await;

        result
            .map_err(BoxedError::new)
            .context(server_error::ExecuteGrpcRequestSnafu)
    }

    fn handle_put_record_batch_stream(
        &self,
        stream: servers::grpc::flight::PutRecordBatchRequestStream,
        ctx: QueryContextRef,
    ) -> Pin<Box<dyn Stream<Item = server_error::Result<DoPutResponse>> + Send>> {
        Box::pin(
            self.handle_put_record_batch_stream_inner(stream, ctx)
                .map(|result| {
                    result
                        .map_err(BoxedError::new)
                        .context(server_error::ExecuteGrpcRequestSnafu)
                }),
        )
    }
}

fn fill_catalog_and_schema_from_context(ddl_expr: &mut DdlExpr, ctx: &QueryContextRef) {
    let catalog = ctx.current_catalog();
    let schema = ctx.current_schema();

    macro_rules! check_and_fill {
        ($expr:ident) => {
            if $expr.catalog_name.is_empty() {
                $expr.catalog_name = catalog.to_string();
            }
            if $expr.schema_name.is_empty() {
                $expr.schema_name = schema.to_string();
            }
        };
    }

    match ddl_expr {
        Expr::CreateDatabase(_) | Expr::AlterDatabase(_) => { /* do nothing*/ }
        Expr::CreateTable(expr) => {
            check_and_fill!(expr);
        }
        Expr::AlterTable(expr) => {
            check_and_fill!(expr);
        }
        Expr::DropTable(expr) => {
            check_and_fill!(expr);
        }
        Expr::TruncateTable(expr) => {
            check_and_fill!(expr);
        }
        Expr::CreateFlow(expr) => {
            if expr.catalog_name.is_empty() {
                expr.catalog_name = catalog.to_string();
            }
        }
        Expr::DropFlow(expr) => {
            if expr.catalog_name.is_empty() {
                expr.catalog_name = catalog.to_string();
            }
        }
        Expr::CreateView(expr) => {
            check_and_fill!(expr);
        }
        Expr::DropView(expr) => {
            check_and_fill!(expr);
        }
        Expr::CommentOn(expr) => {
            check_and_fill!(expr);
        }
    }
}

impl Instance {
    fn handle_put_record_batch_stream_inner(
        &self,
        mut stream: servers::grpc::flight::PutRecordBatchRequestStream,
        ctx: QueryContextRef,
    ) -> Pin<Box<dyn Stream<Item = Result<DoPutResponse>> + Send>> {
        // Clone all necessary data to make it 'static
        let catalog_manager = self.catalog_manager().clone();
        let plugins = self.plugins.clone();
        let inserter = self.inserter.clone();
        let ctx = ctx.clone();
        let mut table_ref: Option<TableRef> = None;
        let mut table_checked = false;

        Box::pin(try_stream! {
            // Process each request in the stream
            while let Some(request_result) = stream.next().await {
                let request = request_result.map_err(|e| {
                    let error_msg = format!("Stream error: {:?}", e);
                    IncompleteGrpcRequestSnafu { err_msg: error_msg }.build()
                })?;

                // Resolve table and check permissions on first RecordBatch (after schema is received)
                if !table_checked {
                    let table_name = &request.table_name;

                    plugins
                        .get::<PermissionCheckerRef>()
                        .as_ref()
                        .check_permission(ctx.current_user(), PermissionReq::BulkInsert)
                        .context(PermissionSnafu)?;

                    // Resolve table reference
                    table_ref = Some(
                        catalog_manager
                            .table(
                                &table_name.catalog_name,
                                &table_name.schema_name,
                                &table_name.table_name,
                                None,
                            )
                            .await
                            .context(CatalogSnafu)?
                            .with_context(|| TableNotFoundSnafu {
                                table_name: table_name.to_string(),
                            })?,
                    );

                    // Check permissions for the table
                    let interceptor_ref = plugins.get::<GrpcQueryInterceptorRef<Error>>();
                    let interceptor = interceptor_ref.as_ref();
                    interceptor.pre_bulk_insert(table_ref.clone().unwrap(), ctx.clone())?;

                    table_checked = true;
                }

                let request_id = request.request_id;
                let start = Instant::now();
                let rows = inserter
                    .handle_bulk_insert(
                        table_ref.clone().unwrap(),
                        request.flight_data,
                        request.record_batch,
                        request.schema_bytes,
                    )
                    .await
                    .context(TableOperationSnafu)?;
                let elapsed_secs = start.elapsed().as_secs_f64();
                yield DoPutResponse::new(request_id, rows, elapsed_secs);
            }
        })
    }

    async fn handle_insert_plan(
        &self,
        insert: InsertIntoPlan,
        ctx: QueryContextRef,
    ) -> Result<Output> {
        let timer = GRPC_HANDLE_PLAN_ELAPSED.start_timer();
        let table_name = insert.table_name.context(IncompleteGrpcRequestSnafu {
            err_msg: "'table_name' is absent in InsertIntoPlan",
        })?;

        // use dummy catalog to provide table
        let plan_decoder = self
            .query_engine()
            .engine_context(ctx.clone())
            .new_plan_decoder()
            .context(PlanStatementSnafu)?;

        let dummy_catalog_list =
            Arc::new(catalog::table_source::dummy_catalog::DummyCatalogList::new(
                self.catalog_manager().clone(),
            ));

        // no optimize yet since we still need to add stuff
        let logical_plan = plan_decoder
            .decode(
                bytes::Bytes::from(insert.logical_plan),
                dummy_catalog_list,
                false,
            )
            .await
            .context(SubstraitDecodeLogicalPlanSnafu)?;

        let table = self
            .catalog_manager()
            .table(
                &table_name.catalog_name,
                &table_name.schema_name,
                &table_name.table_name,
                None,
            )
            .await
            .context(CatalogSnafu)?
            .with_context(|| TableNotFoundSnafu {
                table_name: [
                    table_name.catalog_name.clone(),
                    table_name.schema_name.clone(),
                    table_name.table_name.clone(),
                ]
                .join("."),
            })?;
        let table_provider = Arc::new(DfTableProviderAdapter::new(table));
        let table_source = Arc::new(DefaultTableSource::new(table_provider));

        let insert_into = add_insert_to_logical_plan(table_name, table_source, logical_plan)
            .context(SubstraitDecodeLogicalPlanSnafu)?;

        let engine_ctx = self.query_engine().engine_context(ctx.clone());
        let state = engine_ctx.state();
        // Analyze the plan
        let analyzed_plan = state
            .analyzer()
            .execute_and_check(insert_into, state.config_options(), |_, _| {})
            .context(DataFusionSnafu)?;

        // Optimize the plan
        let optimized_plan = state.optimize(&analyzed_plan).context(DataFusionSnafu)?;

        let output = self
            .do_exec_plan_inner(None, optimized_plan, ctx.clone())
            .await?;

        Ok(attach_timer(output, timer))
    }
    #[tracing::instrument(skip_all)]
    pub async fn handle_inserts(
        &self,
        requests: InsertRequests,
        ctx: QueryContextRef,
    ) -> Result<Output> {
        self.inserter
            .handle_column_inserts(requests, ctx, self.statement_executor.as_ref())
            .await
            .context(TableOperationSnafu)
    }

    #[tracing::instrument(skip_all)]
    pub async fn handle_row_inserts(
        &self,
        requests: RowInsertRequests,
        ctx: QueryContextRef,
        accommodate_existing_schema: bool,
        is_single_value: bool,
    ) -> Result<Output> {
        self.inserter
            .handle_row_inserts(
                requests,
                ctx,
                self.statement_executor.as_ref(),
                accommodate_existing_schema,
                is_single_value,
            )
            .await
            .context(TableOperationSnafu)
    }

    #[tracing::instrument(skip_all)]
    pub async fn handle_influx_row_inserts(
        &self,
        requests: RowInsertRequests,
        ctx: QueryContextRef,
    ) -> Result<Output> {
        self.inserter
            .handle_last_non_null_inserts(
                requests,
                ctx,
                self.statement_executor.as_ref(),
                true,
                // Influx protocol may writes multiple fields (values).
                false,
            )
            .await
            .context(TableOperationSnafu)
    }

    #[tracing::instrument(skip_all)]
    pub async fn handle_metric_row_inserts(
        &self,
        requests: RowInsertRequests,
        ctx: QueryContextRef,
        physical_table: String,
    ) -> Result<Output> {
        self.inserter
            .handle_metric_row_inserts(requests, ctx, &self.statement_executor, physical_table)
            .await
            .context(TableOperationSnafu)
    }

    #[tracing::instrument(skip_all)]
    pub async fn handle_deletes(
        &self,
        requests: DeleteRequests,
        ctx: QueryContextRef,
    ) -> Result<Output> {
        self.deleter
            .handle_column_deletes(requests, ctx)
            .await
            .context(TableOperationSnafu)
    }

    #[tracing::instrument(skip_all)]
    pub async fn handle_row_deletes(
        &self,
        requests: RowDeleteRequests,
        ctx: QueryContextRef,
    ) -> Result<Output> {
        self.deleter
            .handle_row_deletes(requests, ctx)
            .await
            .context(TableOperationSnafu)
    }
}
