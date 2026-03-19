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

use api::prom_store::remote::read_request::ResponseType;
use api::prom_store::remote::{Query, QueryResult, ReadRequest, ReadResponse};
use api::v1::alter_table_expr::Kind;
use api::v1::{
    AddColumn, AddColumns, AlterTableExpr, ColumnDataType, ColumnDef, CreateTableExpr,
    RowInsertRequests, SemanticType,
};
use async_trait::async_trait;
use auth::{PermissionChecker, PermissionCheckerRef, PermissionReq};
use client::OutputData;
use common_catalog::format_full_table_name;
use common_error::ext::BoxedError;
use common_query::Output;
use common_query::prelude::GREPTIME_PHYSICAL_TABLE;
use common_recordbatch::RecordBatches;
use common_telemetry::{debug, tracing};
use operator::insert::{
    AutoCreateTableType, InserterRef, build_create_table_expr, fill_table_options_for_create,
};
use operator::statement::StatementExecutor;
use prost::Message;
use servers::error::{self, AuthSnafu, Result as ServerResult};
use servers::http::header::{CONTENT_ENCODING_SNAPPY, CONTENT_TYPE_PROTOBUF, collect_plan_metrics};
use servers::http::prom_store::PHYSICAL_TABLE_PARAM;
use servers::interceptor::{PromStoreProtocolInterceptor, PromStoreProtocolInterceptorRef};
use servers::pending_rows_batcher::PendingRowsSchemaAlterer;
use servers::prom_store::{self, Metrics};
use servers::query_handler::{
    PromStoreProtocolHandler, PromStoreProtocolHandlerRef, PromStoreResponse,
};
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt};
use store_api::metric_engine_consts::{METRIC_ENGINE_NAME, PHYSICAL_TABLE_METADATA_KEY};
use store_api::mito_engine_options::SST_FORMAT_KEY;
use table::table_reference::TableReference;
use tracing::instrument;

use crate::error::{
    CatalogSnafu, ExecLogicalPlanSnafu, PromStoreRemoteQueryPlanSnafu, ReadTableSnafu, Result,
    TableNotFoundSnafu,
};
use crate::instance::Instance;

const SAMPLES_RESPONSE_TYPE: i32 = ResponseType::Samples as i32;

fn auto_create_table_type_for_prom_remote_write(
    ctx: &QueryContextRef,
    with_metric_engine: bool,
) -> AutoCreateTableType {
    if with_metric_engine {
        let physical_table = ctx
            .extension(PHYSICAL_TABLE_PARAM)
            .unwrap_or(GREPTIME_PHYSICAL_TABLE)
            .to_string();
        AutoCreateTableType::Logical(physical_table)
    } else {
        AutoCreateTableType::Physical
    }
}

fn required_physical_table_for_create_type(create_type: &AutoCreateTableType) -> Option<&str> {
    match create_type {
        AutoCreateTableType::Logical(physical_table) => Some(physical_table.as_str()),
        _ => None,
    }
}

#[inline]
fn is_supported(response_type: i32) -> bool {
    // Only supports samples response right now
    response_type == SAMPLES_RESPONSE_TYPE
}

/// Negotiating the content type of the remote read response.
///
/// Response types are taken from the list in the FIFO order. If no response type in `accepted_response_types` is
/// implemented by server, error is returned.
/// For request that do not contain `accepted_response_types` field the SAMPLES response type will be used.
fn negotiate_response_type(accepted_response_types: &[i32]) -> ServerResult<ResponseType> {
    if accepted_response_types.is_empty() {
        return Ok(ResponseType::Samples);
    }

    let response_type = accepted_response_types
        .iter()
        .find(|t| is_supported(**t))
        .with_context(|| error::NotSupportedSnafu {
            feat: format!(
                "server does not support any of the requested response types: {accepted_response_types:?}",
            ),
        })?;

    // It's safe to unwrap here, we known that it should be SAMPLES_RESPONSE_TYPE
    Ok(ResponseType::try_from(*response_type).unwrap())
}

#[instrument(skip_all, fields(table_name))]
async fn to_query_result(table_name: &str, output: Output) -> ServerResult<QueryResult> {
    let OutputData::Stream(stream) = output.data else {
        unreachable!()
    };
    let recordbatches = RecordBatches::try_collect(stream)
        .await
        .context(error::CollectRecordbatchSnafu)?;
    Ok(QueryResult {
        timeseries: prom_store::recordbatches_to_timeseries(table_name, recordbatches)?,
    })
}

impl Instance {
    #[tracing::instrument(skip_all)]
    async fn handle_remote_query(
        &self,
        ctx: &QueryContextRef,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        query: &Query,
    ) -> Result<Output> {
        let table = self
            .catalog_manager
            .table(catalog_name, schema_name, table_name, Some(ctx))
            .await
            .context(CatalogSnafu)?
            .with_context(|| TableNotFoundSnafu {
                table_name: format_full_table_name(catalog_name, schema_name, table_name),
            })?;

        let dataframe = self
            .query_engine
            .read_table(table)
            .with_context(|_| ReadTableSnafu {
                table_name: format_full_table_name(catalog_name, schema_name, table_name),
            })?;

        let logical_plan =
            prom_store::query_to_plan(dataframe, query).context(PromStoreRemoteQueryPlanSnafu)?;

        debug!(
            "Prometheus remote read, table: {}, logical plan: {}",
            table_name,
            logical_plan.display_indent(),
        );

        self.query_engine
            .execute(logical_plan, ctx.clone())
            .await
            .context(ExecLogicalPlanSnafu)
    }

    #[tracing::instrument(skip_all)]
    async fn handle_remote_queries(
        &self,
        ctx: QueryContextRef,
        queries: &[Query],
    ) -> ServerResult<Vec<(String, Output)>> {
        let mut results = Vec::with_capacity(queries.len());

        let catalog_name = ctx.current_catalog();
        let schema_name = ctx.current_schema();

        for query in queries {
            let table_name = prom_store::table_name(query)?;

            let output = self
                .handle_remote_query(&ctx, catalog_name, &schema_name, &table_name, query)
                .await
                .map_err(BoxedError::new)
                .context(error::ExecuteQuerySnafu)?;

            results.push((table_name, output));
        }
        Ok(results)
    }
}

#[async_trait]
impl PendingRowsSchemaAlterer for Instance {
    async fn create_table_if_missing(
        &self,
        catalog: &str,
        schema: &str,
        table_name: &str,
        request_schema: &[api::v1::ColumnSchema],
        with_metric_engine: bool,
        ctx: QueryContextRef,
    ) -> ServerResult<()> {
        let table = self
            .catalog_manager()
            .table(catalog, schema, table_name, Some(ctx.as_ref()))
            .await
            .map_err(BoxedError::new)
            .context(error::ExecuteGrpcQuerySnafu)?;
        if table.is_some() {
            return Ok(());
        }

        let create_type = auto_create_table_type_for_prom_remote_write(&ctx, with_metric_engine);
        if let Some(physical_table) = required_physical_table_for_create_type(&create_type) {
            self.create_metric_physical_table_if_missing(
                catalog,
                schema,
                physical_table,
                ctx.clone(),
            )
            .await?;
        }

        let table_ref = TableReference::full(catalog, schema, table_name);
        let engine = if matches!(create_type, AutoCreateTableType::Logical(_)) {
            METRIC_ENGINE_NAME
        } else {
            common_catalog::consts::default_engine()
        };
        let mut create_table_expr = build_create_table_expr(&table_ref, request_schema, engine)
            .map_err(BoxedError::new)
            .context(error::ExecuteGrpcQuerySnafu)?;

        let mut table_options = std::collections::HashMap::with_capacity(4);
        fill_table_options_for_create(&mut table_options, &create_type, &ctx);
        match create_type {
            AutoCreateTableType::Logical(_) => {
                create_table_expr.table_options.extend(table_options);
                self.statement_executor
                    .create_logical_tables(&[create_table_expr], ctx)
                    .await
                    .map_err(BoxedError::new)
                    .context(error::ExecuteGrpcQuerySnafu)?;
            }
            AutoCreateTableType::Physical => {
                // Pending-rows writes benefit from the flat SST format to leverage bulk memtables,
                // so we force `sst_format=flat` only when auto-creating physical tables.
                table_options.insert(SST_FORMAT_KEY.to_string(), "flat".to_string());
                create_table_expr.table_options.extend(table_options);
                self.statement_executor
                    .create_table_inner(&mut create_table_expr, None, ctx)
                    .await
                    .map_err(BoxedError::new)
                    .context(error::ExecuteGrpcQuerySnafu)?;
            }
            _ => {
                unreachable!("prom remote write only supports logical or physical auto-create");
            }
        }

        Ok(())
    }

    async fn add_missing_prom_tag_columns(
        &self,
        catalog: &str,
        schema: &str,
        table_name: &str,
        columns: &[String],
        ctx: QueryContextRef,
    ) -> ServerResult<()> {
        if columns.is_empty() {
            return Ok(());
        }

        let add_columns = AddColumns {
            add_columns: columns
                .iter()
                .map(|column_name| AddColumn {
                    column_def: Some(ColumnDef {
                        name: column_name.clone(),
                        data_type: ColumnDataType::String as i32,
                        is_nullable: true,
                        semantic_type: SemanticType::Tag as i32,
                        comment: String::new(),
                        ..Default::default()
                    }),
                    location: None,
                    add_if_not_exists: true,
                })
                .collect(),
        };

        self.statement_executor
            .alter_table_inner(
                AlterTableExpr {
                    catalog_name: catalog.to_string(),
                    schema_name: schema.to_string(),
                    table_name: table_name.to_string(),
                    kind: Some(Kind::AddColumns(add_columns)),
                },
                ctx,
            )
            .await
            .map_err(BoxedError::new)
            .context(error::ExecuteGrpcQuerySnafu)?;

        Ok(())
    }

    async fn create_tables_if_missing_batch(
        &self,
        catalog: &str,
        schema: &str,
        tables: &[(&str, &[api::v1::ColumnSchema])],
        with_metric_engine: bool,
        ctx: QueryContextRef,
    ) -> ServerResult<()> {
        if tables.is_empty() {
            return Ok(());
        }

        let create_type = auto_create_table_type_for_prom_remote_write(&ctx, with_metric_engine);
        if let Some(physical_table) = required_physical_table_for_create_type(&create_type) {
            self.create_metric_physical_table_if_missing(
                catalog,
                schema,
                physical_table,
                ctx.clone(),
            )
            .await?;
        }

        let engine = if matches!(create_type, AutoCreateTableType::Logical(_)) {
            METRIC_ENGINE_NAME
        } else {
            common_catalog::consts::default_engine()
        };

        // Check which tables actually still need to be created (may have been
        // concurrently created by another request).
        let mut create_exprs: Vec<CreateTableExpr> = Vec::with_capacity(tables.len());
        for &(table_name, request_schema) in tables {
            let existing = self
                .catalog_manager()
                .table(catalog, schema, table_name, Some(ctx.as_ref()))
                .await
                .map_err(BoxedError::new)
                .context(error::ExecuteGrpcQuerySnafu)?;
            if existing.is_some() {
                continue;
            }

            let table_ref = TableReference::full(catalog, schema, table_name);
            let mut create_table_expr = build_create_table_expr(&table_ref, request_schema, engine)
                .map_err(BoxedError::new)
                .context(error::ExecuteGrpcQuerySnafu)?;

            let mut table_options = std::collections::HashMap::with_capacity(4);
            fill_table_options_for_create(&mut table_options, &create_type, &ctx);
            create_table_expr.table_options.extend(table_options);
            create_exprs.push(create_table_expr);
        }

        if create_exprs.is_empty() {
            return Ok(());
        }

        match create_type {
            AutoCreateTableType::Logical(_) => {
                // Use the batch API for logical tables.
                self.statement_executor
                    .create_logical_tables(&create_exprs, ctx)
                    .await
                    .map_err(BoxedError::new)
                    .context(error::ExecuteGrpcQuerySnafu)?;
            }
            AutoCreateTableType::Physical => {
                // Physical tables don't have a batch DDL path; create one at a time.
                for mut expr in create_exprs {
                    expr.table_options
                        .insert(SST_FORMAT_KEY.to_string(), "flat".to_string());
                    self.statement_executor
                        .create_table_inner(&mut expr, None, ctx.clone())
                        .await
                        .map_err(BoxedError::new)
                        .context(error::ExecuteGrpcQuerySnafu)?;
                }
            }
            _ => {
                unreachable!("prom remote write only supports logical or physical auto-create");
            }
        }

        Ok(())
    }

    async fn add_missing_prom_tag_columns_batch(
        &self,
        catalog: &str,
        schema: &str,
        tables: &[(&str, &[String])],
        ctx: QueryContextRef,
    ) -> ServerResult<()> {
        if tables.is_empty() {
            return Ok(());
        }

        let alter_exprs: Vec<AlterTableExpr> = tables
            .iter()
            .filter(|(_, columns)| !columns.is_empty())
            .map(|&(table_name, columns)| {
                let add_columns = AddColumns {
                    add_columns: columns
                        .iter()
                        .map(|column_name| AddColumn {
                            column_def: Some(ColumnDef {
                                name: column_name.clone(),
                                data_type: ColumnDataType::String as i32,
                                is_nullable: true,
                                semantic_type: SemanticType::Tag as i32,
                                comment: String::new(),
                                ..Default::default()
                            }),
                            location: None,
                            add_if_not_exists: true,
                        })
                        .collect(),
                };

                AlterTableExpr {
                    catalog_name: catalog.to_string(),
                    schema_name: schema.to_string(),
                    table_name: table_name.to_string(),
                    kind: Some(Kind::AddColumns(add_columns)),
                }
            })
            .collect();

        if alter_exprs.is_empty() {
            return Ok(());
        }

        self.statement_executor
            .alter_logical_tables(alter_exprs, ctx)
            .await
            .map_err(BoxedError::new)
            .context(error::ExecuteGrpcQuerySnafu)?;

        Ok(())
    }
}

#[async_trait]
impl PromStoreProtocolHandler for Instance {
    async fn pre_write(
        &self,
        request: &RowInsertRequests,
        ctx: QueryContextRef,
    ) -> ServerResult<()> {
        self.plugins
            .get::<PermissionCheckerRef>()
            .as_ref()
            .check_permission(ctx.current_user(), PermissionReq::PromStoreWrite)
            .context(AuthSnafu)?;
        let interceptor_ref = self
            .plugins
            .get::<PromStoreProtocolInterceptorRef<servers::error::Error>>();
        interceptor_ref.pre_write(request, ctx)?;
        Ok(())
    }

    async fn write(
        &self,
        request: RowInsertRequests,
        ctx: QueryContextRef,
        with_metric_engine: bool,
    ) -> ServerResult<Output> {
        self.pre_write(&request, ctx.clone()).await?;

        let output = if with_metric_engine {
            let physical_table = ctx
                .extension(PHYSICAL_TABLE_PARAM)
                .unwrap_or(GREPTIME_PHYSICAL_TABLE)
                .to_string();
            self.handle_metric_row_inserts(request, ctx.clone(), physical_table.clone())
                .await
                .map_err(BoxedError::new)
                .context(error::ExecuteGrpcQuerySnafu)?
        } else {
            self.handle_row_inserts(request, ctx.clone(), true, true)
                .await
                .map_err(BoxedError::new)
                .context(error::ExecuteGrpcQuerySnafu)?
        };

        Ok(output)
    }

    #[instrument(skip_all, fields(table_name))]
    async fn read(
        &self,
        request: ReadRequest,
        ctx: QueryContextRef,
    ) -> ServerResult<PromStoreResponse> {
        self.plugins
            .get::<PermissionCheckerRef>()
            .as_ref()
            .check_permission(ctx.current_user(), PermissionReq::PromStoreRead)
            .context(AuthSnafu)?;
        let interceptor_ref = self
            .plugins
            .get::<PromStoreProtocolInterceptorRef<servers::error::Error>>();
        interceptor_ref.pre_read(&request, ctx.clone())?;

        let response_type = negotiate_response_type(&request.accepted_response_types)?;

        // TODO(dennis): use read_hints to speedup query if possible
        let results = self.handle_remote_queries(ctx, &request.queries).await?;

        match response_type {
            ResponseType::Samples => {
                let mut query_results = Vec::with_capacity(results.len());
                let mut map = HashMap::new();
                for (table_name, output) in results {
                    let plan = output.meta.plan.clone();
                    query_results.push(to_query_result(&table_name, output).await?);
                    if let Some(ref plan) = plan {
                        collect_plan_metrics(plan, &mut [&mut map]);
                    }
                }

                let response = ReadResponse {
                    results: query_results,
                };

                let resp_metrics = map
                    .into_iter()
                    .map(|(k, v)| (k, v.into()))
                    .collect::<HashMap<_, _>>();

                // TODO(dennis): may consume too much memory, adds flow control
                Ok(PromStoreResponse {
                    content_type: CONTENT_TYPE_PROTOBUF.clone(),
                    content_encoding: CONTENT_ENCODING_SNAPPY.clone(),
                    resp_metrics,
                    body: prom_store::snappy_compress(&response.encode_to_vec())?,
                })
            }
            ResponseType::StreamedXorChunks => error::NotSupportedSnafu {
                feat: "streamed remote read",
            }
            .fail(),
        }
    }

    async fn ingest_metrics(&self, _metrics: Metrics) -> ServerResult<()> {
        todo!();
    }
}

impl Instance {
    async fn create_metric_physical_table_if_missing(
        &self,
        catalog: &str,
        schema: &str,
        physical_table: &str,
        ctx: QueryContextRef,
    ) -> ServerResult<()> {
        let table = self
            .catalog_manager()
            .table(catalog, schema, physical_table, Some(ctx.as_ref()))
            .await
            .map_err(BoxedError::new)
            .context(error::ExecuteGrpcQuerySnafu)?;
        if table.is_some() {
            return Ok(());
        }

        let table_ref = TableReference::full(catalog, schema, physical_table);
        let default_schema = vec![
            api::v1::ColumnSchema {
                column_name: common_query::prelude::greptime_timestamp().to_string(),
                datatype: api::v1::ColumnDataType::TimestampMillisecond as i32,
                semantic_type: api::v1::SemanticType::Timestamp as i32,
                datatype_extension: None,
                options: None,
            },
            api::v1::ColumnSchema {
                column_name: common_query::prelude::greptime_value().to_string(),
                datatype: api::v1::ColumnDataType::Float64 as i32,
                semantic_type: api::v1::SemanticType::Field as i32,
                datatype_extension: None,
                options: None,
            },
        ];
        let mut create_table_expr = build_create_table_expr(
            &table_ref,
            &default_schema,
            common_catalog::consts::default_engine(),
        )
        .map_err(BoxedError::new)
        .context(error::ExecuteGrpcQuerySnafu)?;
        create_table_expr.engine = METRIC_ENGINE_NAME.to_string();
        create_table_expr
            .table_options
            .insert(PHYSICAL_TABLE_METADATA_KEY.to_string(), "true".to_string());

        self.statement_executor
            .create_table_inner(&mut create_table_expr, None, ctx)
            .await
            .map_err(BoxedError::new)
            .context(error::ExecuteGrpcQuerySnafu)?;

        Ok(())
    }
}

/// This handler is mainly used for `frontend` or `standalone` to directly import
/// the metrics collected by itself, thereby avoiding importing metrics through the network,
/// thus reducing compression and network transmission overhead,
/// so only implement `PromStoreProtocolHandler::write` method.
pub struct ExportMetricHandler {
    inserter: InserterRef,
    statement_executor: Arc<StatementExecutor>,
}

impl ExportMetricHandler {
    pub fn new_handler(
        inserter: InserterRef,
        statement_executor: Arc<StatementExecutor>,
    ) -> PromStoreProtocolHandlerRef {
        Arc::new(Self {
            inserter,
            statement_executor,
        })
    }
}

#[async_trait]
impl PromStoreProtocolHandler for ExportMetricHandler {
    async fn write(
        &self,
        request: RowInsertRequests,
        ctx: QueryContextRef,
        _: bool,
    ) -> ServerResult<Output> {
        self.inserter
            .handle_metric_row_inserts(
                request,
                ctx,
                &self.statement_executor,
                GREPTIME_PHYSICAL_TABLE.to_string(),
            )
            .await
            .map_err(BoxedError::new)
            .context(error::ExecuteGrpcQuerySnafu)
    }

    async fn read(
        &self,
        _request: ReadRequest,
        _ctx: QueryContextRef,
    ) -> ServerResult<PromStoreResponse> {
        unreachable!();
    }

    async fn ingest_metrics(&self, _metrics: Metrics) -> ServerResult<()> {
        unreachable!();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use session::context::QueryContext;

    use super::*;

    #[test]
    fn test_auto_create_table_type_for_prom_remote_write_metric_engine() {
        let mut query_ctx = QueryContext::with(
            common_catalog::consts::DEFAULT_CATALOG_NAME,
            common_catalog::consts::DEFAULT_SCHEMA_NAME,
        );
        query_ctx.set_extension(PHYSICAL_TABLE_PARAM, "metric_physical".to_string());
        let ctx = Arc::new(query_ctx);

        let create_type = auto_create_table_type_for_prom_remote_write(&ctx, true);
        match create_type {
            AutoCreateTableType::Logical(physical) => assert_eq!(physical, "metric_physical"),
            _ => panic!("expected logical table create type"),
        }
    }

    #[test]
    fn test_auto_create_table_type_for_prom_remote_write_without_metric_engine() {
        let ctx = Arc::new(QueryContext::with(
            common_catalog::consts::DEFAULT_CATALOG_NAME,
            common_catalog::consts::DEFAULT_SCHEMA_NAME,
        ));

        let create_type = auto_create_table_type_for_prom_remote_write(&ctx, false);
        match create_type {
            AutoCreateTableType::Physical => {}
            _ => panic!("expected physical table create type"),
        }
    }

    #[test]
    fn test_required_physical_table_for_create_type() {
        let logical = AutoCreateTableType::Logical("phy_table".to_string());
        assert_eq!(
            Some("phy_table"),
            required_physical_table_for_create_type(&logical)
        );

        let physical = AutoCreateTableType::Physical;
        assert_eq!(None, required_physical_table_for_create_type(&physical));
    }
}
