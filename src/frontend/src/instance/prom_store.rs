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
use api::v1::RowInsertRequests;
use async_trait::async_trait;
use auth::{PermissionChecker, PermissionCheckerRef, PermissionReq};
use client::OutputData;
use common_catalog::format_full_table_name;
use common_error::ext::BoxedError;
use common_query::prelude::GREPTIME_PHYSICAL_TABLE;
use common_query::Output;
use common_recordbatch::RecordBatches;
use common_telemetry::{debug, tracing};
use operator::insert::InserterRef;
use operator::statement::StatementExecutor;
use prost::Message;
use servers::error::{self, AuthSnafu, Result as ServerResult};
use servers::http::header::{collect_plan_metrics, CONTENT_ENCODING_SNAPPY, CONTENT_TYPE_PROTOBUF};
use servers::http::prom_store::PHYSICAL_TABLE_PARAM;
use servers::interceptor::{PromStoreProtocolInterceptor, PromStoreProtocolInterceptorRef};
use servers::prom_store::{self, Metrics};
use servers::query_handler::{
    PromStoreProtocolHandler, PromStoreProtocolHandlerRef, PromStoreResponse,
};
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt};

use crate::error::{
    CatalogSnafu, ExecLogicalPlanSnafu, PromStoreRemoteQueryPlanSnafu, ReadTableSnafu, Result,
    TableNotFoundSnafu,
};
use crate::instance::Instance;

const SAMPLES_RESPONSE_TYPE: i32 = ResponseType::Samples as i32;

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
            .table(catalog_name, schema_name, table_name)
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
impl PromStoreProtocolHandler for Instance {
    async fn write(
        &self,
        request: RowInsertRequests,
        ctx: QueryContextRef,
        with_metric_engine: bool,
    ) -> ServerResult<Output> {
        self.plugins
            .get::<PermissionCheckerRef>()
            .as_ref()
            .check_permission(ctx.current_user(), PermissionReq::PromStoreWrite)
            .context(AuthSnafu)?;
        let interceptor_ref = self
            .plugins
            .get::<PromStoreProtocolInterceptorRef<servers::error::Error>>();
        interceptor_ref.pre_write(&request, ctx.clone())?;

        let output = if with_metric_engine {
            let physical_table = ctx
                .extension(PHYSICAL_TABLE_PARAM)
                .unwrap_or(GREPTIME_PHYSICAL_TABLE)
                .to_string();
            self.handle_metric_row_inserts(request, ctx.clone(), physical_table.to_string())
                .await
                .map_err(BoxedError::new)
                .context(error::ExecuteGrpcQuerySnafu)?
        } else {
            self.handle_row_inserts(request, ctx.clone())
                .await
                .map_err(BoxedError::new)
                .context(error::ExecuteGrpcQuerySnafu)?
        };

        Ok(output)
    }

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
