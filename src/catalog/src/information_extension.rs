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

use std::sync::Arc;

use api::v1::meta::ProcedureStatus;
use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_meta::cluster::{ClusterInfo, NodeInfo, Role};
use common_meta::datanode::RegionStat;
use common_meta::key::flow::flow_state::FlowStat;
use common_meta::node_manager::DatanodeManagerRef;
use common_meta::procedure_executor::{ExecutorContext, ProcedureExecutor};
use common_meta::rpc::procedure;
use common_procedure::{ProcedureInfo, ProcedureState};
use common_query::request::QueryRequest;
use common_recordbatch::adapter::{AsyncRecordBatchStreamAdapter, DfRecordBatchStreamAdapter};
use common_recordbatch::util::{ChainedRecordBatchStream, LimitedRecordBatchStream};
use common_recordbatch::{
    DfSendableRecordBatchStream, EmptyRecordBatchStream, SendableRecordBatchStream,
};
use datafusion::common::Result;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datatypes::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datatypes::schema::SchemaRef;
use futures_util::StreamExt;
use futures_util::future::{join_all, try_join_all};
use meta_client::MetaClientRef;
use session::context::{QueryContext, QueryContextRef};
use snafu::{IntoError, ResultExt};
use store_api::storage::RegionId;

use crate::error;
use crate::information_schema::{DatanodeInspectRequest, InformationExtension};

pub struct DistributedInformationExtension {
    meta_client: MetaClientRef,
    datanode_manager: DatanodeManagerRef,
}

impl DistributedInformationExtension {
    pub fn new(meta_client: MetaClientRef, datanode_manager: DatanodeManagerRef) -> Self {
        Self {
            meta_client,
            datanode_manager,
        }
    }

    async fn inspect_datanode_stream(
        meta_client: MetaClientRef,
        datanode_manager: DatanodeManagerRef,
        request: DatanodeInspectRequest,
        query_ctx: Option<QueryContextRef>,
    ) -> std::result::Result<SendableRecordBatchStream, crate::error::Error> {
        let limit = request.scan.limit;
        let plan = request
            .build_plan()
            .context(crate::error::DatafusionSnafu)?;
        let stream =
            Self::inspect_query_stream(meta_client, datanode_manager, plan, query_ctx).await?;
        match limit {
            Some(limit) => Ok(Box::pin(LimitedRecordBatchStream::new(stream, limit))),
            None => Ok(stream),
        }
    }

    async fn inspect_query_stream(
        meta_client: MetaClientRef,
        datanode_manager: DatanodeManagerRef,
        plan: LogicalPlan,
        query_ctx: Option<QueryContextRef>,
    ) -> crate::error::Result<SendableRecordBatchStream> {
        let schema = inspection_query_schema(&plan)?;
        let nodes = meta_client
            .list_nodes(Some(Role::Datanode))
            .await
            .map_err(BoxedError::new)
            .context(crate::error::ListNodesSnafu)?;

        let requests = nodes.into_iter().map(|node| {
            let datanode_manager = datanode_manager.clone();
            let plan = plan.clone();
            async move {
                let client = datanode_manager.datanode(&node.peer).await;
                client
                    .handle_query(QueryRequest {
                        plan,
                        region_id: RegionId::default(),
                        header: None,
                    })
                    .await
                    .map(|stream| (node.peer.addr.clone(), stream))
                    .context(crate::error::HandleQuerySnafu {
                        addr: node.peer.addr,
                    })
            }
        });

        inspection_stream(requests, query_ctx, schema).await
    }
}

fn inspection_query_schema(plan: &LogicalPlan) -> crate::error::Result<SchemaRef> {
    datatypes::schema::Schema::try_from(plan.schema().clone())
        .map(Arc::new)
        .map_err(|error| datafusion::common::DataFusionError::External(Box::new(error)))
        .context(crate::error::DatafusionSnafu)
}

fn chain_inspection_streams(
    streams: Vec<SendableRecordBatchStream>,
    schema: SchemaRef,
) -> crate::error::Result<SendableRecordBatchStream> {
    if streams.is_empty() {
        return Ok(Box::pin(EmptyRecordBatchStream::new(schema)));
    }
    Ok(Box::pin(
        ChainedRecordBatchStream::new(streams).context(crate::error::CreateRecordBatchSnafu)?,
    ))
}

async fn inspection_stream<F>(
    requests: impl IntoIterator<Item = F>,
    query_ctx: Option<QueryContextRef>,
    schema: SchemaRef,
) -> crate::error::Result<SendableRecordBatchStream>
where
    F: std::future::Future<Output = crate::error::Result<(String, SendableRecordBatchStream)>>,
{
    if let Some(query_ctx) = query_ctx.filter(|ctx| ctx.allow_partial_inspection_results()) {
        partial_inspection_stream(join_all(requests).await, query_ctx)
    } else {
        let streams = try_join_all(requests)
            .await?
            .into_iter()
            .map(|(_, stream)| stream)
            .collect();
        chain_inspection_streams(streams, schema)
    }
}

/// Only transport unavailability and per-node deadlines may be ignored. In particular,
/// retryability alone is insufficient: it also includes routing and application errors.
fn inspection_node_unavailable(error: &dyn ErrorExt) -> bool {
    if error.status_code() == StatusCode::DeadlineExceeded || error.is_transport_unavailable() {
        return true;
    }
    let mut cause = error.source();
    while let Some(current) = cause {
        if current
            .downcast_ref::<BoxedError>()
            .is_some_and(|error| error.is_transport_unavailable())
        {
            return true;
        }
        cause = current.source();
    }
    false
}

fn record_inspection_failure(ctx: &QueryContext, addr: &str, error: &dyn ErrorExt) {
    common_telemetry::warn!(error; "Returning partial inspection results; datanode: {}", addr);
    // Keep the client notice bounded. Full errors and all affected peers are logged.
    ctx.set_warning(format!("Incomplete inspection results: datanode {addr} is unavailable; other datanodes may also have been omitted. See server logs for details."));
}

/// Retains batches already received if a node disconnects, but fails if no node
/// completes successfully. A successful empty stream counts as a successful node.
fn partial_inspection_stream(
    results: Vec<crate::error::Result<(String, SendableRecordBatchStream)>>,
    query_ctx: QueryContextRef,
) -> crate::error::Result<SendableRecordBatchStream> {
    let mut streams = Vec::new();
    let mut last_error = None;
    for result in results {
        match result {
            Ok(stream) => streams.push(stream),
            Err(error) if inspection_node_unavailable(&error) => {
                if let error::Error::HandleQuery { addr, .. } = &error {
                    record_inspection_failure(&query_ctx, addr, &error);
                }
                last_error = Some(error);
            }
            Err(error) => return Err(error),
        }
    }
    let Some((_, first)) = streams.first() else {
        return Err(last_error.unwrap_or_else(|| {
            error::CreateRecordBatchSnafu
                .into_error(common_recordbatch::error::EmptyStreamSnafu.build())
        }));
    };
    let schema = first.schema();
    // Validate every stream before yielding rows, including streams that may later fail.
    for (_, stream) in &streams {
        if stream.schema() != schema {
            return Err(error::CreateRecordBatchSnafu.into_error(
                common_recordbatch::error::SchemaNotMatchSnafu {
                    left: schema.clone(),
                    right: stream.schema(),
                }
                .build(),
            ));
        }
    }
    let stream = async_stream::try_stream! {
        let mut completed = 0;
        let mut last_error = last_error.map(|error| {
            common_recordbatch::error::ExternalSnafu.into_error(BoxedError::new(error))
        });
        for (addr, mut stream) in streams {
            loop {
                match stream.next().await {
                    Some(Ok(batch)) => yield batch,
                    Some(Err(error)) if inspection_node_unavailable(&error) => {
                        record_inspection_failure(&query_ctx, &addr, &error);
                        last_error = Some(error);
                        break;
                    }
                    Some(Err(error)) => Err(error)?,
                    None => { completed += 1; break; }
                }
            }
        }
        if completed == 0 && let Some(error) = last_error {
            Err(error)?;
        }
    };
    Ok(Box::pin(common_recordbatch::RecordBatchStreamWrapper::new(
        schema,
        Box::pin(stream),
    )))
}

#[derive(Debug, Clone)]
enum InspectPlan {
    Scan(DatanodeInspectRequest),
    Query(LogicalPlan),
}

struct DistributedInspectExec {
    plan: InspectPlan,
    meta_client: MetaClientRef,
    datanode_manager: DatanodeManagerRef,
    schema: SchemaRef,
    arrow_schema: ArrowSchemaRef,
    properties: Arc<PlanProperties>,
}

impl std::fmt::Debug for DistributedInspectExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DistributedInspectExec")
            .field("plan", &self.plan)
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl DistributedInspectExec {
    fn new(
        request: DatanodeInspectRequest,
        schema: SchemaRef,
        meta_client: MetaClientRef,
        datanode_manager: DatanodeManagerRef,
    ) -> Self {
        Self::with_plan(
            InspectPlan::Scan(request),
            schema,
            meta_client,
            datanode_manager,
        )
    }

    fn with_plan(
        plan: InspectPlan,
        schema: SchemaRef,
        meta_client: MetaClientRef,
        datanode_manager: DatanodeManagerRef,
    ) -> Self {
        let arrow_schema = schema.arrow_schema().clone();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(arrow_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            plan,
            meta_client,
            datanode_manager,
            schema,
            arrow_schema,
            properties,
        }
    }
}

impl DisplayAs for DistributedInspectExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.plan {
            InspectPlan::Scan(request) => write!(
                f,
                "DistributedInspectExec: kind={:?}, scan={}, schema={:?}",
                request.kind, request.scan, self.arrow_schema
            ),
            InspectPlan::Query(plan) => write!(
                f,
                "DistributedInspectExec: query={}, schema={:?}",
                plan.display_indent(),
                self.arrow_schema
            ),
        }
    }
}

impl ExecutionPlan for DistributedInspectExec {
    fn name(&self) -> &str {
        "DistributedInspectExec"
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.arrow_schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(datafusion::error::DataFusionError::Internal(
                "DistributedInspectExec is a leaf execution plan and cannot accept children"
                    .to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::error::Result<DfSendableRecordBatchStream> {
        if partition != 0 {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "DistributedInspectExec invalid partition. Expected 0, got {partition}"
            )));
        }

        let meta_client = self.meta_client.clone();
        let datanode_manager = self.datanode_manager.clone();
        let plan = self.plan.clone();
        let schema = self.schema.clone();
        let query_ctx = context.session_config().get_extension::<QueryContext>();
        if let Some(ctx) = &query_ctx
            && ctx.allow_partial_inspection_results()
        {
            // Streaming protocols can send notices before polling the result stream.
            ctx.set_warning(
                "Partial inspection results are enabled; unavailable datanodes may be omitted."
                    .to_string(),
            );
        }
        let future = async move {
            match plan {
                InspectPlan::Scan(request) => {
                    DistributedInformationExtension::inspect_datanode_stream(
                        meta_client,
                        datanode_manager,
                        request,
                        query_ctx,
                    )
                    .await
                }
                InspectPlan::Query(plan) => {
                    DistributedInformationExtension::inspect_query_stream(
                        meta_client,
                        datanode_manager,
                        plan,
                        query_ctx,
                    )
                    .await
                }
            }
            .map_err(BoxedError::new)
            .context(common_recordbatch::error::ExternalSnafu)
        };
        let stream = AsyncRecordBatchStreamAdapter::new(schema, Box::pin(future));
        Ok(Box::pin(DfRecordBatchStreamAdapter::new(Box::pin(stream))))
    }
}

#[async_trait::async_trait]
impl InformationExtension for DistributedInformationExtension {
    type Error = crate::error::Error;

    async fn nodes(&self) -> std::result::Result<Vec<NodeInfo>, Self::Error> {
        self.meta_client
            .list_nodes(None)
            .await
            .map_err(BoxedError::new)
            .context(error::ListNodesSnafu)
    }

    async fn procedures(&self) -> std::result::Result<Vec<(String, ProcedureInfo)>, Self::Error> {
        let procedures = self
            .meta_client
            .list_procedures(&ExecutorContext::default())
            .await
            .map_err(BoxedError::new)
            .context(error::ListProceduresSnafu)?
            .procedures;
        let mut result = Vec::with_capacity(procedures.len());
        for procedure in procedures {
            let pid = match procedure.id {
                Some(pid) => pid,
                None => return error::ProcedureIdNotFoundSnafu {}.fail(),
            };
            let pid = procedure::pb_pid_to_pid(&pid)
                .map_err(BoxedError::new)
                .context(error::ConvertProtoDataSnafu)?;
            let status = ProcedureStatus::try_from(procedure.status)
                .map(|v| v.as_str_name())
                .unwrap_or("Unknown")
                .to_string();
            let procedure_info = ProcedureInfo {
                id: pid,
                type_name: procedure.type_name,
                start_time_ms: procedure.start_time_ms,
                end_time_ms: procedure.end_time_ms,
                state: ProcedureState::Running,
                lock_keys: procedure.lock_keys,
            };
            result.push((status, procedure_info));
        }

        Ok(result)
    }

    async fn region_stats(&self) -> std::result::Result<Vec<RegionStat>, Self::Error> {
        self.meta_client
            .list_region_stats()
            .await
            .map_err(BoxedError::new)
            .context(error::ListRegionStatsSnafu)
    }

    async fn flow_stats(&self) -> std::result::Result<Option<FlowStat>, Self::Error> {
        self.meta_client
            .list_flow_stats()
            .await
            .map_err(BoxedError::new)
            .context(crate::error::ListFlowStatsSnafu)
    }

    async fn inspect_datanode(
        &self,
        request: DatanodeInspectRequest,
    ) -> std::result::Result<SendableRecordBatchStream, Self::Error> {
        Self::inspect_datanode_stream(
            self.meta_client.clone(),
            self.datanode_manager.clone(),
            request,
            None,
        )
        .await
    }

    fn supports_inspect_query_plan(&self) -> bool {
        true
    }

    fn inspect_datanode_query_plan(
        &self,
        plan: LogicalPlan,
    ) -> crate::error::Result<Option<Arc<dyn ExecutionPlan>>> {
        let schema = inspection_query_schema(&plan)?;
        Ok(Some(Arc::new(DistributedInspectExec::with_plan(
            InspectPlan::Query(plan),
            schema,
            self.meta_client.clone(),
            self.datanode_manager.clone(),
        ))))
    }

    fn inspect_datanode_plan(
        &self,
        request: DatanodeInspectRequest,
        schema: SchemaRef,
    ) -> std::result::Result<Option<Arc<dyn ExecutionPlan>>, Self::Error> {
        Ok(Some(Arc::new(DistributedInspectExec::new(
            request,
            schema,
            self.meta_client.clone(),
            self.datanode_manager.clone(),
        ))))
    }
}

#[cfg(test)]
mod tests {
    use common_meta::node_manager::{DatanodeManager, DatanodeRef};
    use common_meta::peer::Peer;
    use datafusion::common::tree_node::TreeNode;
    use datafusion::logical_expr::LogicalPlan;
    use datafusion::physical_plan::sorts::sort::SortExec;
    use meta_client::client::MetaClientBuilder;
    use store_api::sst_entry::ManifestSstEntry;
    use store_api::storage::ScanRequest;

    use super::*;
    use crate::information_schema::DatanodeInspectKind;

    #[derive(Debug)]
    struct UnavailableTransport;

    impl std::fmt::Display for UnavailableTransport {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "connection unavailable")
        }
    }
    impl std::error::Error for UnavailableTransport {}
    impl common_error::ext::StackError for UnavailableTransport {
        fn debug_fmt(&self, _: usize, _: &mut Vec<String>) {}
        fn next(&self) -> Option<&dyn common_error::ext::StackError> {
            None
        }
    }
    impl ErrorExt for UnavailableTransport {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn is_transport_unavailable(&self) -> bool {
            true
        }
    }

    #[test]
    fn partial_inspection_nested_transport_error() {
        let error = error::HandleQuerySnafu {
            addr: "unavailable",
        }
        .into_error(
            common_meta::error::ExternalSnafu.into_error(BoxedError::new(UnavailableTransport)),
        );
        assert!(inspection_node_unavailable(&error));
        let error = common_recordbatch::error::ExternalSnafu.into_error(BoxedError::new(error));
        assert!(inspection_node_unavailable(&error));
        let retryable_application_error = common_error::mock::MockError::new(StatusCode::Internal)
            .with_retry_hint(common_error::ext::RetryHint::Retryable);
        assert!(!inspection_node_unavailable(&retryable_application_error));
    }

    fn inspection_context(allow_partial: bool) -> QueryContextRef {
        let ctx = QueryContext::arc();
        ctx.configuration_parameter()
            .set_query_option(
                session::query_options::ALLOW_PARTIAL_INSPECTION_RESULTS,
                if allow_partial { "true" } else { "false" },
            )
            .unwrap();
        Arc::new(ctx.query_option_snapshot().unwrap())
    }

    fn node_error(code: StatusCode) -> crate::error::Error {
        error::HandleQuerySnafu {
            addr: "failed-node",
        }
        .into_error(
            common_meta::error::ExternalSnafu
                .into_error(BoxedError::new(common_error::mock::MockError::new(code))),
        )
    }

    fn node_stream(rows: usize, failure: Option<StatusCode>) -> SendableRecordBatchStream {
        let schema = Arc::new(datatypes::schema::Schema::new(vec![
            datatypes::schema::ColumnSchema::new(
                "value",
                datatypes::data_type::ConcreteDataType::int64_datatype(),
                false,
            ),
        ]));
        let batch = common_recordbatch::RecordBatch::new(
            schema.clone(),
            vec![
                Arc::new(datatypes::vectors::Int64Vector::from_vec(vec![42; rows]))
                    as datatypes::vectors::VectorRef,
            ],
        )
        .unwrap();
        let mut batches = vec![Ok(batch)];
        if let Some(code) = failure {
            batches.push(Err(common_recordbatch::error::ExternalSnafu.into_error(
                BoxedError::new(common_error::mock::MockError::new(code)),
            )));
        }
        Box::pin(common_recordbatch::RecordBatchStreamWrapper::new(
            schema,
            futures_util::stream::iter(batches),
        ))
    }

    #[tokio::test]
    async fn partial_inspection_ablation() {
        // Identical inputs, varying only the option; repeat to exercise stable behavior.
        for _ in 0..3 {
            for allow in [false, true] {
                for late_failure in [false, true] {
                    let ctx = inspection_context(allow);
                    let failed = if late_failure {
                        Ok((
                            "failed-node".to_string(),
                            node_stream(1, Some(StatusCode::DeadlineExceeded)),
                        ))
                    } else {
                        Err(node_error(StatusCode::DeadlineExceeded))
                    };
                    let inputs = vec![
                        failed,
                        Ok(("healthy-node".to_string(), node_stream(2, None))),
                    ];
                    let result = inspection_stream(
                        inputs.into_iter().map(std::future::ready),
                        Some(ctx.clone()),
                        node_stream(0, None).schema(),
                    )
                    .await;
                    let result = match result {
                        Ok(stream) => common_recordbatch::util::collect(stream)
                            .await
                            .map_err(|e| e.to_string()),
                        Err(error) => Err(error.to_string()),
                    };
                    if allow {
                        assert_eq!(
                            result.unwrap().iter().map(|b| b.num_rows()).sum::<usize>(),
                            if late_failure { 3 } else { 2 }
                        );
                        assert!(ctx.warning().unwrap().contains("failed-node"));
                    } else {
                        assert!(result.is_err());
                        assert!(ctx.warning().is_none());
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn partial_inspection_failure_boundaries() {
        for code in [
            StatusCode::PermissionDenied,
            StatusCode::PlanQuery,
            StatusCode::Internal,
            StatusCode::Cancelled,
        ] {
            for late in [false, true] {
                let failed = if late {
                    Ok(("failed-node".to_string(), node_stream(1, Some(code))))
                } else {
                    Err(node_error(code))
                };
                let result = inspection_stream(
                    vec![failed, Ok(("healthy".to_string(), node_stream(1, None)))]
                        .into_iter()
                        .map(std::future::ready),
                    Some(inspection_context(true)),
                    node_stream(0, None).schema(),
                )
                .await;
                match result {
                    Ok(stream) => assert!(common_recordbatch::util::collect(stream).await.is_err()),
                    Err(_) => assert!(!late),
                }
            }
        }
        // All failed must remain an error, even after yielding some rows.
        for late in [false, true] {
            let failed = if late {
                Ok((
                    "failed-node".to_string(),
                    node_stream(1, Some(StatusCode::DeadlineExceeded)),
                ))
            } else {
                Err(node_error(StatusCode::DeadlineExceeded))
            };
            let result = inspection_stream(
                vec![std::future::ready(failed)],
                Some(inspection_context(true)),
                node_stream(0, None).schema(),
            )
            .await;
            match result {
                Ok(stream) => assert!(common_recordbatch::util::collect(stream).await.is_err()),
                Err(_) => assert!(!late),
            }
        }
        // A healthy empty node distinguishes an empty result from total failure.
        let stream = inspection_stream(
            vec![
                std::future::ready(Err(node_error(StatusCode::DeadlineExceeded))),
                std::future::ready(Ok(("empty".to_string(), node_stream(0, None)))),
            ],
            Some(inspection_context(true)),
            node_stream(0, None).schema(),
        )
        .await
        .unwrap();
        assert_eq!(
            common_recordbatch::util::collect(stream)
                .await
                .unwrap()
                .iter()
                .map(|b| b.num_rows())
                .sum::<usize>(),
            0
        );
    }

    struct UnusedDatanodeManager;

    #[async_trait::async_trait]
    impl DatanodeManager for UnusedDatanodeManager {
        async fn datanode(&self, _node: &Peer) -> DatanodeRef {
            panic!("plan tests must not contact datanodes")
        }
    }

    #[test]
    fn partial_inspection_reads_task_context() {
        for allow in [false, true] {
            for pushed_query in [false, true] {
                let ctx = inspection_context(allow);
                let mut config = datafusion::execution::config::SessionConfig::new();
                config.set_extension(ctx.clone());
                let task = Arc::new(TaskContext::default().with_session_config(config));
                let request = DatanodeInspectRequest {
                    kind: DatanodeInspectKind::SstManifest,
                    scan: ScanRequest::default(),
                };
                let plan = if pushed_query {
                    InspectPlan::Query(request.build_plan().unwrap())
                } else {
                    InspectPlan::Scan(request)
                };
                let exec = DistributedInspectExec::with_plan(
                    plan,
                    ManifestSstEntry::schema(),
                    Arc::new(MetaClientBuilder::new(0, api::v1::meta::Role::Frontend).build()),
                    Arc::new(UnusedDatanodeManager),
                );
                // Both scan and pushed query plans read options without contacting a node.
                let stream = exec.execute(0, task).unwrap();
                assert_eq!(ctx.warning().is_some(), allow);
                drop(stream);
            }
        }
    }

    #[tokio::test]
    async fn partial_inspection_schema_and_empty_nodes() {
        for allow in [false, true] {
            let schema = node_stream(0, None).schema();
            let result =
                inspection_stream(
                    Vec::<
                        std::future::Ready<
                            crate::error::Result<(String, SendableRecordBatchStream)>,
                        >,
                    >::new(),
                    Some(inspection_context(allow)),
                    schema.clone(),
                )
                .await;
            if allow {
                assert!(result.is_err());
            } else {
                let mut stream = result.unwrap();
                assert_eq!(stream.schema(), schema);
                assert!(stream.next().await.is_none());
            }
        }
        let empty = common_recordbatch::RecordBatches::empty().as_stream();
        let result = inspection_stream(
            vec![
                std::future::ready(Ok(("first".to_string(), node_stream(1, None)))),
                std::future::ready(Ok(("second".to_string(), empty))),
            ],
            Some(inspection_context(true)),
            node_stream(0, None).schema(),
        )
        .await;
        assert!(matches!(
            result,
            Err(error::Error::CreateRecordBatch { .. })
        ));
    }

    #[derive(Debug)]
    struct ManifestScanProvider;

    #[async_trait::async_trait]
    impl datafusion::catalog::TableProvider for ManifestScanProvider {
        fn schema(&self) -> ArrowSchemaRef {
            ManifestSstEntry::schema().arrow_schema().clone()
        }

        fn table_type(&self) -> datafusion::logical_expr::TableType {
            datafusion::logical_expr::TableType::View
        }

        fn supports_filters_pushdown(
            &self,
            filters: &[&datafusion::logical_expr::Expr],
        ) -> Result<Vec<datafusion::logical_expr::TableProviderFilterPushDown>> {
            Ok(vec![datafusion::logical_expr::TableProviderFilterPushDown::Inexact; filters.len()])
        }

        async fn scan(
            &self,
            _state: &dyn datafusion::catalog::Session,
            projection: Option<&Vec<usize>>,
            filters: &[datafusion::logical_expr::Expr],
            limit: Option<usize>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            let schema = ManifestSstEntry::schema();
            let schema = match projection {
                Some(projection) => Arc::new(schema.try_project(projection).unwrap()),
                None => schema,
            };
            Ok(Arc::new(DistributedInspectExec::new(
                DatanodeInspectRequest {
                    kind: DatanodeInspectKind::SstManifest,
                    scan: ScanRequest {
                        projection: projection.cloned(),
                        filters: filters.to_vec(),
                        limit,
                        ..Default::default()
                    },
                },
                schema,
                Arc::new(MetaClientBuilder::new(0, api::v1::meta::Role::Frontend).build()),
                Arc::new(UnusedDatanodeManager),
            )))
        }
    }

    #[tokio::test]
    async fn test_inspection_query_plan_is_lazy_and_preserves_schema() {
        let ctx = datafusion::prelude::SessionContext::new();
        let remote = ctx
            .sql("SELECT count(*), sum(value) FROM (VALUES (1), (2)) AS t(value)")
            .await
            .unwrap()
            .into_unoptimized_plan();
        let extension = DistributedInformationExtension::new(
            Arc::new(MetaClientBuilder::new(0, api::v1::meta::Role::Frontend).build()),
            Arc::new(UnusedDatanodeManager),
        );
        assert!(extension.supports_inspect_query_plan());
        let exec = extension
            .inspect_datanode_query_plan(remote.clone())
            .unwrap()
            .unwrap();
        assert_eq!(exec.schema(), remote.schema().inner().clone());
        let inspect = exec.downcast_ref::<DistributedInspectExec>().unwrap();
        let InspectPlan::Query(plan) = &inspect.plan else {
            panic!("expected inspection query")
        };
        assert_eq!(plan, &remote);
        assert!(exec.children().is_empty());
        // Creating or dropping an unpolled execution must not contact metadata or datanodes.
        for _ in 0..2 {
            let stream = exec.execute(0, ctx.task_ctx()).unwrap();
            assert_eq!(stream.schema(), exec.schema());
            drop(stream);
        }
        assert!(exec.execute(1, ctx.task_ctx()).is_err());
    }

    #[tokio::test]
    async fn test_empty_inspection_query_stream_keeps_output_schema() {
        use futures_util::StreamExt;

        let ctx = datafusion::prelude::SessionContext::new();
        let remote = ctx
            .sql("SELECT count(*), sum(value) FROM (VALUES (1)) AS t(value)")
            .await
            .unwrap()
            .into_unoptimized_plan();
        let schema = inspection_query_schema(&remote).unwrap();
        let mut stream = chain_inspection_streams(vec![], schema.clone()).unwrap();
        assert_eq!(stream.schema(), schema);
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_sql_manifest_sort_stays_on_frontend() {
        let ctx = datafusion::prelude::SessionContext::new();
        ctx.register_table("ssts_manifest", Arc::new(ManifestScanProvider))
            .unwrap();
        for sql in [
            "SELECT region_id, file_id, min_ts, max_ts, file_size FROM ssts_manifest WHERE region_id = 42 ORDER BY min_ts LIMIT 100",
            "SELECT file_id FROM ssts_manifest WHERE region_id = 42 ORDER BY max_ts DESC LIMIT 10 OFFSET 3",
            "SELECT min_ts AS ts, file_id FROM ssts_manifest WHERE region_id = 42 ORDER BY ts DESC NULLS FIRST LIMIT 10",
            "SELECT file_id FROM ssts_manifest WHERE region_id = 42 ORDER BY min_ts, max_ts DESC LIMIT 10",
        ] {
            let plan = ctx
                .sql(sql)
                .await
                .unwrap()
                .create_physical_plan()
                .await
                .unwrap();
            let mut found_scan = false;
            let mut found_sort = false;
            plan.apply(|plan| {
                found_sort |= plan.is::<SortExec>();
                if let Some(scan) = plan.downcast_ref::<DistributedInspectExec>() {
                    found_scan = true;
                    // All matching rows must reach the frontend global sort.
                    let InspectPlan::Scan(request) = &scan.plan else {
                        panic!("expected scan plan")
                    };
                    assert_eq!(request.scan.limit, None);
                    let remote = request.clone().build_plan().unwrap();
                    remote.apply(|node| {
                        assert!(!matches!(
                            node,
                            LogicalPlan::Sort(_) | LogicalPlan::Limit(_)
                        ));
                        Ok(TreeNodeRecursion::Continue)
                    })?;
                }
                Ok(TreeNodeRecursion::Continue)
            })
            .unwrap();
            assert!(
                found_scan && found_sort,
                "{}",
                datafusion::physical_plan::displayable(plan.as_ref()).indent(true)
            );
        }
    }
}
