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
use common_error::ext::BoxedError;
use common_meta::cluster::{ClusterInfo, NodeInfo, Role};
use common_meta::datanode::RegionStat;
use common_meta::key::flow::flow_state::FlowStat;
use common_meta::node_manager::DatanodeManagerRef;
use common_meta::procedure_executor::{ExecutorContext, ProcedureExecutor};
use common_meta::rpc::procedure;
use common_procedure::{ProcedureInfo, ProcedureState};
use common_query::request::QueryRequest;
use common_recordbatch::adapter::{AsyncRecordBatchStreamAdapter, DfRecordBatchStreamAdapter};
use common_recordbatch::filter::MetadataFilter;
use common_recordbatch::util::{ChainedRecordBatchStream, LimitedRecordBatchStream};
use common_recordbatch::{
    DfSendableRecordBatchStream, EmptyRecordBatchStream, SendableRecordBatchStream,
};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Result, ScalarValue};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datatypes::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datatypes::schema::SchemaRef;
use futures_util::future::try_join_all;
use meta_client::MetaClientRef;
use snafu::ResultExt;
use store_api::compaction_task;
use store_api::storage::RegionId;

use crate::error;
use crate::information_schema::{
    DatanodeInspectKind, DatanodeInspectRequest, InformationExtension,
};

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
    ) -> crate::error::Result<SendableRecordBatchStream> {
        let nodes = meta_client
            .list_nodes(Some(Role::Datanode))
            .await
            .map_err(BoxedError::new)
            .context(crate::error::ListNodesSnafu)?;
        Self::inspect_nodes(datanode_manager, request, nodes).await
    }

    /// Executes inspection against discovered nodes after applying safe node pruning.
    async fn inspect_nodes(
        datanode_manager: DatanodeManagerRef,
        request: DatanodeInspectRequest,
        nodes: Vec<NodeInfo>,
    ) -> crate::error::Result<SendableRecordBatchStream> {
        let nodes = prune_compaction_nodes(&request, nodes);
        let limit = request.scan.limit;
        let plan = request
            .build_plan()
            .context(crate::error::DatafusionSnafu)?;
        let stream = Self::inspect_query_nodes(datanode_manager, plan, nodes).await?;
        match limit {
            Some(limit) => Ok(Box::pin(LimitedRecordBatchStream::new(stream, limit))),
            None => Ok(stream),
        }
    }

    async fn inspect_query_stream(
        meta_client: MetaClientRef,
        datanode_manager: DatanodeManagerRef,
        plan: LogicalPlan,
    ) -> crate::error::Result<SendableRecordBatchStream> {
        let nodes = meta_client
            .list_nodes(Some(Role::Datanode))
            .await
            .map_err(BoxedError::new)
            .context(crate::error::ListNodesSnafu)?;
        Self::inspect_query_nodes(datanode_manager, plan, nodes).await
    }

    /// Shares query dispatch and empty-stream schema handling for scans and query plans.
    async fn inspect_query_nodes(
        datanode_manager: DatanodeManagerRef,
        plan: LogicalPlan,
        nodes: Vec<NodeInfo>,
    ) -> crate::error::Result<SendableRecordBatchStream> {
        let schema = inspection_query_schema(&plan)?;
        let streams = try_join_all(nodes.into_iter().map(|node| {
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
                    .context(crate::error::HandleQuerySnafu {
                        addr: node.peer.addr,
                    })
            }
        }))
        .await?;

        chain_inspection_streams(streams, schema)
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

#[derive(Debug, Clone)]
enum InspectPlan {
    Scan(DatanodeInspectRequest),
    Query(LogicalPlan),
}

/// Prunes reporting nodes only for live compaction inspection. Region routes cannot
/// locate tasks left on a former owner, so only known node IDs can exclude nodes.
fn prune_compaction_nodes(request: &DatanodeInspectRequest, nodes: Vec<NodeInfo>) -> Vec<NodeInfo> {
    if request.kind != DatanodeInspectKind::CompactionTasks {
        return nodes;
    }
    let filter = MetadataFilter::new(&request.scan.filters);
    nodes
        .into_iter()
        .filter(|node| {
            filter.matches(|name| {
                (name == compaction_task::NODE_ID)
                    .then_some(ScalarValue::UInt64(Some(node.peer.id)))
            })
        })
        .collect()
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
        _context: Arc<TaskContext>,
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
        let future = async move {
            match plan {
                InspectPlan::Scan(request) => {
                    DistributedInformationExtension::inspect_datanode_stream(
                        meta_client,
                        datanode_manager,
                        request,
                    )
                    .await
                }
                InspectPlan::Query(plan) => {
                    DistributedInformationExtension::inspect_query_stream(
                        meta_client,
                        datanode_manager,
                        plan,
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
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::logical_expr::{Expr, LogicalPlan};
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::prelude::SessionContext;
    use datatypes::arrow::datatypes::DataType;
    use meta_client::client::MetaClientBuilder;
    use store_api::sst_entry::ManifestSstEntry;
    use store_api::storage::ScanRequest;

    use store_api::compaction_task::CompactionTaskEntry;

    use super::*;

    struct UnusedDatanodeManager;

    #[async_trait::async_trait]
    impl DatanodeManager for UnusedDatanodeManager {
        async fn datanode(&self, _node: &Peer) -> DatanodeRef {
            panic!("this test must not contact datanodes")
        }
    }

    fn inspection_nodes() -> Vec<NodeInfo> {
        [1, 2, 3]
            .into_iter()
            .map(|id| NodeInfo {
                peer: Peer::new(id, format!("datanode-{id}:3001")),
                last_activity_ts: 0,
                status: common_meta::cluster::NodeStatus::Standalone,
                version: String::new(),
                git_commit: String::new(),
                start_time_ms: 0,
                total_cpu_millicores: 0,
                total_memory_bytes: 0,
                cpu_usage_millicores: 0,
                memory_usage_bytes: 0,
                hostname: String::new(),
                env_vars: Default::default(),
                schedulable: None,
            })
            .collect()
    }

    /// Uses the default optimizer to produce the predicates seen by inspection scans.
    async fn compaction_sql_filters(predicate: Option<&str>) -> Vec<Expr> {
        let ctx = SessionContext::new();
        ctx.register_table(
            "compaction_tasks",
            Arc::new(EmptyTable::new(
                CompactionTaskEntry::schema().arrow_schema().clone(),
            )),
        )
        .unwrap();
        let mut sql = "SELECT * FROM compaction_tasks".to_string();
        if let Some(predicate) = predicate {
            sql.push_str(" WHERE ");
            sql.push_str(predicate);
        }
        let plan = ctx.sql(&sql).await.unwrap().into_optimized_plan().unwrap();
        let mut filters = Vec::new();
        plan.apply(|plan| {
            match plan {
                LogicalPlan::Filter(filter) => filters.push(filter.predicate.clone()),
                LogicalPlan::TableScan(scan) => filters.extend(scan.filters.iter().cloned()),
                _ => {}
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
        filters
    }

    #[tokio::test]
    async fn test_compaction_node_pruning() {
        for (predicate, expected, expect_cast) in [
            (Some("node_id = 2"), vec![2], false),
            (Some("node_id != 2"), vec![1, 3], false),
            (Some("node_id > 2"), vec![3], false),
            (Some("node_id >= 2"), vec![2, 3], false),
            (Some("node_id < 2"), vec![1], false),
            (Some("node_id <= 2"), vec![1, 2], false),
            (Some("2 = node_id"), vec![2], false),
            (Some("2 < node_id"), vec![3], false),
            (None, vec![1, 2, 3], false),
            (Some("state = 'running'"), vec![1, 2, 3], false),
            (Some("node_id = 2 OR table_id = 42"), vec![1, 2, 3], false),
            (Some("node_id = 2 AND state = 'running'"), vec![2], false),
        ] {
            let filters = compaction_sql_filters(predicate).await;
            let mut has_cast = false;
            for filter in &filters {
                filter
                    .apply(|expr| {
                        has_cast |= matches!(expr, Expr::Cast(cast) if cast.field.data_type() == &DataType::Decimal128(20, 0)
                            && matches!(cast.expr.as_ref(), Expr::Column(_)));
                        Ok(TreeNodeRecursion::Continue)
                    })
                    .unwrap();
            }
            assert_eq!(has_cast, expect_cast, "optimized predicate: {predicate:?}");
            let request = DatanodeInspectRequest {
                kind: DatanodeInspectKind::CompactionTasks,
                scan: ScanRequest {
                    filters,
                    ..Default::default()
                },
            };
            let actual: Vec<_> = prune_compaction_nodes(&request, inspection_nodes())
                .into_iter()
                .map(|node| node.peer.id)
                .collect();
            assert_eq!(
                actual, expected,
                "{predicate:?}: {:?}",
                request.scan.filters
            );
        }
        let filters = compaction_sql_filters(Some("node_id = 99")).await;
        for kind in [
            DatanodeInspectKind::SstManifest,
            DatanodeInspectKind::SstStorage,
            DatanodeInspectKind::SstIndexMeta,
            DatanodeInspectKind::RegionInfo,
        ] {
            let request = DatanodeInspectRequest {
                kind,
                scan: ScanRequest {
                    filters: filters.clone(),
                    ..Default::default()
                },
            };
            let actual: Vec<_> = prune_compaction_nodes(&request, inspection_nodes())
                .into_iter()
                .map(|node| node.peer.id)
                .collect();
            assert_eq!(actual, vec![1, 2, 3]);
        }
    }

    #[tokio::test]
    async fn test_compaction_empty_nodes_preserve_projected_schema() {
        let filters = compaction_sql_filters(Some("node_id = 99")).await;
        for nodes in [vec![], inspection_nodes()] {
            for (projection, expected) in [
                (
                    vec![3, 0, 2],
                    vec![
                        ("node_id", DataType::UInt64, true),
                        ("task_id", DataType::Utf8, false),
                        ("table_id", DataType::UInt32, false),
                    ],
                ),
                (vec![], vec![]),
            ] {
                let request = DatanodeInspectRequest {
                    kind: DatanodeInspectKind::CompactionTasks,
                    scan: ScanRequest {
                        projection: Some(projection),
                        filters: filters.clone(),
                        ..Default::default()
                    },
                };
                // Panics if the production dispatch path contacts any excluded node.
                let stream = DistributedInformationExtension::inspect_nodes(
                    Arc::new(UnusedDatanodeManager),
                    request,
                    nodes.clone(),
                )
                .await
                .unwrap();
                let batches = common_recordbatch::RecordBatches::try_collect(stream)
                    .await
                    .unwrap();
                assert_eq!(
                    batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                    0
                );
                let schema = batches.schema();
                let actual: Vec<_> = schema
                    .arrow_schema()
                    .fields()
                    .iter()
                    .map(|field| {
                        (
                            field.name().as_str(),
                            field.data_type().clone(),
                            field.is_nullable(),
                        )
                    })
                    .collect();
                assert_eq!(actual, expected);
            }
        }
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
