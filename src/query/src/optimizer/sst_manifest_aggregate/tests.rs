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

use std::sync::atomic::{AtomicUsize, Ordering};

use catalog::information_schema::{DatanodeInspectRequest, InformationExtension};
use common_meta::cluster::NodeInfo;
use common_meta::datanode::RegionStat;
use common_meta::key::flow::flow_state::FlowStat;
use common_procedure::ProcedureInfo;
use common_recordbatch::SendableRecordBatchStream;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::{MemTable, provider_as_source};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::SendableRecordBatchStream as DfStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_planner::DefaultPhysicalPlanner;
use datafusion::prelude::SessionContext;
use datafusion_optimizer::Analyzer;
use store_api::data_source::DataSource;
use table::metadata::{FilterPushDownType, TableInfoBuilder, TableMetaBuilder};
use table::table::Table;

use super::*;

struct PlanningOnlySource;
impl DataSource for PlanningOnlySource {
    fn get_stream(
        &self,
        _request: ScanRequest,
    ) -> std::result::Result<SendableRecordBatchStream, common_error::ext::BoxedError> {
        unreachable!("tests replace table sources before execution")
    }
}

fn table_provider(table_id: u32) -> Arc<DfTableProviderAdapter> {
    let schema = ManifestSstEntry::schema();
    let next_column_id = u32::try_from(schema.num_columns()).unwrap();
    let meta = TableMetaBuilder::new_external_table()
        .schema(schema)
        .next_column_id(next_column_id)
        .build()
        .unwrap();
    let info = TableInfoBuilder::new("ssts_manifest", meta)
        .table_id(table_id)
        .schema_name(consts::INFORMATION_SCHEMA_NAME)
        .build()
        .unwrap();
    Arc::new(DfTableProviderAdapter::new(Arc::new(Table::new(
        Arc::new(info),
        FilterPushDownType::Unsupported,
        Arc::new(PlanningOnlySource),
    ))))
}

fn batch(sizes: Vec<u64>) -> RecordBatch {
    let entries = sizes
        .into_iter()
        .enumerate()
        .map(|(index, file_size)| ManifestSstEntry {
            table_dir: "test/".into(),
            region_id: 42.into(),
            table_id: 0,
            region_number: 42,
            region_group: 0,
            region_sequence: 42,
            file_id: index.to_string(),
            index_version: 0,
            level: 0,
            file_path: format!("test/{index}.parquet"),
            file_size,
            max_row_group_uncompressed_size: 0,
            index_file_path: None,
            index_file_size: None,
            num_rows: 1,
            num_row_groups: 1,
            num_series: None,
            min_ts: common_time::Timestamp::new_millisecond(index as i64),
            max_ts: common_time::Timestamp::new_millisecond(index as i64),
            sequence: None,
            partition_expr: None,
            origin_region_id: 42.into(),
            node_id: None,
            visible: true,
            primary_key_min: None,
            primary_key_max: None,
        })
        .collect::<Vec<_>>();
    ManifestSstEntry::to_record_batch(&entries).unwrap()
}

fn replace_source(plan: LogicalPlan, batches: Vec<RecordBatch>) -> LogicalPlan {
    let table = Arc::new(
        MemTable::try_new(
            ManifestSstEntry::schema().arrow_schema().clone(),
            vec![batches],
        )
        .unwrap(),
    );
    plan.transform_up(|plan| {
        if let LogicalPlan::TableScan(mut scan) = plan {
            scan.source = provider_as_source(table.clone());
            Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
        } else {
            Ok(Transformed::no(plan))
        }
    })
    .unwrap()
    .data
}

#[derive(Debug, Clone)]
struct LocalNodes {
    batches: Vec<RecordBatch>,
    states: Arc<AtomicUsize>,
}

#[derive(Debug)]
struct StatePartition {
    nodes: LocalNodes,
    remote: LogicalPlan,
    schema: datafusion::arrow::datatypes::SchemaRef,
}

impl PartitionStream for StatePartition {
    fn schema(&self) -> &datafusion::arrow::datatypes::SchemaRef {
        &self.schema
    }
    fn execute(&self, _ctx: Arc<TaskContext>) -> DfStream {
        let nodes = self.nodes.clone();
        let remote = self.remote.clone();
        Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            async_stream::try_stream! {
                for batch in nodes.batches {
                    let ctx = SessionContext::new();
                    let plan = replace_source(remote.clone(), vec![batch]);
                    for state in datafusion::dataframe::DataFrame::new(ctx.state(), plan).collect().await? {
                        nodes.states.fetch_add(state.num_rows(), Ordering::SeqCst);
                        yield state;
                    }
                }
            },
        ))
    }
}

#[async_trait::async_trait]
impl InformationExtension for LocalNodes {
    type Error = catalog::error::Error;
    async fn nodes(&self) -> std::result::Result<Vec<NodeInfo>, Self::Error> {
        unimplemented!()
    }
    async fn procedures(&self) -> std::result::Result<Vec<(String, ProcedureInfo)>, Self::Error> {
        unimplemented!()
    }
    async fn region_stats(&self) -> std::result::Result<Vec<RegionStat>, Self::Error> {
        unimplemented!()
    }
    async fn flow_stats(&self) -> std::result::Result<Option<FlowStat>, Self::Error> {
        unimplemented!()
    }
    async fn inspect_datanode(
        &self,
        _request: DatanodeInspectRequest,
    ) -> std::result::Result<SendableRecordBatchStream, Self::Error> {
        unimplemented!()
    }
    fn supports_inspect_query_plan(&self) -> bool {
        true
    }
    fn inspect_datanode_query_plan(
        &self,
        remote: LogicalPlan,
    ) -> std::result::Result<Option<Arc<dyn ExecutionPlan>>, Self::Error> {
        let schema = Arc::new(remote.schema().as_arrow().clone());
        let partition = StatePartition {
            nodes: self.clone(),
            remote,
            schema: schema.clone(),
        };
        Ok(Some(Arc::new(
            StreamingTableExec::try_new(schema, vec![Arc::new(partition)], None, None, false, None)
                .unwrap(),
        )))
    }
}

async fn analyzed(sql: &str, table_id: u32) -> LogicalPlan {
    let context = SessionContext::new();
    context
        .register_table("ssts_manifest", table_provider(table_id))
        .unwrap();
    let plan = context.sql(sql).await.unwrap().into_unoptimized_plan();
    Analyzer::new()
        .execute_and_check(plan, &ConfigOptions::default(), |_, _| {})
        .unwrap()
}

fn has_remote(plan: &LogicalPlan) -> bool {
    let mut found = false;
    plan.apply(|plan| {
        if let LogicalPlan::Extension(extension) = plan {
            found |= extension.node.as_any().is::<SstManifestAggregate>();
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    found
}

#[tokio::test]
async fn test_manifest_aggregate_states_match_baseline() {
    for sizes in [
        vec![vec![0, 2, 10], vec![3, 9], vec![]],
        vec![vec![], vec![]],
        vec![],
    ] {
        for sql in [
            "SELECT COUNT(*), COUNT(1), SUM(file_size) FROM ssts_manifest",
            "SELECT COUNT(*) AS files, SUM(m.file_size) AS bytes FROM ssts_manifest m WHERE m.file_size >= 2",
            "SELECT COUNT(*), SUM(file_size), COUNT(*) FILTER (WHERE file_size < 5), SUM(file_size) FILTER (WHERE file_size < 5) FROM ssts_manifest",
            "SELECT COUNT(*), SUM(file_size) FROM ssts_manifest WHERE file_size > 100",
            "SELECT SUM(file_size) FILTER (WHERE file_size > 100) AS missing FROM ssts_manifest",
            "SELECT COUNT(*) FILTER (WHERE file_size < 5), COUNT(*) FILTER (WHERE file_size < 10), SUM(file_size) FILTER (WHERE file_size < 5), SUM(file_size) FILTER (WHERE file_size < 10) FROM ssts_manifest",
        ] {
            let plan = analyzed(sql, consts::INFORMATION_SCHEMA_SSTS_MANIFEST_TABLE_ID).await;
            let mut analyzer = Analyzer::new();
            analyzer.rules.push(Arc::new(SstManifestAggregateRule));
            analyzer
                .rules
                .push(Arc::new(crate::dist_plan::DistPlannerAnalyzer));
            analyzer.rules.push(Arc::new(
                common_function::aggrs::aggr_wrapper::fix_order::FixStateUdafOrderingAnalyzer,
            ));
            let rewritten = analyzer
                .execute_and_check(plan.clone(), &ConfigOptions::default(), |_, _| {})
                .unwrap();
            assert!(
                has_remote(&rewritten),
                "{sql}: {rewritten}; original: {plan:?}"
            );
            assert_eq!(plan.schema(), rewritten.schema());
            let batches = sizes.iter().cloned().map(batch).collect::<Vec<_>>();
            let expected_context = SessionContext::new();
            let expected = datafusion::dataframe::DataFrame::new(
                expected_context.state(),
                replace_source(plan, batches.clone()),
            )
            .collect()
            .await
            .unwrap();
            let nodes = Arc::new(LocalNodes {
                batches,
                states: Arc::new(AtomicUsize::new(0)),
            });
            let planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
                SstManifestAggregatePlanner::new(nodes.clone()),
            )]);
            let context = SessionContext::new();
            let physical = planner
                .create_physical_plan(&rewritten, &context.state())
                .await
                .unwrap();
            let actual = datafusion::physical_plan::collect(physical, context.task_ctx())
                .await
                .unwrap();
            assert_eq!(actual, expected, "{sql}, {sizes:?}");
            assert_eq!(
                nodes.states.load(Ordering::SeqCst),
                sizes.len(),
                "one state row per node"
            );
        }
    }
}

#[tokio::test]
async fn test_manifest_aggregate_fallbacks() {
    for sql in [
        "SELECT COUNT(DISTINCT file_size) FROM ssts_manifest",
        "SELECT file_size, COUNT(*) FROM ssts_manifest GROUP BY file_size",
        "SELECT SUM(file_size + 1) FROM ssts_manifest",
        "SELECT COUNT(file_size) FROM ssts_manifest",
        "SELECT COUNT(*) FROM ssts_manifest WHERE length(file_path) > 0",
        "SELECT COUNT(*) FROM (SELECT * FROM ssts_manifest LIMIT 1) m",
        "SELECT SUM(file_size) FILTER (WHERE file_size % 2 = 0) FROM ssts_manifest",
    ] {
        let plan = analyzed(sql, consts::INFORMATION_SCHEMA_SSTS_MANIFEST_TABLE_ID).await;
        let rewritten = SstManifestAggregateRule
            .analyze(plan.clone(), &ConfigOptions::default())
            .unwrap();
        assert_eq!(rewritten, plan, "{sql}");
    }
    let plan = analyzed("SELECT COUNT(*) FROM ssts_manifest", 1000).await;
    assert_eq!(
        SstManifestAggregateRule
            .analyze(plan.clone(), &ConfigOptions::default())
            .unwrap(),
        plan
    );
}

#[tokio::test]
async fn test_manifest_aggregate_real_catalog_timestamp_cast() {
    use cache::{build_fundamental_cache_registry, with_default_composite_cache_registry};
    use common_meta::cache::{CacheRegistryBuilder, LayeredCacheRegistryBuilder};
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use session::context::QueryContext;

    let nodes = Arc::new(LocalNodes {
        batches: vec![batch(vec![0, 2, 10]), batch(vec![3, 9])],
        states: Arc::new(AtomicUsize::new(0)),
    });
    let backend = Arc::new(MemoryKvBackend::default());
    let caches = LayeredCacheRegistryBuilder::default()
        .add_cache_registry(CacheRegistryBuilder::default().build())
        .add_cache_registry(build_fundamental_cache_registry(backend.clone()));
    let caches = Arc::new(
        with_default_composite_cache_registry(caches)
            .unwrap()
            .build(),
    );
    let catalog =
        catalog::kvbackend::KvBackendCatalogManagerBuilder::new(nodes.clone(), backend, caches)
            .build();
    let engine = crate::QueryEngineFactory::try_new(
        catalog,
        None,
        None,
        None,
        None,
        true,
        Default::default(),
    )
    .unwrap()
    .query_engine();
    let query_ctx = QueryContext::arc();
    let sql = "SELECT COUNT(*) AS files, SUM(file_size) AS bytes, COUNT(*) FILTER (WHERE file_size >= 1) AS nonempty, SUM(file_size) FILTER (WHERE file_size = 0) AS zero_bytes FROM information_schema.ssts_manifest WHERE region_id > 0 AND min_ts >= '1970-01-01T00:00:00.001Z'::TIMESTAMP_NS AND max_ts < '1970-01-01T00:00:00.003Z'::TIMESTAMP_NS";
    let statement = crate::parser::QueryLanguageParser::parse_sql(sql, &query_ctx).unwrap();
    let original = engine
        .planner()
        .plan(&statement, query_ctx.clone())
        .await
        .unwrap();
    let state = engine.engine_state().session_state();
    // Uses the real catalog identity, parser, registered analyzer ordering and planner.
    let rewritten = state
        .analyzer()
        .execute_and_check(original.clone(), state.config_options(), |_, _| {})
        .unwrap();
    assert!(has_remote(&rewritten), "{rewritten}");
    let physical = state.create_physical_plan(&rewritten).await.unwrap();
    let actual = datafusion::physical_plan::collect(physical, state.task_ctx())
        .await
        .unwrap();
    let context = SessionContext::new();
    let expected = datafusion::dataframe::DataFrame::new(
        context.state(),
        replace_source(original, nodes.batches.clone()),
    )
    .collect()
    .await
    .unwrap();
    assert_eq!(actual, expected);
    assert_eq!(nodes.states.load(Ordering::SeqCst), nodes.batches.len());
}
