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

#[cfg(test)]
#[path = "standalone/native_promql_tests.rs"]
mod native_promql_tests;

use std::sync::Arc;
use std::time::Duration;

use cache::{
    build_datanode_layered_cache_registry, build_fundamental_cache_registry,
    with_default_composite_cache_registry,
};
use catalog::information_schema::NoopInformationExtension;
use catalog::kvbackend::KvBackendCatalogManagerBuilder;
use catalog::process_manager::ProcessManager;
use common_base::Plugins;
use common_catalog::consts::{MIN_USER_FLOW_ID, MIN_USER_TABLE_ID};
use common_config::KvBackendConfig;
use common_datasource::object_store::LocalFileAccess;
use common_event_recorder::EventRecorderOptions;
use common_meta::cache::LayeredCacheRegistryBuilder;
use common_meta::ddl::flow_meta::FlowMetadataAllocator;
use common_meta::ddl::table_meta::TableMetadataAllocator;
use common_meta::ddl::{DdlContext, NoopRegionFailureDetectorControl};
use common_meta::ddl_manager::DdlManager;
use common_meta::key::TableMetadataManager;
use common_meta::key::flow::FlowMetadataManager;
use common_meta::kv_backend::KvBackendRef;
use common_meta::procedure_executor::LocalProcedureExecutor;
use common_meta::region_keeper::MemoryRegionKeeper;
use common_meta::region_registry::LeaderRegionRegistry;
use common_meta::sequence::SequenceBuilder;
use common_procedure::ProcedureManagerRef;
use common_procedure::local::EventRecorderHandle;
use common_procedure::options::ProcedureConfig;
use common_telemetry::logging::SlowQueryOptions;
use common_test_util::find_workspace_path;
use common_wal::config::DatanodeWalConfig;
use datanode::datanode::{Datanode, DatanodeBuilder};
use flow::{FlownodeBuilder, FrontendClient, GrpcQueryHandlerWithBoxedError};
use frontend::frontend::Frontend;
use frontend::instance::Instance;
use frontend::instance::builder::FrontendBuilder;
use frontend::server::Services;
use frontend::service_config::{BatcherOptions, PendingRowsBatcherOptions};
use meta_srv::metasrv::{FLOW_ID_SEQ, TABLE_ID_SEQ};
use servers::grpc::GrpcOptions;
use standalone::options::StandaloneOptions;
use standalone::{StandaloneDatanodeManager, StandaloneRepartitionProcedureFactory};

use crate::test_util::{self, StorageType, TestGuard, create_tmp_dir_and_datanode_opts};

pub struct GreptimeDbStandalone {
    /// Storage engine for assertions across protocol and storage boundaries.
    pub mito_engine: mito2::engine::MitoEngine,
    /// Owns the region server and the log store, so a test can shut them down.
    pub datanode: Datanode,
    pub frontend: Arc<Frontend>,
    pub opts: StandaloneOptions,
    pub guard: TestGuard,
    // Used in rebuild.
    pub kv_backend: KvBackendRef,
    pub procedure_manager: ProcedureManagerRef,
    pub event_recorder_handle: EventRecorderHandle,
}

impl GreptimeDbStandalone {
    pub fn fe_instance(&self) -> &Arc<Instance> {
        &self.frontend.instance
    }
}

pub struct GreptimeDbStandaloneBuilder {
    instance_name: String,
    datanode_wal_config: DatanodeWalConfig,
    store_providers: Option<Vec<StorageType>>,
    default_store: Option<StorageType>,
    plugin: Option<Plugins>,
    slow_query_options: SlowQueryOptions,
    event_recorder_options: EventRecorderOptions,
    auto_create_table: bool,
    logical_batcher: Option<BatcherOptions>,
    table_batcher: BatcherOptions,
    mito_config: Option<mito2::config::MitoConfig>,
    query_options: Option<query::options::QueryOptions>,
}

impl GreptimeDbStandaloneBuilder {
    pub fn new(instance_name: &str) -> Self {
        Self {
            instance_name: instance_name.to_string(),
            store_providers: None,
            plugin: None,
            default_store: None,
            datanode_wal_config: DatanodeWalConfig::default(),
            // Enable slow query log with 1s threshold by default for integration tests.
            slow_query_options: SlowQueryOptions {
                enable: true,
                threshold: Duration::from_secs(1),
                ..Default::default()
            },
            event_recorder_options: EventRecorderOptions::default(),
            auto_create_table: true,
            logical_batcher: None,
            table_batcher: BatcherOptions::default(),
            mito_config: None,
            query_options: None,
        }
    }

    /// Overrides the query options for this test instance.
    #[must_use]
    pub fn with_query_options(mut self, options: query::options::QueryOptions) -> Self {
        self.query_options = Some(options);
        self
    }

    /// Overrides the Mito configuration for this test instance.
    #[must_use]
    pub fn with_mito_config(mut self, config: mito2::config::MitoConfig) -> Self {
        self.mito_config = Some(config);
        self
    }

    /// Configures ordinary-table batching for protocol integration tests.
    #[must_use]
    pub fn with_table_batcher(mut self, options: BatcherOptions) -> Self {
        self.table_batcher = options;
        self
    }

    /// Configures logical-table batching for integration tests.
    #[must_use]
    pub fn with_logical_batcher(mut self, options: BatcherOptions) -> Self {
        self.logical_batcher = Some(options);
        self
    }

    #[must_use]
    pub fn with_auto_create_table(mut self, auto_create_table: bool) -> Self {
        self.auto_create_table = auto_create_table;
        self
    }

    /// Sets the event recorder options for the standalone instance.
    #[must_use]
    pub fn with_event_recorder_options(
        mut self,
        event_recorder_options: EventRecorderOptions,
    ) -> Self {
        self.event_recorder_options = event_recorder_options;
        self
    }

    #[must_use]
    pub fn with_default_store_type(self, store_type: StorageType) -> Self {
        Self {
            default_store: Some(store_type),
            ..self
        }
    }

    #[cfg(test)]
    #[must_use]
    pub fn with_store_providers(self, store_providers: Vec<StorageType>) -> Self {
        Self {
            store_providers: Some(store_providers),
            ..self
        }
    }

    #[must_use]
    pub fn with_plugin(self, plugin: Plugins) -> Self {
        Self {
            plugin: Some(plugin),
            ..self
        }
    }

    #[must_use]
    pub fn with_slow_query_threshold(mut self, threshold: Duration) -> Self {
        self.slow_query_options.threshold = threshold;
        self
    }

    #[must_use]
    pub fn with_datanode_wal_config(mut self, datanode_wal_config: DatanodeWalConfig) -> Self {
        self.datanode_wal_config = datanode_wal_config;
        self
    }

    pub async fn build_with(
        &self,
        kv_backend: KvBackendRef,
        guard: TestGuard,
        opts: StandaloneOptions,
        procedure_manager: ProcedureManagerRef,
        event_recorder_handle: EventRecorderHandle,
        register_procedure_loaders: bool,
    ) -> GreptimeDbStandalone {
        let plugins = self.plugin.clone().unwrap_or_default();

        let layered_cache_registry =
            Arc::new(build_datanode_layered_cache_registry(kv_backend.clone()));

        let mut builder =
            DatanodeBuilder::new(opts.datanode_options(), plugins.clone(), kv_backend.clone());
        let local_file_access = LocalFileAccess::sandboxed(find_workspace_path(".")).unwrap();
        builder.with_cache_registry(layered_cache_registry);
        builder.with_local_file_access(local_file_access.clone());
        let datanode = builder.build().await.unwrap();

        let table_metadata_manager = Arc::new(TableMetadataManager::new(kv_backend.clone()));
        table_metadata_manager.init().await.unwrap();

        let flow_metadata_manager = Arc::new(FlowMetadataManager::new(kv_backend.clone()));

        let layered_cache_builder = LayeredCacheRegistryBuilder::default();
        let fundamental_cache_registry = build_fundamental_cache_registry(kv_backend.clone());
        let cache_registry = Arc::new(
            with_default_composite_cache_registry(
                layered_cache_builder.add_cache_registry(fundamental_cache_registry),
            )
            .unwrap()
            .build(),
        );

        let catalog_manager = KvBackendCatalogManagerBuilder::new(
            Arc::new(NoopInformationExtension),
            kv_backend.clone(),
            cache_registry.clone(),
        )
        .with_procedure_manager(procedure_manager.clone())
        .build();

        let (frontend_client, frontend_instance_handler) =
            FrontendClient::from_empty_grpc_handler(opts.query.clone());
        let flow_builder = FlownodeBuilder::new(
            Default::default(),
            plugins.clone(),
            table_metadata_manager.clone(),
            catalog_manager.clone(),
            flow_metadata_manager.clone(),
            Arc::new(frontend_client),
        );
        let flownode = Arc::new(flow_builder.build().await.unwrap());

        let node_manager = Arc::new(StandaloneDatanodeManager {
            region_server: datanode.region_server(),
            flow_server: flownode.flow_engine(),
        });

        let table_id_allocator = Arc::new(
            SequenceBuilder::new(TABLE_ID_SEQ, kv_backend.clone())
                .initial(MIN_USER_TABLE_ID as u64)
                .step(10)
                .build(),
        );
        let flow_id_sequence = Arc::new(
            SequenceBuilder::new(FLOW_ID_SEQ, kv_backend.clone())
                .initial(MIN_USER_FLOW_ID as u64)
                .step(10)
                .build(),
        );
        let wal_provider = cmd::standalone::build_standalone_wal_provider(
            &opts.wal,
            opts.datanode_options().node_id,
            kv_backend.clone(),
        )
        .await
        .unwrap();
        let wal_provider = Arc::new(wal_provider);
        let table_metadata_allocator = Arc::new(TableMetadataAllocator::new(
            table_id_allocator,
            wal_provider.clone(),
        ));
        let flow_metadata_allocator = Arc::new(FlowMetadataAllocator::with_noop_peer_allocator(
            flow_id_sequence,
        ));

        let repartition_procedure_factory = Arc::new(StandaloneRepartitionProcedureFactory);
        let ddl_manager = Arc::new(DdlManager::new(
            DdlContext {
                node_manager: node_manager.clone(),
                cache_invalidator: cache_registry.clone(),
                memory_region_keeper: Arc::new(MemoryRegionKeeper::default()),
                leader_region_registry: Arc::new(LeaderRegionRegistry::default()),
                table_metadata_manager,
                table_metadata_allocator,
                flow_metadata_manager,
                flow_metadata_allocator,
                region_failure_detector_controller: Arc::new(NoopRegionFailureDetectorControl),
                soft_drop_enabled: false,
                soft_drop_retention: None,
                create_database_metadata_committer: None,
            },
            procedure_manager.clone(),
            repartition_procedure_factory,
        ));
        if register_procedure_loaders {
            ddl_manager.register_loaders().unwrap();
        }
        let procedure_executor = Arc::new(LocalProcedureExecutor::new(
            ddl_manager,
            procedure_manager.clone(),
        ));

        let server_addr = opts.frontend_options().grpc.server_addr.clone();

        let instance = FrontendBuilder::new(
            opts.frontend_options(),
            kv_backend.clone(),
            cache_registry.clone(),
            catalog_manager.clone(),
            node_manager.clone(),
            procedure_executor.clone(),
            Arc::new(ProcessManager::new(server_addr, None)),
        )
        .with_local_file_access(local_file_access)
        .with_plugin(plugins.clone())
        .try_build()
        .await
        .unwrap();
        let instance = Arc::new(instance);

        event_recorder_handle.install(instance.event_recorder());

        // set the frontend client for flownode
        let grpc_handler = instance.clone() as Arc<dyn GrpcQueryHandlerWithBoxedError>;
        let weak_grpc_handler = Arc::downgrade(&grpc_handler);
        frontend_instance_handler
            .set_handler(weak_grpc_handler)
            .await;
        procedure_manager.start().await.unwrap();
        wal_provider.start().await.unwrap();

        test_util::prepare_another_catalog_and_schema(&instance).await;

        let servers = Services::new(opts.clone(), instance.clone(), plugins)
            .build()
            .unwrap();
        let frontend = Frontend {
            instance,
            servers,
            heartbeat_task: None,
        };

        GreptimeDbStandalone {
            mito_engine: datanode.region_server().mito_engine().unwrap(),
            datanode,
            frontend: Arc::new(frontend),
            opts,
            guard,
            kv_backend,
            procedure_manager,
            event_recorder_handle,
        }
    }

    pub async fn build(&self) -> GreptimeDbStandalone {
        let default_store_type = self.default_store.unwrap_or(StorageType::File);
        let store_types = self.store_providers.clone().unwrap_or_default();

        let (opts, guard) = create_tmp_dir_and_datanode_opts(
            default_store_type,
            store_types,
            &self.instance_name,
            self.datanode_wal_config.clone(),
            Default::default(),
        );

        let kv_backend_config = KvBackendConfig::default();
        let procedure_config = ProcedureConfig::default();

        let kv_backend = standalone::build_metadata_kvbackend(
            format!("{}/kv", opts.storage.data_home),
            kv_backend_config,
        )
        .unwrap();
        let (procedure_manager, event_recorder_handle) =
            standalone::build_procedure_manager(kv_backend.clone(), procedure_config);

        let mut standalone_opts = StandaloneOptions {
            storage: opts.storage,
            procedure: procedure_config,
            metadata_store: kv_backend_config,
            wal: opts.wal,
            grpc: GrpcOptions::default().with_server_addr("127.0.0.1:4001"),
            slow_query: self.slow_query_options.clone(),
            event_recorder: self.event_recorder_options.clone(),
            auto_create_table: self.auto_create_table,
            pending_rows_batcher: PendingRowsBatcherOptions {
                logical_table: self.logical_batcher.clone(),
                table: self.table_batcher.clone(),
            },
            // Tests cover the descriptor, so they run with it enabled.
            otlp: frontend::service_config::OtlpOptions {
                experimental_enable_resource_info: true,
                ..Default::default()
            },
            ..StandaloneOptions::default()
        };

        if let Some(query_options) = &self.query_options {
            standalone_opts.query = query_options.clone();
        }

        if let Some(config) = &self.mito_config {
            for engine in &mut standalone_opts.region_engine {
                if let datanode::config::RegionEngineConfig::Mito(mito) = engine {
                    *mito = config.clone();
                }
            }
        }

        self.build_with(
            kv_backend,
            guard,
            standalone_opts,
            procedure_manager,
            event_recorder_handle,
            true,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::region::RegionResponse;
    use api::v1::region::{
        ListMetadataRequest, RegionRequest, RegionRequestHeader, region_request,
    };
    use api::v1::{QueryContext as ProtoQueryContext, SnapshotSequences};
    use common_meta::error::Result as MetaResult;
    use common_meta::node_manager::{Datanode, bind_raw_series_query};
    use common_meta::peer::Peer;
    use common_query::request::{
        QueryRequest, RAW_SERIES_REQUEST_EXTENSION_KEY, RawSeriesRequest, RawSeriesTag,
    };
    use common_recordbatch::SendableRecordBatchStream;
    use datafusion::common::{ScalarValue, TableReference};
    use datafusion::logical_expr::TableSource;
    use datafusion_expr::{LogicalPlan, LogicalPlanBuilder, col, lit};
    use datatypes::series::raw::RawSeriesOrdering;
    use datatypes::series::samples::SampleRef;
    use frontend::instance::Instance;
    use futures_util::TryStreamExt;
    use mito2::test_util::sst_util::new_primary_key;
    use query::parser::QueryLanguageParser;
    use session::context::{QueryContext, QueryContextRef};
    use store_api::metadata::RegionMetadata;
    use store_api::region_engine::RawLabelSource;

    use super::GreptimeDbStandaloneBuilder;
    use crate::test_util::execute_sql;

    struct NoExecutionDatanode;

    #[async_trait::async_trait]
    impl Datanode for NoExecutionDatanode {
        async fn handle_request(&self, _request: Arc<RegionRequest>) -> MetaResult<RegionResponse> {
            panic!("unexpected handle_request")
        }

        async fn handle_query(
            &self,
            _request: QueryRequest,
        ) -> MetaResult<SendableRecordBatchStream> {
            panic!("default prepare must not execute a query")
        }
    }

    #[tokio::test]
    async fn test_raw_series_binding_and_default_prepare_contract() {
        let binding: RawSeriesRequest = serde_json::from_str(include_str!(
            "../../tests/compatibility/wire/raw_series_request/v1_request.json"
        ))
        .unwrap();
        let mut header = RegionRequestHeader::default();
        header.tracing_context = std::collections::HashMap::from([(
            "traceparent".to_string(),
            "00-11111111111111111111111111111111-2222222222222222-01".to_string(),
        )]);
        header.query_context = Some(ProtoQueryContext {
            current_catalog: "custom_catalog".to_string(),
            current_schema: "custom_schema".to_string(),
            timezone: "Asia/Shanghai".to_string(),
            channel: common_base::protocol::Channel::Mysql as u32,
            snapshot_seqs: Some(SnapshotSequences {
                snapshot_seqs: std::collections::HashMap::from([(
                    binding.physical_metadata.region_id.as_u64(),
                    77,
                )]),
                sst_min_sequences: std::collections::HashMap::new(),
            }),
            extensions: std::collections::HashMap::from([
                ("raw-series-unrelated".to_string(), "preserved".to_string()),
                (
                    session::hints::READ_PREFERENCE_EXTENSION_KEY.to_string(),
                    "follower".to_string(),
                ),
            ]),
            explain: None,
        });
        let request = || QueryRequest {
            header: Some(header.clone()),
            region_id: binding.physical_metadata.region_id,
            plan: LogicalPlanBuilder::empty(false).build().unwrap(),
        };
        let original_header = request().header.unwrap();
        let (bound, schema, labels) = bind_raw_series_query(request(), &binding).unwrap();
        assert_eq!(
            vec!["aux", "value"],
            schema
                .fields
                .iter()
                .map(|f| f.name.as_ref())
                .collect::<Vec<_>>()
        );
        match labels {
            RawLabelSource::PrimaryKey {
                metric_name,
                logical_tags,
                ..
            } => {
                assert_eq!("cpu", metric_name.as_ref());
                assert_eq!(1, logical_tags.len());
                assert_eq!(1, logical_tags[0].column_id);
                assert_eq!("host", logical_tags[0].name.as_ref());
            }
            RawLabelSource::Explicit => panic!("expected key-backed source"),
        }
        let mut bound_header = bound.header.unwrap();
        let extension = bound_header
            .query_context
            .as_mut()
            .unwrap()
            .extensions
            .remove(RAW_SERIES_REQUEST_EXTENSION_KEY)
            .unwrap();
        assert_eq!(binding.to_extension_value().unwrap(), extension);
        assert_eq!(original_header, bound_header);

        let datanode = NoExecutionDatanode;
        let mut invalid = binding.clone();
        invalid.version = 42;
        assert_eq!(
            None,
            datanode
                .prepare_raw_series_query(request(), invalid)
                .await
                .unwrap()
        );
        let mut mismatch = request();
        mismatch.region_id = store_api::storage::RegionId::from_u64(
            binding.physical_metadata.region_id.as_u64() + 1,
        );
        let error = bind_raw_series_query(mismatch, &binding).unwrap_err();
        assert!(format!("{error:?}").contains("region does not match"));
        let mut invalid = binding.clone();
        invalid.version = 42;
        let error = bind_raw_series_query(request(), &invalid).unwrap_err();
        assert!(format!("{error:?}").contains("Invalid raw-series request"));
        let mut invalid_field = binding.clone();
        invalid_field.field_ids = vec![u32::MAX];
        let error = bind_raw_series_query(request(), &invalid_field).unwrap_err();
        assert!(format!("{error:?}").contains("Invalid raw-series fields"));
        let mut collision = request();
        let mut context = session::context::QueryContext::with_db_name(None);
        context.set_extension(RAW_SERIES_REQUEST_EXTENSION_KEY, "occupied");
        collision.header = Some(RegionRequestHeader {
            query_context: Some((&context).into()),
            ..Default::default()
        });
        let error = bind_raw_series_query(collision, &binding).unwrap_err();
        assert!(format!("{error:?}").contains("marker already exists"));
    }

    fn table_source(plan: &LogicalPlan) -> Option<Arc<dyn TableSource>> {
        match plan {
            LogicalPlan::TableScan(scan) => Some(scan.source.clone()),
            _ => plan.inputs().into_iter().find_map(table_source),
        }
    }

    async fn plan_sql(
        frontend: &Arc<Instance>,
        sql: &str,
        query_ctx: QueryContextRef,
    ) -> LogicalPlan {
        let statement = QueryLanguageParser::parse_sql(sql, &query_ctx).unwrap();
        frontend
            .statement_executor()
            .plan(&statement, query_ctx)
            .await
            .unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_standalone_native_raw_series_default_and_revalidation() {
        common_telemetry::init_default_ut_logging();
        const TABLE: &str = "standalone_native_raw_series";
        let standalone = GreptimeDbStandaloneBuilder::new(TABLE).build().await;
        let frontend = standalone.fe_instance().clone();
        execute_sql(&frontend, &format!("CREATE TABLE {TABLE} (host STRING, sample_value DOUBLE, ts TIMESTAMP, TIME INDEX (ts), PRIMARY KEY(host)) ENGINE=mito")).await;
        execute_sql(
            &frontend,
            &format!("INSERT INTO {TABLE} VALUES ('alpha', 1.5, 1000), ('alpha', NULL, 2000)"),
        )
        .await;

        let table = frontend
            .catalog_manager()
            .table("greptime", "public", TABLE, None)
            .await
            .unwrap()
            .unwrap();
        let table_id = table.table_info().ident.table_id;
        let partitions = frontend
            .partition_manager()
            .batch_find_table_partitions(&[table_id])
            .await
            .unwrap()[&table_id]
            .clone();
        assert_eq!(1, partitions.len());
        let region_id = partitions[0].id;

        let datanode = frontend.node_manager().datanode(&Peer::empty(0)).await;
        let response = datanode
            .handle(RegionRequest {
                header: Some(RegionRequestHeader::default()),
                body: Some(region_request::Body::ListMetadata(ListMetadataRequest {
                    region_ids: vec![region_id.as_u64()],
                })),
            })
            .await
            .unwrap();
        let metadata: Vec<Option<RegionMetadata>> =
            serde_json::from_slice(&response.metadata).unwrap();
        assert_eq!(1, metadata.len());
        let metadata = metadata[0].as_ref().unwrap();
        assert_eq!(region_id, metadata.region_id);
        let region = standalone.mito_engine.find_region(region_id).unwrap();
        let snapshot = region.find_committed_sequence();
        let column_id = |name: &str| {
            metadata
                .column_metadatas
                .iter()
                .find(|column| column.column_schema.name == name)
                .unwrap()
                .column_id
        };
        let host_id = column_id("host");
        let value_id = column_id("sample_value");

        let ctx = QueryContext::with_db_name(None);
        ctx.set_snapshot(region_id.as_u64(), snapshot);
        let ctx = Arc::new(ctx);
        let base = plan_sql(&frontend, &format!("SELECT * FROM {TABLE}"), ctx.clone()).await;
        let source = table_source(&base).unwrap();
        let plan = LogicalPlanBuilder::scan_with_filters(
            TableReference::bare(TABLE),
            source,
            Some(vec![
                metadata
                    .column_metadatas
                    .iter()
                    .position(|c| c.column_id == value_id)
                    .unwrap(),
            ]),
            vec![col("ts").gt_eq(lit(ScalarValue::TimestampMillisecond(Some(1000), None)))],
        )
        .unwrap()
        .build()
        .unwrap();
        let binding = RawSeriesRequest {
            version: 1,
            physical_metadata: metadata.clone(),
            metric_name: TABLE.to_string(),
            logical_tags: vec![RawSeriesTag {
                column_id: host_id,
                name: "host".into(),
            }],
            field_ids: vec![value_id],
        };
        let request = || QueryRequest {
            header: Some(RegionRequestHeader {
                query_context: Some(ctx.as_ref().into()),
                ..Default::default()
            }),
            region_id,
            plan: plan.clone(),
        };
        let mut parent_request = request();
        parent_request.plan = LogicalPlanBuilder::from(plan.clone())
            .filter(col("sample_value").gt(lit(ScalarValue::Float64(Some(0.0)))))
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(
            None,
            datanode
                .prepare_raw_series_query(parent_request, binding.clone())
                .await
                .unwrap()
        );
        let mut wrong_region = request();
        wrong_region.region_id = store_api::storage::RegionId::from_u64(region_id.as_u64() + 1);
        assert!(
            datanode
                .prepare_raw_series_query(wrong_region, binding.clone())
                .await
                .is_err()
        );
        let mut marked_request = request();
        marked_request
            .header
            .as_mut()
            .unwrap()
            .query_context
            .as_mut()
            .unwrap()
            .extensions
            .insert(
                RAW_SERIES_REQUEST_EXTENSION_KEY.to_string(),
                "occupied".to_string(),
            );
        assert!(
            datanode
                .prepare_raw_series_query(marked_request, binding.clone())
                .await
                .is_err()
        );
        let prepared = datanode
            .prepare_raw_series_query(request(), binding.clone())
            .await
            .unwrap();
        assert_eq!(Some(RawSeriesOrdering::ReadKeyThenTime), prepared);

        let raw = datanode
            .handle_raw_series_query(request(), binding.clone())
            .await
            .unwrap();
        assert_eq!(RawSeriesOrdering::ReadKeyThenTime, raw.ordering());
        match raw.label_source() {
            RawLabelSource::PrimaryKey {
                metric_name,
                logical_tags,
                ..
            } => {
                assert_eq!(TABLE, metric_name.as_ref());
                assert_eq!(1, logical_tags.len());
                assert_eq!(host_id, logical_tags[0].column_id);
                assert_eq!("host", logical_tags[0].name.as_ref());
            }
            RawLabelSource::Explicit => panic!("expected key-backed source"),
        }
        assert_eq!("sample_value", raw.schema().fields[0].name.as_ref());
        let chunks = raw.try_collect::<Vec<_>>().await.unwrap();
        let mut timestamps = Vec::new();
        let mut values = Vec::new();
        for chunk in &chunks {
            assert_eq!(new_primary_key(&["alpha"]), chunk.read_key.as_slice());
            assert!(chunk.metric.is_none());
            assert_eq!(1, chunk.fields.len());
            assert_eq!(chunk.time.len(), chunk.fields.get(0).unwrap().len());
            timestamps.extend_from_slice(chunk.time.as_explicit().unwrap());
            for i in 0..chunk.time.len() {
                values.push(chunk.fields.get(0).unwrap().sample(i));
            }
        }
        assert_eq!(vec![1000, 2000], timestamps);
        assert_eq!(2, values.len());
        assert!(matches!(values[0], SampleRef::Float(1.5)));
        assert!(matches!(values[1], SampleRef::Missing));
        assert!(chunks.last().unwrap().is_last);
        assert!(
            chunks
                .iter()
                .take(chunks.len() - 1)
                .all(|chunk| !chunk.is_last)
        );

        execute_sql(
            &frontend,
            &format!("INSERT INTO {TABLE} VALUES ('alpha', 9.5, 3000)"),
        )
        .await;
        let old = datanode
            .handle_raw_series_query(request(), binding.clone())
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            vec![1000, 2000],
            old.iter()
                .flat_map(|chunk| chunk.time.as_explicit().unwrap().iter().copied())
                .collect::<Vec<_>>()
        );

        let mut stale = binding;
        stale.physical_metadata.schema_version += 1;
        let prepare_error = datanode
            .prepare_raw_series_query(request(), stale.clone())
            .await
            .unwrap_err();
        assert!(format!("{prepare_error:?}").contains("Raw-series metadata mismatch"));
        let error = match datanode.handle_raw_series_query(request(), stale).await {
            Ok(_) => panic!("stale raw-series metadata unexpectedly succeeded"),
            Err(error) => error,
        };
        let message = format!("{error:?}");
        assert!(
            message.contains("Raw-series metadata mismatch"),
            "unexpected stale binding error chain: {message}"
        );
    }
}
