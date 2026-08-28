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

//! Implementation of grpc service for flow node

use std::net::SocketAddr;
use std::sync::Arc;

use api::v1::flow::DirtyWindowRequests;
use catalog::CatalogManagerRef;
use common_base::Plugins;
use common_error::ext::BoxedError;
use common_meta::key::TableMetadataManagerRef;
use common_meta::key::flow::FlowMetadataManagerRef;
use common_meta::node_manager::Flownode;
use futures::TryStreamExt;
use greptime_proto::v1::flow::{FlowRequest, FlowResponse, InsertRequests, flow_server};
use itertools::Itertools;
use query::QueryEngineFactory;
use servers::add_service;
use servers::grpc::builder::GrpcServerBuilder;
use servers::grpc::{GrpcServer, GrpcServerConfig};
use servers::http::HttpServerBuilder;
use servers::metrics_handler::MetricsHandler;
use servers::server::{ServerHandler, ServerHandlers};
use snafu::ResultExt;
use tonic::codec::CompressionEncoding;
use tonic::{Request, Response, Status};

use crate::adapter::flownode_impl::{FlowDualEngine, FlowDualEngineRef};
use crate::batching_mode::engine::BatchingEngine;
use crate::error::{
    DatafusionSnafu, ExternalSnafu, ListFlowsSnafu, ParseAddrSnafu, ShutdownServerSnafu,
    StartServerSnafu, to_status_with_last_err,
};
use crate::heartbeat::HeartbeatTask;
use crate::metrics::{METRIC_FLOW_PROCESSING_TIME, METRIC_FLOW_ROWS};
use crate::utils::{SizeReportSender, StateReportHandler};
use crate::{Error, FlownodeOptions, FrontendClient, StreamingEngine};

pub const FLOW_NODE_SERVER_NAME: &str = "FLOW_NODE_SERVER";
/// wrapping flow node manager to avoid orphan rule with Arc<...>
#[derive(Clone)]
pub struct FlowService {
    pub dual_engine: FlowDualEngineRef,
}

impl FlowService {
    pub fn new(manager: FlowDualEngineRef) -> Self {
        Self {
            dual_engine: manager,
        }
    }
}

#[async_trait::async_trait]
impl flow_server::Flow for FlowService {
    async fn handle_create_remove(
        &self,
        request: Request<FlowRequest>,
    ) -> Result<Response<FlowResponse>, Status> {
        let _timer = METRIC_FLOW_PROCESSING_TIME
            .with_label_values(&["ddl"])
            .start_timer();

        let request = request.into_inner();
        self.dual_engine
            .handle(request)
            .await
            .map_err(|err| {
                common_telemetry::error!(err; "Failed to handle flow request");
                err
            })
            .map(Response::new)
            .map_err(to_status_with_last_err)
    }

    async fn handle_mirror_request(
        &self,
        request: Request<InsertRequests>,
    ) -> Result<Response<FlowResponse>, Status> {
        let _timer = METRIC_FLOW_PROCESSING_TIME
            .with_label_values(&["insert"])
            .start_timer();

        let request = request.into_inner();
        // TODO(discord9): fix protobuf import order shenanigans to remove this duplicated define
        let mut row_count = 0;
        let request = api::v1::region::InsertRequests {
            requests: request
                .requests
                .into_iter()
                .map(|insert| {
                    insert.rows.as_ref().inspect(|x| row_count += x.rows.len());
                    api::v1::region::InsertRequest {
                        region_id: insert.region_id,
                        rows: insert.rows,
                        partition_expr_version: insert.partition_expr_version,
                    }
                })
                .collect_vec(),
        };

        METRIC_FLOW_ROWS
            .with_label_values(&["in"])
            .inc_by(row_count as u64);

        self.dual_engine
            .handle_inserts(request)
            .await
            .map(Response::new)
            .map_err(to_status_with_last_err)
    }

    async fn handle_mark_dirty_time_window(
        &self,
        reqs: Request<DirtyWindowRequests>,
    ) -> Result<Response<FlowResponse>, Status> {
        self.dual_engine
            .handle_mark_window_dirty(reqs.into_inner())
            .await
            .map(Response::new)
            .map_err(to_status_with_last_err)
    }
}

#[derive(Clone)]
pub struct FlownodeServer {
    inner: Arc<FlownodeServerInner>,
}

/// FlownodeServerInner is the inner state of FlownodeServer,
/// this struct mostly useful for construct/start and stop the
/// flow node server
struct FlownodeServerInner {
    flow_service: FlowService,
}

impl FlownodeServer {
    pub fn new(flow_service: FlowService) -> Self {
        Self {
            inner: Arc::new(FlownodeServerInner { flow_service }),
        }
    }
}

impl FlownodeServer {
    pub fn create_flow_service(&self) -> flow_server::FlowServer<impl flow_server::Flow> {
        flow_server::FlowServer::new(self.inner.flow_service.clone())
            .accept_compressed(CompressionEncoding::Gzip)
            .send_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Zstd)
            .send_compressed(CompressionEncoding::Zstd)
    }
}

/// The flownode server instance.
pub struct FlownodeInstance {
    flownode_server: FlownodeServer,
    services: ServerHandlers,
    heartbeat_task: Option<HeartbeatTask>,
    state_report_task: Option<common_runtime::JoinHandle<()>>,
    consistent_check_task_started: bool,
}

impl FlownodeInstance {
    pub async fn start(&mut self) -> Result<(), crate::Error> {
        if let Some(task) = &self.heartbeat_task {
            task.start().await?;
        }

        let engine = self.flow_engine();
        // The state-report task owns the only report receiver, so keep it alive
        // across an ordinary stop/start cycle of this instance.
        if self.state_report_task.is_none() {
            self.state_report_task = engine.clone().start_state_report_task().await;
        }
        if let Err(err) = engine.start_flow_consistent_check_task().await {
            self.rollback_background_tasks().await;
            return Err(err);
        }
        self.consistent_check_task_started = true;

        if let Err(err) = self.services.start_all().await.context(StartServerSnafu) {
            self.rollback_background_tasks().await;
            return Err(err);
        }

        Ok(())
    }
    pub async fn shutdown(&mut self) -> Result<(), Error> {
        let services_result = self
            .services
            .shutdown_all()
            .await
            .context(ShutdownServerSnafu);
        let tasks_result = self.stop_background_tasks().await;

        services_result?;
        tasks_result?;
        Ok(())
    }

    async fn stop_background_tasks(&mut self) -> Result<(), Error> {
        let check_result = if self.consistent_check_task_started {
            self.consistent_check_task_started = false;
            self.flow_engine().stop_flow_consistent_check_task().await
        } else {
            Ok(())
        };

        if let Some(task) = &self.heartbeat_task {
            task.shutdown();
        }

        check_result
    }

    async fn rollback_background_tasks(&mut self) {
        if let Err(err) = self.stop_background_tasks().await {
            common_telemetry::error!(err; "Failed to roll back flownode background tasks");
        }
    }

    pub fn flownode_server(&self) -> &FlownodeServer {
        &self.flownode_server
    }

    pub fn flow_engine(&self) -> FlowDualEngineRef {
        self.flownode_server.inner.flow_service.dual_engine.clone()
    }

    pub fn setup_services(&mut self, services: ServerHandlers) {
        self.services = services;
    }
}

/// [`FlownodeInstance`] Builder
pub struct FlownodeBuilder {
    opts: FlownodeOptions,
    plugins: Plugins,
    table_meta: TableMetadataManagerRef,
    catalog_manager: CatalogManagerRef,
    flow_metadata_manager: FlowMetadataManagerRef,
    heartbeat_task: Option<HeartbeatTask>,
    /// receive a oneshot sender to send state size report
    state_report_handler: Option<StateReportHandler>,
    frontend_client: Arc<FrontendClient>,
}

impl FlownodeBuilder {
    /// init flownode builder
    pub fn new(
        opts: FlownodeOptions,
        plugins: Plugins,
        table_meta: TableMetadataManagerRef,
        catalog_manager: CatalogManagerRef,
        flow_metadata_manager: FlowMetadataManagerRef,
        frontend_client: Arc<FrontendClient>,
    ) -> Self {
        Self {
            opts,
            plugins,
            table_meta,
            catalog_manager,
            flow_metadata_manager,
            heartbeat_task: None,
            state_report_handler: None,
            frontend_client,
        }
    }

    pub fn with_heartbeat_task(self, heartbeat_task: HeartbeatTask) -> Self {
        let (sender, receiver) = SizeReportSender::new();
        Self {
            heartbeat_task: Some(heartbeat_task.with_query_stat_size(sender)),
            state_report_handler: Some(receiver),
            ..self
        }
    }

    pub fn opts(&self) -> &FlownodeOptions {
        &self.opts
    }

    pub fn table_meta(&self) -> &TableMetadataManagerRef {
        &self.table_meta
    }

    pub fn catalog_manager(&self) -> &CatalogManagerRef {
        &self.catalog_manager
    }

    pub fn flow_metadata_manager(&self) -> &FlowMetadataManagerRef {
        &self.flow_metadata_manager
    }

    pub fn frontend_client(&self) -> &Arc<FrontendClient> {
        &self.frontend_client
    }

    pub fn set_plugins(&mut self, plugins: Plugins) {
        self.plugins = plugins;
    }

    pub async fn build(mut self) -> Result<FlownodeInstance, Error> {
        // TODO(discord9): does this query engine need those?
        let query_engine_factory = QueryEngineFactory::try_new_with_plugins(
            // query engine in flownode is only used for translate plan with resolved table source.
            self.catalog_manager.clone(),
            None,
            None,
            None,
            None,
            None,
            false,
            Default::default(),
            self.opts.query.clone(),
        )
        .context(DatafusionSnafu {
            context: "Failed to build query engine",
        })?;
        let manager = Arc::new(self.build_manager(query_engine_factory.query_engine()));
        let batching = Arc::new(BatchingEngine::new(
            self.frontend_client.clone(),
            query_engine_factory.query_engine(),
            self.flow_metadata_manager.clone(),
            self.table_meta.clone(),
            self.catalog_manager.clone(),
            self.opts.flow.batching_mode.clone(),
        ));
        let dual = Arc::new(FlowDualEngine::new(
            manager.clone(),
            batching,
            self.flow_metadata_manager.clone(),
            self.catalog_manager.clone(),
            self.plugins.clone(),
        ));
        if let Some(handler) = self.state_report_handler.take() {
            dual.set_state_report_handler(handler).await;
        }

        let server = FlownodeServer::new(FlowService::new(dual));

        let heartbeat_task = self.heartbeat_task;

        let instance = FlownodeInstance {
            flownode_server: server,
            services: ServerHandlers::default(),
            heartbeat_task,
            state_report_task: None,
            consistent_check_task_started: false,
        };
        Ok(instance)
    }

    fn build_manager(&self, query_engine: Arc<dyn query::QueryEngine>) -> StreamingEngine {
        StreamingEngine::new(
            self.opts.node_id.map(|id| id as u32),
            query_engine,
            self.table_meta.clone(),
            self.frontend_client.clone(),
        )
    }
}

/// Useful in distributed mode
pub struct FlownodeServiceBuilder<'a> {
    opts: &'a FlownodeOptions,
    grpc_server: Option<GrpcServer>,
    enable_http_service: bool,
}

impl<'a> FlownodeServiceBuilder<'a> {
    pub fn new(opts: &'a FlownodeOptions) -> Self {
        Self {
            opts,
            grpc_server: None,
            enable_http_service: false,
        }
    }

    pub fn enable_http_service(self) -> Self {
        Self {
            enable_http_service: true,
            ..self
        }
    }

    pub fn with_grpc_server(self, grpc_server: GrpcServer) -> Self {
        Self {
            grpc_server: Some(grpc_server),
            ..self
        }
    }

    pub fn with_default_grpc_server(mut self, flownode_server: &FlownodeServer) -> Self {
        let grpc_server = Self::grpc_server_builder(self.opts, flownode_server).build();
        self.grpc_server = Some(grpc_server);
        self
    }

    pub fn build(mut self) -> Result<ServerHandlers, Error> {
        let handlers = ServerHandlers::default();
        if let Some(grpc_server) = self.grpc_server.take() {
            let addr: SocketAddr = self.opts.grpc.bind_addr.parse().context(ParseAddrSnafu {
                addr: &self.opts.grpc.bind_addr,
            })?;
            let handler: ServerHandler = (Box::new(grpc_server), addr);
            handlers.insert(handler);
        }

        if self.enable_http_service {
            let http_server = HttpServerBuilder::new(self.opts.http.clone())
                .with_metrics_handler(MetricsHandler)
                .build();
            let addr: SocketAddr = self.opts.http.addr.parse().context(ParseAddrSnafu {
                addr: &self.opts.http.addr,
            })?;
            let handler: ServerHandler = (Box::new(http_server), addr);
            handlers.insert(handler);
        }
        Ok(handlers)
    }

    pub fn grpc_server_builder(
        opts: &FlownodeOptions,
        flownode_server: &FlownodeServer,
    ) -> GrpcServerBuilder {
        let config = GrpcServerConfig {
            max_recv_message_size: opts.grpc.max_recv_message_size.as_bytes() as usize,
            max_send_message_size: opts.grpc.max_send_message_size.as_bytes() as usize,
            tls: opts.grpc.tls.clone(),
            max_connection_age: opts.grpc.max_connection_age,
        };
        let service = flownode_server.create_flow_service();
        let runtime = common_runtime::global_runtime();
        let mut builder = GrpcServerBuilder::new(config, runtime);
        add_service!(builder, service);
        builder
    }
}

/// get all flow ids in this flownode
pub(crate) async fn get_all_flow_ids(
    flow_metadata_manager: &FlowMetadataManagerRef,
    catalog_manager: &CatalogManagerRef,
    nodeid: Option<u64>,
) -> Result<Vec<u32>, Error> {
    let ret = if let Some(nodeid) = nodeid {
        let flow_ids_one_node = flow_metadata_manager
            .flownode_flow_manager()
            .flows(nodeid)
            .try_collect::<Vec<_>>()
            .await
            .context(ListFlowsSnafu { id: Some(nodeid) })?;
        flow_ids_one_node.into_iter().map(|(id, _)| id).collect()
    } else {
        let all_catalogs = catalog_manager
            .catalog_names()
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;
        let mut all_flow_ids = vec![];
        for catalog in all_catalogs {
            let flows = flow_metadata_manager
                .flow_name_manager()
                .flow_names(&catalog)
                .await
                .try_collect::<Vec<_>>()
                .await
                .map_err(BoxedError::new)
                .context(ExternalSnafu)?;

            all_flow_ids.extend(flows.into_iter().map(|(_, id)| id.flow_id()));
        }
        all_flow_ids
    };

    Ok(ret)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use catalog::memory::new_memory_catalog_manager;
    use common_base::Plugins;
    use common_meta::key::TableMetadataManager;
    use common_meta::key::flow::FlowMetadataManager;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use query::options::QueryOptions;

    use super::*;
    use crate::batching_mode::BatchingModeOptions;

    async fn new_test_flownode_server() -> (FlownodeServer, SizeReportSender) {
        let (frontend_client, _handler) =
            FrontendClient::from_empty_grpc_handler(QueryOptions::default());

        new_test_flownode_server_with_frontend_client(
            frontend_client,
            BatchingModeOptions::default(),
            None,
        )
        .await
    }

    async fn new_test_flownode_server_with_frontend_client(
        frontend_client: FrontendClient,
        batching_opts: BatchingModeOptions,
        node_id: Option<u32>,
    ) -> (FlownodeServer, SizeReportSender) {
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let table_meta = Arc::new(TableMetadataManager::new(kv_backend.clone()));
        table_meta.init().await.unwrap();
        let flow_meta = Arc::new(FlowMetadataManager::new(kv_backend.clone()));
        let catalog_manager = new_memory_catalog_manager().unwrap();
        let query_engine = crate::test_utils::create_test_query_engine();

        let frontend_client = Arc::new(frontend_client);
        let streaming_engine = Arc::new(StreamingEngine::new(
            node_id,
            query_engine.clone(),
            table_meta.clone(),
            frontend_client.clone(),
        ));
        let batching_engine = Arc::new(BatchingEngine::new(
            frontend_client,
            query_engine,
            flow_meta.clone(),
            table_meta,
            catalog_manager.clone(),
            batching_opts,
        ));
        let dual_engine = Arc::new(FlowDualEngine::new(
            streaming_engine,
            batching_engine,
            flow_meta,
            catalog_manager,
            Plugins::new(),
        ));

        let (report_sender, report_handler) = SizeReportSender::new();
        dual_engine.set_state_report_handler(report_handler).await;

        let server = FlownodeServer::new(FlowService::new(dual_engine));
        (server, report_sender)
    }
}
