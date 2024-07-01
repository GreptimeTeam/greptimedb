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

use catalog::CatalogManagerRef;
use common_base::Plugins;
use common_meta::ddl::table_meta;
use common_meta::heartbeat::handler::HandlerGroupExecutor;
use common_meta::key::TableMetadataManagerRef;
use common_meta::kv_backend::KvBackendRef;
use common_meta::node_manager::Flownode;
use common_telemetry::tracing::info;
use futures::FutureExt;
use greptime_proto::v1::flow::{flow_server, FlowRequest, FlowResponse, InsertRequests};
use itertools::Itertools;
use meta_client::client::MetaClient;
use query::QueryEngineFactory;
use serde::de::Unexpected;
use servers::error::{AlreadyStartedSnafu, StartGrpcSnafu, TcpBindSnafu, TcpIncomingSnafu};
use servers::heartbeat_options::HeartbeatOptions;
use servers::server::Server;
use snafu::{ensure, ResultExt};
use tokio::net::TcpListener;
use tokio::sync::{oneshot, Mutex};
use tonic::transport::server::TcpIncoming;
use tonic::{Request, Response, Status};

use crate::adapter::FlowWorkerManagerRef;
use crate::error::{ParseAddrSnafu, ShutdownServerSnafu, StartServerSnafu, UnexpectedSnafu};
use crate::heartbeat::HeartbeatTask;
use crate::transform::register_function_to_query_engine;
use crate::{Error, FlowWorkerManager, FlownodeOptions};
pub const FLOW_NODE_SERVER_NAME: &str = "FLOW_NODE_SERVER";

/// wrapping flow node manager to avoid orphan rule with Arc<...>
#[derive(Clone)]
pub struct FlowService {
    pub manager: FlowWorkerManagerRef,
}

impl FlowService {
    pub fn new(manager: FlowWorkerManagerRef) -> Self {
        Self { manager }
    }
}

#[async_trait::async_trait]
impl flow_server::Flow for FlowService {
    async fn handle_create_remove(
        &self,
        request: Request<FlowRequest>,
    ) -> Result<Response<FlowResponse>, Status> {
        let request = request.into_inner();
        self.manager
            .handle(request)
            .await
            .map(Response::new)
            .map_err(|e| {
                let msg = format!("failed to handle request: {:?}", e);
                Status::internal(msg)
            })
    }

    async fn handle_mirror_request(
        &self,
        request: Request<InsertRequests>,
    ) -> Result<Response<FlowResponse>, Status> {
        let request = request.into_inner();
        // TODO(discord9): fix protobuf import order shenanigans to remove this duplicated define
        let request = api::v1::region::InsertRequests {
            requests: request
                .requests
                .into_iter()
                .map(|insert| api::v1::region::InsertRequest {
                    region_id: insert.region_id,
                    rows: insert.rows,
                })
                .collect_vec(),
        };
        self.manager
            .handle_inserts(request)
            .await
            .map(Response::new)
            .map_err(|e| {
                let msg = format!("failed to handle request: {:?}", e);
                Status::internal(msg)
            })
    }
}

pub struct FlownodeServer {
    shutdown_tx: Mutex<Option<oneshot::Sender<()>>>,
    flow_service: FlowService,
}

impl FlownodeServer {
    pub fn new(flow_service: FlowService) -> Self {
        Self {
            flow_service,
            shutdown_tx: Mutex::new(None),
        }
    }
}

impl FlownodeServer {
    pub fn create_flow_service(&self) -> flow_server::FlowServer<impl flow_server::Flow> {
        flow_server::FlowServer::new(self.flow_service.clone())
    }
}

#[async_trait::async_trait]
impl servers::server::Server for FlownodeServer {
    async fn shutdown(&self) -> Result<(), servers::error::Error> {
        let mut shutdown_tx = self.shutdown_tx.lock().await;
        if let Some(tx) = shutdown_tx.take() {
            if tx.send(()).is_err() {
                info!("Receiver dropped, the flow node server has already shutdown");
            }
        }
        info!("Shutdown flow node server");

        Ok(())
    }
    async fn start(&self, addr: SocketAddr) -> Result<SocketAddr, servers::error::Error> {
        let (tx, rx) = oneshot::channel::<()>();
        let (incoming, addr) = {
            let mut shutdown_tx = self.shutdown_tx.lock().await;
            ensure!(
                shutdown_tx.is_none(),
                AlreadyStartedSnafu { server: "flow" }
            );
            let listener = TcpListener::bind(addr)
                .await
                .context(TcpBindSnafu { addr })?;
            let addr = listener.local_addr().context(TcpBindSnafu { addr })?;
            let incoming =
                TcpIncoming::from_listener(listener, true, None).context(TcpIncomingSnafu)?;
            info!("flow server is bound to {}", addr);

            *shutdown_tx = Some(tx);

            (incoming, addr)
        };

        let builder = tonic::transport::Server::builder().add_service(self.create_flow_service());
        let _handle = common_runtime::spawn_bg(async move {
            let _result = builder
                .serve_with_incoming_shutdown(incoming, rx.map(drop))
                .await
                .context(StartGrpcSnafu);
        });

        let manager_ref = self.flow_service.manager.clone();
        let _handle = manager_ref.clone().run_background();

        Ok(addr)
    }

    fn name(&self) -> &str {
        FLOW_NODE_SERVER_NAME
    }
}

/// The flownode server instance.
pub struct FlownodeInstance {
    server: FlownodeServer,
    addr: SocketAddr,
    heartbeat_task: Option<HeartbeatTask>,
}

impl FlownodeInstance {
    pub async fn start(&mut self) -> Result<(), crate::Error> {
        if let Some(task) = &self.heartbeat_task {
            task.start().await?;
        }

        self.addr = self
            .server
            .start(self.addr)
            .await
            .context(StartServerSnafu)?;
        Ok(())
    }
    pub async fn shutdown(&self) -> Result<(), crate::Error> {
        self.server.shutdown().await.context(ShutdownServerSnafu)?;

        if let Some(task) = &self.heartbeat_task {
            task.shutdown();
        }

        Ok(())
    }

    pub fn flow_worker_manager(&self) -> FlowWorkerManagerRef {
        self.server.flow_service.manager.clone()
    }
}

/// [`FlownodeInstance`] Builder
pub struct FlownodeBuilder {
    opts: FlownodeOptions,
    plugins: Plugins,
    table_meta: TableMetadataManagerRef,
    catalog_manager: CatalogManagerRef,
    heartbeat_task: Option<HeartbeatTask>,
}

impl FlownodeBuilder {
    /// init flownode builder
    pub fn new(
        opts: FlownodeOptions,
        plugins: Plugins,
        table_meta: TableMetadataManagerRef,
        catalog_manager: CatalogManagerRef,
    ) -> Self {
        Self {
            opts,
            plugins,
            table_meta,
            catalog_manager,
            heartbeat_task: None,
        }
    }

    pub fn with_heartbeat_task(self, heartbeat_task: HeartbeatTask) -> Self {
        Self {
            heartbeat_task: Some(heartbeat_task),
            ..self
        }
    }

    pub async fn build(self) -> Result<FlownodeInstance, Error> {
        let manager = Arc::new(self.build_manager().await?);
        let server = FlownodeServer::new(FlowService::new(manager.clone()));

        let heartbeat_task = self.heartbeat_task;

        let addr = self.opts.grpc.addr;
        let instance = FlownodeInstance {
            server,
            addr: addr.parse().context(ParseAddrSnafu { addr })?,
            heartbeat_task,
        };
        Ok(instance)
    }

    /// build [`FlowWorkerManager`], note this doesn't take ownership of `self`,
    /// nor does it actually start running the worker.
    async fn build_manager(&self) -> Result<FlowWorkerManager, Error> {
        let catalog_manager = self.catalog_manager.clone();
        let table_meta = self.table_meta.clone();

        let query_engine_factory = QueryEngineFactory::new_with_plugins(
            // query engine in flownode is only used for translate plan with resolved table source.
            catalog_manager,
            None,
            None,
            None,
            false,
            self.plugins.clone(),
        );
        let query_engine = query_engine_factory.query_engine();

        register_function_to_query_engine(&query_engine);

        let (tx, rx) = oneshot::channel();

        let node_id = self.opts.node_id.map(|id| id as u32);
        let _handle = std::thread::spawn(move || {
            let (flow_node_manager, mut worker) =
                FlowWorkerManager::new_with_worker(node_id, query_engine, table_meta);
            let _ = tx.send(flow_node_manager);
            info!("Flow Worker started in new thread");
            worker.run();
        });
        let man = rx.await.map_err(|_e| {
            UnexpectedSnafu {
                reason: "sender is dropped, failed to create flow node manager",
            }
            .build()
        })?;
        info!("Flow Node Manager started");
        Ok(man)
    }
}
