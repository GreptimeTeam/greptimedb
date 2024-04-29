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

use api::v1::flow::{CreateRequest, RemoveRequest};
use common_telemetry::tracing::info;
use futures::FutureExt;
use greptime_proto::v1::flow::{
    flow_request, flow_server, FlowRequest, FlowResponse, InsertRequests,
};
use itertools::Itertools;
use servers::error::{AlreadyStartedSnafu, StartGrpcSnafu, TcpBindSnafu, TcpIncomingSnafu};
use snafu::{ensure, ResultExt};
use tokio::net::TcpListener;
use tokio::sync::{oneshot, Mutex};
use tonic::transport::server::TcpIncoming;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use crate::adapter::{FlowNodeManager, FlowNodeManagerRef};
use crate::repr::{self, DiffRow};

pub const FLOW_NODE_SERVER_NAME: &str = "FLOW_NODE_SERVER";

#[derive(Clone)]
pub struct FlowService {
    pub manager: FlowNodeManagerRef,
}

#[async_trait::async_trait]
impl flow_server::Flow for FlowService {
    async fn handle_create_remove(
        &self,
        request: Request<FlowRequest>,
    ) -> Result<Response<FlowResponse>, Status> {
        match request.into_inner().body {
            Some(flow_request::Body::Create(CreateRequest {
                task_id: Some(task_id),
                source_table_ids,
                sink_table_name: Some(sink_table_name),
                create_if_not_exists,
                expire_when,
                comment,
                sql,
                task_options,
            })) => {
                let source_table_ids = source_table_ids.into_iter().map(|id| id.id).collect_vec();
                let sink_table_id = self
                    .manager
                    .table_info_source
                    .get_table_id_from_proto_name(&sink_table_name)
                    .await?;
                let ret = self
                    .manager
                    .create_task(
                        task_id.id as u64,
                        sink_table_id,
                        &source_table_ids,
                        create_if_not_exists,
                        Some(expire_when),
                        Some(comment),
                        sql,
                        task_options,
                    )
                    .await
                    .map_err(|e| tonic::Status::internal(e.to_string()))?;
                Ok(Response::new(FlowResponse {
                    affected_tasks: ret
                        .map(|id| greptime_proto::v1::flow::TaskId { id: id as u32 })
                        .into_iter()
                        .collect_vec(),
                    ..Default::default()
                }))
            }
            Some(flow_request::Body::Remove(RemoveRequest {
                task_id: Some(task_id),
            })) => {
                self.manager
                    .remove_task(task_id.id as u64)
                    .await
                    .map_err(|e| tonic::Status::internal(e.to_string()))?;
                Ok(Response::new(Default::default()))
            }
            None => Err(Status::invalid_argument("Missing request body.")),
            _ => Err(Status::invalid_argument("Invalid request body.")),
        }
    }

    async fn handle_mirror_request(
        &self,
        request: Request<InsertRequests>,
    ) -> Result<Response<FlowResponse>, Status> {
        for write_request in request.into_inner().requests {
            let region_id = write_request.region_id;
            let rows_proto = write_request.rows.map(|r| r.rows).unwrap_or(vec![]);
            // TODO(discord9): reconsider time assignment mechanism
            let now = self.manager.tick_manager.tick();
            let rows: Vec<DiffRow> = rows_proto
                .into_iter()
                .map(repr::Row::from)
                .map(|r| (r, now, 1))
                .collect_vec();
            self.manager
                .handle_write_request(region_id.into(), rows)
                .await
                .map_err(|e| tonic::Status::internal(e.to_string()))?;
        }
        // since `run_available` doesn't blocking, we can just trigger a run here
        self.manager.run_available().await;
        // write back should be config to be timed in somewhere else like one attempt per second
        Ok(Response::new(Default::default()))
    }
}

pub struct FlowNodeServer {
    pub shutdown_tx: Mutex<Option<oneshot::Sender<()>>>,
    pub flow_service: FlowService,
}

impl FlowNodeServer {
    pub fn create_flow_service(&self) -> flow_server::FlowServer<impl flow_server::Flow> {
        flow_server::FlowServer::new(self.flow_service.clone())
    }
}

#[async_trait::async_trait]
impl servers::server::Server for FlowNodeServer {
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
            // TODO(discord9): better place for dataflow to run per second
        });
        let manager_ref = self.flow_service.manager.clone();
        let _handle_trigger_run = common_runtime::spawn_bg(async move {
            manager_ref.run().await;
        });

        Ok(addr)
    }

    fn name(&self) -> &str {
        FLOW_NODE_SERVER_NAME
    }
}
