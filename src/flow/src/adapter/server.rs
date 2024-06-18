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

use common_meta::node_manager::Flownode;
use common_telemetry::tracing::info;
use futures::FutureExt;
use greptime_proto::v1::flow::{flow_server, FlowRequest, FlowResponse, InsertRequests};
use itertools::Itertools;
use servers::error::{AlreadyStartedSnafu, StartGrpcSnafu, TcpBindSnafu, TcpIncomingSnafu};
use snafu::{ensure, ResultExt};
use tokio::net::TcpListener;
use tokio::sync::{oneshot, Mutex};
use tonic::transport::server::TcpIncoming;
use tonic::{Request, Response, Status};

use crate::adapter::FlownodeManagerRef;
pub const FLOW_NODE_SERVER_NAME: &str = "FLOW_NODE_SERVER";

/// wrapping flow node manager to avoid orphan rule with Arc<...>
#[derive(Clone)]
pub struct FlowService {
    pub manager: FlownodeManagerRef,
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
    pub shutdown_tx: Mutex<Option<oneshot::Sender<()>>>,
    pub flow_service: FlowService,
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
