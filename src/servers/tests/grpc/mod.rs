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

use std::net::SocketAddr;
use std::sync::Arc;

use api::v1::auth_header::AuthScheme;
use api::v1::Basic;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use async_trait::async_trait;
use auth::tests::MockUserProvider;
use auth::UserProviderRef;
use client::{Client, Database, DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_runtime::{Builder as RuntimeBuilder, Runtime};
use servers::error::{Result, StartGrpcSnafu, TcpBindSnafu};
use servers::grpc::flight::FlightCraftWrapper;
use servers::grpc::greptime_handler::GreptimeRequestHandler;
use servers::query_handler::grpc::ServerGrpcQueryHandlerRef;
use servers::server::Server;
use snafu::ResultExt;
use table::test_util::MemTable;
use table::TableRef;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::codec::CompressionEncoding;

use crate::{create_testing_grpc_query_handler, LOCALHOST_WITH_0};

struct MockGrpcServer {
    query_handler: ServerGrpcQueryHandlerRef,
    user_provider: Option<UserProviderRef>,
    runtime: Runtime,
}

impl MockGrpcServer {
    fn new(
        query_handler: ServerGrpcQueryHandlerRef,
        user_provider: Option<UserProviderRef>,
        runtime: Runtime,
    ) -> Self {
        Self {
            query_handler,
            user_provider,
            runtime,
        }
    }

    fn create_service(&self) -> FlightServiceServer<impl FlightService> {
        let service: FlightCraftWrapper<_> = GreptimeRequestHandler::new(
            self.query_handler.clone(),
            self.user_provider.clone(),
            Some(self.runtime.clone()),
        )
        .into();
        FlightServiceServer::new(service)
            .accept_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Zstd)
            .send_compressed(CompressionEncoding::Gzip)
            .send_compressed(CompressionEncoding::Zstd)
    }
}

#[async_trait]
impl Server for MockGrpcServer {
    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }

    async fn start(&self, addr: SocketAddr) -> Result<SocketAddr> {
        let (listener, addr) = {
            let listener = TcpListener::bind(addr)
                .await
                .context(TcpBindSnafu { addr })?;
            let addr = listener.local_addr().context(TcpBindSnafu { addr })?;
            (listener, addr)
        };

        let service = self.create_service();
        // Would block to serve requests.
        let _handle = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .context(StartGrpcSnafu)
                .unwrap()
        });

        Ok(addr)
    }

    fn name(&self) -> &str {
        "MockGrpcServer"
    }
}

fn create_grpc_server(table: TableRef) -> Result<Arc<dyn Server>> {
    let query_handler = create_testing_grpc_query_handler(table);
    let io_runtime = RuntimeBuilder::default()
        .worker_threads(4)
        .thread_name("grpc-io-handlers")
        .build()
        .unwrap();

    let provider = MockUserProvider::default();

    Ok(Arc::new(MockGrpcServer::new(
        query_handler,
        Some(Arc::new(provider)),
        io_runtime,
    )))
}

#[tokio::test]
async fn test_grpc_server_startup() {
    let server = create_grpc_server(MemTable::default_numbers_table()).unwrap();
    let re = server.start(LOCALHOST_WITH_0.parse().unwrap()).await;
    let _ = re.unwrap();
}

#[tokio::test]
async fn test_grpc_query() {
    let server = create_grpc_server(MemTable::default_numbers_table()).unwrap();
    let re = server
        .start(LOCALHOST_WITH_0.parse().unwrap())
        .await
        .unwrap();
    let grpc_client = Client::with_urls(vec![re.to_string()]);
    let mut db = Database::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, grpc_client);

    let re = db.sql("select * from numbers").await;
    assert!(re.is_err());

    let greptime = "greptime".to_string();
    db.set_auth(AuthScheme::Basic(Basic {
        username: greptime.clone(),
        password: greptime.clone(),
    }));
    let re = db.sql("select * from numbers").await;
    let _ = re.unwrap();
}
