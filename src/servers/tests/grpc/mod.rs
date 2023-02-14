use std::net::SocketAddr;
use std::sync::Arc;

use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use async_trait::async_trait;
use common_runtime::{Builder as RuntimeBuilder, Runtime};
use servers::auth::UserProviderRef;
use servers::error::{Result, StartGrpcSnafu, TcpBindSnafu};
use servers::grpc::flight::FlightHandler;
use servers::query_handler::grpc::ServerGrpcQueryHandlerRef;
use servers::server::Server;
use snafu::ResultExt;
use table::test_util::MemTable;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

use crate::auth::MockUserProvider;
use crate::{create_testing_grpc_query_handler, LOCALHOST_WITH_0};

struct MockGrpcServer {
    query_handler: ServerGrpcQueryHandlerRef,
    user_provider: Option<UserProviderRef>,
    runtime: Arc<Runtime>,
}

impl MockGrpcServer {
    fn new(
        query_handler: ServerGrpcQueryHandlerRef,
        user_provider: Option<UserProviderRef>,
        runtime: Arc<Runtime>,
    ) -> Self {
        Self {
            query_handler,
            user_provider,
            runtime,
        }
    }

    fn create_service(&self) -> FlightServiceServer<impl FlightService> {
        let service = FlightHandler::new(
            self.query_handler.clone(),
            self.user_provider.clone(),
            self.runtime.clone(),
        );
        FlightServiceServer::new(service)
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
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .context(StartGrpcSnafu)
                .unwrap()
        });

        Ok(addr)
    }
}

fn create_grpc_server(table: MemTable) -> Result<Arc<dyn Server>> {
    let query_handler = create_testing_grpc_query_handler(table);
    let io_runtime = Arc::new(
        RuntimeBuilder::default()
            .worker_threads(4)
            .thread_name("grpc-io-handlers")
            .build()
            .unwrap(),
    );

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
    assert!(re.is_ok());
}

#[tokio::test]
async fn test_grpc_query() {
    // let server = create_grpc_server(MemTable::default_numbers_table()).unwrap();
    // let re = server.start(LOCALHOST_WITH_0.parse().unwrap()).await;
    // assert!(re.is_ok());
    //
    // let grpc_client = Client::with_urls(vec![re.unwrap().to_string()]);
    // TODO(shuiyisong): wait db to support grpc request input
    // let db = Database::with_client(grpc_client);
}
