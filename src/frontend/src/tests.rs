use std::sync::Arc;

use api::v1::greptime_client::GreptimeClient;
use client::Client;
use common_runtime::Builder as RuntimeBuilder;
use datanode::instance::Instance as DatanodeInstance;
use servers::grpc::GrpcServer;
use tonic::transport::{Endpoint, Server};
use tower::service_fn;

use crate::instance::Instance;

async fn create_datanode_instance() -> Arc<DatanodeInstance> {
    // TODO(LFC) Use real Mito engine when we can alter its region schema,
    //   and delete the `new_mock` method.
    let instance = Arc::new(DatanodeInstance::new_mock().await.unwrap());
    instance.start().await.unwrap();
    instance
}

pub(crate) async fn create_frontend_instance() -> Arc<Instance> {
    let datanode_instance = create_datanode_instance().await;

    let (client, server) = tokio::io::duplex(1024);

    let runtime = Arc::new(
        RuntimeBuilder::default()
            .worker_threads(2)
            .thread_name("grpc-handlers")
            .build()
            .unwrap(),
    );

    // create a mock datanode grpc service, see example here:
    // https://github.com/hyperium/tonic/blob/master/examples/src/mock/mock.rs
    let datanode_service =
        GrpcServer::new(datanode_instance.clone(), datanode_instance, runtime).create_service();
    tokio::spawn(async move {
        Server::builder()
            .add_service(datanode_service)
            .serve_with_incoming(futures::stream::iter(vec![Ok::<_, std::io::Error>(server)]))
            .await
    });

    // Move client to an option so we can _move_ the inner value
    // on the first attempt to connect. All other attempts will fail.
    let mut client = Some(client);
    // "http://[::]:50051" is just a placeholder, does not actually connect to it,
    // see https://github.com/hyperium/tonic/issues/727#issuecomment-881532934
    let channel = Endpoint::try_from("http://[::]:50051")
        .unwrap()
        .connect_with_connector(service_fn(move |_| {
            let client = client.take();

            async move {
                if let Some(client) = client {
                    Ok(client)
                } else {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Client already taken",
                    ))
                }
            }
        }))
        .await
        .unwrap();
    let client = Client::with_client(GreptimeClient::new(channel));
    Arc::new(Instance::with_client(client))
}
