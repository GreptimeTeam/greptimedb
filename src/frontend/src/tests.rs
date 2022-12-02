// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use client::Client;
use common_grpc::channel_manager::ChannelManager;
use common_runtime::Builder as RuntimeBuilder;
use datanode::instance::Instance as DatanodeInstance;
use servers::grpc::GrpcServer;
use tonic::transport::Server;
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
    let datanode_instance: Arc<DatanodeInstance> = create_datanode_instance().await;
    let dn_catalog_manager = datanode_instance.catalog_manager().clone();
    let (_, client) = create_datanode_client(datanode_instance).await;
    Arc::new(Instance::with_client_and_catalog_manager(
        client,
        dn_catalog_manager,
    ))
}

pub(crate) async fn create_datanode_client(
    datanode_instance: Arc<DatanodeInstance>,
) -> (String, Client) {
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
    // "127.0.0.1:3001" is just a placeholder, does not actually connect to it.
    let addr = "127.0.0.1:3001";
    let channel_manager = ChannelManager::new();
    channel_manager
        .reset_with_connector(
            addr,
            service_fn(move |_| {
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
            }),
        )
        .unwrap();
    (
        addr.to_string(),
        Client::with_manager_and_urls(channel_manager, vec![addr]),
    )
}
