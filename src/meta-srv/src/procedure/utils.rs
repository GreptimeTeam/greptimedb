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

use store_api::storage::{RegionNumber, TableId};

pub fn region_lock_key(table_id: TableId, region_number: RegionNumber) -> String {
    format!("{}/region-{}", table_id, region_number)
}

#[cfg(feature = "mock")]
pub mod mock {
    use std::io::Error;
    use std::sync::Arc;

    use api::v1::region::region_server::RegionServer;
    use api::v1::region::{region_request, RegionResponse};
    use api::v1::{ResponseHeader, Status as PbStatus};
    use async_trait::async_trait;
    use client::Client;
    use common_grpc::channel_manager::ChannelManager;
    use common_meta::peer::Peer;
    use common_runtime::{Builder as RuntimeBuilder, Runtime};
    use servers::grpc::region_server::{RegionServerHandler, RegionServerRequestHandler};
    use tokio::sync::mpsc;
    use tonic::transport::Server;
    use tower::service_fn;

    /// An mock implementation of region server that simply echoes the request.
    #[derive(Clone)]
    pub struct EchoRegionServer {
        runtime: Arc<Runtime>,
        received_requests: mpsc::Sender<region_request::Body>,
    }

    impl EchoRegionServer {
        pub fn new() -> (Self, mpsc::Receiver<region_request::Body>) {
            let (tx, rx) = mpsc::channel(10);
            (
                Self {
                    runtime: Arc::new(RuntimeBuilder::default().worker_threads(2).build().unwrap()),
                    received_requests: tx,
                },
                rx,
            )
        }

        pub fn new_client(&self, datanode: &Peer) -> Client {
            let (client, server) = tokio::io::duplex(1024);

            let handler =
                RegionServerRequestHandler::new(Arc::new(self.clone()), self.runtime.clone());

            tokio::spawn(async move {
                Server::builder()
                    .add_service(RegionServer::new(handler))
                    .serve_with_incoming(futures::stream::iter(vec![Ok::<_, Error>(server)]))
                    .await
            });

            let channel_manager = ChannelManager::new();
            let mut client = Some(client);
            channel_manager
                .reset_with_connector(
                    datanode.addr.clone(),
                    service_fn(move |_| {
                        let client = client.take().unwrap();
                        async move { Ok::<_, Error>(client) }
                    }),
                )
                .unwrap();
            Client::with_manager_and_urls(channel_manager, vec![datanode.addr.clone()])
        }
    }

    #[async_trait]
    impl RegionServerHandler for EchoRegionServer {
        async fn handle(
            &self,
            request: region_request::Body,
        ) -> servers::error::Result<RegionResponse> {
            self.received_requests.send(request).await.unwrap();

            Ok(RegionResponse {
                header: Some(ResponseHeader {
                    status: Some(PbStatus {
                        status_code: 0,
                        err_msg: "".to_string(),
                    }),
                }),
                affected_rows: 0,
            })
        }
    }
}

#[cfg(test)]
pub mod test_data {
    use std::sync::Arc;

    use chrono::DateTime;
    use common_catalog::consts::MITO2_ENGINE;
    use common_meta::datanode_manager::DatanodeManagerRef;
    use common_meta::ddl::DdlContext;
    use common_meta::key::TableMetadataManager;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::peer::Peer;
    use common_meta::region_keeper::MemoryRegionKeeper;
    use common_meta::rpc::router::RegionRoute;
    use common_meta::sequence::SequenceBuilder;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, RawSchema};
    use table::metadata::{RawTableInfo, RawTableMeta, TableIdent, TableType};
    use table::requests::TableOptions;

    use crate::cache_invalidator::MetasrvCacheInvalidator;
    use crate::handler::{HeartbeatMailbox, Pushers};
    use crate::metasrv::MetasrvInfo;
    use crate::test_util::new_region_route;

    pub fn new_region_routes() -> Vec<RegionRoute> {
        let peers = vec![
            Peer::new(1, "127.0.0.1:4001"),
            Peer::new(2, "127.0.0.1:4002"),
            Peer::new(3, "127.0.0.1:4003"),
        ];
        vec![
            new_region_route(1, &peers, 3),
            new_region_route(2, &peers, 2),
            new_region_route(3, &peers, 1),
        ]
    }

    pub fn new_table_info() -> RawTableInfo {
        RawTableInfo {
            ident: TableIdent {
                table_id: 42,
                version: 1,
            },
            name: "my_table".to_string(),
            desc: Some("blabla".to_string()),
            catalog_name: "my_catalog".to_string(),
            schema_name: "my_schema".to_string(),
            meta: RawTableMeta {
                schema: RawSchema {
                    column_schemas: vec![
                        ColumnSchema::new(
                            "ts".to_string(),
                            ConcreteDataType::timestamp_millisecond_datatype(),
                            false,
                        ),
                        ColumnSchema::new(
                            "my_tag1".to_string(),
                            ConcreteDataType::string_datatype(),
                            true,
                        ),
                        ColumnSchema::new(
                            "my_tag2".to_string(),
                            ConcreteDataType::string_datatype(),
                            true,
                        ),
                        ColumnSchema::new(
                            "my_field_column".to_string(),
                            ConcreteDataType::int32_datatype(),
                            true,
                        ),
                    ],
                    timestamp_index: Some(0),
                    version: 0,
                },
                primary_key_indices: vec![1, 2],
                value_indices: vec![2],
                engine: MITO2_ENGINE.to_string(),
                next_column_id: 3,
                region_numbers: vec![1, 2, 3],
                options: TableOptions::default(),
                created_on: DateTime::default(),
                partition_key_indices: vec![],
            },
            table_type: TableType::Base,
        }
    }

    pub(crate) fn new_ddl_context(datanode_manager: DatanodeManagerRef) -> DdlContext {
        let kv_backend = Arc::new(MemoryKvBackend::new());

        let mailbox_sequence =
            SequenceBuilder::new("test_heartbeat_mailbox", kv_backend.clone()).build();
        let mailbox = HeartbeatMailbox::create(Pushers::default(), mailbox_sequence);

        DdlContext {
            datanode_manager,
            cache_invalidator: Arc::new(MetasrvCacheInvalidator::new(
                mailbox,
                MetasrvInfo {
                    server_addr: "127.0.0.1:4321".to_string(),
                },
            )),
            table_metadata_manager: Arc::new(TableMetadataManager::new(kv_backend)),
            memory_region_keeper: Arc::new(MemoryRegionKeeper::new()),
        }
    }
}
