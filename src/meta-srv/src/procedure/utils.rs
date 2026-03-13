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

use std::time::Duration;

use api::v1::meta::MailboxMessage;
use common_meta::instruction::{FlushErrorStrategy, FlushRegions, Instruction, InstructionReply};
use common_meta::peer::Peer;
use common_telemetry::tracing_context::TracingContext;
use common_telemetry::{info, warn};
use snafu::ResultExt;
use store_api::storage::RegionId;
use tokio::time::Instant;

use crate::error::{self, Error, Result};
use crate::handler::HeartbeatMailbox;
use crate::service::mailbox::{Channel, MailboxRef};

pub(crate) enum ErrorStrategy {
    Ignore,
    Retry,
}

fn handle_flush_region_reply(
    reply: &InstructionReply,
    region_ids: &[RegionId],
    msg: &MailboxMessage,
) -> Result<(bool, Option<String>)> {
    let result = match reply {
        InstructionReply::FlushRegions(flush_reply) => {
            if flush_reply.results.len() != region_ids.len() {
                return error::UnexpectedInstructionReplySnafu {
                    mailbox_message: msg.to_string(),
                    reason: format!(
                        "expect {} region flush result, but got {}",
                        region_ids.len(),
                        flush_reply.results.len()
                    ),
                }
                .fail();
            }

            match flush_reply.overall_success {
                true => (true, None),
                false => (
                    false,
                    Some(
                        flush_reply
                            .results
                            .iter()
                            .filter_map(|(region_id, result)| match result {
                                Ok(_) => None,
                                Err(e) => Some(format!("{}: {:?}", region_id, e)),
                            })
                            .collect::<Vec<String>>()
                            .join("; "),
                    ),
                ),
            }
        }
        _ => {
            return error::UnexpectedInstructionReplySnafu {
                mailbox_message: msg.to_string(),
                reason: "expect flush region reply",
            }
            .fail();
        }
    };

    Ok(result)
}

/// Flushes the regions on the datanode.
///
/// Retry Or Ignore:
/// - [PusherNotFound](error::Error::PusherNotFound), The datanode is unreachable.
/// - Failed to flush region on the Datanode.
///
/// Abort:
/// - [MailboxTimeout](error::Error::MailboxTimeout), Timeout.
/// - [UnexpectedInstructionReply](error::Error::UnexpectedInstructionReply).
/// - [ExceededDeadline](error::Error::ExceededDeadline)
/// - Invalid JSON.
pub(crate) async fn flush_region(
    mailbox: &MailboxRef,
    server_addr: &str,
    region_ids: &[RegionId],
    datanode: &Peer,
    timeout: Duration,
    error_strategy: ErrorStrategy,
) -> Result<()> {
    let flush_instruction = Instruction::FlushRegions(FlushRegions::sync_batch(
        region_ids.to_vec(),
        FlushErrorStrategy::TryAll,
    ));

    let tracing_ctx = TracingContext::from_current_span();
    let msg = MailboxMessage::json_message(
        &format!("Flush regions: {:?}", region_ids),
        &format!("Metasrv@{}", server_addr),
        &format!("Datanode-{}@{}", datanode.id, datanode.addr),
        common_time::util::current_time_millis(),
        &flush_instruction,
        Some(tracing_ctx.to_w3c()),
    )
    .with_context(|_| error::SerializeToJsonSnafu {
        input: flush_instruction.to_string(),
    })?;

    let ch = Channel::Datanode(datanode.id);
    let now = Instant::now();
    let receiver = mailbox.send(&ch, msg, timeout).await;
    let receiver = match receiver {
        Ok(receiver) => receiver,
        Err(error::Error::PusherNotFound { .. }) => match error_strategy {
            ErrorStrategy::Ignore => {
                warn!(
                    "Failed to flush regions({:?}), the datanode({}) is unreachable(PusherNotFound). Skip flush operation.",
                    region_ids, datanode
                );
                return Ok(());
            }
            ErrorStrategy::Retry => error::RetryLaterSnafu {
                reason: format!(
                    "Pusher not found for flush regions on datanode {:?}, elapsed: {:?}",
                    datanode,
                    now.elapsed()
                ),
            }
            .fail()?,
        },
        Err(err) => {
            return Err(err);
        }
    };

    match receiver.await {
        Ok(msg) => {
            let reply = HeartbeatMailbox::json_reply(&msg)?;
            info!(
                "Received flush region reply: {:?}, regions: {:?}, elapsed: {:?}",
                reply,
                region_ids,
                now.elapsed()
            );
            let (result, error) = handle_flush_region_reply(&reply, region_ids, &msg)?;
            if let Some(error) = error {
                match error_strategy {
                    ErrorStrategy::Ignore => {
                        warn!(
                            "Failed to flush regions {:?}, the datanode({}) error is ignored: {}",
                            region_ids, datanode, error
                        );
                    }
                    ErrorStrategy::Retry => {
                        return error::RetryLaterSnafu {
                            reason: format!(
                                "Failed to flush regions {:?}, the datanode({}) error is retried: {}",
                                region_ids,
                                datanode,
                                error,
                            ),
                        }
                        .fail()?;
                    }
                }
            } else if result {
                info!(
                    "The flush regions {:?} on datanode {:?} is successful, elapsed: {:?}",
                    region_ids,
                    datanode,
                    now.elapsed()
                );
            }

            Ok(())
        }
        Err(Error::MailboxTimeout { .. }) => error::ExceededDeadlineSnafu {
            operation: "Flush regions",
        }
        .fail(),
        Err(err) => Err(err),
    }
}

#[cfg(any(test, feature = "mock"))]
pub mod mock {
    use std::io::Error;
    use std::sync::Arc;

    use api::v1::region::region_server::RegionServer;
    use api::v1::region::{RegionResponse, region_request};
    use api::v1::{ResponseHeader, Status as PbStatus};
    use async_trait::async_trait;
    use client::Client;
    use common_grpc::channel_manager::ChannelManager;
    use common_meta::peer::Peer;
    use common_runtime::runtime::BuilderBuild;
    use common_runtime::{Builder as RuntimeBuilder, Runtime};
    use hyper_util::rt::TokioIo;
    use servers::grpc::region_server::{RegionServerHandler, RegionServerRequestHandler};
    use tokio::sync::mpsc;
    use tonic::codec::CompressionEncoding;
    use tonic::transport::Server;
    use tower::service_fn;

    /// An mock implementation of region server that simply echoes the request.
    #[derive(Clone)]
    pub struct EchoRegionServer {
        runtime: Runtime,
        received_requests: mpsc::Sender<region_request::Body>,
    }

    impl EchoRegionServer {
        pub fn new() -> (Self, mpsc::Receiver<region_request::Body>) {
            let (tx, rx) = mpsc::channel(10);
            (
                Self {
                    runtime: RuntimeBuilder::default().worker_threads(2).build().unwrap(),
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
                    .add_service(
                        RegionServer::new(handler)
                            .accept_compressed(CompressionEncoding::Gzip)
                            .accept_compressed(CompressionEncoding::Zstd)
                            .send_compressed(CompressionEncoding::Gzip)
                            .send_compressed(CompressionEncoding::Zstd),
                    )
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
                        async move { Ok::<_, Error>(TokioIo::new(client)) }
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
                        err_msg: String::default(),
                    }),
                }),
                affected_rows: 0,
                extensions: Default::default(),
                metadata: Vec::new(),
            })
        }
    }
}

#[cfg(test)]
pub mod test_data {
    use std::sync::Arc;

    use chrono::DateTime;
    use common_catalog::consts::MITO2_ENGINE;
    use common_meta::ddl::flow_meta::FlowMetadataAllocator;
    use common_meta::ddl::table_meta::TableMetadataAllocator;
    use common_meta::ddl::{DdlContext, NoopRegionFailureDetectorControl};
    use common_meta::key::TableMetadataManager;
    use common_meta::key::flow::FlowMetadataManager;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::node_manager::NodeManagerRef;
    use common_meta::peer::Peer;
    use common_meta::region_keeper::MemoryRegionKeeper;
    use common_meta::region_registry::LeaderRegionRegistry;
    use common_meta::rpc::router::RegionRoute;
    use common_meta::sequence::SequenceBuilder;
    use common_meta::wal_provider::WalProvider;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use table::metadata::{TableIdent, TableInfo, TableMeta, TableType};
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

    pub fn new_table_info() -> TableInfo {
        TableInfo {
            ident: TableIdent {
                table_id: 42,
                version: 1,
            },
            name: "my_table".to_string(),
            desc: Some("blabla".to_string()),
            catalog_name: "my_catalog".to_string(),
            schema_name: "my_schema".to_string(),
            meta: TableMeta {
                schema: Arc::new(Schema::new(vec![
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
                ])),
                primary_key_indices: vec![1, 2],
                value_indices: vec![2],
                engine: MITO2_ENGINE.to_string(),
                next_column_id: 3,
                options: TableOptions::default(),
                created_on: DateTime::default(),
                updated_on: DateTime::default(),
                partition_key_indices: vec![],
                column_ids: vec![],
            },
            table_type: TableType::Base,
        }
    }

    pub(crate) fn new_ddl_context(node_manager: NodeManagerRef) -> DdlContext {
        let kv_backend = Arc::new(MemoryKvBackend::new());

        let mailbox_sequence =
            SequenceBuilder::new("test_heartbeat_mailbox", kv_backend.clone()).build();
        let mailbox = HeartbeatMailbox::create(Pushers::default(), mailbox_sequence);
        let table_metadata_manager = Arc::new(TableMetadataManager::new(kv_backend.clone()));
        let table_metadata_allocator = Arc::new(TableMetadataAllocator::new(
            Arc::new(SequenceBuilder::new("test", kv_backend.clone()).build()),
            Arc::new(WalProvider::default()),
        ));
        let flow_metadata_manager = Arc::new(FlowMetadataManager::new(kv_backend.clone()));
        let flow_metadata_allocator = Arc::new(FlowMetadataAllocator::with_noop_peer_allocator(
            Arc::new(SequenceBuilder::new("test", kv_backend).build()),
        ));
        DdlContext {
            node_manager,
            cache_invalidator: Arc::new(MetasrvCacheInvalidator::new(
                mailbox,
                MetasrvInfo {
                    server_addr: "127.0.0.1:4321".to_string(),
                },
            )),
            table_metadata_manager,
            table_metadata_allocator,
            flow_metadata_manager,
            flow_metadata_allocator,
            memory_region_keeper: Arc::new(MemoryRegionKeeper::new()),
            leader_region_registry: Arc::new(LeaderRegionRegistry::default()),
            region_failure_detector_controller: Arc::new(NoopRegionFailureDetectorControl),
        }
    }
}
