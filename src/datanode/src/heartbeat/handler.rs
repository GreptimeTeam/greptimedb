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

use async_trait::async_trait;
use common_meta::error::{InvalidHeartbeatResponseSnafu, Result as MetaResult};
use common_meta::heartbeat::handler::{
    HandleControl, HeartbeatResponseHandler, HeartbeatResponseHandlerContext,
};
use common_meta::instruction::{
    DowngradeRegion, DowngradeRegionReply, Instruction, InstructionReply, OpenRegion, SimpleReply,
};
use common_meta::RegionIdent;
use common_telemetry::error;
use futures::future::BoxFuture;
use snafu::OptionExt;
use store_api::path_utils::region_dir;
use store_api::region_engine::SetReadonlyResponse;
use store_api::region_request::{RegionCloseRequest, RegionOpenRequest, RegionRequest};
use store_api::storage::RegionId;

use crate::error;
use crate::region_server::RegionServer;
/// Handler for [Instruction::OpenRegion] and [Instruction::CloseRegion].
#[derive(Clone)]
pub struct RegionHeartbeatResponseHandler {
    region_server: RegionServer,
}

/// Handler of the instruction.
pub type InstructionHandler =
    Box<dyn FnOnce(RegionServer) -> BoxFuture<'static, InstructionReply> + Send>;

impl RegionHeartbeatResponseHandler {
    /// Returns the [RegionHeartbeatResponseHandler].
    pub fn new(region_server: RegionServer) -> Self {
        Self { region_server }
    }

    /// Builds the [InstructionHandler].
    fn build_handler(instruction: Instruction) -> MetaResult<InstructionHandler> {
        match instruction {
            Instruction::OpenRegion(OpenRegion {
                region_ident,
                region_storage_path,
                region_options,
                region_wal_options,
                skip_wal_replay,
            }) => Ok(Box::new(move |region_server| {
                Box::pin(async move {
                    let region_id = Self::region_ident_to_region_id(&region_ident);
                    // TODO(niebayes): extends region options with region_wal_options.
                    let _ = region_wal_options;
                    let request = RegionRequest::Open(RegionOpenRequest {
                        engine: region_ident.engine,
                        region_dir: region_dir(&region_storage_path, region_id),
                        options: region_options,
                        skip_wal_replay,
                    });
                    let result = region_server.handle_request(region_id, request).await;

                    let success = result.is_ok();
                    let error = result.as_ref().map_err(|e| e.to_string()).err();

                    InstructionReply::OpenRegion(SimpleReply {
                        result: success,
                        error,
                    })
                })
            })),
            Instruction::CloseRegion(region_ident) => Ok(Box::new(|region_server| {
                Box::pin(async move {
                    let region_id = Self::region_ident_to_region_id(&region_ident);
                    let request = RegionRequest::Close(RegionCloseRequest {});
                    let result = region_server.handle_request(region_id, request).await;

                    match result {
                        Ok(_) => InstructionReply::CloseRegion(SimpleReply {
                            result: true,
                            error: None,
                        }),
                        Err(error::Error::RegionNotFound { .. }) => {
                            InstructionReply::CloseRegion(SimpleReply {
                                result: true,
                                error: None,
                            })
                        }
                        Err(err) => InstructionReply::CloseRegion(SimpleReply {
                            result: false,
                            error: Some(err.to_string()),
                        }),
                    }
                })
            })),
            Instruction::DowngradeRegion(DowngradeRegion { region_id }) => {
                Ok(Box::new(move |region_server| {
                    Box::pin(async move {
                        match region_server.set_readonly_gracefully(region_id).await {
                            Ok(SetReadonlyResponse::Success { last_entry_id }) => {
                                InstructionReply::DowngradeRegion(DowngradeRegionReply {
                                    last_entry_id,
                                    exists: true,
                                    error: None,
                                })
                            }
                            Ok(SetReadonlyResponse::NotFound) => {
                                InstructionReply::DowngradeRegion(DowngradeRegionReply {
                                    last_entry_id: None,
                                    exists: false,
                                    error: None,
                                })
                            }
                            Err(err) => InstructionReply::DowngradeRegion(DowngradeRegionReply {
                                last_entry_id: None,
                                exists: false,
                                error: Some(err.to_string()),
                            }),
                        }
                    })
                }))
            }
            Instruction::UpgradeRegion(_) => {
                todo!()
            }
            Instruction::InvalidateTableIdCache(_) | Instruction::InvalidateTableNameCache(_) => {
                InvalidHeartbeatResponseSnafu.fail()
            }
        }
    }

    fn region_ident_to_region_id(region_ident: &RegionIdent) -> RegionId {
        RegionId::new(region_ident.table_id, region_ident.region_number)
    }
}

#[async_trait]
impl HeartbeatResponseHandler for RegionHeartbeatResponseHandler {
    fn is_acceptable(&self, ctx: &HeartbeatResponseHandlerContext) -> bool {
        matches!(
            ctx.incoming_message.as_ref(),
            Some((_, Instruction::OpenRegion { .. }))
                | Some((_, Instruction::CloseRegion { .. }))
                | Some((_, Instruction::DowngradeRegion { .. }))
        )
    }

    async fn handle(&self, ctx: &mut HeartbeatResponseHandlerContext) -> MetaResult<HandleControl> {
        let (meta, instruction) = ctx
            .incoming_message
            .take()
            .context(InvalidHeartbeatResponseSnafu)?;

        let mailbox = ctx.mailbox.clone();
        let region_server = self.region_server.clone();
        let handler = Self::build_handler(instruction)?;
        let _handle = common_runtime::spawn_bg(async move {
            let reply = handler(region_server).await;

            if let Err(e) = mailbox.send((meta, reply)).await {
                error!(e; "Failed to send reply to mailbox");
            }
        });

        Ok(HandleControl::Done)
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::collections::HashMap;
    use std::sync::Arc;

    use common_meta::heartbeat::mailbox::{
        HeartbeatMailbox, IncomingMessage, MailboxRef, MessageMeta,
    };
    use mito2::config::MitoConfig;
    use mito2::engine::MITO_ENGINE_NAME;
    use mito2::test_util::{CreateRequestBuilder, TestEnv};
    use store_api::region_request::RegionRequest;
    use store_api::storage::RegionId;
    use tokio::sync::mpsc::{self, Receiver};

    use super::*;
    use crate::error;
    use crate::tests::mock_region_server;

    pub struct HeartbeatResponseTestEnv {
        mailbox: MailboxRef,
        receiver: Receiver<(MessageMeta, InstructionReply)>,
    }

    impl HeartbeatResponseTestEnv {
        pub fn new() -> Self {
            let (tx, rx) = mpsc::channel(8);
            let mailbox = Arc::new(HeartbeatMailbox::new(tx));

            HeartbeatResponseTestEnv {
                mailbox,
                receiver: rx,
            }
        }

        pub fn create_handler_ctx(
            &self,
            incoming_message: IncomingMessage,
        ) -> HeartbeatResponseHandlerContext {
            HeartbeatResponseHandlerContext {
                mailbox: self.mailbox.clone(),
                response: Default::default(),
                incoming_message: Some(incoming_message),
            }
        }
    }

    fn close_region_instruction(region_id: RegionId) -> Instruction {
        Instruction::CloseRegion(RegionIdent {
            table_id: region_id.table_id(),
            region_number: region_id.region_number(),
            cluster_id: 1,
            datanode_id: 2,
            engine: MITO_ENGINE_NAME.to_string(),
        })
    }

    fn open_region_instruction(region_id: RegionId, path: &str) -> Instruction {
        Instruction::OpenRegion(OpenRegion::new(
            RegionIdent {
                table_id: region_id.table_id(),
                region_number: region_id.region_number(),
                cluster_id: 1,
                datanode_id: 2,
                engine: MITO_ENGINE_NAME.to_string(),
            },
            path,
            HashMap::new(),
            HashMap::new(),
            false,
        ))
    }

    #[tokio::test]
    async fn test_close_region() {
        common_telemetry::init_default_ut_logging();

        let mut region_server = mock_region_server();
        let heartbeat_handler = RegionHeartbeatResponseHandler::new(region_server.clone());

        let mut engine_env = TestEnv::with_prefix("close-region");
        let engine = engine_env.create_engine(MitoConfig::default()).await;
        region_server.register_engine(Arc::new(engine));
        let region_id = RegionId::new(1024, 1);

        let builder = CreateRequestBuilder::new();
        let create_req = builder.build();
        region_server
            .handle_request(region_id, RegionRequest::Create(create_req))
            .await
            .unwrap();

        let mut heartbeat_env = HeartbeatResponseTestEnv::new();

        // Should be ok, if we try to close it twice.
        for _ in 0..2 {
            let meta = MessageMeta::new_test(1, "test", "dn-1", "me-0");
            let instruction = close_region_instruction(region_id);

            let mut ctx = heartbeat_env.create_handler_ctx((meta, instruction));
            let control = heartbeat_handler.handle(&mut ctx).await.unwrap();
            assert_matches!(control, HandleControl::Done);

            let (_, reply) = heartbeat_env.receiver.recv().await.unwrap();

            if let InstructionReply::CloseRegion(reply) = reply {
                assert!(reply.result);
                assert!(reply.error.is_none());
            } else {
                unreachable!()
            }

            assert_matches!(
                region_server.set_writable(region_id, true).unwrap_err(),
                error::Error::RegionNotFound { .. }
            );
        }
    }

    #[tokio::test]
    async fn test_open_region_ok() {
        common_telemetry::init_default_ut_logging();

        let mut region_server = mock_region_server();
        let heartbeat_handler = RegionHeartbeatResponseHandler::new(region_server.clone());

        let mut engine_env = TestEnv::with_prefix("open-region");
        let engine = engine_env.create_engine(MitoConfig::default()).await;
        region_server.register_engine(Arc::new(engine));
        let region_id = RegionId::new(1024, 1);

        let builder = CreateRequestBuilder::new();
        let mut create_req = builder.build();
        let storage_path = "test";
        create_req.region_dir = region_dir(storage_path, region_id);

        region_server
            .handle_request(region_id, RegionRequest::Create(create_req))
            .await
            .unwrap();

        region_server
            .handle_request(region_id, RegionRequest::Close(RegionCloseRequest {}))
            .await
            .unwrap();
        let mut heartbeat_env = HeartbeatResponseTestEnv::new();

        // Should be ok, if we try to open it twice.
        for _ in 0..2 {
            let meta = MessageMeta::new_test(1, "test", "dn-1", "me-0");
            let instruction = open_region_instruction(region_id, storage_path);

            let mut ctx = heartbeat_env.create_handler_ctx((meta, instruction));
            let control = heartbeat_handler.handle(&mut ctx).await.unwrap();
            assert_matches!(control, HandleControl::Done);

            let (_, reply) = heartbeat_env.receiver.recv().await.unwrap();

            if let InstructionReply::OpenRegion(reply) = reply {
                assert!(reply.result);
                assert!(reply.error.is_none());
            } else {
                unreachable!()
            }
        }
    }

    #[tokio::test]
    async fn test_open_not_exists_region() {
        common_telemetry::init_default_ut_logging();

        let mut region_server = mock_region_server();
        let heartbeat_handler = RegionHeartbeatResponseHandler::new(region_server.clone());

        let mut engine_env = TestEnv::with_prefix("open-not-exists-region");
        let engine = engine_env.create_engine(MitoConfig::default()).await;
        region_server.register_engine(Arc::new(engine));
        let region_id = RegionId::new(1024, 1);
        let storage_path = "test";

        let mut heartbeat_env = HeartbeatResponseTestEnv::new();

        let meta = MessageMeta::new_test(1, "test", "dn-1", "me-0");
        let instruction = open_region_instruction(region_id, storage_path);

        let mut ctx = heartbeat_env.create_handler_ctx((meta, instruction));
        let control = heartbeat_handler.handle(&mut ctx).await.unwrap();
        assert_matches!(control, HandleControl::Done);

        let (_, reply) = heartbeat_env.receiver.recv().await.unwrap();

        if let InstructionReply::OpenRegion(reply) = reply {
            assert!(!reply.result);
            assert!(reply.error.is_some());
        } else {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn test_downgrade_region() {
        common_telemetry::init_default_ut_logging();

        let mut region_server = mock_region_server();
        let heartbeat_handler = RegionHeartbeatResponseHandler::new(region_server.clone());

        let mut engine_env = TestEnv::with_prefix("downgrade-region");
        let engine = engine_env.create_engine(MitoConfig::default()).await;
        region_server.register_engine(Arc::new(engine));
        let region_id = RegionId::new(1024, 1);

        let builder = CreateRequestBuilder::new();
        let mut create_req = builder.build();
        let storage_path = "test";
        create_req.region_dir = region_dir(storage_path, region_id);

        region_server
            .handle_request(region_id, RegionRequest::Create(create_req))
            .await
            .unwrap();

        let mut heartbeat_env = HeartbeatResponseTestEnv::new();

        // Should be ok, if we try to downgrade it twice.
        for _ in 0..2 {
            let meta = MessageMeta::new_test(1, "test", "dn-1", "me-0");
            let instruction = Instruction::DowngradeRegion(DowngradeRegion { region_id });

            let mut ctx = heartbeat_env.create_handler_ctx((meta, instruction));
            let control = heartbeat_handler.handle(&mut ctx).await.unwrap();
            assert_matches!(control, HandleControl::Done);

            let (_, reply) = heartbeat_env.receiver.recv().await.unwrap();

            if let InstructionReply::DowngradeRegion(reply) = reply {
                assert!(reply.exists);
                assert!(reply.error.is_none());
                assert_eq!(reply.last_entry_id.unwrap(), 0);
            } else {
                unreachable!()
            }
        }

        // Downgrades a not exists region.
        let meta = MessageMeta::new_test(1, "test", "dn-1", "me-0");
        let instruction = Instruction::DowngradeRegion(DowngradeRegion {
            region_id: RegionId::new(2048, 1),
        });
        let mut ctx = heartbeat_env.create_handler_ctx((meta, instruction));
        let control = heartbeat_handler.handle(&mut ctx).await.unwrap();
        assert_matches!(control, HandleControl::Done);

        let (_, reply) = heartbeat_env.receiver.recv().await.unwrap();

        if let InstructionReply::DowngradeRegion(reply) = reply {
            assert!(!reply.exists);
            assert!(reply.error.is_none());
            assert!(reply.last_entry_id.is_none());
        } else {
            unreachable!()
        }
    }
}
