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
use common_meta::instruction::{Instruction, InstructionReply};
use common_telemetry::error;
use snafu::OptionExt;
use store_api::storage::GcReport;

mod close_region;
mod downgrade_region;
mod file_ref;
mod flush_region;
mod gc_worker;
mod open_region;
mod upgrade_region;

use crate::heartbeat::handler::close_region::CloseRegionsHandler;
use crate::heartbeat::handler::downgrade_region::DowngradeRegionsHandler;
use crate::heartbeat::handler::file_ref::GetFileRefsHandler;
use crate::heartbeat::handler::flush_region::FlushRegionsHandler;
use crate::heartbeat::handler::gc_worker::GcRegionsHandler;
use crate::heartbeat::handler::open_region::OpenRegionsHandler;
use crate::heartbeat::handler::upgrade_region::UpgradeRegionsHandler;
use crate::heartbeat::task_tracker::TaskTracker;
use crate::region_server::RegionServer;

/// The handler for [`Instruction`]s.
#[derive(Clone)]
pub struct RegionHeartbeatResponseHandler {
    region_server: RegionServer,
    catchup_tasks: TaskTracker<()>,
    downgrade_tasks: TaskTracker<()>,
    flush_tasks: TaskTracker<()>,
    open_region_parallelism: usize,
    gc_tasks: TaskTracker<GcReport>,
}

#[async_trait::async_trait]
pub trait InstructionHandler: Send + Sync {
    type Instruction;
    async fn handle(
        &self,
        ctx: &HandlerContext,
        instruction: Self::Instruction,
    ) -> Option<InstructionReply>;
}

#[derive(Clone)]
pub struct HandlerContext {
    region_server: RegionServer,
    catchup_tasks: TaskTracker<()>,
    downgrade_tasks: TaskTracker<()>,
    flush_tasks: TaskTracker<()>,
    gc_tasks: TaskTracker<GcReport>,
}

impl HandlerContext {
    #[cfg(test)]
    pub fn new_for_test(region_server: RegionServer) -> Self {
        Self {
            region_server,
            catchup_tasks: TaskTracker::new(),
            downgrade_tasks: TaskTracker::new(),
            flush_tasks: TaskTracker::new(),
            gc_tasks: TaskTracker::new(),
        }
    }
}

impl RegionHeartbeatResponseHandler {
    /// Returns the [RegionHeartbeatResponseHandler].
    pub fn new(region_server: RegionServer) -> Self {
        Self {
            region_server,
            catchup_tasks: TaskTracker::new(),
            downgrade_tasks: TaskTracker::new(),
            flush_tasks: TaskTracker::new(),
            // Default to half of the number of CPUs.
            open_region_parallelism: (num_cpus::get() / 2).max(1),
            gc_tasks: TaskTracker::new(),
        }
    }

    /// Sets the parallelism for opening regions.
    pub fn with_open_region_parallelism(mut self, parallelism: usize) -> Self {
        self.open_region_parallelism = parallelism;
        self
    }

    fn build_handler(&self, instruction: &Instruction) -> MetaResult<Box<InstructionHandlers>> {
        match instruction {
            Instruction::CloseRegions(_) => Ok(Box::new(CloseRegionsHandler.into())),
            Instruction::OpenRegions(_) => Ok(Box::new(
                OpenRegionsHandler {
                    open_region_parallelism: self.open_region_parallelism,
                }
                .into(),
            )),
            Instruction::FlushRegions(_) => Ok(Box::new(FlushRegionsHandler.into())),
            Instruction::DowngradeRegions(_) => Ok(Box::new(DowngradeRegionsHandler.into())),
            Instruction::UpgradeRegions(_) => Ok(Box::new(UpgradeRegionsHandler.into())),
            Instruction::GetFileRefs(_) => Ok(Box::new(GetFileRefsHandler.into())),
            Instruction::GcRegions(_) => Ok(Box::new(GcRegionsHandler.into())),
            Instruction::InvalidateCaches(_) => InvalidHeartbeatResponseSnafu.fail(),
        }
    }
}

#[allow(clippy::enum_variant_names)]
pub enum InstructionHandlers {
    CloseRegions(CloseRegionsHandler),
    OpenRegions(OpenRegionsHandler),
    FlushRegions(FlushRegionsHandler),
    DowngradeRegions(DowngradeRegionsHandler),
    UpgradeRegions(UpgradeRegionsHandler),
    GetFileRefs(GetFileRefsHandler),
    GcRegions(GcRegionsHandler),
}

macro_rules! impl_from_handler {
    ($($handler:ident => $variant:ident),*) => {
        $(
            impl From<$handler> for InstructionHandlers {
                fn from(handler: $handler) -> Self {
                    InstructionHandlers::$variant(handler)
                }
            }
        )*
    };
}

impl_from_handler!(
    CloseRegionsHandler => CloseRegions,
    OpenRegionsHandler => OpenRegions,
    FlushRegionsHandler => FlushRegions,
    DowngradeRegionsHandler => DowngradeRegions,
    UpgradeRegionsHandler => UpgradeRegions,
    GetFileRefsHandler => GetFileRefs,
    GcRegionsHandler => GcRegions
);

macro_rules! dispatch_instr {
    (
        $( $instr_variant:ident => $handler_variant:ident ),* $(,)?
    ) => {
        impl InstructionHandlers {
            pub async fn handle(
                &self,
                ctx: &HandlerContext,
                instruction: Instruction,
            ) -> Option<InstructionReply> {
                match (self, instruction) {
                    $(
                        (
                            InstructionHandlers::$handler_variant(handler),
                            Instruction::$instr_variant(instr),
                        ) => handler.handle(ctx, instr).await,
                    )*
                    // Safety: must be used in pairs with `build_handler`.
                    _ => unreachable!(),
                }
            }
            /// Check whether this instruction is acceptable by any handler.
            pub fn is_acceptable(instruction: &Instruction) -> bool {
                matches!(
                    instruction,
                    $(
                        Instruction::$instr_variant { .. }
                    )|*
                )
            }
        }
    };
}

dispatch_instr!(
    CloseRegions => CloseRegions,
    OpenRegions => OpenRegions,
    FlushRegions => FlushRegions,
    DowngradeRegions => DowngradeRegions,
    UpgradeRegions => UpgradeRegions,
    GetFileRefs => GetFileRefs,
    GcRegions => GcRegions,
);

#[async_trait]
impl HeartbeatResponseHandler for RegionHeartbeatResponseHandler {
    fn is_acceptable(&self, ctx: &HeartbeatResponseHandlerContext) -> bool {
        if let Some((_, instruction)) = ctx.incoming_message.as_ref() {
            return InstructionHandlers::is_acceptable(instruction);
        }
        false
    }

    async fn handle(&self, ctx: &mut HeartbeatResponseHandlerContext) -> MetaResult<HandleControl> {
        let (meta, instruction) = ctx
            .incoming_message
            .take()
            .context(InvalidHeartbeatResponseSnafu)?;

        let mailbox = ctx.mailbox.clone();
        let region_server = self.region_server.clone();
        let catchup_tasks = self.catchup_tasks.clone();
        let downgrade_tasks = self.downgrade_tasks.clone();
        let flush_tasks = self.flush_tasks.clone();
        let gc_tasks = self.gc_tasks.clone();
        let handler = self.build_handler(&instruction)?;
        let _handle = common_runtime::spawn_global(async move {
            let reply = handler
                .handle(
                    &HandlerContext {
                        region_server,
                        catchup_tasks,
                        downgrade_tasks,
                        flush_tasks,
                        gc_tasks,
                    },
                    instruction,
                )
                .await;

            if let Some(reply) = reply
                && let Err(e) = mailbox.send((meta, reply)).await
            {
                error!(e; "Failed to send reply to mailbox");
            }
        });

        Ok(HandleControl::Continue)
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use common_meta::RegionIdent;
    use common_meta::heartbeat::mailbox::{
        HeartbeatMailbox, IncomingMessage, MailboxRef, MessageMeta,
    };
    use common_meta::instruction::{DowngradeRegion, OpenRegion, UpgradeRegion};
    use mito2::config::MitoConfig;
    use mito2::engine::MITO_ENGINE_NAME;
    use mito2::test_util::{CreateRequestBuilder, TestEnv};
    use store_api::path_utils::table_dir;
    use store_api::region_engine::RegionRole;
    use store_api::region_request::{RegionCloseRequest, RegionRequest};
    use store_api::storage::RegionId;
    use tokio::sync::mpsc::{self, Receiver};

    use super::*;
    use crate::error;
    use crate::tests::mock_region_server;

    pub struct HeartbeatResponseTestEnv {
        pub(crate) mailbox: MailboxRef,
        pub(crate) receiver: Receiver<(MessageMeta, InstructionReply)>,
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

    #[test]
    fn test_is_acceptable() {
        common_telemetry::init_default_ut_logging();
        let region_server = mock_region_server();
        let heartbeat_handler = RegionHeartbeatResponseHandler::new(region_server.clone());
        let heartbeat_env = HeartbeatResponseTestEnv::new();
        let meta = MessageMeta::new_test(1, "test", "dn-1", "me-0");

        // Open region
        let region_id = RegionId::new(1024, 1);
        let storage_path = "test";
        let instruction = open_region_instruction(region_id, storage_path);
        assert!(
            heartbeat_handler
                .is_acceptable(&heartbeat_env.create_handler_ctx((meta.clone(), instruction)))
        );

        // Close region
        let instruction = close_region_instruction(region_id);
        assert!(
            heartbeat_handler
                .is_acceptable(&heartbeat_env.create_handler_ctx((meta.clone(), instruction)))
        );

        // Downgrade region
        let instruction = Instruction::DowngradeRegions(vec![DowngradeRegion {
            region_id: RegionId::new(2048, 1),
            flush_timeout: Some(Duration::from_secs(1)),
        }]);
        assert!(
            heartbeat_handler
                .is_acceptable(&heartbeat_env.create_handler_ctx((meta.clone(), instruction)))
        );

        // Upgrade region
        let instruction = Instruction::UpgradeRegions(vec![UpgradeRegion {
            region_id,
            ..Default::default()
        }]);
        assert!(
            heartbeat_handler.is_acceptable(&heartbeat_env.create_handler_ctx((meta, instruction)))
        );
    }

    fn close_region_instruction(region_id: RegionId) -> Instruction {
        Instruction::CloseRegions(vec![RegionIdent {
            table_id: region_id.table_id(),
            region_number: region_id.region_number(),
            datanode_id: 2,
            engine: MITO_ENGINE_NAME.to_string(),
        }])
    }

    fn open_region_instruction(region_id: RegionId, path: &str) -> Instruction {
        Instruction::OpenRegions(vec![OpenRegion::new(
            RegionIdent {
                table_id: region_id.table_id(),
                region_number: region_id.region_number(),
                datanode_id: 2,
                engine: MITO_ENGINE_NAME.to_string(),
            },
            path,
            HashMap::new(),
            HashMap::new(),
            false,
        )])
    }

    #[tokio::test]
    async fn test_close_region() {
        common_telemetry::init_default_ut_logging();

        let mut region_server = mock_region_server();
        let heartbeat_handler = RegionHeartbeatResponseHandler::new(region_server.clone());

        let mut engine_env = TestEnv::with_prefix("close-region").await;
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
            assert_matches!(control, HandleControl::Continue);

            let (_, reply) = heartbeat_env.receiver.recv().await.unwrap();

            if let InstructionReply::CloseRegions(reply) = reply {
                assert!(reply.result);
                assert!(reply.error.is_none());
            } else {
                unreachable!()
            }

            assert_matches!(
                region_server
                    .set_region_role(region_id, RegionRole::Leader)
                    .unwrap_err(),
                error::Error::RegionNotFound { .. }
            );
        }
    }

    #[tokio::test]
    async fn test_open_region_ok() {
        common_telemetry::init_default_ut_logging();

        let mut region_server = mock_region_server();
        let heartbeat_handler = RegionHeartbeatResponseHandler::new(region_server.clone());

        let mut engine_env = TestEnv::with_prefix("open-region").await;
        let engine = engine_env.create_engine(MitoConfig::default()).await;
        region_server.register_engine(Arc::new(engine));
        let region_id = RegionId::new(1024, 1);

        let builder = CreateRequestBuilder::new();
        let mut create_req = builder.build();
        let storage_path = "test";
        create_req.table_dir = table_dir(storage_path, region_id.table_id());

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
            assert_matches!(control, HandleControl::Continue);

            let (_, reply) = heartbeat_env.receiver.recv().await.unwrap();

            if let InstructionReply::OpenRegions(reply) = reply {
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

        let mut engine_env = TestEnv::with_prefix("open-not-exists-region").await;
        let engine = engine_env.create_engine(MitoConfig::default()).await;
        region_server.register_engine(Arc::new(engine));
        let region_id = RegionId::new(1024, 1);
        let storage_path = "test";

        let mut heartbeat_env = HeartbeatResponseTestEnv::new();

        let meta = MessageMeta::new_test(1, "test", "dn-1", "me-0");
        let instruction = open_region_instruction(region_id, storage_path);

        let mut ctx = heartbeat_env.create_handler_ctx((meta, instruction));
        let control = heartbeat_handler.handle(&mut ctx).await.unwrap();
        assert_matches!(control, HandleControl::Continue);

        let (_, reply) = heartbeat_env.receiver.recv().await.unwrap();

        if let InstructionReply::OpenRegions(reply) = reply {
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

        let mut engine_env = TestEnv::with_prefix("downgrade-region").await;
        let engine = engine_env.create_engine(MitoConfig::default()).await;
        region_server.register_engine(Arc::new(engine));
        let region_id = RegionId::new(1024, 1);

        let builder = CreateRequestBuilder::new();
        let mut create_req = builder.build();
        let storage_path = "test";
        create_req.table_dir = table_dir(storage_path, region_id.table_id());

        region_server
            .handle_request(region_id, RegionRequest::Create(create_req))
            .await
            .unwrap();

        let mut heartbeat_env = HeartbeatResponseTestEnv::new();

        // Should be ok, if we try to downgrade it twice.
        for _ in 0..2 {
            let meta = MessageMeta::new_test(1, "test", "dn-1", "me-0");
            let instruction = Instruction::DowngradeRegions(vec![DowngradeRegion {
                region_id,
                flush_timeout: Some(Duration::from_secs(1)),
            }]);

            let mut ctx = heartbeat_env.create_handler_ctx((meta, instruction));
            let control = heartbeat_handler.handle(&mut ctx).await.unwrap();
            assert_matches!(control, HandleControl::Continue);

            let (_, reply) = heartbeat_env.receiver.recv().await.unwrap();

            let reply = &reply.expect_downgrade_regions_reply()[0];
            assert!(reply.exists);
            assert!(reply.error.is_none());
            assert_eq!(reply.last_entry_id.unwrap(), 0);
        }

        // Downgrades a not exists region.
        let meta = MessageMeta::new_test(1, "test", "dn-1", "me-0");
        let instruction = Instruction::DowngradeRegions(vec![DowngradeRegion {
            region_id: RegionId::new(2048, 1),
            flush_timeout: Some(Duration::from_secs(1)),
        }]);
        let mut ctx = heartbeat_env.create_handler_ctx((meta, instruction));
        let control = heartbeat_handler.handle(&mut ctx).await.unwrap();
        assert_matches!(control, HandleControl::Continue);

        let (_, reply) = heartbeat_env.receiver.recv().await.unwrap();

        let reply = reply.expect_downgrade_regions_reply();
        assert!(!reply[0].exists);
        assert!(reply[0].error.is_none());
        assert!(reply[0].last_entry_id.is_none());
    }
}
