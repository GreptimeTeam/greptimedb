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

use std::sync::Arc;

use api::v1::meta::HeartbeatResponse;
use catalog::CatalogManagerRef;
use common_meta::instruction::{Instruction, InstructionReply};
use table::engine::manager::TableEngineManagerRef;
use table::requests::{CloseTableRequest, OpenTableRequest};
use tokio::sync::Mutex;

use crate::error::Result;
use crate::heartbeat::utils::mailbox_message_to_incoming_message;

mod close_table;
mod open_table;

pub type IncomingMessage = (MessageMeta, Instruction);
pub type OutgoingMessage = (MessageMeta, InstructionReply);
pub struct MessageMeta {
    pub id: u64,
    pub subject: String,
    pub to: String,
    pub from: String,
}

#[derive(Default)]
pub struct HeartbeatMailbox {
    pub replies: Vec<OutgoingMessage>,
}

pub type MailboxRef = Arc<Mutex<HeartbeatMailbox>>;

pub type HeartbeatResponseHeadlerExecutorRef = Arc<dyn HeartbeatResponseHeadlerExecutor>;

pub struct HeartbeatResponseHandlerContext {
    pub mailbox: MailboxRef,
}

impl From<MailboxRef> for HeartbeatResponseHandlerContext {
    fn from(mailbox: MailboxRef) -> Self {
        Self { mailbox }
    }
}

pub trait HeartbeatResponseHeadler: Send + Sync {
    fn handle(
        &self,
        ctx: &mut HeartbeatResponseHandlerContext,
        resp: &HeartbeatResponse,
    ) -> Result<()>;
}

pub trait HeartbeatResponseHeadlerExecutor: Send + Sync {
    fn handle(&self, ctx: HeartbeatResponseHandlerContext, resp: HeartbeatResponse) -> Result<()>;
}

pub struct NaiveHandlerExecutor {
    handler: Arc<dyn HeartbeatResponseHeadler>,
}

impl NaiveHandlerExecutor {
    pub fn new(handler: Arc<dyn HeartbeatResponseHeadler>) -> Self {
        Self { handler }
    }
}

impl HeartbeatResponseHeadlerExecutor for NaiveHandlerExecutor {
    fn handle(
        &self,
        mut ctx: HeartbeatResponseHandlerContext,
        resp: HeartbeatResponse,
    ) -> Result<()> {
        self.handler.handle(&mut ctx, &resp)
    }
}

#[derive(Clone)]
pub struct InstructionHandler {
    catalog_manager: CatalogManagerRef,
    table_engine_manager: TableEngineManagerRef,
}

impl InstructionHandler {
    pub fn new(
        catalog_manager: CatalogManagerRef,
        table_engine_manager: TableEngineManagerRef,
    ) -> Self {
        Self {
            catalog_manager,
            table_engine_manager,
        }
    }

    pub fn execute(
        &self,
        ctx: &mut HeartbeatResponseHandlerContext,
        (meta, instruction): IncomingMessage,
    ) -> Result<()> {
        match instruction {
            Instruction::OpenTable {
                catalog,
                schema,
                table,
                table_id,
                engine,
                ..
            } => self.handle_open_table(
                ctx,
                meta,
                engine,
                OpenTableRequest {
                    catalog_name: catalog,
                    schema_name: schema,
                    table_name: table,
                    table_id,
                },
            ),
            Instruction::CloseTable {
                catalog,
                schema,
                table,
                table_id,
                engine,
                ..
            } => self.handle_close_table(
                ctx,
                meta,
                engine,
                CloseTableRequest {
                    catalog_name: catalog,
                    schema_name: schema,
                    table_name: table,
                    table_id,
                },
            ),
        }
        Ok(())
    }
}

impl HeartbeatResponseHeadler for InstructionHandler {
    fn handle(
        &self,
        ctx: &mut HeartbeatResponseHandlerContext,
        resp: &HeartbeatResponse,
    ) -> Result<()> {
        let executable_messages = resp
            .mailbox_messages
            .iter()
            .filter_map(|m| {
                m.payload
                    .as_ref()
                    .map(|_| mailbox_message_to_incoming_message(m.clone()))
            })
            .collect::<Result<Vec<IncomingMessage>>>()?;

        for msg in executable_messages {
            self.execute(ctx, msg)?;
        }

        Ok(())
    }
}
