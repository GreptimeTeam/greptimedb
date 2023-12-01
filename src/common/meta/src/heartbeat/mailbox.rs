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

use tokio::sync::mpsc::Sender;

use crate::error::{self, Result};
use crate::instruction::{Instruction, InstructionReply};

pub type IncomingMessage = (MessageMeta, Instruction);
pub type OutgoingMessage = (MessageMeta, InstructionReply);

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct MessageMeta {
    pub id: u64,
    pub subject: String,
    pub to: String,
    pub from: String,
}

impl MessageMeta {
    #[cfg(any(test, feature = "testing"))]
    pub fn new_test(id: u64, subject: &str, to: &str, from: &str) -> Self {
        MessageMeta {
            id,
            subject: subject.to_string(),
            to: to.to_string(),
            from: from.to_string(),
        }
    }
}

pub struct HeartbeatMailbox {
    sender: Sender<OutgoingMessage>,
}

impl HeartbeatMailbox {
    pub fn new(sender: Sender<OutgoingMessage>) -> Self {
        Self { sender }
    }

    pub async fn send(&self, message: OutgoingMessage) -> Result<()> {
        self.sender.send(message).await.map_err(|e| {
            error::SendMessageSnafu {
                err_msg: e.to_string(),
            }
            .build()
        })
    }
}

pub type MailboxRef = Arc<HeartbeatMailbox>;
