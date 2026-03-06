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
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use common_telemetry::{info, warn};

use crate::error::Result;
use crate::heartbeat::handler::{
    HandleControl, HeartbeatResponseHandler, HeartbeatResponseHandlerContext,
};
use crate::instruction::Instruction;

/// A heartbeat response handler that handles special "suspend" error.
/// It will simply set or clear (if previously set) the inner suspend atomic state.
pub struct SuspendHandler {
    suspend: Arc<AtomicBool>,
}

impl SuspendHandler {
    pub fn new(suspend: Arc<AtomicBool>) -> Self {
        Self { suspend }
    }
}

#[async_trait]
impl HeartbeatResponseHandler for SuspendHandler {
    fn is_acceptable(&self, context: &HeartbeatResponseHandlerContext) -> bool {
        matches!(
            context.incoming_message,
            Some((_, _, Instruction::Suspend)) | None
        )
    }

    async fn handle(&self, context: &mut HeartbeatResponseHandlerContext) -> Result<HandleControl> {
        let flip_state = |expect: bool| {
            self.suspend
                .compare_exchange(expect, !expect, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        };

        if let Some((_, _, Instruction::Suspend)) = context.incoming_message.take() {
            if flip_state(false) {
                warn!("Suspend instruction received from meta, entering suspension state");
            }
        } else {
            // Suspended components are made always tried to get rid of this state, we don't want
            // an "un-suspend" instruction to resume them running. That can be error-prone.
            // So if the "suspend" instruction is not found in the heartbeat, just unset the state.
            if flip_state(true) {
                info!("clear suspend state");
            }
        }
        Ok(HandleControl::Continue)
    }
}
