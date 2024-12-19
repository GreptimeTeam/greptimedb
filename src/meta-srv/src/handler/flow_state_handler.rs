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

use api::v1::meta::{FlowStat, HeartbeatRequest, Role};
use common_meta::key::flow::flow_state::{FlowStateManager, FlowStateValue};
use snafu::ResultExt;

use crate::error::{FlowStateHandlerSnafu, Result};
use crate::handler::{HandleControl, HeartbeatAccumulator, HeartbeatHandler};
use crate::metasrv::Context;

pub struct FlowStateHandler {
    flow_state_manager: FlowStateManager,
}

impl FlowStateHandler {
    pub fn new(flow_state_manager: FlowStateManager) -> Self {
        Self { flow_state_manager }
    }
}

#[async_trait::async_trait]
impl HeartbeatHandler for FlowStateHandler {
    fn is_acceptable(&self, role: Role) -> bool {
        role == Role::Flownode
    }

    async fn handle(
        &self,
        req: &HeartbeatRequest,
        _ctx: &mut Context,
        _acc: &mut HeartbeatAccumulator,
    ) -> Result<HandleControl> {
        if let Some(FlowStat { flow_stat_size }) = &req.flow_stat {
            let state_size = flow_stat_size
                .iter()
                .map(|(k, v)| (*k, *v as usize))
                .collect();
            let value = FlowStateValue::new(state_size);
            self.flow_state_manager
                .put(value)
                .await
                .context(FlowStateHandlerSnafu)?;
        }
        Ok(HandleControl::Continue)
    }
}
