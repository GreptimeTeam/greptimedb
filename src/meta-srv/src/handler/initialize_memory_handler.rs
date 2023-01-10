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

use api::v1::meta::HeartbeatRequest;

use crate::error::Result;
use crate::handler::{HeartbeatAccumulator, HeartbeatHandler};
use crate::metasrv::Context;
use crate::service::store::memory::MemStore;

#[derive(Default)]
pub struct InitializeMemoryHandler {
    in_memory: Arc<MemStore>,
}

impl InitializeMemoryHandler {
    pub fn new(in_memory: Arc<MemStore>) -> Self {
        Self { in_memory }
    }
}

#[async_trait::async_trait]
impl HeartbeatHandler for InitializeMemoryHandler {
    async fn handle(
        &self,
        _req: &HeartbeatRequest,
        ctx: &Context,
        _acc: &mut HeartbeatAccumulator,
    ) -> Result<()> {
        if let Some(election) = &ctx.election {
            if election.in_infancy() {
                self.in_memory.clear();
            }
        }
        Ok(())
    }
}
