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

use std::any::Any;

use common_procedure::{Context as ProcedureContext, Status};
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::procedure::drain_datanode::{Context, State};

/// Finishes the procedure with the drain report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DrainDatanodeEnd;

#[async_trait::async_trait]
#[typetag::serde]
impl State for DrainDatanodeEnd {
    async fn next(
        &mut self,
        ctx: &mut Context,
        _procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        let persistent_ctx = &ctx.persistent_ctx;
        let output = serde_json::json!({
            "node_id": persistent_ctx.node_id,
            "migrated": persistent_ctx.migrated,
            "failed_regions": persistent_ctx.failed_regions,
            "follower_region_count": persistent_ctx.follower_region_count,
            // `false` means the datanode still leads the regions in `failed_regions`.
            "drained": persistent_ctx.failed_regions.is_empty(),
        });

        Ok((Box::new(DrainDatanodeEnd), Status::done_with_output(output)))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
