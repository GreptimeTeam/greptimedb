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

use common_procedure::Status;
use common_telemetry::warn;
use serde::{Deserialize, Serialize};

use crate::error::{self, Result};
use crate::procedure::region_migration::{Context, State};

#[derive(Debug, Serialize, Deserialize)]
pub struct RegionMigrationAbort {
    reason: String,
}

impl RegionMigrationAbort {
    /// Returns the [RegionMigrationAbort] with `reason`.
    pub fn new(reason: &str) -> Self {
        Self {
            reason: reason.to_string(),
        }
    }
}

#[async_trait::async_trait]
#[typetag::serde]
impl State for RegionMigrationAbort {
    async fn next(&mut self, ctx: &mut Context) -> Result<(Box<dyn State>, Status)> {
        warn!(
            "Region migration is aborted: {}, region_id: {}, from_peer: {}, to_peer: {}, elapsed: {:?}, downgrade_leader_region_elapsed: {:?}, open_candidate_region_elapsed: {:?}, upgrade_candidate_region_elapsed: {:?}",
            self.reason,
            ctx.region_id(),
            ctx.persistent_ctx.from_peer,
            ctx.persistent_ctx.to_peer,
            ctx.volatile_ctx.operations_elapsed,
            ctx.volatile_ctx.downgrade_leader_region_elapsed,
            ctx.volatile_ctx.open_candidate_region_elapsed,
            ctx.volatile_ctx.upgrade_candidate_region_elapsed,
        );
        error::MigrationAbortSnafu {
            reason: &self.reason,
        }
        .fail()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
