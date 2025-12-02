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

pub(crate) mod apply_staging_region;
pub(crate) mod rollback_staging_region;

use std::any::Any;

use common_procedure::{Context as ProcedureContext, Status};
use common_telemetry::warn;
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::procedure::repartition::group::repartition_start::RepartitionStart;
use crate::procedure::repartition::group::{Context, State};

#[derive(Debug, Serialize, Deserialize)]
pub enum UpdateMetadata {
    /// Applies the new partition expressions for staging regions.
    ApplyStaging,
    /// Rolls back the new partition expressions for staging regions.
    RollbackStaging,
}

impl UpdateMetadata {
    #[allow(dead_code)]
    fn next_state() -> (Box<dyn State>, Status) {
        // TODO(weny): change it later.
        (Box::new(RepartitionStart), Status::executing(true))
    }
}

#[async_trait::async_trait]
#[typetag::serde]
impl State for UpdateMetadata {
    async fn next(
        &mut self,
        ctx: &mut Context,
        _procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        match self {
            UpdateMetadata::ApplyStaging => {
                // TODO(weny): If all metadata have already been updated, skip applying staging regions.
                self.apply_staging_regions(ctx).await?;

                if let Err(err) = ctx.invalidate_table_cache().await {
                    warn!(
                        "Failed to broadcast the invalidate table cache message during the apply staging regions, error: {err:?}"
                    );
                };
                Ok(Self::next_state())
            }
            UpdateMetadata::RollbackStaging => {
                self.rollback_staging_regions(ctx).await?;

                if let Err(err) = ctx.invalidate_table_cache().await {
                    warn!(
                        "Failed to broadcast the invalidate table cache message during the rollback staging regions, error: {err:?}"
                    );
                };
                Ok(Self::next_state())
            }
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
