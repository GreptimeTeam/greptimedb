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

pub(crate) mod downgrade_leader_region;
pub(crate) mod rollback_downgraded_region;
pub(crate) mod upgrade_candidate_region;

use std::any::Any;

use common_procedure::Status;
use common_telemetry::warn;
use serde::{Deserialize, Serialize};

use super::migration_abort::RegionMigrationAbort;
use super::migration_end::RegionMigrationEnd;
use crate::error::Result;
use crate::procedure::region_migration::downgrade_leader_region::DowngradeLeaderRegion;
use crate::procedure::region_migration::{Context, State};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "UpdateMetadata")]
pub enum UpdateMetadata {
    /// Downgrades the leader region.
    Downgrade,
    /// Upgrades the candidate region.
    Upgrade,
    /// Rolls back the downgraded region.
    Rollback,
}

#[async_trait::async_trait]
#[typetag::serde]
impl State for UpdateMetadata {
    async fn next(&mut self, ctx: &mut Context) -> Result<(Box<dyn State>, Status)> {
        match self {
            UpdateMetadata::Downgrade => {
                self.downgrade_leader_region(ctx).await?;

                Ok((
                    Box::<DowngradeLeaderRegion>::default(),
                    Status::executing(false),
                ))
            }
            UpdateMetadata::Upgrade => {
                self.upgrade_candidate_region(ctx).await?;

                if let Err(err) = ctx.invalidate_table_cache().await {
                    warn!("Failed to broadcast the invalidate table cache message during the upgrade candidate, error: {err:?}");
                };
                Ok((Box::new(RegionMigrationEnd), Status::Done))
            }
            UpdateMetadata::Rollback => {
                self.rollback_downgraded_region(ctx).await?;

                if let Err(err) = ctx.invalidate_table_cache().await {
                    warn!("Failed to broadcast the invalidate table cache message during the rollback, error: {err:?}");
                };
                Ok((
                    Box::new(RegionMigrationAbort::new(
                        "Failed to upgrade the candidate region.",
                    )),
                    Status::executing(false),
                ))
            }
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
