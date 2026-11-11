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
pub(crate) mod exit_staging_region;

use std::any::Any;
use std::time::Instant;

use common_meta::lock_key::TableLock;
use common_procedure::{Context as ProcedureContext, Status};
use common_telemetry::warn;
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::procedure::repartition::group::enter_staging_region::EnterStagingRegion;
use crate::procedure::repartition::group::repartition_end::RepartitionEnd;
use crate::procedure::repartition::group::{Context, State};

#[derive(Debug, Serialize, Deserialize)]
#[allow(clippy::enum_variant_names)]
pub enum UpdateMetadata {
    /// Applies the new partition expressions for staging regions.
    ApplyStaging,
    /// Exits the staging regions after the group finishes its forward path.
    ExitStaging,
}

#[async_trait::async_trait]
#[typetag::serde]
impl State for UpdateMetadata {
    async fn next(
        &mut self,
        ctx: &mut Context,
        procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        let timer = Instant::now();
        let table_lock = TableLock::Write(ctx.persistent_ctx.table_id).into();
        let _guard = procedure_ctx.provider.acquire_lock(&table_lock).await;
        match self {
            UpdateMetadata::ApplyStaging => {
                // TODO(weny): If all metadata have already been updated, skip applying staging regions.
                self.apply_staging_regions(ctx).await?;

                // Do not enter staging after an explicitly failed invalidation broadcast.
                ctx.invalidate_table_cache().await?;
                ctx.update_update_metadata_elapsed(timer.elapsed());
                Ok((Box::new(EnterStagingRegion), Status::executing(false)))
            }
            UpdateMetadata::ExitStaging => {
                self.exit_staging_regions(ctx).await?;
                if let Err(err) = ctx.invalidate_table_cache().await {
                    warn!(
                        err;
                        "Failed to broadcast the invalidate table cache message during the exit staging regions"
                    );
                };
                ctx.update_update_metadata_elapsed(timer.elapsed());
                Ok((Box::new(RepartitionEnd), Status::executing(false)))
            }
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use store_api::storage::RegionId;

    use super::*;
    use crate::handler::test_utils::FailingCacheInvalidator;
    use crate::procedure::repartition::group::repartition_start::RepartitionStart;
    use crate::procedure::repartition::plan::{SourceRegionDescriptor, TargetRegionDescriptor};
    use crate::procedure::repartition::test_util::{
        TestingEnv, new_persistent_context, range_expr, test_region_route,
    };

    #[tokio::test]
    async fn test_pending_read_invalidation_failure_blocks_enter_staging() {
        let env = TestingEnv::new();
        let table_id = 1024;
        let source_id = RegionId::new(table_id, 1);
        let target_id = RegionId::new(table_id, 2);
        let source_expr = range_expr("x", 0, 100);
        let mut target = test_region_route(target_id, "");
        target.set_repartition_pending_read();
        env.create_physical_table_metadata(
            table_id,
            vec![
                test_region_route(source_id, &source_expr.as_json_str().unwrap()),
                target,
            ],
        )
        .await;
        let persistent = new_persistent_context(
            table_id,
            vec![SourceRegionDescriptor::partitioned(
                source_id,
                source_expr.clone(),
            )],
            vec![TargetRegionDescriptor {
                region_id: target_id,
                partition_expr: range_expr("x", 0, 50),
            }],
        );
        let mut ctx = env.create_context(persistent);
        let procedure_ctx = TestingEnv::procedure_context();
        RepartitionStart
            .next(&mut ctx, &procedure_ctx)
            .await
            .unwrap();
        let invalidator = Arc::new(FailingCacheInvalidator::default());
        ctx.cache_invalidator = invalidator.clone();
        let mut state = UpdateMetadata::ApplyStaging;
        let error = state.next(&mut ctx, &procedure_ctx).await.unwrap_err();
        assert!(matches!(
            error,
            crate::error::Error::RetryLaterWithSource { .. }
        ));
        assert_eq!(invalidator.attempts(), 1);
        let value = ctx.get_table_route_value().await.unwrap();
        let routes = value.region_routes().unwrap();
        assert_eq!(
            routes[0].region.partition_expr,
            source_expr.as_json_str().unwrap()
        );
        assert!(!routes[0].is_repartition_pending_read());
        assert!(!routes[1].is_repartition_pending_read());
        // Retrying the same persisted phase cannot advance while notification still fails.
        assert!(state.next(&mut ctx, &procedure_ctx).await.is_err());
        assert_eq!(invalidator.attempts(), 2);
    }
}
