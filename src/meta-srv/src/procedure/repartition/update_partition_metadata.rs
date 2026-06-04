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

use common_meta::lock_key::TableLock;
use common_procedure::{Context as ProcedureContext, Status};
use serde::{Deserialize, Serialize};
use snafu::ensure;

use crate::error::{self, Result};
use crate::procedure::repartition::allocate_region::AllocateRegion;
use crate::procedure::repartition::plan::AllocationPlanEntry;
use crate::procedure::repartition::{Context, State};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionMetadataUpdate {
    pub partition_key_indices: Vec<usize>,
}

impl PartitionMetadataUpdate {
    pub fn new(partition_key_indices: Vec<usize>) -> Self {
        Self {
            partition_key_indices,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdatePartitionMetadata {
    plan_entries: Vec<AllocationPlanEntry>,
}

impl UpdatePartitionMetadata {
    pub fn new(plan_entries: Vec<AllocationPlanEntry>) -> Self {
        Self { plan_entries }
    }
}

#[async_trait::async_trait]
#[typetag::serde]
impl State for UpdatePartitionMetadata {
    async fn next(
        &mut self,
        ctx: &mut Context,
        procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        let Some(update) = ctx.persistent_ctx.partition_metadata_update.as_ref() else {
            return Ok((
                Box::new(AllocateRegion::new(self.plan_entries.clone())),
                Status::executing(false),
            ));
        };
        let partition_key_indices = update.partition_key_indices.clone();
        ensure!(
            !partition_key_indices.is_empty(),
            error::InvalidArgumentsSnafu {
                err_msg:
                    "Repartition partition metadata update expects non-empty partition key indices"
                        .to_string(),
            }
        );

        let table_id = ctx.persistent_ctx.table_id;
        let table_lock = TableLock::Write(table_id).into();
        let _guard = procedure_ctx.provider.acquire_lock(&table_lock).await;
        let table_info_value = ctx.get_raw_table_info_value().await?;
        let current_partition_key_indices = &table_info_value.table_info.meta.partition_key_indices;
        if current_partition_key_indices == &partition_key_indices {
            return Ok((
                Box::new(AllocateRegion::new(self.plan_entries.clone())),
                Status::executing(true),
            ));
        }
        ensure!(
            current_partition_key_indices.is_empty(),
            error::InvalidArgumentsSnafu {
                err_msg: format!(
                    "Repartition partition metadata update expects an unpartitioned table, but table {} has partition key indices: {:?}",
                    table_id, current_partition_key_indices
                ),
            }
        );

        let mut new_table_info = table_info_value.table_info.clone();
        new_table_info.meta.partition_key_indices = partition_key_indices;
        common_telemetry::info!(
            "Update table partition metadata, table_id: {}, partition_key_indices: {:?}, partition_columns: {:?}",
            table_id,
            new_table_info.meta.partition_key_indices,
            new_table_info
                .meta
                .partition_column_names()
                .collect::<Vec<_>>(),
        );
        ctx.update_table_info(&table_info_value, table_info_value.update(new_table_info))
            .await?;
        ctx.invalidate_table_cache().await?;

        Ok((
            Box::new(AllocateRegion::new(self.plan_entries.clone())),
            Status::executing(true),
        ))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_meta::ddl::test_util::datanode_handler::NaiveDatanodeHandler;
    use common_meta::test_util::MockDatanodeManager;
    use store_api::storage::{RegionId, TableId};

    use super::*;
    use crate::procedure::repartition::test_util::{
        TestingEnv, new_parent_context, range_expr, test_region_route, test_region_wal_options,
    };

    async fn new_test_context(env: &TestingEnv, table_id: TableId) -> Context {
        env.create_physical_table_metadata_for_repartition(
            table_id,
            vec![test_region_route(RegionId::new(table_id, 1), "")],
            test_region_wal_options(&[1]),
        )
        .await;
        let node_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
        let mut ctx = new_parent_context(env, node_manager, table_id);
        ctx.persistent_ctx.partition_metadata_update = Some(PartitionMetadataUpdate::new(vec![0]));
        ctx
    }

    async fn set_partition_key_indices(ctx: &Context, partition_key_indices: Vec<usize>) {
        let current = ctx.get_raw_table_info_value().await.unwrap();
        let mut table_info = current.table_info.clone();
        table_info.meta.partition_key_indices = partition_key_indices;
        ctx.update_table_info(&current, current.update(table_info))
            .await
            .unwrap();
    }

    async fn partition_key_indices(ctx: &Context) -> Vec<usize> {
        ctx.get_table_info_value()
            .await
            .unwrap()
            .table_info
            .meta
            .partition_key_indices
    }

    #[tokio::test]
    async fn test_update_partition_metadata_applies_to_unpartitioned_table() {
        let env = TestingEnv::new();
        let table_id = 1024;
        let mut ctx = new_test_context(&env, table_id).await;
        let mut state = UpdatePartitionMetadata::new(vec![]);

        let (next, status) = state
            .next(&mut ctx, &TestingEnv::procedure_context())
            .await
            .unwrap();

        assert!(status.need_persist());
        assert!(next.as_any().is::<AllocateRegion>());
        assert_eq!(partition_key_indices(&ctx).await, vec![0]);
    }

    #[tokio::test]
    async fn test_update_partition_metadata_replay_is_noop() {
        let env = TestingEnv::new();
        let table_id = 1024;
        let mut ctx = new_test_context(&env, table_id).await;
        set_partition_key_indices(&ctx, vec![0]).await;
        let mut state = UpdatePartitionMetadata::new(vec![]);

        let (next, status) = state
            .next(&mut ctx, &TestingEnv::procedure_context())
            .await
            .unwrap();

        assert!(status.need_persist());
        assert!(next.as_any().is::<AllocateRegion>());
        assert_eq!(partition_key_indices(&ctx).await, vec![0]);
    }

    #[tokio::test]
    async fn test_update_partition_metadata_rejects_empty_partition_key_indices() {
        let env = TestingEnv::new();
        let table_id = 1024;
        let mut ctx = new_test_context(&env, table_id).await;
        ctx.persistent_ctx.partition_metadata_update = Some(PartitionMetadataUpdate::new(vec![]));
        let mut state = UpdatePartitionMetadata::new(vec![]);

        let err = state
            .next(&mut ctx, &TestingEnv::procedure_context())
            .await
            .unwrap_err();

        assert!(err.to_string().contains("non-empty partition key indices"));
        assert!(partition_key_indices(&ctx).await.is_empty());
    }

    #[tokio::test]
    async fn test_update_partition_metadata_rejects_other_partition_keys() {
        let env = TestingEnv::new();
        let table_id = 1024;
        let mut ctx = new_test_context(&env, table_id).await;
        set_partition_key_indices(&ctx, vec![1]).await;
        let mut state = UpdatePartitionMetadata::new(vec![]);

        let err = state
            .next(&mut ctx, &TestingEnv::procedure_context())
            .await
            .unwrap_err();

        assert!(err.to_string().contains("expects an unpartitioned table"));
        assert_eq!(partition_key_indices(&ctx).await, vec![1]);
    }

    #[tokio::test]
    async fn test_update_partition_metadata_preserves_plan_entries() {
        let env = TestingEnv::new();
        let table_id = 1024;
        let mut ctx = new_test_context(&env, table_id).await;
        let plan_entries = vec![crate::procedure::repartition::plan::AllocationPlanEntry {
            group_id: uuid::Uuid::new_v4(),
            source_regions: vec![
                crate::procedure::repartition::plan::SourceRegionDescriptor::Default {
                    region_id: RegionId::new(table_id, 1),
                },
            ],
            target_partition_exprs: vec![range_expr("x", 0, 10)],
            transition_map: vec![vec![0]],
        }];
        let mut state = UpdatePartitionMetadata::new(plan_entries);

        let (next, _) = state
            .next(&mut ctx, &TestingEnv::procedure_context())
            .await
            .unwrap();

        assert!(next.as_any().is::<AllocateRegion>());
    }
}
