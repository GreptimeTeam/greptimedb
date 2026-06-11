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

use common_meta::key::table_route::PhysicalTableRouteValue;
use common_procedure::{Context as ProcedureContext, Status};
use common_telemetry::debug;
use partition::collider::Collider;
use partition::expr::PartitionExpr;
use partition::subtask::{self, RepartitionSubtask};
use serde::{Deserialize, Deserializer, Serialize};
use snafu::{OptionExt, ResultExt, ensure};
use tokio::time::Instant;
use uuid::Uuid;

use crate::error::{self, Result};
use crate::procedure::repartition::allocate_region::AllocateRegion;
use crate::procedure::repartition::plan::{AllocationPlanEntry, SourceRegionDescriptor};
use crate::procedure::repartition::repartition_end::RepartitionEnd;
use crate::procedure::repartition::update_partition_metadata::{
    PartitionMetadataUpdate, UpdatePartitionMetadata,
};
use crate::procedure::repartition::{Context, State};

#[derive(Debug, Clone, Serialize)]
pub enum RepartitionFrom {
    Partitioned {
        exprs: Vec<PartitionExpr>,
        /// Full target partition columns to overwrite table metadata.
        ///
        /// `None` means the repartition keeps using the current table
        /// partition columns, so the procedure won't update
        /// `partition_key_indices`.
        target_partition_columns: Option<Vec<String>>,
    },
    Unpartitioned {
        partition_columns: Vec<String>,
    },
}

impl<'de> Deserialize<'de> for RepartitionFrom {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        enum CurrentRepartitionFrom {
            Partitioned {
                exprs: Vec<PartitionExpr>,
                #[serde(default)]
                target_partition_columns: Option<Vec<String>>,
            },
            Unpartitioned {
                partition_columns: Vec<String>,
            },
        }

        #[derive(Deserialize)]
        #[serde(untagged)]
        enum RepartitionFromRepr {
            Current(CurrentRepartitionFrom),
            Legacy(Vec<PartitionExpr>),
        }

        match RepartitionFromRepr::deserialize(deserializer)? {
            RepartitionFromRepr::Current(CurrentRepartitionFrom::Partitioned {
                exprs,
                target_partition_columns,
            }) => Ok(Self::Partitioned {
                exprs,
                target_partition_columns,
            }),
            RepartitionFromRepr::Current(CurrentRepartitionFrom::Unpartitioned {
                partition_columns,
            }) => Ok(Self::Unpartitioned { partition_columns }),
            RepartitionFromRepr::Legacy(exprs) => Ok(Self::Partitioned {
                exprs,
                target_partition_columns: None,
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepartitionStart {
    #[serde(alias = "from_exprs")]
    from: RepartitionFrom,
    to_exprs: Vec<PartitionExpr>,
}

impl RepartitionStart {
    pub fn new(from: RepartitionFrom, to_exprs: Vec<PartitionExpr>) -> Self {
        Self { from, to_exprs }
    }
}

#[async_trait::async_trait]
#[typetag::serde]
impl State for RepartitionStart {
    async fn next(
        &mut self,
        ctx: &mut Context,
        _: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        ensure!(
            !self.to_exprs.is_empty(),
            error::InvalidArgumentsSnafu {
                err_msg: "Repartition expects non-empty target partition expressions".to_string(),
            }
        );

        let timer = Instant::now();
        let (physical_table_id, table_route) = ctx
            .table_metadata_manager
            .table_route_manager()
            .get_physical_table_route(ctx.persistent_ctx.table_id)
            .await
            .context(error::TableMetadataManagerSnafu)?;
        let table_id = ctx.persistent_ctx.table_id;
        ensure!(
            physical_table_id == table_id,
            error::UnexpectedSnafu {
                violated: format!(
                    "Repartition only works on the physical table, but got logical table: {}, physical table id: {}",
                    table_id, physical_table_id
                ),
            }
        );

        let from_exprs = self.prepare_from(ctx).await?;
        let plans = Self::build_plan(&table_route, from_exprs, &self.to_exprs)?;
        let plan_count = plans.len();
        let total_source_regions: usize = plans.iter().map(|p| p.source_regions.len()).sum();
        let total_target_regions: usize =
            plans.iter().map(|p| p.target_partition_exprs.len()).sum();
        common_telemetry::info!(
            "Repartition start, table_id: {}, plans: {}, total_source_regions: {}, total_target_regions: {}",
            table_id,
            plan_count,
            total_source_regions,
            total_target_regions
        );

        ctx.update_build_plan_elapsed(timer.elapsed());

        if plans.is_empty() {
            return Ok((Box::new(RepartitionEnd), Status::done()));
        }

        if ctx.persistent_ctx.partition_metadata_update.is_some() {
            Ok((
                Box::new(UpdatePartitionMetadata::new(plans)),
                Status::executing(true),
            ))
        } else {
            Ok((
                Box::new(AllocateRegion::new(plans)),
                Status::executing(false),
            ))
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl RepartitionStart {
    async fn prepare_from<'a>(&'a self, ctx: &mut Context) -> Result<&'a [PartitionExpr]> {
        match &self.from {
            RepartitionFrom::Partitioned {
                exprs,
                target_partition_columns,
            } => {
                Self::prepare_partitioned(ctx, target_partition_columns.as_deref()).await?;
                Ok(exprs)
            }
            RepartitionFrom::Unpartitioned { partition_columns } => {
                Self::prepare_unpartitioned(ctx, partition_columns).await?;
                Ok(&[])
            }
        }
    }

    async fn prepare_unpartitioned(ctx: &mut Context, partition_columns: &[String]) -> Result<()> {
        if ctx.persistent_ctx.partition_metadata_update.is_some() {
            return Ok(());
        }

        ensure!(
            !partition_columns.is_empty(),
            error::InvalidArgumentsSnafu {
                err_msg: "Unpartitioned repartition expects non-empty partition columns"
                    .to_string(),
            }
        );

        let table_info_value = ctx.get_table_info_value().await?;
        ensure!(
            table_info_value
                .table_info
                .meta
                .partition_key_indices
                .is_empty(),
            error::InvalidArgumentsSnafu {
                err_msg: format!(
                    "Unpartitioned repartition expects an unpartitioned table, but table {} has partition key indices: {:?}",
                    ctx.persistent_ctx.table_id,
                    table_info_value.table_info.meta.partition_key_indices
                ),
            }
        );

        let schema = &table_info_value.table_info.meta.schema;
        let partition_key_indices = partition_columns
            .iter()
            .map(|column_name| {
                schema.column_index_by_name(column_name).with_context(|| {
                    error::InvalidArgumentsSnafu {
                        err_msg: format!(
                            "Partition column {} not found in table {}",
                            column_name, ctx.persistent_ctx.table_id
                        ),
                    }
                })
            })
            .collect::<Result<Vec<_>>>()?;
        ctx.persistent_ctx.partition_metadata_update = Some(
            PartitionMetadataUpdate::from_unpartitioned(partition_key_indices),
        );

        Ok(())
    }

    async fn prepare_partitioned(
        ctx: &mut Context,
        target_partition_columns: Option<&[String]>,
    ) -> Result<()> {
        let Some(target_partition_columns) = target_partition_columns else {
            return Ok(());
        };
        if ctx.persistent_ctx.partition_metadata_update.is_some() {
            return Ok(());
        }

        ensure!(
            !target_partition_columns.is_empty(),
            error::InvalidArgumentsSnafu {
                err_msg: "Partitioned source expects non-empty target partition columns"
                    .to_string(),
            }
        );

        let table_info_value = ctx.get_table_info_value().await?;
        let schema = &table_info_value.table_info.meta.schema;
        let target_partition_key_indices = target_partition_columns
            .iter()
            .map(|column_name| {
                schema.column_index_by_name(column_name).with_context(|| {
                    error::InvalidArgumentsSnafu {
                        err_msg: format!(
                            "Target partition column {} not found in table {}",
                            column_name, ctx.persistent_ctx.table_id
                        ),
                    }
                })
            })
            .collect::<Result<Vec<_>>>()?;
        ctx.persistent_ctx.partition_metadata_update =
            Some(PartitionMetadataUpdate::from_partitioned(
                table_info_value.table_info.meta.partition_key_indices,
                target_partition_key_indices,
            ));

        Ok(())
    }

    pub(crate) fn build_plan(
        physical_route: &PhysicalTableRouteValue,
        from_exprs: &[PartitionExpr],
        to_exprs: &[PartitionExpr],
    ) -> Result<Vec<AllocationPlanEntry>> {
        let subtasks = if from_exprs.is_empty() {
            Self::default_source_subtasks(to_exprs)?
        } else {
            subtask::create_subtasks(from_exprs, to_exprs)
                .context(error::RepartitionCreateSubtasksSnafu)?
        };
        if subtasks.is_empty() {
            return Ok(vec![]);
        }

        let src_descriptors = Self::source_region_descriptors(from_exprs, physical_route)?;
        Ok(Self::build_plan_entries(
            subtasks,
            &src_descriptors,
            to_exprs,
        ))
    }

    fn build_plan_entries(
        subtasks: Vec<RepartitionSubtask>,
        source_index: &[SourceRegionDescriptor],
        target_exprs: &[PartitionExpr],
    ) -> Vec<AllocationPlanEntry> {
        subtasks
            .into_iter()
            .map(|subtask| {
                let group_id = Uuid::new_v4();
                let source_regions = subtask
                    .from_expr_indices
                    .iter()
                    .map(|&idx| source_index[idx].clone())
                    .collect::<Vec<_>>();

                let target_partition_exprs = subtask
                    .to_expr_indices
                    .iter()
                    .map(|&idx| target_exprs[idx].clone())
                    .collect::<Vec<_>>();
                AllocationPlanEntry {
                    group_id,
                    source_regions,
                    target_partition_exprs,
                    transition_map: subtask.transition_map,
                }
            })
            .collect::<Vec<_>>()
    }

    fn default_source_subtasks(to_exprs: &[PartitionExpr]) -> Result<Vec<RepartitionSubtask>> {
        ensure!(
            !to_exprs.is_empty(),
            error::UnexpectedSnafu {
                violated: "Default source repartition expects non-empty target partition exprs",
            }
        );

        Collider::new(to_exprs).context(error::RepartitionCreateSubtasksSnafu)?;

        let to_expr_indices = (0..to_exprs.len()).collect::<Vec<_>>();
        Ok(vec![RepartitionSubtask {
            from_expr_indices: vec![0],
            to_expr_indices: to_expr_indices.clone(),
            transition_map: vec![to_expr_indices],
        }])
    }

    fn source_region_descriptors(
        from_exprs: &[PartitionExpr],
        physical_route: &PhysicalTableRouteValue,
    ) -> Result<Vec<SourceRegionDescriptor>> {
        if from_exprs.is_empty() {
            return Self::default_source_region_descriptors(physical_route);
        }

        let existing_regions = physical_route
            .region_routes
            .iter()
            .map(|route| (route.region.id, route.region.partition_expr()))
            .collect::<Vec<_>>();

        let descriptors = from_exprs
            .iter()
            .map(|expr| {
                let expr_json = expr
                    .as_json_str()
                    .context(error::SerializePartitionExprSnafu)?;

                let matched_region_id = existing_regions
                    .iter()
                    .find_map(|(region_id, existing_expr)| {
                        (existing_expr == &expr_json).then_some(*region_id)
                    })
                    .with_context(|| error::RepartitionSourceExprMismatchSnafu { expr: &expr_json })
                    .inspect_err(|_| {
                        debug!("Failed to find matching region for partition expression: {}, existing regions: {:?}", expr_json, existing_regions);
                    })?;

                Ok(SourceRegionDescriptor::partitioned(
                    matched_region_id,
                    expr.clone(),
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(descriptors)
    }

    fn default_source_region_descriptors(
        physical_route: &PhysicalTableRouteValue,
    ) -> Result<Vec<SourceRegionDescriptor>> {
        ensure!(
            physical_route.region_routes.len() == 1,
            error::UnexpectedSnafu {
                violated: format!(
                    "Default source repartition expects exactly one source region, but got {}",
                    physical_route.region_routes.len()
                ),
            }
        );
        let source_region = &physical_route.region_routes[0].region;
        ensure!(
            source_region.partition_expr().is_empty(),
            error::UnexpectedSnafu {
                violated: format!(
                    "Default source repartition expects an empty partition expr, but got {}",
                    source_region.partition_expr()
                ),
            }
        );

        Ok(vec![SourceRegionDescriptor::Default {
            region_id: source_region.id,
        }])
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_meta::ddl::test_util::datanode_handler::NaiveDatanodeHandler;
    use common_meta::key::table_route::PhysicalTableRouteValue;
    use common_meta::peer::Peer;
    use common_meta::rpc::router::{Region, RegionRoute};
    use common_meta::test_util::MockDatanodeManager;
    use datatypes::prelude::Value;
    use partition::expr::{Operand, RestrictedOp};
    use store_api::storage::RegionId;

    use super::*;
    use crate::procedure::repartition::test_util::{
        TestingEnv, new_parent_context, range_expr, test_region_route, test_region_wal_options,
    };

    fn physical_route(region_routes: Vec<RegionRoute>) -> PhysicalTableRouteValue {
        PhysicalTableRouteValue::new(region_routes)
    }

    async fn new_test_context(env: &TestingEnv, table_id: u32) -> Context {
        env.create_physical_table_metadata_for_repartition(
            table_id,
            vec![test_region_route(RegionId::new(table_id, 1), "")],
            test_region_wal_options(&[1]),
        )
        .await;
        let node_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
        new_parent_context(env, node_manager, table_id)
    }

    #[test]
    fn test_build_plan_with_default_source_region() {
        let table_id = 1024;
        let physical_route =
            physical_route(vec![test_region_route(RegionId::new(table_id, 1), "")]);
        let to_exprs = vec![range_expr("x", 0, 50), range_expr("x", 50, 100)];

        let plans = RepartitionStart::build_plan(&physical_route, &[], &to_exprs).unwrap();

        assert_eq!(plans.len(), 1);
        let plan = &plans[0];
        assert_eq!(
            plan.source_regions,
            vec![SourceRegionDescriptor::Default {
                region_id: RegionId::new(table_id, 1)
            }]
        );
        assert_eq!(plan.target_partition_exprs, to_exprs);
        assert_eq!(plan.transition_map, vec![vec![0, 1]]);
    }

    #[test]
    fn test_build_plan_with_default_source_rejects_non_empty_partition_expr() {
        let table_id = 1024;
        let physical_route = physical_route(vec![test_region_route(
            RegionId::new(table_id, 1),
            &range_expr("x", 0, 100).as_json_str().unwrap(),
        )]);
        let to_exprs = vec![range_expr("x", 0, 50), range_expr("x", 50, 100)];

        let err = RepartitionStart::build_plan(&physical_route, &[], &to_exprs).unwrap_err();

        assert!(err.to_string().contains("empty partition expr"));
    }

    #[test]
    fn test_build_plan_with_default_source_rejects_multiple_regions() {
        let table_id = 1024;
        let physical_route = physical_route(vec![
            test_region_route(RegionId::new(table_id, 1), ""),
            test_region_route(RegionId::new(table_id, 2), ""),
        ]);
        let to_exprs = vec![range_expr("x", 0, 50), range_expr("x", 50, 100)];

        let err = RepartitionStart::build_plan(&physical_route, &[], &to_exprs).unwrap_err();

        assert!(err.to_string().contains("exactly one source region"));
    }

    #[test]
    fn test_build_plan_with_default_source_rejects_empty_targets() {
        let table_id = 1024;
        let physical_route =
            physical_route(vec![test_region_route(RegionId::new(table_id, 1), "")]);

        let err = RepartitionStart::build_plan(&physical_route, &[], &[]).unwrap_err();

        assert!(err.to_string().contains("non-empty target partition exprs"));
    }

    #[test]
    fn test_build_plan_with_default_source_rejects_invalid_targets() {
        let table_id = 1024;
        let physical_route =
            physical_route(vec![test_region_route(RegionId::new(table_id, 1), "")]);
        let invalid_to_expr = PartitionExpr::new(
            Operand::Value(Value::Int64(1)),
            RestrictedOp::Eq,
            Operand::Value(Value::Int64(2)),
        );

        let err =
            RepartitionStart::build_plan(&physical_route, &[], &[invalid_to_expr]).unwrap_err();

        assert!(
            err.to_string()
                .contains("Failed to create repartition subtasks")
        );
    }

    #[test]
    fn test_build_plan_keeps_partitioned_source_matching() {
        let table_id = 1024;
        let from_exprs = vec![range_expr("x", 0, 100)];
        let to_exprs = vec![range_expr("x", 0, 50), range_expr("x", 50, 100)];
        let physical_route = physical_route(vec![RegionRoute {
            region: Region {
                id: RegionId::new(table_id, 1),
                partition_expr: from_exprs[0].as_json_str().unwrap(),
                ..Default::default()
            },
            leader_peer: Some(Peer::empty(1)),
            ..Default::default()
        }]);

        let plans = RepartitionStart::build_plan(&physical_route, &from_exprs, &to_exprs).unwrap();

        assert_eq!(plans.len(), 1);
        assert_eq!(
            plans[0].source_regions,
            vec![SourceRegionDescriptor::partitioned(
                RegionId::new(table_id, 1),
                from_exprs[0].clone()
            )]
        );
    }

    #[test]
    fn test_repartition_start_deserializes_legacy_from_exprs() {
        let from_exprs = vec![range_expr("x", 0, 100)];
        let to_exprs = vec![range_expr("x", 0, 50), range_expr("x", 50, 100)];
        let json = serde_json::json!({
            "from_exprs": from_exprs,
            "to_exprs": to_exprs,
        })
        .to_string();

        let state: RepartitionStart = serde_json::from_str(&json).unwrap();

        let RepartitionFrom::Partitioned {
            exprs,
            target_partition_columns,
        } = state.from
        else {
            panic!("expected partition source");
        };
        assert_eq!(exprs, vec![range_expr("x", 0, 100)]);
        assert!(target_partition_columns.is_none());
    }

    #[test]
    fn test_repartition_start_deserializes_current_from() {
        let state = RepartitionStart::new(
            RepartitionFrom::Unpartitioned {
                partition_columns: vec!["col1".to_string()],
            },
            vec![range_expr("col1", 0, 50)],
        );
        let json = serde_json::to_string(&state).unwrap();

        let state: RepartitionStart = serde_json::from_str(&json).unwrap();

        let RepartitionFrom::Unpartitioned { partition_columns } = state.from else {
            panic!("expected unpartitioned source");
        };
        assert_eq!(partition_columns, vec!["col1"]);
    }

    #[tokio::test]
    async fn test_partitioned_source_does_not_initialize_partition_metadata_update() {
        let env = TestingEnv::new();
        let table_id = 1024;
        env.create_physical_table_metadata_for_repartition(
            table_id,
            vec![test_region_route(
                RegionId::new(table_id, 1),
                &range_expr("x", 0, 100).as_json_str().unwrap(),
            )],
            test_region_wal_options(&[1]),
        )
        .await;
        let node_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
        let mut ctx = new_parent_context(&env, node_manager, table_id);
        let mut state = RepartitionStart::new(
            RepartitionFrom::Partitioned {
                exprs: vec![range_expr("x", 0, 100)],
                target_partition_columns: None,
            },
            vec![range_expr("x", 0, 50), range_expr("x", 50, 100)],
        );

        let (next, status) = state
            .next(&mut ctx, &TestingEnv::procedure_context())
            .await
            .unwrap();

        assert!(!status.need_persist());
        assert!(next.as_any().is::<AllocateRegion>());
        assert!(ctx.persistent_ctx.partition_metadata_update.is_none());
    }

    #[tokio::test]
    async fn test_partitioned_source_initializes_target_partition_metadata_update() {
        let env = TestingEnv::new();
        let table_id = 1024;
        env.create_physical_table_metadata_for_repartition(
            table_id,
            vec![test_region_route(
                RegionId::new(table_id, 1),
                &range_expr("x", 0, 100).as_json_str().unwrap(),
            )],
            test_region_wal_options(&[1]),
        )
        .await;
        let node_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
        let mut ctx = new_parent_context(&env, node_manager, table_id);
        let current = ctx.get_raw_table_info_value().await.unwrap();
        let mut table_info = current.table_info.clone();
        table_info.meta.partition_key_indices = vec![0];
        ctx.update_table_info(&current, current.update(table_info))
            .await
            .unwrap();
        let mut state = RepartitionStart::new(
            RepartitionFrom::Partitioned {
                exprs: vec![range_expr("x", 0, 100)],
                target_partition_columns: Some(vec!["col2".to_string(), "col1".to_string()]),
            },
            vec![range_expr("x", 0, 50), range_expr("x", 50, 100)],
        );

        let (next, status) = state
            .next(&mut ctx, &TestingEnv::procedure_context())
            .await
            .unwrap();

        assert!(status.need_persist());
        assert!(next.as_any().is::<UpdatePartitionMetadata>());
        let update = ctx
            .persistent_ctx
            .partition_metadata_update
            .as_ref()
            .unwrap();
        assert_eq!(update.original_partition_key_indices, vec![0]);
        assert_eq!(update.target_partition_key_indices, vec![2, 0]);
        assert!(!update.expect_empty_partition_key_indices);
    }

    #[tokio::test]
    async fn test_unpartitioned_source_initializes_partition_metadata_update() {
        let env = TestingEnv::new();
        let table_id = 1024;
        let mut ctx = new_test_context(&env, table_id).await;
        let mut state = RepartitionStart::new(
            RepartitionFrom::Unpartitioned {
                partition_columns: vec!["col2".to_string(), "col1".to_string()],
            },
            vec![range_expr("col2", 0, 50), range_expr("col2", 50, 100)],
        );

        let (next, status) = state
            .next(&mut ctx, &TestingEnv::procedure_context())
            .await
            .unwrap();

        assert!(status.need_persist());
        assert!(next.as_any().is::<UpdatePartitionMetadata>());
        assert_eq!(
            ctx.persistent_ctx
                .partition_metadata_update
                .as_ref()
                .unwrap()
                .target_partition_key_indices,
            vec![2, 0]
        );
    }

    #[tokio::test]
    async fn test_unpartitioned_source_rejects_existing_partition_metadata() {
        let env = TestingEnv::new();
        let table_id = 1024;
        let mut ctx = new_test_context(&env, table_id).await;
        let current = ctx.get_raw_table_info_value().await.unwrap();
        let mut table_info = current.table_info.clone();
        table_info.meta.partition_key_indices = vec![0];
        ctx.update_table_info(&current, current.update(table_info))
            .await
            .unwrap();
        let mut state = RepartitionStart::new(
            RepartitionFrom::Unpartitioned {
                partition_columns: vec!["col1".to_string()],
            },
            vec![range_expr("col1", 0, 50)],
        );

        let err = state
            .next(&mut ctx, &TestingEnv::procedure_context())
            .await
            .unwrap_err();

        assert!(err.to_string().contains("expects an unpartitioned table"));
        assert!(ctx.persistent_ctx.partition_metadata_update.is_none());
    }

    #[tokio::test]
    async fn test_repartition_start_rejects_empty_target_partition_exprs() {
        let env = TestingEnv::new();
        let table_id = 1024;
        let mut ctx = new_test_context(&env, table_id).await;
        let mut state = RepartitionStart::new(
            RepartitionFrom::Partitioned {
                exprs: vec![],
                target_partition_columns: None,
            },
            vec![],
        );

        let err = state
            .next(&mut ctx, &TestingEnv::procedure_context())
            .await
            .unwrap_err();

        assert!(
            err.to_string()
                .contains("non-empty target partition expressions")
        );
    }

    #[tokio::test]
    async fn test_unpartitioned_source_rejects_empty_target_partition_exprs() {
        let env = TestingEnv::new();
        let table_id = 1024;
        let mut ctx = new_test_context(&env, table_id).await;
        let mut state = RepartitionStart::new(
            RepartitionFrom::Unpartitioned {
                partition_columns: vec!["col1".to_string()],
            },
            vec![],
        );

        let err = state
            .next(&mut ctx, &TestingEnv::procedure_context())
            .await
            .unwrap_err();

        assert!(
            err.to_string()
                .contains("non-empty target partition expressions")
        );
        assert!(ctx.persistent_ctx.partition_metadata_update.is_none());
    }

    #[tokio::test]
    async fn test_unpartitioned_source_rejects_empty_partition_columns() {
        let env = TestingEnv::new();
        let table_id = 1024;
        let mut ctx = new_test_context(&env, table_id).await;
        let mut state = RepartitionStart::new(
            RepartitionFrom::Unpartitioned {
                partition_columns: vec![],
            },
            vec![range_expr("col1", 0, 50)],
        );

        let err = state
            .next(&mut ctx, &TestingEnv::procedure_context())
            .await
            .unwrap_err();

        assert!(err.to_string().contains("non-empty partition columns"));
        assert!(ctx.persistent_ctx.partition_metadata_update.is_none());
    }

    #[tokio::test]
    async fn test_unpartitioned_source_rejects_missing_partition_column() {
        let env = TestingEnv::new();
        let table_id = 1024;
        let mut ctx = new_test_context(&env, table_id).await;
        let mut state = RepartitionStart::new(
            RepartitionFrom::Unpartitioned {
                partition_columns: vec!["missing_col".to_string()],
            },
            vec![range_expr("col1", 0, 50)],
        );

        let err = state
            .next(&mut ctx, &TestingEnv::procedure_context())
            .await
            .unwrap_err();

        assert!(
            err.to_string()
                .contains("Partition column missing_col not found")
        );
        assert!(ctx.persistent_ctx.partition_metadata_update.is_none());
    }
}
