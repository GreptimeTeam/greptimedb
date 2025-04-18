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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Display;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use common_meta::key::table_info::TableInfoValue;
use common_meta::key::table_route::TableRouteValue;
use common_meta::peer::Peer;
use common_meta::rpc::router::RegionRoute;
use common_procedure::{watcher, ProcedureId, ProcedureManagerRef, ProcedureWithId};
use common_telemetry::{error, info};
use snafu::{ensure, OptionExt, ResultExt};
use store_api::storage::RegionId;
use table::table_name::TableName;

use crate::error::{self, Result};
use crate::metrics::{METRIC_META_REGION_MIGRATION_DATANODES, METRIC_META_REGION_MIGRATION_FAIL};
use crate::procedure::region_migration::{
    DefaultContextFactory, PersistentContext, RegionMigrationProcedure,
};

pub type RegionMigrationManagerRef = Arc<RegionMigrationManager>;

/// Manager of region migration procedure.
pub struct RegionMigrationManager {
    procedure_manager: ProcedureManagerRef,
    context_factory: DefaultContextFactory,
    tracker: RegionMigrationProcedureTracker,
}

#[derive(Default, Clone)]
pub struct RegionMigrationProcedureTracker {
    running_procedures: Arc<RwLock<HashMap<RegionId, RegionMigrationProcedureTask>>>,
}

impl RegionMigrationProcedureTracker {
    /// Returns the [RegionMigrationProcedureGuard] if current region isn't migrating.
    pub(crate) fn insert_running_procedure(
        &self,
        task: &RegionMigrationProcedureTask,
    ) -> Option<RegionMigrationProcedureGuard> {
        let mut procedures = self.running_procedures.write().unwrap();
        match procedures.entry(task.region_id) {
            Entry::Occupied(_) => None,
            Entry::Vacant(v) => {
                v.insert(task.clone());
                Some(RegionMigrationProcedureGuard {
                    region_id: task.region_id,
                    running_procedures: self.running_procedures.clone(),
                })
            }
        }
    }

    /// Returns true if it contains the specific region(`region_id`).
    pub(crate) fn contains(&self, region_id: RegionId) -> bool {
        self.running_procedures
            .read()
            .unwrap()
            .contains_key(&region_id)
    }
}

/// The guard of running [RegionMigrationProcedureTask].
pub(crate) struct RegionMigrationProcedureGuard {
    region_id: RegionId,
    running_procedures: Arc<RwLock<HashMap<RegionId, RegionMigrationProcedureTask>>>,
}

impl Drop for RegionMigrationProcedureGuard {
    fn drop(&mut self) {
        let exists = self
            .running_procedures
            .read()
            .unwrap()
            .contains_key(&self.region_id);
        if exists {
            self.running_procedures
                .write()
                .unwrap()
                .remove(&self.region_id);
        }
    }
}

#[derive(Debug, Clone)]
pub struct RegionMigrationProcedureTask {
    pub(crate) region_id: RegionId,
    pub(crate) from_peer: Peer,
    pub(crate) to_peer: Peer,
    pub(crate) timeout: Duration,
}

impl RegionMigrationProcedureTask {
    pub fn new(region_id: RegionId, from_peer: Peer, to_peer: Peer, timeout: Duration) -> Self {
        Self {
            region_id,
            from_peer,
            to_peer,
            timeout,
        }
    }
}

impl Display for RegionMigrationProcedureTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "region: {}, from_peer: {}, to_peer: {}",
            self.region_id, self.from_peer, self.to_peer
        )
    }
}

impl RegionMigrationManager {
    /// Returns new [`RegionMigrationManager`]
    pub(crate) fn new(
        procedure_manager: ProcedureManagerRef,
        context_factory: DefaultContextFactory,
    ) -> Self {
        Self {
            procedure_manager,
            context_factory,
            tracker: RegionMigrationProcedureTracker::default(),
        }
    }

    /// Returns the [`RegionMigrationProcedureTracker`].
    pub fn tracker(&self) -> &RegionMigrationProcedureTracker {
        &self.tracker
    }

    /// Registers the loader of [RegionMigrationProcedure] to the `ProcedureManager`.
    pub(crate) fn try_start(&self) -> Result<()> {
        let context_factory = self.context_factory.clone();
        let tracker = self.tracker.clone();
        self.procedure_manager
            .register_loader(
                RegionMigrationProcedure::TYPE_NAME,
                Box::new(move |json| {
                    let context_factory = context_factory.clone();
                    let tracker = tracker.clone();
                    RegionMigrationProcedure::from_json(json, context_factory, tracker)
                        .map(|p| Box::new(p) as _)
                }),
            )
            .context(error::RegisterProcedureLoaderSnafu {
                type_name: RegionMigrationProcedure::TYPE_NAME,
            })
    }

    fn insert_running_procedure(
        &self,
        task: &RegionMigrationProcedureTask,
    ) -> Option<RegionMigrationProcedureGuard> {
        self.tracker.insert_running_procedure(task)
    }

    fn verify_task(&self, task: &RegionMigrationProcedureTask) -> Result<()> {
        if task.to_peer.id == task.from_peer.id {
            return error::InvalidArgumentsSnafu {
                err_msg: "The `from_peer_id` can't equal `to_peer_id`",
            }
            .fail();
        }

        Ok(())
    }

    async fn retrieve_table_route(&self, region_id: RegionId) -> Result<TableRouteValue> {
        let table_route = self
            .context_factory
            .table_metadata_manager
            .table_route_manager()
            .table_route_storage()
            .get(region_id.table_id())
            .await
            .context(error::TableMetadataManagerSnafu)?
            .context(error::TableRouteNotFoundSnafu {
                table_id: region_id.table_id(),
            })?;

        Ok(table_route)
    }

    async fn retrieve_table_info(&self, region_id: RegionId) -> Result<TableInfoValue> {
        let table_route = self
            .context_factory
            .table_metadata_manager
            .table_info_manager()
            .get(region_id.table_id())
            .await
            .context(error::TableMetadataManagerSnafu)?
            .context(error::TableInfoNotFoundSnafu {
                table_id: region_id.table_id(),
            })?
            .into_inner();

        Ok(table_route)
    }

    /// Verifies the type of region migration table route.
    fn verify_table_route(
        &self,
        table_route: &TableRouteValue,
        task: &RegionMigrationProcedureTask,
    ) -> Result<()> {
        if !table_route.is_physical() {
            return error::UnexpectedSnafu {
                violated: format!(
                    "Trying to execute region migration on the logical table, task {task}"
                ),
            }
            .fail();
        }

        Ok(())
    }

    /// Returns true if the region has been migrated.
    fn has_migrated(
        &self,
        region_route: &RegionRoute,
        task: &RegionMigrationProcedureTask,
    ) -> Result<bool> {
        if region_route.is_leader_downgrading() {
            return Ok(false);
        }

        let leader_peer = region_route
            .leader_peer
            .as_ref()
            .context(error::UnexpectedSnafu {
                violated: "Region route leader peer is not found",
            })?;

        Ok(leader_peer.id == task.to_peer.id)
    }

    /// Throws an error if `leader_peer` is not the `from_peer`.
    fn verify_region_leader_peer(
        &self,
        region_route: &RegionRoute,
        task: &RegionMigrationProcedureTask,
    ) -> Result<()> {
        let leader_peer = region_route
            .leader_peer
            .as_ref()
            .context(error::UnexpectedSnafu {
                violated: "Region route leader peer is not found",
            })?;

        ensure!(
            leader_peer.id == task.from_peer.id,
            error::InvalidArgumentsSnafu {
                err_msg: format!(
                    "Region's leader peer({}) is not the `from_peer`({}), region: {}",
                    leader_peer.id, task.from_peer.id, task.region_id
                ),
            }
        );

        Ok(())
    }

    /// Throws an error if `to_peer` is already has a region follower.
    fn verify_region_follower_peers(
        &self,
        region_route: &RegionRoute,
        task: &RegionMigrationProcedureTask,
    ) -> Result<()> {
        ensure!(
            !region_route.follower_peers.contains(&task.to_peer),
            error::InvalidArgumentsSnafu {
                err_msg: format!(
                    "The `to_peer`({}) is already has a region follower, region: {}",
                    task.to_peer.id, task.region_id
                ),
            },
        );

        Ok(())
    }

    /// Submits a new region migration procedure.
    pub async fn submit_procedure(
        &self,
        task: RegionMigrationProcedureTask,
    ) -> Result<Option<ProcedureId>> {
        let Some(guard) = self.insert_running_procedure(&task) else {
            return error::MigrationRunningSnafu {
                region_id: task.region_id,
            }
            .fail();
        };

        self.verify_task(&task)?;

        let region_id = task.region_id;

        let table_route = self.retrieve_table_route(region_id).await?;
        self.verify_table_route(&table_route, &task)?;

        // Safety: checked before.
        let region_route = table_route
            .region_route(region_id)
            .context(error::UnexpectedLogicalRouteTableSnafu {
                err_msg: format!("{table_route:?} is a non-physical TableRouteValue."),
            })?
            .context(error::RegionRouteNotFoundSnafu { region_id })?;

        if self.has_migrated(&region_route, &task)? {
            info!("Skipping region migration task: {task}");
            return Ok(None);
        }

        self.verify_region_leader_peer(&region_route, &task)?;
        self.verify_region_follower_peers(&region_route, &task)?;
        let table_info = self.retrieve_table_info(region_id).await?;
        let TableName {
            catalog_name,
            schema_name,
            ..
        } = table_info.table_name();
        METRIC_META_REGION_MIGRATION_DATANODES
            .with_label_values(&["src", &task.from_peer.id.to_string()])
            .inc();
        METRIC_META_REGION_MIGRATION_DATANODES
            .with_label_values(&["desc", &task.to_peer.id.to_string()])
            .inc();
        let RegionMigrationProcedureTask {
            region_id,
            from_peer,
            to_peer,
            timeout,
        } = task.clone();
        let procedure = RegionMigrationProcedure::new(
            PersistentContext {
                catalog: catalog_name,
                schema: schema_name,
                region_id,
                from_peer,
                to_peer,
                timeout,
            },
            self.context_factory.clone(),
            Some(guard),
        );
        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));
        let procedure_id = procedure_with_id.id;
        info!("Starting region migration procedure {procedure_id} for {task}");
        let procedure_manager = self.procedure_manager.clone();
        common_runtime::spawn_global(async move {
            let watcher = &mut match procedure_manager.submit(procedure_with_id).await {
                Ok(watcher) => watcher,
                Err(e) => {
                    error!(e; "Failed to submit region migration procedure {procedure_id} for {task}");
                    return;
                }
            };

            if let Err(e) = watcher::wait(watcher).await {
                error!(e; "Failed to wait region migration procedure {procedure_id} for {task}");
                METRIC_META_REGION_MIGRATION_FAIL.inc();
                return;
            }

            info!("Region migration procedure {procedure_id} for {task} is finished successfully!");
        });

        Ok(Some(procedure_id))
    }
}

#[cfg(test)]
mod test {
    use std::assert_matches::assert_matches;

    use common_meta::key::table_route::LogicalTableRouteValue;
    use common_meta::key::test_utils::new_test_table_info;
    use common_meta::rpc::router::Region;

    use super::*;
    use crate::procedure::region_migration::test_util::TestingEnv;

    #[tokio::test]
    async fn test_insert_running_procedure() {
        let env = TestingEnv::new();
        let context_factory = env.context_factory();
        let manager = RegionMigrationManager::new(env.procedure_manager().clone(), context_factory);
        let region_id = RegionId::new(1024, 1);
        let task = RegionMigrationProcedureTask {
            region_id,
            from_peer: Peer::empty(2),
            to_peer: Peer::empty(1),
            timeout: Duration::from_millis(1000),
        };
        // Inserts one
        manager
            .tracker
            .running_procedures
            .write()
            .unwrap()
            .insert(region_id, task.clone());

        let err = manager.submit_procedure(task).await.unwrap_err();
        assert_matches!(err, error::Error::MigrationRunning { .. });
    }

    #[tokio::test]
    async fn test_submit_procedure_invalid_task() {
        let env = TestingEnv::new();
        let context_factory = env.context_factory();
        let manager = RegionMigrationManager::new(env.procedure_manager().clone(), context_factory);
        let region_id = RegionId::new(1024, 1);
        let task = RegionMigrationProcedureTask {
            region_id,
            from_peer: Peer::empty(1),
            to_peer: Peer::empty(1),
            timeout: Duration::from_millis(1000),
        };

        let err = manager.submit_procedure(task).await.unwrap_err();
        assert_matches!(err, error::Error::InvalidArguments { .. });
    }

    #[tokio::test]
    async fn test_submit_procedure_table_not_found() {
        let env = TestingEnv::new();
        let context_factory = env.context_factory();
        let manager = RegionMigrationManager::new(env.procedure_manager().clone(), context_factory);
        let region_id = RegionId::new(1024, 1);
        let task = RegionMigrationProcedureTask {
            region_id,
            from_peer: Peer::empty(1),
            to_peer: Peer::empty(2),
            timeout: Duration::from_millis(1000),
        };

        let err = manager.submit_procedure(task).await.unwrap_err();
        assert_matches!(err, error::Error::TableRouteNotFound { .. });
    }

    #[tokio::test]
    async fn test_submit_procedure_region_route_not_found() {
        let env = TestingEnv::new();
        let context_factory = env.context_factory();
        let manager = RegionMigrationManager::new(env.procedure_manager().clone(), context_factory);
        let region_id = RegionId::new(1024, 1);
        let task = RegionMigrationProcedureTask {
            region_id,
            from_peer: Peer::empty(1),
            to_peer: Peer::empty(2),
            timeout: Duration::from_millis(1000),
        };

        let table_info = new_test_table_info(1024, vec![1]).into();
        let region_routes = vec![RegionRoute {
            region: Region::new_test(RegionId::new(1024, 2)),
            leader_peer: Some(Peer::empty(3)),
            ..Default::default()
        }];

        env.create_physical_table_metadata(table_info, region_routes)
            .await;

        let err = manager.submit_procedure(task).await.unwrap_err();
        assert_matches!(err, error::Error::RegionRouteNotFound { .. });
    }

    #[tokio::test]
    async fn test_submit_procedure_incorrect_from_peer() {
        let env = TestingEnv::new();
        let context_factory = env.context_factory();
        let manager = RegionMigrationManager::new(env.procedure_manager().clone(), context_factory);
        let region_id = RegionId::new(1024, 1);
        let task = RegionMigrationProcedureTask {
            region_id,
            from_peer: Peer::empty(1),
            to_peer: Peer::empty(2),
            timeout: Duration::from_millis(1000),
        };

        let table_info = new_test_table_info(1024, vec![1]).into();
        let region_routes = vec![RegionRoute {
            region: Region::new_test(RegionId::new(1024, 1)),
            leader_peer: Some(Peer::empty(3)),
            ..Default::default()
        }];

        env.create_physical_table_metadata(table_info, region_routes)
            .await;

        let err = manager.submit_procedure(task).await.unwrap_err();
        assert_matches!(err, error::Error::InvalidArguments { .. });
        assert_eq!(err.to_string(), "Invalid arguments: Region's leader peer(3) is not the `from_peer`(1), region: 4398046511105(1024, 1)");
    }

    #[tokio::test]
    async fn test_submit_procedure_region_follower_on_to_peer() {
        let env = TestingEnv::new();
        let context_factory = env.context_factory();
        let manager = RegionMigrationManager::new(env.procedure_manager().clone(), context_factory);
        let region_id = RegionId::new(1024, 1);
        let task = RegionMigrationProcedureTask {
            region_id,
            from_peer: Peer::empty(3),
            to_peer: Peer::empty(2),
            timeout: Duration::from_millis(1000),
        };

        let table_info = new_test_table_info(1024, vec![1]).into();
        let region_routes = vec![RegionRoute {
            region: Region::new_test(region_id),
            leader_peer: Some(Peer::empty(3)),
            follower_peers: vec![Peer::empty(2)],
            ..Default::default()
        }];

        env.create_physical_table_metadata(table_info, region_routes)
            .await;

        let err = manager.submit_procedure(task).await.unwrap_err();
        assert_matches!(err, error::Error::InvalidArguments { .. });
        assert_eq!(
            err.to_string(),
            "Invalid arguments: The `to_peer`(2) is already has a region follower, region: 4398046511105(1024, 1)"
        );
    }

    #[tokio::test]
    async fn test_submit_procedure_has_migrated() {
        common_telemetry::init_default_ut_logging();
        let env = TestingEnv::new();
        let context_factory = env.context_factory();
        let manager = RegionMigrationManager::new(env.procedure_manager().clone(), context_factory);
        let region_id = RegionId::new(1024, 1);
        let task = RegionMigrationProcedureTask {
            region_id,
            from_peer: Peer::empty(1),
            to_peer: Peer::empty(2),
            timeout: Duration::from_millis(1000),
        };

        let table_info = new_test_table_info(1024, vec![1]).into();
        let region_routes = vec![RegionRoute {
            region: Region::new_test(RegionId::new(1024, 1)),
            leader_peer: Some(Peer::empty(2)),
            ..Default::default()
        }];

        env.create_physical_table_metadata(table_info, region_routes)
            .await;

        manager.submit_procedure(task).await.unwrap();
    }

    #[tokio::test]
    async fn test_verify_table_route_error() {
        let env = TestingEnv::new();
        let context_factory = env.context_factory();
        let manager = RegionMigrationManager::new(env.procedure_manager().clone(), context_factory);
        let region_id = RegionId::new(1024, 1);
        let task = RegionMigrationProcedureTask {
            region_id,
            from_peer: Peer::empty(1),
            to_peer: Peer::empty(2),
            timeout: Duration::from_millis(1000),
        };

        let err = manager
            .verify_table_route(
                &TableRouteValue::Logical(LogicalTableRouteValue::new(0, vec![])),
                &task,
            )
            .unwrap_err();

        assert_matches!(err, error::Error::Unexpected { .. });
    }
}
