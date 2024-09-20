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
pub(crate) mod manager;
pub(crate) mod migration_abort;
pub(crate) mod migration_end;
pub(crate) mod migration_start;
pub(crate) mod open_candidate_region;
#[cfg(test)]
pub mod test_util;
pub(crate) mod update_metadata;
pub(crate) mod upgrade_candidate_region;

use std::any::Any;
use std::fmt::Debug;
use std::time::Duration;

use common_error::ext::BoxedError;
use common_meta::cache_invalidator::CacheInvalidatorRef;
use common_meta::ddl::RegionFailureDetectorControllerRef;
use common_meta::instruction::CacheIdent;
use common_meta::key::datanode_table::{DatanodeTableKey, DatanodeTableValue};
use common_meta::key::table_info::TableInfoValue;
use common_meta::key::table_route::TableRouteValue;
use common_meta::key::{DeserializedValueWithBytes, TableMetadataManagerRef};
use common_meta::lock_key::{CatalogLock, RegionLock, SchemaLock, TableLock};
use common_meta::peer::Peer;
use common_meta::region_keeper::{MemoryRegionKeeperRef, OperatingRegionGuard};
use common_meta::ClusterId;
use common_procedure::error::{
    Error as ProcedureError, FromJsonSnafu, Result as ProcedureResult, ToJsonSnafu,
};
use common_procedure::{Context as ProcedureContext, LockKey, Procedure, Status, StringKey};
pub use manager::RegionMigrationProcedureTask;
use manager::{RegionMigrationProcedureGuard, RegionMigrationProcedureTracker};
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use store_api::storage::RegionId;
use tokio::time::Instant;

use self::migration_start::RegionMigrationStart;
use crate::error::{self, Result};
use crate::service::mailbox::MailboxRef;

/// It's shared in each step and available even after recovering.
///
/// It will only be updated/stored after the Red node has succeeded.
///
/// **Notes: Stores with too large data in the context might incur replication overhead.**
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PersistentContext {
    /// The table catalog.
    catalog: String,
    /// The table schema.
    schema: String,
    /// The Id of the cluster.
    cluster_id: ClusterId,
    /// The [Peer] of migration source.
    from_peer: Peer,
    /// The [Peer] of migration destination.
    to_peer: Peer,
    /// The [RegionId] of migration region.
    region_id: RegionId,
    /// The timeout of waiting for a candidate to replay the WAL.
    #[serde(with = "humantime_serde", default = "default_replay_timeout")]
    replay_timeout: Duration,
}

fn default_replay_timeout() -> Duration {
    Duration::from_secs(1)
}

impl PersistentContext {
    pub fn lock_key(&self) -> Vec<StringKey> {
        let region_id = self.region_id;
        let lock_key = vec![
            CatalogLock::Read(&self.catalog).into(),
            SchemaLock::read(&self.catalog, &self.schema).into(),
            TableLock::Read(region_id.table_id()).into(),
            RegionLock::Write(region_id).into(),
        ];

        lock_key
    }
}

/// It's shared in each step and available in executing (including retrying).
///
/// It will be dropped if the procedure runner crashes.
///
/// The additional remote fetches are only required in the worst cases.
#[derive(Debug, Clone, Default)]
pub struct VolatileContext {
    /// `opening_region_guard` will be set after the
    /// [OpenCandidateRegion](crate::procedure::region_migration::open_candidate_region::OpenCandidateRegion) step.
    ///
    /// `opening_region_guard` should be consumed after
    /// the corresponding [RegionRoute](common_meta::rpc::router::RegionRoute) of the opening region
    /// was written into [TableRouteValue](common_meta::key::table_route::TableRouteValue).
    opening_region_guard: Option<OperatingRegionGuard>,
    /// `table_route` is stored via previous steps for future use.
    table_route: Option<DeserializedValueWithBytes<TableRouteValue>>,
    /// `datanode_table` is stored via previous steps for future use.
    from_peer_datanode_table: Option<DatanodeTableValue>,
    /// `table_info` is stored via previous steps for future use.
    ///
    /// `table_info` should remain unchanged during the procedure;
    /// no other DDL procedure executed concurrently for the current table.
    table_info: Option<DeserializedValueWithBytes<TableInfoValue>>,
    /// The deadline of leader region lease.
    leader_region_lease_deadline: Option<Instant>,
    /// The last_entry_id of leader region.
    leader_region_last_entry_id: Option<u64>,
}

impl VolatileContext {
    /// Sets the `leader_region_lease_deadline` if it does not exist.
    pub fn set_leader_region_lease_deadline(&mut self, lease_timeout: Duration) {
        if self.leader_region_lease_deadline.is_none() {
            self.leader_region_lease_deadline = Some(Instant::now() + lease_timeout);
        }
    }

    /// Resets the `leader_region_lease_deadline`.
    pub fn reset_leader_region_lease_deadline(&mut self) {
        self.leader_region_lease_deadline = None;
    }

    /// Sets the `leader_region_last_entry_id`.
    pub fn set_last_entry_id(&mut self, last_entry_id: u64) {
        self.leader_region_last_entry_id = Some(last_entry_id)
    }
}

/// Used to generate new [Context].
pub trait ContextFactory {
    fn new_context(self, persistent_ctx: PersistentContext) -> Context;
}

/// Default implementation.
#[derive(Clone)]
pub struct DefaultContextFactory {
    volatile_ctx: VolatileContext,
    table_metadata_manager: TableMetadataManagerRef,
    opening_region_keeper: MemoryRegionKeeperRef,
    region_failure_detector_controller: RegionFailureDetectorControllerRef,
    mailbox: MailboxRef,
    server_addr: String,
    cache_invalidator: CacheInvalidatorRef,
}

impl DefaultContextFactory {
    /// Returns an [`DefaultContextFactory`].
    pub fn new(
        table_metadata_manager: TableMetadataManagerRef,
        opening_region_keeper: MemoryRegionKeeperRef,
        region_failure_detector_controller: RegionFailureDetectorControllerRef,
        mailbox: MailboxRef,
        server_addr: String,
        cache_invalidator: CacheInvalidatorRef,
    ) -> Self {
        Self {
            volatile_ctx: VolatileContext::default(),
            table_metadata_manager,
            opening_region_keeper,
            region_failure_detector_controller,
            mailbox,
            server_addr,
            cache_invalidator,
        }
    }
}

impl ContextFactory for DefaultContextFactory {
    fn new_context(self, persistent_ctx: PersistentContext) -> Context {
        Context {
            persistent_ctx,
            volatile_ctx: self.volatile_ctx,
            table_metadata_manager: self.table_metadata_manager,
            opening_region_keeper: self.opening_region_keeper,
            region_failure_detector_controller: self.region_failure_detector_controller,
            mailbox: self.mailbox,
            server_addr: self.server_addr,
            cache_invalidator: self.cache_invalidator,
        }
    }
}

/// The context of procedure execution.
pub struct Context {
    persistent_ctx: PersistentContext,
    volatile_ctx: VolatileContext,
    table_metadata_manager: TableMetadataManagerRef,
    opening_region_keeper: MemoryRegionKeeperRef,
    region_failure_detector_controller: RegionFailureDetectorControllerRef,
    mailbox: MailboxRef,
    server_addr: String,
    cache_invalidator: CacheInvalidatorRef,
}

impl Context {
    /// Returns address of meta server.
    pub fn server_addr(&self) -> &str {
        &self.server_addr
    }

    /// Returns the `table_route` of [VolatileContext] if any.
    /// Otherwise, returns the value retrieved from remote.
    ///
    /// Retry:
    /// - Failed to retrieve the metadata of table.
    pub async fn get_table_route_value(
        &mut self,
    ) -> Result<&DeserializedValueWithBytes<TableRouteValue>> {
        let table_route_value = &mut self.volatile_ctx.table_route;

        if table_route_value.is_none() {
            let table_id = self.persistent_ctx.region_id.table_id();
            let table_route = self
                .table_metadata_manager
                .table_route_manager()
                .table_route_storage()
                .get_with_raw_bytes(table_id)
                .await
                .context(error::TableMetadataManagerSnafu)
                .map_err(BoxedError::new)
                .context(error::RetryLaterWithSourceSnafu {
                    reason: format!("Failed to get TableRoute: {table_id}"),
                })?
                .context(error::TableRouteNotFoundSnafu { table_id })?;

            *table_route_value = Some(table_route);
        }

        Ok(table_route_value.as_ref().unwrap())
    }

    /// Notifies the RegionSupervisor to register failure detectors of failed region.
    ///
    /// The original failure detector was removed once the procedure was triggered.
    /// Now, we need to register the failure detector for the failed region again.
    pub async fn register_failure_detectors(&self) {
        let cluster_id = self.persistent_ctx.cluster_id;
        let datanode_id = self.persistent_ctx.from_peer.id;
        let region_id = self.persistent_ctx.region_id;

        self.region_failure_detector_controller
            .register_failure_detectors(vec![(cluster_id, datanode_id, region_id)])
            .await;
    }

    /// Notifies the RegionSupervisor to deregister failure detectors.
    ///
    /// The original failure detectors was removed once the procedure was triggered.
    /// However, the `from_peer` may still send the heartbeats contains the failed region.
    pub async fn deregister_failure_detectors(&self) {
        let cluster_id = self.persistent_ctx.cluster_id;
        let datanode_id = self.persistent_ctx.from_peer.id;
        let region_id = self.persistent_ctx.region_id;

        self.region_failure_detector_controller
            .deregister_failure_detectors(vec![(cluster_id, datanode_id, region_id)])
            .await;
    }

    /// Removes the `table_route` of [VolatileContext], returns true if any.
    pub fn remove_table_route_value(&mut self) -> bool {
        let value = self.volatile_ctx.table_route.take();
        value.is_some()
    }

    /// Returns the `table_info` of [VolatileContext] if any.
    /// Otherwise, returns the value retrieved from remote.
    ///
    /// Retry:
    /// - Failed to retrieve the metadata of table.
    pub async fn get_table_info_value(
        &mut self,
    ) -> Result<&DeserializedValueWithBytes<TableInfoValue>> {
        let table_info_value = &mut self.volatile_ctx.table_info;

        if table_info_value.is_none() {
            let table_id = self.persistent_ctx.region_id.table_id();
            let table_info = self
                .table_metadata_manager
                .table_info_manager()
                .get(table_id)
                .await
                .context(error::TableMetadataManagerSnafu)
                .map_err(BoxedError::new)
                .context(error::RetryLaterWithSourceSnafu {
                    reason: format!("Failed to get TableInfo: {table_id}"),
                })?
                .context(error::TableInfoNotFoundSnafu { table_id })?;

            *table_info_value = Some(table_info);
        }

        Ok(table_info_value.as_ref().unwrap())
    }

    /// Returns the `table_info` of [VolatileContext] if any.
    /// Otherwise, returns the value retrieved from remote.
    ///
    /// Retry:
    /// - Failed to retrieve the metadata of datanode.
    pub async fn get_from_peer_datanode_table_value(&mut self) -> Result<&DatanodeTableValue> {
        let datanode_value = &mut self.volatile_ctx.from_peer_datanode_table;

        if datanode_value.is_none() {
            let table_id = self.persistent_ctx.region_id.table_id();
            let datanode_id = self.persistent_ctx.from_peer.id;

            let datanode_table = self
                .table_metadata_manager
                .datanode_table_manager()
                .get(&DatanodeTableKey {
                    datanode_id,
                    table_id,
                })
                .await
                .context(error::TableMetadataManagerSnafu)
                .map_err(BoxedError::new)
                .context(error::RetryLaterWithSourceSnafu {
                    reason: format!("Failed to get DatanodeTable: ({datanode_id},{table_id})"),
                })?
                .context(error::DatanodeTableNotFoundSnafu {
                    table_id,
                    datanode_id,
                })?;

            *datanode_value = Some(datanode_table);
        }

        Ok(datanode_value.as_ref().unwrap())
    }

    /// Removes the `table_info` of [VolatileContext], returns true if any.
    pub fn remove_table_info_value(&mut self) -> bool {
        let value = self.volatile_ctx.table_info.take();
        value.is_some()
    }

    /// Returns the [RegionId].
    pub fn region_id(&self) -> RegionId {
        self.persistent_ctx.region_id
    }

    /// Broadcasts the invalidate table cache message.
    pub async fn invalidate_table_cache(&self) -> Result<()> {
        let table_id = self.region_id().table_id();
        // ignore the result
        let ctx = common_meta::cache_invalidator::Context::default();
        let _ = self
            .cache_invalidator
            .invalidate(&ctx, &[CacheIdent::TableId(table_id)])
            .await;
        Ok(())
    }
}

#[async_trait::async_trait]
#[typetag::serde(tag = "region_migration_state")]
pub(crate) trait State: Sync + Send + Debug {
    /// Yields the next [State] and [Status].
    async fn next(&mut self, ctx: &mut Context) -> Result<(Box<dyn State>, Status)>;

    /// Returns as [Any](std::any::Any).
    fn as_any(&self) -> &dyn Any;
}

/// Persistent data of [RegionMigrationProcedure].
#[derive(Debug, Serialize, Deserialize)]
pub struct RegionMigrationDataOwned {
    persistent_ctx: PersistentContext,
    state: Box<dyn State>,
}

/// Persistent data of [RegionMigrationProcedure].
#[derive(Debug, Serialize)]
pub struct RegionMigrationData<'a> {
    persistent_ctx: &'a PersistentContext,
    state: &'a dyn State,
}

pub(crate) struct RegionMigrationProcedure {
    state: Box<dyn State>,
    context: Context,
    _guard: Option<RegionMigrationProcedureGuard>,
}

impl RegionMigrationProcedure {
    const TYPE_NAME: &'static str = "metasrv-procedure::RegionMigration";

    pub fn new(
        persistent_context: PersistentContext,
        context_factory: impl ContextFactory,
        guard: Option<RegionMigrationProcedureGuard>,
    ) -> Self {
        let state = Box::new(RegionMigrationStart {});
        Self::new_inner(state, persistent_context, context_factory, guard)
    }

    fn new_inner(
        state: Box<dyn State>,
        persistent_context: PersistentContext,
        context_factory: impl ContextFactory,
        guard: Option<RegionMigrationProcedureGuard>,
    ) -> Self {
        Self {
            state,
            context: context_factory.new_context(persistent_context),
            _guard: guard,
        }
    }

    fn from_json(
        json: &str,
        context_factory: impl ContextFactory,
        tracker: RegionMigrationProcedureTracker,
    ) -> ProcedureResult<Self> {
        let RegionMigrationDataOwned {
            persistent_ctx,
            state,
        } = serde_json::from_str(json).context(FromJsonSnafu)?;

        let guard = tracker.insert_running_procedure(&RegionMigrationProcedureTask {
            cluster_id: persistent_ctx.cluster_id,
            region_id: persistent_ctx.region_id,
            from_peer: persistent_ctx.from_peer.clone(),
            to_peer: persistent_ctx.to_peer.clone(),
            replay_timeout: persistent_ctx.replay_timeout,
        });
        let context = context_factory.new_context(persistent_ctx);

        Ok(Self {
            state,
            context,
            _guard: guard,
        })
    }
}

#[async_trait::async_trait]
impl Procedure for RegionMigrationProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        let state = &mut self.state;

        let (next, status) = state.next(&mut self.context).await.map_err(|e| {
            if e.is_retryable() {
                ProcedureError::retry_later(e)
            } else {
                ProcedureError::external(e)
            }
        })?;

        *state = next;
        Ok(status)
    }

    fn dump(&self) -> ProcedureResult<String> {
        let data = RegionMigrationData {
            state: self.state.as_ref(),
            persistent_ctx: &self.context.persistent_ctx,
        };
        serde_json::to_string(&data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        LockKey::new(self.context.persistent_ctx.lock_key())
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::sync::Arc;

    use common_meta::distributed_time_constants::REGION_LEASE_SECS;
    use common_meta::instruction::Instruction;
    use common_meta::key::test_utils::new_test_table_info;
    use common_meta::rpc::router::{Region, RegionRoute};

    use super::migration_end::RegionMigrationEnd;
    use super::update_metadata::UpdateMetadata;
    use super::*;
    use crate::handler::HeartbeatMailbox;
    use crate::procedure::region_migration::open_candidate_region::OpenCandidateRegion;
    use crate::procedure::region_migration::test_util::*;
    use crate::service::mailbox::Channel;

    fn new_persistent_context() -> PersistentContext {
        test_util::new_persistent_context(1, 2, RegionId::new(1024, 1))
    }

    #[test]
    fn test_lock_key() {
        let persistent_context = new_persistent_context();
        let expected_keys = persistent_context.lock_key();

        let env = TestingEnv::new();
        let context = env.context_factory();

        let procedure = RegionMigrationProcedure::new(persistent_context, context, None);

        let key = procedure.lock_key();
        let keys = key.keys_to_lock().cloned().collect::<Vec<_>>();

        for key in expected_keys {
            assert!(keys.contains(&key));
        }
    }

    #[test]
    fn test_data_serialization() {
        let persistent_context = new_persistent_context();

        let env = TestingEnv::new();
        let context = env.context_factory();

        let procedure = RegionMigrationProcedure::new(persistent_context, context, None);

        let serialized = procedure.dump().unwrap();
        let expected = r#"{"persistent_ctx":{"catalog":"greptime","schema":"public","cluster_id":0,"from_peer":{"id":1,"addr":""},"to_peer":{"id":2,"addr":""},"region_id":4398046511105,"replay_timeout":"1s"},"state":{"region_migration_state":"RegionMigrationStart"}}"#;
        assert_eq!(expected, serialized);
    }

    #[test]
    fn test_backward_compatibility() {
        let persistent_ctx = test_util::new_persistent_context(1, 2, RegionId::new(1024, 1));
        // NOTES: Changes it will break backward compatibility.
        let serialized = r#"{"catalog":"greptime","schema":"public","cluster_id":0,"from_peer":{"id":1,"addr":""},"to_peer":{"id":2,"addr":""},"region_id":4398046511105}"#;
        let deserialized: PersistentContext = serde_json::from_str(serialized).unwrap();

        assert_eq!(persistent_ctx, deserialized);
    }

    #[derive(Debug, Serialize, Deserialize, Default)]
    pub struct MockState;

    #[async_trait::async_trait]
    #[typetag::serde]
    impl State for MockState {
        async fn next(&mut self, ctx: &mut Context) -> Result<(Box<dyn State>, Status)> {
            let pc = &mut ctx.persistent_ctx;

            if pc.cluster_id == 2 {
                Ok((Box::new(RegionMigrationEnd), Status::done()))
            } else {
                pc.cluster_id += 1;
                Ok((Box::new(MockState), Status::executing(false)))
            }
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[tokio::test]
    async fn test_execution_after_deserialized() {
        let env = TestingEnv::new();

        fn new_mock_procedure(env: &TestingEnv) -> RegionMigrationProcedure {
            let persistent_context = new_persistent_context();
            let context_factory = env.context_factory();
            let state = Box::<MockState>::default();
            RegionMigrationProcedure::new_inner(state, persistent_context, context_factory, None)
        }

        let ctx = TestingEnv::procedure_context();
        let mut procedure = new_mock_procedure(&env);
        let mut status = None;
        for _ in 0..3 {
            status = Some(procedure.execute(&ctx).await.unwrap());
        }
        assert!(status.unwrap().is_done());

        let ctx = TestingEnv::procedure_context();
        let mut procedure = new_mock_procedure(&env);

        status = Some(procedure.execute(&ctx).await.unwrap());

        let serialized = procedure.dump().unwrap();

        let context_factory = env.context_factory();
        let tracker = env.tracker();
        let mut procedure =
            RegionMigrationProcedure::from_json(&serialized, context_factory, tracker.clone())
                .unwrap();
        assert!(tracker.contains(procedure.context.persistent_ctx.region_id));

        for _ in 1..3 {
            status = Some(procedure.execute(&ctx).await.unwrap());
        }
        assert_eq!(procedure.context.persistent_ctx.cluster_id, 2);
        assert!(status.unwrap().is_done());
    }

    #[tokio::test]
    async fn test_broadcast_invalidate_table_cache() {
        let mut env = TestingEnv::new();
        let persistent_context = test_util::new_persistent_context(1, 2, RegionId::new(1024, 1));
        let ctx = env.context_factory().new_context(persistent_context);
        let mailbox_ctx = env.mailbox_context();

        // No receivers.
        ctx.invalidate_table_cache().await.unwrap();

        let (tx, mut rx) = tokio::sync::mpsc::channel(1);

        mailbox_ctx
            .insert_heartbeat_response_receiver(Channel::Frontend(1), tx)
            .await;

        ctx.invalidate_table_cache().await.unwrap();

        let resp = rx.recv().await.unwrap().unwrap();
        let msg = resp.mailbox_message.unwrap();

        let instruction = HeartbeatMailbox::json_instruction(&msg).unwrap();
        assert_eq!(
            instruction,
            Instruction::InvalidateCaches(vec![CacheIdent::TableId(1024)])
        );
    }

    fn procedure_flow_steps(from_peer_id: u64, to_peer_id: u64) -> Vec<Step> {
        vec![
            // MigrationStart
            Step::next(
                "Should be the update metadata for downgrading",
                None,
                Assertion::simple(assert_update_metadata_downgrade, assert_need_persist),
            ),
            // UpdateMetadata::Downgrade
            Step::next(
                "Should be the downgrade leader region",
                None,
                Assertion::simple(assert_downgrade_leader_region, assert_no_persist),
            ),
            // Downgrade Candidate
            Step::next(
                "Should be the upgrade candidate region",
                Some(mock_datanode_reply(
                    from_peer_id,
                    Arc::new(|id| Ok(new_downgrade_region_reply(id, None, true, None))),
                )),
                Assertion::simple(assert_upgrade_candidate_region, assert_no_persist),
            ),
            // Upgrade Candidate
            Step::next(
                "Should be the update metadata for upgrading",
                Some(mock_datanode_reply(
                    to_peer_id,
                    Arc::new(|id| Ok(new_upgrade_region_reply(id, true, true, None))),
                )),
                Assertion::simple(assert_update_metadata_upgrade, assert_no_persist),
            ),
            // UpdateMetadata::Upgrade
            Step::next(
                "Should be the region migration end",
                None,
                Assertion::simple(assert_region_migration_end, assert_done),
            ),
            // RegionMigrationEnd
            Step::next(
                "Should be the region migration end again",
                None,
                Assertion::simple(assert_region_migration_end, assert_done),
            ),
        ]
    }

    #[tokio::test]
    async fn test_procedure_flow() {
        common_telemetry::init_default_ut_logging();

        let persistent_context = test_util::new_persistent_context(1, 2, RegionId::new(1024, 1));
        let state = Box::new(RegionMigrationStart);

        // The table metadata.
        let from_peer_id = persistent_context.from_peer.id;
        let to_peer_id = persistent_context.to_peer.id;
        let from_peer = persistent_context.from_peer.clone();
        let to_peer = persistent_context.to_peer.clone();
        let region_id = persistent_context.region_id;
        let table_info = new_test_table_info(1024, vec![1]).into();
        let region_routes = vec![RegionRoute {
            region: Region::new_test(region_id),
            leader_peer: Some(from_peer),
            follower_peers: vec![to_peer],
            ..Default::default()
        }];

        let suite = ProcedureMigrationTestSuite::new(persistent_context, state);
        suite.init_table_metadata(table_info, region_routes).await;

        let steps = procedure_flow_steps(from_peer_id, to_peer_id);
        let timer = Instant::now();

        // Run the table tests.
        let runner = ProcedureMigrationSuiteRunner::new(suite)
            .steps(steps)
            .run_once()
            .await;

        // Ensure it didn't run into the slow path.
        assert!(timer.elapsed().as_secs() < REGION_LEASE_SECS / 2);

        runner.suite.verify_table_metadata().await;
    }

    #[tokio::test]
    async fn test_procedure_flow_idempotent() {
        common_telemetry::init_default_ut_logging();

        let persistent_context = test_util::new_persistent_context(1, 2, RegionId::new(1024, 1));
        let state = Box::new(RegionMigrationStart);

        // The table metadata.
        let from_peer_id = persistent_context.from_peer.id;
        let to_peer_id = persistent_context.to_peer.id;
        let from_peer = persistent_context.from_peer.clone();
        let to_peer = persistent_context.to_peer.clone();
        let region_id = persistent_context.region_id;
        let table_info = new_test_table_info(1024, vec![1]).into();
        let region_routes = vec![RegionRoute {
            region: Region::new_test(region_id),
            leader_peer: Some(from_peer),
            follower_peers: vec![to_peer],
            ..Default::default()
        }];

        let suite = ProcedureMigrationTestSuite::new(persistent_context, state);
        suite.init_table_metadata(table_info, region_routes).await;

        let steps = procedure_flow_steps(from_peer_id, to_peer_id);
        let setup_to_latest_persisted_state = Step::setup(
            "Sets state to UpdateMetadata::Downgrade",
            merge_before_test_fn(vec![
                setup_state(Arc::new(|| Box::new(UpdateMetadata::Downgrade))),
                Arc::new(reset_volatile_ctx),
            ]),
        );

        let steps = [
            steps.clone(),
            vec![setup_to_latest_persisted_state.clone()],
            steps.clone()[1..].to_vec(),
            vec![setup_to_latest_persisted_state],
            steps.clone()[1..].to_vec(),
        ]
        .concat();
        let timer = Instant::now();

        // Run the table tests.
        let runner = ProcedureMigrationSuiteRunner::new(suite)
            .steps(steps.clone())
            .run_once()
            .await;

        // Ensure it didn't run into the slow path.
        assert!(timer.elapsed().as_secs() < REGION_LEASE_SECS / 2);

        runner.suite.verify_table_metadata().await;
    }

    #[tokio::test]
    async fn test_procedure_flow_open_candidate_region_retryable_error() {
        common_telemetry::init_default_ut_logging();

        let persistent_context = test_util::new_persistent_context(1, 2, RegionId::new(1024, 1));
        let state = Box::new(RegionMigrationStart);

        // The table metadata.
        let to_peer_id = persistent_context.to_peer.id;
        let from_peer = persistent_context.from_peer.clone();
        let region_id = persistent_context.region_id;
        let table_info = new_test_table_info(1024, vec![1]).into();
        let region_routes = vec![RegionRoute {
            region: Region::new_test(region_id),
            leader_peer: Some(from_peer),
            follower_peers: vec![],
            ..Default::default()
        }];

        let suite = ProcedureMigrationTestSuite::new(persistent_context, state);
        suite.init_table_metadata(table_info, region_routes).await;

        let steps = vec![
            // Migration Start
            Step::next(
                "Should be the open candidate region",
                None,
                Assertion::simple(assert_open_candidate_region, assert_need_persist),
            ),
            // OpenCandidateRegion
            Step::next(
                "Should be throwing a non-retry error",
                Some(mock_datanode_reply(
                    to_peer_id,
                    Arc::new(|id| error::MailboxTimeoutSnafu { id }.fail()),
                )),
                Assertion::error(|error| assert!(error.is_retryable())),
            ),
            // OpenCandidateRegion
            Step::next(
                "Should be throwing a non-retry error again",
                Some(mock_datanode_reply(
                    to_peer_id,
                    Arc::new(|id| error::MailboxTimeoutSnafu { id }.fail()),
                )),
                Assertion::error(|error| assert!(error.is_retryable())),
            ),
        ];

        let setup_to_latest_persisted_state = Step::setup(
            "Sets state to UpdateMetadata::Downgrade",
            merge_before_test_fn(vec![
                setup_state(Arc::new(|| Box::new(OpenCandidateRegion))),
                Arc::new(reset_volatile_ctx),
            ]),
        );

        let steps = [
            steps.clone(),
            // Mocks the volatile ctx lost(i.g., Meta leader restarts).
            vec![setup_to_latest_persisted_state.clone()],
            steps.clone()[1..].to_vec(),
            vec![setup_to_latest_persisted_state],
            steps.clone()[1..].to_vec(),
        ]
        .concat();

        // Run the table tests.
        let runner = ProcedureMigrationSuiteRunner::new(suite)
            .steps(steps.clone())
            .run_once()
            .await;

        let table_routes_version = runner
            .env()
            .table_metadata_manager()
            .table_route_manager()
            .table_route_storage()
            .get(region_id.table_id())
            .await
            .unwrap()
            .unwrap()
            .version();
        // Should be unchanged.
        assert_eq!(table_routes_version.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_procedure_flow_upgrade_candidate_with_retry_and_failed() {
        common_telemetry::init_default_ut_logging();

        let persistent_context = test_util::new_persistent_context(1, 2, RegionId::new(1024, 1));
        let state = Box::new(RegionMigrationStart);

        // The table metadata.
        let from_peer_id = persistent_context.from_peer.id;
        let to_peer_id = persistent_context.to_peer.id;
        let from_peer = persistent_context.from_peer.clone();
        let to_peer = persistent_context.to_peer.clone();
        let region_id = persistent_context.region_id;
        let table_info = new_test_table_info(1024, vec![1]).into();
        let region_routes = vec![RegionRoute {
            region: Region::new_test(region_id),
            leader_peer: Some(from_peer),
            follower_peers: vec![to_peer],
            ..Default::default()
        }];

        let suite = ProcedureMigrationTestSuite::new(persistent_context, state);
        suite.init_table_metadata(table_info, region_routes).await;

        let steps = vec![
            // MigrationStart
            Step::next(
                "Should be the update metadata for downgrading",
                None,
                Assertion::simple(assert_update_metadata_downgrade, assert_need_persist),
            ),
            // UpdateMetadata::Downgrade
            Step::next(
                "Should be the downgrade leader region",
                None,
                Assertion::simple(assert_downgrade_leader_region, assert_no_persist),
            ),
            // Downgrade Candidate
            Step::next(
                "Should be the upgrade candidate region",
                Some(mock_datanode_reply(
                    from_peer_id,
                    Arc::new(|id| Ok(new_downgrade_region_reply(id, None, true, None))),
                )),
                Assertion::simple(assert_upgrade_candidate_region, assert_no_persist),
            ),
            // Upgrade Candidate
            Step::next(
                "Should be the rollback metadata",
                Some(mock_datanode_reply(
                    to_peer_id,
                    Arc::new(|id| error::MailboxTimeoutSnafu { id }.fail()),
                )),
                Assertion::simple(assert_update_metadata_rollback, assert_no_persist),
            ),
            // UpdateMetadata::Rollback
            Step::next(
                "Should be the region migration abort",
                None,
                Assertion::simple(assert_region_migration_abort, assert_no_persist),
            ),
            // RegionMigrationAbort
            Step::next(
                "Should throw an error",
                None,
                Assertion::error(|error| {
                    assert!(!error.is_retryable());
                    assert_matches!(error, error::Error::MigrationAbort { .. });
                }),
            ),
        ];

        let setup_to_latest_persisted_state = Step::setup(
            "Sets state to UpdateMetadata::Downgrade",
            merge_before_test_fn(vec![
                setup_state(Arc::new(|| Box::new(UpdateMetadata::Downgrade))),
                Arc::new(reset_volatile_ctx),
            ]),
        );

        let steps = [
            steps.clone(),
            vec![setup_to_latest_persisted_state.clone()],
            steps.clone()[1..].to_vec(),
            vec![setup_to_latest_persisted_state],
            steps.clone()[1..].to_vec(),
        ]
        .concat();

        // Run the table tests.
        ProcedureMigrationSuiteRunner::new(suite)
            .steps(steps.clone())
            .run_once()
            .await;
    }

    #[tokio::test]
    async fn test_procedure_flow_upgrade_candidate_with_retry() {
        common_telemetry::init_default_ut_logging();

        let persistent_context = test_util::new_persistent_context(1, 2, RegionId::new(1024, 1));
        let state = Box::new(RegionMigrationStart);

        // The table metadata.
        let to_peer_id = persistent_context.to_peer.id;
        let from_peer_id = persistent_context.from_peer.id;
        let from_peer = persistent_context.from_peer.clone();
        let region_id = persistent_context.region_id;
        let table_info = new_test_table_info(1024, vec![1]).into();
        let region_routes = vec![RegionRoute {
            region: Region::new_test(region_id),
            leader_peer: Some(from_peer),
            follower_peers: vec![],
            ..Default::default()
        }];

        let suite = ProcedureMigrationTestSuite::new(persistent_context, state);
        suite.init_table_metadata(table_info, region_routes).await;

        let steps = vec![
            // Migration Start
            Step::next(
                "Should be the open candidate region",
                None,
                Assertion::simple(assert_open_candidate_region, assert_need_persist),
            ),
            // OpenCandidateRegion
            Step::next(
                "Should be throwing a retryable error",
                Some(mock_datanode_reply(
                    to_peer_id,
                    Arc::new(|id| Ok(new_open_region_reply(id, false, None))),
                )),
                Assertion::error(|error| assert!(error.is_retryable(), "err: {error:?}")),
            ),
            // OpenCandidateRegion
            Step::next(
                "Should be the update metadata for downgrading",
                Some(mock_datanode_reply(
                    to_peer_id,
                    Arc::new(|id| Ok(new_open_region_reply(id, true, None))),
                )),
                Assertion::simple(assert_update_metadata_downgrade, assert_no_persist),
            ),
            // UpdateMetadata::Downgrade
            Step::next(
                "Should be the downgrade leader region",
                None,
                Assertion::simple(assert_downgrade_leader_region, assert_no_persist),
            ),
            // Downgrade Leader
            Step::next(
                "Should be the upgrade candidate region",
                Some(mock_datanode_reply(
                    from_peer_id,
                    merge_mailbox_messages(vec![
                        Arc::new(|id| error::MailboxTimeoutSnafu { id }.fail()),
                        Arc::new(|id| Ok(new_downgrade_region_reply(id, None, true, None))),
                    ]),
                )),
                Assertion::simple(assert_upgrade_candidate_region, assert_no_persist),
            ),
            // Upgrade Candidate
            Step::next(
                "Should be the update metadata for upgrading",
                Some(mock_datanode_reply(
                    to_peer_id,
                    merge_mailbox_messages(vec![
                        Arc::new(|id| error::MailboxTimeoutSnafu { id }.fail()),
                        Arc::new(|id| Ok(new_upgrade_region_reply(id, true, true, None))),
                    ]),
                )),
                Assertion::simple(assert_update_metadata_upgrade, assert_no_persist),
            ),
            // UpdateMetadata::Upgrade
            Step::next(
                "Should be the region migration end",
                None,
                Assertion::simple(assert_region_migration_end, assert_done),
            ),
            // RegionMigrationEnd
            Step::next(
                "Should be the region migration end again",
                None,
                Assertion::simple(assert_region_migration_end, assert_done),
            ),
            // RegionMigrationStart
            Step::setup(
                "Sets state to RegionMigrationStart",
                merge_before_test_fn(vec![
                    setup_state(Arc::new(|| Box::new(RegionMigrationStart))),
                    Arc::new(reset_volatile_ctx),
                ]),
            ),
            // RegionMigrationEnd
            // Note: We can't run this test multiple times;
            // the `peer_id`'s `DatanodeTable` will be removed after first-time migration success.
            Step::next(
                "Should be the region migration end(has been migrated)",
                None,
                Assertion::simple(assert_region_migration_end, assert_done),
            ),
        ];

        let steps = [steps.clone()].concat();
        let timer = Instant::now();

        // Run the table tests.
        let runner = ProcedureMigrationSuiteRunner::new(suite)
            .steps(steps.clone())
            .run_once()
            .await;

        // Ensure it didn't run into the slow path.
        assert!(timer.elapsed().as_secs() < REGION_LEASE_SECS);
        runner.suite.verify_table_metadata().await;
    }
}
