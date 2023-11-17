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
pub(crate) mod migration_end;
pub(crate) mod migration_start;
pub(crate) mod open_candidate_region;
#[cfg(test)]
pub(crate) mod test_util;
pub(crate) mod update_metadata;

use std::any::Any;
use std::fmt::Debug;

use common_meta::key::table_route::TableRouteValue;
use common_meta::key::{DeserializedValueWithBytes, TableMetadataManagerRef};
use common_meta::peer::Peer;
use common_meta::ClusterId;
use common_procedure::error::{
    Error as ProcedureError, FromJsonSnafu, Result as ProcedureResult, ToJsonSnafu,
};
use common_procedure::{Context as ProcedureContext, LockKey, Procedure, Status};
use serde::{Deserialize, Serialize};
use snafu::{location, Location, OptionExt, ResultExt};
use store_api::storage::RegionId;

use self::migration_start::RegionMigrationStart;
use crate::error::{self, Error, Result};
use crate::procedure::utils::region_lock_key;
use crate::region::lease_keeper::{OpeningRegionGuard, OpeningRegionKeeperRef};
use crate::service::mailbox::MailboxRef;

/// It's shared in each step and available even after recovering.
///
/// It will only be updated/stored after the Red node has succeeded.
///
/// **Notes: Stores with too large data in the context might incur replication overhead.**
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistentContext {
    /// The Id of the cluster.
    cluster_id: ClusterId,
    /// The [Peer] of migration source.
    from_peer: Peer,
    /// The [Peer] of migration destination.
    to_peer: Peer,
    /// The [RegionId] of migration region.
    region_id: RegionId,
}

impl PersistentContext {
    pub fn lock_key(&self) -> String {
        region_lock_key(self.region_id.table_id(), self.region_id.region_number())
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
    opening_region_guard: Option<OpeningRegionGuard>,
    /// `table_route_info` is stored via previous steps for future use.
    table_route_info: Option<DeserializedValueWithBytes<TableRouteValue>>,
}

/// Used to generate new [Context].
pub trait ContextFactory {
    fn new_context(self, persistent_ctx: PersistentContext) -> Context;
}

/// Default implementation.
pub struct ContextFactoryImpl {
    volatile_ctx: VolatileContext,
    table_metadata_manager: TableMetadataManagerRef,
    opening_region_keeper: OpeningRegionKeeperRef,
    mailbox: MailboxRef,
    server_addr: String,
}

impl ContextFactory for ContextFactoryImpl {
    fn new_context(self, persistent_ctx: PersistentContext) -> Context {
        Context {
            persistent_ctx,
            volatile_ctx: self.volatile_ctx,
            table_metadata_manager: self.table_metadata_manager,
            opening_region_keeper: self.opening_region_keeper,
            mailbox: self.mailbox,
            server_addr: self.server_addr,
        }
    }
}

// TODO(weny): remove it.
#[allow(dead_code)]
/// The context of procedure execution.
pub struct Context {
    persistent_ctx: PersistentContext,
    volatile_ctx: VolatileContext,
    table_metadata_manager: TableMetadataManagerRef,
    opening_region_keeper: OpeningRegionKeeperRef,
    mailbox: MailboxRef,
    server_addr: String,
}

impl Context {
    /// Returns address of meta server.
    pub fn server_addr(&self) -> &str {
        &self.server_addr
    }

    /// Returns the `table_route_value` of [VolatileContext] if any.
    /// Otherwise, returns the value retrieved from remote.
    ///
    /// Retry:
    /// - Failed to retrieve the metadata of table.
    pub async fn get_table_route_value(
        &mut self,
    ) -> Result<&DeserializedValueWithBytes<TableRouteValue>> {
        let table_route_value = &mut self.volatile_ctx.table_route_info;

        if table_route_value.is_none() {
            let table_id = self.persistent_ctx.region_id.table_id();
            let table_route = self
                .table_metadata_manager
                .table_route_manager()
                .get(table_id)
                .await
                .context(error::TableMetadataManagerSnafu)
                .map_err(|e| error::Error::RetryLater {
                    reason: e.to_string(),
                    location: location!(),
                })?
                .context(error::TableRouteNotFoundSnafu { table_id })?;

            *table_route_value = Some(table_route);
        }

        Ok(table_route_value.as_ref().unwrap())
    }

    /// Removes the `table_route_value` of [VolatileContext], returns true if any.
    pub fn remove_table_route_value(&mut self) -> bool {
        let value = self.volatile_ctx.table_route_info.take();
        value.is_some()
    }

    /// Returns the [RegionId].
    pub fn region_id(&self) -> RegionId {
        self.persistent_ctx.region_id
    }
}

#[async_trait::async_trait]
#[typetag::serde(tag = "region_migration_state")]
trait State: Sync + Send + Debug {
    /// Yields the next state.
    async fn next(&mut self, ctx: &mut Context) -> Result<Box<dyn State>>;

    /// Indicates the procedure execution status of the `State`.
    fn status(&self) -> Status {
        Status::Executing { persist: true }
    }

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

pub struct RegionMigrationProcedure {
    state: Box<dyn State>,
    context: Context,
}

// TODO(weny): remove it.
#[allow(dead_code)]
impl RegionMigrationProcedure {
    const TYPE_NAME: &str = "metasrv-procedure::RegionMigration";

    pub fn new(
        persistent_context: PersistentContext,
        context_factory: impl ContextFactory,
    ) -> Self {
        let state = Box::new(RegionMigrationStart {});
        Self::new_inner(state, persistent_context, context_factory)
    }

    fn new_inner(
        state: Box<dyn State>,
        persistent_context: PersistentContext,
        context_factory: impl ContextFactory,
    ) -> Self {
        Self {
            state,
            context: context_factory.new_context(persistent_context),
        }
    }

    fn from_json(json: &str, context_factory: impl ContextFactory) -> ProcedureResult<Self> {
        let RegionMigrationDataOwned {
            persistent_ctx,
            state,
        } = serde_json::from_str(json).context(FromJsonSnafu)?;

        let context = context_factory.new_context(persistent_ctx);

        Ok(Self { state, context })
    }
}

#[async_trait::async_trait]
impl Procedure for RegionMigrationProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        let state = &mut self.state;

        *state = state.next(&mut self.context).await.map_err(|e| {
            if matches!(e, Error::RetryLater { .. }) {
                ProcedureError::retry_later(e)
            } else {
                ProcedureError::external(e)
            }
        })?;
        Ok(state.status())
    }

    fn dump(&self) -> ProcedureResult<String> {
        let data = RegionMigrationData {
            state: self.state.as_ref(),
            persistent_ctx: &self.context.persistent_ctx,
        };
        serde_json::to_string(&data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        let key = self.context.persistent_ctx.lock_key();
        LockKey::single(key)
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use super::migration_end::RegionMigrationEnd;
    use super::*;
    use crate::procedure::region_migration::test_util::TestingEnv;

    fn new_persistent_context() -> PersistentContext {
        PersistentContext {
            from_peer: Peer::empty(1),
            to_peer: Peer::empty(2),
            region_id: RegionId::new(1024, 1),
            cluster_id: 0,
        }
    }

    #[test]
    fn test_lock_key() {
        let persistent_context = new_persistent_context();
        let expected_key = persistent_context.lock_key();

        let env = TestingEnv::new();
        let context = env.context_factory();

        let procedure = RegionMigrationProcedure::new(persistent_context, context);

        let key = procedure.lock_key();
        let keys = key.keys_to_lock().cloned().collect::<Vec<_>>();

        assert!(keys.contains(&expected_key));
    }

    #[test]
    fn test_data_serialization() {
        let persistent_context = new_persistent_context();

        let env = TestingEnv::new();
        let context = env.context_factory();

        let procedure = RegionMigrationProcedure::new(persistent_context, context);

        let serialized = procedure.dump().unwrap();

        let expected = r#"{"persistent_ctx":{"cluster_id":0,"from_peer":{"id":1,"addr":""},"to_peer":{"id":2,"addr":""},"region_id":4398046511105},"state":{"region_migration_state":"RegionMigrationStart"}}"#;
        assert_eq!(expected, serialized);
    }

    #[derive(Debug, Serialize, Deserialize, Default)]
    pub struct MockState;

    #[async_trait::async_trait]
    #[typetag::serde]
    impl State for MockState {
        async fn next(&mut self, ctx: &mut Context) -> Result<Box<dyn State>> {
            let pc = &mut ctx.persistent_ctx;

            if pc.cluster_id == 2 {
                Ok(Box::new(RegionMigrationEnd))
            } else {
                pc.cluster_id += 1;
                Ok(Box::new(MockState))
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
            RegionMigrationProcedure::new_inner(state, persistent_context, context_factory)
        }

        let ctx = TestingEnv::procedure_context();
        let mut procedure = new_mock_procedure(&env);
        let mut status = None;
        for _ in 0..3 {
            status = Some(procedure.execute(&ctx).await.unwrap());
        }
        assert_matches!(status.unwrap(), Status::Done);

        let ctx = TestingEnv::procedure_context();
        let mut procedure = new_mock_procedure(&env);

        status = Some(procedure.execute(&ctx).await.unwrap());

        let serialized = procedure.dump().unwrap();

        let context_factory = env.context_factory();
        let mut procedure =
            RegionMigrationProcedure::from_json(&serialized, context_factory).unwrap();

        for _ in 1..3 {
            status = Some(procedure.execute(&ctx).await.unwrap());
        }
        assert_eq!(procedure.context.persistent_ctx.cluster_id, 2);
        assert_matches!(status.unwrap(), Status::Done);
    }
}
