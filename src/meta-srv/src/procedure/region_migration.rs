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

pub(crate) mod migration_end;
pub(crate) mod migration_start;

use std::fmt::Debug;

use common_meta::peer::Peer;
use common_procedure::error::{
    Error as ProcedureError, FromJsonSnafu, Result as ProcedureResult, ToJsonSnafu,
};
use common_procedure::{Context as ProcedureContext, LockKey, Procedure, Status};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::storage::RegionId;

use self::migration_start::RegionMigrationStart;
use crate::error::{Error, Result};

/// It's shared in each step and available even after recovering.
///
/// It will only be updated/stored after the Red node has succeeded.
///
/// **Notes: Stores with too large data in the context might incur replication overhead.**
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistentContext {
    /// The [Peer] of migration source.
    from_peer: Peer,
    /// The [Peer] of migration destination.
    to_peer: Option<Peer>,
    /// Closes the migrated region on the `from_peer`.
    close_migrated_region: bool,
    /// The [RegionId] of migration region.
    region_id: RegionId,
}

impl PersistentContext {
    pub fn lock_key(&self) -> String {
        format!(
            "{}/{}",
            self.region_id.table_id(),
            self.region_id.region_number()
        )
    }
}

/// It's shared in each step and available in executing (including retrying).
///
/// It will be dropped if the procedure runner crashes.
///
/// The additional remote fetches are only required in the worst cases.
#[derive(Debug, Clone, Default)]
pub struct VolatileContext {}

/// The context of procedure execution.
#[derive(Debug, Clone)]
pub struct Context {}

#[async_trait::async_trait]
#[typetag::serde(tag = "region_migration_state")]
trait State: Sync + Send + Debug {
    /// Yields the next state.
    async fn next(
        &mut self,
        ctx: &Context,
        pc: &mut PersistentContext,
        vc: &mut VolatileContext,
    ) -> Result<Box<dyn State>>;

    /// Indicates the procedure execution status of the `State`.
    fn status(&self) -> Status {
        Status::Executing { persist: true }
    }
}

/// Persistent data of [RegionMigrationProcedure].
#[derive(Debug, Serialize, Deserialize)]
pub struct RegionMigrationData {
    context: PersistentContext,
    state: Box<dyn State>,
}

#[derive(Debug)]
pub struct RegionMigrationProcedure {
    data: RegionMigrationData,
    context: Context,
    volatile_context: VolatileContext,
}

// TODO(weny): remove it.
#[allow(dead_code)]
impl RegionMigrationProcedure {
    const TYPE_NAME: &str = "metasrv-procedure::RegionMigration";

    pub fn new(persistent_context: PersistentContext, context: Context) -> Self {
        let state = Box::new(RegionMigrationStart {});
        Self::new_inner(state, persistent_context, context)
    }

    fn new_inner(
        state: Box<dyn State>,
        persistent_context: PersistentContext,
        context: Context,
    ) -> Self {
        Self {
            data: RegionMigrationData {
                context: persistent_context,
                state,
            },
            context,
            volatile_context: VolatileContext::default(),
        }
    }

    fn from_json(json: &str, context: Context) -> ProcedureResult<Self> {
        let data: RegionMigrationData = serde_json::from_str(json).context(FromJsonSnafu)?;

        Ok(Self {
            data,
            context,
            volatile_context: VolatileContext::default(),
        })
    }
}

#[async_trait::async_trait]
impl Procedure for RegionMigrationProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        let data = &mut self.data;
        let state = &mut data.state;
        let persistent_context = &mut data.context;
        let volatile_context = &mut self.volatile_context;

        *state = state
            .next(&self.context, persistent_context, volatile_context)
            .await
            .map_err(|e| {
                if matches!(e, Error::RetryLater { .. }) {
                    ProcedureError::retry_later(e)
                } else {
                    ProcedureError::external(e)
                }
            })?;
        Ok(state.status())
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        LockKey::single(self.data.context.lock_key())
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::sync::Arc;

    use common_procedure::ProcedureId;
    use common_procedure_test::MockContextProvider;

    use super::migration_end::RegionMigrationEnd;
    use super::*;

    fn persistent_context_factory() -> PersistentContext {
        PersistentContext {
            from_peer: Peer::empty(1),
            to_peer: None,
            close_migrated_region: false,
            region_id: RegionId::new(1024, 1),
        }
    }

    fn context_factory() -> Context {
        Context {}
    }

    fn procedure_context_factory() -> ProcedureContext {
        ProcedureContext {
            procedure_id: ProcedureId::random(),
            provider: Arc::new(MockContextProvider::default()),
        }
    }

    #[test]
    fn test_lock_key() {
        let persistent_context = persistent_context_factory();
        let expected_key = persistent_context.lock_key();

        let context = context_factory();

        let procedure = RegionMigrationProcedure::new(persistent_context, context);

        let key = procedure.lock_key();
        let keys = key.keys_to_lock().cloned().collect::<Vec<_>>();

        assert!(keys.contains(&expected_key));
    }

    #[test]
    fn test_data_serialization() {
        let persistent_context = persistent_context_factory();

        let context = context_factory();

        let procedure = RegionMigrationProcedure::new(persistent_context, context);

        let serialized = procedure.dump().unwrap();

        let expected = r#"{"context":{"from_peer":{"id":1,"addr":""},"to_peer":null,"close_migrated_region":false,"region_id":4398046511105},"state":{"region_migration_state":"RegionMigrationStart"}}"#;
        assert_eq!(expected, serialized);
    }

    #[derive(Debug, Serialize, Deserialize, Default)]
    pub struct MockState {
        count: usize,
    }

    #[async_trait::async_trait]
    #[typetag::serde]
    impl State for MockState {
        async fn next(
            &mut self,
            _: &Context,
            _: &mut PersistentContext,
            _: &mut VolatileContext,
        ) -> Result<Box<dyn State>> {
            if self.count == 2 {
                Ok(Box::new(RegionMigrationEnd))
            } else {
                Ok(Box::new(MockState {
                    count: self.count + 1,
                }))
            }
        }
    }

    #[tokio::test]
    async fn test_execution_after_deserialized() {
        fn new_mock_procedure() -> RegionMigrationProcedure {
            let persistent_context = persistent_context_factory();
            let context = context_factory();
            let state = Box::<MockState>::default();
            RegionMigrationProcedure::new_inner(state, persistent_context, context)
        }

        let ctx = procedure_context_factory();
        let mut procedure = new_mock_procedure();
        let mut status = None;
        for _ in 0..3 {
            status = Some(procedure.execute(&ctx).await.unwrap());
        }
        assert_matches!(status.unwrap(), Status::Done);

        let ctx = procedure_context_factory();
        let mut procedure = new_mock_procedure();

        status = Some(procedure.execute(&ctx).await.unwrap());

        let serialized = procedure.dump().unwrap();

        let context = context_factory();
        let mut procedure = RegionMigrationProcedure::from_json(&serialized, context).unwrap();

        for _ in 1..3 {
            status = Some(procedure.execute(&ctx).await.unwrap());
        }
        assert_matches!(status.unwrap(), Status::Done);
    }
}
