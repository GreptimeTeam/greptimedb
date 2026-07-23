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

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use common_error::ext::{BoxedError, ErrorExt};
use common_procedure::error::{FromJsonSnafu, Result as ProcedureResult, ToJsonSnafu};
use common_procedure::{
    Context as ProcedureContext, EventContext, EventTrigger, LockKey, Procedure, ProcedureId,
    Status,
};
use serde::{Deserialize, Serialize};
use serde_with::{DefaultOnNull, serde_as};
use snafu::{ResultExt, ensure};
use strum::AsRefStr;

use crate::ddl::DdlContext;
use crate::ddl::event::DatabaseDdlEvent;
use crate::ddl::utils::map_to_procedure_error;
use crate::error::{self, Result};
use crate::instruction::{CacheIdent, UserCacheIdent};
use crate::key::schema_name::{SchemaNameKey, SchemaNameValue};
use crate::lock_key::{CatalogLock, SchemaLock};
use crate::rpc::ddl::CreatorGrantIntent;

/// Describes the creator-access result of an atomic create.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AtomicCreateAccess {
    /// Added an exact `ALL` ACL entry for the new database.
    AddedExactAll,
    /// The creator already had effective `ALL` access, so their ACL was unchanged.
    AlreadyEffectiveAll,
}

/// Outcome of atomically creating database metadata and ensuring creator access.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AtomicCreateOutcome {
    /// This attempt created the database with the given access result.
    Created(AtomicCreateAccess),
    /// The same create procedure committed during an earlier attempt.
    AlreadyCommitted,
    /// The creator row changed concurrently and the operation should be retried.
    UserChanged,
    /// The creator is missing or no longer identifies the same account generation.
    UserMissing,
    /// A different value already exists for the database metadata.
    SchemaConflict,
}

/// Commits database metadata and any required creator access atomically.
///
/// Implementations classify conditional-write failures with [`AtomicCreateOutcome`] so the
/// procedure can retry without overwriting concurrent user changes.
#[async_trait]
pub trait CreateDatabaseMetadataCommitter: Send + Sync {
    async fn commit(
        &self,
        catalog: &str,
        schema: &str,
        value: &SchemaNameValue,
        creator: &CreatorGrantIntent,
    ) -> std::result::Result<AtomicCreateOutcome, BoxedError>;
}

/// Shared [`CreateDatabaseMetadataCommitter`].
pub type CreateDatabaseMetadataCommitterRef = Arc<dyn CreateDatabaseMetadataCommitter>;

/// Procedure that creates a database and optionally ensures its creator has access.
pub struct CreateDatabaseProcedure {
    pub context: DdlContext,
    pub data: CreateDatabaseData,
}

impl CreateDatabaseProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::CreateDatabase";

    pub fn new(
        catalog: String,
        schema: String,
        create_if_not_exists: bool,
        options: HashMap<String, String>,
        creator: Option<CreatorGrantIntent>,
        context: DdlContext,
    ) -> Self {
        Self {
            context,
            data: CreateDatabaseData {
                state: CreateDatabaseState::Prepare,
                catalog,
                schema,
                create_if_not_exists,
                options,
                creator,
            },
        }
    }

    pub fn from_json(json: &str, context: DdlContext) -> ProcedureResult<Self> {
        let data = serde_json::from_str(json).context(FromJsonSnafu)?;

        Ok(Self { context, data })
    }

    pub async fn on_prepare(&mut self) -> Result<Status> {
        let exists = self
            .context
            .table_metadata_manager
            .schema_manager()
            .exists(SchemaNameKey::new(&self.data.catalog, &self.data.schema))
            .await?;

        if exists && self.data.create_if_not_exists {
            return Ok(Status::done());
        }

        ensure!(
            !exists,
            error::SchemaAlreadyExistsSnafu {
                catalog: &self.data.catalog,
                schema: &self.data.schema,
            }
        );

        self.data.state = CreateDatabaseState::CreateMetadata;
        Ok(Status::executing(true))
    }

    pub async fn on_create_metadata(&mut self, procedure_id: ProcedureId) -> Result<Status> {
        let mut value: SchemaNameValue = (&self.data.options).try_into()?;

        if let Some(creator) = self.data.creator.as_ref() {
            // Distinguishes a replay of this procedure from a competing create.
            value.create_procedure_id = Some(procedure_id.to_string());
            let Some(committer) = self.context.create_database_metadata_committer.as_ref() else {
                return error::UnexpectedSnafu {
                    err_msg: "Create-database creator grant committer is not configured"
                        .to_string(),
                }
                .fail();
            };
            let _timer = crate::metrics::METRIC_META_CREATE_SCHEMA.start_timer();
            let outcome = match committer
                .commit(&self.data.catalog, &self.data.schema, &value, creator)
                .await
            {
                Ok(outcome) => outcome,
                Err(err) if err.is_retryable() => {
                    return Err(error::Error::RetryLater {
                        source: err,
                        clean_poisons: false,
                    });
                }
                Err(err) => return Err(err).context(error::ExternalSnafu),
            };

            match outcome {
                AtomicCreateOutcome::Created(_) => {
                    crate::metrics::METRIC_META_CREATE_SCHEMA_COUNTER.inc();
                }
                AtomicCreateOutcome::AlreadyCommitted => {}
                AtomicCreateOutcome::UserChanged => {
                    let source = error::UnexpectedSnafu {
                        err_msg: format!(
                            "User '{}' changed while creating database '{}.{}'",
                            creator.username, self.data.catalog, self.data.schema
                        ),
                    }
                    .build();
                    return Err(error::Error::retry_later(source));
                }
                AtomicCreateOutcome::UserMissing => {
                    return error::UnexpectedSnafu {
                        err_msg: format!(
                            "Creator '{}' no longer exists in catalog '{}'",
                            creator.username, self.data.catalog
                        ),
                    }
                    .fail();
                }
                AtomicCreateOutcome::SchemaConflict if self.data.create_if_not_exists => {
                    return Ok(Status::done());
                }
                AtomicCreateOutcome::SchemaConflict => {
                    return error::SchemaAlreadyExistsSnafu {
                        catalog: &self.data.catalog,
                        schema: &self.data.schema,
                    }
                    .fail();
                }
            }

            // The durable transaction is complete; retry cache invalidation separately.
            self.data.state = CreateDatabaseState::InvalidateCreatorCache;
            return Ok(Status::executing(true));
        }

        self.context
            .table_metadata_manager
            .schema_manager()
            .create(
                SchemaNameKey::new(&self.data.catalog, &self.data.schema),
                Some(value),
                self.data.create_if_not_exists,
            )
            .await?;

        Ok(Status::done())
    }

    pub async fn on_invalidate_creator_cache(&mut self) -> Result<Status> {
        let Some(creator) = self.data.creator.as_ref() else {
            return Ok(Status::done());
        };

        let ident = CacheIdent::User(UserCacheIdent {
            catalog: self.data.catalog.clone(),
            username: creator.username.clone(),
        });
        let ctx = crate::cache_invalidator::Context {
            subject: Some(format!(
                "Invalidate creator cache after creating database '{}.{}' for '{}'",
                self.data.catalog, self.data.schema, creator.username
            )),
        };
        if let Err(source) = self
            .context
            .cache_invalidator
            .invalidate(&ctx, &[ident])
            .await
        {
            return Err(error::Error::retry_later(source));
        }

        Ok(Status::done())
    }
}

#[async_trait]
impl Procedure for CreateDatabaseProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, ctx: &ProcedureContext) -> ProcedureResult<Status> {
        let state = &self.data.state;

        match state {
            CreateDatabaseState::Prepare => self.on_prepare().await,
            CreateDatabaseState::CreateMetadata => self.on_create_metadata(ctx.procedure_id).await,
            CreateDatabaseState::InvalidateCreatorCache => self.on_invalidate_creator_cache().await,
        }
        .map_err(map_to_procedure_error)
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        let lock_key = vec![
            CatalogLock::Read(&self.data.catalog).into(),
            SchemaLock::write(&self.data.catalog, &self.data.schema).into(),
        ];

        LockKey::new(lock_key)
    }

    fn event(&self, ctx: &EventContext<'_>) -> Option<Box<dyn common_event_recorder::Event>> {
        let event = if matches!(&ctx.trigger, EventTrigger::Submitted) {
            DatabaseDdlEvent::create_submitted(
                &self.data.catalog,
                &self.data.schema,
                self.data.create_if_not_exists,
                &self.data.options,
            )
        } else {
            DatabaseDdlEvent::create_lifecycle()
        };
        Some(Box::new(event))
    }
}

/// Persistent states of [`CreateDatabaseProcedure`].
#[derive(Debug, Clone, Serialize, Deserialize, AsRefStr)]
pub enum CreateDatabaseState {
    /// Checks whether the database already exists.
    Prepare,
    /// Creates metadata and, when supplied, ensures the creator has access atomically.
    CreateMetadata,
    /// Invalidates the creator's cached user data after the durable commit.
    InvalidateCreatorCache,
}

/// Persistent data of [`CreateDatabaseProcedure`].
#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
pub struct CreateDatabaseData {
    pub state: CreateDatabaseState,
    pub catalog: String,
    pub schema: String,
    pub create_if_not_exists: bool,
    #[serde_as(deserialize_as = "DefaultOnNull")]
    pub options: HashMap<String, String>,
    /// Authenticated creator whose access is ensured, absent for legacy schema-only requests.
    #[serde(default)]
    pub creator: Option<CreatorGrantIntent>,
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use common_error::ext::ErrorExt;

    use super::*;
    use crate::cache_invalidator::{CacheInvalidator, Context};
    use crate::test_util::{MockDatanodeManager, new_ddl_context};

    const PROCEDURE_ID: &str = "4ee0ba94-11f0-4d4d-9468-5ebf732e3ab2";

    fn procedure_id() -> ProcedureId {
        ProcedureId::parse_str(PROCEDURE_ID).unwrap()
    }

    struct MockCommitter {
        calls: AtomicUsize,
        outcomes: Mutex<VecDeque<std::result::Result<AtomicCreateOutcome, BoxedError>>>,
    }

    impl MockCommitter {
        fn new(
            outcomes: impl IntoIterator<Item = std::result::Result<AtomicCreateOutcome, BoxedError>>,
        ) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                outcomes: Mutex::new(outcomes.into_iter().collect()),
            }
        }
    }

    #[async_trait]
    impl CreateDatabaseMetadataCommitter for MockCommitter {
        async fn commit(
            &self,
            _catalog: &str,
            _schema: &str,
            value: &SchemaNameValue,
            _creator: &CreatorGrantIntent,
        ) -> std::result::Result<AtomicCreateOutcome, BoxedError> {
            assert_eq!(value.create_procedure_id.as_deref(), Some(PROCEDURE_ID));
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.outcomes
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected commit call")
        }
    }

    #[derive(Default)]
    struct FailOnceInvalidator {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl CacheInvalidator for FailOnceInvalidator {
        async fn invalidate(&self, _ctx: &Context, _caches: &[CacheIdent]) -> Result<()> {
            if self.calls.fetch_add(1, Ordering::Relaxed) == 0 {
                return error::UnexpectedSnafu {
                    err_msg: "injected cache invalidation failure",
                }
                .fail();
            }
            Ok(())
        }

        fn invalidate_all(&self) -> Result<()> {
            Ok(())
        }
    }

    fn procedure(
        outcomes: impl IntoIterator<Item = std::result::Result<AtomicCreateOutcome, BoxedError>>,
        cache_invalidator: Arc<dyn CacheInvalidator>,
    ) -> (CreateDatabaseProcedure, Arc<MockCommitter>) {
        let mut context = new_ddl_context(Arc::new(MockDatanodeManager::new(())));
        context.cache_invalidator = cache_invalidator;
        let committer = Arc::new(MockCommitter::new(outcomes));
        context.create_database_metadata_committer = Some(committer.clone());
        let procedure = CreateDatabaseProcedure::new(
            "greptime".to_string(),
            "metrics".to_string(),
            false,
            HashMap::new(),
            Some(CreatorGrantIntent {
                username: "alice".to_string(),
                created_at_ns: 1,
            }),
            context,
        );
        (procedure, committer)
    }

    #[tokio::test]
    async fn created_and_recovered_commits_retry_only_cache_invalidation() {
        let invalidator = Arc::new(FailOnceInvalidator::default());
        let (mut procedure, committer) = procedure(
            [Ok(AtomicCreateOutcome::Created(
                AtomicCreateAccess::AddedExactAll,
            ))],
            invalidator.clone(),
        );
        procedure.data.state = CreateDatabaseState::CreateMetadata;

        assert!(
            !procedure
                .on_create_metadata(procedure_id())
                .await
                .unwrap()
                .is_done()
        );
        assert!(matches!(
            procedure.data.state,
            CreateDatabaseState::InvalidateCreatorCache
        ));
        assert_eq!(committer.calls.load(Ordering::Relaxed), 1);

        assert!(
            procedure
                .on_invalidate_creator_cache()
                .await
                .unwrap_err()
                .is_retryable()
        );
        assert!(
            procedure
                .on_invalidate_creator_cache()
                .await
                .unwrap()
                .is_done()
        );
        assert_eq!(committer.calls.load(Ordering::Relaxed), 1);
        assert_eq!(invalidator.calls.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn user_change_is_explicitly_retryable() {
        let (mut procedure, committer) = procedure(
            [Ok(AtomicCreateOutcome::UserChanged)],
            Arc::new(FailOnceInvalidator::default()),
        );
        procedure.data.state = CreateDatabaseState::CreateMetadata;

        let err = procedure
            .on_create_metadata(procedure_id())
            .await
            .unwrap_err();
        assert!(err.is_retryable());
        assert!(matches!(
            procedure.data.state,
            CreateDatabaseState::CreateMetadata
        ));
        assert_eq!(committer.calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn lost_commit_response_replays_as_already_committed() {
        let (mut procedure, committer) = procedure(
            [
                Err(BoxedError::new(
                    error::ElectionLeaderLeaseChangedSnafu.build(),
                )),
                Ok(AtomicCreateOutcome::AlreadyCommitted),
            ],
            Arc::new(FailOnceInvalidator::default()),
        );
        procedure.data.state = CreateDatabaseState::CreateMetadata;

        let err = procedure
            .on_create_metadata(procedure_id())
            .await
            .unwrap_err();
        assert!(matches!(err, error::Error::RetryLater { .. }));
        assert!(matches!(
            procedure.data.state,
            CreateDatabaseState::CreateMetadata
        ));
        assert!(
            !procedure
                .on_create_metadata(procedure_id())
                .await
                .unwrap()
                .is_done()
        );
        assert!(matches!(
            procedure.data.state,
            CreateDatabaseState::InvalidateCreatorCache
        ));
        assert_eq!(committer.calls.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn lost_state_transition_recovers_committed_business_state() {
        let (mut procedure, committer) = procedure(
            [
                Ok(AtomicCreateOutcome::Created(
                    AtomicCreateAccess::AddedExactAll,
                )),
                Ok(AtomicCreateOutcome::AlreadyCommitted),
            ],
            Arc::new(FailOnceInvalidator::default()),
        );
        let context = procedure.context.clone();
        procedure.data.state = CreateDatabaseState::CreateMetadata;
        let stale_json = procedure.dump().unwrap();

        assert!(
            !procedure
                .on_create_metadata(procedure_id())
                .await
                .unwrap()
                .is_done()
        );
        assert!(matches!(
            procedure.data.state,
            CreateDatabaseState::InvalidateCreatorCache
        ));

        let mut recovered = CreateDatabaseProcedure::from_json(&stale_json, context).unwrap();
        assert_eq!(recovered.data.creator.as_ref().unwrap().username, "alice");
        assert!(
            !recovered
                .on_create_metadata(procedure_id())
                .await
                .unwrap()
                .is_done()
        );
        assert!(matches!(
            recovered.data.state,
            CreateDatabaseState::InvalidateCreatorCache
        ));
        assert_eq!(committer.calls.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn missing_creator_uses_legacy_schema_path() {
        let mut context = new_ddl_context(Arc::new(MockDatanodeManager::new(())));
        let committer = Arc::new(MockCommitter::new([Ok(AtomicCreateOutcome::Created(
            AtomicCreateAccess::AddedExactAll,
        ))]));
        context.create_database_metadata_committer = Some(committer.clone());
        let mut procedure = CreateDatabaseProcedure::new(
            "greptime".to_string(),
            "legacy".to_string(),
            false,
            HashMap::new(),
            None,
            context,
        );
        procedure.data.state = CreateDatabaseState::CreateMetadata;

        assert!(
            procedure
                .on_create_metadata(procedure_id())
                .await
                .unwrap()
                .is_done()
        );
        assert_eq!(committer.calls.load(Ordering::Relaxed), 0);
        assert!(
            procedure
                .context
                .table_metadata_manager
                .schema_manager()
                .exists(SchemaNameKey::new("greptime", "legacy"))
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn creator_without_committer_fails_closed() {
        let context = new_ddl_context(Arc::new(MockDatanodeManager::new(())));
        let mut procedure = CreateDatabaseProcedure::new(
            "greptime".to_string(),
            "missing_committer".to_string(),
            false,
            HashMap::new(),
            Some(CreatorGrantIntent {
                username: "alice".to_string(),
                created_at_ns: 1,
            }),
            context,
        );
        procedure.data.state = CreateDatabaseState::CreateMetadata;

        assert!(procedure.on_create_metadata(procedure_id()).await.is_err());
        assert!(
            !procedure
                .context
                .table_metadata_manager
                .schema_manager()
                .exists(SchemaNameKey::new("greptime", "missing_committer"))
                .await
                .unwrap()
        );
    }

    #[test]
    fn legacy_procedure_json_defaults_creator() {
        let context = new_ddl_context(Arc::new(MockDatanodeManager::new(())));
        let procedure = CreateDatabaseProcedure::from_json(
            r#"{
                "state":"CreateMetadata",
                "catalog":"greptime",
                "schema":"metrics",
                "create_if_not_exists":false,
                "options":{}
            }"#,
            context,
        )
        .unwrap();

        assert!(procedure.data.creator.is_none());
    }
}
