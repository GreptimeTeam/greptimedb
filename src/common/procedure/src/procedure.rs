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

use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use smallvec::{smallvec, SmallVec};
use snafu::{ResultExt, Snafu};
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::watcher::Watcher;

/// Procedure execution status.
#[derive(Debug)]
pub enum Status {
    /// The procedure is still executing.
    Executing {
        /// Whether the framework needs to persist the procedure.
        persist: bool,
    },
    /// The procedure has suspended itself and is waiting for subprocedures.
    Suspended {
        subprocedures: Vec<ProcedureWithId>,
        /// Whether the framework needs to persist the procedure.
        persist: bool,
    },
    /// the procedure is done.
    Done,
}

impl Status {
    /// Returns a [Status::Executing] with given `persist` flag.
    pub fn executing(persist: bool) -> Status {
        Status::Executing { persist }
    }

    /// Returns `true` if the procedure needs the framework to persist its intermediate state.
    pub fn need_persist(&self) -> bool {
        // If the procedure is done, the framework doesn't need to persist the procedure
        // anymore. It only needs to mark the procedure as committed.
        match self {
            Status::Executing { persist } | Status::Suspended { persist, .. } => *persist,
            Status::Done => false,
        }
    }
}

/// [ContextProvider] provides information about procedures in the [ProcedureManager].
#[async_trait]
pub trait ContextProvider: Send + Sync {
    /// Query the procedure state.
    async fn procedure_state(&self, procedure_id: ProcedureId) -> Result<Option<ProcedureState>>;
}

/// Reference-counted pointer to [ContextProvider].
pub type ContextProviderRef = Arc<dyn ContextProvider>;

/// Procedure execution context.
#[derive(Clone)]
pub struct Context {
    /// Id of the procedure.
    pub procedure_id: ProcedureId,
    /// [ProcedureManager] context provider.
    pub provider: ContextProviderRef,
}

/// A `Procedure` represents an operation or a set of operations to be performed step-by-step.
#[async_trait]
pub trait Procedure: Send + Sync {
    /// Type name of the procedure.
    fn type_name(&self) -> &str;

    /// Execute the procedure.
    ///
    /// The implementation must be idempotent.
    async fn execute(&mut self, ctx: &Context) -> Result<Status>;

    /// Dump the state of the procedure to a string.
    fn dump(&self) -> Result<String>;

    /// Returns the [LockKey] that this procedure needs to acquire.
    fn lock_key(&self) -> LockKey;
}

#[async_trait]
impl<T: Procedure + ?Sized> Procedure for Box<T> {
    fn type_name(&self) -> &str {
        (**self).type_name()
    }

    async fn execute(&mut self, ctx: &Context) -> Result<Status> {
        (**self).execute(ctx).await
    }

    fn dump(&self) -> Result<String> {
        (**self).dump()
    }

    fn lock_key(&self) -> LockKey {
        (**self).lock_key()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum StringKey {
    Share(String),
    Exclusive(String),
}

/// Keys to identify required locks.
///
/// [LockKey] always sorts keys lexicographically so that they can be acquired
/// in the same order.
/// Most procedures should only acquire 1 ~ 2 locks so we use smallvec to hold keys.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LockKey(SmallVec<[StringKey; 2]>);

impl StringKey {
    pub fn into_string(self) -> String {
        match self {
            StringKey::Share(s) => s,
            StringKey::Exclusive(s) => s,
        }
    }

    pub fn as_string(&self) -> &String {
        match self {
            StringKey::Share(s) => s,
            StringKey::Exclusive(s) => s,
        }
    }
}

impl LockKey {
    /// Returns a new [LockKey] with only one key.
    pub fn single(key: impl Into<StringKey>) -> LockKey {
        LockKey(smallvec![key.into()])
    }

    /// Returns a new [LockKey] with only one key.
    pub fn single_exclusive(key: impl Into<String>) -> LockKey {
        LockKey(smallvec![StringKey::Exclusive(key.into())])
    }

    /// Returns a new [LockKey] with keys from specific `iter`.
    pub fn new(iter: impl IntoIterator<Item = StringKey>) -> LockKey {
        let mut vec: SmallVec<_> = iter.into_iter().collect();
        vec.sort();
        // Dedup keys to avoid acquiring the same key multiple times.
        vec.dedup();
        LockKey(vec)
    }

    /// Returns a new [LockKey] with keys from specific `iter`.
    pub fn new_exclusive(iter: impl IntoIterator<Item = String>) -> LockKey {
        Self::new(iter.into_iter().map(StringKey::Exclusive))
    }

    /// Returns the keys to lock.
    pub fn keys_to_lock(&self) -> impl Iterator<Item = &StringKey> {
        self.0.iter()
    }
}

/// Boxed [Procedure].
pub type BoxedProcedure = Box<dyn Procedure>;

/// A procedure with specific id.
pub struct ProcedureWithId {
    /// Id of the procedure.
    pub id: ProcedureId,
    pub procedure: BoxedProcedure,
}

impl ProcedureWithId {
    /// Returns a new [ProcedureWithId] that holds specific `procedure`
    /// and a random [ProcedureId].
    pub fn with_random_id(procedure: BoxedProcedure) -> ProcedureWithId {
        ProcedureWithId {
            id: ProcedureId::random(),
            procedure,
        }
    }
}

impl fmt::Debug for ProcedureWithId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.procedure.type_name(), self.id)
    }
}

#[derive(Debug, Snafu)]
pub struct ParseIdError {
    source: uuid::Error,
}

/// Unique id for [Procedure].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProcedureId(Uuid);

impl ProcedureId {
    /// Returns a new unique [ProcedureId] randomly.
    pub fn random() -> ProcedureId {
        ProcedureId(Uuid::new_v4())
    }

    /// Parses id from string.
    pub fn parse_str(input: &str) -> std::result::Result<ProcedureId, ParseIdError> {
        Uuid::parse_str(input)
            .map(ProcedureId)
            .context(ParseIdSnafu)
    }
}

impl fmt::Display for ProcedureId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for ProcedureId {
    type Err = ParseIdError;

    fn from_str(s: &str) -> std::result::Result<ProcedureId, ParseIdError> {
        ProcedureId::parse_str(s)
    }
}

/// Loader to recover the [Procedure] instance from serialized data.
pub type BoxedProcedureLoader = Box<dyn Fn(&str) -> Result<BoxedProcedure> + Send>;

/// State of a submitted procedure.
#[derive(Debug, Default, Clone)]
pub enum ProcedureState {
    /// The procedure is running.
    #[default]
    Running,
    /// The procedure is finished.
    Done,
    /// The procedure is failed and can be retried.
    Retrying { error: Arc<Error> },
    /// The procedure is failed and cannot proceed anymore.
    Failed { error: Arc<Error> },
}

impl ProcedureState {
    /// Returns a [ProcedureState] with failed state.
    pub fn failed(error: Arc<Error>) -> ProcedureState {
        ProcedureState::Failed { error }
    }

    /// Returns a [ProcedureState] with retrying state.
    pub fn retrying(error: Arc<Error>) -> ProcedureState {
        ProcedureState::Retrying { error }
    }

    /// Returns true if the procedure state is running.
    pub fn is_running(&self) -> bool {
        matches!(self, ProcedureState::Running)
    }

    /// Returns true if the procedure state is done.
    pub fn is_done(&self) -> bool {
        matches!(self, ProcedureState::Done)
    }

    /// Returns true if the procedure state failed.
    pub fn is_failed(&self) -> bool {
        matches!(self, ProcedureState::Failed { .. })
    }

    /// Returns true if the procedure state is retrying.
    pub fn is_retrying(&self) -> bool {
        matches!(self, ProcedureState::Retrying { .. })
    }

    /// Returns the error.
    pub fn error(&self) -> Option<&Arc<Error>> {
        match self {
            ProcedureState::Failed { error } => Some(error),
            ProcedureState::Retrying { error } => Some(error),
            _ => None,
        }
    }
}

// TODO(yingwen): Shutdown
/// `ProcedureManager` executes [Procedure] submitted to it.
#[async_trait]
pub trait ProcedureManager: Send + Sync + 'static {
    /// Registers loader for specific procedure type `name`.
    fn register_loader(&self, name: &str, loader: BoxedProcedureLoader) -> Result<()>;

    /// Starts the background GC task.
    ///
    /// Recovers unfinished procedures and reruns them.
    ///
    /// Callers should ensure all loaders are registered.
    async fn start(&self) -> Result<()>;

    /// Stops the background GC task.
    async fn stop(&self) -> Result<()>;

    /// Submits a procedure to execute.
    ///
    /// Returns a [Watcher] to watch the created procedure.
    async fn submit(&self, procedure: ProcedureWithId) -> Result<Watcher>;

    /// Query the procedure state.
    ///
    /// Returns `Ok(None)` if the procedure doesn't exist.
    async fn procedure_state(&self, procedure_id: ProcedureId) -> Result<Option<ProcedureState>>;

    /// Returns a [Watcher] to watch [ProcedureState] of specific procedure.
    fn procedure_watcher(&self, procedure_id: ProcedureId) -> Option<Watcher>;
}

/// Ref-counted pointer to the [ProcedureManager].
pub type ProcedureManagerRef = Arc<dyn ProcedureManager>;

#[cfg(test)]
mod tests {
    use common_error::mock::MockError;
    use common_error::status_code::StatusCode;

    use super::*;

    #[test]
    fn test_status() {
        let status = Status::Executing { persist: false };
        assert!(!status.need_persist());

        let status = Status::Executing { persist: true };
        assert!(status.need_persist());

        let status = Status::Suspended {
            subprocedures: Vec::new(),
            persist: false,
        };
        assert!(!status.need_persist());

        let status = Status::Suspended {
            subprocedures: Vec::new(),
            persist: true,
        };
        assert!(status.need_persist());

        let status = Status::Done;
        assert!(!status.need_persist());
    }

    #[test]
    fn test_lock_key() {
        let entity = "catalog.schema.my_table";
        let key = LockKey::single_exclusive(entity);
        assert_eq!(
            vec![&StringKey::Exclusive(entity.to_string())],
            key.keys_to_lock().collect::<Vec<_>>()
        );

        let key = LockKey::new_exclusive([
            "b".to_string(),
            "c".to_string(),
            "a".to_string(),
            "c".to_string(),
        ]);
        assert_eq!(
            vec![
                &StringKey::Exclusive("a".to_string()),
                &StringKey::Exclusive("b".to_string()),
                &StringKey::Exclusive("c".to_string())
            ],
            key.keys_to_lock().collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_procedure_id() {
        let id = ProcedureId::random();
        let uuid_str = id.to_string();
        assert_eq!(id.0.to_string(), uuid_str);

        let parsed = ProcedureId::parse_str(&uuid_str).unwrap();
        assert_eq!(id, parsed);
        let parsed = uuid_str.parse().unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn test_procedure_id_serialization() {
        let id = ProcedureId::random();
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(format!("\"{id}\""), json);

        let parsed = serde_json::from_str(&json).unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn test_procedure_state() {
        assert!(ProcedureState::Running.is_running());
        assert!(ProcedureState::Running.error().is_none());
        assert!(ProcedureState::Done.is_done());

        let state = ProcedureState::failed(Arc::new(Error::external(MockError::new(
            StatusCode::Unexpected,
        ))));
        assert!(state.is_failed());
        let _ = state.error().unwrap();
    }
}
