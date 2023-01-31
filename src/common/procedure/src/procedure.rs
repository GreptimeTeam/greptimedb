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
use snafu::{ResultExt, Snafu};
use uuid::Uuid;

use crate::error::Result;

/// Procedure execution status.
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

/// Procedure execution context.
#[derive(Debug)]
pub struct Context {
    /// Id of the procedure.
    pub procedure_id: ProcedureId,
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

    /// Returns the [LockKey] if this procedure needs to acquire lock.
    fn lock_key(&self) -> Option<LockKey>;
}

/// A key to identify the lock.
// We might hold multiple keys in this struct. When there are multiple keys, we need to sort the
// keys lock all the keys in order to avoid dead lock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockKey(String);

impl LockKey {
    /// Returns a new [LockKey].
    pub fn new(key: impl Into<String>) -> LockKey {
        LockKey(key.into())
    }

    /// Returns the lock key.
    pub fn key(&self) -> &str {
        &self.0
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

// TODO(yingwen): Find a way to return the error message if the procedure is failed.
/// State of a submitted procedure.
#[derive(Debug)]
pub enum ProcedureState {
    /// The procedure is running.
    Running,
    /// The procedure is finished.
    Done,
    /// The procedure is failed and cannot proceed anymore.
    Failed,
}

// TODO(yingwen): Shutdown
/// `ProcedureManager` executes [Procedure] submitted to it.
#[async_trait]
pub trait ProcedureManager: Send + Sync + 'static {
    /// Registers loader for specific procedure type `name`.
    fn register_loader(&self, name: &str, loader: BoxedProcedureLoader) -> Result<()>;

    /// Submits a procedure to execute.
    async fn submit(&self, procedure: ProcedureWithId) -> Result<()>;

    /// Recovers unfinished procedures and reruns them.
    ///
    /// Callers should ensure all loaders are registered.
    async fn recover(&self) -> Result<()>;

    /// Query the procedure state.
    ///
    /// Returns `Ok(None)` if the procedure doesn't exist.
    async fn procedure_state(&self, procedure_id: ProcedureId) -> Result<Option<ProcedureState>>;
}

/// Ref-counted pointer to the [ProcedureManager].
pub type ProcedureManagerRef = Arc<dyn ProcedureManager>;

/// Serialized data of a procedure.
#[derive(Debug, Serialize, Deserialize)]
struct ProcedureMessage {
    /// Type name of the procedure. The procedure framework also use the type name to
    /// find a loader to load the procedure.
    type_name: String,
    /// The data of the procedure.
    data: String,
    /// Parent procedure id.
    parent_id: Option<ProcedureId>,
}

#[cfg(test)]
mod tests {
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
        let key = LockKey::new(entity);
        assert_eq!(entity, key.key());
    }

    #[test]
    fn test_procedure_id() {
        let id = ProcedureId::random();
        let uuid_str = id.to_string();
        assert_eq!(id.0.to_string(), uuid_str);

        let parsed = ProcedureId::parse_str(&uuid_str).unwrap();
        assert_eq!(id, parsed);
    }
}
