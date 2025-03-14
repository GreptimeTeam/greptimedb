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

pub mod cursor;
pub mod end;
pub mod executor;
pub mod metadata;
pub mod start;
use std::any::Any;
use std::fmt::Debug;

use common_error::ext::BoxedError;
use common_procedure::error::{Error as ProcedureError, ExternalSnafu, FromJsonSnafu, ToJsonSnafu};
use common_procedure::{
    Context as ProcedureContext, LockKey, Procedure, Result as ProcedureResult, Status,
};
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use tonic::async_trait;

use self::start::DropDatabaseStart;
use crate::ddl::DdlContext;
use crate::error::Result;
use crate::key::table_name::TableNameValue;
use crate::lock_key::{CatalogLock, RemoteWalLock, SchemaLock};

pub struct DropDatabaseProcedure {
    /// The context of procedure runtime.
    runtime_context: DdlContext,
    context: DropDatabaseContext,

    state: Box<dyn State>,
}

/// Target of dropping tables.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum DropTableTarget {
    Logical,
    Physical,
}

/// Context of [DropDatabaseProcedure] execution.
pub(crate) struct DropDatabaseContext {
    catalog: String,
    schema: String,
    drop_if_exists: bool,
    tables: Option<BoxStream<'static, Result<(String, TableNameValue)>>>,
}

#[async_trait::async_trait]
#[typetag::serde(tag = "drop_database_state")]
pub(crate) trait State: Send + Debug {
    /// Yields the next [State] and [Status].
    async fn next(
        &mut self,
        ddl_ctx: &DdlContext,
        ctx: &mut DropDatabaseContext,
    ) -> Result<(Box<dyn State>, Status)>;

    /// The hook is called during the recovery.
    fn recover(&mut self, _ddl_ctx: &DdlContext) -> Result<()> {
        Ok(())
    }

    /// Returns as [Any](std::any::Any).
    fn as_any(&self) -> &dyn Any;
}

impl DropDatabaseProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::DropDatabase";

    pub fn new(catalog: String, schema: String, drop_if_exists: bool, context: DdlContext) -> Self {
        Self {
            runtime_context: context,
            context: DropDatabaseContext {
                catalog,
                schema,
                drop_if_exists,
                tables: None,
            },
            state: Box::new(DropDatabaseStart),
        }
    }

    pub fn from_json(json: &str, runtime_context: DdlContext) -> ProcedureResult<Self> {
        let DropDatabaseOwnedData {
            catalog,
            schema,
            drop_if_exists,
            state,
        } = serde_json::from_str(json).context(FromJsonSnafu)?;

        Ok(Self {
            runtime_context,
            context: DropDatabaseContext {
                catalog,
                schema,
                drop_if_exists,
                tables: None,
            },
            state,
        })
    }

    #[cfg(test)]
    pub(crate) fn state(&self) -> &dyn State {
        self.state.as_ref()
    }
}

#[async_trait]
impl Procedure for DropDatabaseProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    fn recover(&mut self) -> ProcedureResult<()> {
        self.state
            .recover(&self.runtime_context)
            .map_err(BoxedError::new)
            .context(ExternalSnafu)
    }

    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        let state = &mut self.state;

        let (next, status) = state
            .next(&self.runtime_context, &mut self.context)
            .await
            .map_err(|e| {
                if e.is_retry_later() {
                    ProcedureError::retry_later(e)
                } else {
                    ProcedureError::external(e)
                }
            })?;

        *state = next;
        Ok(status)
    }

    fn dump(&self) -> ProcedureResult<String> {
        let data = DropDatabaseData {
            catalog: &self.context.catalog,
            schema: &self.context.schema,
            drop_if_exists: self.context.drop_if_exists,
            state: self.state.as_ref(),
        };

        serde_json::to_string(&data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        let lock_key = vec![
            CatalogLock::Read(&self.context.catalog).into(),
            SchemaLock::write(&self.context.catalog, &self.context.schema).into(),
            RemoteWalLock::Write.into(),
        ];

        LockKey::new(lock_key)
    }
}

#[derive(Debug, Serialize)]
struct DropDatabaseData<'a> {
    // The catalog name
    catalog: &'a str,
    // The schema name
    schema: &'a str,
    drop_if_exists: bool,
    state: &'a dyn State,
}

#[derive(Debug, Deserialize)]
struct DropDatabaseOwnedData {
    // The catalog name
    catalog: String,
    // The schema name
    schema: String,
    drop_if_exists: bool,
    state: Box<dyn State>,
}
