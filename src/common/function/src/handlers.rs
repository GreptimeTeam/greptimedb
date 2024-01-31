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

use std::sync::Arc;
use std::time::Duration;

use api::v1::meta::ProcedureStateResponse;
use async_trait::async_trait;
use common_query::error::Result;
use session::context::QueryContextRef;
use table::requests::{DeleteRequest, InsertRequest};

pub type AffectedRows = usize;

/// A trait for handling table mutations in `QueryEngine`.
#[async_trait]
pub trait TableMutationHandler: Send + Sync {
    /// Inserts rows into the table.
    async fn insert(&self, request: InsertRequest, ctx: QueryContextRef) -> Result<AffectedRows>;

    /// Delete rows from the table.
    async fn delete(&self, request: DeleteRequest, ctx: QueryContextRef) -> Result<AffectedRows>;

    /// Migrate a region from source peer to target peer, returns the procedure id if success.
    async fn migrate_region(
        &self,
        region_id: u64,
        from_peer: u64,
        to_peer: u64,
        replay_timeout: Duration,
    ) -> Result<String>;
}

/// A trait for handling meta service requests in `QueryEngine`.
#[async_trait]
pub trait MetaServiceHandler: Send + Sync {
    /// Query the procedure' state by its id
    async fn query_procedure_state(&self, pid: &str) -> Result<ProcedureStateResponse>;
}

pub type TableMutationHandlerRef = Arc<dyn TableMutationHandler>;

pub type MetaServiceHandlerRef = Arc<dyn MetaServiceHandler>;
