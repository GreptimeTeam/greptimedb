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

pub mod allocate_region;
pub mod collect;
pub mod deallocate_region;
pub mod dispatch;
pub mod group;
pub mod plan;
pub mod repartition_end;
pub mod repartition_start;

use std::any::Any;
use std::fmt::Debug;

use common_meta::cache_invalidator::CacheInvalidatorRef;
use common_meta::key::TableMetadataManagerRef;
use common_procedure::{Context as ProcedureContext, Status};
use serde::{Deserialize, Serialize};
use store_api::storage::TableId;

use crate::error::Result;
use crate::procedure::repartition::plan::RepartitionPlanEntry;
use crate::service::mailbox::MailboxRef;

#[cfg(test)]
pub mod test_util;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PersistentContext {
    pub catalog_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub table_id: TableId,
    pub plans: Vec<RepartitionPlanEntry>,
}

pub struct Context {
    pub persistent_ctx: PersistentContext,
    pub table_metadata_manager: TableMetadataManagerRef,
    pub mailbox: MailboxRef,
    pub server_addr: String,
    pub cache_invalidator: CacheInvalidatorRef,
}

#[async_trait::async_trait]
#[typetag::serde(tag = "repartition_state")]
pub(crate) trait State: Sync + Send + Debug {
    fn name(&self) -> &'static str {
        let type_name = std::any::type_name::<Self>();
        // short name
        type_name.split("::").last().unwrap_or(type_name)
    }

    /// Yields the next [State] and [Status].
    async fn next(
        &mut self,
        ctx: &mut Context,
        procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)>;

    fn as_any(&self) -> &dyn Any;
}
