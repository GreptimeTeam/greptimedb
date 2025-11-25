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

pub(crate) mod repartition_start;

use std::any::Any;
use std::fmt::Debug;

use common_error::ext::BoxedError;
use common_meta::key::table_route::TableRouteValue;
use common_meta::key::{DeserializedValueWithBytes, TableMetadataManagerRef};
use common_meta::rpc::router::RegionRoute;
use common_procedure::{Context as ProcedureContext, Status};
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use store_api::storage::{RegionId, TableId};
use uuid::Uuid;

use crate::error::{self, Result};
use crate::procedure::repartition::plan::RegionDescriptor;

pub type GroupId = Uuid;

pub struct RepartitionGroupProcedure {}

pub struct Context {
    pub persistent_ctx: PersistentContext,

    pub table_metadata_manager: TableMetadataManagerRef,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GroupPrepareResult {
    pub source_routes: Vec<RegionRoute>,
    pub target_routes: Vec<RegionRoute>,
    pub central_region: RegionId,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PersistentContext {
    pub group_id: GroupId,
    /// The table id of the repartition group.
    pub table_id: TableId,
    /// The source regions of the repartition group.
    pub sources: Vec<RegionDescriptor>,
    /// The target regions of the repartition group.
    pub targets: Vec<RegionDescriptor>,
    /// The result of group prepare.
    /// The value will be set in [RepartitionStart](crate::procedure::repartition::group::repartition_start::RepartitionStart) state.
    pub group_prepare_result: Option<GroupPrepareResult>,
}

impl Context {
    /// Retrieves the table route value for the given table id.
    ///
    /// Retry:
    /// - Failed to retrieve the metadata of table.
    ///
    /// Abort:
    /// - Table route not found.
    pub async fn get_table_route_value(
        &self,
    ) -> Result<DeserializedValueWithBytes<TableRouteValue>> {
        let table_id = self.persistent_ctx.table_id;
        let group_id = self.persistent_ctx.group_id;
        let table_route_value = self
            .table_metadata_manager
            .table_route_manager()
            .table_route_storage()
            .get_with_raw_bytes(table_id)
            .await
            .map_err(BoxedError::new)
            .with_context(|_| error::RetryLaterWithSourceSnafu {
                reason: format!(
                    "Failed to get table route for table: {}, repartition group: {}",
                    table_id, group_id
                ),
            })?
            .context(error::TableRouteNotFoundSnafu { table_id })?;

        Ok(table_route_value)
    }
}

#[async_trait::async_trait]
#[typetag::serde(tag = "repartition_group_state")]
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

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::sync::Arc;

    use common_meta::key::TableMetadataManager;
    use common_meta::kv_backend::test_util::MockKvBackendBuilder;

    use crate::error::Error;
    use crate::procedure::repartition::test_util::{TestingEnv, new_persistent_context};

    #[tokio::test]
    async fn test_get_table_route_value_not_found_error() {
        let env = TestingEnv::new();
        let persistent_context = new_persistent_context(1024, vec![], vec![]);
        let ctx = env.create_context(persistent_context);
        let err = ctx.get_table_route_value().await.unwrap_err();
        assert_matches!(err, Error::TableRouteNotFound { .. });
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn test_get_table_route_value_retry_error() {
        let kv = MockKvBackendBuilder::default()
            .range_fn(Arc::new(|_| {
                common_meta::error::UnexpectedSnafu {
                    err_msg: "mock err",
                }
                .fail()
            }))
            .build()
            .unwrap();
        let mut env = TestingEnv::new();
        env.table_metadata_manager = Arc::new(TableMetadataManager::new(Arc::new(kv)));
        let persistent_context = new_persistent_context(1024, vec![], vec![]);
        let ctx = env.create_context(persistent_context);
        let err = ctx.get_table_route_value().await.unwrap_err();
        assert!(err.is_retryable());
    }
}
