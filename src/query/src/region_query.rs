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

use async_trait::async_trait;
use common_meta::node_manager::NodeManagerRef;
use common_query::request::QueryRequest;
use common_recordbatch::SendableRecordBatchStream;
use partition::manager::PartitionRuleManagerRef;
use session::ReadPreference;

use crate::error::Result;

/// A factory to create a [`RegionQueryHandler`].
pub trait RegionQueryHandlerFactory: Send + Sync {
    /// Build a [`RegionQueryHandler`] with the given partition manager and node manager.
    fn build(
        &self,
        partition_manager: PartitionRuleManagerRef,
        node_manager: NodeManagerRef,
    ) -> RegionQueryHandlerRef;
}

pub type RegionQueryHandlerFactoryRef = Arc<dyn RegionQueryHandlerFactory>;

#[async_trait]
pub trait RegionQueryHandler: Send + Sync {
    async fn do_get(
        &self,
        read_preference: ReadPreference,
        request: QueryRequest,
    ) -> Result<SendableRecordBatchStream>;
}

pub type RegionQueryHandlerRef = Arc<dyn RegionQueryHandler>;
