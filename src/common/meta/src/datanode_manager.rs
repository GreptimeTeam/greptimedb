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

use api::v1::region::{QueryRequest, RegionRequest};
use common_recordbatch::SendableRecordBatchStream;

use crate::error::Result;
use crate::peer::Peer;

pub type AffectedRows = u64;

#[async_trait::async_trait]
pub trait Datanode: Send + Sync {
    /// Handles DML, and DDL requests.
    async fn handle(&self, request: RegionRequest) -> Result<AffectedRows>;

    async fn handle_query(&self, request: QueryRequest) -> Result<SendableRecordBatchStream>;
}

pub type DatanodeRef = Arc<dyn Datanode>;

#[async_trait::async_trait]
pub trait DatanodeManager: Send + Sync {
    /// Retrieves a target `datanode`.
    async fn datanode(&self, datanode: &Peer) -> DatanodeRef;
}

pub type DatanodeManagerRef = Arc<dyn DatanodeManager>;
