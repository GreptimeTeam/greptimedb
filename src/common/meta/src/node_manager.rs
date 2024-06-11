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

use api::region::RegionResponse;
use api::v1::flow::{FlowRequest, FlowResponse};
use api::v1::region::{InsertRequests, RegionRequest};
pub use common_base::AffectedRows;
use common_query::request::QueryRequest;
use common_recordbatch::SendableRecordBatchStream;

use crate::error::Result;
use crate::peer::Peer;

/// The trait for handling requests to datanode.
#[async_trait::async_trait]
pub trait Datanode: Send + Sync {
    /// Handles DML, and DDL requests.
    async fn handle(&self, request: RegionRequest) -> Result<RegionResponse>;

    /// Handles query requests
    async fn handle_query(&self, request: QueryRequest) -> Result<SendableRecordBatchStream>;
}

pub type DatanodeRef = Arc<dyn Datanode>;

/// The trait for handling requests to flownode
#[async_trait::async_trait]
pub trait Flownode: Send + Sync {
    async fn handle(&self, request: FlowRequest) -> Result<FlowResponse>;

    async fn handle_inserts(&self, request: InsertRequests) -> Result<FlowResponse>;
}

pub type FlownodeRef = Arc<dyn Flownode>;

/// Datanode manager
#[async_trait::async_trait]
pub trait NodeManager: Send + Sync {
    /// Retrieves a target `datanode`.
    async fn datanode(&self, node: &Peer) -> DatanodeRef;

    /// Retrieves a target `flownode`.
    async fn flownode(&self, node: &Peer) -> FlownodeRef;
}

pub type NodeManagerRef = Arc<dyn NodeManager>;
