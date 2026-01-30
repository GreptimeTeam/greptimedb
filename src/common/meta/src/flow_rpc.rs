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

use api::v1::flow::{DirtyWindowRequests, FlowRequest, FlowResponse};
use api::v1::region::InsertRequests;

use crate::error::Result;
use crate::peer::Peer;

/// RPC surface for communicating with flownode's flow service.
///
/// This is a transport-facing API (gRPC vs in-process) and should not encode business logic.
#[async_trait::async_trait]
pub trait FlowRpc: Send + Sync {
    /// Handles create/drop/flush requests to flownode.
    async fn handle_flow(&self, peer: &Peer, request: FlowRequest) -> Result<FlowResponse>;

    /// Handles mirrored inserts to flownode.
    async fn handle_flow_inserts(
        &self,
        peer: &Peer,
        request: InsertRequests,
    ) -> Result<FlowResponse>;

    /// Handles requests to mark time window as dirty.
    async fn handle_mark_window_dirty(
        &self,
        peer: &Peer,
        req: DirtyWindowRequests,
    ) -> Result<FlowResponse>;
}

pub type FlowRpcRef = Arc<dyn FlowRpc>;
