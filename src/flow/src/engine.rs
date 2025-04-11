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

//! Define a trait for flow engine, which is used by both streaming engine and batch engine

use std::collections::HashMap;

use session::context::QueryContext;
use table::metadata::TableId;

use crate::Error;
// TODO(discord9): refactor common types for flow to a separate module
/// FlowId is a unique identifier for a flow task
pub type FlowId = u64;
pub type TableName = [String; 3];

/// The arguments to create a flow
#[derive(Debug, Clone)]
pub struct CreateFlowArgs {
    pub flow_id: FlowId,
    pub sink_table_name: TableName,
    pub source_table_ids: Vec<TableId>,
    pub create_if_not_exists: bool,
    pub or_replace: bool,
    pub expire_after: Option<i64>,
    pub comment: Option<String>,
    pub sql: String,
    pub flow_options: HashMap<String, String>,
    pub query_ctx: Option<QueryContext>,
}

pub trait FlowEngine {
    /// Create a flow using the provided arguments, return previous flow id if exists and is replaced
    async fn create_flow(&self, args: CreateFlowArgs) -> Result<Option<FlowId>, Error>;
    /// Remove a flow by its ID
    async fn remove_flow(&self, flow_id: FlowId) -> Result<(), Error>;
    /// Flush the flow, return the number of rows flushed
    async fn flush_flow(&self, flow_id: FlowId) -> Result<usize, Error>;
    /// Check if the flow exists
    async fn flow_exist(&self, flow_id: FlowId) -> Result<bool, Error>;
    /// Handle the insert requests for the flow
    async fn handle_flow_inserts(
        &self,
        request: api::v1::region::InsertRequests,
    ) -> Result<(), Error>;
}
