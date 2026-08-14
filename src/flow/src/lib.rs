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

//! This crate manage dataflow in Greptime, including adapter, expr, plan, repr and utils.
//! It can transform substrait plan into it's own plan and execute it.
//! It also contains definition of expression, adapter and plan, and internal state management.

#![allow(dead_code)]
#![warn(clippy::too_many_lines)]

// TODO(discord9): enable this lint to handle out of bound access
// #![cfg_attr(not(test), warn(clippy::indexing_slicing))]

// allow unused for now because it should be use later
mod adapter;
pub(crate) mod batching_mode;
mod compute;
mod df_optimizer;
pub(crate) mod engine;
pub mod error;
mod expr;
pub mod heartbeat;
mod metrics;
mod plan;
mod repr;
mod server;
mod transform;
mod utils;

// Re-export the reserved checkpoint-persistence contract so internal
// producers of the sink state schema (e.g. the enterprise incremental flow
// state table) can reference the exact epoch column name and sentinel window
// timestamp without duplicating literals. The definitions themselves live in
// the private `batching_mode` module; see its docs for the sentinel safety
// caveat.
pub use batching_mode::{
    CHECKPOINT_SENTINEL_WINDOW_TS_MILLIS, INTERNAL_FLOW_EPOCH_COL_NAME, INTERNAL_FLOW_STATE_COL_KEY,
};

#[cfg(test)]
mod test_utils;

pub use adapter::flownode_impl::FlowDualEngineRef;
pub use adapter::{FlowConfig, FlowStreamingEngineRef, StreamingEngine};
pub use batching_mode::frontend_client::{FrontendClient, GrpcQueryHandlerWithBoxedError};
pub(crate) use engine::{CreateFlowArgs, FlowId, TableName};
pub use error::{Error, Result};
pub use server::{
    FlownodeBuilder, FlownodeInstance, FlownodeServer, FlownodeServiceBuilder, FrontendInvoker,
};

pub use crate::adapter::FlownodeOptions;
