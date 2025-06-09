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

#![feature(let_chains)]
#![allow(dead_code)]
#![warn(clippy::missing_docs_in_private_items)]
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

#[cfg(test)]
mod test_utils;

pub use adapter::{FlowConfig, FlowStreamingEngineRef, FlownodeOptions, StreamingEngine};
pub use batching_mode::frontend_client::{FrontendClient, GrpcQueryHandlerWithBoxedError};
pub use engine::FlowAuthHeader;
pub(crate) use engine::{CreateFlowArgs, FlowId, TableName};
pub use error::{Error, Result};
pub use server::{
    get_flow_auth_options, FlownodeBuilder, FlownodeInstance, FlownodeServer,
    FlownodeServiceBuilder, FrontendInvoker,
};
