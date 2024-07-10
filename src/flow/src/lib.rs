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
#![feature(duration_abs_diff)]
#![allow(dead_code)]
#![allow(unused_imports)]
#![warn(clippy::missing_docs_in_private_items)]
#![warn(clippy::too_many_lines)]
// allow unused for now because it should be use later
mod adapter;
mod compute;
pub mod error;
mod expr;
pub mod heartbeat;
mod plan;
mod repr;
mod server;
mod transform;
mod utils;

pub use adapter::{FlowWorkerManager, FlowWorkerManagerRef, FlownodeOptions};
pub use error::{Error, Result};
pub use server::{FlownodeBuilder, FlownodeInstance, FlownodeServer, FrontendInvoker};
