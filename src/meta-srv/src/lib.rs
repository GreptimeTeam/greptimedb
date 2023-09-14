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

#![feature(async_closure)]
#![feature(result_flattening)]

pub mod bootstrap;
mod cache_invalidator;
pub mod cluster;
pub mod election;
pub mod error;
mod failure_detector;
pub mod handler;
pub mod keys;
pub mod lease;
pub mod lock;
pub mod metasrv;
mod metrics;
#[cfg(feature = "mock")]
pub mod mocks;
pub mod procedure;
pub mod pubsub;
pub mod selector;
pub mod service;
pub mod table_meta_alloc;
pub mod table_routes;

pub use crate::error::Result;

mod inactive_region_manager;

mod greptimedb_telemetry;

#[cfg(test)]
mod test_util;
