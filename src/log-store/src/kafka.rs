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

pub(crate) mod client_manager;
pub(crate) mod consumer;
pub mod log_store;
pub(crate) mod offset;
mod periodic_offset_fetcher;
pub(crate) mod producer;
#[cfg(test)]
pub(crate) mod test_util;
pub(crate) mod util;
pub(crate) mod worker;

pub use client_manager::DEFAULT_PARTITION;
use serde::{Deserialize, Serialize};

/// Kafka Namespace implementation.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct NamespaceImpl {
    pub region_id: u64,
    pub topic: String,
}
