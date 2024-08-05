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
#[allow(unused)]
pub(crate) mod index;
pub mod log_store;
pub(crate) mod producer;
pub(crate) mod util;

use serde::{Deserialize, Serialize};
use store_api::logstore::entry::Id as EntryId;

/// Kafka Namespace implementation.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct NamespaceImpl {
    pub region_id: u64,
    pub topic: String,
}

/// Kafka Entry implementation.
#[derive(Debug, PartialEq, Clone)]
pub struct EntryImpl {
    /// Entry payload.
    pub data: Vec<u8>,
    /// The logical entry id.
    pub id: EntryId,
    /// The namespace used to identify and isolate log entries from different regions.
    pub ns: NamespaceImpl,
}
