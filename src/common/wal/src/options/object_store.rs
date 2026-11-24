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

use serde::{Deserialize, Serialize};

/// Object store wal options allocated to a region.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObjectStoreWalOptions {
    /// Public WAL root in distributed mode, unchanged when the region moves.
    /// The local log store resolves it to its own node directory and rejects
    /// roots that differ from its configuration. Standalone metadata may hold
    /// the concrete node prefix, which remains accepted by that same store.
    pub prefix: String,
}

impl ObjectStoreWalOptions {
    /// Creates object store WAL options with the prefix.
    pub fn new(prefix: String) -> Self {
        Self { prefix }
    }
}
