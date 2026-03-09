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

/// NATS JetStream WAL options allocated to a region.
///
/// The `topic` field holds the full NATS subject string for this region's WAL
/// (e.g. `"greptimedb_wal_subject.42"`).  It is serialised into the region's
/// `wal_options` map by metasrv and deserialised by the datanode at region open.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NatsWalOptions {
    /// The NATS subject used as the WAL namespace for this region.
    pub topic: String,
}
