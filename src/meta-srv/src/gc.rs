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

use std::collections::{HashMap, HashSet};

use common_meta::peer::Peer;
use store_api::storage::RegionId;

mod candidate;
mod ctx;
mod dropped;
mod handler;
#[cfg(test)]
mod mock;
mod options;
mod procedure;
mod scheduler;
mod tracker;
mod util;

pub use options::GcSchedulerOptions;
pub use procedure::BatchGcProcedure;
pub use scheduler::{Event, GcScheduler, GcTickerRef};

/// Mapping from region ID to its associated peers (leader and followers).
pub type Region2Peers = HashMap<RegionId, (Peer, Vec<Peer>)>;

/// Mapping from leader peer to the set of region IDs it is responsible for.
pub(crate) type Peer2Regions = HashMap<Peer, HashSet<RegionId>>;
