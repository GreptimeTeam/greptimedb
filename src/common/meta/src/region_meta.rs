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

pub mod wal_meta;

use serde::{Deserialize, Serialize};

use crate::region_meta::wal_meta::RegionWalMeta;

/// Stores a region's unique metadata. Any common metadata or options among regions shall not be stored in the struct.
#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq)]
pub struct RegionMeta {
    /// The region's unique wal metadata.
    pub wal_meta: RegionWalMeta,
}
