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

use std::collections::HashMap;
use std::time::Instant;

use common_meta::datanode::{RegionManifestInfo, RegionStat};
use common_telemetry::{debug, info};
use ordered_float::OrderedFloat;
use store_api::region_engine::RegionRole;
use store_api::storage::RegionId;
use table::metadata::TableId;

use crate::error::Result;

/// Represents a region candidate for GC with its priority score.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GcCandidate {
    pub(crate) region_id: RegionId,
    pub(crate) score: OrderedFloat<f64>,
    pub(crate) region_stat: RegionStat,
}

impl GcCandidate {
    fn new(region_id: RegionId, score: f64, region_stat: RegionStat) -> Self {
        Self {
            region_id,
            score: OrderedFloat(score),
            region_stat,
        }
    }

    #[allow(unused)]
    fn score_f64(&self) -> f64 {
        self.score.into_inner()
    }
}
