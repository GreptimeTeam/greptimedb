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

//! Trusted, scan-derived statistics exposed by region scanners for higher-level optimizations.
//!
//! These are not raw storage-format statistics. They are conservative summaries that scanner
//! implementations can safely expose for optimizer consumption.

use std::collections::HashMap;

use common_time::Timestamp;
use datatypes::value::Value;

use crate::storage::FileId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegionScanColumnStats {
    pub min_value: Option<Value>,
    pub max_value: Option<Value>,
    pub exact_non_null_rows: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegionScanFileStats {
    /// Stable file id for explainability and cross-plan correlation.
    pub file_id: FileId,
    /// Stable file ordinal within one `RegionScanStats` snapshot.
    pub file_ordinal: usize,
    pub exact_num_rows: Option<usize>,
    pub time_range: Option<(Timestamp, Timestamp)>,
    pub field_stats: HashMap<String, RegionScanColumnStats>,
    pub partition_expr_matches_region: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegionScanStats {
    pub files: Vec<RegionScanFileStats>,
}
