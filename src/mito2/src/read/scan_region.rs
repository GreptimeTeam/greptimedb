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

//! Scans a region according to the scan request.

use common_time::range::TimestampRange;
use store_api::storage::ScanRequest;

use crate::read::seq_scan::SeqScan;
use crate::region::version::VersionRef;

/// Helper to scans a region by [ScanRequest].
pub(crate) struct ScanRegion {
    /// Version of the region at scan.
    version: VersionRef,
    /// Scan request.
    request: ScanRequest,
}

impl ScanRegion {
    /// Creates a [ScanRegion].
    pub(crate) fn new(version: VersionRef, request: ScanRequest) -> ScanRegion {
        ScanRegion { version, request }
    }

    /// Scan sequentailly.
    fn seq_scan(&self) -> SeqScan {
        unimplemented!()
    }

    /// Build time range predicate from filters.
    fn build_time_range_predicate(&self) -> TimestampRange {
        unimplemented!()
    }
}
