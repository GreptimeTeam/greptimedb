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

//! storage metrics

/// Elapsed time of updating manifest when creating regions.
pub const CREATE_REGION_UPDATE_MANIFEST: &str = "storage.create_region.update_manifest";
/// Counter of scheduled flush requests.
pub const FLUSH_REQUESTS_TOTAL: &str = "storage.flush.requests_total";
/// Counter of scheduled failed flush jobs.
pub const FLUSH_ERRORS_TOTAL: &str = "storage.flush.errors_total";
/// Elapsed time of a flush job.
pub const FLUSH_ELAPSED: &str = "storage.flush.elapsed";
