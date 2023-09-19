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

//! All keys used for distributed locking in the Metasrv.
//! Place them in this unified module for better maintenance.

use common_meta::RegionIdent;

use crate::lock::Key;

pub(crate) fn table_metadata_lock_key(region: &RegionIdent) -> Key {
    format!(
        "table_metadata_lock_({}-{})",
        region.cluster_id, region.table_id,
    )
    .into_bytes()
}
