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

//! Validation of the history represented by a WAL catalog.

use store_api::logstore::EntryId;
use store_api::storage::RegionId;

use crate::object_store_wal::batch::sequence_floor;
use crate::object_store_wal::catalog::ObjectCatalog;

/// Returns why the observed prefix cannot contain history through `entry_id`.
/// Local histories started on this instance are handled by the caller.
pub(crate) fn missing_history(
    catalog: &ObjectCatalog,
    region_id: RegionId,
    entry_id: EntryId,
    found_empty: bool,
    history_end: u64,
) -> Option<String> {
    if catalog
        .region_max_entry_id(region_id)
        .is_some_and(|max_entry_id| max_entry_id >= entry_id)
    {
        return None;
    }
    if found_empty {
        return Some("the prefix held no object when the WAL opened".to_string());
    }
    let sequence_floor = sequence_floor(entry_id);
    (sequence_floor > history_end).then(|| {
            let held = match history_end {
                0 => "no entry".to_string(),
                history_end => format!("entries only in objects below {history_end}"),
            };
            format!(
                "its watermark, entry {entry_id}, names object {}, but the prefix held {held} when the WAL opened",
                sequence_floor - 1
            )
        })
}
