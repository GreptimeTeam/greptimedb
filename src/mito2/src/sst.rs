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

//! Sorted strings tables.

pub mod file;
pub mod file_purger;
mod index;
pub mod parquet;
pub(crate) mod version;

use object_store::util;

use crate::sst::file::FileId;

/// Returns the `file_path` for the `file_id` in the object store.
pub(crate) fn sst_file_path(region_dir: &str, file_id: FileId) -> String {
    util::join_path(region_dir, &file_id.as_parquet())
}
