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

use object_store::util;

use crate::sst::file::FileId;

/// Returns the path of the SST file in the object store:
/// `{region_dir}/{sst_file_id}.parquet`
pub fn sst_file_path(region_dir: &str, sst_file_id: FileId) -> String {
    util::join_path(region_dir, &sst_file_id.as_parquet())
}

/// Returns the path of the index file in the object store:
/// `{region_dir}/index/{sst_file_id}.puffin`
pub fn index_file_path(region_dir: &str, sst_file_id: FileId) -> String {
    let dir = util::join_dir(region_dir, "index");
    util::join_path(&dir, &sst_file_id.as_puffin())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sst_file_path() {
        let file_id = FileId::random();
        assert_eq!(
            sst_file_path("region_dir", file_id),
            format!("region_dir/{file_id}.parquet")
        );
    }

    #[test]
    fn test_index_file_path() {
        let file_id = FileId::random();
        assert_eq!(
            index_file_path("region_dir", file_id),
            format!("region_dir/index/{file_id}.puffin")
        );
    }
}
