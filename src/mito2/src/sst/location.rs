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
use store_api::path_utils::region_name;

use crate::sst::file::RegionFileId;

pub fn sst_file_path(table_dir: &str, region_file_id: RegionFileId) -> String {
    let region_name = region_name(
        region_file_id.region_id().table_id(),
        region_file_id.region_id().region_number(),
    );
    let region_dir = util::join_dir(table_dir, &region_name);
    util::join_path(
        &region_dir,
        &format!("{}.parquet", region_file_id.file_id()),
    )
}

pub fn index_file_path(table_dir: &str, region_file_id: RegionFileId) -> String {
    let region_name = region_name(
        region_file_id.region_id().table_id(),
        region_file_id.region_id().region_number(),
    );
    let region_dir = util::join_dir(table_dir, &region_name);
    let index_dir = util::join_dir(&region_dir, "index");
    util::join_path(&index_dir, &format!("{}.puffin", region_file_id.file_id()))
}

#[cfg(test)]
mod tests {
    use store_api::storage::RegionId;

    use super::*;
    use crate::sst::file::FileId;

    #[test]
    fn test_sst_file_path() {
        let file_id = FileId::random();
        let region_file_id = RegionFileId::new(RegionId::new(1, 2), file_id);
        assert_eq!(
            sst_file_path("table_dir", region_file_id),
            format!("table_dir/1_0000000002/{}.parquet", file_id)
        );
    }

    #[test]
    fn test_index_file_path() {
        let file_id = FileId::random();
        let region_file_id = RegionFileId::new(RegionId::new(1, 2), file_id);
        assert_eq!(
            index_file_path("table_dir", region_file_id),
            format!("table_dir/1_0000000002/index/{}.puffin", file_id)
        );
    }
}
