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
use store_api::region_request::PathType;

use crate::sst::file::RegionFileId;

pub fn sst_file_path(table_dir: &str, region_file_id: RegionFileId, path_type: PathType) -> String {
    let region_name = region_name(
        region_file_id.region_id().table_id(),
        region_file_id.region_id().region_sequence(),
    );
    let region_dir = util::join_dir(table_dir, &region_name);
    let final_dir = match path_type {
        PathType::Bare => region_dir,
        PathType::Data => util::join_dir(&region_dir, "data"),
        PathType::Metadata => util::join_dir(&region_dir, "metadata"),
    };
    util::join_path(&final_dir, &format!("{}.parquet", region_file_id.file_id()))
}

pub fn index_file_path(
    table_dir: &str,
    region_file_id: RegionFileId,
    path_type: PathType,
) -> String {
    let region_name = region_name(
        region_file_id.region_id().table_id(),
        region_file_id.region_id().region_sequence(),
    );
    let region_dir = util::join_dir(table_dir, &region_name);
    let final_dir = match path_type {
        PathType::Bare => region_dir,
        PathType::Data => util::join_dir(&region_dir, "data"),
        PathType::Metadata => util::join_dir(&region_dir, "metadata"),
    };
    let index_dir = util::join_dir(&final_dir, "index");
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
            sst_file_path("table_dir", region_file_id, PathType::Bare),
            format!("table_dir/1_0000000002/{}.parquet", file_id)
        );
        assert_eq!(
            sst_file_path("table_dir", region_file_id, PathType::Data),
            format!("table_dir/1_0000000002/data/{}.parquet", file_id)
        );
        assert_eq!(
            sst_file_path("table_dir", region_file_id, PathType::Metadata),
            format!("table_dir/1_0000000002/metadata/{}.parquet", file_id)
        );
    }

    #[test]
    fn test_index_file_path() {
        let file_id = FileId::random();
        let region_file_id = RegionFileId::new(RegionId::new(1, 2), file_id);
        assert_eq!(
            index_file_path("table_dir", region_file_id, PathType::Bare),
            format!("table_dir/1_0000000002/index/{}.puffin", file_id)
        );
        assert_eq!(
            index_file_path("table_dir", region_file_id, PathType::Data),
            format!("table_dir/1_0000000002/data/index/{}.puffin", file_id)
        );
        assert_eq!(
            index_file_path("table_dir", region_file_id, PathType::Metadata),
            format!("table_dir/1_0000000002/metadata/index/{}.puffin", file_id)
        );
    }
}
