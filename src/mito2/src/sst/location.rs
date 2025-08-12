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
use store_api::metric_engine_consts::{DATA_REGION_SUBDIR, METADATA_REGION_SUBDIR};
use store_api::path_utils::region_name;
use store_api::region_request::PathType;
use store_api::storage::RegionId;

use crate::sst::file::RegionFileId;

/// Generate region dir from table_dir, region_id and path_type
pub fn region_dir_from_table_dir(
    table_dir: &str,
    region_id: RegionId,
    path_type: PathType,
) -> String {
    let region_name = region_name(region_id.table_id(), region_id.region_sequence());
    let base_region_dir = util::join_dir(table_dir, &region_name);

    match path_type {
        PathType::Bare => base_region_dir,
        PathType::Data => util::join_dir(&base_region_dir, DATA_REGION_SUBDIR),
        PathType::Metadata => util::join_dir(&base_region_dir, METADATA_REGION_SUBDIR),
    }
}

/// Generate staging region dir from table_dir, region_id and path_type
pub fn staging_region_dir_from_table_dir(
    table_dir: &str,
    region_id: RegionId,
    path_type: PathType,
) -> String {
    let base_region_dir = region_dir_from_table_dir(table_dir, region_id, path_type);
    util::join_dir(&base_region_dir, "staging")
}

pub fn sst_file_path(table_dir: &str, region_file_id: RegionFileId, path_type: PathType) -> String {
    let region_dir = region_dir_from_table_dir(table_dir, region_file_id.region_id(), path_type);
    util::join_path(
        &region_dir,
        &format!("{}.parquet", region_file_id.file_id()),
    )
}

pub fn staging_sst_file_path(
    table_dir: &str,
    region_file_id: RegionFileId,
    path_type: PathType,
) -> String {
    let staging_region_dir =
        staging_region_dir_from_table_dir(table_dir, region_file_id.region_id(), path_type);
    util::join_path(
        &staging_region_dir,
        &format!("{}.parquet", region_file_id.file_id()),
    )
}

pub fn index_file_path(
    table_dir: &str,
    region_file_id: RegionFileId,
    path_type: PathType,
) -> String {
    let region_dir = region_dir_from_table_dir(table_dir, region_file_id.region_id(), path_type);
    let index_dir = util::join_dir(&region_dir, "index");
    util::join_path(&index_dir, &format!("{}.puffin", region_file_id.file_id()))
}

pub fn staging_index_file_path(
    table_dir: &str,
    region_file_id: RegionFileId,
    path_type: PathType,
) -> String {
    let staging_region_dir =
        staging_region_dir_from_table_dir(table_dir, region_file_id.region_id(), path_type);
    let staging_index_dir = util::join_dir(&staging_region_dir, "index");
    util::join_path(
        &staging_index_dir,
        &format!("{}.puffin", region_file_id.file_id()),
    )
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
    fn test_staging_sst_file_path() {
        let file_id = FileId::random();
        let region_file_id = RegionFileId::new(RegionId::new(1, 2), file_id);
        assert_eq!(
            staging_sst_file_path("table_dir", region_file_id, PathType::Bare),
            format!("table_dir/1_0000000002/staging/{}.parquet", file_id)
        );
        assert_eq!(
            staging_sst_file_path("table_dir", region_file_id, PathType::Data),
            format!("table_dir/1_0000000002/data/staging/{}.parquet", file_id)
        );
        assert_eq!(
            staging_sst_file_path("table_dir", region_file_id, PathType::Metadata),
            format!(
                "table_dir/1_0000000002/metadata/staging/{}.parquet",
                file_id
            )
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

    #[test]
    fn test_staging_index_file_path() {
        let file_id = FileId::random();
        let region_file_id = RegionFileId::new(RegionId::new(1, 2), file_id);
        assert_eq!(
            staging_index_file_path("table_dir", region_file_id, PathType::Bare),
            format!("table_dir/1_0000000002/staging/index/{}.puffin", file_id)
        );
        assert_eq!(
            staging_index_file_path("table_dir", region_file_id, PathType::Data),
            format!(
                "table_dir/1_0000000002/data/staging/index/{}.puffin",
                file_id
            )
        );
        assert_eq!(
            staging_index_file_path("table_dir", region_file_id, PathType::Metadata),
            format!(
                "table_dir/1_0000000002/metadata/staging/index/{}.puffin",
                file_id
            )
        );
    }
}
