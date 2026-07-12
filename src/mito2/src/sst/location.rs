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
use snafu::OptionExt as _;
use store_api::metric_engine_consts::{DATA_REGION_SUBDIR, METADATA_REGION_SUBDIR};
use store_api::path_utils::region_name;
use store_api::region_request::PathType;
use store_api::storage::{FileId, RegionId};

use crate::cache::file_cache::FileType;
use crate::error::UnexpectedSnafu;
use crate::sst::file::{RegionFileId, RegionIndexId};

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

pub fn sst_file_path(table_dir: &str, region_file_id: RegionFileId, path_type: PathType) -> String {
    let region_dir = region_dir_from_table_dir(table_dir, region_file_id.region_id(), path_type);
    util::join_path(
        &region_dir,
        &format!("{}.parquet", region_file_id.file_id()),
    )
}

pub fn index_file_path(table_dir: &str, index_id: RegionIndexId, path_type: PathType) -> String {
    let region_dir = region_dir_from_table_dir(table_dir, index_id.file_id.region_id(), path_type);
    let index_dir = util::join_dir(&region_dir, "index");

    let filename = if index_id.version == 0 {
        format!("{}.puffin", index_id.file_id.file_id())
    } else {
        format!("{}.{}.puffin", index_id.file_id.file_id(), index_id.version)
    };

    util::join_path(&index_dir, &filename)
}

/// Legacy function for backward compatibility - creates index file path using RegionFileId with version 0
pub fn index_file_path_legacy(
    table_dir: &str,
    region_file_id: RegionFileId,
    path_type: PathType,
) -> String {
    let index_id = RegionIndexId::new(region_file_id, 0);
    index_file_path(table_dir, index_id, path_type)
}

/// Parse file ID and version from index filename
pub fn parse_index_file_info(filepath: &str) -> crate::error::Result<(FileId, u64)> {
    let filename = filepath.rsplit('/').next().context(UnexpectedSnafu {
        reason: format!("invalid file path: {}", filepath),
    })?;
    let parts: Vec<&str> = filename.split('.').collect();

    if parts.len() == 2 && parts[1] == "puffin" {
        // Legacy format: {file_id}.puffin (version 0)
        let file_id = parts[0];
        FileId::parse_str(file_id).map(|id| (id, 0)).map_err(|e| {
            UnexpectedSnafu {
                reason: format!("invalid file id: {}, err: {}", file_id, e),
            }
            .build()
        })
    } else if parts.len() == 3 && parts[2] == "puffin" {
        // New format: {file_id}.{version}.puffin
        let file_id = parts[0];
        let version = parts[1].parse::<u64>().map_err(|_| {
            UnexpectedSnafu {
                reason: format!("invalid version in file name: {}", filename),
            }
            .build()
        })?;
        FileId::parse_str(file_id)
            .map(|id| (id, version))
            .map_err(|e| {
                UnexpectedSnafu {
                    reason: format!("invalid file id: {}, err: {}", file_id, e),
                }
                .build()
            })
    } else {
        UnexpectedSnafu {
            reason: format!("invalid index file name: {}", filename),
        }
        .fail()
    }
}

pub fn parse_file_id_type_from_path(filepath: &str) -> crate::error::Result<(FileId, FileType)> {
    let filename = filepath.rsplit('/').next().context(UnexpectedSnafu {
        reason: format!("invalid file path: {}", filepath),
    })?;
    // get part before first '.'
    let parts: Vec<&str> = filename.split('.').collect();
    if parts.len() < 2 {
        return UnexpectedSnafu {
            reason: format!("invalid file name: {}", filename),
        }
        .fail();
    }
    let file_id = parts[0];
    let file_id = FileId::parse_str(file_id).map_err(|e| {
        UnexpectedSnafu {
            reason: format!("invalid file id: {}, err: {}", file_id, e),
        }
        .build()
    })?;
    let file_type = FileType::parse(parts[1..].join(".").as_str()).context(UnexpectedSnafu {
        reason: format!("invalid file type in file name: {}", filename),
    })?;
    Ok((file_id, file_type))
}

#[cfg(test)]
mod tests {
    use store_api::storage::{FileId, RegionId};

    use super::*;

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
        let index_id = RegionIndexId::new(region_file_id, 0);
        assert_eq!(
            index_file_path("table_dir", index_id, PathType::Bare),
            format!("table_dir/1_0000000002/index/{}.puffin", file_id)
        );
        assert_eq!(
            index_file_path("table_dir", index_id, PathType::Data),
            format!("table_dir/1_0000000002/data/index/{}.puffin", file_id)
        );
        assert_eq!(
            index_file_path("table_dir", index_id, PathType::Metadata),
            format!("table_dir/1_0000000002/metadata/index/{}.puffin", file_id)
        );
    }

    #[test]
    fn test_index_file_path_versioned() {
        let file_id = FileId::random();
        let region_file_id = RegionFileId::new(RegionId::new(1, 2), file_id);
        let index_id_v1 = RegionIndexId::new(region_file_id, 1);
        let index_id_v2 = RegionIndexId::new(region_file_id, 2);

        assert_eq!(
            index_file_path("table_dir", index_id_v1, PathType::Bare),
            format!("table_dir/1_0000000002/index/{}.1.puffin", file_id)
        );
        assert_eq!(
            index_file_path("table_dir", index_id_v2, PathType::Bare),
            format!("table_dir/1_0000000002/index/{}.2.puffin", file_id)
        );
    }

    #[test]
    fn test_parse_index_file_info() {
        // Test legacy format
        let file_id = FileId::random();
        let result =
            parse_index_file_info(&format!("table_dir/1_0000000002/index/{file_id}.puffin"))
                .unwrap();
        assert_eq!(result.0.to_string(), file_id.to_string());
        assert_eq!(result.1, 0);

        // Test versioned format
        let result =
            parse_index_file_info(&format!("table_dir/1_0000000002/index/{file_id}.1.puffin"))
                .unwrap();
        assert_eq!(result.0.to_string(), file_id.to_string());
        assert_eq!(result.1, 1);

        let result =
            parse_index_file_info(&format!("table_dir/1_0000000002/index/{file_id}.42.puffin"))
                .unwrap();
        assert_eq!(result.0.to_string(), file_id.to_string());
        assert_eq!(result.1, 42);
    }

    #[test]
    fn test_parse_file_id_type_from_path() {
        use crate::cache::file_cache::FileType;

        // Test parquet file
        let file_id = FileId::random();
        let path = format!("table_dir/1_0000000002/data/{}.parquet", file_id);
        let result = parse_file_id_type_from_path(&path).unwrap();
        assert_eq!(result.0.to_string(), file_id.to_string());
        assert_eq!(result.1, FileType::Parquet);

        // Test puffin file (legacy format, version 0)
        let file_id = FileId::random();
        let path = format!("table_dir/1_0000000002/index/{}.puffin", file_id);
        let result = parse_file_id_type_from_path(&path).unwrap();
        assert_eq!(result.0.to_string(), file_id.to_string());
        assert_eq!(result.1, FileType::Puffin(0));

        // Test versioned puffin file
        let file_id = FileId::random();
        let path = format!("table_dir/1_0000000002/index/{}.1.puffin", file_id);
        let result = parse_file_id_type_from_path(&path).unwrap();
        assert_eq!(result.0.to_string(), file_id.to_string());
        assert_eq!(result.1, FileType::Puffin(1));

        // Test with different path types
        let file_id = FileId::random();
        let path = format!("table_dir/1_0000000002/metadata/{}.parquet", file_id);
        let result = parse_file_id_type_from_path(&path).unwrap();
        assert_eq!(result.0.to_string(), file_id.to_string());
        assert_eq!(result.1, FileType::Parquet);

        // Test with bare path type
        let file_id = FileId::random();
        let path = format!("table_dir/1_0000000002/{}.parquet", file_id);
        let result = parse_file_id_type_from_path(&path).unwrap();
        assert_eq!(result.0.to_string(), file_id.to_string());
        assert_eq!(result.1, FileType::Parquet);

        // Test error cases
        // Invalid file extension
        let result = parse_file_id_type_from_path("table_dir/1_0000000002/data/test.invalid");
        assert!(result.is_err());

        // Invalid file ID
        let result =
            parse_file_id_type_from_path("table_dir/1_0000000002/data/invalid-file-id.parquet");
        assert!(result.is_err());

        // No file extension
        let result = parse_file_id_type_from_path("table_dir/1_0000000002/data/test");
        assert!(result.is_err());

        // Empty filename
        let result = parse_file_id_type_from_path("table_dir/1_0000000002/data/");
        assert!(result.is_err());
    }
}
