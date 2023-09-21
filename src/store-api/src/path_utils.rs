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

//! Path constants for table engines, cluster states and WAL

use crate::storage::{RegionId, RegionNumber, TableId};
/// All paths relative to data_home(file storage) or root path(S3, OSS etc).

/// WAL dir for local file storage
pub const WAL_DIR: &str = "wal/";

/// Data dir for table engines
pub const DATA_DIR: &str = "data/";

/// Cluster state dir
pub const CLUSTER_DIR: &str = "cluster/";

/// Generate region name in the form of "{TABLE_ID}_{REGION_NUMBER}"
#[inline]
pub fn region_name(table_id: TableId, region_number: RegionNumber) -> String {
    format!("{table_id}_{region_number:010}")
}

// TODO(jeremy): There are still some dependencies on it. Someone will be here soon to remove it.
pub fn table_dir_with_catalog_and_schema(catalog: &str, schema: &str, table_id: TableId) -> String {
    let path = format!("{}/{}", catalog, schema);
    table_dir(&path, table_id)
}

#[inline]
pub fn table_dir(path: &str, table_id: TableId) -> String {
    format!("{DATA_DIR}{path}/{table_id}/")
}

pub fn region_dir(path: &str, region_id: RegionId) -> String {
    format!(
        "{}{}/",
        table_dir(path, region_id.table_id()),
        region_name(region_id.table_id(), region_id.region_number())
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_region_dir() {
        let region_id = RegionId::new(42, 1);
        assert_eq!(
            region_dir("my_catalog/my_schema", region_id),
            "data/my_catalog/my_schema/42/42_0000000001/"
        );
    }
}
