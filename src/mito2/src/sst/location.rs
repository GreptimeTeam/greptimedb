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
use uuid::Uuid;

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

/// `IntermediateLocation` produces paths for intermediate files
/// during external sorting.
#[derive(Debug, Clone)]
pub struct IntermediateLocation {
    root_path: String,
}

impl IntermediateLocation {
    /// Create a new `IntermediateLocation`. Set the root directory to
    /// `{region_dir}/index/__intermediate/{sst_file_id}/{uuid}/`, incorporating
    /// uuid to differentiate active sorting files from orphaned data due to unexpected
    /// process termination.
    pub fn new(region_dir: &str, sst_file_id: &FileId) -> Self {
        let uuid = Uuid::new_v4();
        let child = format!("index/__intermediate/{sst_file_id}/{uuid}/");
        Self {
            root_path: util::join_path(region_dir, &child),
        }
    }

    /// Returns the root directory of the intermediate files
    pub fn root_path(&self) -> &str {
        &self.root_path
    }

    /// Returns the path of the directory for intermediate files associated with a column:
    /// `{region_dir}/index/__intermediate/{sst_file_id}/{uuid}/{column_name}/`
    pub fn column_path(&self, column_name: &str) -> String {
        util::join_path(&self.root_path, &format!("{column_name}/"))
    }

    /// Returns the path of the intermediate file with the given id for a column:
    /// `{region_dir}/index/__intermediate/{sst_file_id}/{uuid}/{column_name}/{im_file_id}.im`
    pub fn file_path(&self, column_name: &str, im_file_id: &str) -> String {
        util::join_path(&self.column_path(column_name), &format!("{im_file_id}.im"))
    }
}

#[cfg(test)]
mod tests {
    use regex::Regex;

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

    #[test]
    fn test_intermediate_location() {
        let sst_file_id = FileId::random();
        let location = IntermediateLocation::new("region_dir", &sst_file_id);

        let re = Regex::new(&format!(
            "region_dir/index/__intermediate/{sst_file_id}/{}/",
            r"\w{8}-\w{4}-\w{4}-\w{4}-\w{12}"
        ))
        .unwrap();
        assert!(re.is_match(location.root_path()));

        let uuid = location.root_path().split('/').nth(4).unwrap();

        let column_name = "column_name";
        assert_eq!(
            location.column_path(column_name),
            format!("region_dir/index/__intermediate/{sst_file_id}/{uuid}/{column_name}/")
        );

        let im_file_id = "000000000010";
        assert_eq!(
            location.file_path(column_name, im_file_id),
            format!(
                "region_dir/index/__intermediate/{sst_file_id}/{uuid}/{column_name}/{im_file_id}.im"
            )
        );
    }
}
