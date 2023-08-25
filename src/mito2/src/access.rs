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

use std::sync::Arc;

use object_store::{util, ObjectStore};
use snafu::ResultExt;

use crate::error::{DeleteSstSnafu, Result};
use crate::sst::file::FileId;

pub type AccessLayerRef = Arc<FsAccessLayer>;

/// Sst access layer.
pub struct FsAccessLayer {
    sst_dir: String,
    object_store: ObjectStore,
}

impl std::fmt::Debug for FsAccessLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AccessLayer")
            .field("sst_dir", &self.sst_dir)
            .finish()
    }
}

impl FsAccessLayer {
    pub fn new(sst_dir: &str, object_store: ObjectStore) -> FsAccessLayer {
        FsAccessLayer {
            sst_dir: util::normalize_dir(sst_dir),
            object_store,
        }
    }

    fn sst_file_path(&self, file_name: &str) -> String {
        format!("{}{}", self.sst_dir, file_name)
    }

    /// Deletes a SST file with given file id.
    pub async fn delete_sst(&self, file_id: FileId) -> Result<()> {
        let path = self.sst_file_path(&file_id.as_parquet());
        self.object_store
            .delete(&path)
            .await
            .context(DeleteSstSnafu {
                file_id,
            })
    }
}
