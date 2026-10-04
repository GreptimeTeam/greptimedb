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

//! Direct deletion of per-SST range index files.

use object_store::{ErrorKind, ObjectStore};
use snafu::ResultExt;
use store_api::storage::{FileId, RegionId};

use crate::error::{OpenDalSnafu, Result};
use crate::metrics::SERIES_INDEX_FILE_OPERATION_TOTAL;

/// Deletes range indexes belonging to one region, independently of SST garbage collection.
#[derive(Debug, Clone)]
pub struct RangeIndexDeleter {
    store: ObjectStore,
    region_id: RegionId,
}

impl RangeIndexDeleter {
    /// Creates a deleter using the owning region ID, including for imported SSTs.
    pub fn new(store: ObjectStore, region_id: RegionId) -> Self {
        Self { store, region_id }
    }

    /// Deletes the range index directly from the index store.
    pub async fn delete(&self, file_id: FileId) -> Result<()> {
        let path = range_index_path(self.region_id, file_id);
        let result = match self.store.delete(&path).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error).context(OpenDalSnafu),
        };
        SERIES_INDEX_FILE_OPERATION_TOTAL
            .with_label_values(&[
                "range",
                "delete",
                if result.is_ok() { "success" } else { "failure" },
            ])
            .inc();
        result
    }
}

pub(crate) fn range_index_path(region_id: RegionId, file_id: FileId) -> String {
    format!("{}/range/{file_id}.parquet", region_id.as_u64())
}
