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

use std::collections::BTreeSet;
use std::sync::Arc;

use index::fulltext_index::search::{FulltextIndexSearcher, RowId, TantivyFulltextIndexSearcher};
use object_store::ObjectStore;
use puffin::puffin_manager::{DirGuard, PuffinManager, PuffinReader};
use snafu::ResultExt;
use store_api::storage::ColumnId;

use crate::error::{ApplyFulltextIndexSnafu, PuffinBuildReaderSnafu, PuffinReadBlobSnafu, Result};
use crate::metrics::INDEX_APPLY_ELAPSED;
use crate::sst::file::FileId;
use crate::sst::index::fulltext_index::INDEX_BLOB_TYPE_V1;
use crate::sst::index::puffin_manager::{PuffinManagerFactory, SstPuffinDir};
use crate::sst::index::TYPE_FULLTEXT_INDEX;
use crate::sst::location;

pub mod builder;

/// `FulltextIndexApplier` is responsible for applying fulltext index to the provided SST files
pub struct FulltextIndexApplier {
    /// The root directory of the region.
    region_dir: String,

    /// Queries to apply to the index.
    queries: Vec<(ColumnId, String)>,

    /// The puffin manager factory.
    puffin_manager_factory: PuffinManagerFactory,

    /// Store responsible for accessing index files.
    store: ObjectStore,
}

pub type FulltextIndexApplierRef = Arc<FulltextIndexApplier>;

impl FulltextIndexApplier {
    /// Creates a new `FulltextIndexApplier`.
    pub fn new(
        region_dir: String,
        store: ObjectStore,
        queries: Vec<(ColumnId, String)>,
        puffin_manager_factory: PuffinManagerFactory,
    ) -> Self {
        Self {
            region_dir,
            store,
            queries,
            puffin_manager_factory,
        }
    }

    /// Applies the queries to the fulltext index of the specified SST file.
    pub async fn apply(&self, file_id: FileId) -> Result<BTreeSet<RowId>> {
        let _timer = INDEX_APPLY_ELAPSED
            .with_label_values(&[TYPE_FULLTEXT_INDEX])
            .start_timer();

        let mut inited = false;
        let mut row_ids = BTreeSet::new();

        for (column_id, query) in &self.queries {
            let dir = self.index_dir_path(file_id, *column_id).await?;
            let path = match &dir {
                Some(dir) => dir.path(),
                None => {
                    // Return empty set if the index not found.
                    return Ok(BTreeSet::new());
                }
            };

            let searcher =
                TantivyFulltextIndexSearcher::new(path).context(ApplyFulltextIndexSnafu)?;
            let result = searcher
                .search(query)
                .await
                .context(ApplyFulltextIndexSnafu)?;

            if !inited {
                row_ids = result;
                inited = true;
                continue;
            }

            row_ids.retain(|id| result.contains(id));
            if row_ids.is_empty() {
                break;
            }
        }

        Ok(row_ids)
    }

    /// Returns `None` if the index not found.
    async fn index_dir_path(
        &self,
        file_id: FileId,
        column_id: ColumnId,
    ) -> Result<Option<SstPuffinDir>> {
        let puffin_manager = self.puffin_manager_factory.build(self.store.clone());
        let file_path = location::index_file_path(&self.region_dir, file_id);

        match puffin_manager
            .reader(&file_path)
            .await
            .context(PuffinBuildReaderSnafu)?
            .dir(&format!("{INDEX_BLOB_TYPE_V1}-{column_id}"))
            .await
        {
            Ok(dir) => Ok(Some(dir)),
            Err(puffin::error::Error::BlobNotFound { .. }) => Ok(None),
            Err(err) => Err(err).context(PuffinReadBlobSnafu),
        }
    }
}
