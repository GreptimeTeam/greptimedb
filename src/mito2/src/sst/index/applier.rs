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

pub mod builder;

use std::collections::BTreeSet;
use std::sync::Arc;

use index::inverted_index::format::reader::InvertedIndexBlobReader;
use index::inverted_index::search::index_apply::{
    IndexApplier, IndexNotFoundStrategy, SearchContext,
};
use object_store::ObjectStore;
use puffin::file_format::reader::{PuffinAsyncReader, PuffinFileReader};
use snafu::{OptionExt, ResultExt};

use crate::error::{
    ApplyIndexSnafu, PuffinBlobTypeNotFoundSnafu, PuffinReadBlobSnafu, PuffinReadMetadataSnafu,
    Result,
};
use crate::metrics::{
    INDEX_APPLY_COST_TIME, INDEX_APPLY_MEMORY_USAGE, INDEX_PUFFIN_READ_BYTES_TOTAL,
};
use crate::sst::file::FileId;
use crate::sst::index::store::InstrumentedStore;
use crate::sst::index::INDEX_BLOB_TYPE;
use crate::sst::location;

/// The [`SstIndexApplier`] is responsible for applying predicates to the provided SST files
/// and returning the relevant row group ids for further scan.
pub struct SstIndexApplier {
    /// The root directory of the region.
    region_dir: String,

    /// Object store responsible for accessing SST files.
    store: InstrumentedStore,

    /// Predefined index applier used to apply predicates to index files
    /// and return the relevant row group ids for further scan.
    index_applier: Box<dyn IndexApplier>,
}

pub type SstIndexApplierRef = Arc<SstIndexApplier>;

impl SstIndexApplier {
    /// Creates a new [`SstIndexApplier`].
    pub fn new(
        region_dir: String,
        store: ObjectStore,
        index_applier: Box<dyn IndexApplier>,
    ) -> Self {
        INDEX_APPLY_MEMORY_USAGE.add(index_applier.memory_usage() as i64);

        Self {
            region_dir,
            store: InstrumentedStore::new(store),
            index_applier,
        }
    }

    pub async fn apply(&self, file_id: FileId) -> Result<BTreeSet<usize>> {
        let _timer = INDEX_APPLY_COST_TIME.start_timer();

        let file_path = location::index_file_path(&self.region_dir, &file_id);

        let file_reader = self
            .store
            .reader(&file_path, &INDEX_PUFFIN_READ_BYTES_TOTAL)
            .await?;
        let mut puffin_reader = PuffinFileReader::new(file_reader);

        let file_meta = puffin_reader
            .metadata()
            .await
            .context(PuffinReadMetadataSnafu)?;
        let blob_meta = file_meta
            .blobs
            .iter()
            .find(|blob| blob.blob_type == INDEX_BLOB_TYPE)
            .context(PuffinBlobTypeNotFoundSnafu {
                blob_type: INDEX_BLOB_TYPE,
            })?;

        let blob_reader = puffin_reader
            .blob_reader(blob_meta)
            .context(PuffinReadBlobSnafu)?;
        let mut index_reader = InvertedIndexBlobReader::new(blob_reader);

        let context = SearchContext {
            // Encountering a non-existing column indicates that it doesn't match predicates.
            index_not_found_strategy: IndexNotFoundStrategy::ReturnEmpty,
        };
        self.index_applier
            .apply(context, &mut index_reader)
            .await
            .context(ApplyIndexSnafu)
    }
}

impl Drop for SstIndexApplier {
    fn drop(&mut self) {
        INDEX_APPLY_MEMORY_USAGE.sub(self.index_applier.memory_usage() as i64);
    }
}
