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

#[derive(Clone)]
pub struct SstIndexApplier {
    region_dir: String,
    store: InstrumentedStore,

    index_applier: Arc<dyn IndexApplier>,
}

impl SstIndexApplier {
    pub(crate) fn new(
        region_dir: String,
        store: InstrumentedStore,
        index_applier: Arc<dyn IndexApplier>,
    ) -> Self {
        INDEX_APPLY_MEMORY_USAGE.add(index_applier.memory_usage() as i64);

        Self {
            region_dir,
            store,
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
            index_not_found_strategy: IndexNotFoundStrategy::ReturnEmpty,
        };
        let res = self
            .index_applier
            .apply(context, &mut index_reader)
            .await
            .context(ApplyIndexSnafu)?;

        Ok(res)
    }
}

impl Drop for SstIndexApplier {
    fn drop(&mut self) {
        INDEX_APPLY_MEMORY_USAGE.sub(self.index_applier.memory_usage() as i64);
    }
}
