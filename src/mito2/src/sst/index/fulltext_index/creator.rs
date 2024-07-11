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

use std::collections::HashMap;
use std::path::PathBuf;

use common_telemetry::warn;
use datatypes::schema::FulltextAnalyzer;
use index::fulltext_index::create::{FulltextIndexCreator, TantivyFulltextIndexCreator};
use index::fulltext_index::{Analyzer, Config};
use puffin::blob_metadata::CompressionCodec;
use puffin::puffin_manager::{PuffinWriter, PutOptions};
use snafu::{ensure, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{ColumnId, ConcreteDataType, RegionId};

use crate::error::{
    CastVectorSnafu, CreateFulltextCreatorSnafu, FieldTypeMismatchSnafu, FulltextFinishSnafu,
    FulltextOptionsSnafu, FulltextPushTextSnafu, OperateAbortedIndexSnafu, PuffinAddBlobSnafu,
    Result,
};
use crate::read::Batch;
use crate::sst::file::FileId;
use crate::sst::index::fulltext_index::INDEX_BLOB_TYPE;
use crate::sst::index::intermediate::IntermediateManager;
use crate::sst::index::puffin_manager::SstPuffinWriter;
use crate::sst::index::statistics::{ByteCount, RowCount, Statistics};

/// `SstIndexCreator` is responsible for creating fulltext indexes for SST files.
pub struct SstIndexCreator {
    /// Creators for each column.
    creators: HashMap<ColumnId, SingleCreator>,
    /// Whether the index creation was aborted.
    aborted: bool,
    /// Statistics of index creation.
    stats: Statistics,
}

impl SstIndexCreator {
    /// Creates a new `SstIndexCreator`.
    pub async fn new(
        region_id: &RegionId,
        sst_file_id: &FileId,
        intermediate_manager: &IntermediateManager,
        metadata: &RegionMetadataRef,
        compress: bool,
        mem_limit: usize,
    ) -> Result<Self> {
        let mut creators = HashMap::new();

        for column in &metadata.column_metadatas {
            let options =
                column
                    .column_schema
                    .fulltext_options()
                    .context(FulltextOptionsSnafu {
                        column_name: &column.column_schema.name,
                    })?;

            // Relax the type constraint here as many types can be casted to string.

            let options = match options {
                Some(options) if options.enable => options,
                _ => continue,
            };

            let column_id = column.column_id;
            let intm_path = intermediate_manager.fulltext_path(region_id, sst_file_id, column_id);

            let config = Config {
                analyzer: match options.analyzer {
                    FulltextAnalyzer::English => Analyzer::English,
                    FulltextAnalyzer::Chinese => Analyzer::Chinese,
                },
                case_sensitive: options.case_sensitive,
            };

            let creator = TantivyFulltextIndexCreator::new(&intm_path, config, mem_limit)
                .await
                .context(CreateFulltextCreatorSnafu)?;

            creators.insert(
                column_id,
                SingleCreator {
                    column_id,
                    inner: Box::new(creator),
                    intm_path,
                    compress,
                },
            );
        }

        Ok(Self {
            creators,
            aborted: false,
            stats: Statistics::default(),
        })
    }

    /// Updates the index with the given batch.
    pub async fn update(&mut self, batch: &Batch) -> Result<()> {
        ensure!(!self.aborted, OperateAbortedIndexSnafu);

        if let Err(update_err) = self.do_update(batch).await {
            if let Err(err) = self.do_abort().await {
                if cfg!(any(test, feature = "test")) {
                    panic!("Failed to abort index creator, err: {err}");
                } else {
                    warn!(err; "Failed to abort index creator");
                }
            }
            return Err(update_err);
        }

        Ok(())
    }

    /// Finalizes the index creation.
    pub async fn finish(
        &mut self,
        puffin_writer: &mut SstPuffinWriter,
    ) -> Result<(RowCount, ByteCount)> {
        ensure!(!self.aborted, OperateAbortedIndexSnafu);

        match self.do_finish(puffin_writer).await {
            Ok(()) => Ok((self.stats.row_count(), self.stats.byte_count())),
            Err(finish_err) => {
                if let Err(err) = self.do_abort().await {
                    if cfg!(any(test, feature = "test")) {
                        panic!("Failed to abort index creator, err: {err}");
                    } else {
                        warn!(err; "Failed to abort index creator");
                    }
                }
                Err(finish_err)
            }
        }
    }

    /// Aborts the index creation.
    pub async fn abort(&mut self) -> Result<()> {
        if self.aborted {
            return Ok(());
        }

        self.do_abort().await
    }

    /// Returns the memory usage of the index creator.
    pub fn memory_usage(&self) -> usize {
        self.creators.values().map(|c| c.inner.memory_usage()).sum()
    }

    /// Returns IDs of columns that the creator is responsible for.
    pub fn column_ids(&self) -> impl Iterator<Item = ColumnId> + '_ {
        self.creators.keys().copied()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.creators.is_empty()
    }
}

impl SstIndexCreator {
    async fn do_update(&mut self, batch: &Batch) -> Result<()> {
        let mut guard = self.stats.record_update();
        guard.inc_row_count(batch.num_rows());

        for creator in self.creators.values_mut() {
            creator.update(batch).await?;
        }

        Ok(())
    }

    async fn do_finish(&mut self, puffin_writer: &mut SstPuffinWriter) -> Result<()> {
        let mut guard = self.stats.record_finish();

        let mut written_bytes = 0;
        for creator in self.creators.values_mut() {
            written_bytes += creator.finish(puffin_writer).await?;
        }

        guard.inc_byte_count(written_bytes);
        Ok(())
    }

    async fn do_abort(&mut self) -> Result<()> {
        let _guard = self.stats.record_cleanup();

        self.aborted = true;

        for (_, mut creator) in self.creators.drain() {
            creator.abort().await?;
        }

        Ok(())
    }
}

/// `SingleCreator` is a creator for a single column.
struct SingleCreator {
    /// Column ID.
    column_id: ColumnId,
    /// Inner creator.
    inner: Box<dyn FulltextIndexCreator>,
    /// Intermediate path where the index is written to.
    intm_path: PathBuf,
    /// Whether the index should be compressed.
    compress: bool,
}

impl SingleCreator {
    async fn update(&mut self, batch: &Batch) -> Result<()> {
        let text_column = batch
            .fields()
            .iter()
            .find(|c| c.column_id == self.column_id);
        match text_column {
            Some(column) => {
                let data = column
                    .data
                    .cast(&ConcreteDataType::string_datatype())
                    .context(CastVectorSnafu {
                        from: column.data.data_type(),
                        to: ConcreteDataType::string_datatype(),
                    })?;

                for i in 0..batch.num_rows() {
                    let data = data.get_ref(i);
                    let text = data
                        .as_string()
                        .context(FieldTypeMismatchSnafu)?
                        .unwrap_or_default();
                    self.inner
                        .push_text(text)
                        .await
                        .context(FulltextPushTextSnafu)?;
                }
            }
            _ => {
                // If the column is not found in the batch, push empty text.
                // Ensure that the number of texts pushed is the same as the number of rows in the SST,
                // so that the texts are aligned with the row ids.
                for _ in 0..batch.num_rows() {
                    self.inner
                        .push_text("")
                        .await
                        .context(FulltextPushTextSnafu)?;
                }
            }
        }

        Ok(())
    }

    async fn finish(&mut self, puffin_writer: &mut SstPuffinWriter) -> Result<ByteCount> {
        self.inner.finish().await.context(FulltextFinishSnafu)?;

        let options = PutOptions {
            compression: self.compress.then_some(CompressionCodec::Zstd),
        };

        let key = format!("{INDEX_BLOB_TYPE}-{}", self.column_id);
        puffin_writer
            .put_dir(&key, self.intm_path.clone(), options)
            .await
            .context(PuffinAddBlobSnafu)
    }

    async fn abort(&mut self) -> Result<()> {
        if let Err(err) = self.inner.finish().await {
            warn!(err; "Failed to finish fulltext index creator, col_id: {:?}, dir_path: {:?}", self.column_id, self.intm_path);
        }
        if let Err(err) = tokio::fs::remove_dir_all(&self.intm_path).await {
            warn!(err; "Failed to remove fulltext index directory, col_id: {:?}, dir_path: {:?}", self.column_id, self.intm_path);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // TODO(zhongzc): After search is implemented, add tests for full-text indexer.
}
