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

use common_telemetry::{debug, warn};
use puffin::puffin_manager::{PuffinManager, PuffinWriter};
use store_api::storage::ColumnId;

use crate::sst::file::{RegionFileId, RegionIndexId};
#[cfg(feature = "vector_index")]
use crate::sst::index::VectorIndexOutput;
use crate::sst::index::puffin_manager::SstPuffinWriter;
use crate::sst::index::statistics::{ByteCount, RowCount};
use crate::sst::index::{
    BloomFilterOutput, FulltextIndexOutput, IndexOutput, Indexer, InvertedIndexOutput,
};

impl Indexer {
    pub(crate) async fn do_finish(&mut self) -> IndexOutput {
        let mut output = IndexOutput::default();

        let Some(mut writer) = self.build_puffin_writer().await else {
            self.do_abort().await;
            return output;
        };

        let success = self
            .do_finish_inverted_index(&mut writer, &mut output)
            .await;
        if !success {
            self.do_abort().await;
            return IndexOutput::default();
        }

        let success = self
            .do_finish_fulltext_index(&mut writer, &mut output)
            .await;
        if !success {
            self.do_abort().await;
            return IndexOutput::default();
        }

        let success = self.do_finish_bloom_filter(&mut writer, &mut output).await;
        if !success {
            self.do_abort().await;
            return IndexOutput::default();
        }

        #[cfg(feature = "vector_index")]
        {
            let success = self.do_finish_vector_index(&mut writer, &mut output).await;
            if !success {
                self.do_abort().await;
                return IndexOutput::default();
            }
        }

        self.do_prune_intm_sst_dir().await;
        output.file_size = self.do_finish_puffin_writer(writer).await;
        output.version = self.index_version;
        output
    }

    async fn build_puffin_writer(&mut self) -> Option<SstPuffinWriter> {
        let puffin_manager = self.puffin_manager.clone()?;

        let err = match puffin_manager
            .writer(&RegionIndexId::new(
                RegionFileId::new(self.region_id, self.file_id),
                self.index_version,
            ))
            .await
        {
            Ok(writer) => return Some(writer),
            Err(err) => err,
        };

        if cfg!(any(test, feature = "test")) {
            panic!(
                "Failed to create puffin writer, region_id: {}, file_id: {}, err: {:?}",
                self.region_id, self.file_id, err
            );
        } else {
            warn!(
                err; "Failed to create puffin writer, region_id: {}, file_id: {}",
                self.region_id, self.file_id,
            );
        }

        None
    }

    async fn do_finish_puffin_writer(&mut self, writer: SstPuffinWriter) -> ByteCount {
        let err = match writer.finish().await {
            Ok(size) => return size,
            Err(err) => err,
        };

        if cfg!(any(test, feature = "test")) {
            panic!(
                "Failed to finish puffin writer, region_id: {}, file_id: {}, err: {:?}",
                self.region_id, self.file_id, err
            );
        } else {
            warn!(
                err; "Failed to finish puffin writer, region_id: {}, file_id: {}",
                self.region_id, self.file_id,
            );
        }

        0
    }

    /// Returns false if the finish failed.
    async fn do_finish_inverted_index(
        &mut self,
        puffin_writer: &mut SstPuffinWriter,
        index_output: &mut IndexOutput,
    ) -> bool {
        let Some(mut indexer) = self.inverted_indexer.take() else {
            return true;
        };

        let column_ids = indexer.column_ids().collect();
        let err = match indexer.finish(puffin_writer).await {
            Ok((row_count, byte_count)) => {
                self.fill_inverted_index_output(
                    &mut index_output.inverted_index,
                    row_count,
                    byte_count,
                    column_ids,
                );
                return true;
            }
            Err(err) => err,
        };

        if cfg!(any(test, feature = "test")) {
            panic!(
                "Failed to finish inverted index, region_id: {}, file_id: {}, err: {:?}",
                self.region_id, self.file_id, err
            );
        } else {
            warn!(
                err; "Failed to finish inverted index, region_id: {}, file_id: {}",
                self.region_id, self.file_id,
            );
        }

        false
    }

    async fn do_finish_fulltext_index(
        &mut self,
        puffin_writer: &mut SstPuffinWriter,
        index_output: &mut IndexOutput,
    ) -> bool {
        let Some(mut indexer) = self.fulltext_indexer.take() else {
            return true;
        };

        let column_ids = indexer.column_ids().collect();
        let err = match indexer.finish(puffin_writer).await {
            Ok((row_count, byte_count)) => {
                self.fill_fulltext_index_output(
                    &mut index_output.fulltext_index,
                    row_count,
                    byte_count,
                    column_ids,
                );
                return true;
            }
            Err(err) => err,
        };

        if cfg!(any(test, feature = "test")) {
            panic!(
                "Failed to finish full-text index, region_id: {}, file_id: {}, err: {:?}",
                self.region_id, self.file_id, err
            );
        } else {
            warn!(
                err; "Failed to finish full-text index, region_id: {}, file_id: {}",
                self.region_id, self.file_id,
            );
        }

        false
    }

    async fn do_finish_bloom_filter(
        &mut self,
        puffin_writer: &mut SstPuffinWriter,
        index_output: &mut IndexOutput,
    ) -> bool {
        let Some(mut indexer) = self.bloom_filter_indexer.take() else {
            return true;
        };

        let column_ids = indexer.column_ids().collect();
        let err = match indexer.finish(puffin_writer).await {
            Ok((row_count, byte_count)) => {
                self.fill_bloom_filter_output(
                    &mut index_output.bloom_filter,
                    row_count,
                    byte_count,
                    column_ids,
                );
                return true;
            }
            Err(err) => err,
        };

        if cfg!(any(test, feature = "test")) {
            panic!(
                "Failed to finish bloom filter, region_id: {}, file_id: {}, err: {:?}",
                self.region_id, self.file_id, err
            );
        } else {
            warn!(
                err; "Failed to finish bloom filter, region_id: {}, file_id: {}",
                self.region_id, self.file_id,
            );
        }

        false
    }

    fn fill_inverted_index_output(
        &mut self,
        output: &mut InvertedIndexOutput,
        row_count: RowCount,
        byte_count: ByteCount,
        column_ids: Vec<ColumnId>,
    ) {
        debug!(
            "Inverted index created, region_id: {}, file_id: {}, written_bytes: {}, written_rows: {}, columns: {:?}",
            self.region_id, self.file_id, byte_count, row_count, column_ids
        );

        output.index_size = byte_count;
        output.row_count = row_count;
        output.columns = column_ids;
    }

    fn fill_fulltext_index_output(
        &mut self,
        output: &mut FulltextIndexOutput,
        row_count: RowCount,
        byte_count: ByteCount,
        column_ids: Vec<ColumnId>,
    ) {
        debug!(
            "Full-text index created, region_id: {}, file_id: {}, written_bytes: {}, written_rows: {}, columns: {:?}",
            self.region_id, self.file_id, byte_count, row_count, column_ids
        );

        output.index_size = byte_count;
        output.row_count = row_count;
        output.columns = column_ids;
    }

    fn fill_bloom_filter_output(
        &mut self,
        output: &mut BloomFilterOutput,
        row_count: RowCount,
        byte_count: ByteCount,
        column_ids: Vec<ColumnId>,
    ) {
        debug!(
            "Bloom filter created, region_id: {}, file_id: {}, written_bytes: {}, written_rows: {}, columns: {:?}",
            self.region_id, self.file_id, byte_count, row_count, column_ids
        );

        output.index_size = byte_count;
        output.row_count = row_count;
        output.columns = column_ids;
    }

    #[cfg(feature = "vector_index")]
    async fn do_finish_vector_index(
        &mut self,
        puffin_writer: &mut SstPuffinWriter,
        index_output: &mut IndexOutput,
    ) -> bool {
        let Some(mut indexer) = self.vector_indexer.take() else {
            return true;
        };

        let column_ids = indexer.column_ids().collect();
        let err = match indexer.finish(puffin_writer).await {
            Ok((row_count, byte_count)) => {
                self.fill_vector_index_output(
                    &mut index_output.vector_index,
                    row_count,
                    byte_count,
                    column_ids,
                );
                return true;
            }
            Err(err) => err,
        };

        if cfg!(any(test, feature = "test")) {
            panic!(
                "Failed to finish vector index, region_id: {}, file_id: {}, err: {:?}",
                self.region_id, self.file_id, err
            );
        } else {
            warn!(
                err; "Failed to finish vector index, region_id: {}, file_id: {}",
                self.region_id, self.file_id,
            );
        }

        false
    }

    #[cfg(feature = "vector_index")]
    fn fill_vector_index_output(
        &mut self,
        output: &mut VectorIndexOutput,
        row_count: RowCount,
        byte_count: ByteCount,
        column_ids: Vec<ColumnId>,
    ) {
        debug!(
            "Vector index created, region_id: {}, file_id: {}, written_bytes: {}, written_rows: {}, columns: {:?}",
            self.region_id, self.file_id, byte_count, row_count, column_ids
        );

        output.index_size = byte_count;
        output.row_count = row_count;
        output.columns = column_ids;
    }

    pub(crate) async fn do_prune_intm_sst_dir(&mut self) {
        if let Some(manager) = self.intermediate_manager.take()
            && let Err(e) = manager.prune_sst_dir(&self.region_id, &self.file_id).await
        {
            warn!(e; "Failed to prune intermediate SST directory, region_id: {}, file_id: {}", self.region_id, self.file_id);
        }
    }
}
