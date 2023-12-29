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

mod statistics;
mod temp_provider;

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

use common_telemetry::warn;
use index::inverted_index::create::sort::external_sort::ExternalSorter;
use index::inverted_index::create::sort_create::SortIndexCreator;
use index::inverted_index::create::InvertedIndexCreator;
use index::inverted_index::format::writer::InvertedIndexBlobWriter;
use object_store::ObjectStore;
use puffin::file_format::writer::{Blob, PuffinAsyncWriter, PuffinFileWriter};
use snafu::ResultExt;
use store_api::metadata::RegionMetadataRef;
use tokio::io::duplex;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use crate::error::{OpenDalSnafu, PushIndexValueSnafu, Result};
use crate::read::Batch;
use crate::sst::file::FileId;
use crate::sst::index::codec::{IndexValueCodec, IndexValuesCodec};
use crate::sst::index::creator::statistics::Statistics;
use crate::sst::index::creator::temp_provider::TempFileProvider;
use crate::sst::index::{
    INDEX_BLOB_TYPE, MIN_MEMORY_USAGE_THRESHOLD, PIPE_BUFFER_SIZE_FOR_SENDING_BLOB,
};
use crate::sst::location::{self, IntermediateLocation};

type ByteCount = usize;
type RowCount = usize;

pub struct SstIndexCreator {
    region_dir: String,
    sst_file_id: FileId,
    object_store: ObjectStore,

    codec: IndexValuesCodec,
    index_creator: Box<dyn InvertedIndexCreator>,

    temp_file_provider: Arc<TempFileProvider>,
    value_buf: Vec<u8>,

    stats: Statistics,
}

impl SstIndexCreator {
    pub fn new(
        region_dir: String,
        sst_file_id: FileId,
        metadata: &RegionMetadataRef,
        object_store: ObjectStore,
        memory_usage_threshold: Option<usize>,
        row_group_size: NonZeroUsize,
    ) -> Self {
        let temp_file_provider = Arc::new(TempFileProvider::new(
            IntermediateLocation::new(&region_dir, &sst_file_id),
            object_store.clone(),
        ));
        let memory_usage_threshold = memory_usage_threshold.map(|threshold| {
            (threshold / metadata.primary_key.len()).max(MIN_MEMORY_USAGE_THRESHOLD)
        });
        let sorter =
            ExternalSorter::factory(temp_file_provider.clone() as _, memory_usage_threshold);
        let index_creator = Box::new(SortIndexCreator::new(sorter, row_group_size));

        let codec = IndexValuesCodec::from_tag_columns(metadata.primary_key_columns());
        Self {
            region_dir,
            sst_file_id,
            object_store,
            codec,
            index_creator,
            temp_file_provider,
            value_buf: vec![],
            stats: Statistics::default(),
        }
    }

    pub async fn update(&mut self, batch: &Batch) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        if let Err(err) = self.do_update(batch).await {
            // clean up garbage if failed to update
            if let Err(err) = self.do_cleanup().await {
                let region_dir = &self.region_dir;
                let sst_file_id = &self.sst_file_id;
                warn!(err; "Failed to clean up index creator, region_dir: {region_dir}, sst_file_id: {sst_file_id}");
            }
            return Err(err);
        }

        Ok(())
    }

    pub async fn finish(&mut self) -> Result<(RowCount, ByteCount)> {
        if self.stats.row_count() == 0 {
            // no IO is performed, no garbage to clean up, just return
            return Ok((0, 0));
        }

        let finish_res = self.do_finish().await;
        // clean up garbage no matter finish success or not
        let cleanup_res = self.do_cleanup().await;

        if let Err(err) = cleanup_res {
            let region_dir = &self.region_dir;
            let sst_file_id = &self.sst_file_id;
            warn!(err; "Failed to clean up index creator, region_dir: {region_dir}, sst_file_id: {sst_file_id}");
        }

        finish_res.map(|_| (self.stats.row_count(), self.stats.byte_count()))
    }

    async fn do_update(&mut self, batch: &Batch) -> Result<()> {
        let mut guard = self.stats.record_update();

        let n = batch.num_rows();
        guard.inc_row_count(n);
        for (column_name, field, value) in self.codec.decode(batch.primary_key())? {
            if let Some(value) = value.as_ref() {
                self.value_buf.clear();
                IndexValueCodec::encode_value(value.as_value_ref(), field, &mut self.value_buf)?;
            }

            let v = value.is_some().then_some(self.value_buf.as_slice());
            self.index_creator
                .push_with_name_n(column_name, v, n)
                .await
                .context(PushIndexValueSnafu)?;
        }

        Ok(())
    }

    async fn do_finish(&mut self) -> Result<()> {
        let mut guard = self.stats.record_finish();

        let file_path = location::index_file_path(&self.region_dir, &self.sst_file_id);
        let writer = self
            .object_store
            .writer(&file_path)
            .await
            .context(OpenDalSnafu)?;
        let mut puffin_writer = PuffinFileWriter::new(writer);

        let (tx, rx) = duplex(PIPE_BUFFER_SIZE_FOR_SENDING_BLOB);

        let blob = Blob {
            blob_type: INDEX_BLOB_TYPE.to_string(),
            data: rx.compat(),
            properties: HashMap::default(),
        };

        let mut index_writer = InvertedIndexBlobWriter::new(tx.compat_write());
        let (source, sink) = futures::join!(
            self.index_creator.finish(&mut index_writer),
            puffin_writer.add_blob(blob)
        );

        source.unwrap();
        sink.unwrap();

        let byte_count = puffin_writer.finish().await.unwrap();
        guard.inc_byte_count(byte_count);
        Ok(())
    }

    async fn do_cleanup(&mut self) -> Result<()> {
        let _guard = self.stats.record_cleanup();
        self.temp_file_provider.cleanup().await
    }
}
