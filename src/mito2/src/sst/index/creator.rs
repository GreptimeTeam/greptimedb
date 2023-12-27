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
use std::num::NonZeroUsize;
use std::sync::Arc;

use async_trait::async_trait;
use common_telemetry::logging;
use futures::{AsyncRead, AsyncWrite};
use index::inverted_index::create::sort::external_provider::ExternalTempFileProvider;
use index::inverted_index::create::sort::external_sort::ExternalSorter;
use index::inverted_index::create::sort_create::SortIndexCreator;
use index::inverted_index::create::InvertedIndexCreator;
use index::inverted_index::error::Result as IndexResult;
use index::inverted_index::format::writer::InvertedIndexBlobWriter;
use object_store::{util, ObjectStore};
use puffin::file_format::writer::{Blob, PuffinAsyncWriter, PuffinFileWriter};
use snafu::ResultExt;
use store_api::metadata::RegionMetadataRef;
use tokio::io::duplex;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use crate::error::{PushIndexValueSnafu, Result};
use crate::read::Batch;
use crate::sst::file::FileId;
use crate::sst::index::codec::{IndexValueCodec, IndexValuesCodec};
use crate::sst::location;

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

    row_count: RowCount,
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
        let temp_file_provider = Arc::new(TempFileProvider {
            temp_file_dir: location::index_creation_temp_dir(&region_dir, &sst_file_id),
            object_store: object_store.clone(),
        });
        let memory_usage_threshold = memory_usage_threshold
            .map(|threshold| (threshold / metadata.primary_key.len()).max(1024));
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
            row_count: 0,
        }
    }

    pub async fn update(&mut self, batch: &Batch) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        if let Err(err) = self.do_update(batch).await {
            if let Err(err) = self.cleanup().await {
                logging::warn!(
                    "Failed to clean up index creator, region_dir: {}, sst_file_id: {}, error: {err}",
                    self.region_dir, self.sst_file_id
                );
            }
            return Err(err);
        }

        Ok(())
    }

    pub async fn finish(&mut self) -> Result<(RowCount, ByteCount)> {
        if self.row_count == 0 {
            return Ok((0, 0));
        }

        let res = self.do_finish().await;

        if let Err(err) = self.cleanup().await {
            logging::warn!(
                "Failed to clean up index creator, region_dir: {}, sst_file_id: {}, error: {err}",
                self.region_dir,
                self.sst_file_id
            );
        }

        res.map(|bytes| (self.row_count, bytes))
    }

    async fn do_update(&mut self, batch: &Batch) -> Result<()> {
        let n = batch.num_rows();
        self.row_count += n;
        for (column_name, field, value) in self.codec.decode(batch.primary_key())? {
            if let Some(value) = value.as_ref() {
                self.value_buf.clear();
                IndexValueCodec::encode_value(value.as_value_ref(), field, &mut self.value_buf)?;
            }

            let v = value.is_some().then(|| self.value_buf.as_slice());
            self.index_creator
                .push_with_name_n(&column_name, v, n)
                .await
                .context(PushIndexValueSnafu)?;
        }

        Ok(())
    }

    async fn do_finish(&mut self) -> Result<ByteCount> {
        let file_path = location::index_file_path(&self.region_dir, &self.sst_file_id);
        let writer = self.object_store.writer(&file_path).await.unwrap();
        let mut puffin_writer = PuffinFileWriter::new(writer);

        let (tx, rx) = duplex(8 * 1024);

        let blob = Blob {
            blob_type: "greptime-inverted-index-v1".to_string(),
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

        Ok(puffin_writer.finish().await.unwrap())
    }

    async fn cleanup(&mut self) -> Result<()> {
        self.temp_file_provider.cleanup().await
    }
}

struct TempFileProvider {
    temp_file_dir: String,
    object_store: ObjectStore,
}

#[async_trait]
impl ExternalTempFileProvider for TempFileProvider {
    async fn create(
        &self,
        index_name: &str,
        tmp_file_id: &str,
    ) -> IndexResult<Box<dyn AsyncWrite + Unpin + Send>> {
        let child = format!("{index_name}/{tmp_file_id}.im");
        let path = util::join_path(&self.temp_file_dir, &child);
        Ok(Box::new(self.object_store.writer(&path).await.unwrap()))
    }

    async fn read_all(
        &self,
        index_name: &str,
    ) -> IndexResult<Vec<Box<dyn AsyncRead + Unpin + Send>>> {
        let dir = util::join_dir(&self.temp_file_dir, index_name);

        let entries = self.object_store.list(&dir).await.unwrap();
        let mut readers = Vec::with_capacity(entries.len());

        for entry in entries {
            let path = entry.path();
            readers.push(Box::new(self.object_store.reader(path).await.unwrap()) as _);
        }

        Ok(readers)
    }
}

impl TempFileProvider {
    async fn cleanup(&self) -> Result<()> {
        self.object_store
            .remove_all(&self.temp_file_dir)
            .await
            .unwrap();
        Ok(())
    }
}
