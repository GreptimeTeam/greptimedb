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
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use common_telemetry::{debug, warn};
use datatypes::schema::SkippingIndexType;
use index::bloom_filter::creator::BloomFilterCreator;
use puffin::puffin_manager::{PuffinWriter, PutOptions};
use snafu::{ensure, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use crate::error::{
    BiErrorsSnafu, BloomFilterFinishSnafu, IndexOptionsSnafu, OperateAbortedIndexSnafu,
    PuffinAddBlobSnafu, PushBloomFilterValueSnafu, Result,
};
use crate::read::Batch;
use crate::row_converter::SortField;
use crate::sst::file::FileId;
use crate::sst::index::bloom_filter::INDEX_BLOB_TYPE;
use crate::sst::index::codec::{IndexValueCodec, IndexValuesCodec};
use crate::sst::index::intermediate::{
    IntermediateLocation, IntermediateManager, TempFileProvider,
};
use crate::sst::index::puffin_manager::SstPuffinWriter;
use crate::sst::index::statistics::{ByteCount, RowCount, Statistics};
use crate::sst::index::TYPE_BLOOM_FILTER_INDEX;

/// The buffer size for the pipe used to send index data to the puffin blob.
const PIPE_BUFFER_SIZE_FOR_SENDING_BLOB: usize = 8192;

/// The indexer for the bloom filter index.
pub struct BloomFilterIndexer {
    /// The bloom filter creators.
    creators: HashMap<ColumnId, BloomFilterCreator>,

    /// The provider for intermediate files.
    temp_file_provider: Arc<TempFileProvider>,

    /// Codec for decoding primary keys.
    codec: IndexValuesCodec,

    /// Whether the indexing process has been aborted.
    aborted: bool,

    /// The statistics of the indexer.
    stats: Statistics,

    /// The global memory usage.
    global_memory_usage: Arc<AtomicUsize>,
}

impl BloomFilterIndexer {
    /// Creates a new bloom filter indexer.
    pub fn new(
        sst_file_id: FileId,
        metadata: &RegionMetadataRef,
        intermediate_manager: IntermediateManager,
        memory_usage_threshold: Option<usize>,
    ) -> Result<Option<Self>> {
        let mut creators = HashMap::new();

        let temp_file_provider = Arc::new(TempFileProvider::new(
            IntermediateLocation::new(&metadata.region_id, &sst_file_id),
            intermediate_manager,
        ));
        let global_memory_usage = Arc::new(AtomicUsize::new(0));

        for column in &metadata.column_metadatas {
            let options =
                column
                    .column_schema
                    .skipping_index_options()
                    .context(IndexOptionsSnafu {
                        column_name: &column.column_schema.name,
                    })?;

            let options = match options {
                Some(options) if options.index_type == SkippingIndexType::BloomFilter => options,
                _ => continue,
            };

            let creator = BloomFilterCreator::new(
                options.granularity as _,
                temp_file_provider.clone(),
                global_memory_usage.clone(),
                memory_usage_threshold,
            );
            creators.insert(column.column_id, creator);
        }

        if creators.is_empty() {
            return Ok(None);
        }

        let codec = IndexValuesCodec::from_tag_columns(
            metadata.primary_key_encoding,
            metadata.primary_key_columns(),
        );
        let indexer = Self {
            creators,
            temp_file_provider,
            codec,
            aborted: false,
            stats: Statistics::new(TYPE_BLOOM_FILTER_INDEX),
            global_memory_usage,
        };
        Ok(Some(indexer))
    }

    /// Updates index with a batch of rows.
    /// Garbage will be cleaned up if failed to update.
    ///
    /// TODO(zhongzc): duplicate with `mito2::sst::index::inverted_index::creator::InvertedIndexCreator`
    pub async fn update(&mut self, batch: &mut Batch) -> Result<()> {
        ensure!(!self.aborted, OperateAbortedIndexSnafu);

        if self.creators.is_empty() {
            return Ok(());
        }

        if let Err(update_err) = self.do_update(batch).await {
            // clean up garbage if failed to update
            if let Err(err) = self.do_cleanup().await {
                if cfg!(any(test, feature = "test")) {
                    panic!("Failed to clean up index creator, err: {err:?}",);
                } else {
                    warn!(err; "Failed to clean up index creator");
                }
            }
            return Err(update_err);
        }

        Ok(())
    }

    /// Finishes index creation and cleans up garbage.
    /// Returns the number of rows and bytes written.
    ///
    /// TODO(zhongzc): duplicate with `mito2::sst::index::inverted_index::creator::InvertedIndexCreator`
    pub async fn finish(
        &mut self,
        puffin_writer: &mut SstPuffinWriter,
    ) -> Result<(RowCount, ByteCount)> {
        ensure!(!self.aborted, OperateAbortedIndexSnafu);

        if self.stats.row_count() == 0 {
            // no IO is performed, no garbage to clean up, just return
            return Ok((0, 0));
        }

        let finish_res = self.do_finish(puffin_writer).await;
        // clean up garbage no matter finish successfully or not
        if let Err(err) = self.do_cleanup().await {
            if cfg!(any(test, feature = "test")) {
                panic!("Failed to clean up index creator, err: {err:?}",);
            } else {
                warn!(err; "Failed to clean up index creator");
            }
        }

        finish_res.map(|_| (self.stats.row_count(), self.stats.byte_count()))
    }

    /// Aborts index creation and clean up garbage.
    ///
    /// TODO(zhongzc): duplicate with `mito2::sst::index::inverted_index::creator::InvertedIndexCreator`
    pub async fn abort(&mut self) -> Result<()> {
        if self.aborted {
            return Ok(());
        }
        self.aborted = true;

        self.do_cleanup().await
    }

    async fn do_update(&mut self, batch: &mut Batch) -> Result<()> {
        let mut guard = self.stats.record_update();

        let n = batch.num_rows();
        guard.inc_row_count(n);

        for (col_id, creator) in &mut self.creators {
            match self.codec.pk_col_info(*col_id) {
                // tags
                Some(col_info) => {
                    let pk_idx = col_info.idx;
                    let field = &col_info.field;
                    let elems = batch
                        .pk_col_value(self.codec.decoder(), pk_idx, *col_id)?
                        .filter(|v| !v.is_null())
                        .map(|v| {
                            let mut buf = vec![];
                            IndexValueCodec::encode_nonnull_value(
                                v.as_value_ref(),
                                field,
                                &mut buf,
                            )?;
                            Ok(buf)
                        })
                        .transpose()?;
                    creator
                        .push_n_row_elems(n, elems)
                        .await
                        .context(PushBloomFilterValueSnafu)?;
                }
                // fields
                None => {
                    let Some(values) = batch.field_col_value(*col_id) else {
                        debug!(
                            "Column {} not found in the batch during building bloom filter index",
                            col_id
                        );
                        continue;
                    };
                    let sort_field = SortField::new(values.data.data_type());
                    for i in 0..n {
                        let value = values.data.get_ref(i);
                        let elems = (!value.is_null())
                            .then(|| {
                                let mut buf = vec![];
                                IndexValueCodec::encode_nonnull_value(
                                    value,
                                    &sort_field,
                                    &mut buf,
                                )?;
                                Ok(buf)
                            })
                            .transpose()?;

                        creator
                            .push_row_elems(elems)
                            .await
                            .context(PushBloomFilterValueSnafu)?;
                    }
                }
            }
        }

        Ok(())
    }

    /// TODO(zhongzc): duplicate with `mito2::sst::index::inverted_index::creator::InvertedIndexCreator`
    async fn do_finish(&mut self, puffin_writer: &mut SstPuffinWriter) -> Result<()> {
        let mut guard = self.stats.record_finish();

        for (id, creator) in &mut self.creators {
            let written_bytes = Self::do_finish_single_creator(id, creator, puffin_writer).await?;
            guard.inc_byte_count(written_bytes);
        }

        Ok(())
    }

    async fn do_cleanup(&mut self) -> Result<()> {
        let mut _guard = self.stats.record_cleanup();

        self.creators.clear();
        self.temp_file_provider.cleanup().await
    }

    /// Data flow of finishing index:
    ///
    /// ```text
    ///                               (In Memory Buffer)
    ///                                    ┌──────┐
    ///  ┌─────────────┐                   │ PIPE │
    ///  │             │ write index data  │      │
    ///  │ IndexWriter ├──────────────────►│ tx   │
    ///  │             │                   │      │
    ///  └─────────────┘                   │      │
    ///                  ┌─────────────────┤ rx   │
    ///  ┌─────────────┐ │ read as blob    └──────┘
    ///  │             │ │
    ///  │ PuffinWriter├─┤
    ///  │             │ │ copy to file    ┌──────┐
    ///  └─────────────┘ └────────────────►│ File │
    ///                                    └──────┘
    /// ```
    ///
    /// TODO(zhongzc): duplicate with `mito2::sst::index::inverted_index::creator::InvertedIndexCreator`
    async fn do_finish_single_creator(
        col_id: &ColumnId,
        creator: &mut BloomFilterCreator,
        puffin_writer: &mut SstPuffinWriter,
    ) -> Result<ByteCount> {
        let (tx, rx) = tokio::io::duplex(PIPE_BUFFER_SIZE_FOR_SENDING_BLOB);

        let blob_name = format!("{}-{}", INDEX_BLOB_TYPE, col_id);
        let (index_finish, puffin_add_blob) = futures::join!(
            creator.finish(tx.compat_write()),
            puffin_writer.put_blob(
                &blob_name,
                rx.compat(),
                PutOptions::default(),
                Default::default(),
            )
        );

        match (
            puffin_add_blob.context(PuffinAddBlobSnafu),
            index_finish.context(BloomFilterFinishSnafu),
        ) {
            (Err(e1), Err(e2)) => BiErrorsSnafu {
                first: Box::new(e1),
                second: Box::new(e2),
            }
            .fail()?,

            (Ok(_), e @ Err(_)) => e?,
            (e @ Err(_), Ok(_)) => e.map(|_| ())?,
            (Ok(written_bytes), Ok(_)) => {
                return Ok(written_bytes);
            }
        }

        Ok(0)
    }

    /// Returns the memory usage of the indexer.
    pub fn memory_usage(&self) -> usize {
        self.global_memory_usage
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Returns the column ids to be indexed.
    pub fn column_ids(&self) -> impl Iterator<Item = ColumnId> + use<'_> {
        self.creators.keys().copied()
    }
}

#[cfg(test)]
pub(crate) mod tests {

    use api::v1::SemanticType;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, SkippingIndexOptions};
    use datatypes::value::ValueRef;
    use datatypes::vectors::{UInt64Vector, UInt8Vector};
    use index::bloom_filter::reader::{BloomFilterReader, BloomFilterReaderImpl};
    use object_store::services::Memory;
    use object_store::ObjectStore;
    use puffin::puffin_manager::{PuffinManager, PuffinReader};
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::RegionId;

    use super::*;
    use crate::access_layer::FilePathProvider;
    use crate::read::BatchColumn;
    use crate::row_converter::{DensePrimaryKeyCodec, PrimaryKeyCodecExt};
    use crate::sst::index::puffin_manager::PuffinManagerFactory;

    pub fn mock_object_store() -> ObjectStore {
        ObjectStore::new(Memory::default()).unwrap().finish()
    }

    pub async fn new_intm_mgr(path: impl AsRef<str>) -> IntermediateManager {
        IntermediateManager::init_fs(path).await.unwrap()
    }

    pub struct TestPathProvider;

    impl FilePathProvider for TestPathProvider {
        fn build_index_file_path(&self, file_id: FileId) -> String {
            file_id.to_string()
        }

        fn build_sst_file_path(&self, file_id: FileId) -> String {
            file_id.to_string()
        }
    }

    /// tag_str:
    ///   - type: string
    ///   - index: bloom filter
    ///   - granularity: 2
    ///   - column_id: 1
    ///
    /// ts:
    ///   - type: timestamp
    ///   - index: time index
    ///   - column_id: 2
    ///
    /// field_u64:
    ///   - type: uint64
    ///   - index: bloom filter
    ///   - granularity: 4
    ///   - column_id: 3
    pub fn mock_region_metadata() -> RegionMetadataRef {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 2));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "tag_str",
                    ConcreteDataType::string_datatype(),
                    false,
                )
                .with_skipping_options(SkippingIndexOptions {
                    index_type: SkippingIndexType::BloomFilter,
                    granularity: 2,
                })
                .unwrap(),
                semantic_type: SemanticType::Tag,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "field_u64",
                    ConcreteDataType::uint64_datatype(),
                    false,
                )
                .with_skipping_options(SkippingIndexOptions {
                    index_type: SkippingIndexType::BloomFilter,
                    granularity: 4,
                })
                .unwrap(),
                semantic_type: SemanticType::Field,
                column_id: 3,
            })
            .primary_key(vec![1]);

        Arc::new(builder.build().unwrap())
    }

    pub fn new_batch(str_tag: impl AsRef<str>, u64_field: impl IntoIterator<Item = u64>) -> Batch {
        let fields = vec![(0, SortField::new(ConcreteDataType::string_datatype()))];
        let codec = DensePrimaryKeyCodec::with_fields(fields);
        let row: [ValueRef; 1] = [str_tag.as_ref().into()];
        let primary_key = codec.encode(row.into_iter()).unwrap();

        let u64_field = BatchColumn {
            column_id: 3,
            data: Arc::new(UInt64Vector::from_iter_values(u64_field)),
        };
        let num_rows = u64_field.data.len();

        Batch::new(
            primary_key,
            Arc::new(UInt64Vector::from_iter_values(std::iter::repeat_n(
                0, num_rows,
            ))),
            Arc::new(UInt64Vector::from_iter_values(std::iter::repeat_n(
                0, num_rows,
            ))),
            Arc::new(UInt8Vector::from_iter_values(std::iter::repeat_n(
                1, num_rows,
            ))),
            vec![u64_field],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_bloom_filter_indexer() {
        let prefix = "test_bloom_filter_indexer_";
        let tempdir = common_test_util::temp_dir::create_temp_dir(prefix);
        let object_store = mock_object_store();
        let intm_mgr = new_intm_mgr(tempdir.path().to_string_lossy()).await;
        let region_metadata = mock_region_metadata();
        let memory_usage_threshold = Some(1024);

        let mut indexer = BloomFilterIndexer::new(
            FileId::random(),
            &region_metadata,
            intm_mgr,
            memory_usage_threshold,
        )
        .unwrap()
        .unwrap();

        // push 20 rows
        let mut batch = new_batch("tag1", 0..10);
        indexer.update(&mut batch).await.unwrap();

        let mut batch = new_batch("tag2", 10..20);
        indexer.update(&mut batch).await.unwrap();

        let (_d, factory) = PuffinManagerFactory::new_for_test_async(prefix).await;
        let puffin_manager = factory.build(object_store, TestPathProvider);

        let file_id = FileId::random();
        let mut puffin_writer = puffin_manager.writer(&file_id).await.unwrap();
        let (row_count, byte_count) = indexer.finish(&mut puffin_writer).await.unwrap();
        assert_eq!(row_count, 20);
        assert!(byte_count > 0);
        puffin_writer.finish().await.unwrap();

        let puffin_reader = puffin_manager.reader(&file_id).await.unwrap();

        // tag_str
        {
            let blob_guard = puffin_reader
                .blob("greptime-bloom-filter-v1-1")
                .await
                .unwrap();
            let reader = blob_guard.reader().await.unwrap();
            let bloom_filter = BloomFilterReaderImpl::new(reader);
            let metadata = bloom_filter.metadata().await.unwrap();

            assert_eq!(metadata.segment_count, 10);
            for i in 0..5 {
                let loc = &metadata.bloom_filter_locs[metadata.segment_loc_indices[i] as usize];
                let bf = bloom_filter.bloom_filter(loc).await.unwrap();
                assert!(bf.contains(b"tag1"));
            }
            for i in 5..10 {
                let loc = &metadata.bloom_filter_locs[metadata.segment_loc_indices[i] as usize];
                let bf = bloom_filter.bloom_filter(loc).await.unwrap();
                assert!(bf.contains(b"tag2"));
            }
        }

        // field_u64
        {
            let sort_field = SortField::new(ConcreteDataType::uint64_datatype());

            let blob_guard = puffin_reader
                .blob("greptime-bloom-filter-v1-3")
                .await
                .unwrap();
            let reader = blob_guard.reader().await.unwrap();
            let bloom_filter = BloomFilterReaderImpl::new(reader);
            let metadata = bloom_filter.metadata().await.unwrap();

            assert_eq!(metadata.segment_count, 5);
            for i in 0u64..20 {
                let idx = i as usize / 4;
                let loc = &metadata.bloom_filter_locs[metadata.segment_loc_indices[idx] as usize];
                let bf = bloom_filter.bloom_filter(loc).await.unwrap();
                let mut buf = vec![];
                IndexValueCodec::encode_nonnull_value(ValueRef::UInt64(i), &sort_field, &mut buf)
                    .unwrap();

                assert!(bf.contains(&buf));
            }
        }
    }
}
