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

use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use common_telemetry::warn;
use index::inverted_index::create::sort::external_sort::ExternalSorter;
use index::inverted_index::create::sort_create::SortIndexCreator;
use index::inverted_index::create::InvertedIndexCreator;
use index::inverted_index::format::writer::InvertedIndexBlobWriter;
use object_store::ObjectStore;
use puffin::file_format::writer::{Blob, PuffinAsyncWriter, PuffinFileWriter};
use snafu::{ensure, ResultExt};
use store_api::metadata::RegionMetadataRef;
use tokio::io::duplex;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use crate::error::{
    BiSnafu, IndexFinishSnafu, OperateAbortedIndexSnafu, PuffinAddBlobSnafu, PuffinFinishSnafu,
    PushIndexValueSnafu, Result,
};
use crate::metrics::{
    INDEX_PUFFIN_FLUSH_OP_TOTAL, INDEX_PUFFIN_WRITE_BYTES_TOTAL, INDEX_PUFFIN_WRITE_OP_TOTAL,
};
use crate::read::Batch;
use crate::sst::file::FileId;
use crate::sst::index::codec::{ColumnId, IndexValueCodec, IndexValuesCodec};
use crate::sst::index::creator::statistics::Statistics;
use crate::sst::index::creator::temp_provider::TempFileProvider;
use crate::sst::index::intermediate::{IntermediateLocation, IntermediateManager};
use crate::sst::index::store::InstrumentedStore;
use crate::sst::index::INDEX_BLOB_TYPE;

/// The minimum memory usage threshold for one column.
const MIN_MEMORY_USAGE_THRESHOLD_PER_COLUMN: usize = 1024 * 1024; // 1MB

/// The buffer size for the pipe used to send index data to the puffin blob.
const PIPE_BUFFER_SIZE_FOR_SENDING_BLOB: usize = 8192;

type ByteCount = u64;
type RowCount = usize;

/// Creates SST index.
pub struct SstIndexCreator {
    /// Path of index file to write.
    file_path: String,
    /// The store to write index files.
    store: InstrumentedStore,
    /// The index creator.
    index_creator: Box<dyn InvertedIndexCreator>,
    /// The provider of intermediate files.
    temp_file_provider: Arc<TempFileProvider>,

    /// Codec for decoding primary keys.
    codec: IndexValuesCodec,
    /// Reusable buffer for encoding index values.
    value_buf: Vec<u8>,

    /// Statistics of index creation.
    stats: Statistics,
    /// Whether the index creation is aborted.
    aborted: bool,

    /// Ignore column IDs for index creation.
    ignore_column_ids: HashSet<ColumnId>,

    /// The memory usage of the index creator.
    memory_usage: Arc<AtomicUsize>,
}

impl SstIndexCreator {
    /// Creates a new `SstIndexCreator`.
    /// Should ensure that the number of tag columns is greater than 0.
    pub fn new(
        file_path: String,
        sst_file_id: FileId,
        metadata: &RegionMetadataRef,
        index_store: ObjectStore,
        intermediate_manager: IntermediateManager,
        memory_usage_threshold: Option<usize>,
        segment_row_count: NonZeroUsize,
    ) -> Self {
        let temp_file_provider = Arc::new(TempFileProvider::new(
            IntermediateLocation::new(&metadata.region_id, &sst_file_id),
            intermediate_manager,
        ));

        let memory_usage = Arc::new(AtomicUsize::new(0));

        let sorter = ExternalSorter::factory(
            temp_file_provider.clone() as _,
            Some(MIN_MEMORY_USAGE_THRESHOLD_PER_COLUMN),
            memory_usage.clone(),
            memory_usage_threshold,
        );
        let index_creator = Box::new(SortIndexCreator::new(sorter, segment_row_count));

        let codec = IndexValuesCodec::from_tag_columns(metadata.primary_key_columns());
        Self {
            file_path,
            store: InstrumentedStore::new(index_store),
            codec,
            index_creator,
            temp_file_provider,

            value_buf: vec![],

            stats: Statistics::default(),
            aborted: false,

            ignore_column_ids: HashSet::default(),
            memory_usage,
        }
    }

    /// Sets the write buffer size of the store.
    pub fn with_buffer_size(mut self, write_buffer_size: Option<usize>) -> Self {
        self.store = self.store.with_write_buffer_size(write_buffer_size);
        self
    }

    /// Sets the ignore column IDs for index creation.
    pub fn with_ignore_column_ids(mut self, ignore_column_ids: HashSet<ColumnId>) -> Self {
        self.ignore_column_ids = ignore_column_ids;
        self
    }

    /// Updates index with a batch of rows.
    /// Garbage will be cleaned up if failed to update.
    pub async fn update(&mut self, batch: &Batch) -> Result<()> {
        ensure!(!self.aborted, OperateAbortedIndexSnafu);

        if batch.is_empty() {
            return Ok(());
        }

        if let Err(update_err) = self.do_update(batch).await {
            // clean up garbage if failed to update
            if let Err(err) = self.do_cleanup().await {
                if cfg!(any(test, feature = "test")) {
                    panic!(
                        "Failed to clean up index creator, file_path: {}, err: {}",
                        self.file_path, err
                    );
                } else {
                    warn!(err; "Failed to clean up index creator, file_path: {}", self.file_path);
                }
            }
            return Err(update_err);
        }

        Ok(())
    }

    /// Finishes index creation and cleans up garbage.
    /// Returns the number of rows and bytes written.
    pub async fn finish(&mut self) -> Result<(RowCount, ByteCount)> {
        ensure!(!self.aborted, OperateAbortedIndexSnafu);

        if self.stats.row_count() == 0 {
            // no IO is performed, no garbage to clean up, just return
            return Ok((0, 0));
        }

        let finish_res = self.do_finish().await;
        // clean up garbage no matter finish successfully or not
        if let Err(err) = self.do_cleanup().await {
            if cfg!(any(test, feature = "test")) {
                panic!(
                    "Failed to clean up index creator, file_path: {}, err: {}",
                    self.file_path, err
                );
            } else {
                warn!(err; "Failed to clean up index creator, file_path: {}", self.file_path);
            }
        }

        finish_res.map(|_| (self.stats.row_count(), self.stats.byte_count()))
    }

    /// Aborts index creation and clean up garbage.
    pub async fn abort(&mut self) -> Result<()> {
        if self.aborted {
            return Ok(());
        }
        self.aborted = true;

        self.do_cleanup().await
    }

    async fn do_update(&mut self, batch: &Batch) -> Result<()> {
        let mut guard = self.stats.record_update();

        let n = batch.num_rows();
        guard.inc_row_count(n);

        for (column_id, field, value) in self.codec.decode(batch.primary_key())? {
            if self.ignore_column_ids.contains(column_id) {
                continue;
            }

            if let Some(value) = value.as_ref() {
                self.value_buf.clear();
                IndexValueCodec::encode_nonnull_value(
                    value.as_value_ref(),
                    field,
                    &mut self.value_buf,
                )?;
            }

            // non-null value -> Some(encoded_bytes), null value -> None
            let value = value.is_some().then_some(self.value_buf.as_slice());
            self.index_creator
                .push_with_name_n(column_id, value, n)
                .await
                .context(PushIndexValueSnafu)?;
        }

        Ok(())
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
    async fn do_finish(&mut self) -> Result<()> {
        let mut guard = self.stats.record_finish();

        let file_writer = self
            .store
            .writer(
                &self.file_path,
                &INDEX_PUFFIN_WRITE_BYTES_TOTAL,
                &INDEX_PUFFIN_WRITE_OP_TOTAL,
                &INDEX_PUFFIN_FLUSH_OP_TOTAL,
            )
            .await?;
        let mut puffin_writer = PuffinFileWriter::new(file_writer);

        let (tx, rx) = duplex(PIPE_BUFFER_SIZE_FOR_SENDING_BLOB);
        let blob = Blob {
            blob_type: INDEX_BLOB_TYPE.to_string(),
            compressed_data: rx.compat(),
            properties: HashMap::default(),
            compression_codec: None,
        };
        let mut index_writer = InvertedIndexBlobWriter::new(tx.compat_write());

        let (index_finish, puffin_add_blob) = futures::join!(
            self.index_creator.finish(&mut index_writer),
            puffin_writer.add_blob(blob)
        );

        match (
            puffin_add_blob.context(PuffinAddBlobSnafu),
            index_finish.context(IndexFinishSnafu),
        ) {
            (Err(e1), Err(e2)) => BiSnafu {
                first: Box::new(e1),
                second: Box::new(e2),
            }
            .fail()?,

            (Ok(_), e @ Err(_)) => e?,
            (e @ Err(_), Ok(_)) => e.map(|_| ())?,
            _ => {}
        }

        let byte_count = puffin_writer.finish().await.context(PuffinFinishSnafu)?;
        guard.inc_byte_count(byte_count);
        Ok(())
    }

    async fn do_cleanup(&mut self) -> Result<()> {
        let _guard = self.stats.record_cleanup();

        self.temp_file_provider.cleanup().await
    }

    pub fn memory_usage(&self) -> usize {
        self.memory_usage.load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::iter;

    use api::v1::SemanticType;
    use datafusion_expr::{binary_expr, col, lit, Expr as DfExpr, Operator};
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use datatypes::value::ValueRef;
    use datatypes::vectors::{UInt64Vector, UInt8Vector};
    use futures::future::BoxFuture;
    use index::inverted_index::format::reader::cache::InvertedIndexCache;
    use object_store::services::Memory;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::RegionId;

    use super::*;
    use crate::row_converter::{McmpRowCodec, RowCodec, SortField};
    use crate::sst::index::applier::builder::SstIndexApplierBuilder;
    use crate::sst::location;

    fn mock_object_store() -> ObjectStore {
        ObjectStore::new(Memory::default()).unwrap().finish()
    }

    fn mock_intm_mgr() -> IntermediateManager {
        IntermediateManager::new(mock_object_store())
    }

    fn mock_region_metadata() -> RegionMetadataRef {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 2));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "tag_str",
                    ConcreteDataType::string_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Tag,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "tag_i32",
                    ConcreteDataType::int32_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Tag,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 3,
            })
            .primary_key(vec![1, 2]);

        Arc::new(builder.build().unwrap())
    }

    fn new_batch(num_rows: usize, str_tag: impl AsRef<str>, i32_tag: impl Into<i32>) -> Batch {
        let fields = vec![
            SortField::new(ConcreteDataType::string_datatype()),
            SortField::new(ConcreteDataType::int32_datatype()),
        ];
        let codec = McmpRowCodec::new(fields);
        let row: [ValueRef; 2] = [str_tag.as_ref().into(), i32_tag.into().into()];
        let primary_key = codec.encode(row.into_iter()).unwrap();

        Batch::new(
            primary_key,
            Arc::new(UInt64Vector::from_iter_values(
                iter::repeat(0).take(num_rows),
            )),
            Arc::new(UInt64Vector::from_iter_values(
                iter::repeat(0).take(num_rows),
            )),
            Arc::new(UInt8Vector::from_iter_values(
                iter::repeat(1).take(num_rows),
            )),
            vec![],
        )
        .unwrap()
    }

    async fn build_applier_factory(
        tags: BTreeSet<(&'static str, i32)>,
    ) -> impl Fn(DfExpr) -> BoxFuture<'static, Vec<usize>> {
        let region_dir = "region0".to_string();
        let sst_file_id = FileId::random();
        let file_path = location::index_file_path(&region_dir, sst_file_id);
        let object_store = mock_object_store();
        let region_metadata = mock_region_metadata();
        let intm_mgr = mock_intm_mgr();
        let memory_threshold = None;
        let segment_row_count = 2;

        let mut creator = SstIndexCreator::new(
            file_path,
            sst_file_id,
            &region_metadata,
            object_store.clone(),
            intm_mgr,
            memory_threshold,
            NonZeroUsize::new(segment_row_count).unwrap(),
        );

        for (str_tag, i32_tag) in &tags {
            let batch = new_batch(segment_row_count, str_tag, *i32_tag);
            creator.update(&batch).await.unwrap();
        }

        let (row_count, _) = creator.finish().await.unwrap();
        assert_eq!(row_count, tags.len() * segment_row_count);

        move |expr| {
            let cache = Arc::new(InvertedIndexCache::new(10, 10, 10));
            let applier = SstIndexApplierBuilder::new(
                region_dir.clone(),
                object_store.clone(),
                None,
                Some(cache),
                &region_metadata,
                Default::default(),
            )
            .build(&[expr])
            .unwrap()
            .unwrap();
            Box::pin(async move {
                applier
                    .apply(sst_file_id)
                    .await
                    .unwrap()
                    .matched_segment_ids
                    .iter_ones()
                    .collect()
            })
        }
    }

    #[tokio::test]
    async fn test_create_and_query_get_key() {
        let tags = BTreeSet::from_iter([
            ("aaa", 1),
            ("aaa", 2),
            ("aaa", 3),
            ("aab", 1),
            ("aab", 2),
            ("aab", 3),
            ("abc", 1),
            ("abc", 2),
            ("abc", 3),
        ]);

        let applier_factory = build_applier_factory(tags).await;

        let expr = col("tag_str").eq(lit("aaa"));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![0, 1, 2]);

        let expr = col("tag_i32").eq(lit(2));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![1, 4, 7]);

        let expr = col("tag_str").eq(lit("aaa")).and(col("tag_i32").eq(lit(2)));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![1]);

        let expr = col("tag_str")
            .eq(lit("aaa"))
            .or(col("tag_str").eq(lit("abc")));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![0, 1, 2, 6, 7, 8]);

        let expr = col("tag_str").in_list(vec![lit("aaa"), lit("abc")], false);
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![0, 1, 2, 6, 7, 8]);
    }

    #[tokio::test]
    async fn test_create_and_query_range() {
        let tags = BTreeSet::from_iter([
            ("aaa", 1),
            ("aaa", 2),
            ("aaa", 3),
            ("aab", 1),
            ("aab", 2),
            ("aab", 3),
            ("abc", 1),
            ("abc", 2),
            ("abc", 3),
        ]);

        let applier_factory = build_applier_factory(tags).await;

        let expr = col("tag_str").between(lit("aaa"), lit("aab"));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![0, 1, 2, 3, 4, 5]);

        let expr = col("tag_i32").between(lit(2), lit(3));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![1, 2, 4, 5, 7, 8]);

        let expr = col("tag_str").between(lit("aaa"), lit("aaa"));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![0, 1, 2]);

        let expr = col("tag_i32").between(lit(2), lit(2));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![1, 4, 7]);
    }

    #[tokio::test]
    async fn test_create_and_query_comparison() {
        let tags = BTreeSet::from_iter([
            ("aaa", 1),
            ("aaa", 2),
            ("aaa", 3),
            ("aab", 1),
            ("aab", 2),
            ("aab", 3),
            ("abc", 1),
            ("abc", 2),
            ("abc", 3),
        ]);

        let applier_factory = build_applier_factory(tags).await;

        let expr = col("tag_str").lt(lit("aab"));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![0, 1, 2]);

        let expr = col("tag_i32").lt(lit(2));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![0, 3, 6]);

        let expr = col("tag_str").gt(lit("aab"));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![6, 7, 8]);

        let expr = col("tag_i32").gt(lit(2));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![2, 5, 8]);

        let expr = col("tag_str").lt_eq(lit("aab"));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![0, 1, 2, 3, 4, 5]);

        let expr = col("tag_i32").lt_eq(lit(2));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![0, 1, 3, 4, 6, 7]);

        let expr = col("tag_str").gt_eq(lit("aab"));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![3, 4, 5, 6, 7, 8]);

        let expr = col("tag_i32").gt_eq(lit(2));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![1, 2, 4, 5, 7, 8]);

        let expr = col("tag_str")
            .gt(lit("aaa"))
            .and(col("tag_str").lt(lit("abc")));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![3, 4, 5]);

        let expr = col("tag_i32").gt(lit(1)).and(col("tag_i32").lt(lit(3)));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![1, 4, 7]);
    }

    #[tokio::test]
    async fn test_create_and_query_regex() {
        let tags = BTreeSet::from_iter([
            ("aaa", 1),
            ("aaa", 2),
            ("aaa", 3),
            ("aab", 1),
            ("aab", 2),
            ("aab", 3),
            ("abc", 1),
            ("abc", 2),
            ("abc", 3),
        ]);

        let applier_factory = build_applier_factory(tags).await;

        let expr = binary_expr(col("tag_str"), Operator::RegexMatch, lit(".*"));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![0, 1, 2, 3, 4, 5, 6, 7, 8]);

        let expr = binary_expr(col("tag_str"), Operator::RegexMatch, lit("a.*c"));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![6, 7, 8]);

        let expr = binary_expr(col("tag_str"), Operator::RegexMatch, lit("a.*b$"));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![3, 4, 5]);

        let expr = binary_expr(col("tag_str"), Operator::RegexMatch, lit("\\w"));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![0, 1, 2, 3, 4, 5, 6, 7, 8]);

        let expr = binary_expr(col("tag_str"), Operator::RegexMatch, lit("\\d"));
        let res = applier_factory(expr).await;
        assert!(res.is_empty());

        let expr = binary_expr(col("tag_str"), Operator::RegexMatch, lit("^aaa$"));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![0, 1, 2]);
    }
}
