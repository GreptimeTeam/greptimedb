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

use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use common_telemetry::{debug, warn};
use index::inverted_index::create::sort::external_sort::ExternalSorter;
use index::inverted_index::create::sort_create::SortIndexCreator;
use index::inverted_index::create::InvertedIndexCreator;
use index::inverted_index::format::writer::InvertedIndexBlobWriter;
use puffin::puffin_manager::{PuffinWriter, PutOptions};
use snafu::{ensure, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;
use tokio::io::duplex;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use crate::error::{
    BiErrorsSnafu, IndexFinishSnafu, OperateAbortedIndexSnafu, PuffinAddBlobSnafu,
    PushIndexValueSnafu, Result,
};
use crate::read::Batch;
use crate::row_converter::SortField;
use crate::sst::file::FileId;
use crate::sst::index::codec::{IndexValueCodec, IndexValuesCodec};
use crate::sst::index::intermediate::{
    IntermediateLocation, IntermediateManager, TempFileProvider,
};
use crate::sst::index::inverted_index::INDEX_BLOB_TYPE;
use crate::sst::index::puffin_manager::SstPuffinWriter;
use crate::sst::index::statistics::{ByteCount, RowCount, Statistics};
use crate::sst::index::TYPE_INVERTED_INDEX;

/// The minimum memory usage threshold for one column.
const MIN_MEMORY_USAGE_THRESHOLD_PER_COLUMN: usize = 1024 * 1024; // 1MB

/// The buffer size for the pipe used to send index data to the puffin blob.
const PIPE_BUFFER_SIZE_FOR_SENDING_BLOB: usize = 8192;

/// `InvertedIndexer` creates inverted index for SST files.
pub struct InvertedIndexer {
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

    /// The memory usage of the index creator.
    memory_usage: Arc<AtomicUsize>,

    /// Ids of indexed columns and their names (`to_string` of the column id).
    indexed_column_ids: Vec<(ColumnId, String)>,
}

impl InvertedIndexer {
    /// Creates a new `InvertedIndexer`.
    /// Should ensure that the number of tag columns is greater than 0.
    pub fn new(
        sst_file_id: FileId,
        metadata: &RegionMetadataRef,
        intermediate_manager: IntermediateManager,
        memory_usage_threshold: Option<usize>,
        segment_row_count: NonZeroUsize,
        indexed_column_ids: HashSet<ColumnId>,
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

        let codec = IndexValuesCodec::from_tag_columns(
            metadata.primary_key_encoding,
            metadata.primary_key_columns(),
        );
        let indexed_column_ids = indexed_column_ids
            .into_iter()
            .map(|col_id| {
                let col_id_str = col_id.to_string();
                (col_id, col_id_str)
            })
            .collect();
        Self {
            codec,
            index_creator,
            temp_file_provider,
            value_buf: vec![],
            stats: Statistics::new(TYPE_INVERTED_INDEX),
            aborted: false,
            memory_usage,
            indexed_column_ids,
        }
    }

    /// Updates index with a batch of rows.
    /// Garbage will be cleaned up if failed to update.
    pub async fn update(&mut self, batch: &mut Batch) -> Result<()> {
        ensure!(!self.aborted, OperateAbortedIndexSnafu);

        if batch.is_empty() {
            return Ok(());
        }

        if let Err(update_err) = self.do_update(batch).await {
            // clean up garbage if failed to update
            if let Err(err) = self.do_cleanup().await {
                if cfg!(any(test, feature = "test")) {
                    panic!("Failed to clean up index creator, err: {err}",);
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
                panic!("Failed to clean up index creator, err: {err}",);
            } else {
                warn!(err; "Failed to clean up index creator");
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

    async fn do_update(&mut self, batch: &mut Batch) -> Result<()> {
        let mut guard = self.stats.record_update();

        let n = batch.num_rows();
        guard.inc_row_count(n);

        for (col_id, col_id_str) in &self.indexed_column_ids {
            match self.codec.pk_col_info(*col_id) {
                // pk
                Some(col_info) => {
                    let pk_idx = col_info.idx;
                    let field = &col_info.field;
                    let value = batch
                        .pk_col_value(self.codec.decoder(), pk_idx, *col_id)?
                        .filter(|v| !v.is_null())
                        .map(|v| {
                            self.value_buf.clear();
                            IndexValueCodec::encode_nonnull_value(
                                v.as_value_ref(),
                                field,
                                &mut self.value_buf,
                            )?;
                            Ok(self.value_buf.as_slice())
                        })
                        .transpose()?;

                    self.index_creator
                        .push_with_name_n(col_id_str, value, n)
                        .await
                        .context(PushIndexValueSnafu)?;
                }
                // fields
                None => {
                    let Some(values) = batch.field_col_value(*col_id) else {
                        debug!(
                            "Column {} not found in the batch during building inverted index",
                            col_id
                        );
                        continue;
                    };
                    let sort_field = SortField::new(values.data.data_type());
                    for i in 0..n {
                        self.value_buf.clear();
                        let value = values.data.get_ref(i);
                        if value.is_null() {
                            self.index_creator
                                .push_with_name(col_id_str, None)
                                .await
                                .context(PushIndexValueSnafu)?;
                        } else {
                            IndexValueCodec::encode_nonnull_value(
                                value,
                                &sort_field,
                                &mut self.value_buf,
                            )?;
                            self.index_creator
                                .push_with_name(col_id_str, Some(&self.value_buf))
                                .await
                                .context(PushIndexValueSnafu)?;
                        }
                    }
                }
            }
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
    async fn do_finish(&mut self, puffin_writer: &mut SstPuffinWriter) -> Result<()> {
        let mut guard = self.stats.record_finish();

        let (tx, rx) = duplex(PIPE_BUFFER_SIZE_FOR_SENDING_BLOB);
        let mut index_writer = InvertedIndexBlobWriter::new(tx.compat_write());

        let (index_finish, puffin_add_blob) = futures::join!(
            // TODO(zhongzc): config bitmap type
            self.index_creator
                .finish(&mut index_writer, index::bitmap::BitmapType::Roaring),
            puffin_writer.put_blob(
                INDEX_BLOB_TYPE,
                rx.compat(),
                PutOptions::default(),
                Default::default(),
            )
        );

        match (
            puffin_add_blob.context(PuffinAddBlobSnafu),
            index_finish.context(IndexFinishSnafu),
        ) {
            (Err(e1), Err(e2)) => BiErrorsSnafu {
                first: Box::new(e1),
                second: Box::new(e2),
            }
            .fail()?,

            (Ok(_), e @ Err(_)) => e?,
            (e @ Err(_), Ok(_)) => e.map(|_| ())?,
            (Ok(written_bytes), Ok(_)) => {
                guard.inc_byte_count(written_bytes);
            }
        }

        Ok(())
    }

    async fn do_cleanup(&mut self) -> Result<()> {
        let _guard = self.stats.record_cleanup();

        self.temp_file_provider.cleanup().await
    }

    pub fn column_ids(&self) -> impl Iterator<Item = ColumnId> + '_ {
        self.indexed_column_ids.iter().map(|(col_id, _)| *col_id)
    }

    pub fn memory_usage(&self) -> usize {
        self.memory_usage.load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use api::v1::SemanticType;
    use datafusion_expr::{binary_expr, col, lit, Expr as DfExpr, Operator};
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use datatypes::value::ValueRef;
    use datatypes::vectors::{UInt64Vector, UInt8Vector};
    use futures::future::BoxFuture;
    use object_store::services::Memory;
    use object_store::ObjectStore;
    use puffin::puffin_manager::cache::PuffinMetadataCache;
    use puffin::puffin_manager::PuffinManager;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::RegionId;

    use super::*;
    use crate::access_layer::RegionFilePathFactory;
    use crate::cache::index::inverted_index::InvertedIndexCache;
    use crate::metrics::CACHE_BYTES;
    use crate::read::BatchColumn;
    use crate::row_converter::{DensePrimaryKeyCodec, PrimaryKeyCodecExt};
    use crate::sst::index::inverted_index::applier::builder::InvertedIndexApplierBuilder;
    use crate::sst::index::puffin_manager::PuffinManagerFactory;

    fn mock_object_store() -> ObjectStore {
        ObjectStore::new(Memory::default()).unwrap().finish()
    }

    async fn new_intm_mgr(path: impl AsRef<str>) -> IntermediateManager {
        IntermediateManager::init_fs(path).await.unwrap()
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
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "field_u64",
                    ConcreteDataType::uint64_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Field,
                column_id: 4,
            })
            .primary_key(vec![1, 2]);

        Arc::new(builder.build().unwrap())
    }

    fn new_batch(
        str_tag: impl AsRef<str>,
        i32_tag: impl Into<i32>,
        u64_field: impl IntoIterator<Item = u64>,
    ) -> Batch {
        let fields = vec![
            (0, SortField::new(ConcreteDataType::string_datatype())),
            (1, SortField::new(ConcreteDataType::int32_datatype())),
        ];
        let codec = DensePrimaryKeyCodec::with_fields(fields);
        let row: [ValueRef; 2] = [str_tag.as_ref().into(), i32_tag.into().into()];
        let primary_key = codec.encode(row.into_iter()).unwrap();

        let u64_field = BatchColumn {
            column_id: 4,
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

    async fn build_applier_factory(
        prefix: &str,
        rows: BTreeSet<(&'static str, i32, [u64; 2])>,
    ) -> impl Fn(DfExpr) -> BoxFuture<'static, Vec<usize>> {
        let (d, factory) = PuffinManagerFactory::new_for_test_async(prefix).await;
        let region_dir = "region0".to_string();
        let sst_file_id = FileId::random();
        let object_store = mock_object_store();
        let region_metadata = mock_region_metadata();
        let intm_mgr = new_intm_mgr(d.path().to_string_lossy()).await;
        let memory_threshold = None;
        let segment_row_count = 2;
        let indexed_column_ids = HashSet::from_iter([1, 2, 4]);

        let mut creator = InvertedIndexer::new(
            sst_file_id,
            &region_metadata,
            intm_mgr,
            memory_threshold,
            NonZeroUsize::new(segment_row_count).unwrap(),
            indexed_column_ids.clone(),
        );

        for (str_tag, i32_tag, u64_field) in &rows {
            let mut batch = new_batch(str_tag, *i32_tag, u64_field.iter().copied());
            creator.update(&mut batch).await.unwrap();
        }

        let puffin_manager = factory.build(
            object_store.clone(),
            RegionFilePathFactory::new(region_dir.clone()),
        );
        let mut writer = puffin_manager.writer(&sst_file_id).await.unwrap();
        let (row_count, _) = creator.finish(&mut writer).await.unwrap();
        assert_eq!(row_count, rows.len() * segment_row_count);
        writer.finish().await.unwrap();

        move |expr| {
            let _d = &d;
            let cache = Arc::new(InvertedIndexCache::new(10, 10, 100));
            let puffin_metadata_cache = Arc::new(PuffinMetadataCache::new(10, &CACHE_BYTES));
            let applier = InvertedIndexApplierBuilder::new(
                region_dir.clone(),
                object_store.clone(),
                &region_metadata,
                indexed_column_ids.clone(),
                factory.clone(),
            )
            .with_inverted_index_cache(Some(cache))
            .with_puffin_metadata_cache(Some(puffin_metadata_cache))
            .build(&[expr])
            .unwrap()
            .unwrap();
            Box::pin(async move {
                applier
                    .apply(sst_file_id, None)
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
        let rows = BTreeSet::from_iter([
            ("aaa", 1, [1, 2]),
            ("aaa", 2, [2, 3]),
            ("aaa", 3, [3, 4]),
            ("aab", 1, [4, 5]),
            ("aab", 2, [5, 6]),
            ("aab", 3, [6, 7]),
            ("abc", 1, [7, 8]),
            ("abc", 2, [8, 9]),
            ("abc", 3, [9, 10]),
        ]);

        let applier_factory = build_applier_factory("test_create_and_query_get_key_", rows).await;

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

        let expr = col("field_u64").eq(lit(2u64));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![0, 1]);
    }

    #[tokio::test]
    async fn test_create_and_query_range() {
        let rows = BTreeSet::from_iter([
            ("aaa", 1, [1, 2]),
            ("aaa", 2, [2, 3]),
            ("aaa", 3, [3, 4]),
            ("aab", 1, [4, 5]),
            ("aab", 2, [5, 6]),
            ("aab", 3, [6, 7]),
            ("abc", 1, [7, 8]),
            ("abc", 2, [8, 9]),
            ("abc", 3, [9, 10]),
        ]);

        let applier_factory = build_applier_factory("test_create_and_query_range_", rows).await;

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

        let expr = col("field_u64").between(lit(2u64), lit(5u64));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![0, 1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn test_create_and_query_comparison() {
        let rows = BTreeSet::from_iter([
            ("aaa", 1, [1, 2]),
            ("aaa", 2, [2, 3]),
            ("aaa", 3, [3, 4]),
            ("aab", 1, [4, 5]),
            ("aab", 2, [5, 6]),
            ("aab", 3, [6, 7]),
            ("abc", 1, [7, 8]),
            ("abc", 2, [8, 9]),
            ("abc", 3, [9, 10]),
        ]);

        let applier_factory =
            build_applier_factory("test_create_and_query_comparison_", rows).await;

        let expr = col("tag_str").lt(lit("aab"));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![0, 1, 2]);

        let expr = col("tag_i32").lt(lit(2));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![0, 3, 6]);

        let expr = col("field_u64").lt(lit(2u64));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![0]);

        let expr = col("tag_str").gt(lit("aab"));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![6, 7, 8]);

        let expr = col("tag_i32").gt(lit(2));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![2, 5, 8]);

        let expr = col("field_u64").gt(lit(8u64));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![7, 8]);

        let expr = col("tag_str").lt_eq(lit("aab"));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![0, 1, 2, 3, 4, 5]);

        let expr = col("tag_i32").lt_eq(lit(2));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![0, 1, 3, 4, 6, 7]);

        let expr = col("field_u64").lt_eq(lit(2u64));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![0, 1]);

        let expr = col("tag_str").gt_eq(lit("aab"));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![3, 4, 5, 6, 7, 8]);

        let expr = col("tag_i32").gt_eq(lit(2));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![1, 2, 4, 5, 7, 8]);

        let expr = col("field_u64").gt_eq(lit(8u64));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![6, 7, 8]);

        let expr = col("tag_str")
            .gt(lit("aaa"))
            .and(col("tag_str").lt(lit("abc")));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![3, 4, 5]);

        let expr = col("tag_i32").gt(lit(1)).and(col("tag_i32").lt(lit(3)));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![1, 4, 7]);

        let expr = col("field_u64")
            .gt(lit(2u64))
            .and(col("field_u64").lt(lit(9u64)));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![1, 2, 3, 4, 5, 6, 7]);
    }

    #[tokio::test]
    async fn test_create_and_query_regex() {
        let rows = BTreeSet::from_iter([
            ("aaa", 1, [1, 2]),
            ("aaa", 2, [2, 3]),
            ("aaa", 3, [3, 4]),
            ("aab", 1, [4, 5]),
            ("aab", 2, [5, 6]),
            ("aab", 3, [6, 7]),
            ("abc", 1, [7, 8]),
            ("abc", 2, [8, 9]),
            ("abc", 3, [9, 10]),
        ]);

        let applier_factory = build_applier_factory("test_create_and_query_regex_", rows).await;

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
