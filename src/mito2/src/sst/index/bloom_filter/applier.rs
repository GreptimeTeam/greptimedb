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

mod builder;

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use common_base::range_read::RangeReader;
use common_telemetry::warn;
use index::bloom_filter::applier::BloomFilterApplier;
use index::bloom_filter::reader::{BloomFilterReader, BloomFilterReaderImpl};
use object_store::ObjectStore;
use puffin::puffin_manager::cache::PuffinMetadataCacheRef;
use puffin::puffin_manager::{BlobGuard, PuffinManager, PuffinReader};
use snafu::ResultExt;
use store_api::storage::{ColumnId, RegionId};

use crate::cache::file_cache::{FileCacheRef, FileType, IndexKey};
use crate::cache::index::bloom_filter_index::{
    BloomFilterIndexCacheRef, CachedBloomFilterIndexBlobReader,
};
use crate::error::{
    ApplyBloomFilterIndexSnafu, Error, MetadataSnafu, PuffinBuildReaderSnafu, PuffinReadBlobSnafu,
    Result,
};
use crate::metrics::INDEX_APPLY_ELAPSED;
use crate::sst::file::FileId;
pub use crate::sst::index::bloom_filter::applier::builder::BloomFilterIndexApplierBuilder;
use crate::sst::index::bloom_filter::applier::builder::Predicate;
use crate::sst::index::bloom_filter::INDEX_BLOB_TYPE;
use crate::sst::index::puffin_manager::{BlobReader, PuffinManagerFactory};
use crate::sst::index::TYPE_BLOOM_FILTER_INDEX;
use crate::sst::location;

pub(crate) type BloomFilterIndexApplierRef = Arc<BloomFilterIndexApplier>;

/// `BloomFilterIndexApplier` applies bloom filter predicates to the SST file.
pub struct BloomFilterIndexApplier {
    /// Directory of the region.
    region_dir: String,

    /// ID of the region.
    region_id: RegionId,

    /// Object store to read the index file.
    object_store: ObjectStore,

    /// File cache to read the index file.
    file_cache: Option<FileCacheRef>,

    /// Factory to create puffin manager.
    puffin_manager_factory: PuffinManagerFactory,

    /// Cache for puffin metadata.
    puffin_metadata_cache: Option<PuffinMetadataCacheRef>,

    /// Cache for bloom filter index.
    bloom_filter_index_cache: Option<BloomFilterIndexCacheRef>,

    /// Bloom filter predicates.
    filters: HashMap<ColumnId, Vec<Predicate>>,
}

impl BloomFilterIndexApplier {
    /// Creates a new `BloomFilterIndexApplier`.
    pub fn new(
        region_dir: String,
        region_id: RegionId,
        object_store: ObjectStore,
        puffin_manager_factory: PuffinManagerFactory,
        filters: HashMap<ColumnId, Vec<Predicate>>,
    ) -> Self {
        Self {
            region_dir,
            region_id,
            object_store,
            file_cache: None,
            puffin_manager_factory,
            puffin_metadata_cache: None,
            bloom_filter_index_cache: None,
            filters,
        }
    }

    pub fn with_file_cache(mut self, file_cache: Option<FileCacheRef>) -> Self {
        self.file_cache = file_cache;
        self
    }

    pub fn with_puffin_metadata_cache(
        mut self,
        puffin_metadata_cache: Option<PuffinMetadataCacheRef>,
    ) -> Self {
        self.puffin_metadata_cache = puffin_metadata_cache;
        self
    }

    pub fn with_bloom_filter_cache(
        mut self,
        bloom_filter_index_cache: Option<BloomFilterIndexCacheRef>,
    ) -> Self {
        self.bloom_filter_index_cache = bloom_filter_index_cache;
        self
    }

    /// Applies bloom filter predicates to the provided SST file and returns a
    /// list of row group ranges that match the predicates.
    ///
    /// The `row_groups` iterator provides the row group lengths and whether to search in the row group.
    pub async fn apply(
        &self,
        file_id: FileId,
        file_size_hint: Option<u64>,
        row_groups: impl Iterator<Item = (usize, bool)>,
    ) -> Result<Vec<(usize, Vec<Range<usize>>)>> {
        let _timer = INDEX_APPLY_ELAPSED
            .with_label_values(&[TYPE_BLOOM_FILTER_INDEX])
            .start_timer();

        // Calculates row groups' ranges based on start of the file.
        let mut input = Vec::with_capacity(row_groups.size_hint().0);
        let mut start = 0;
        for (i, (len, to_search)) in row_groups.enumerate() {
            let end = start + len;
            if to_search {
                input.push((i, start..end));
            }
            start = end;
        }

        // Initializes output with input ranges, but ranges are based on start of the file not the row group,
        // so we need to adjust them later.
        let mut output = input
            .iter()
            .map(|(i, range)| (*i, vec![range.clone()]))
            .collect::<Vec<_>>();

        for (column_id, predicates) in &self.filters {
            let blob = match self
                .blob_reader(file_id, *column_id, file_size_hint)
                .await?
            {
                Some(blob) => blob,
                None => continue,
            };

            // Create appropriate reader based on whether we have caching enabled
            if let Some(bloom_filter_cache) = &self.bloom_filter_index_cache {
                let blob_size = blob.metadata().await.context(MetadataSnafu)?.content_length;
                let reader = CachedBloomFilterIndexBlobReader::new(
                    file_id,
                    *column_id,
                    blob_size,
                    BloomFilterReaderImpl::new(blob),
                    bloom_filter_cache.clone(),
                );
                self.apply_filters(reader, predicates, &input, &mut output)
                    .await
                    .context(ApplyBloomFilterIndexSnafu)?;
            } else {
                let reader = BloomFilterReaderImpl::new(blob);
                self.apply_filters(reader, predicates, &input, &mut output)
                    .await
                    .context(ApplyBloomFilterIndexSnafu)?;
            }
        }

        // adjust ranges to be based on row group
        for ((_, output), (_, input)) in output.iter_mut().zip(input) {
            let start = input.start;
            for range in output.iter_mut() {
                range.start -= start;
                range.end -= start;
            }
        }
        output.retain(|(_, ranges)| !ranges.is_empty());

        Ok(output)
    }

    /// Creates a blob reader from the cached or remote index file.
    ///
    /// Returus `None` if the column does not have an index.
    async fn blob_reader(
        &self,
        file_id: FileId,
        column_id: ColumnId,
        file_size_hint: Option<u64>,
    ) -> Result<Option<BlobReader>> {
        let reader = match self
            .cached_blob_reader(file_id, column_id, file_size_hint)
            .await
        {
            Ok(Some(puffin_reader)) => puffin_reader,
            other => {
                if let Err(err) = other {
                    // Blob not found means no index for this column
                    if is_blob_not_found(&err) {
                        return Ok(None);
                    }
                    warn!(err; "An unexpected error occurred while reading the cached index file. Fallback to remote index file.")
                }
                let res = self
                    .remote_blob_reader(file_id, column_id, file_size_hint)
                    .await;
                if let Err(err) = res {
                    // Blob not found means no index for this column
                    if is_blob_not_found(&err) {
                        return Ok(None);
                    }
                    return Err(err);
                }

                res?
            }
        };

        Ok(Some(reader))
    }

    /// Creates a blob reader from the cached index file
    async fn cached_blob_reader(
        &self,
        file_id: FileId,
        column_id: ColumnId,
        file_size_hint: Option<u64>,
    ) -> Result<Option<BlobReader>> {
        let Some(file_cache) = &self.file_cache else {
            return Ok(None);
        };

        let index_key = IndexKey::new(self.region_id, file_id, FileType::Puffin);
        if file_cache.get(index_key).await.is_none() {
            return Ok(None);
        };

        let puffin_manager = self.puffin_manager_factory.build(file_cache.local_store());
        let puffin_file_name = file_cache.cache_file_path(index_key);

        let reader = puffin_manager
            .reader(&puffin_file_name)
            .await
            .context(PuffinBuildReaderSnafu)?
            .with_file_size_hint(file_size_hint)
            .blob(&Self::column_blob_name(column_id))
            .await
            .context(PuffinReadBlobSnafu)?
            .reader()
            .await
            .context(PuffinBuildReaderSnafu)?;
        Ok(Some(reader))
    }

    // TODO(ruihang): use the same util with the code in creator
    fn column_blob_name(column_id: ColumnId) -> String {
        format!("{INDEX_BLOB_TYPE}-{column_id}")
    }

    /// Creates a blob reader from the remote index file
    async fn remote_blob_reader(
        &self,
        file_id: FileId,
        column_id: ColumnId,
        file_size_hint: Option<u64>,
    ) -> Result<BlobReader> {
        let puffin_manager = self
            .puffin_manager_factory
            .build(self.object_store.clone())
            .with_puffin_metadata_cache(self.puffin_metadata_cache.clone());

        let file_path = location::index_file_path(&self.region_dir, file_id);
        puffin_manager
            .reader(&file_path)
            .await
            .context(PuffinBuildReaderSnafu)?
            .with_file_size_hint(file_size_hint)
            .blob(&Self::column_blob_name(column_id))
            .await
            .context(PuffinReadBlobSnafu)?
            .reader()
            .await
            .context(PuffinBuildReaderSnafu)
    }

    async fn apply_filters<R: BloomFilterReader + Send + 'static>(
        &self,
        reader: R,
        predicates: &[Predicate],
        input: &[(usize, Range<usize>)],
        output: &mut [(usize, Vec<Range<usize>>)],
    ) -> std::result::Result<(), index::bloom_filter::error::Error> {
        let mut applier = BloomFilterApplier::new(Box::new(reader)).await?;

        for ((_, r), (_, output)) in input.iter().zip(output.iter_mut()) {
            // All rows are filtered out, skip the search
            if output.is_empty() {
                continue;
            }

            for predicate in predicates {
                match predicate {
                    Predicate::InList(in_list) => {
                        let res = applier.search(&in_list.list, r.clone()).await?;
                        if res.is_empty() {
                            output.clear();
                            break;
                        }

                        *output = intersect_ranges(output, &res);
                        if output.is_empty() {
                            break;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

/// Intersects two lists of ranges and returns the intersection.
///
/// The input lists are assumed to be sorted and non-overlapping.
fn intersect_ranges(lhs: &[Range<usize>], rhs: &[Range<usize>]) -> Vec<Range<usize>> {
    let mut i = 0;
    let mut j = 0;

    let mut output = Vec::new();
    while i < lhs.len() && j < rhs.len() {
        let r1 = &lhs[i];
        let r2 = &rhs[j];

        // Find intersection if exists
        let start = r1.start.max(r2.start);
        let end = r1.end.min(r2.end);

        if start < end {
            output.push(start..end);
        }

        // Move forward the range that ends first
        if r1.end < r2.end {
            i += 1;
        } else {
            j += 1;
        }
    }

    output
}

fn is_blob_not_found(err: &Error) -> bool {
    matches!(
        err,
        Error::PuffinBuildReader {
            source: puffin::error::Error::BlobNotFound { .. },
            ..
        }
    )
}

#[cfg(test)]
mod tests {

    use datafusion_expr::{col, lit, Expr};
    use futures::future::BoxFuture;
    use puffin::puffin_manager::PuffinWriter;
    use store_api::metadata::RegionMetadata;

    use super::*;
    use crate::sst::index::bloom_filter::creator::tests::{
        mock_object_store, mock_region_metadata, new_batch, new_intm_mgr,
    };
    use crate::sst::index::bloom_filter::creator::BloomFilterIndexer;

    #[allow(clippy::type_complexity)]
    fn tester(
        region_dir: String,
        object_store: ObjectStore,
        metadata: &RegionMetadata,
        puffin_manager_factory: PuffinManagerFactory,
        file_id: FileId,
    ) -> impl Fn(&[Expr], Vec<(usize, bool)>) -> BoxFuture<'static, Vec<(usize, Vec<Range<usize>>)>>
           + use<'_> {
        move |exprs, row_groups| {
            let region_dir = region_dir.clone();
            let object_store = object_store.clone();
            let metadata = metadata.clone();
            let puffin_manager_factory = puffin_manager_factory.clone();
            let exprs = exprs.to_vec();

            Box::pin(async move {
                let builder = BloomFilterIndexApplierBuilder::new(
                    region_dir,
                    object_store,
                    &metadata,
                    puffin_manager_factory,
                );

                let applier = builder.build(&exprs).unwrap().unwrap();
                applier
                    .apply(file_id, None, row_groups.into_iter())
                    .await
                    .unwrap()
            })
        }
    }

    #[tokio::test]
    #[allow(clippy::single_range_in_vec_init)]
    async fn test_bloom_filter_applier() {
        // tag_str:
        //   - type: string
        //   - index: bloom filter
        //   - granularity: 2
        //   - column_id: 1
        //
        // ts:
        //   - type: timestamp
        //   - index: time index
        //   - column_id: 2
        //
        // field_u64:
        //   - type: uint64
        //   - index: bloom filter
        //   - granularity: 4
        //   - column_id: 3
        let region_metadata = mock_region_metadata();
        let prefix = "test_bloom_filter_applier_";
        let object_store = mock_object_store();
        let intm_mgr = new_intm_mgr(prefix).await;
        let memory_usage_threshold = Some(1024);
        let file_id = FileId::random();
        let region_dir = "region_dir".to_string();
        let path = location::index_file_path(&region_dir, file_id);

        let mut indexer =
            BloomFilterIndexer::new(file_id, &region_metadata, intm_mgr, memory_usage_threshold)
                .unwrap()
                .unwrap();

        // push 20 rows
        let batch = new_batch("tag1", 0..10);
        indexer.update(&batch).await.unwrap();
        let batch = new_batch("tag2", 10..20);
        indexer.update(&batch).await.unwrap();

        let (_d, factory) = PuffinManagerFactory::new_for_test_async(prefix).await;
        let puffin_manager = factory.build(object_store.clone());

        let mut puffin_writer = puffin_manager.writer(&path).await.unwrap();
        indexer.finish(&mut puffin_writer).await.unwrap();
        puffin_writer.finish().await.unwrap();

        let tester = tester(
            region_dir.clone(),
            object_store.clone(),
            &region_metadata,
            factory.clone(),
            file_id,
        );

        // rows        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
        // row group: | o  row group |  o row group | o  row group     |  o row group     |
        // tag_str:   |      o pred                 |   x pred                            |
        let res = tester(
            &[col("tag_str").eq(lit("tag1"))],
            vec![(5, true), (5, true), (5, true), (5, true)],
        )
        .await;
        assert_eq!(res, vec![(0, vec![0..5]), (1, vec![0..5])]);

        // rows        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
        // row group: | o  row group |  x row group | o  row group     |  o row group     |
        // tag_str:   |      o pred                 |   x pred                            |
        let res = tester(
            &[col("tag_str").eq(lit("tag1"))],
            vec![(5, true), (5, false), (5, true), (5, true)],
        )
        .await;
        assert_eq!(res, vec![(0, vec![0..5])]);

        // rows        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
        // row group: | o  row group |  o row group | o  row group     |  o row group     |
        // tag_str:   |      o pred                 |   x pred                            |
        // field_u64: | o pred   | x pred    |  x pred     |  x pred       | x pred       |
        let res = tester(
            &[
                col("tag_str").eq(lit("tag1")),
                col("field_u64").eq(lit(1u64)),
            ],
            vec![(5, true), (5, true), (5, true), (5, true)],
        )
        .await;
        assert_eq!(res, vec![(0, vec![0..4])]);

        // rows        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
        // row group: | o  row group |  o row group | x  row group     |  o row group     |
        // field_u64: | o pred   | x pred    |  o pred     |  x pred       | x pred       |
        let res = tester(
            &[col("field_u64").in_list(vec![lit(1u64), lit(11u64)], false)],
            vec![(5, true), (5, true), (5, false), (5, true)],
        )
        .await;
        assert_eq!(res, vec![(0, vec![0..4]), (1, vec![3..5])]);
    }

    #[test]
    #[allow(clippy::single_range_in_vec_init)]
    fn test_intersect_ranges() {
        // empty inputs
        assert_eq!(intersect_ranges(&[], &[]), Vec::<Range<usize>>::new());
        assert_eq!(intersect_ranges(&[1..5], &[]), Vec::<Range<usize>>::new());
        assert_eq!(intersect_ranges(&[], &[1..5]), Vec::<Range<usize>>::new());

        // no overlap
        assert_eq!(
            intersect_ranges(&[1..3, 5..7], &[3..5, 7..9]),
            Vec::<Range<usize>>::new()
        );

        // single overlap
        assert_eq!(intersect_ranges(&[1..5], &[3..7]), vec![3..5]);

        // multiple overlaps
        assert_eq!(
            intersect_ranges(&[1..5, 7..10, 12..15], &[2..6, 8..13]),
            vec![2..5, 8..10, 12..13]
        );

        // exact overlap
        assert_eq!(
            intersect_ranges(&[1..3, 5..7], &[1..3, 5..7]),
            vec![1..3, 5..7]
        );

        // contained ranges
        assert_eq!(
            intersect_ranges(&[1..10], &[2..4, 5..7, 8..9]),
            vec![2..4, 5..7, 8..9]
        );

        // partial overlaps
        assert_eq!(
            intersect_ranges(&[1..4, 6..9], &[2..7, 8..10]),
            vec![2..4, 6..7, 8..9]
        );

        // single point overlap
        assert_eq!(
            intersect_ranges(&[1..3], &[3..5]),
            Vec::<Range<usize>>::new()
        );

        // large ranges
        assert_eq!(intersect_ranges(&[0..100], &[50..150]), vec![50..100]);
    }
}
