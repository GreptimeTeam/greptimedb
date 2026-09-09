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

//! Builds range and aggregate series indexes from SST readers.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use async_stream::try_stream;
use futures::TryStreamExt;
use object_store::ObjectStore;
use snafu::OptionExt;
use store_api::storage::FileId;

use crate::error::{Result, UnexpectedSnafu};
use crate::read::BoxedRecordBatchStream;
use crate::read::flat_merge::FlatMergeReader;
use crate::read::prune::FlatPruneReader;
use crate::read::read_columns::ReadColumns;
use crate::region::MitoRegionRef;
use crate::region::version::VersionRef;
use crate::series_index::bucket::SeriesBucket;
use crate::series_index::catalog::{
    SeriesIndexEntry, range_index_path, series_index_path, series_metadata,
};
use crate::series_index::purger::{IndexFilePurger, IndexFileType, file_operation};
use crate::series_index::version::SeriesIndexFileHandle;
use crate::series_index::{SeriesIndexWriter, SeriesIndexWriterOptions};
use crate::sst::file::FileHandle;
use crate::sst::parquet::reader::{FlatRowGroupReader, ReaderMetrics};
use crate::sst::parquet::row_group::ParquetFetchMetrics;
use crate::sst::range_index::{SstRangeIndexWriter, SstRangeIndexWriterOptions};

async fn reader_input(
    region: &MitoRegionRef,
    file: FileHandle,
) -> Result<
    Option<(
        Arc<crate::sst::parquet::file_range::FileRangeContext>,
        crate::sst::parquet::row_selection::RowGroupSelection,
    )>,
> {
    Ok(region
        .access_layer
        .read_sst(file)
        .projection(Some(ReadColumns::new([])))
        .build_reader_input(&mut ReaderMetrics::default())
        .await?
        .map(|(context, selection)| (Arc::new(context), selection)))
}

pub(crate) async fn build_range_index(
    store: &ObjectStore,
    region: &MitoRegionRef,
    version: &VersionRef,
    file: FileHandle,
) -> Result<Option<FileId>> {
    let file_id = file.file_id().file_id();
    let path = range_index_path(region.region_id, file_id);
    let mut writer = SstRangeIndexWriter::try_new(
        version.metadata.clone(),
        store.clone(),
        &path,
        SstRangeIndexWriterOptions::default(),
    )
    .await?;
    let Some((context, mut selection)) = reader_input(region, file).await? else {
        writer.abort().await?;
        return Ok(None);
    };
    let fetch_metrics = ParquetFetchMetrics::default();
    while let Some((row_group_id, row_selection)) = selection.pop_first() {
        let parquet_reader = context
            .reader_builder()
            .build(context.build_context(row_group_id, Some(row_selection), Some(&fetch_metrics)))
            .await?;
        let mut reader = FlatPruneReader::new_with_row_group_reader(
            context.clone(),
            FlatRowGroupReader::new(context.clone(), parquet_reader),
            context.pre_filter_mode().skip_fields(),
        );
        while let Some(batch) = reader.next_batch().await? {
            writer.write(row_group_id as u32, &batch).await?;
        }
    }
    writer.finish().await?;
    file_operation(IndexFileType::Range, "build", "success");
    Ok(Some(file_id))
}

pub(crate) async fn build_series_index(
    store: &ObjectStore,
    region: &MitoRegionRef,
    version: &VersionRef,
    bucket: &SeriesBucket,
    entry: &SeriesIndexEntry,
    missing_range_ids: &HashSet<FileId>,
    purger: &IndexFilePurger,
) -> Result<(SeriesIndexFileHandle, HashSet<FileId>)> {
    let completed_ranges = Arc::new(Mutex::new(HashSet::new()));
    let mut sources = Vec::<BoxedRecordBatchStream>::new();
    let mut schema = None;
    let mut expected_ranges = 0;
    for file in &bucket.files {
        let file_id = file.file_id().file_id();
        let Some((context, mut selection)) = reader_input(region, file.clone()).await? else {
            continue;
        };
        schema.get_or_insert(context.read_format().output_arrow_schema()?);
        let range_writer = if missing_range_ids.contains(&file_id) {
            expected_ranges += 1;
            let path = range_index_path(region.region_id, file_id);
            Some(
                SstRangeIndexWriter::try_new(
                    version.metadata.clone(),
                    store.clone(),
                    &path,
                    SstRangeIndexWriterOptions::default(),
                )
                .await?,
            )
        } else {
            None
        };
        let completed_ranges = completed_ranges.clone();
        sources.push(Box::pin(try_stream! {
            let fetch_metrics = ParquetFetchMetrics::default();
            let mut range_writer = range_writer;
            while let Some((row_group_id, row_selection)) = selection.pop_first() {
                let parquet_reader = context.reader_builder().build(context.build_context(
                    row_group_id,
                    Some(row_selection),
                    Some(&fetch_metrics),
                )).await?;
                let mut reader = FlatPruneReader::new_with_row_group_reader(
                    context.clone(),
                    FlatRowGroupReader::new(context.clone(), parquet_reader),
                    context.pre_filter_mode().skip_fields(),
                );
                while let Some(batch) = reader.next_batch().await? {
                    if let Some(writer) = &mut range_writer {
                        writer.write(row_group_id as u32, &batch).await?;
                    }
                    yield batch;
                }
            }
            if let Some(writer) = range_writer {
                writer.finish().await?;
                file_operation(IndexFileType::Range, "build", "success");
                completed_ranges.lock().unwrap().insert(file_id);
            }
        }));
    }
    let schema = schema.context(UnexpectedSnafu {
        reason: "series-index bucket has no readable SST",
    })?;
    let merged: BoxedRecordBatchStream = if sources.len() == 1 {
        sources.pop().context(UnexpectedSnafu {
            reason: "series-index source disappeared",
        })?
    } else {
        Box::pin(
            FlatMergeReader::new(schema, sources, 8192, None)
                .await?
                .into_stream(),
        )
    };
    // TODO: Deduplicate update-mode rows before series indexes are used by queries.
    let mut visible = merged;
    let path = series_index_path(region.region_id, entry.index_uuid);
    let mut writer = SeriesIndexWriter::try_new(
        region.metadata(),
        store.clone(),
        &path,
        SeriesIndexWriterOptions::default(),
        Some(series_metadata(entry)?),
    )
    .await?;
    while let Some(batch) = visible.try_next().await? {
        writer.write(&batch).await?;
    }
    let range_indexes = std::mem::take(&mut *completed_ranges.lock().unwrap());
    if range_indexes.len() != expected_ranges {
        return UnexpectedSnafu {
            reason: "not all combined range-index builds completed",
        }
        .fail();
    }
    writer.finish().await?;
    file_operation(IndexFileType::Series, "build", "success");
    Ok((
        SeriesIndexFileHandle::new(region.region_id, entry.clone(), purger.clone()),
        range_indexes,
    ))
}

#[cfg(test)]
mod tests {
    use object_store::layers::mock::{self, MockLayerBuilder, oio};
    use object_store::services::Memory;
    use store_api::region_engine::RegionEngine;

    use super::*;
    use crate::series_index::bucket::{group_files_into_series_buckets, plan_series_indexes};
    use crate::series_index::purger::series_index_channel;
    use crate::series_index::tests::prepare_region;
    use crate::test_util::TestEnv;

    struct FailingSeriesWriter(oio::Writer);

    impl mock::Write for FailingSeriesWriter {
        async fn write(&mut self, buffer: mock::Buffer) -> mock::Result<()> {
            self.0.write(buffer).await
        }

        async fn close(&mut self) -> mock::Result<mock::Metadata> {
            Err(mock::Error::new(
                mock::ErrorKind::Unexpected,
                "injected series write failure",
            ))
        }

        async fn abort(&mut self) -> mock::Result<()> {
            self.0.abort().await
        }
    }

    #[rstest::rstest]
    #[case::success(false)]
    #[case::series_failure(true)]
    #[tokio::test]
    async fn test_build_indexes_without_publication(#[case] fail_series: bool) {
        let mut env = TestEnv::with_prefix("series-builder").await;
        let (engine, region) = prepare_region(&mut env).await;
        let version = region.version();
        let files = version
            .ssts
            .levels()
            .iter()
            .flat_map(|level| level.files())
            .cloned()
            .collect::<Vec<_>>();
        let store = ObjectStore::new(Memory::default()).unwrap();
        let (purger, mut receiver) = series_index_channel(store.clone());
        // Build one range separately, then reuse it during the combined build.
        let existing = build_range_index(&store, &region, &version, files[0].clone())
            .await
            .unwrap()
            .unwrap();
        let existing_path = range_index_path(region.region_id, existing);
        let existing_bytes = store.read(&existing_path).await.unwrap().to_bytes();
        let missing = files
            .iter()
            .map(|file| file.file_id().file_id())
            .filter(|id| *id != existing)
            .collect::<HashSet<_>>();
        let mut plan = plan_series_indexes(
            group_files_into_series_buckets(&files, 100),
            Default::default(),
            None,
            0,
        );
        assert_eq!(plan.builds.len(), 1);
        let (bucket, entry) = plan.builds.pop().unwrap();
        let layer = MockLayerBuilder::default()
            .writer_factory(Arc::new(move |path, _, inner| -> oio::Writer {
                if fail_series && path.contains("/series/") {
                    Box::new(FailingSeriesWriter(inner))
                } else {
                    inner
                }
            }))
            .build()
            .unwrap();
        let result = build_series_index(
            &store.clone().layer(layer),
            &region,
            &version,
            &bucket,
            &entry,
            &missing,
            &purger,
        )
        .await;
        if fail_series {
            assert!(result.is_err());
        } else {
            let (handle, built_ranges) = result.unwrap();
            assert_eq!(built_ranges, missing);
            assert_eq!(handle.entry(), &entry);
            assert!(
                store
                    .exists(&series_index_path(region.region_id, entry.index_uuid))
                    .await
                    .unwrap()
            );
        }
        // A failed series build must retain completed companion range indexes.
        for file in &files {
            assert!(
                store
                    .exists(&range_index_path(
                        region.region_id,
                        file.file_id().file_id()
                    ))
                    .await
                    .unwrap()
            );
        }
        assert_eq!(
            store.read(&existing_path).await.unwrap().to_bytes(),
            existing_bytes
        );
        assert!(region.series_index_version().range_indexes.is_empty());
        assert!(region.series_index_version().series_indexes.is_empty());
        assert!(receiver.try_recv().is_err());
        engine.stop().await.unwrap();
    }
}
