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

//! Independently builds range and aggregate series indexes from SST readers.

use std::sync::Arc;

use async_stream::try_stream;
use common_telemetry::warn;
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

/// Builds one range index, or returns `None` when the SST has no readable input.
pub(crate) async fn build_range_index(
    store: &ObjectStore,
    region: &MitoRegionRef,
    version: &VersionRef,
    file: FileHandle,
) -> Result<Option<FileId>> {
    let file_id = file.file_id().file_id();
    let Some((context, mut selection)) = reader_input(region, file).await? else {
        return Ok(None);
    };
    let path = range_index_path(region.region_id, file_id);
    let mut writer = SstRangeIndexWriter::try_new(
        version.metadata.clone(),
        store.clone(),
        &path,
        SstRangeIndexWriterOptions::default(),
    )
    .await?;
    let result: Result<()> = async {
        let fetch_metrics = ParquetFetchMetrics::default();
        while let Some((row_group_id, row_selection)) = selection.pop_first() {
            let parquet_reader = context
                .reader_builder()
                .build(context.build_context(
                    row_group_id,
                    Some(row_selection),
                    Some(&fetch_metrics),
                ))
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
        Ok(())
    }
    .await;
    if let Err(error) = result {
        if let Err(cleanup_error) = writer.abort().await {
            warn!(cleanup_error; "Failed to abort range-index build");
        }
        return Err(error);
    }
    writer.finish().await?;
    file_operation(IndexFileType::Range, "build", "success");
    Ok(Some(file_id))
}

/// Builds only the series index. Callers build needed range indexes separately.
pub(crate) async fn build_series_index(
    store: &ObjectStore,
    region: &MitoRegionRef,
    version: &VersionRef,
    bucket: &SeriesBucket,
    entry: &SeriesIndexEntry,
    purger: &IndexFilePurger,
) -> Result<SeriesIndexFileHandle> {
    let mut sources = Vec::<BoxedRecordBatchStream>::new();
    let mut schema = None;
    for file in &bucket.files {
        let Some((context, mut selection)) = reader_input(region, file.clone()).await? else {
            continue;
        };
        schema.get_or_insert(context.read_format().output_arrow_schema()?);
        sources.push(Box::pin(try_stream! {
            let fetch_metrics = ParquetFetchMetrics::default();
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
                    yield batch;
                }
            }
        }));
    }
    let schema = schema.context(UnexpectedSnafu {
        reason: "series-index bucket has no readable SST",
    })?;
    let mut visible: BoxedRecordBatchStream = if sources.len() == 1 {
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
    let path = series_index_path(region.region_id, entry.index_uuid);
    let mut writer = SeriesIndexWriter::try_new(
        version.metadata.clone(),
        store.clone(),
        &path,
        SeriesIndexWriterOptions::default(),
        Some(series_metadata(entry)?),
    )
    .await?;
    let result: Result<()> = async {
        while let Some(batch) = visible.try_next().await? {
            writer.write(&batch).await?;
        }
        Ok(())
    }
    .await;
    if let Err(error) = result {
        if let Err(cleanup_error) = writer.abort().await {
            warn!(cleanup_error; "Failed to abort series-index build");
        }
        return Err(error);
    }
    writer.finish().await?;
    file_operation(IndexFileType::Series, "build", "success");
    Ok(SeriesIndexFileHandle::new(
        region.region_id,
        entry.clone(),
        purger.clone(),
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    use datatypes::data_type::ConcreteDataType;
    use object_store::layers::mock::{self, MockLayerBuilder, oio};
    use object_store::services::Memory;
    use store_api::region_engine::RegionEngine;

    use super::*;
    use crate::series_index::bucket::{group_files_into_series_buckets, plan_series_indexes};
    use crate::series_index::catalog::{
        SeriesIndexCatalog, load_version_control, series_catalog_path, store_catalog,
    };
    use crate::series_index::purger::series_index_channel;
    use crate::series_index::tests::prepare_region;
    use crate::test_util::TestEnv;

    fn build_input(version: &VersionRef) -> (SeriesBucket, SeriesIndexEntry) {
        let files = version
            .ssts
            .levels()
            .iter()
            .flat_map(|level| level.files())
            .cloned()
            .collect::<Vec<_>>();
        let mut plan = plan_series_indexes(
            group_files_into_series_buckets(&files, 100, 10),
            BTreeMap::new(),
            None,
            0,
        );
        assert_eq!(1, plan.builds.len());
        plan.builds.pop().unwrap()
    }

    fn metadata_with_seconds(version: &VersionRef) -> store_api::metadata::RegionMetadataRef {
        let mut metadata = (*version.metadata).clone();
        let time_index = metadata.time_index_column_pos();
        metadata.column_metadatas[time_index]
            .column_schema
            .data_type = ConcreteDataType::timestamp_second_datatype();
        Arc::new(metadata)
    }

    #[rstest::rstest]
    #[case::range_then_series(true, false)]
    #[case::series_only(false, false)]
    #[case::series_failure(true, true)]
    #[tokio::test]
    async fn test_build_indexes_without_publication(
        #[case] build_ranges: bool,
        #[case] fail_series: bool,
    ) {
        let mut env = TestEnv::with_prefix("series-builder").await;
        let (engine, region) = prepare_region(&mut env).await;
        let version = region.version();
        let (bucket, entry) = build_input(&version);
        let files = &bucket.files;
        let store = ObjectStore::new(Memory::default()).unwrap();
        let (purger, mut receiver) = series_index_channel(store.clone());
        let mut range_bytes = HashMap::new();
        if build_ranges {
            for file in files {
                let id = build_range_index(&store, &region, &version, file.clone())
                    .await
                    .unwrap()
                    .unwrap();
                range_bytes.insert(
                    id,
                    store
                        .read(&range_index_path(region.region_id, id))
                        .await
                        .unwrap()
                        .to_bytes(),
                );
            }
        }
        // Both builders use the captured version, even if live metadata changes.
        region
            .version_control
            .alter_metadata(metadata_with_seconds(&version));
        let layer = writer_layer(&WriterStates::default(), move |path| {
            assert!(path.contains("/series/"), "series stage opened {path}");
            if fail_series {
                WriterFailure::Finish
            } else {
                WriterFailure::None
            }
        });
        let result = build_series_index(
            &store.clone().layer(layer),
            &region,
            &version,
            &bucket,
            &entry,
            &purger,
        )
        .await;
        if fail_series {
            assert!(result.is_err());
        } else {
            let handle = result.unwrap();
            assert_eq!(handle.entry(), &entry);
            assert!(
                store
                    .exists(&series_index_path(region.region_id, entry.index_uuid))
                    .await
                    .unwrap()
            );
            store_catalog(
                &store,
                &series_catalog_path(region.region_id),
                &SeriesIndexCatalog {
                    indexes: vec![handle.entry().clone()],
                },
            )
            .await
            .unwrap();
            let recovered = load_version_control(&store, region.region_id, &purger)
                .await
                .current();
            let repeated = plan_series_indexes(
                group_files_into_series_buckets(files, 100, 10),
                recovered.index_buckets.clone(),
                None,
                0,
            );
            assert!(repeated.builds.is_empty());
            assert_eq!(recovered.index_buckets, repeated.index_buckets);
        }
        // Series success or failure leaves completed range indexes unchanged.
        for file in files {
            let id = file.file_id().file_id();
            let path = range_index_path(region.region_id, id);
            if let Some(bytes) = range_bytes.get(&id) {
                assert_eq!(*bytes, store.read(&path).await.unwrap().to_bytes());
            } else {
                assert!(!store.exists(&path).await.unwrap());
            }
        }
        assert!(region.series_index_version().range_indexes.is_empty());
        assert!(region.series_index_version().series_indexes.is_empty());
        assert!(receiver.try_recv().is_err());
        engine.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_range_failure_preserves_completed_indexes_and_stops_series_stage() {
        let mut env = TestEnv::with_prefix("range-builder-stage-failure").await;
        let (engine, region) = prepare_region(&mut env).await;
        let version = region.version();
        let (bucket, entry) = build_input(&version);
        let files = &bucket.files;
        let failed_path = range_index_path(region.region_id, files[1].file_id().file_id());
        let states = WriterStates::default();
        let layer = writer_layer(&states, move |path| {
            if path == failed_path {
                WriterFailure::Finish
            } else {
                WriterFailure::None
            }
        });
        let store = ObjectStore::new(Memory::default()).unwrap().layer(layer);
        let (purger, _receiver) = series_index_channel(store.clone());
        let mut completed = Vec::new();
        let result: Result<Option<SeriesIndexFileHandle>> = async {
            for file in files {
                let Some(id) = build_range_index(&store, &region, &version, file.clone()).await?
                else {
                    // Defer series construction if a needed range is not ready.
                    return Ok(None);
                };
                completed.push(id);
            }
            build_series_index(&store, &region, &version, &bucket, &entry, &purger)
                .await
                .map(Some)
        }
        .await;
        assert!(format!("{:?}", result.unwrap_err()).contains("injected index finish failure"));
        assert_eq!(vec![files[0].file_id().file_id()], completed);
        {
            let states = states.lock().unwrap();
            assert_eq!(2, states.len());
            assert!(states.keys().all(|path| !path.contains("/series/")));
            assert_eq!(
                1,
                states[&range_index_path(region.region_id, completed[0])].closed
            );
            assert_eq!(
                1,
                states[&range_index_path(region.region_id, files[1].file_id().file_id())].aborted
            );
        }
        for (i, file) in files.iter().enumerate() {
            assert_eq!(
                i == 0,
                store
                    .exists(&range_index_path(
                        region.region_id,
                        file.file_id().file_id()
                    ))
                    .await
                    .unwrap()
            );
        }
        engine.stop().await.unwrap();
    }

    #[derive(Default)]
    struct WriterState {
        closed: usize,
        aborted: usize,
    }

    type WriterStates = Arc<Mutex<BTreeMap<String, WriterState>>>;

    enum WriterFailure {
        None,
        Finish,
        Abort,
    }

    fn writer_layer(
        states: &WriterStates,
        failure: impl Fn(&str) -> WriterFailure + Send + Sync + 'static,
    ) -> mock::MockLayer {
        let states = states.clone();
        MockLayerBuilder::default()
            .writer_factory(Arc::new(move |path, _, inner| -> oio::Writer {
                states
                    .lock()
                    .unwrap()
                    .insert(path.to_string(), WriterState::default());
                Box::new(RecordingWriter {
                    inner,
                    path: path.to_string(),
                    states: states.clone(),
                    failure: failure(path),
                })
            }))
            .build()
            .unwrap()
    }

    struct RecordingWriter {
        inner: oio::Writer,
        path: String,
        states: WriterStates,
        failure: WriterFailure,
    }

    impl mock::Write for RecordingWriter {
        async fn write(&mut self, buffer: mock::Buffer) -> mock::Result<()> {
            self.inner.write(buffer).await
        }

        async fn close(&mut self) -> mock::Result<mock::Metadata> {
            if matches!(self.failure, WriterFailure::Finish) {
                return Err(mock::Error::new(
                    mock::ErrorKind::Unexpected,
                    "injected index finish failure",
                ));
            }
            let metadata = self.inner.close().await?;
            self.states
                .lock()
                .unwrap()
                .get_mut(&self.path)
                .unwrap()
                .closed += 1;
            Ok(metadata)
        }

        async fn abort(&mut self) -> mock::Result<()> {
            self.states
                .lock()
                .unwrap()
                .get_mut(&self.path)
                .unwrap()
                .aborted += 1;
            if matches!(self.failure, WriterFailure::Abort) {
                return Err(mock::Error::new(
                    mock::ErrorKind::Unexpected,
                    "injected abort failure",
                ));
            }
            self.inner.abort().await
        }
    }

    struct FailingSstReader {
        inner: oio::Reader,
        fail: Arc<AtomicBool>,
    }

    impl mock::Read for FailingSstReader {
        async fn read(
            &self,
            range: mock::BytesRange,
        ) -> mock::Result<(mock::RpRead, mock::Buffer)> {
            if self.fail.load(Ordering::Relaxed) {
                return Err(mock::Error::new(
                    mock::ErrorKind::Unexpected,
                    "injected SST read failure",
                ));
            }
            self.inner.read(range).await
        }

        async fn open(
            &self,
            range: mock::BytesRange,
        ) -> mock::Result<(mock::RpRead, Box<dyn mock::ReadStreamDyn>)> {
            if self.fail.load(Ordering::Relaxed) {
                return Err(mock::Error::new(
                    mock::ErrorKind::Unexpected,
                    "injected SST read failure",
                ));
            }
            self.inner.open(range).await
        }
    }

    #[rstest::rstest]
    #[case::range(false, false, false)]
    #[case::range_abort_failure(false, false, true)]
    #[case::series_setup(true, true, false)]
    #[case::series_read(true, false, false)]
    #[case::series_abort_failure(true, false, true)]
    #[tokio::test]
    async fn test_read_failure_aborts_unfinished_outputs(
        #[case] series: bool,
        #[case] fail_before_open: bool,
        #[case] fail_abort: bool,
    ) {
        let fail = Arc::new(AtomicBool::new(false));
        let read_fail = fail.clone();
        let read_layer = MockLayerBuilder::default()
            .reader_factory(Arc::new(move |path, _, inner| -> oio::Reader {
                if path.ends_with(".parquet") {
                    Box::new(FailingSstReader {
                        inner,
                        fail: read_fail.clone(),
                    })
                } else {
                    inner
                }
            }))
            .build()
            .unwrap();
        let mut env = TestEnv::with_prefix("series-builder-read-failure")
            .await
            .with_mock_layer(read_layer);
        let (engine, region) = prepare_region(&mut env).await;
        let version = region.version();
        let (mut bucket, mut entry) = build_input(&version);
        // A single source is first polled after opening the series writer.
        bucket.files.truncate(1);
        entry.source_file_ids = vec![bucket.files[0].file_id().file_id()];
        let states = WriterStates::default();
        let write_fail = fail.clone();
        let write_layer = writer_layer(&states, move |_| {
            write_fail.store(true, Ordering::Relaxed);
            if fail_abort {
                WriterFailure::Abort
            } else {
                WriterFailure::None
            }
        });
        let store = ObjectStore::new(Memory::default())
            .unwrap()
            .layer(write_layer);
        fail.store(fail_before_open, Ordering::Relaxed);
        let result = if !series {
            build_range_index(&store, &region, &version, bucket.files[0].clone())
                .await
                .map(|_| ())
        } else {
            let (purger, _receiver) = series_index_channel(store.clone());
            build_series_index(&store, &region, &version, &bucket, &entry, &purger)
                .await
                .map(|_| ())
        };
        fail.store(false, Ordering::Relaxed);
        let error = result.unwrap_err();
        assert!(
            format!("{error:?}").contains("injected SST read failure"),
            "{error:?}"
        );
        {
            let states = states.lock().unwrap();
            assert_eq!(usize::from(!fail_before_open), states.len());
            for (path, state) in states.iter() {
                assert_eq!(0, state.closed, "{path}");
                assert_eq!(1, state.aborted, "{path}");
            }
        }
        assert!(store.list("/").await.unwrap().is_empty());
        assert!(region.series_index_version().series_indexes.is_empty());
        engine.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_series_batch_error_aborts_writer() {
        let mut env = TestEnv::with_prefix("series-builder-batch-failure").await;
        let (engine, region) = prepare_region(&mut env).await;
        let version = region.version();
        let (bucket, entry) = build_input(&version);
        let files = &bucket.files;
        // Give the series writer a different timestamp unit from the SST batches.
        let mut bad_version = (*version).clone();
        bad_version.metadata = metadata_with_seconds(&version);
        let bad_version = Arc::new(bad_version);
        let states = WriterStates::default();
        let layer = writer_layer(&states, |_| WriterFailure::None);
        let store = ObjectStore::new(Memory::default()).unwrap().layer(layer);
        let (purger, _receiver) = series_index_channel(store.clone());
        for file in files {
            build_range_index(&store, &region, &version, file.clone())
                .await
                .unwrap()
                .unwrap();
        }
        let error = build_series_index(&store, &region, &bad_version, &bucket, &entry, &purger)
            .await
            .unwrap_err();
        assert!(
            format!("{error:?}").contains("does not match the index unit"),
            "{error:?}"
        );
        let path = series_index_path(region.region_id, entry.index_uuid);
        {
            let states = states.lock().unwrap();
            assert_eq!(1, states[&path].aborted);
            assert_eq!(0, states[&path].closed);
            for state in states.values() {
                assert_eq!(1, state.aborted + state.closed);
            }
        }
        assert!(!store.exists(&path).await.unwrap());
        assert!(region.series_index_version().series_indexes.is_empty());
        engine.stop().await.unwrap();
    }
}
