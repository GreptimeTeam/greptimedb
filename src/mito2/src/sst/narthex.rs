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

//! Native Narthex sample and series-index writers.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;

use datatypes::arrow::array::{Array, BinaryArray};
use datatypes::arrow::compute::{cast, max, min};
use datatypes::arrow::datatypes::DataType;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use snafu::ResultExt;
use store_api::metadata::RegionMetadata;
use store_api::storage::FileId;

use crate::access_layer::SstInfoArray;
use crate::access_layer::{AccessLayer, SstWriteRequest};
use crate::error::{ComputeArrowSnafu, InvalidRecordBatchSnafu, Result};
use crate::series_index::artifact::SamplesFileHandle;
use crate::series_index::columnar::reader::ColumnarReader;
use crate::series_index::columnar::{ColumnarSeriesFormat, invalid, require};
use crate::series_index::format::{FormatWriter, SamplesFormatReader};
use crate::series_index::identity::ExactSeriesKey;
use crate::series_index::samples::ColumnarSamplesFormat;
use crate::series_index::samples::reader::SamplesReader;
use crate::sst::file::{
    FileFormat, FileHandle, FileMeta, NarthexPartMeta, NarthexSamplesMeta, NarthexSeriesIndexMeta,
    RegionFileId,
};
use crate::sst::location;
use crate::sst::parquet::SstInfo;
use crate::sst::parquet::writer::SstWriteGuard;

/// Reuses the narthex codec and the caller's merged, sequence-normalized source.
pub(crate) async fn write_samples(
    layer: &AccessLayer,
    mut request: SstWriteRequest,
    guard: Option<Arc<dyn SstWriteGuard>>,
    cancellation: Option<tokio_util::sync::CancellationToken>,
) -> Result<SstInfoArray> {
    let id = FileId::random();
    let region_id = request.metadata.region_id;
    if let Some(guard) = guard {
        guard
            .before_create_narthex(RegionFileId::new(region_id, id))
            .await?;
    }
    let prefix = location::narthex_file_dir(&layer.build_region_dir(region_id), id);
    let mut writer = ColumnarSamplesFormat::default().writer(
        request.metadata.clone(),
        layer.object_store().clone(),
        &prefix,
        id,
        id,
    )?;
    let mut rows = 0;
    let mut series = 0;
    let mut previous_key = Vec::new();
    let mut lower = i64::MAX;
    let mut upper = i64::MIN;
    let timestamp = &request.metadata.time_index_column().column_schema.name;
    let write_batches = async {
        while let Some(batch) = request.source.next_batch().await? {
            if batch.num_rows() == 0 {
                continue;
            }
            let ts = batch.column_by_name(timestamp).ok_or_else(|| {
                InvalidRecordBatchSnafu {
                    reason: "narthex samples lack timestamp".to_string(),
                }
                .build()
            })?;
            let (values, _) =
                datatypes::timestamp::timestamp_array_to_primitive(ts).ok_or_else(|| {
                    InvalidRecordBatchSnafu {
                        reason: "invalid narthex timestamp".to_string(),
                    }
                    .build()
                })?;
            lower = lower.min(min(&values).unwrap_or(lower));
            upper = upper.max(max(&values).unwrap_or(upper));
            let keys = batch.column_by_name("__primary_key").ok_or_else(|| {
                InvalidRecordBatchSnafu {
                    reason: "narthex samples lack primary key".to_string(),
                }
                .build()
            })?;
            let keys = cast(keys, &DataType::Binary).context(ComputeArrowSnafu)?;
            let keys = keys.as_any().downcast_ref::<BinaryArray>().ok_or_else(|| {
                InvalidRecordBatchSnafu {
                    reason: "invalid narthex primary key".to_string(),
                }
                .build()
            })?;
            for key in keys.iter().flatten() {
                if key != previous_key {
                    series += 1;
                    previous_key.clear();
                    previous_key.extend_from_slice(key);
                }
            }
            rows += batch.num_rows();
            // Narthex files always retain row sequences; import barriers are applied by readers.
            writer.write(&batch).await?;
        }
        Ok(())
    };
    let write_result: Result<()> = if let Some(signal) = cancellation {
        tokio::select! {
            biased;
            _ = signal.cancelled() => crate::error::CompactionCancelledSnafu.fail(),
            result = write_batches => result,
        }
    } else {
        write_batches.await
    };
    if let Err(error) = write_result {
        return Err(abort_narthex_write(writer, error).await);
    }
    if rows == 0 {
        writer.abort().await?;
        return Ok(Default::default());
    }
    let artifact = writer.finish().await?;
    // The samples codec returns its manifest first, then parts in ID order.
    let (manifest, parts) = artifact.objects.split_first().ok_or_else(|| {
        InvalidRecordBatchSnafu {
            reason: "missing Narthex samples manifest",
        }
        .build()
    })?;
    let unit = request.metadata.time_index_type().unit();
    Ok(smallvec::smallvec![SstInfo {
        file_id: id,
        time_range: (
            common_time::Timestamp::new(lower, unit),
            common_time::Timestamp::new(upper, unit)
        ),
        file_size: artifact.file_size,
        num_rows: rows,
        num_series: series,
        num_row_groups: 0,
        max_row_group_uncompressed_size: 0,
        max_row_group_decoded_size: 0,
        file_metadata: None,
        index_metadata: Default::default(),
        format: FileFormat::NarthexSamples(Box::new(NarthexSamplesMeta {
            manifest_size: manifest.file_size,
            parts: parts
                .iter()
                .map(|part| NarthexPartMeta {
                    file_size: part.file_size
                })
                .collect()
        })),
    }])
}

/// Reconstruct the canonical sparse key from a native series row, in SST order.
async fn series_keys(
    layer: &AccessLayer,
    source: &FileHandle,
    metadata: &RegionMetadata,
) -> Result<BoxStream<'static, Result<(ExactSeriesKey, u64)>>> {
    let mut reader = ColumnarReader::open(
        layer.object_store().clone(),
        source.clone(),
        &layer.build_region_dir(source.region_id()),
    )
    .await?;
    reader.adapt_tags(metadata)?;
    Ok(reader.read_full_keys())
}

/// Merge sorted sample keys or stored index rows. A separate manifest edit publishes the index.
pub(crate) async fn write_series(
    layer: &AccessLayer,
    metadata: store_api::metadata::RegionMetadataRef,
    sources: &[FileHandle],
    window: crate::sst::file::WindowSequence,
) -> Result<crate::sst::file::FileMeta> {
    require(!sources.is_empty(), "narthex series index requires inputs")?;
    let id = FileId::random();
    let prefix = location::narthex_file_dir(&layer.build_region_dir(metadata.region_id), id);
    let mut file = FileMeta {
        region_id: metadata.region_id,
        file_id: id,
        time_range: sources
            .iter()
            .map(|file| file.meta_ref().time_range)
            .reduce(|left, right| (left.0.min(right.0), left.1.max(right.1)))
            .ok_or_else(|| {
                crate::series_index::columnar::invalid("empty narthex index source set")
            })?,
        format: FileFormat::NarthexSeriesIndex(Box::new(NarthexSeriesIndexMeta {
            manifest_size: 0,
            parts: Default::default(),
            window,
        })),
        ..Default::default()
    };
    let mut writer = ColumnarSeriesFormat::default().narthex_writer(
        metadata.clone(),
        layer.object_store().clone(),
        &file,
        prefix,
    )?;
    let write_result: Result<()> = async {
        let mut streams = Vec::with_capacity(sources.len());
        let mut heads = BinaryHeap::new();
        for source in sources {
            let mut stream = match &source.meta_ref().format {
                FileFormat::NarthexSeriesIndex(_) => series_keys(layer, source, &metadata).await?,
                FileFormat::NarthexSamples(_) => {
                    let handle = SamplesFileHandle::from_narthex(
                        source.clone(),
                        &layer.build_region_dir(source.region_id()),
                    )?;
                    SamplesReader::open(layer.object_store().clone(), handle)
                        .await?
                        .read(Default::default())?
                        .map_ok(|block| (block.key, block.batch.num_rows() as u64))
                        .boxed()
                }
                FileFormat::Parquet => {
                    return Err(invalid("expected native series or samples"));
                }
            };
            if let Some((key, rows)) = stream.try_next().await? {
                heads.push(Reverse((key, streams.len(), rows)));
            }
            streams.push(stream);
        }
        while let Some(Reverse((key, index, rows))) = heads.pop() {
            writer.append_key(key.bytes(), rows).await?;
            if let Some((key, rows)) = streams[index].try_next().await? {
                heads.push(Reverse((key, index, rows)));
            }
        }
        Ok(())
    }
    .await;
    if let Err(error) = write_result {
        return Err(abort_narthex_write(writer, error).await);
    }
    let artifact = writer.finish().await?;
    file.file_size = artifact.file_size;
    if let FileFormat::NarthexSeriesIndex(index) = &mut file.format {
        // The series codec returns its manifest after the ordered parts.
        let (manifest, parts) = artifact.objects.split_last().ok_or_else(|| {
            crate::series_index::columnar::invalid("missing Narthex series manifest")
        })?;
        index.manifest_size = manifest.file_size;
        index.parts = parts
            .iter()
            .map(|part| NarthexPartMeta {
                file_size: part.file_size,
            })
            .collect();
    }
    Ok(file)
}

/// Complete cleanup before reporting a failed write. Drop cleanup remains a fallback
/// for cancellation, but can race runtime shutdown after an ordinary returned error.
pub(crate) async fn abort_narthex_write(
    writer: Box<dyn crate::series_index::format::FormatWriter>,
    error: crate::error::Error,
) -> crate::error::Error {
    if let Err(cleanup_error) = writer.abort().await {
        common_telemetry::warn!(cleanup_error; "Failed to abort unpublished narthex artifact");
    }
    error
}
