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

//! Authoritative narthex samples SST writing.

use std::sync::Arc;

use datatypes::arrow::array::{Array, BinaryArray};
use datatypes::arrow::compute::{cast, max, min};
use datatypes::arrow::datatypes::DataType;
use snafu::ResultExt;
use store_api::storage::FileId;

use crate::access_layer::SstInfoArray;
use crate::access_layer::{AccessLayer, SstWriteRequest};
use crate::error::{ComputeArrowSnafu, InvalidRecordBatchSnafu, Result};
use crate::series_index::format::SamplesFormat;
use crate::series_index::samples::ColumnarSamplesFormat;
use crate::sst::file::{FileFormat, NarthexPartMeta, NarthexSamplesMeta, RegionFileId};
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
    let mut writer = ColumnarSamplesFormat::default()
        .writer(
            request.metadata.clone(),
            layer.object_store().clone(),
            &prefix,
            id,
            id,
        )
        .await?;
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

/// Merge the sorted sample key areas. A separate manifest edit publishes the index.
#[cfg(test)]
pub(crate) async fn write_series(
    layer: &AccessLayer,
    metadata: store_api::metadata::RegionMetadataRef,
    sources: &[crate::sst::file::FileHandle],
) -> Result<crate::sst::file::FileMeta> {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    use crate::series_index::artifact::SamplesFileHandle;
    use crate::series_index::columnar::{ColumnarSeriesFormat, require};
    use crate::series_index::format::{FormatWriter, SamplesFormatReader};
    use crate::series_index::samples::reader::SamplesReader;
    use crate::sst::file::{FileMeta, NarthexSeriesIndexMeta};
    use futures::TryStreamExt;
    require(!sources.is_empty(), "narthex series index requires samples")?;
    require(
        sources
            .iter()
            .all(|source| matches!(source.meta_ref().format, FileFormat::NarthexSamples(_))),
        "narthex series index requires narthex sample sources",
    )?;
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
            sources: sources.iter().map(|file| file.file_id()).collect(),
        })),
        ..Default::default()
    };
    let entry = crate::series_index::narthex_series_entry(
        &file,
        &layer.build_region_dir(metadata.region_id),
    )?;
    let mut writer = ColumnarSeriesFormat::default().narthex_writer(
        metadata,
        layer.object_store().clone(),
        &entry,
        prefix,
    )?;
    let write_result: Result<()> = async {
        let mut streams = Vec::with_capacity(sources.len());
        let mut heads = BinaryHeap::new();
        for source in sources {
            let handle = SamplesFileHandle::from_narthex(
                source.clone(),
                &layer.build_region_dir(source.region_id()),
            )?;
            let reader = SamplesReader::open(layer.object_store().clone(), handle).await?;
            // Empty projection and no time predicate read only metadata and exact keys.
            let mut stream = reader.read(Default::default())?;
            if let Some(block) = stream.try_next().await? {
                heads.push(Reverse((block.key, streams.len())));
            }
            streams.push(stream);
        }
        let mut previous = None;
        while let Some(Reverse((key, index))) = heads.pop() {
            if previous.as_ref() != Some(&key) {
                writer.append_key(key.bytes()).await?;
                previous = Some(key);
            }
            if let Some(block) = streams[index].try_next().await? {
                heads.push(Reverse((block.key, index)));
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
