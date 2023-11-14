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

use common_base::readable_size::ReadableSize;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::RegionId;

use crate::access_layer::AccessLayerRef;
use crate::error;
use crate::read::projection::ProjectionMapper;
use crate::read::seq_scan::SeqScan;
use crate::read::{BoxedBatchReader, Source};
use crate::sst::file::{FileHandle, FileId, FileMeta, Level};
use crate::sst::parquet::{SstInfo, WriteOptions};

#[derive(Debug)]
pub(crate) struct CompactionOutput {
    pub output_file_id: FileId,
    /// Compaction output file level.
    pub output_level: Level,
    /// Compaction input files.
    pub inputs: Vec<FileHandle>,
}

impl CompactionOutput {
    pub(crate) async fn build(
        &self,
        region_id: RegionId,
        schema: RegionMetadataRef,
        sst_layer: AccessLayerRef,
        sst_write_buffer_size: ReadableSize,
    ) -> error::Result<Option<FileMeta>> {
        let reader = build_sst_reader(schema.clone(), sst_layer.clone(), &self.inputs).await?;

        let opts = WriteOptions {
            write_buffer_size: sst_write_buffer_size,
            ..Default::default()
        };

        // TODO(hl): measure merge elapsed time.

        let mut writer = sst_layer.write_sst(self.output_file_id, schema, Source::Reader(reader));
        let meta = writer.write_all(&opts).await?.map(
            |SstInfo {
                 time_range,
                 file_size,
                 ..
             }| {
                FileMeta {
                    region_id,
                    file_id: self.output_file_id,
                    time_range,
                    level: self.output_level,
                    file_size,
                }
            },
        );

        Ok(meta)
    }
}

/// Builds [BoxedBatchReader] that reads all SST files and yields batches in primary key order.
async fn build_sst_reader(
    schema: RegionMetadataRef,
    sst_layer: AccessLayerRef,
    inputs: &[FileHandle],
) -> error::Result<BoxedBatchReader> {
    SeqScan::new(sst_layer, ProjectionMapper::all(&schema)?)
        .with_files(inputs.to_vec())
        // We ignore file not found error during compaction.
        .with_ignore_file_not_found(true)
        .build_reader()
        .await
}
