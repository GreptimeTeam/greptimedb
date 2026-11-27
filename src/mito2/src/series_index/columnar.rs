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

//! Columnar series format. See `docs/formats/mito-series.md` for the wire contract.

pub(super) mod codec;
mod metadata;
pub(crate) mod reader;
#[cfg(test)]
mod tests;
mod writer;

use std::sync::Arc;

use crate::sst::file::{FileMeta, WindowSequence};
use common_time::Timestamp;
use datatypes::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::FileId;

use crate::error::{InvalidMetaSnafu, Result};
use crate::series_index::artifact::ArtifactFormat;
use crate::series_index::identity::IdentityEncoding;
use crate::series_index::{MetricSeriesId, ROW_COUNT_COLUMN, TABLE_ID_COLUMN, TSID_COLUMN};

pub(crate) const VERSION: u32 = 1;
pub(crate) const MANIFEST_LIMIT: usize = 64 * 1024 * 1024;

/// Packing and compression choices are internal and do not change the wire version.
#[derive(Debug, Clone)]
pub(crate) struct ColumnarSeriesFormat {
    pub(crate) block_rows: usize,
    pub(crate) zstd_level: i32,
    pub(crate) compression: codec::Compression,
    pub(crate) part_bytes: u64,
    pub(crate) metadata_bytes: usize,
    pub(crate) page_bytes: usize,
    pub(crate) root_capacity: usize,
}

impl Default for ColumnarSeriesFormat {
    fn default() -> Self {
        Self {
            block_rows: 8192,
            zstd_level: 3,
            compression: codec::Compression::Zstd,
            part_bytes: 512 * 1024 * 1024,
            metadata_bytes: 8 * 1024 * 1024,
            page_bytes: metadata::PAGE_TARGET,
            root_capacity: metadata::ROOT_CAPACITY,
        }
    }
}

impl ColumnarSeriesFormat {
    pub(crate) fn narthex_writer(
        &self,
        metadata: RegionMetadataRef,
        store: ObjectStore,
        file: &FileMeta,
        prefix: String,
    ) -> Result<Box<writer::ColumnarWriter>> {
        Ok(Box::new(writer::ColumnarWriter::new(
            self.clone(),
            metadata,
            store,
            file,
            prefix,
        )?))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Tag {
    pub(crate) column_id: u32,
    pub(crate) name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Part {
    pub(crate) part_id: u32,
    pub(crate) file_size: u64,
    pub(crate) num_series: u64,
    pub(crate) first: MetricSeriesId,
    pub(crate) last: MetricSeriesId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Manifest {
    #[serde(skip)]
    pub(crate) prefix: String,
    pub(crate) format: ArtifactFormat,
    pub(crate) version: u32,
    /// Mandatory discriminator; see [`IdentityEncoding`]. Never defaulted on read.
    pub(crate) identity_encoding: IdentityEncoding,
    pub(crate) region_id: u64,
    pub(crate) generation: FileId,
    pub(crate) tags: Vec<Tag>,
    pub(crate) coverage: SeriesCoverage,
    pub(crate) files: Vec<Part>,
}

/// V1 series-file coverage header. The region manifest holds the certified window.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SeriesCoverage {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    artifact: Option<crate::series_index::artifact::ArtifactMetadata>,
    file_size: u64,
    index_uuid: FileId,
    bucket_start: Timestamp,
    bucket_end: Timestamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source_file_ids: Option<Vec<FileId>>,
    min_file_sequence: u64,
    max_file_sequence: u64,
    compaction_window_secs: i64,
    window_sequences: BTreeMap<i64, WindowSequence>,
}

impl SeriesCoverage {
    fn from_file(file: &FileMeta) -> Self {
        Self {
            artifact: None,
            file_size: 0,
            index_uuid: file.file_id,
            bucket_start: file.time_range.0,
            bucket_end: file.time_range.1,
            source_file_ids: None,
            min_file_sequence: 0,
            max_file_sequence: 0,
            compaction_window_secs: 0,
            window_sequences: BTreeMap::new(),
        }
    }
}

impl Manifest {
    fn retained_size(&self) -> usize {
        std::mem::size_of::<Self>() + self.prefix.capacity() + self.files.capacity() * std::mem::size_of::<Part>()
            + self.tags.capacity() * std::mem::size_of::<Tag>()
            + self.tags.iter().map(|tag| tag.name.capacity()).sum::<usize>()
            + self.coverage.source_file_ids.as_ref().map_or(0, Vec::capacity) * std::mem::size_of::<FileId>()
            // BTree nodes include allocation and partially occupied key/value slots.
            + 512
            + self.coverage.window_sequences.len() * 128
    }

    pub(crate) fn part_path(&self, id: u32) -> String {
        format!("{}part-{id}.series", self.prefix)
    }
    pub(crate) fn path(&self) -> String {
        format!("{}manifest.json", self.prefix)
    }
    pub(crate) fn schema(&self) -> SchemaRef {
        let mut fields = vec![
            Field::new(TABLE_ID_COLUMN, DataType::UInt32, false),
            Field::new(TSID_COLUMN, DataType::UInt64, false),
            Field::new(ROW_COUNT_COLUMN, DataType::UInt64, false),
        ];
        fields.extend(self.tags.iter().map(|tag| {
            crate::sst::with_field_id(Field::new(&tag.name, DataType::Utf8, true), tag.column_id)
        }));
        Arc::new(Schema::new(fields))
    }
}

pub(crate) fn invalid(reason: impl Into<String>) -> crate::error::Error {
    InvalidMetaSnafu {
        reason: reason.into(),
    }
    .build()
}

pub(crate) fn require(valid: bool, reason: &str) -> Result<()> {
    if valid { Ok(()) } else { Err(invalid(reason)) }
}

pub(crate) fn unsupported(reason: impl Into<String>) -> crate::error::Error {
    crate::error::UnsupportedOperationSnafu {
        err_msg: reason.into(),
    }
    .build()
}
