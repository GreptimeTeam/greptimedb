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
mod reader;
#[cfg(test)]
mod tests;
mod writer;

use std::sync::Arc;

use async_trait::async_trait;
use datatypes::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::FileId;

use crate::error::{InvalidMetaSnafu, Result};
use crate::series_index::artifact::ArtifactFormat;
use crate::series_index::catalog::SeriesIndexEntry;
use crate::series_index::format::{FormatReader, FormatWriter, SeriesIndexFormat};
use crate::series_index::{
    MetricSeriesId, ROW_COUNT_COLUMN, SeriesIndexFileHandle, TABLE_ID_COLUMN, TSID_COLUMN,
};

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

#[async_trait]
impl SeriesIndexFormat for ColumnarSeriesFormat {
    fn format(&self) -> ArtifactFormat {
        ArtifactFormat::ColumnarSeries
    }

    async fn writer(
        &self,
        metadata: RegionMetadataRef,
        store: ObjectStore,
        entry: &SeriesIndexEntry,
    ) -> Result<Box<dyn FormatWriter>> {
        Ok(Box::new(writer::ColumnarWriter::new(
            self.clone(),
            metadata,
            store,
            entry,
        )?))
    }

    async fn reader(
        &self,
        store: ObjectStore,
        handle: SeriesIndexFileHandle,
    ) -> Result<Box<dyn FormatReader>> {
        Ok(Box::new(reader::ColumnarReader::open(store, handle).await?))
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
    pub(crate) format: ArtifactFormat,
    pub(crate) version: u32,
    pub(crate) region_id: u64,
    pub(crate) generation: FileId,
    pub(crate) tags: Vec<Tag>,
    pub(crate) coverage: SeriesIndexEntry,
    pub(crate) files: Vec<Part>,
}

impl Manifest {
    pub(crate) fn prefix(&self) -> String {
        format!("{}/series/{}/", self.region_id, self.generation)
    }
    pub(crate) fn part_path(&self, id: u32) -> String {
        format!("{}part-{id}.series", self.prefix())
    }
    pub(crate) fn path(&self) -> String {
        format!("{}manifest.json", self.prefix())
    }
    pub(crate) fn schema(&self) -> SchemaRef {
        let mut fields = vec![
            Field::new(TABLE_ID_COLUMN, DataType::UInt32, false),
            Field::new(TSID_COLUMN, DataType::UInt64, false),
            Field::new(ROW_COUNT_COLUMN, DataType::UInt64, false),
        ];
        fields.extend(
            self.tags
                .iter()
                .map(|tag| Field::new(&tag.name, DataType::Utf8, true)),
        );
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
