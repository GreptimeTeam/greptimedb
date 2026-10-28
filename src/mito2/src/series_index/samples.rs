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

//! Columnar samples sidecars. Wire contract: `docs/formats/mito-samples.md`.

mod codec;
mod metadata;
mod reader;
#[cfg(test)]
mod tests;
mod writer;

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use datatypes::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::FileId;

use crate::error::Result;
use crate::series_index::artifact::{ArtifactFormat, SamplesFileHandle};
use crate::series_index::columnar::{invalid, require, unsupported};
use crate::series_index::format::{FormatWriter, SamplesFormat, SamplesFormatReader};
use crate::series_index::{MetricSeriesId, TABLE_ID_COLUMN, TSID_COLUMN};

const VERSION: u32 = 1;
const MANIFEST_LIMIT: usize = 64 * 1024 * 1024;
const MAX_ROWS: usize = 8192;
const MAX_FIELDS: usize = 1024;
const BLOCK_BYTES: usize = 256 * 1024;
const SEQUENCE: &str = "__sequence";
const OPERATION: &str = "__op_type";

/// Internal packing knobs; the backend is disabled unless selected explicitly.
#[derive(Debug, Clone)]
pub(crate) struct ColumnarSamplesFormat {
    pub(crate) block_rows: usize,
    pub(crate) part_bytes: u64,
    pub(crate) metadata_bytes: usize,
    pub(crate) page_bytes: usize,
    pub(crate) root_capacity: usize,
}
impl Default for ColumnarSamplesFormat {
    fn default() -> Self {
        Self {
            block_rows: MAX_ROWS,
            part_bytes: 512 * 1024 * 1024,
            metadata_bytes: 8 * 1024 * 1024,
            page_bytes: metadata::PAGE_LIMIT,
            root_capacity: metadata::ROOT_CAPACITY,
        }
    }
}
#[async_trait]
impl SamplesFormat for ColumnarSamplesFormat {
    fn format(&self) -> ArtifactFormat {
        ArtifactFormat::ColumnarSamples
    }
    async fn writer(
        &self,
        metadata: RegionMetadataRef,
        store: ObjectStore,
        prefix: &str,
        source_file_id: FileId,
        generation: FileId,
    ) -> Result<Box<dyn FormatWriter>> {
        Ok(Box::new(writer::SamplesWriter::new(
            self.clone(),
            metadata,
            store,
            prefix,
            source_file_id,
            generation,
        )?))
    }
    async fn reader(
        &self,
        store: ObjectStore,
        handle: SamplesFileHandle,
    ) -> Result<Box<dyn SamplesFormatReader>> {
        Ok(Box::new(reader::SamplesReader::open(store, handle).await?))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TimestampColumn {
    column_id: u32,
    name: String,
    unit: TimestampUnit,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum TimestampUnit {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}
impl TimestampUnit {
    fn arrow(self) -> TimeUnit {
        match self {
            Self::Second => TimeUnit::Second,
            Self::Millisecond => TimeUnit::Millisecond,
            Self::Microsecond => TimeUnit::Microsecond,
            Self::Nanosecond => TimeUnit::Nanosecond,
        }
    }
    fn from_arrow(unit: &TimeUnit) -> Self {
        match unit {
            TimeUnit::Second => Self::Second,
            TimeUnit::Millisecond => Self::Millisecond,
            TimeUnit::Microsecond => Self::Microsecond,
            TimeUnit::Nanosecond => Self::Nanosecond,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ValueColumn {
    column_id: u32,
    name: String,
    #[serde(rename = "type")]
    data_type: String,
    nullable: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Part {
    part_id: u32,
    file_size: u64,
    num_rows: u64,
    num_blocks: u64,
    first: MetricSeriesId,
    last: MetricSeriesId,
    min_timestamp: i64,
    max_timestamp: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Manifest {
    format: ArtifactFormat,
    version: u32,
    region_id: u64,
    source_file_id: FileId,
    generation: FileId,
    timestamp: TimestampColumn,
    fields: Vec<ValueColumn>,
    files: Vec<Part>,
}
impl Manifest {
    fn prefix(&self) -> String {
        samples_prefix(self.region_id, self.generation)
    }
    fn path(&self) -> String {
        format!("{}manifest.json", self.prefix())
    }
    fn part_path(&self, id: u32) -> String {
        format!("{}part-{id}.samples", self.prefix())
    }
    fn validate_schema(&self) -> Result<()> {
        require(
            (1..=MAX_FIELDS).contains(&self.fields.len()),
            "invalid samples field count",
        )?;
        let mut names = HashSet::from([TABLE_ID_COLUMN, TSID_COLUMN, SEQUENCE, OPERATION]);
        require(
            names.insert(&self.timestamp.name),
            "duplicate samples timestamp name",
        )?;
        let mut ids = HashSet::from([self.timestamp.column_id]);
        for field in &self.fields {
            require(
                field.data_type == "float64" && field.nullable,
                "unsupported samples field type/nullability",
            )?;
            require(
                names.insert(&field.name) && ids.insert(field.column_id),
                "duplicate samples field name/id",
            )?;
        }
        Ok(())
    }
    fn schema(&self) -> SchemaRef {
        let mut fields = vec![
            Field::new(TABLE_ID_COLUMN, DataType::UInt32, false),
            Field::new(TSID_COLUMN, DataType::UInt64, false),
            crate::sst::with_field_id(
                Field::new(
                    &self.timestamp.name,
                    DataType::Timestamp(self.timestamp.unit.arrow(), None),
                    false,
                ),
                self.timestamp.column_id,
            ),
            Field::new(SEQUENCE, DataType::UInt64, false),
            Field::new(OPERATION, DataType::UInt8, false),
        ];
        fields.extend(self.fields.iter().map(|f| {
            crate::sst::with_field_id(Field::new(&f.name, DataType::Float64, true), f.column_id)
        }));
        Arc::new(Schema::new(fields))
    }
}

fn valid_rows(rows: usize, fields: usize) -> bool {
    (1..=MAX_ROWS).contains(&rows)
        && (1..=MAX_FIELDS).contains(&fields)
        && rows * (17 + 8 * fields) + fields * rows.div_ceil(8) <= BLOCK_BYTES
}

/// Immutable samples output directory, independent of the source SST identity.
pub(crate) fn samples_prefix(region_id: u64, generation: FileId) -> String {
    format!("{region_id}/samples/{generation}/")
}
