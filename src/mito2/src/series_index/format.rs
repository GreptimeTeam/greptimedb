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

//! Storage-format boundaries. Encodings stay outside reconciliation and source scheduling.

use std::collections::{BTreeSet, HashSet};
use std::fmt::Debug;
use std::sync::Arc;

use api::v1::SemanticType;
use async_trait::async_trait;
use common_recordbatch::filter::SimpleFilterEvaluator;
use datatypes::arrow::datatypes::{Field, SchemaRef};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::prelude::DataType;
use futures::stream::BoxStream;
use object_store::ObjectStore;
use store_api::metadata::{RegionMetadata, RegionMetadataRef};
use store_api::storage::FileId;
use store_api::storage::consts::ReservedColumnId;

use crate::error::{InvalidRecordBatchSnafu, Result};
use crate::series_index::artifact::{ArtifactFormat, ArtifactMetadata, SamplesFileHandle};
use crate::series_index::catalog::SeriesIndexEntry;
use crate::series_index::{MetricSeriesId, SeriesIndexFileHandle};
use crate::sst::PARQUET_FIELD_ID_KEY;

/// Writers own partial outputs until finish succeeds; abort removes every partial object.
/// A failed finish must clean up its output before returning; implementations also own
/// cleanup on cancellation while a writer is live.
#[async_trait]
pub(crate) trait FormatWriter: Send {
    async fn write(&mut self, batch: &RecordBatch) -> Result<()>;
    async fn finish(self: Box<Self>) -> Result<ArtifactMetadata>;
    async fn abort(self: Box<Self>) -> Result<()>;
}

/// A read has independent traversal state and pins its immutable artifact.
#[derive(Debug, Clone, Default)]
pub(crate) struct SeriesReadRequest {
    pub(crate) projection: Vec<String>,
    pub(crate) table_id: Option<u32>,
    pub(crate) ids: Option<BTreeSet<MetricSeriesId>>,
    pub(crate) filters: Vec<SimpleFilterEvaluator>,
}

pub(crate) trait FormatReader: Send + Sync {
    fn schema(&self) -> SchemaRef;
    fn read(&self, request: SeriesReadRequest) -> Result<BoxStream<'static, Result<RecordBatch>>>;
}

/// Builds and opens immutable series-index artifacts.
#[async_trait]
pub(crate) trait SeriesIndexFormat: Debug + Send + Sync {
    fn format(&self) -> ArtifactFormat;
    async fn reader(
        &self,
        _store: ObjectStore,
        _handle: SeriesIndexFileHandle,
    ) -> Result<Box<dyn FormatReader>> {
        crate::error::UnsupportedOperationSnafu {
            err_msg: "This series format has no reader",
        }
        .fail()
    }
    async fn writer(
        &self,
        metadata: RegionMetadataRef,
        store: ObjectStore,
        entry: &SeriesIndexEntry,
    ) -> Result<Box<dyn FormatWriter>>;
}

/// Selection in the stored timestamp unit. No visibility filtering is implicit.
#[derive(Debug, Clone, Default)]
pub(crate) struct SamplesReadRequest {
    pub(crate) projection: Vec<String>,
    pub(crate) table_id: Option<u32>,
    pub(crate) ids: Option<BTreeSet<MetricSeriesId>>,
    pub(crate) start: Option<i64>,
    pub(crate) end: Option<i64>,
}

pub(crate) trait SamplesFormatReader: Send + Sync {
    fn schema(&self) -> SchemaRef;
    fn read(&self, request: SamplesReadRequest) -> Result<BoxStream<'static, Result<RecordBatch>>>;
}

/// Builds and opens immutable per-SST samples artifacts.
#[async_trait]
pub(crate) trait SamplesFormat: Debug + Send + Sync {
    fn format(&self) -> ArtifactFormat;
    async fn reader(
        &self,
        _store: ObjectStore,
        _handle: SamplesFileHandle,
    ) -> Result<Box<dyn SamplesFormatReader>> {
        crate::error::UnsupportedOperationSnafu {
            err_msg: "This samples format has no reader",
        }
        .fail()
    }
    /// The caller supplies a unique prefix. Finish reports all objects owned below it.
    async fn writer(
        &self,
        metadata: RegionMetadataRef,
        store: ObjectStore,
        prefix: &str,
        source_file_id: FileId,
        generation: FileId,
    ) -> Result<Box<dyn FormatWriter>>;
}

/// Optional writers, captured once per reconciliation pass.
/// Without an index backend, builds use the existing Parquet writer.
#[derive(Debug, Clone, Default)]
pub(crate) struct SeriesFormats {
    pub(crate) index: Option<Arc<dyn SeriesIndexFormat>>,
    pub(crate) samples: Option<Arc<dyn SamplesFormat>>,
}

impl SeriesFormats {
    pub(crate) fn from_config(format: crate::config::SeriesIndexFormat) -> Self {
        match format {
            crate::config::SeriesIndexFormat::Legacy => Self::default(),
            crate::config::SeriesIndexFormat::Columnar => Self {
                index: Some(Arc::new(
                    crate::series_index::columnar::ColumnarSeriesFormat::default(),
                )),
                samples: Some(Arc::new(
                    crate::series_index::samples::ColumnarSamplesFormat::default(),
                )),
            },
        }
    }
}

/// Reads a column ID from an artifact schema field.
pub(crate) fn field_id(field: &Field) -> Result<u32> {
    field
        .metadata()
        .get(PARQUET_FIELD_ID_KEY)
        .and_then(|id| id.parse().ok())
        .ok_or_else(|| invalid(format!("missing column ID for {}", field.name())))
}

/// Checks whether samples can be read with the current region schema.
pub(crate) fn validate_sample_schema(schema: &SchemaRef, metadata: &RegionMetadata) -> Result<()> {
    let mut ids = HashSet::new();
    for field in schema
        .fields()
        .iter()
        .skip(2)
        .filter(|f| f.name() != "__sequence" && f.name() != "__op_type")
    {
        let id = field_id(field)?;
        let column = metadata
            .column_by_id(id)
            .ok_or_else(|| invalid("sample column absent from scan schema"))?;
        if column.semantic_type == SemanticType::Tag
            || column.column_schema.name != *field.name()
            || column.column_schema.data_type.as_arrow_type() != *field.data_type()
        {
            return Err(invalid("samples schema changed; rebuild series indexes"));
        }
        ids.insert(id);
    }
    if metadata
        .column_metadatas
        .iter()
        .any(|c| c.semantic_type != SemanticType::Tag && !ids.contains(&c.column_id))
    {
        return Err(invalid(
            "samples are missing scan fields; rebuild series indexes",
        ));
    }
    Ok(())
}

/// Checks whether a series index contains exactly the current non-reserved tags.
pub(crate) fn validate_series_schema(schema: &SchemaRef, metadata: &RegionMetadata) -> Result<()> {
    let mut ids = HashSet::new();
    for field in schema.fields().iter().skip(3) {
        let id = field_id(field)?;
        let column = metadata
            .column_by_id(id)
            .ok_or_else(|| invalid("series tag absent from scan schema"))?;
        if column.semantic_type != SemanticType::Tag || column.column_schema.name != *field.name() {
            return Err(invalid("series tag schema changed; rebuild series indexes"));
        }
        ids.insert(id);
    }
    if metadata
        .primary_key_columns()
        .any(|c| !ReservedColumnId::is_reserved(c.column_id) && !ids.contains(&c.column_id))
    {
        return Err(invalid(
            "series index is missing scan tags; rebuild series indexes",
        ));
    }
    Ok(())
}

fn invalid(reason: impl Into<String>) -> crate::error::Error {
    InvalidRecordBatchSnafu {
        reason: reason.into(),
    }
    .build()
}

#[cfg(test)]
mod tests {
    use datatypes::arrow::datatypes::{DataType, Schema};
    use datatypes::prelude::DataType as _;
    use store_api::codec::PrimaryKeyEncoding;

    use super::*;
    use crate::test_util::sst_util::sst_region_metadata_with_encoding;

    #[test]
    fn schema_compatibility_requires_current_columns() {
        let metadata = sst_region_metadata_with_encoding(PrimaryKeyEncoding::Sparse);
        for tags in [false, true] {
            let mut fields = vec![
                Field::new("__table_id", DataType::UInt32, false),
                Field::new("__tsid", DataType::UInt64, false),
            ];
            if tags {
                fields.push(Field::new("__row_count", DataType::UInt64, false));
            }
            fields.extend(
                metadata
                    .column_metadatas
                    .iter()
                    .filter(|c| {
                        (c.semantic_type == SemanticType::Tag) == tags
                            && !ReservedColumnId::is_reserved(c.column_id)
                    })
                    .map(|c| {
                        crate::sst::with_field_id(
                            Field::new(
                                &c.column_schema.name,
                                c.column_schema.data_type.as_arrow_type(),
                                true,
                            ),
                            c.column_id,
                        )
                    }),
            );
            let validate = |fields: Vec<Field>| {
                let schema = Arc::new(Schema::new(fields));
                if tags {
                    validate_series_schema(&schema, &metadata)
                } else {
                    validate_sample_schema(&schema, &metadata)
                }
            };
            validate(fields.clone()).unwrap();
            let index = if tags { 3 } else { 2 };
            let mut missing = fields.clone();
            missing.remove(index);
            assert!(validate(missing).is_err());
            let mut renamed = fields.clone();
            renamed[index] = renamed[index].clone().with_name("renamed");
            assert!(validate(renamed).is_err());
            let mut replaced = fields.clone();
            replaced[index] = crate::sst::with_field_id(replaced[index].clone(), 100);
            assert!(validate(replaced).is_err());
            if !tags {
                let mut changed_type = fields.clone();
                changed_type[index] = changed_type[index]
                    .clone()
                    .with_data_type(DataType::Float64);
                assert!(validate(changed_type).is_err());
            }
            let mut reordered = fields.clone();
            reordered[index..].reverse();
            validate(reordered).unwrap();
        }
    }
}
