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

//! Reader and writer interfaces for native series and samples files.

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

use api::v1::SemanticType;
use async_trait::async_trait;
use common_recordbatch::filter::SimpleFilterEvaluator;
use datatypes::arrow::datatypes::{Field, SchemaRef};
use datatypes::arrow::record_batch::RecordBatch;
use futures::stream::BoxStream;
use store_api::metadata::RegionMetadata;
use store_api::storage::consts::ReservedColumnId;

use crate::error::{InvalidRecordBatchSnafu, Result};
use crate::series_index::MetricSeriesId;
use crate::series_index::artifact::ArtifactMetadata;
use crate::series_index::identity::ExactSeriesKey;
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

/// Selection in the stored timestamp unit. No visibility filtering is implicit.
#[derive(Debug, Clone, Default)]
pub(crate) struct SamplesReadRequest {
    /// Native scans account transient read buffers against their query pool.
    pub(crate) memory_pool: Option<Arc<dyn datafusion::execution::memory_pool::MemoryPool>>,
    pub(crate) projection: Vec<String>,
    pub(crate) table_id: Option<u32>,
    /// Exact selection. A block whose key is absent here is never read, even when its
    /// coarse `(table_id, tsid)` is present. The reader derives its coarse pruning set
    /// from these keys, so the selection cannot disagree with itself.
    pub(crate) keys: Option<BTreeSet<ExactSeriesKey>>,
    pub(crate) start: Option<i64>,
    pub(crate) end: Option<i64>,
}

/// One samples output batch and the exact series that owns every row in it.
///
/// A sample block belongs to exactly one series, so the exact key travels with the batch
/// instead of being repeated once per row: consumers attribute the whole batch at once.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SamplesBatch {
    pub(crate) batch: RecordBatch,
    pub(crate) key: ExactSeriesKey,
}

/// Batches read as batches: the key is the only column the wrapped batch does not carry.
impl std::ops::Deref for SamplesBatch {
    type Target = RecordBatch;

    fn deref(&self) -> &RecordBatch {
        &self.batch
    }
}

pub(crate) trait SamplesFormatReader: Send + Sync {
    fn schema(&self) -> SchemaRef;
    fn read(&self, request: SamplesReadRequest)
    -> Result<BoxStream<'static, Result<SamplesBatch>>>;
}

/// Reads a column ID from an artifact schema field.
pub(crate) fn field_id(field: &Field) -> Result<u32> {
    field
        .metadata()
        .get(PARQUET_FIELD_ID_KEY)
        .and_then(|id| id.parse().ok())
        .ok_or_else(|| invalid(format!("missing column ID for {}", field.name())))
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
    fn series_schema_requires_current_tags() {
        let metadata = sst_region_metadata_with_encoding(PrimaryKeyEncoding::Sparse);
        let mut fields = vec![
            Field::new("__table_id", DataType::UInt32, false),
            Field::new("__tsid", DataType::UInt64, false),
            Field::new("__row_count", DataType::UInt64, false),
        ];
        fields.extend(
            metadata
                .column_metadatas
                .iter()
                .filter(|c| {
                    c.semantic_type == SemanticType::Tag
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
        let validate =
            |fields: Vec<Field>| validate_series_schema(&Arc::new(Schema::new(fields)), &metadata);
        validate(fields.clone()).unwrap();
        let index = 3;
        let mut missing = fields.clone();
        missing.remove(index);
        assert!(validate(missing).is_err());
        let mut renamed = fields.clone();
        renamed[index] = renamed[index].clone().with_name("renamed");
        assert!(validate(renamed).is_err());
        let mut replaced = fields.clone();
        replaced[index] = crate::sst::with_field_id(replaced[index].clone(), 100);
        assert!(validate(replaced).is_err());
        let mut reordered = fields.clone();
        reordered[index..].reverse();
        validate(reordered).unwrap();
    }
}
