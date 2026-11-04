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

use std::sync::Arc;

use common_recordbatch::DfRecordBatch;
use common_recordbatch::filter::MetadataFilter;
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::LogicalPlan;
use datatypes::arrow::array::{
    ArrayRef, StringArray, TimestampMillisecondArray, UInt32Array, UInt64Array,
};
use datatypes::arrow::error::ArrowError;
use datatypes::arrow::record_batch::RecordBatchOptions;
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};

use crate::storage::ScanRequest;

/// A live region-level compaction cycle. Completed cycles are not retained.
#[derive(Debug, Clone)]
pub struct CompactionTaskEntry {
    /// UUID retained from acceptance until this cycle leaves the scheduler.
    pub task_id: String,
    /// Physical region whose scheduler owns this cycle.
    pub region_id: u64,
    /// Physical table ID derived from the region ID.
    pub table_id: u32,
    /// Reporting datanode ID (0 in standalone), not the remote executor ID.
    pub node_id: Option<u64>,
    /// Whether this cycle was triggered automatically or manually.
    pub trigger_type: String,
    /// Queued, running, or cancelling, aggregated across the cycle.
    pub state: String,
    /// Waiting, planning, executing, committing, or remote execution.
    pub phase: String,
    /// Acceptance time in milliseconds since the Unix epoch.
    pub submitted_at: i64,
    /// Monotonic elapsed time since acceptance, including waiting.
    pub elapsed_millis: u64,
    /// Distinct files retained for execution; unknown before plan acceptance.
    pub input_files: Option<u64>,
    /// Total size of retained inputs, not bytes processed.
    pub input_bytes: Option<u64>,
    /// Number of local atomic units; unavailable for remote execution.
    pub total_units: Option<u64>,
    /// Local units with a terminal result, including failures.
    pub completed_units: Option<u64>,
    /// JSON snapshot of request arguments, including requested parallelism.
    pub request_options: String,
    /// JSON snapshot of resolved settings and picker output, if available.
    pub effective_options: Option<String>,
}

impl CompactionTaskEntry {
    /// Returns the public inspection schema.
    pub fn schema() -> SchemaRef {
        use datatypes::prelude::ConcreteDataType as Ty;
        Arc::new(Schema::new(vec![
            ColumnSchema::new("task_id", Ty::string_datatype(), false),
            ColumnSchema::new("region_id", Ty::uint64_datatype(), false),
            ColumnSchema::new("table_id", Ty::uint32_datatype(), false),
            ColumnSchema::new("node_id", Ty::uint64_datatype(), true),
            ColumnSchema::new("trigger_type", Ty::string_datatype(), false),
            ColumnSchema::new("state", Ty::string_datatype(), false),
            ColumnSchema::new("phase", Ty::string_datatype(), false),
            ColumnSchema::new("submitted_at", Ty::timestamp_millisecond_datatype(), false),
            ColumnSchema::new("elapsed_millis", Ty::uint64_datatype(), false),
            ColumnSchema::new("input_files", Ty::uint64_datatype(), true),
            ColumnSchema::new("input_bytes", Ty::uint64_datatype(), true),
            ColumnSchema::new("total_units", Ty::uint64_datatype(), true),
            ColumnSchema::new("completed_units", Ty::uint64_datatype(), true),
            ColumnSchema::new("request_options", Ty::string_datatype(), false),
            ColumnSchema::new("effective_options", Ty::string_datatype(), true),
        ]))
    }

    /// Prunes using known values, leaving unsupported predicates to DataFusion.
    pub fn matches(&self, filters: &MetadataFilter) -> bool {
        filters.matches(|name| {
            Some(match name {
                "task_id" => ScalarValue::Utf8(Some(self.task_id.clone())),
                "region_id" => ScalarValue::UInt64(Some(self.region_id)),
                "table_id" => ScalarValue::UInt32(Some(self.table_id)),
                "node_id" => ScalarValue::UInt64(self.node_id),
                "trigger_type" => ScalarValue::Utf8(Some(self.trigger_type.clone())),
                "state" => ScalarValue::Utf8(Some(self.state.clone())),
                "phase" => ScalarValue::Utf8(Some(self.phase.clone())),
                "submitted_at" => ScalarValue::TimestampMillisecond(Some(self.submitted_at), None),
                "elapsed_millis" => ScalarValue::UInt64(Some(self.elapsed_millis)),
                "input_files" => ScalarValue::UInt64(self.input_files),
                "input_bytes" => ScalarValue::UInt64(self.input_bytes),
                "total_units" => ScalarValue::UInt64(self.total_units),
                "completed_units" => ScalarValue::UInt64(self.completed_units),
                "request_options" => ScalarValue::Utf8(Some(self.request_options.clone())),
                "effective_options" => ScalarValue::Utf8(self.effective_options.clone()),
                _ => return None,
            })
        })
    }

    /// Builds only projected columns, including a zero-column batch for COUNT(*).
    pub fn to_record_batch(
        entries: &[Self],
        projection: Option<&[usize]>,
    ) -> Result<DfRecordBatch, ArrowError> {
        let schema = Self::schema().arrow_schema().clone();
        let schema = match projection {
            Some(p) => Arc::new(schema.project(p)?),
            None => schema,
        };
        let columns: Vec<ArrayRef> = schema
            .fields()
            .iter()
            .map(|field| {
                let array: ArrayRef = match field.name().as_str() {
                    "task_id" => Arc::new(StringArray::from_iter_values(
                        entries.iter().map(|e| e.task_id.as_str()),
                    )),
                    "region_id" => Arc::new(UInt64Array::from_iter_values(
                        entries.iter().map(|e| e.region_id),
                    )),
                    "table_id" => Arc::new(UInt32Array::from_iter_values(
                        entries.iter().map(|e| e.table_id),
                    )),
                    "node_id" => {
                        Arc::new(UInt64Array::from_iter(entries.iter().map(|e| e.node_id)))
                    }
                    "trigger_type" => Arc::new(StringArray::from_iter_values(
                        entries.iter().map(|e| e.trigger_type.as_str()),
                    )),
                    "state" => Arc::new(StringArray::from_iter_values(
                        entries.iter().map(|e| e.state.as_str()),
                    )),
                    "phase" => Arc::new(StringArray::from_iter_values(
                        entries.iter().map(|e| e.phase.as_str()),
                    )),
                    "submitted_at" => Arc::new(TimestampMillisecondArray::from_iter_values(
                        entries.iter().map(|e| e.submitted_at),
                    )),
                    "elapsed_millis" => Arc::new(UInt64Array::from_iter_values(
                        entries.iter().map(|e| e.elapsed_millis),
                    )),
                    "input_files" => Arc::new(UInt64Array::from_iter(
                        entries.iter().map(|e| e.input_files),
                    )),
                    "input_bytes" => Arc::new(UInt64Array::from_iter(
                        entries.iter().map(|e| e.input_bytes),
                    )),
                    "total_units" => Arc::new(UInt64Array::from_iter(
                        entries.iter().map(|e| e.total_units),
                    )),
                    "completed_units" => Arc::new(UInt64Array::from_iter(
                        entries.iter().map(|e| e.completed_units),
                    )),
                    "request_options" => Arc::new(StringArray::from_iter_values(
                        entries.iter().map(|e| e.request_options.as_str()),
                    )),
                    "effective_options" => Arc::new(StringArray::from_iter(
                        entries.iter().map(|e| e.effective_options.as_deref()),
                    )),
                    _ => {
                        return Err(ArrowError::SchemaError(format!(
                            "Unknown compaction field: {}",
                            field.name()
                        )));
                    }
                };
                Ok(array)
            })
            .collect::<Result<Vec<_>, ArrowError>>()?;
        DfRecordBatch::try_new_with_options(
            schema,
            columns,
            &RecordBatchOptions::new().with_row_count(Some(entries.len())),
        )
    }

    /// Internal name, inaccessible through user table resolution.
    pub fn reserved_table_name_for_inspection() -> &'static str {
        "__inspect/__mito/__compaction_tasks"
    }

    /// Builds a remote scan with filtering before projection and limit.
    pub fn build_plan(request: ScanRequest) -> Result<LogicalPlan, DataFusionError> {
        crate::sst_entry::build_plan_helper(
            request,
            Self::reserved_table_name_for_inspection(),
            Self::schema(),
        )
    }
}

#[cfg(test)]
mod tests {
    use datafusion_expr::{col, lit};
    use datatypes::arrow::datatypes::DataType;

    use super::*;

    fn entry() -> CompactionTaskEntry {
        CompactionTaskEntry {
            task_id: "task-1".into(),
            region_id: 42,
            table_id: 1,
            node_id: None,
            trigger_type: "manual".into(),
            state: "queued".into(),
            phase: "waiting".into(),
            submitted_at: 123,
            elapsed_millis: 10,
            input_files: None,
            input_bytes: None,
            total_units: None,
            completed_units: None,
            request_options: "{}".into(),
            effective_options: None,
        }
    }

    #[test]
    fn test_compaction_task_schema_and_projection() {
        let entries = [entry()];
        let full = CompactionTaskEntry::to_record_batch(&entries, None).unwrap();
        let fields: Vec<_> = full
            .schema()
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type().clone(), f.is_nullable()))
            .collect();
        let expected = [
            ("task_id", DataType::Utf8, false),
            ("region_id", DataType::UInt64, false),
            ("table_id", DataType::UInt32, false),
            ("node_id", DataType::UInt64, true),
            ("trigger_type", DataType::Utf8, false),
            ("state", DataType::Utf8, false),
            ("phase", DataType::Utf8, false),
            (
                "submitted_at",
                DataType::Timestamp(datatypes::arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            ("elapsed_millis", DataType::UInt64, false),
            ("input_files", DataType::UInt64, true),
            ("input_bytes", DataType::UInt64, true),
            ("total_units", DataType::UInt64, true),
            ("completed_units", DataType::UInt64, true),
            ("request_options", DataType::Utf8, false),
            ("effective_options", DataType::Utf8, true),
        ]
        .map(|(name, ty, nullable)| (name.to_string(), ty, nullable));
        assert_eq!(fields, expected);
        for projection in [vec![], vec![5, 0, 5], vec![14, 3, 8]] {
            let actual = CompactionTaskEntry::to_record_batch(&entries, Some(&projection)).unwrap();
            assert_eq!(actual, full.project(&projection).unwrap());
            assert_eq!(actual.num_rows(), 1);
        }
        assert!(CompactionTaskEntry::to_record_batch(&entries, Some(&[99])).is_err());
    }

    #[test]
    fn test_compaction_task_predicates_and_plan() {
        let entry = entry();
        for (expr, expected) in [
            (col("task_id").eq(lit("task-1")), true),
            (col("table_id").eq(lit(2u32)), false),
            (col("state").eq(lit("running")), false),
            (col("node_id").is_null(), true),
            (
                col("state")
                    .eq(lit("running"))
                    .or(col("unknown").eq(lit(1))),
                true,
            ),
        ] {
            assert_eq!(entry.matches(&MetadataFilter::new(&[expr])), expected);
        }
        let plan = CompactionTaskEntry::build_plan(ScanRequest {
            projection: Some(vec![0]),
            filters: vec![col("state").eq(lit("running"))],
            limit: Some(1),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(plan.schema().fields().len(), 1);
        let plan = plan.display_indent().to_string();
        assert!(plan.contains("Filter:"), "{plan}");
        assert!(plan.contains("Limit:"), "{plan}");
    }
}
