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
use common_time::Timestamp;
use common_time::timestamp::TimeUnit;
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::LogicalPlan;
use datatypes::arrow::array::{
    ArrayRef, StringArray, TimestampMillisecondArray, TimestampNanosecondArray, UInt32Array,
    UInt64Array,
};
use datatypes::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datatypes::arrow::error::ArrowError;
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};

use crate::inspect::{self, ProjectedBatchBuilder};
use crate::storage::{RegionId, ScanRequest, TableId};

// Compaction task column names.
pub const TASK_ID: &str = "task_id";
pub const REGION_ID: &str = "region_id";
pub const TABLE_ID: &str = "table_id";
pub const NODE_ID: &str = "node_id";
pub const TRIGGER_TYPE: &str = "trigger_type";
pub const STATE: &str = "state";
pub const PHASE: &str = "phase";
pub const SUBMITTED_AT: &str = "submitted_at";
pub const ELAPSED_MILLIS: &str = "elapsed_millis";
pub const INPUT_FILES: &str = "input_files";
pub const INPUT_BYTES: &str = "input_bytes";
pub const TOTAL_UNITS: &str = "total_units";
pub const COMPLETED_UNITS: &str = "completed_units";
pub const REQUEST_OPTIONS: &str = "request_options";
pub const EFFECTIVE_OPTIONS: &str = "effective_options";
pub const INPUT_TIME_START: &str = "input_time_start";
pub const INPUT_TIME_END: &str = "input_time_end";
pub const OUTPUT_TIME_START: &str = "output_time_start";
pub const OUTPUT_TIME_END: &str = "output_time_end";

/// Queued compaction cycle.
pub const STATE_QUEUED: &str = "queued";
/// Running compaction cycle.
pub const STATE_RUNNING: &str = "running";
/// Compaction cycle with cancellation requested.
pub const STATE_CANCELLING: &str = "cancelling";
/// Cycle waiting for execution.
pub const PHASE_WAITING: &str = "waiting";
/// Cycle selecting a compaction plan.
pub const PHASE_PLANNING: &str = "planning";
/// Cycle executing local compaction units.
pub const PHASE_EXECUTING: &str = "executing";
/// Cycle committing compaction output.
pub const PHASE_COMMITTING: &str = "committing";
/// Cycle executing through a remote compactor.
pub const PHASE_REMOTE: &str = "remote";
/// Compaction requested manually.
pub const TRIGGER_TYPE_MANUAL: &str = "manual";
/// Compaction triggered automatically.
pub const TRIGGER_TYPE_AUTOMATIC: &str = "automatic";

/// A live region-level compaction cycle. Completed cycles are not retained.
///
/// String fields are shared so cloning a snapshot does not copy parameter payloads.
/// Time ranges are envelopes and may contain holes. SQL endpoints are NULL when
/// unknown, unbounded, or outside the representable nanosecond timestamp range.
#[derive(Debug, Clone)]
pub struct CompactionTaskEntry {
    /// UUID retained from acceptance until this cycle leaves the scheduler.
    pub task_id: Arc<str>,
    /// Physical region whose scheduler owns this cycle.
    pub region_id: RegionId,
    /// Physical table ID derived from the region ID.
    pub table_id: TableId,
    /// Reporting datanode ID (0 in standalone), not the remote executor ID.
    pub node_id: Option<u64>,
    /// Whether this cycle was triggered automatically or manually.
    pub trigger_type: Arc<str>,
    /// Queued, running, or cancelling, aggregated across the cycle.
    pub state: Arc<str>,
    /// Phase label: `waiting`, `planning`, `executing`, `committing`, or `remote`.
    pub phase: Arc<str>,
    /// Acceptance time in milliseconds since the Unix epoch.
    pub submitted_at: i64,
    /// Monotonic elapsed time since acceptance, including waiting.
    pub elapsed_millis: u64,
    /// Distinct plan input files, including expired SSTs; unknown before plan acceptance.
    pub input_files: Option<u64>,
    /// Total size of plan inputs, including expired SSTs; not bytes processed.
    pub input_bytes: Option<u64>,
    /// Number of local atomic units; unavailable for remote execution.
    pub total_units: Option<u64>,
    /// Local units with a terminal result, including failures.
    pub completed_units: Option<u64>,
    /// JSON snapshot of request arguments, including requested parallelism.
    pub request_options: Arc<str>,
    /// JSON snapshot of resolved settings and picker output, if available.
    pub effective_options: Option<Arc<str>>,
    /// Inclusive earliest timestamp in plan input SSTs, including expired SSTs.
    pub input_time_start: Option<Timestamp>,
    /// Inclusive latest timestamp in plan input SSTs, including expired SSTs.
    pub input_time_end: Option<Timestamp>,
    /// Inclusive start of the planned output envelope, not actual output SST bounds.
    pub output_time_start: Option<Timestamp>,
    /// Exclusive end of the planned output envelope, not actual output SST bounds.
    pub output_time_end: Option<Timestamp>,
}

impl CompactionTaskEntry {
    /// Returns the public inspection schema.
    pub fn schema() -> SchemaRef {
        use datatypes::prelude::ConcreteDataType as Ty;
        Arc::new(Schema::new(vec![
            ColumnSchema::new(TASK_ID, Ty::string_datatype(), false),
            ColumnSchema::new(REGION_ID, Ty::uint64_datatype(), false),
            ColumnSchema::new(TABLE_ID, Ty::uint32_datatype(), false),
            ColumnSchema::new(NODE_ID, Ty::uint64_datatype(), true),
            ColumnSchema::new(TRIGGER_TYPE, Ty::string_datatype(), false),
            ColumnSchema::new(STATE, Ty::string_datatype(), false),
            ColumnSchema::new(PHASE, Ty::string_datatype(), false),
            ColumnSchema::new(SUBMITTED_AT, Ty::timestamp_millisecond_datatype(), false),
            ColumnSchema::new(ELAPSED_MILLIS, Ty::uint64_datatype(), false),
            ColumnSchema::new(INPUT_FILES, Ty::uint64_datatype(), true),
            ColumnSchema::new(INPUT_BYTES, Ty::uint64_datatype(), true),
            ColumnSchema::new(TOTAL_UNITS, Ty::uint64_datatype(), true),
            ColumnSchema::new(COMPLETED_UNITS, Ty::uint64_datatype(), true),
            ColumnSchema::new(REQUEST_OPTIONS, Ty::string_datatype(), false),
            ColumnSchema::new(EFFECTIVE_OPTIONS, Ty::string_datatype(), true),
            ColumnSchema::new(INPUT_TIME_START, Ty::timestamp_nanosecond_datatype(), true),
            ColumnSchema::new(INPUT_TIME_END, Ty::timestamp_nanosecond_datatype(), true),
            ColumnSchema::new(OUTPUT_TIME_START, Ty::timestamp_nanosecond_datatype(), true),
            ColumnSchema::new(OUTPUT_TIME_END, Ty::timestamp_nanosecond_datatype(), true),
        ]))
    }

    /// Prunes using known values, leaving unsupported predicates to DataFusion.
    pub fn matches(&self, filters: &MetadataFilter) -> bool {
        filters.matches(|name| {
            Some(match name {
                TASK_ID => ScalarValue::Utf8(Some(self.task_id.to_string())),
                REGION_ID => ScalarValue::UInt64(Some(self.region_id.as_u64())),
                TABLE_ID => ScalarValue::UInt32(Some(self.table_id)),
                NODE_ID => ScalarValue::UInt64(self.node_id),
                TRIGGER_TYPE => ScalarValue::Utf8(Some(self.trigger_type.to_string())),
                STATE => ScalarValue::Utf8(Some(self.state.to_string())),
                PHASE => ScalarValue::Utf8(Some(self.phase.to_string())),
                SUBMITTED_AT => ScalarValue::TimestampMillisecond(Some(self.submitted_at), None),
                ELAPSED_MILLIS => ScalarValue::UInt64(Some(self.elapsed_millis)),
                INPUT_FILES => ScalarValue::UInt64(self.input_files),
                INPUT_BYTES => ScalarValue::UInt64(self.input_bytes),
                TOTAL_UNITS => ScalarValue::UInt64(self.total_units),
                COMPLETED_UNITS => ScalarValue::UInt64(self.completed_units),
                REQUEST_OPTIONS => ScalarValue::Utf8(Some(self.request_options.to_string())),
                EFFECTIVE_OPTIONS => {
                    ScalarValue::Utf8(self.effective_options.as_deref().map(str::to_string))
                }
                INPUT_TIME_START => {
                    ScalarValue::TimestampNanosecond(timestamp_nanos(self.input_time_start), None)
                }
                INPUT_TIME_END => {
                    ScalarValue::TimestampNanosecond(timestamp_nanos(self.input_time_end), None)
                }
                OUTPUT_TIME_START => {
                    ScalarValue::TimestampNanosecond(timestamp_nanos(self.output_time_start), None)
                }
                OUTPUT_TIME_END => {
                    ScalarValue::TimestampNanosecond(timestamp_nanos(self.output_time_end), None)
                }
                _ => return None,
            })
        })
    }

    /// Converts compaction task entries to a record batch.
    pub fn to_record_batch(entries: &[Self]) -> Result<DfRecordBatch, ArrowError> {
        Self::to_projected_record_batch(entries, None)
    }

    /// Builds only the requested columns, including empty and repeated projections.
    fn to_projected_record_batch(
        entries: &[Self],
        projection: Option<&[usize]>,
    ) -> Result<DfRecordBatch, ArrowError> {
        let schema = Self::schema().arrow_schema().clone();
        let schema = match projection {
            Some(projection) => Arc::new(schema.project(projection)?),
            None => schema,
        };
        CompactionTaskBatchBuilder::new(schema).build(entries)
    }

    /// Internal name, inaccessible through user table resolution.
    pub fn reserved_table_name_for_inspection() -> &'static str {
        "__inspect/__mito/__compaction_tasks"
    }

    /// Builds a remote scan with filtering before projection and limit.
    pub fn build_plan(request: ScanRequest) -> Result<LogicalPlan, DataFusionError> {
        inspect::build_plan(
            request,
            Self::reserved_table_name_for_inspection(),
            Self::schema(),
        )
    }
}

/// Converts compaction task batches using a schema prepared once per scan.
/// Repeated projected columns share the same Arrow array.
pub struct CompactionTaskBatchBuilder {
    projected: ProjectedBatchBuilder,
}

impl CompactionTaskBatchBuilder {
    /// Creates a builder for a projected compaction task schema.
    pub fn new(schema: ArrowSchemaRef) -> Self {
        Self {
            projected: ProjectedBatchBuilder::new(schema),
        }
    }

    /// Builds the projected columns, copying string contents only into Arrow buffers.
    pub fn build(&self, entries: &[CompactionTaskEntry]) -> Result<DfRecordBatch, ArrowError> {
        self.projected.build(entries.len(), |name| {
            let array: ArrayRef = match name {
                TASK_ID => Arc::new(StringArray::from_iter_values(
                    entries.iter().map(|e| e.task_id.as_ref()),
                )),
                REGION_ID => Arc::new(UInt64Array::from_iter_values(
                    entries.iter().map(|e| e.region_id.as_u64()),
                )),
                TABLE_ID => Arc::new(UInt32Array::from_iter_values(
                    entries.iter().map(|e| e.table_id),
                )),
                NODE_ID => Arc::new(UInt64Array::from_iter(entries.iter().map(|e| e.node_id))),
                TRIGGER_TYPE => Arc::new(StringArray::from_iter_values(
                    entries.iter().map(|e| e.trigger_type.as_ref()),
                )),
                STATE => Arc::new(StringArray::from_iter_values(
                    entries.iter().map(|e| e.state.as_ref()),
                )),
                PHASE => Arc::new(StringArray::from_iter_values(
                    entries.iter().map(|e| e.phase.as_ref()),
                )),
                SUBMITTED_AT => Arc::new(TimestampMillisecondArray::from_iter_values(
                    entries.iter().map(|e| e.submitted_at),
                )),
                ELAPSED_MILLIS => Arc::new(UInt64Array::from_iter_values(
                    entries.iter().map(|e| e.elapsed_millis),
                )),
                INPUT_FILES => Arc::new(UInt64Array::from_iter(
                    entries.iter().map(|e| e.input_files),
                )),
                INPUT_BYTES => Arc::new(UInt64Array::from_iter(
                    entries.iter().map(|e| e.input_bytes),
                )),
                TOTAL_UNITS => Arc::new(UInt64Array::from_iter(
                    entries.iter().map(|e| e.total_units),
                )),
                COMPLETED_UNITS => Arc::new(UInt64Array::from_iter(
                    entries.iter().map(|e| e.completed_units),
                )),
                REQUEST_OPTIONS => Arc::new(StringArray::from_iter_values(
                    entries.iter().map(|e| e.request_options.as_ref()),
                )),
                EFFECTIVE_OPTIONS => Arc::new(StringArray::from_iter(
                    entries.iter().map(|e| e.effective_options.as_deref()),
                )),
                INPUT_TIME_START => Arc::new(TimestampNanosecondArray::from_iter(
                    entries.iter().map(|e| timestamp_nanos(e.input_time_start)),
                )),
                INPUT_TIME_END => Arc::new(TimestampNanosecondArray::from_iter(
                    entries.iter().map(|e| timestamp_nanos(e.input_time_end)),
                )),
                OUTPUT_TIME_START => Arc::new(TimestampNanosecondArray::from_iter(
                    entries.iter().map(|e| timestamp_nanos(e.output_time_start)),
                )),
                OUTPUT_TIME_END => Arc::new(TimestampNanosecondArray::from_iter(
                    entries.iter().map(|e| timestamp_nanos(e.output_time_end)),
                )),
                _ => {
                    return Err(ArrowError::SchemaError(format!(
                        "Unknown compaction task column: {name}"
                    )));
                }
            };
            Ok(array)
        })
    }
}

/// Converts endpoints identically for predicate pruning and Arrow output.
fn timestamp_nanos(timestamp: Option<Timestamp>) -> Option<i64> {
    timestamp
        .and_then(|ts| ts.convert_to(TimeUnit::Nanosecond))
        .map(|ts| ts.value())
}

#[cfg(test)]
mod tests {
    use datafusion_expr::{col, lit};
    use datatypes::arrow::datatypes::DataType;

    use super::*;

    fn entry() -> CompactionTaskEntry {
        CompactionTaskEntry {
            task_id: "task-1".into(),
            region_id: RegionId::new(1, 42),
            table_id: 1,
            node_id: Some(7),
            trigger_type: "manual".into(),
            state: "queued".into(),
            phase: "waiting".into(),
            submitted_at: 123,
            elapsed_millis: 10,
            input_files: Some(3),
            input_bytes: Some(4096),
            total_units: Some(5),
            completed_units: Some(2),
            request_options: "{\"request\":1}".into(),
            effective_options: Some("{\"effective\":2}".into()),
            input_time_start: Some(Timestamp::new_nanosecond(11)),
            input_time_end: Some(Timestamp::new_nanosecond(22)),
            output_time_start: Some(Timestamp::new_nanosecond(33)),
            output_time_end: Some(Timestamp::new_nanosecond(44)),
        }
    }

    #[test]
    fn test_compaction_task_clone_shares_strings() {
        let entry = entry();
        let cloned = entry.clone();
        for (original, cloned) in [
            (&entry.task_id, &cloned.task_id),
            (&entry.trigger_type, &cloned.trigger_type),
            (&entry.state, &cloned.state),
            (&entry.phase, &cloned.phase),
            (&entry.request_options, &cloned.request_options),
            (
                entry.effective_options.as_ref().unwrap(),
                cloned.effective_options.as_ref().unwrap(),
            ),
        ] {
            assert!(Arc::ptr_eq(original, cloned));
        }
        assert_eq!(
            CompactionTaskEntry::to_record_batch(&[entry]).unwrap(),
            CompactionTaskEntry::to_record_batch(&[cloned]).unwrap()
        );
    }

    #[test]
    fn test_compaction_task_column_values_and_predicates() {
        let entry = entry();
        let batch = CompactionTaskEntry::to_record_batch(std::slice::from_ref(&entry)).unwrap();
        // Independent SQL contract values distinguish every same-typed field.
        let expected = [
            ("task_id", ScalarValue::Utf8(Some("task-1".into()))),
            ("region_id", ScalarValue::UInt64(Some(4_294_967_338))),
            ("table_id", ScalarValue::UInt32(Some(1))),
            ("node_id", ScalarValue::UInt64(Some(7))),
            ("trigger_type", ScalarValue::Utf8(Some("manual".into()))),
            ("state", ScalarValue::Utf8(Some("queued".into()))),
            ("phase", ScalarValue::Utf8(Some("waiting".into()))),
            (
                "submitted_at",
                ScalarValue::TimestampMillisecond(Some(123), None),
            ),
            ("elapsed_millis", ScalarValue::UInt64(Some(10))),
            ("input_files", ScalarValue::UInt64(Some(3))),
            ("input_bytes", ScalarValue::UInt64(Some(4096))),
            ("total_units", ScalarValue::UInt64(Some(5))),
            ("completed_units", ScalarValue::UInt64(Some(2))),
            (
                "request_options",
                ScalarValue::Utf8(Some("{\"request\":1}".into())),
            ),
            (
                "effective_options",
                ScalarValue::Utf8(Some("{\"effective\":2}".into())),
            ),
            (
                "input_time_start",
                ScalarValue::TimestampNanosecond(Some(11), None),
            ),
            (
                "input_time_end",
                ScalarValue::TimestampNanosecond(Some(22), None),
            ),
            (
                "output_time_start",
                ScalarValue::TimestampNanosecond(Some(33), None),
            ),
            (
                "output_time_end",
                ScalarValue::TimestampNanosecond(Some(44), None),
            ),
        ];
        assert_eq!(batch.num_columns(), expected.len());
        for (index, (name, value)) in expected.into_iter().enumerate() {
            assert_eq!(batch.schema().field(index).name(), name);
            assert_eq!(
                ScalarValue::try_from_array(batch.column(index), 0).unwrap(),
                value,
                "{name}"
            );
            assert!(
                entry.matches(&MetadataFilter::new(&[col(name).eq(lit(value.clone()))])),
                "{name}"
            );
            let different = match &value {
                ScalarValue::Utf8(Some(value)) => ScalarValue::Utf8(Some(format!("{value}-other"))),
                ScalarValue::UInt32(Some(value)) => ScalarValue::UInt32(Some(value + 1)),
                ScalarValue::UInt64(Some(value)) => ScalarValue::UInt64(Some(value + 1)),
                ScalarValue::TimestampMillisecond(Some(value), timezone) => {
                    ScalarValue::TimestampMillisecond(Some(value + 1), timezone.clone())
                }
                ScalarValue::TimestampNanosecond(Some(value), timezone) => {
                    ScalarValue::TimestampNanosecond(Some(value + 1), timezone.clone())
                }
                unexpected => panic!("unexpected contract value for {name}: {unexpected:?}"),
            };
            assert!(
                !entry.matches(&MetadataFilter::new(&[col(name).eq(lit(different))])),
                "{name}"
            );
            assert!(
                !entry.matches(&MetadataFilter::new(&[col(name).not_eq(lit(value))])),
                "{name}"
            );
        }
    }

    #[test]
    fn test_compaction_task_schema_and_projection() {
        let entries = [entry()];
        let full = CompactionTaskEntry::to_record_batch(&entries).unwrap();
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
            (
                "input_time_start",
                DataType::Timestamp(datatypes::arrow::datatypes::TimeUnit::Nanosecond, None),
                true,
            ),
            (
                "input_time_end",
                DataType::Timestamp(datatypes::arrow::datatypes::TimeUnit::Nanosecond, None),
                true,
            ),
            (
                "output_time_start",
                DataType::Timestamp(datatypes::arrow::datatypes::TimeUnit::Nanosecond, None),
                true,
            ),
            (
                "output_time_end",
                DataType::Timestamp(datatypes::arrow::datatypes::TimeUnit::Nanosecond, None),
                true,
            ),
        ]
        .map(|(name, ty, nullable)| (name.to_string(), ty, nullable));
        assert_eq!(fields, expected);
        for projection in [
            vec![],
            vec![5, 0, 5],
            vec![14, 3, 8],
            vec![18, 15, 17, 16, 15],
        ] {
            let actual =
                CompactionTaskEntry::to_projected_record_batch(&entries, Some(&projection))
                    .unwrap();
            assert_eq!(actual, full.project(&projection).unwrap());
            assert_eq!(actual.num_rows(), 1);
        }
        assert!(CompactionTaskEntry::to_projected_record_batch(&entries, Some(&[99])).is_err());
        let projected_schema = Arc::new(full.schema().project(&[0, 9, 0, 18, 9]).unwrap());
        let builder = CompactionTaskBatchBuilder::new(projected_schema.clone());
        for rows in [entries.as_slice(), &entries[..0]] {
            let projected = builder.build(rows).unwrap();
            assert_eq!(
                projected,
                CompactionTaskEntry::to_record_batch(rows)
                    .unwrap()
                    .project(&[0, 9, 0, 18, 9])
                    .unwrap()
            );
            assert!(Arc::ptr_eq(&projected.schema(), &projected_schema));
            assert!(Arc::ptr_eq(projected.column(0), projected.column(2)));
            assert!(Arc::ptr_eq(projected.column(1), projected.column(4)));
        }
    }

    #[test]
    fn test_compaction_task_time_ranges_precision_and_nulls() {
        let precise = CompactionTaskEntry {
            input_time_start: Some(Timestamp::new(-2, TimeUnit::Second)),
            input_time_end: Some(Timestamp::new(3, TimeUnit::Millisecond)),
            output_time_start: Some(Timestamp::new(-4, TimeUnit::Microsecond)),
            output_time_end: Some(Timestamp::new(5, TimeUnit::Nanosecond)),
            ..entry()
        };
        let overflow = CompactionTaskEntry {
            input_time_start: Some(Timestamp::new(i64::MIN, TimeUnit::Second)),
            input_time_end: Some(Timestamp::new(i64::MAX, TimeUnit::Second)),
            output_time_start: Some(Timestamp::new(i64::MIN, TimeUnit::Millisecond)),
            output_time_end: Some(Timestamp::new(i64::MAX, TimeUnit::Microsecond)),
            ..entry()
        };
        let unknown = CompactionTaskEntry {
            input_time_start: None,
            input_time_end: None,
            output_time_start: None,
            output_time_end: None,
            ..entry()
        };
        let entries = [precise, overflow, unknown];
        let batch =
            CompactionTaskEntry::to_projected_record_batch(&entries, Some(&[15, 16, 17, 18]))
                .unwrap();
        for (index, (name, nanos)) in [
            ("input_time_start", -2_000_000_000),
            ("input_time_end", 3_000_000),
            ("output_time_start", -4_000),
            ("output_time_end", 5),
        ]
        .into_iter()
        .enumerate()
        {
            let values = batch
                .column(index)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            assert_eq!(
                values,
                &TimestampNanosecondArray::from(vec![Some(nanos), None, None])
            );
            let equal = col(name).eq(lit(ScalarValue::TimestampNanosecond(Some(nanos), None)));
            let different =
                col(name).eq(lit(ScalarValue::TimestampNanosecond(Some(nanos + 1), None)));
            assert!(entries[0].matches(&MetadataFilter::new(std::slice::from_ref(&equal))));
            assert!(!entries[0].matches(&MetadataFilter::new(&[different])));
            assert!(!entries[0].matches(&MetadataFilter::new(&[col(name).is_null()])));
            for entry in &entries[1..] {
                assert!(entry.matches(&MetadataFilter::new(&[col(name).is_null()])));
                assert!(!entry.matches(&MetadataFilter::new(&[col(name).is_not_null()])));
                assert!(!entry.matches(&MetadataFilter::new(std::slice::from_ref(&equal))));
            }
        }
        let full = CompactionTaskEntry::to_record_batch(&entries).unwrap();
        let projection = [18, 16, 15, 18, 17];
        assert_eq!(
            CompactionTaskEntry::to_projected_record_batch(&entries, Some(&projection)).unwrap(),
            full.project(&projection).unwrap()
        );
    }

    #[test]
    fn test_compaction_task_predicates_and_plan() {
        let entry = entry();
        for (expr, expected) in [
            (col("task_id").eq(lit("task-1")), true),
            (col("table_id").eq(lit(2u32)), false),
            (col("state").eq(lit("running")), false),
            (col("node_id").is_null(), false),
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
