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
use datafusion_common::DataFusionError;
use datafusion_expr::{LogicalPlan, LogicalPlanBuilder, LogicalTableSource};
use datatypes::arrow::array::{ArrayRef, BooleanArray, UInt32Array, UInt64Array};
use datatypes::arrow::error::ArrowError;
use datatypes::arrow_array::StringArray;
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use serde::{Deserialize, Serialize};

use crate::storage::{RegionId, RegionNumber, RegionSeq, ScanRequest, TableId};

/// Runtime and manifest information of a region for inspection.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegionInfoEntry {
    /// The region id.
    pub region_id: RegionId,
    /// The table id this region belongs to.
    pub table_id: TableId,
    /// The region number inside the table.
    pub region_number: RegionNumber,
    /// The region sequence inside the group.
    pub region_sequence: RegionSeq,
    /// The full runtime role/state label.
    pub state: String,
    /// The coarse region role.
    pub role: String,
    /// Whether the region accepts writes.
    pub writable: bool,
    /// The committed sequence of the region.
    pub sequence: u64,
    /// The manifest version of the region.
    pub manifest_version: u64,
    /// Human-readable compaction time window.
    pub compaction_time_window: Option<String>,
    /// Region options encoded as JSON.
    pub region_options: String,
    /// SST format used by the region.
    pub sst_format: String,
    /// Datanode id that reports the row.
    pub node_id: Option<u64>,
}

impl RegionInfoEntry {
    /// Returns the schema of the region info entry.
    pub fn schema() -> SchemaRef {
        use datatypes::prelude::ConcreteDataType as Ty;
        Arc::new(Schema::new(vec![
            ColumnSchema::new("region_id", Ty::uint64_datatype(), false),
            ColumnSchema::new("table_id", Ty::uint32_datatype(), false),
            ColumnSchema::new("region_number", Ty::uint32_datatype(), false),
            ColumnSchema::new("region_sequence", Ty::uint32_datatype(), false),
            ColumnSchema::new("state", Ty::string_datatype(), false),
            ColumnSchema::new("role", Ty::string_datatype(), false),
            ColumnSchema::new("writable", Ty::boolean_datatype(), false),
            ColumnSchema::new("sequence", Ty::uint64_datatype(), false),
            ColumnSchema::new("manifest_version", Ty::uint64_datatype(), false),
            ColumnSchema::new("compaction_time_window", Ty::string_datatype(), true),
            ColumnSchema::new("region_options", Ty::string_datatype(), false),
            ColumnSchema::new("sst_format", Ty::string_datatype(), false),
            ColumnSchema::new("node_id", Ty::uint64_datatype(), true),
        ]))
    }

    /// Converts a list of region info entries to a record batch.
    pub fn to_record_batch(entries: &[Self]) -> Result<DfRecordBatch, ArrowError> {
        let schema = Self::schema();
        let region_ids = entries.iter().map(|e| e.region_id.as_u64());
        let table_ids = entries.iter().map(|e| e.table_id);
        let region_numbers = entries.iter().map(|e| e.region_number);
        let region_sequences = entries.iter().map(|e| e.region_sequence);
        let states = entries.iter().map(|e| e.state.as_str());
        let roles = entries.iter().map(|e| e.role.as_str());
        let writable = entries.iter().map(|e| Some(e.writable));
        let sequences = entries.iter().map(|e| e.sequence);
        let manifest_versions = entries.iter().map(|e| e.manifest_version);
        let compaction_time_windows = entries.iter().map(|e| e.compaction_time_window.as_ref());
        let region_options = entries.iter().map(|e| e.region_options.as_str());
        let sst_formats = entries.iter().map(|e| e.sst_format.as_str());
        let node_ids = entries.iter().map(|e| e.node_id);

        let columns: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from_iter_values(region_ids)),
            Arc::new(UInt32Array::from_iter_values(table_ids)),
            Arc::new(UInt32Array::from_iter_values(region_numbers)),
            Arc::new(UInt32Array::from_iter_values(region_sequences)),
            Arc::new(StringArray::from_iter_values(states)),
            Arc::new(StringArray::from_iter_values(roles)),
            Arc::new(BooleanArray::from_iter(writable)),
            Arc::new(UInt64Array::from_iter_values(sequences)),
            Arc::new(UInt64Array::from_iter_values(manifest_versions)),
            Arc::new(StringArray::from_iter(compaction_time_windows)),
            Arc::new(StringArray::from_iter_values(region_options)),
            Arc::new(StringArray::from_iter_values(sst_formats)),
            Arc::new(UInt64Array::from_iter(node_ids)),
        ];

        DfRecordBatch::try_new(schema.arrow_schema().clone(), columns)
    }

    /// Reserved internal inspect table name for region info.
    pub fn reserved_table_name_for_inspection() -> &'static str {
        "__inspect/__mito/__region_info"
    }

    /// Builds a logical plan for scanning region info entries.
    pub fn build_plan(scan_request: ScanRequest) -> Result<LogicalPlan, DataFusionError> {
        let table_source = LogicalTableSource::new(Self::schema().arrow_schema().clone());

        let projection = scan_request.projection_input.map(|input| input.projection);
        let mut builder = LogicalPlanBuilder::scan(
            Self::reserved_table_name_for_inspection(),
            Arc::new(table_source),
            projection,
        )?;

        for filter in scan_request.filters {
            builder = builder.filter(filter)?;
        }

        if let Some(limit) = scan_request.limit {
            builder = builder.limit(0, Some(limit))?;
        }

        builder.build()
    }
}

#[cfg(test)]
mod tests {
    use datafusion_common::TableReference;
    use datafusion_expr::{LogicalPlan, Operator, binary_expr, col, lit};
    use datatypes::arrow::array::{Array, BooleanArray, UInt32Array, UInt64Array};
    use datatypes::arrow_array::StringArray;

    use super::*;
    use crate::storage::{RegionId, ScanRequest};

    #[test]
    fn test_region_info_schema() {
        let schema = RegionInfoEntry::schema();
        let columns = schema.column_schemas();

        let names = columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>();
        assert_eq!(
            names,
            vec![
                "region_id",
                "table_id",
                "region_number",
                "region_sequence",
                "state",
                "role",
                "writable",
                "sequence",
                "manifest_version",
                "compaction_time_window",
                "region_options",
                "sst_format",
                "node_id",
            ]
        );
        assert!(!columns[0].is_nullable());
        assert!(!columns[8].is_nullable());
        assert!(columns[9].is_nullable());
        assert!(columns[12].is_nullable());
    }

    #[test]
    fn test_region_info_to_record_batch() {
        let region_id1 = RegionId::with_group_and_seq(10, 1, 20);
        let region_id2 = RegionId::with_group_and_seq(11, 0, 21);
        let entries = vec![
            RegionInfoEntry {
                region_id: region_id1,
                table_id: region_id1.table_id(),
                region_number: region_id1.region_number(),
                region_sequence: region_id1.region_sequence(),
                state: "Leader(Writable)".to_string(),
                role: "Leader".to_string(),
                writable: true,
                sequence: 42,
                manifest_version: 7,
                compaction_time_window: Some("1h".to_string()),
                region_options: "{\"sst_format\":\"flat\"}".to_string(),
                sst_format: "flat".to_string(),
                node_id: Some(3),
            },
            RegionInfoEntry {
                region_id: region_id2,
                table_id: region_id2.table_id(),
                region_number: region_id2.region_number(),
                region_sequence: region_id2.region_sequence(),
                state: "Follower".to_string(),
                role: "Follower".to_string(),
                writable: false,
                sequence: 9,
                manifest_version: 2,
                compaction_time_window: None,
                region_options: "{}".to_string(),
                sst_format: "primary_key".to_string(),
                node_id: None,
            },
        ];

        let batch = RegionInfoEntry::to_record_batch(&entries).unwrap();
        assert_eq!(batch.num_rows(), 2);

        let region_ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(region_id1.as_u64(), region_ids.value(0));
        assert_eq!(region_id2.as_u64(), region_ids.value(1));

        let table_ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(10, table_ids.value(0));
        assert_eq!(11, table_ids.value(1));

        let states = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!("Leader(Writable)", states.value(0));
        assert_eq!("Follower", states.value(1));

        let writable = batch
            .column(6)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(writable.value(0));
        assert!(!writable.value(1));

        let compaction_time_windows = batch
            .column(9)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!("1h", compaction_time_windows.value(0));
        assert!(compaction_time_windows.is_null(1));

        let node_ids = batch
            .column(12)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(3, node_ids.value(0));
        assert!(node_ids.is_null(1));
    }

    #[test]
    fn test_region_info_build_plan() {
        let projection_input = Some(vec![0, 4, 6, 9].into());
        let request = ScanRequest {
            projection_input,
            filters: vec![binary_expr(col("writable"), Operator::Eq, lit(true))],
            limit: Some(10),
            ..Default::default()
        };

        let plan = RegionInfoEntry::build_plan(request).unwrap();
        let (scan, has_filter, has_limit) = extract_scan(&plan);
        assert!(has_filter);
        assert!(has_limit);
        assert_eq!(
            scan.table_name,
            TableReference::bare(RegionInfoEntry::reserved_table_name_for_inspection())
        );
        assert_eq!(scan.projection, Some(vec![0, 4, 6, 9]));

        let fields = scan.projected_schema.fields();
        assert_eq!(fields.len(), 4);
        assert_eq!(fields[0].name(), "region_id");
        assert_eq!(fields[1].name(), "state");
        assert_eq!(fields[2].name(), "writable");
        assert_eq!(fields[3].name(), "compaction_time_window");
    }

    fn extract_scan(plan: &LogicalPlan) -> (&datafusion_expr::logical_plan::TableScan, bool, bool) {
        use datafusion_expr::logical_plan::Limit;

        match plan {
            LogicalPlan::Filter(f) => {
                let (scan, _, has_limit) = extract_scan(&f.input);
                (scan, true, has_limit)
            }
            LogicalPlan::Limit(Limit { input, .. }) => {
                let (scan, has_filter, _) = extract_scan(input);
                (scan, has_filter, true)
            }
            LogicalPlan::TableScan(scan) => (scan, false, false),
            other => panic!("unexpected plan: {other:?}"),
        }
    }
}
