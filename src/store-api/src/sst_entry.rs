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
use common_time::timestamp::TimeUnit;
use common_time::Timestamp;
use datafusion_common::DataFusionError;
use datafusion_expr::{LogicalPlan, LogicalPlanBuilder, LogicalTableSource};
use datatypes::arrow::array::{
    ArrayRef, TimestampMillisecondArray, TimestampNanosecondArray, UInt32Array, UInt64Array,
    UInt8Array,
};
use datatypes::arrow::error::ArrowError;
use datatypes::arrow_array::StringArray;
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use serde::{Deserialize, Serialize};

use crate::storage::{RegionGroup, RegionId, RegionNumber, RegionSeq, ScanRequest, TableId};

/// An entry describing a SST file known by the engine's manifest.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManifestSstEntry {
    /// The table directory this file belongs to.
    pub table_dir: String,
    /// The region id this file belongs to.
    pub region_id: RegionId,
    /// The table id this file belongs to.
    pub table_id: TableId,
    /// The region number this file belongs to.
    pub region_number: RegionNumber,
    /// The region group this file belongs to.
    pub region_group: RegionGroup,
    /// The region sequence this file belongs to.
    pub region_sequence: RegionSeq,
    /// Engine-specific file identifier (string form).
    pub file_id: String,
    /// SST level.
    pub level: u8,
    /// Full path of the SST file in object store.
    pub file_path: String,
    /// File size in bytes.
    pub file_size: u64,
    /// Full path of the index file in object store.
    pub index_file_path: Option<String>,
    /// File size of the index file in object store.
    pub index_file_size: Option<u64>,
    /// Number of rows in the SST.
    pub num_rows: u64,
    /// Number of row groups in the SST.
    pub num_row_groups: u64,
    /// Min timestamp.
    pub min_ts: Timestamp,
    /// Max timestamp.
    pub max_ts: Timestamp,
    /// The sequence number associated with this file.
    pub sequence: Option<u64>,
}

impl ManifestSstEntry {
    /// Returns the schema of the manifest sst entry.
    pub fn schema() -> SchemaRef {
        use datatypes::prelude::ConcreteDataType as Ty;
        Arc::new(Schema::new(vec![
            ColumnSchema::new("table_dir", Ty::string_datatype(), false),
            ColumnSchema::new("region_id", Ty::uint64_datatype(), false),
            ColumnSchema::new("table_id", Ty::uint32_datatype(), false),
            ColumnSchema::new("region_number", Ty::uint32_datatype(), false),
            ColumnSchema::new("region_group", Ty::uint8_datatype(), false),
            ColumnSchema::new("region_sequence", Ty::uint32_datatype(), false),
            ColumnSchema::new("file_id", Ty::string_datatype(), false),
            ColumnSchema::new("level", Ty::uint8_datatype(), false),
            ColumnSchema::new("file_path", Ty::string_datatype(), false),
            ColumnSchema::new("file_size", Ty::uint64_datatype(), false),
            ColumnSchema::new("index_file_path", Ty::string_datatype(), true),
            ColumnSchema::new("index_file_size", Ty::uint64_datatype(), true),
            ColumnSchema::new("num_rows", Ty::uint64_datatype(), false),
            ColumnSchema::new("num_row_groups", Ty::uint64_datatype(), false),
            ColumnSchema::new("min_ts", Ty::timestamp_nanosecond_datatype(), true),
            ColumnSchema::new("max_ts", Ty::timestamp_nanosecond_datatype(), true),
            ColumnSchema::new("sequence", Ty::uint64_datatype(), true),
        ]))
    }

    /// Converts a list of manifest sst entries to a record batch.
    pub fn to_record_batch(entries: &[Self]) -> std::result::Result<DfRecordBatch, ArrowError> {
        let schema = Self::schema();
        let table_dirs = entries.iter().map(|e| e.table_dir.as_str());
        let region_ids = entries.iter().map(|e| e.region_id.as_u64());
        let table_ids = entries.iter().map(|e| e.table_id);
        let region_numbers = entries.iter().map(|e| e.region_number);
        let region_groups = entries.iter().map(|e| e.region_group);
        let region_sequences = entries.iter().map(|e| e.region_sequence);
        let file_ids = entries.iter().map(|e| e.file_id.as_str());
        let levels = entries.iter().map(|e| e.level);
        let file_paths = entries.iter().map(|e| e.file_path.as_str());
        let file_sizes = entries.iter().map(|e| e.file_size);
        let index_file_paths = entries.iter().map(|e| e.index_file_path.as_ref());
        let index_file_sizes = entries.iter().map(|e| e.index_file_size);
        let num_rows = entries.iter().map(|e| e.num_rows);
        let num_row_groups = entries.iter().map(|e| e.num_row_groups);
        let min_ts = entries.iter().map(|e| {
            e.min_ts
                .convert_to(TimeUnit::Nanosecond)
                .map(|ts| ts.value())
        });
        let max_ts = entries.iter().map(|e| {
            e.max_ts
                .convert_to(TimeUnit::Nanosecond)
                .map(|ts| ts.value())
        });
        let sequences = entries.iter().map(|e| e.sequence);

        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from_iter_values(table_dirs)),
            Arc::new(UInt64Array::from_iter_values(region_ids)),
            Arc::new(UInt32Array::from_iter_values(table_ids)),
            Arc::new(UInt32Array::from_iter_values(region_numbers)),
            Arc::new(UInt8Array::from_iter_values(region_groups)),
            Arc::new(UInt32Array::from_iter_values(region_sequences)),
            Arc::new(StringArray::from_iter_values(file_ids)),
            Arc::new(UInt8Array::from_iter_values(levels)),
            Arc::new(StringArray::from_iter_values(file_paths)),
            Arc::new(UInt64Array::from_iter_values(file_sizes)),
            Arc::new(StringArray::from_iter(index_file_paths)),
            Arc::new(UInt64Array::from_iter(index_file_sizes)),
            Arc::new(UInt64Array::from_iter_values(num_rows)),
            Arc::new(UInt64Array::from_iter_values(num_row_groups)),
            Arc::new(TimestampNanosecondArray::from_iter(min_ts)),
            Arc::new(TimestampNanosecondArray::from_iter(max_ts)),
            Arc::new(UInt64Array::from_iter(sequences)),
        ];

        DfRecordBatch::try_new(schema.arrow_schema().clone(), columns)
    }

    /// Reserved internal inspect table name.
    ///
    /// This table name is used only for building logical plans on the
    /// frontend -> datanode path. It is not user-visible and cannot be
    /// referenced by user queries.
    pub fn reserved_table_name_for_inspection() -> &'static str {
        "__inspect/__mito/__sst_manifest"
    }

    /// Builds a logical plan for scanning the manifest sst entries.
    pub fn build_plan(scan_request: ScanRequest) -> Result<LogicalPlan, DataFusionError> {
        build_plan_helper(
            scan_request,
            Self::reserved_table_name_for_inspection(),
            Self::schema(),
        )
    }
}

/// An entry describing a SST file listed from storage layer directly.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageSstEntry {
    /// Full path of the SST file in object store.
    pub file_path: String,
    /// File size in bytes.
    pub file_size: Option<u64>,
    /// Last modified time in milliseconds since epoch, if available from storage.
    pub last_modified_ms: Option<Timestamp>,
}

impl StorageSstEntry {
    /// Returns the schema of the storage sst entry.
    pub fn schema() -> SchemaRef {
        use datatypes::prelude::ConcreteDataType as Ty;
        Arc::new(Schema::new(vec![
            ColumnSchema::new("file_path", Ty::string_datatype(), false),
            ColumnSchema::new("file_size", Ty::uint64_datatype(), true),
            ColumnSchema::new(
                "last_modified_ms",
                Ty::timestamp_millisecond_datatype(),
                true,
            ),
        ]))
    }

    /// Converts a list of storage sst entries to a record batch.
    pub fn to_record_batch(entries: &[Self]) -> std::result::Result<DfRecordBatch, ArrowError> {
        let schema = Self::schema();
        let file_paths = entries.iter().map(|e| e.file_path.as_str());
        let file_sizes = entries.iter().map(|e| e.file_size);
        let last_modified_ms = entries.iter().map(|e| {
            e.last_modified_ms
                .and_then(|ts| ts.convert_to(TimeUnit::Millisecond).map(|ts| ts.value()))
        });

        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from_iter_values(file_paths)),
            Arc::new(UInt64Array::from_iter(file_sizes)),
            Arc::new(TimestampMillisecondArray::from_iter(last_modified_ms)),
        ];

        DfRecordBatch::try_new(schema.arrow_schema().clone(), columns)
    }

    /// Reserved internal inspect table name.
    ///
    /// This table name is used only for building logical plans on the
    /// frontend -> datanode path. It is not user-visible and cannot be
    /// referenced by user queries.
    pub fn reserved_table_name_for_inspection() -> &'static str {
        "__inspect/__mito/__sst_storage"
    }

    /// Builds a logical plan for scanning the storage sst entries.
    pub fn build_plan(scan_request: ScanRequest) -> Result<LogicalPlan, DataFusionError> {
        build_plan_helper(
            scan_request,
            Self::reserved_table_name_for_inspection(),
            Self::schema(),
        )
    }
}

fn build_plan_helper(
    scan_request: ScanRequest,
    table_name: &str,
    schema: SchemaRef,
) -> Result<LogicalPlan, DataFusionError> {
    let table_source = LogicalTableSource::new(schema.arrow_schema().clone());

    let mut builder = LogicalPlanBuilder::scan(
        table_name,
        Arc::new(table_source),
        scan_request.projection.clone(),
    )?;

    for filter in scan_request.filters {
        builder = builder.filter(filter)?;
    }

    if let Some(limit) = scan_request.limit {
        builder = builder.limit(0, Some(limit))?;
    }

    builder.build()
}

#[cfg(test)]
mod tests {
    use datafusion_common::TableReference;
    use datafusion_expr::{binary_expr, col, lit, LogicalPlan, Operator};
    use datatypes::arrow::array::{
        Array, TimestampMillisecondArray, TimestampNanosecondArray, UInt32Array, UInt64Array,
        UInt8Array,
    };
    use datatypes::arrow_array::StringArray;

    use super::*;

    #[test]
    fn test_sst_entry_manifest_to_record_batch() {
        // Prepare entries
        let table_id1: TableId = 1;
        let region_group1: RegionGroup = 2;
        let region_seq1: RegionSeq = 3;
        let region_number1: RegionNumber = ((region_group1 as u32) << 24) | region_seq1;
        let region_id1 = RegionId::with_group_and_seq(table_id1, region_group1, region_seq1);

        let table_id2: TableId = 5;
        let region_group2: RegionGroup = 1;
        let region_seq2: RegionSeq = 42;
        let region_number2: RegionNumber = ((region_group2 as u32) << 24) | region_seq2;
        let region_id2 = RegionId::with_group_and_seq(table_id2, region_group2, region_seq2);

        let entries = vec![
            ManifestSstEntry {
                table_dir: "tdir1".to_string(),
                region_id: region_id1,
                table_id: table_id1,
                region_number: region_number1,
                region_group: region_group1,
                region_sequence: region_seq1,
                file_id: "f1".to_string(),
                level: 1,
                file_path: "/p1".to_string(),
                file_size: 100,
                index_file_path: None,
                index_file_size: None,
                num_rows: 10,
                num_row_groups: 2,
                min_ts: Timestamp::new_millisecond(1000), // 1s -> 1_000_000_000ns
                max_ts: Timestamp::new_second(2),         // 2s -> 2_000_000_000ns
                sequence: None,
            },
            ManifestSstEntry {
                table_dir: "tdir2".to_string(),
                region_id: region_id2,
                table_id: table_id2,
                region_number: region_number2,
                region_group: region_group2,
                region_sequence: region_seq2,
                file_id: "f2".to_string(),
                level: 3,
                file_path: "/p2".to_string(),
                file_size: 200,
                index_file_path: Some("idx".to_string()),
                index_file_size: Some(11),
                num_rows: 20,
                num_row_groups: 4,
                min_ts: Timestamp::new_nanosecond(5),     // 5ns
                max_ts: Timestamp::new_microsecond(2000), // 2ms -> 2_000_000ns
                sequence: Some(9),
            },
        ];

        let schema = ManifestSstEntry::schema();
        let batch = ManifestSstEntry::to_record_batch(&entries).unwrap();

        // Schema checks
        assert_eq!(schema.arrow_schema().fields().len(), batch.num_columns());
        assert_eq!(2, batch.num_rows());
        for (i, f) in schema.arrow_schema().fields().iter().enumerate() {
            assert_eq!(f.name(), batch.schema().field(i).name());
            assert_eq!(f.is_nullable(), batch.schema().field(i).is_nullable());
            assert_eq!(f.data_type(), batch.schema().field(i).data_type());
        }

        // Column asserts
        let table_dirs = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!("tdir1", table_dirs.value(0));
        assert_eq!("tdir2", table_dirs.value(1));

        let region_ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(region_id1.as_u64(), region_ids.value(0));
        assert_eq!(region_id2.as_u64(), region_ids.value(1));

        let table_ids = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(table_id1, table_ids.value(0));
        assert_eq!(table_id2, table_ids.value(1));

        let region_numbers = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(region_number1, region_numbers.value(0));
        assert_eq!(region_number2, region_numbers.value(1));

        let region_groups = batch
            .column(4)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();
        assert_eq!(region_group1, region_groups.value(0));
        assert_eq!(region_group2, region_groups.value(1));

        let region_sequences = batch
            .column(5)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(region_seq1, region_sequences.value(0));
        assert_eq!(region_seq2, region_sequences.value(1));

        let file_ids = batch
            .column(6)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!("f1", file_ids.value(0));
        assert_eq!("f2", file_ids.value(1));

        let levels = batch
            .column(7)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();
        assert_eq!(1, levels.value(0));
        assert_eq!(3, levels.value(1));

        let file_paths = batch
            .column(8)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!("/p1", file_paths.value(0));
        assert_eq!("/p2", file_paths.value(1));

        let file_sizes = batch
            .column(9)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(100, file_sizes.value(0));
        assert_eq!(200, file_sizes.value(1));

        let index_file_paths = batch
            .column(10)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(index_file_paths.is_null(0));
        assert_eq!("idx", index_file_paths.value(1));

        let index_file_sizes = batch
            .column(11)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert!(index_file_sizes.is_null(0));
        assert_eq!(11, index_file_sizes.value(1));

        let num_rows = batch
            .column(12)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(10, num_rows.value(0));
        assert_eq!(20, num_rows.value(1));

        let num_row_groups = batch
            .column(13)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(2, num_row_groups.value(0));
        assert_eq!(4, num_row_groups.value(1));

        let min_ts = batch
            .column(14)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(1_000_000_000, min_ts.value(0));
        assert_eq!(5, min_ts.value(1));

        let max_ts = batch
            .column(15)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(2_000_000_000, max_ts.value(0));
        assert_eq!(2_000_000, max_ts.value(1));

        let sequences = batch
            .column(16)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert!(sequences.is_null(0));
        assert_eq!(9, sequences.value(1));
    }

    #[test]
    fn test_sst_entry_storage_to_record_batch() {
        let entries = vec![
            StorageSstEntry {
                file_path: "/s1".to_string(),
                file_size: None,
                last_modified_ms: None,
            },
            StorageSstEntry {
                file_path: "/s2".to_string(),
                file_size: Some(123),
                last_modified_ms: Some(Timestamp::new_millisecond(456)),
            },
        ];

        let schema = StorageSstEntry::schema();
        let batch = StorageSstEntry::to_record_batch(&entries).unwrap();

        assert_eq!(schema.arrow_schema().fields().len(), batch.num_columns());
        assert_eq!(2, batch.num_rows());

        let file_paths = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!("/s1", file_paths.value(0));
        assert_eq!("/s2", file_paths.value(1));

        let file_sizes = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert!(file_sizes.is_null(0));
        assert_eq!(123, file_sizes.value(1));

        let last_modified = batch
            .column(2)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert!(last_modified.is_null(0));
        assert_eq!(456, last_modified.value(1));
    }

    #[test]
    fn test_manifest_build_plan() {
        // Note: filter must reference a column in the projected schema
        let request = ScanRequest {
            projection: Some(vec![0, 1, 2]),
            filters: vec![binary_expr(col("table_id"), Operator::Gt, lit(0))],
            limit: Some(5),
            ..Default::default()
        };

        let plan = ManifestSstEntry::build_plan(request).unwrap();

        // Expect plan to be Filter -> Limit -> TableScan or Filter+Limit wrapped.
        // We'll pattern match to reach TableScan and verify key fields.
        let (scan, has_filter, has_limit) = extract_scan(&plan);

        assert!(has_filter);
        assert!(has_limit);
        assert_eq!(
            scan.table_name,
            TableReference::bare(ManifestSstEntry::reserved_table_name_for_inspection())
        );
        assert_eq!(scan.projection, Some(vec![0, 1, 2]));

        // projected schema should match projection
        let fields = scan.projected_schema.fields();
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0].name(), "table_dir");
        assert_eq!(fields[1].name(), "region_id");
        assert_eq!(fields[2].name(), "table_id");
    }

    #[test]
    fn test_storage_build_plan() {
        let request = ScanRequest {
            projection: Some(vec![0, 2]),
            filters: vec![binary_expr(col("file_path"), Operator::Eq, lit("/a"))],
            limit: Some(1),
            ..Default::default()
        };

        let plan = StorageSstEntry::build_plan(request).unwrap();
        let (scan, has_filter, has_limit) = extract_scan(&plan);
        assert!(has_filter);
        assert!(has_limit);
        assert_eq!(
            scan.table_name,
            TableReference::bare(StorageSstEntry::reserved_table_name_for_inspection())
        );
        assert_eq!(scan.projection, Some(vec![0, 2]));

        let fields = scan.projected_schema.fields();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name(), "file_path");
        assert_eq!(fields[1].name(), "last_modified_ms");
    }

    // Helper to reach TableScan and detect presence of Filter/Limit in plan
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
