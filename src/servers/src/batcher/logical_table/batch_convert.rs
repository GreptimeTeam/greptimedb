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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::compute::concat_batches;
use arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema, TimeUnit};
use arrow::record_batch::RecordBatch;
use common_query::prelude::{greptime_timestamp, greptime_value};
use metric_engine::batch_modifier::{TagColumnInfo, modify_batch_sparse};
use smallvec::SmallVec;
use snafu::{OptionExt, ResultExt, ensure};
use table::metadata::TableId;

use crate::error;
use crate::error::Result;
use crate::metrics::PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED;

pub(in crate::batcher::logical_table) const PHYSICAL_REGION_ESSENTIAL_COLUMN_COUNT: usize = 3;

/// An aligned logical record batch and its timestamp column index.
#[derive(Debug, Clone)]
pub struct RecordBatchWithTsIdx {
    /// The aligned logical record batch.
    pub(in crate::batcher::logical_table) batch: RecordBatch,
    /// The timestamp column index in `batch`.
    pub(in crate::batcher::logical_table) timestamp_index: usize,
}

impl RecordBatchWithTsIdx {
    /// Creates a record batch with a validated timestamp column index.
    pub fn try_new(batch: RecordBatch, timestamp_index: usize) -> Result<Self> {
        let schema = batch.schema();
        let timestamp_field = schema.fields().get(timestamp_index).with_context(|| {
            error::InvalidPromRemoteRequestSnafu {
                msg: format!(
                    "Timestamp column index {} is out of bounds for record batch with {} columns",
                    timestamp_index,
                    batch.num_columns()
                ),
            }
        })?;
        ensure!(
            matches!(timestamp_field.data_type(), ArrowDataType::Timestamp(_, _)),
            error::InvalidPromRemoteRequestSnafu {
                msg: format!(
                    "Column at index {} is not a timestamp column: {:?}",
                    timestamp_index,
                    timestamp_field.data_type()
                ),
            }
        );

        Ok(Self {
            batch,
            timestamp_index,
        })
    }

    #[cfg(test)]
    pub(crate) fn into_parts(self) -> (RecordBatch, usize) {
        (self.batch, self.timestamp_index)
    }
}

#[derive(Debug, Clone)]
pub struct TableBatch {
    pub table_name: String,
    pub table_id: TableId,
    pub batches: Vec<RecordBatchWithTsIdx>,
    pub row_count: usize,
}

/// Classifies columns in a logical-table batch for sparse primary-key conversion.
///
/// Returns:
/// - `Vec<TagColumnInfo>`: all Utf8 tag columns sorted by tag name, used for
///   TSID and sparse primary-key encoding.
/// - `SmallVec<[usize; 3]>`: indices of columns copied into the physical batch
///   after `__primary_key`, ordered as `[greptime_timestamp, greptime_value,
///   partition_tag_columns...]`.
pub(in crate::batcher::logical_table) fn columns_taxonomy(
    batch_schema: &Arc<ArrowSchema>,
    table_name: &str,
    name_to_ids: &HashMap<String, u32>,
    partition_columns: &HashSet<&str>,
) -> Result<(Vec<TagColumnInfo>, SmallVec<[usize; 3]>)> {
    let mut tag_columns = Vec::new();
    let mut essential_column_indices =
        SmallVec::<[usize; 3]>::with_capacity(2 + partition_columns.len());
    // Placeholder for greptime_timestamp and greptime_value
    essential_column_indices.push(0);
    essential_column_indices.push(0);

    let mut timestamp_index = None;
    let mut value_index = None;

    for (index, field) in batch_schema.fields().iter().enumerate() {
        match field.data_type() {
            ArrowDataType::Utf8 => {
                let column_id = name_to_ids.get(field.name()).copied().with_context(|| {
                    error::InvalidPromRemoteRequestSnafu {
                        msg: format!(
                            "Column '{}' from logical table '{}' not found in physical table column IDs",
                            field.name(),
                            table_name
                        ),
                    }
                })?;
                tag_columns.push(TagColumnInfo {
                    name: field.name().clone(),
                    index,
                    column_id,
                });

                if partition_columns.contains(field.name().as_str()) {
                    essential_column_indices.push(index);
                }
            }
            ArrowDataType::Timestamp(TimeUnit::Millisecond, _) => {
                ensure!(
                    timestamp_index.replace(index).is_none(),
                    error::InvalidPromRemoteRequestSnafu {
                        msg: format!(
                            "Duplicated timestamp column in logical table '{}' batch schema",
                            table_name
                        ),
                    }
                );
            }
            ArrowDataType::Float64 => {
                ensure!(
                    value_index.replace(index).is_none(),
                    error::InvalidPromRemoteRequestSnafu {
                        msg: format!(
                            "Duplicated value column in logical table '{}' batch schema",
                            table_name
                        ),
                    }
                );
            }
            datatype => {
                return error::InvalidPromRemoteRequestSnafu {
                    msg: format!(
                        "Unexpected data type '{datatype:?}' in logical table '{}' batch schema",
                        table_name
                    ),
                }
                .fail();
            }
        }
    }

    let timestamp_index =
        timestamp_index.with_context(|| error::InvalidPromRemoteRequestSnafu {
            msg: format!(
                "Missing essential column '{}' in logical table '{}' batch schema",
                greptime_timestamp(),
                table_name
            ),
        })?;
    let value_index = value_index.with_context(|| error::InvalidPromRemoteRequestSnafu {
        msg: format!(
            "Missing essential column '{}' in logical table '{}' batch schema",
            greptime_value(),
            table_name
        ),
    })?;

    tag_columns.sort_by(|a, b| a.name.cmp(&b.name));

    essential_column_indices[0] = timestamp_index;
    essential_column_indices[1] = value_index;

    Ok((tag_columns, essential_column_indices))
}

pub(in crate::batcher::logical_table) fn strip_partition_columns_from_batch(
    batch: RecordBatch,
) -> Result<RecordBatch> {
    ensure!(
        batch.num_columns() >= PHYSICAL_REGION_ESSENTIAL_COLUMN_COUNT,
        error::InternalSnafu {
            err_msg: format!(
                "Expected at least {} columns in physical batch, got {}",
                PHYSICAL_REGION_ESSENTIAL_COLUMN_COUNT,
                batch.num_columns()
            ),
        }
    );
    let essential_indices: Vec<usize> = (0..PHYSICAL_REGION_ESSENTIAL_COLUMN_COUNT).collect();
    batch.project(&essential_indices).context(error::ArrowSnafu)
}

/// Transforms logical table batches into physical format (sparse primary key encoding).
///
/// It identifies tag columns and essential columns (timestamp, value) for each logical batch
/// and applies sparse primary key modification.
pub(in crate::batcher::logical_table) fn transform_logical_batches_to_physical(
    table_batches: &[TableBatch],
    name_to_ids: &HashMap<String, u32>,
    partition_columns_set: &HashSet<&str>,
) -> Result<Vec<RecordBatch>> {
    let mut modified_batches: Vec<RecordBatch> =
        Vec::with_capacity(table_batches.iter().map(|b| b.batches.len()).sum());

    let mut modify_elapsed = Duration::ZERO;
    let mut columns_taxonomy_elapsed = Duration::ZERO;

    for table_batch in table_batches {
        let table_id = table_batch.table_id;

        for batch in &table_batch.batches {
            let batch = &batch.batch;
            let batch_schema = batch.schema();
            let start = Instant::now();
            let (tag_columns, essential_col_indices) = columns_taxonomy(
                &batch_schema,
                &table_batch.table_name,
                name_to_ids,
                partition_columns_set,
            )?;

            columns_taxonomy_elapsed += start.elapsed();
            if tag_columns.is_empty() && essential_col_indices.is_empty() {
                continue;
            }

            let modified = {
                let start = Instant::now();
                // The schema of modified batch is: __primary_key, timestamp, value, other partition columns...
                let batch = modify_batch_sparse(
                    batch.clone(),
                    table_id,
                    &tag_columns,
                    &essential_col_indices,
                )?;
                modify_elapsed += start.elapsed();
                batch
            };

            modified_batches.push(modified);
        }
    }

    PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED
        .with_label_values(&["flush_physical_modify_batch"])
        .observe(modify_elapsed.as_secs_f64());
    PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED
        .with_label_values(&["flush_physical_columns_taxonomy"])
        .observe(columns_taxonomy_elapsed.as_secs_f64());

    ensure!(
        !modified_batches.is_empty(),
        error::InternalSnafu {
            err_msg: "No batches can be transformed during pending flush",
        }
    );
    Ok(modified_batches)
}

/// Concatenates all modified batches into a single large batch.
///
/// All modified batches share the same physical schema.
pub(in crate::batcher::logical_table) fn concat_modified_batches(
    modified_batches: &[RecordBatch],
) -> Result<RecordBatch> {
    let combined_schema = modified_batches[0].schema();
    let _timer = PENDING_ROWS_BATCH_FLUSH_STAGE_ELAPSED
        .with_label_values(&["flush_physical_concat_all"])
        .start_timer();
    concat_batches(&combined_schema, modified_batches).context(error::ArrowSnafu)
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    use arrow::array::{BinaryArray, StringArray, TimestampMillisecondArray};
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;
    use smallvec::SmallVec;

    use crate::batcher::logical_table::batch_convert::{
        TableBatch, columns_taxonomy, strip_partition_columns_from_batch,
        transform_logical_batches_to_physical,
    };
    use crate::batcher::logical_table::test_util::mock_aligned_tag_batch;
    use crate::error::Error;

    #[test]
    fn test_strip_partition_columns_from_batch_removes_partition_tags() {
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                Field::new("__primary_key", ArrowDataType::Binary, false),
                Field::new(
                    "greptime_timestamp",
                    ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new("greptime_value", ArrowDataType::Float64, true),
                Field::new("host", ArrowDataType::Utf8, true),
            ])),
            vec![
                Arc::new(BinaryArray::from(vec![b"k1".as_slice()])),
                Arc::new(TimestampMillisecondArray::from(vec![1000_i64])),
                Arc::new(arrow::array::Float64Array::from(vec![42.0_f64])),
                Arc::new(StringArray::from(vec!["node-1"])),
            ],
        )
        .unwrap();

        let stripped = strip_partition_columns_from_batch(batch).unwrap();

        assert_eq!(3, stripped.num_columns());
        assert_eq!("__primary_key", stripped.schema().field(0).name());
        assert_eq!("greptime_timestamp", stripped.schema().field(1).name());
        assert_eq!("greptime_value", stripped.schema().field(2).name());
    }

    #[test]
    fn test_strip_partition_columns_from_batch_projects_essential_columns_without_lookup() {
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                Field::new("__primary_key", ArrowDataType::Binary, false),
                Field::new(
                    "greptime_timestamp",
                    ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new("greptime_value", ArrowDataType::Float64, true),
                Field::new("host", ArrowDataType::Utf8, true),
            ])),
            vec![
                Arc::new(BinaryArray::from(vec![b"k1".as_slice()])),
                Arc::new(TimestampMillisecondArray::from(vec![1000_i64])),
                Arc::new(arrow::array::Float64Array::from(vec![42.0_f64])),
                Arc::new(StringArray::from(vec!["node-1"])),
            ],
        )
        .unwrap();

        let stripped = strip_partition_columns_from_batch(batch).unwrap();

        assert_eq!(3, stripped.num_columns());
        assert_eq!("__primary_key", stripped.schema().field(0).name());
        assert_eq!("greptime_timestamp", stripped.schema().field(1).name());
        assert_eq!("greptime_value", stripped.schema().field(2).name());
    }

    #[test]
    fn test_collect_tag_columns_and_non_tag_indices_keeps_partition_tag_column() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new(
                "greptime_timestamp",
                ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("greptime_value", ArrowDataType::Float64, true),
            Field::new("host", ArrowDataType::Utf8, true),
            Field::new("region", ArrowDataType::Utf8, true),
        ]));
        let name_to_ids =
            HashMap::from([("host".to_string(), 1_u32), ("region".to_string(), 2_u32)]);
        let partition_columns = HashSet::from(["host"]);

        let (tag_columns, non_tag_indices) =
            columns_taxonomy(&schema, "cpu", &name_to_ids, &partition_columns).unwrap();

        assert_eq!(2, tag_columns.len());
        assert_eq!(&[0, 1, 2], non_tag_indices.as_slice());
    }

    #[test]
    fn test_collect_tag_columns_and_non_tag_indices_prioritizes_essential_columns() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("host", ArrowDataType::Utf8, true),
            Field::new("greptime_value", ArrowDataType::Float64, true),
            Field::new(
                "greptime_timestamp",
                ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("region", ArrowDataType::Utf8, true),
        ]));
        let name_to_ids =
            HashMap::from([("host".to_string(), 1_u32), ("region".to_string(), 2_u32)]);
        let partition_columns = HashSet::from(["host", "region"]);

        let (_tag_columns, non_tag_indices): (_, SmallVec<[usize; 3]>) =
            columns_taxonomy(&schema, "cpu", &name_to_ids, &partition_columns).unwrap();

        assert_eq!(&[2, 1, 0, 3], non_tag_indices.as_slice());
    }

    #[test]
    fn test_collect_tag_columns_and_non_tag_indices_rejects_unexpected_data_type() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new(
                "greptime_timestamp",
                ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("greptime_value", ArrowDataType::Float64, true),
            Field::new("host", ArrowDataType::Utf8, true),
            Field::new("invalid", ArrowDataType::Boolean, true),
        ]));
        let name_to_ids = HashMap::from([("host".to_string(), 1_u32)]);
        let partition_columns = HashSet::from(["host"]);

        let result = columns_taxonomy(&schema, "cpu", &name_to_ids, &partition_columns);

        assert!(matches!(
            result,
            Err(Error::InvalidPromRemoteRequest { .. })
        ));
    }

    #[test]
    fn test_collect_tag_columns_and_non_tag_indices_rejects_int64_timestamp_column() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("greptime_timestamp", ArrowDataType::Int64, false),
            Field::new("greptime_value", ArrowDataType::Float64, true),
            Field::new("host", ArrowDataType::Utf8, true),
        ]));
        let name_to_ids = HashMap::from([("host".to_string(), 1_u32)]);
        let partition_columns = HashSet::from(["host"]);

        let result = columns_taxonomy(&schema, "cpu", &name_to_ids, &partition_columns);

        assert!(matches!(
            result,
            Err(Error::InvalidPromRemoteRequest { .. })
        ));
    }

    #[test]
    fn test_collect_tag_columns_and_non_tag_indices_rejects_duplicated_timestamp_column() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new(
                "ts1",
                ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(
                "ts2",
                ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("greptime_value", ArrowDataType::Float64, true),
            Field::new("host", ArrowDataType::Utf8, true),
        ]));
        let name_to_ids = HashMap::from([("host".to_string(), 1_u32)]);
        let partition_columns = HashSet::from(["host"]);

        let result = columns_taxonomy(&schema, "cpu", &name_to_ids, &partition_columns);

        assert!(matches!(
            result,
            Err(Error::InvalidPromRemoteRequest { .. })
        ));
    }

    #[test]
    fn test_collect_tag_columns_and_non_tag_indices_rejects_duplicated_value_column() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new(
                "greptime_timestamp",
                ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value1", ArrowDataType::Float64, true),
            Field::new("value2", ArrowDataType::Float64, true),
            Field::new("host", ArrowDataType::Utf8, true),
        ]));
        let name_to_ids = HashMap::from([("host".to_string(), 1_u32)]);
        let partition_columns = HashSet::from(["host"]);

        let result = columns_taxonomy(&schema, "cpu", &name_to_ids, &partition_columns);

        assert!(matches!(
            result,
            Err(Error::InvalidPromRemoteRequest { .. })
        ));
    }

    #[test]
    fn test_modify_batch_sparse_with_taxonomy_per_batch() {
        use arrow::array::BinaryArray;
        use metric_engine::batch_modifier::modify_batch_sparse;

        let schema1 = Arc::new(ArrowSchema::new(vec![
            Field::new(
                "greptime_timestamp",
                ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("greptime_value", ArrowDataType::Float64, true),
            Field::new("tag1", ArrowDataType::Utf8, true),
        ]));

        let schema2 = Arc::new(ArrowSchema::new(vec![
            Field::new(
                "greptime_timestamp",
                ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("greptime_value", ArrowDataType::Float64, true),
            Field::new("tag1", ArrowDataType::Utf8, true),
            Field::new("tag2", ArrowDataType::Utf8, true),
        ]));
        let batch2 = RecordBatch::try_new(
            schema2.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![2000])),
                Arc::new(arrow::array::Float64Array::from(vec![2.0])),
                Arc::new(StringArray::from(vec!["v1"])),
                Arc::new(StringArray::from(vec!["v2"])),
            ],
        )
        .unwrap();

        let name_to_ids = HashMap::from([("tag1".to_string(), 1), ("tag2".to_string(), 2)]);
        let partition_columns = HashSet::new();

        // A batch that only has tag1, same values as batch2 for ts and val.
        let batch3 = RecordBatch::try_new(
            schema1.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![2000])),
                Arc::new(arrow::array::Float64Array::from(vec![2.0])),
                Arc::new(StringArray::from(vec!["v1"])),
            ],
        )
        .unwrap();

        // Simulate the new loop logic in flush_batch_physical:
        // Resolve taxonomy FOR EACH BATCH.
        let (tag_columns2, indices2) =
            columns_taxonomy(&batch2.schema(), "table", &name_to_ids, &partition_columns).unwrap();
        let modified2 = modify_batch_sparse(batch2, 123, &tag_columns2, &indices2).unwrap();

        let (tag_columns3, indices3) =
            columns_taxonomy(&batch3.schema(), "table", &name_to_ids, &partition_columns).unwrap();
        let modified3 = modify_batch_sparse(batch3, 123, &tag_columns3, &indices3).unwrap();

        let pk2 = modified2
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let pk3 = modified3
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();

        // Now they SHOULD be different because tag2 is included in pk2 but not in pk3.
        assert_ne!(
            pk2.value(0),
            pk3.value(0),
            "PK should be different because batch2 has tag2!"
        );
    }

    #[test]
    fn test_transform_logical_batches_to_physical_success() {
        let batch = mock_aligned_tag_batch("tag1", "v1", 1000, 1.0);

        let table_batches = vec![TableBatch {
            table_name: "t1".to_string(),
            table_id: 1,
            batches: vec![batch],
            row_count: 1,
        }];

        let name_to_ids = HashMap::from([("tag1".to_string(), 1)]);
        let partition_columns = HashSet::new();
        let modified =
            transform_logical_batches_to_physical(&table_batches, &name_to_ids, &partition_columns)
                .unwrap();

        assert_eq!(1, modified.len());
        assert_eq!(3, modified[0].num_columns());
        assert_eq!("__primary_key", modified[0].schema().field(0).name());
        assert_eq!("greptime_timestamp", modified[0].schema().field(1).name());
        assert_eq!("greptime_value", modified[0].schema().field(2).name());
    }

    #[test]
    fn test_transform_logical_batches_to_physical_taxonomy_failure() {
        let batch = mock_aligned_tag_batch("tag1", "v1", 1000, 1.0);

        let table_batches = vec![TableBatch {
            table_name: "t1".to_string(),
            table_id: 1,
            batches: vec![batch],
            row_count: 1,
        }];

        // tag1 is missing from name_to_ids, causing columns_taxonomy to fail.
        let name_to_ids = HashMap::new();
        let partition_columns = HashSet::new();
        let err =
            transform_logical_batches_to_physical(&table_batches, &name_to_ids, &partition_columns)
                .unwrap_err();

        assert!(
            err.to_string()
                .contains("not found in physical table column IDs")
        );
    }

    #[test]
    fn test_transform_logical_batches_to_physical_multiple_batches() {
        let batch1 = mock_aligned_tag_batch("tag1", "v1", 1000, 1.0);
        let batch2 = mock_aligned_tag_batch("tag2", "v2", 2000, 2.0);

        let table_batches = vec![
            TableBatch {
                table_name: "t1".to_string(),
                table_id: 1,
                batches: vec![batch1],
                row_count: 1,
            },
            TableBatch {
                table_name: "t2".to_string(),
                table_id: 2,
                batches: vec![batch2],
                row_count: 1,
            },
        ];

        let name_to_ids = HashMap::from([("tag1".to_string(), 1), ("tag2".to_string(), 2)]);
        let partition_columns = HashSet::new();
        let modified =
            transform_logical_batches_to_physical(&table_batches, &name_to_ids, &partition_columns)
                .unwrap();

        assert_eq!(2, modified.len());
    }

    #[test]
    fn test_transform_logical_batches_to_physical_mixed_success_failure() {
        let batch1 = mock_aligned_tag_batch("tag1", "v1", 1000, 1.0);
        let batch2 = mock_aligned_tag_batch("tag2", "v2", 2000, 2.0);

        let table_batches = vec![
            TableBatch {
                table_name: "t1".to_string(),
                table_id: 1,
                batches: vec![batch1],
                row_count: 1,
            },
            TableBatch {
                table_name: "t2".to_string(),
                table_id: 2,
                batches: vec![batch2],
                row_count: 1,
            },
        ];

        // tag1 is missing from name_to_ids, causing batch1 to fail.
        let name_to_ids = HashMap::from([("tag2".to_string(), 2)]);
        let partition_columns = HashSet::new();
        let err =
            transform_logical_batches_to_physical(&table_batches, &name_to_ids, &partition_columns)
                .unwrap_err();

        assert!(err.to_string().contains("tag1"));
    }
}
