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

use api::v1::{ColumnSchema, RowInsertRequests, Rows};
use arrow::datatypes::Schema as ArrowSchema;
use async_trait::async_trait;
use session::context::QueryContextRef;
use snafu::OptionExt;

use crate::batcher::logical_table::LogicalTablePendingRowsBatcher;
use crate::batcher::logical_table::batch_convert::RecordBatchWithTsIdx;
use crate::error;
use crate::error::Result;
use crate::metrics::PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED;
use crate::prom_row_builder::{
    build_prom_create_table_schema_from_proto, identify_missing_columns_from_proto,
    rows_to_aligned_record_batch,
};

#[async_trait]
pub trait PendingRowsSchemaAlterer: Send + Sync {
    /// Batch-create multiple logical tables that are missing.
    /// Each entry is `(table_name, request_schema)`.
    async fn create_tables_if_missing_batch(
        &self,
        catalog: &str,
        schema: &str,
        tables: &[(&str, &[ColumnSchema])],
        with_metric_engine: bool,
        ctx: QueryContextRef,
    ) -> Result<()>;

    /// Batch-alter multiple logical tables to add missing tag columns.
    /// Each entry is `(table_name, missing_column_names)`.
    async fn add_missing_prom_tag_columns_batch(
        &self,
        catalog: &str,
        schema: &str,
        tables: &[(&str, &[String])],
        ctx: QueryContextRef,
    ) -> Result<()>;
}

pub type PendingRowsSchemaAltererRef = Arc<dyn PendingRowsSchemaAlterer>;

/// Intermediate planning state for resolving and preparing logical tables
/// before row-to-batch alignment.
pub(in crate::batcher::logical_table) struct TableResolutionPlan {
    /// Resolved table schema and table id by logical table name.
    pub(in crate::batcher::logical_table) region_schemas: HashMap<String, (Arc<ArrowSchema>, u32)>,
    /// Missing tables that need to be created before alignment.
    pub(in crate::batcher::logical_table) tables_to_create: Vec<(String, Vec<ColumnSchema>)>,
    /// Existing tables that need tag-column schema evolution.
    pub(in crate::batcher::logical_table) tables_to_alter: Vec<(String, Vec<String>)>,
}

impl LogicalTablePendingRowsBatcher {
    /// Converts proto `RowInsertRequests` directly into aligned `RecordBatch`es
    /// in a single pass, handling table creation, schema alteration, column
    /// renaming, reordering, and null-filling without building intermediate
    /// RecordBatches.
    pub(in crate::batcher::logical_table) async fn build_and_align_table_batches(
        &self,
        requests: RowInsertRequests,
        ctx: &QueryContextRef,
    ) -> Result<(Vec<(String, u32, RecordBatchWithTsIdx)>, usize)> {
        let catalog = ctx.current_catalog().to_string();
        let schema = ctx.current_schema();

        let (table_rows, total_rows) = Self::collect_non_empty_table_rows(requests);
        if total_rows == 0 {
            return Ok((Vec::new(), 0));
        }

        let unique_tables = Self::collect_unique_table_schemas(&table_rows)?;
        let mut plan = self
            .plan_table_resolution(&catalog, &schema, ctx, &unique_tables)
            .await?;

        self.create_missing_tables_and_refresh_schemas(
            &catalog,
            &schema,
            ctx,
            &table_rows,
            &mut plan,
        )
        .await?;

        self.alter_tables_and_refresh_schemas(&catalog, &schema, ctx, &mut plan)
            .await?;

        let aligned_batches = Self::build_aligned_batches(&table_rows, &plan.region_schemas)?;

        Ok((aligned_batches, total_rows))
    }
}

impl LogicalTablePendingRowsBatcher {
    /// Extracts non-empty `(table_name, rows)` pairs and computes total row
    /// count across the retained entries.
    pub(in crate::batcher::logical_table) fn collect_non_empty_table_rows(
        requests: RowInsertRequests,
    ) -> (Vec<(String, Rows)>, usize) {
        let mut table_rows: Vec<(String, Rows)> = Vec::with_capacity(requests.inserts.len());
        let mut total_rows = 0;

        for request in requests.inserts {
            let Some(rows) = request.rows else {
                continue;
            };
            if rows.rows.is_empty() {
                continue;
            }

            total_rows += rows.rows.len();
            table_rows.push((request.table_name, rows));
        }

        (table_rows, total_rows)
    }
}

impl LogicalTablePendingRowsBatcher {
    /// Returns unique `(table_name, proto_schema)` pairs while keeping the
    /// first-seen schema for duplicate table names.
    pub(in crate::batcher::logical_table) fn collect_unique_table_schemas(
        table_rows: &[(String, Rows)],
    ) -> Result<Vec<(&str, &[ColumnSchema])>> {
        let mut unique_tables: Vec<(&str, &[ColumnSchema])> = Vec::with_capacity(table_rows.len());
        let mut seen = HashSet::new();

        for (table_name, rows) in table_rows {
            if seen.insert(table_name.as_str()) {
                unique_tables.push((table_name.as_str(), &rows.schema));
            } else {
                // table_rows should group rows by table name.
                return error::InvalidPromRemoteRequestSnafu {
                    msg: format!(
                        "Found duplicated table name in RowInsertRequest: {}",
                        table_name
                    ),
                }
                .fail();
            }
        }

        Ok(unique_tables)
    }
}

impl LogicalTablePendingRowsBatcher {
    /// Resolves table metadata and classifies each table into existing,
    /// to-create, and to-alter groups used by subsequent DDL steps.
    pub(in crate::batcher::logical_table) async fn plan_table_resolution(
        &self,
        catalog: &str,
        schema: &str,
        ctx: &QueryContextRef,
        unique_tables: &[(&str, &[ColumnSchema])],
    ) -> Result<TableResolutionPlan> {
        let mut plan = TableResolutionPlan {
            region_schemas: HashMap::with_capacity(unique_tables.len()),
            tables_to_create: Vec::new(),
            tables_to_alter: Vec::new(),
        };

        let resolved_tables = {
            let _timer = PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED
                .with_label_values(&["align_resolve_table"])
                .start_timer();
            futures::future::join_all(unique_tables.iter().map(|(table_name, _)| {
                self.catalog_manager
                    .table(catalog, schema, table_name, Some(ctx.as_ref()))
            }))
            .await
        };

        for ((table_name, rows_schema), table_result) in unique_tables.iter().zip(resolved_tables) {
            let table = table_result?;

            if let Some(table) = table {
                let table_info = table.table_info();
                let table_id = table_info.ident.table_id;
                let region_schema = table_info.meta.schema.arrow_schema().clone();

                let missing_columns = {
                    let _timer = PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED
                        .with_label_values(&["align_identify_missing_columns"])
                        .start_timer();
                    identify_missing_columns_from_proto(rows_schema, region_schema.as_ref())?
                };
                if !missing_columns.is_empty() {
                    plan.tables_to_alter
                        .push(((*table_name).to_string(), missing_columns));
                }
                plan.region_schemas
                    .insert((*table_name).to_string(), (region_schema, table_id));
            } else {
                let request_schema = {
                    let _timer = PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED
                        .with_label_values(&["align_build_create_table_schema"])
                        .start_timer();
                    build_prom_create_table_schema_from_proto(rows_schema)?
                };
                plan.tables_to_create
                    .push(((*table_name).to_string(), request_schema));
            }
        }

        Ok(plan)
    }
}

impl LogicalTablePendingRowsBatcher {
    /// Batch-creates missing tables, refreshes their schema metadata, and
    /// enqueues follow-up alters for extra tag columns discovered in later rows.
    pub(in crate::batcher::logical_table) async fn create_missing_tables_and_refresh_schemas(
        &self,
        catalog: &str,
        schema: &str,
        ctx: &QueryContextRef,
        table_rows: &[(String, Rows)],
        plan: &mut TableResolutionPlan,
    ) -> Result<()> {
        if plan.tables_to_create.is_empty() {
            return Ok(());
        }

        let create_refs: Vec<(&str, &[ColumnSchema])> = plan
            .tables_to_create
            .iter()
            .map(|(name, schema)| (name.as_str(), schema.as_slice()))
            .collect();

        {
            let _timer = PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED
                .with_label_values(&["align_batch_create_tables"])
                .start_timer();
            self.schema_alterer
                .create_tables_if_missing_batch(
                    catalog,
                    schema,
                    &create_refs,
                    self.prom_store_with_metric_engine,
                    ctx.clone(),
                )
                .await?;
        }

        let created_table_results = {
            let _timer = PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED
                .with_label_values(&["align_resolve_table_after_create"])
                .start_timer();
            futures::future::join_all(plan.tables_to_create.iter().map(|(table_name, _)| {
                self.catalog_manager
                    .table(catalog, schema, table_name, Some(ctx.as_ref()))
            }))
            .await
        };

        for ((table_name, _), table_result) in
            plan.tables_to_create.iter().zip(created_table_results)
        {
            let table = table_result?.with_context(|| error::UnexpectedResultSnafu {
                reason: format!(
                    "Table not found after pending batch create attempt: {}",
                    table_name
                ),
            })?;
            let table_info = table.table_info();
            let table_id = table_info.ident.table_id;
            let region_schema = table_info.meta.schema.arrow_schema().clone();
            plan.region_schemas
                .insert(table_name.clone(), (region_schema, table_id));
        }

        Self::enqueue_alter_for_new_tables(table_rows, plan)?;

        Ok(())
    }
}

impl LogicalTablePendingRowsBatcher {
    /// For newly created tables, re-checks all row schemas and appends alter
    /// operations when additional tag columns are still missing.
    pub(in crate::batcher::logical_table) fn enqueue_alter_for_new_tables(
        table_rows: &[(String, Rows)],
        plan: &mut TableResolutionPlan,
    ) -> Result<()> {
        let created_tables: HashSet<&str> = plan
            .tables_to_create
            .iter()
            .map(|(table_name, _)| table_name.as_str())
            .collect();

        for (table_name, rows) in table_rows {
            if !created_tables.contains(table_name.as_str()) {
                continue;
            }

            let Some((region_schema, _)) = plan.region_schemas.get(table_name) else {
                continue;
            };

            let missing_columns = identify_missing_columns_from_proto(&rows.schema, region_schema)?;
            if missing_columns.is_empty()
                || plan
                    .tables_to_alter
                    .iter()
                    .any(|(existing_name, _)| existing_name == table_name)
            {
                continue;
            }

            plan.tables_to_alter
                .push((table_name.clone(), missing_columns));
        }

        Ok(())
    }
}

impl LogicalTablePendingRowsBatcher {
    /// Batch-alters tables that have missing tag columns and refreshes the
    /// in-memory schema map used for row alignment.
    pub(in crate::batcher::logical_table) async fn alter_tables_and_refresh_schemas(
        &self,
        catalog: &str,
        schema: &str,
        ctx: &QueryContextRef,
        plan: &mut TableResolutionPlan,
    ) -> Result<()> {
        if plan.tables_to_alter.is_empty() {
            return Ok(());
        }

        let alter_refs: Vec<(&str, &[String])> = plan
            .tables_to_alter
            .iter()
            .map(|(name, cols)| (name.as_str(), cols.as_slice()))
            .collect();
        {
            let _timer = PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED
                .with_label_values(&["align_batch_add_missing_columns"])
                .start_timer();
            self.schema_alterer
                .add_missing_prom_tag_columns_batch(catalog, schema, &alter_refs, ctx.clone())
                .await?;
        }

        let altered_table_results = {
            let _timer = PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED
                .with_label_values(&["align_resolve_table_after_schema_alter"])
                .start_timer();
            futures::future::join_all(plan.tables_to_alter.iter().map(|(table_name, _)| {
                self.catalog_manager
                    .table(catalog, schema, table_name, Some(ctx.as_ref()))
            }))
            .await
        };

        for ((table_name, _), table_result) in
            plan.tables_to_alter.iter().zip(altered_table_results)
        {
            let table = table_result?.with_context(|| error::UnexpectedResultSnafu {
                reason: format!(
                    "Table not found after pending batch schema alter: {}",
                    table_name
                ),
            })?;
            let table_info = table.table_info();
            let table_id = table_info.ident.table_id;
            let refreshed_region_schema = table_info.meta.schema.arrow_schema().clone();
            plan.region_schemas
                .insert(table_name.clone(), (refreshed_region_schema, table_id));
        }

        Ok(())
    }
}

impl LogicalTablePendingRowsBatcher {
    /// Converts proto rows to `RecordBatch` values aligned to resolved region
    /// schemas and returns `(table_name, table_id, batch)` tuples.
    pub(in crate::batcher::logical_table) fn build_aligned_batches(
        table_rows: &[(String, Rows)],
        region_schemas: &HashMap<String, (Arc<ArrowSchema>, u32)>,
    ) -> Result<Vec<(String, u32, RecordBatchWithTsIdx)>> {
        let mut aligned_batches = Vec::with_capacity(table_rows.len());
        for (table_name, rows) in table_rows {
            let (region_schema, table_id) =
                region_schemas.get(table_name).cloned().with_context(|| {
                    error::UnexpectedResultSnafu {
                        reason: format!("Region schema not resolved for table: {}", table_name),
                    }
                })?;

            let record_batch = {
                let _timer = PENDING_ROWS_BATCH_INGEST_STAGE_ELAPSED
                    .with_label_values(&["align_rows_to_record_batch"])
                    .start_timer();
                rows_to_aligned_record_batch(rows, region_schema.as_ref())?
            };
            aligned_batches.push((table_name.clone(), table_id, record_batch));
        }

        Ok(aligned_batches)
    }
}

#[cfg(test)]
mod tests {
    use api::v1::value::ValueData;
    use api::v1::{
        ColumnDataType, ColumnSchema, Row, RowInsertRequest, RowInsertRequests, Rows, SemanticType,
        Value,
    };
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
    use common_query::prelude::greptime_timestamp;

    use crate::batcher::logical_table::LogicalTablePendingRowsBatcher;
    use crate::batcher::logical_table::batch_convert::TableBatch;
    use crate::batcher::logical_table::flow_notifier::extract_timestamps;
    use crate::prom_row_builder::rows_to_aligned_record_batch;

    fn mock_rows(row_count: usize, schema_name: &str) -> Rows {
        Rows {
            schema: vec![ColumnSchema {
                column_name: schema_name.to_string(),
                ..Default::default()
            }],
            rows: (0..row_count).map(|_| Row { values: vec![] }).collect(),
        }
    }

    #[test]
    fn test_extract_timestamps_uses_aligned_custom_timestamp_index() {
        let rows = Rows {
            schema: vec![
                ColumnSchema {
                    column_name: greptime_timestamp().to_string(),
                    datatype: ColumnDataType::TimestampMillisecond as i32,
                    semantic_type: SemanticType::Timestamp as i32,
                    ..Default::default()
                },
                ColumnSchema {
                    column_name: "host".to_string(),
                    datatype: ColumnDataType::String as i32,
                    semantic_type: SemanticType::Tag as i32,
                    ..Default::default()
                },
                ColumnSchema {
                    column_name: "greptime_value".to_string(),
                    datatype: ColumnDataType::Float64 as i32,
                    semantic_type: SemanticType::Field as i32,
                    ..Default::default()
                },
            ],
            rows: vec![
                Row {
                    values: vec![
                        Value {
                            value_data: Some(ValueData::TimestampMillisecondValue(1000)),
                        },
                        Value {
                            value_data: Some(ValueData::StringValue("host-1".to_string())),
                        },
                        Value {
                            value_data: Some(ValueData::F64Value(1.0)),
                        },
                    ],
                },
                Row {
                    values: vec![
                        Value {
                            value_data: Some(ValueData::TimestampMillisecondValue(2000)),
                        },
                        Value {
                            value_data: Some(ValueData::StringValue("host-2".to_string())),
                        },
                        Value {
                            value_data: Some(ValueData::F64Value(2.0)),
                        },
                    ],
                },
            ],
        };
        let target_schema = ArrowSchema::new(vec![
            Field::new("host", ArrowDataType::Utf8, true),
            Field::new(
                "timestamp",
                ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("greptime_value", ArrowDataType::Float64, true),
        ]);
        let batch = rows_to_aligned_record_batch(&rows, &target_schema).unwrap();
        assert_eq!(1, batch.timestamp_index);
        let table_batch = TableBatch {
            table_name: "cpu".to_string(),
            table_id: 42,
            row_count: batch.batch.num_rows(),
            batches: vec![batch],
        };

        assert_eq!(vec![1000, 2000], extract_timestamps(&table_batch));
    }

    #[test]
    fn test_collect_non_empty_table_rows_filters_empty_payloads() {
        let requests = RowInsertRequests {
            inserts: vec![
                RowInsertRequest {
                    table_name: "cpu".to_string(),
                    rows: Some(mock_rows(2, "host")),
                },
                RowInsertRequest {
                    table_name: "mem".to_string(),
                    rows: Some(mock_rows(0, "host")),
                },
                RowInsertRequest {
                    table_name: "disk".to_string(),
                    rows: None,
                },
            ],
        };

        let (table_rows, total_rows) =
            LogicalTablePendingRowsBatcher::collect_non_empty_table_rows(requests);

        assert_eq!(2, total_rows);
        assert_eq!(1, table_rows.len());
        assert_eq!("cpu", table_rows[0].0);
        assert_eq!(2, table_rows[0].1.rows.len());
    }
}
