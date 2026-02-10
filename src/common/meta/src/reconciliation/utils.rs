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
use std::fmt::{self, Display};
use std::ops::AddAssign;
use std::sync::Arc;
use std::time::Instant;

use api::v1::SemanticType;
use common_procedure::{Context as ProcedureContext, ProcedureId, watcher};
use common_telemetry::{error, warn};
use datatypes::schema::{ColumnSchema, Schema};
use futures::future::{join_all, try_join_all};
use snafu::{OptionExt, ResultExt, ensure};
use store_api::metadata::{ColumnMetadata, RegionMetadata};
use store_api::storage::consts::ReservedColumnId;
use store_api::storage::{RegionId, TableId};
use table::metadata::{TableInfo, TableMeta};
use table::table_name::TableName;
use table::table_reference::TableReference;

use crate::cache_invalidator::CacheInvalidatorRef;
use crate::error::{
    ColumnIdMismatchSnafu, ColumnNotFoundSnafu, MismatchColumnIdSnafu,
    MissingColumnInColumnMetadataSnafu, ProcedureStateReceiverNotFoundSnafu,
    ProcedureStateReceiverSnafu, Result, TimestampMismatchSnafu, UnexpectedSnafu,
    WaitProcedureSnafu,
};
use crate::key::TableMetadataManagerRef;
use crate::metrics;
use crate::node_manager::NodeManagerRef;
use crate::reconciliation::reconcile_logical_tables::ReconcileLogicalTablesProcedure;
use crate::reconciliation::reconcile_table::ReconcileTableProcedure;
use crate::reconciliation::reconcile_table::resolve_column_metadata::ResolveStrategy;

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct PartialRegionMetadata<'a> {
    pub(crate) column_metadatas: &'a [ColumnMetadata],
    pub(crate) primary_key: &'a [u32],
    pub(crate) table_id: TableId,
}

impl<'a> From<&'a RegionMetadata> for PartialRegionMetadata<'a> {
    fn from(region_metadata: &'a RegionMetadata) -> Self {
        Self {
            column_metadatas: &region_metadata.column_metadatas,
            primary_key: &region_metadata.primary_key,
            table_id: region_metadata.region_id.table_id(),
        }
    }
}

/// Checks if the column metadatas are consistent.
///
/// The column metadatas are consistent if:
/// - The column metadatas are the same.
/// - The primary key are the same.
/// - The table id of the region metadatas are the same.
///
/// ## Panic
/// Panic if region_metadatas is empty.
pub(crate) fn check_column_metadatas_consistent(
    region_metadatas: &[RegionMetadata],
) -> Option<Vec<ColumnMetadata>> {
    let is_column_metadata_consistent = region_metadatas
        .windows(2)
        .all(|w| PartialRegionMetadata::from(&w[0]) == PartialRegionMetadata::from(&w[1]));

    if !is_column_metadata_consistent {
        return None;
    }

    Some(region_metadatas[0].column_metadatas.clone())
}

/// Resolves column metadata inconsistencies among the given region metadatas
/// by using the column metadata from the metasrv as the source of truth.
///
/// All region metadatas whose column metadata differs from the given `column_metadatas`
/// will be marked for reconciliation.
///
/// Returns the region ids that need to be reconciled.
pub(crate) fn resolve_column_metadatas_with_metasrv(
    column_metadatas: &[ColumnMetadata],
    region_metadatas: &[RegionMetadata],
) -> Result<Vec<RegionId>> {
    let is_same_table = region_metadatas
        .windows(2)
        .all(|w| w[0].region_id.table_id() == w[1].region_id.table_id());

    ensure!(
        is_same_table,
        UnexpectedSnafu {
            err_msg: "Region metadatas are not from the same table"
        }
    );

    let mut regions_ids = vec![];
    for region_metadata in region_metadatas {
        if region_metadata.column_metadatas != column_metadatas {
            check_column_metadata_invariants(column_metadatas, &region_metadata.column_metadatas)?;
            regions_ids.push(region_metadata.region_id);
        }
    }
    Ok(regions_ids)
}

/// Resolves column metadata inconsistencies among the given region metadatas
/// by selecting the column metadata with the highest schema version.
///
/// This strategy assumes that at most two versions of column metadata may exist,
/// due to the poison mechanism, making the highest schema version a safe choice.
///
/// Returns the resolved column metadata and the region ids that need to be reconciled.
pub(crate) fn resolve_column_metadatas_with_latest(
    region_metadatas: &[RegionMetadata],
) -> Result<(Vec<ColumnMetadata>, Vec<RegionId>)> {
    let is_same_table = region_metadatas
        .windows(2)
        .all(|w| w[0].region_id.table_id() == w[1].region_id.table_id());

    ensure!(
        is_same_table,
        UnexpectedSnafu {
            err_msg: "Region metadatas are not from the same table"
        }
    );

    let latest_region_metadata = region_metadatas
        .iter()
        .max_by_key(|c| c.schema_version)
        .context(UnexpectedSnafu {
            err_msg: "All Region metadatas have the same schema version",
        })?;
    let latest_column_metadatas = PartialRegionMetadata::from(latest_region_metadata);

    let mut region_ids = vec![];
    for region_metadata in region_metadatas {
        if PartialRegionMetadata::from(region_metadata) != latest_column_metadatas {
            check_column_metadata_invariants(
                &latest_region_metadata.column_metadatas,
                &region_metadata.column_metadatas,
            )?;
            region_ids.push(region_metadata.region_id);
        }
    }

    // TODO(weny): verify the new column metadatas are acceptable for regions.
    Ok((latest_region_metadata.column_metadatas.clone(), region_ids))
}

/// Constructs a vector of [`ColumnMetadata`] from the provided table information.
///
/// This function maps each [`ColumnSchema`] to its corresponding [`ColumnMetadata`] by
/// determining the semantic type (Tag, Timestamp, or Field) and retrieving the column ID
/// from the `name_to_ids` mapping.
///
/// Returns an error if any column name is missing in the mapping.
pub(crate) fn build_column_metadata_from_table_info(
    column_schemas: &[ColumnSchema],
    primary_key_indexes: &[usize],
    name_to_ids: &HashMap<String, u32>,
) -> Result<Vec<ColumnMetadata>> {
    let primary_names = primary_key_indexes
        .iter()
        .map(|i| column_schemas[*i].name.as_str())
        .collect::<HashSet<_>>();

    column_schemas
        .iter()
        .map(|column_schema| {
            let column_id = *name_to_ids
                .get(column_schema.name.as_str())
                .with_context(|| UnexpectedSnafu {
                    err_msg: format!(
                        "Column name {} not found in name_to_ids",
                        column_schema.name
                    ),
                })?;

            let semantic_type = if primary_names.contains(&column_schema.name.as_str()) {
                SemanticType::Tag
            } else if column_schema.is_time_index() {
                SemanticType::Timestamp
            } else {
                SemanticType::Field
            };
            Ok(ColumnMetadata {
                column_schema: column_schema.clone(),
                semantic_type,
                column_id,
            })
        })
        .collect::<Result<Vec<_>>>()
}

/// Checks whether the schema invariants hold between the existing and new column metadata.
///
/// Invariants:
/// - Primary key (Tag) columns must exist in the new metadata, with identical name and ID.
/// - Timestamp column must remain exactly the same in name and ID.
pub(crate) fn check_column_metadata_invariants(
    new_column_metadatas: &[ColumnMetadata],
    column_metadatas: &[ColumnMetadata],
) -> Result<()> {
    let new_primary_keys = new_column_metadatas
        .iter()
        .filter(|c| c.semantic_type == SemanticType::Tag)
        .map(|c| (c.column_schema.name.as_str(), c.column_id))
        .collect::<HashMap<_, _>>();

    let old_primary_keys = column_metadatas
        .iter()
        .filter(|c| c.semantic_type == SemanticType::Tag)
        .map(|c| (c.column_schema.name.as_str(), c.column_id));

    for (name, id) in old_primary_keys {
        let column_id = new_primary_keys
            .get(name)
            .cloned()
            .context(ColumnNotFoundSnafu {
                column_name: name,
                column_id: id,
            })?;

        ensure!(
            column_id == id,
            ColumnIdMismatchSnafu {
                column_name: name,
                expected_column_id: id,
                actual_column_id: column_id,
            }
        );
    }

    let new_ts_column = new_column_metadatas
        .iter()
        .find(|c| c.semantic_type == SemanticType::Timestamp)
        .map(|c| (c.column_schema.name.as_str(), c.column_id))
        .context(UnexpectedSnafu {
            err_msg: "Timestamp column not found in new column metadata",
        })?;

    let old_ts_column = column_metadatas
        .iter()
        .find(|c| c.semantic_type == SemanticType::Timestamp)
        .map(|c| (c.column_schema.name.as_str(), c.column_id))
        .context(UnexpectedSnafu {
            err_msg: "Timestamp column not found in column metadata",
        })?;
    ensure!(
        new_ts_column == old_ts_column,
        TimestampMismatchSnafu {
            expected_column_name: old_ts_column.0,
            expected_column_id: old_ts_column.1,
            actual_column_name: new_ts_column.0,
            actual_column_id: new_ts_column.1,
        }
    );

    Ok(())
}

/// Builds a [`TableMeta`] from the provided [`ColumnMetadata`]s.
///
/// Returns an error if:
/// - Any column is missing in the `name_to_ids`(if `name_to_ids` is provided).
/// - The column id in table metadata is not the same as the column id in the column metadata.(if `name_to_ids` is provided)
/// - The table index is missing in the column metadata.
/// - The primary key or partition key columns are missing in the column metadata.
///
/// TODO(weny): add tests
pub(crate) fn build_table_meta_from_column_metadatas(
    table_id: TableId,
    table_ref: TableReference,
    table_meta: &TableMeta,
    name_to_ids: Option<HashMap<String, u32>>,
    column_metadata: &[ColumnMetadata],
) -> Result<TableMeta> {
    let column_in_column_metadata = column_metadata
        .iter()
        .map(|c| (c.column_schema.name.as_str(), c))
        .collect::<HashMap<_, _>>();
    let column_schemas = table_meta.schema.column_schemas();
    let primary_key_names = table_meta
        .primary_key_indices
        .iter()
        .map(|i| column_schemas[*i].name.as_str())
        .collect::<HashSet<_>>();
    let partition_key_names = table_meta
        .partition_key_indices
        .iter()
        .map(|i| column_schemas[*i].name.as_str())
        .collect::<HashSet<_>>();
    ensure!(
        column_metadata
            .iter()
            .any(|c| c.semantic_type == SemanticType::Timestamp),
        UnexpectedSnafu {
            err_msg: format!(
                "Missing table index in column metadata, table: {}, table_id: {}",
                table_ref, table_id
            ),
        }
    );

    if let Some(name_to_ids) = &name_to_ids {
        // Ensures all primary key and partition key exists in the column metadata.
        for column_name in primary_key_names.iter().chain(partition_key_names.iter()) {
            let column_in_column_metadata = column_in_column_metadata
                .get(column_name)
                .with_context(|| MissingColumnInColumnMetadataSnafu {
                    column_name: column_name.to_string(),
                    table_name: table_ref.to_string(),
                    table_id,
                })?;

            let column_id = *name_to_ids
                .get(*column_name)
                .with_context(|| UnexpectedSnafu {
                    err_msg: format!("column id not found in name_to_ids: {}", column_name),
                })?;
            ensure!(
                column_id == column_in_column_metadata.column_id,
                MismatchColumnIdSnafu {
                    column_name: column_name.to_string(),
                    column_id,
                    table_name: table_ref.to_string(),
                    table_id,
                }
            );
        }
    } else {
        warn!(
            "`name_to_ids` is not provided, table: {}, table_id: {}",
            table_ref, table_id
        );
    }

    let mut new_raw_table_meta = table_meta.clone();
    let primary_key_indices = &mut new_raw_table_meta.primary_key_indices;
    let partition_key_indices = &mut new_raw_table_meta.partition_key_indices;
    let value_indices = &mut new_raw_table_meta.value_indices;
    let mut columns = Vec::with_capacity(column_metadata.len());
    let column_ids = &mut new_raw_table_meta.column_ids;
    let next_column_id = &mut new_raw_table_meta.next_column_id;

    column_ids.clear();
    value_indices.clear();
    primary_key_indices.clear();
    partition_key_indices.clear();

    for (idx, col) in column_metadata.iter().enumerate() {
        if partition_key_names.contains(&col.column_schema.name.as_str()) {
            partition_key_indices.push(idx);
        }
        match col.semantic_type {
            SemanticType::Tag => {
                primary_key_indices.push(idx);
            }
            SemanticType::Field => {
                value_indices.push(idx);
            }
            SemanticType::Timestamp => {
                value_indices.push(idx);
            }
        }

        columns.push(col.column_schema.clone());
        column_ids.push(col.column_id);
    }

    *next_column_id = column_ids
        .iter()
        .filter(|id| !ReservedColumnId::is_reserved(**id))
        .max()
        .map(|max| max + 1)
        .unwrap_or(*next_column_id)
        .max(*next_column_id);

    new_raw_table_meta.schema = Arc::new(Schema::new_with_version(
        columns,
        table_meta.schema.version(),
    ));
    Ok(new_raw_table_meta)
}

/// Returns true if the logical table info needs to be updated.
///
/// The logical table only support to add columns, so we can check the length of column metadatas
/// to determine whether the logical table info needs to be updated.
pub(crate) fn need_update_logical_table_info(
    table_info: &TableInfo,
    column_metadatas: &[ColumnMetadata],
) -> bool {
    table_info.meta.schema.column_schemas().len() != column_metadatas.len()
}

/// The result of waiting for inflight subprocedures.
pub struct PartialSuccessResult<'a> {
    pub failed_procedures: Vec<&'a SubprocedureMeta>,
    pub success_procedures: Vec<&'a SubprocedureMeta>,
}

/// The result of waiting for inflight subprocedures.
pub enum WaitForInflightSubproceduresResult<'a> {
    Success(Vec<&'a SubprocedureMeta>),
    PartialSuccess(PartialSuccessResult<'a>),
}

/// Wait for inflight subprocedures.
///
/// If `fail_fast` is true, the function will return an error if any subprocedure fails.
/// Otherwise, the function will continue waiting for all subprocedures to complete.
pub(crate) async fn wait_for_inflight_subprocedures<'a>(
    procedure_ctx: &ProcedureContext,
    subprocedures: &'a [SubprocedureMeta],
    fail_fast: bool,
) -> Result<WaitForInflightSubproceduresResult<'a>> {
    let mut receivers = Vec::with_capacity(subprocedures.len());
    for subprocedure in subprocedures {
        let procedure_id = subprocedure.procedure_id();
        let receiver = procedure_ctx
            .provider
            .procedure_state_receiver(procedure_id)
            .await
            .context(ProcedureStateReceiverSnafu { procedure_id })?
            .context(ProcedureStateReceiverNotFoundSnafu { procedure_id })?;
        receivers.push((receiver, subprocedure));
    }

    let mut tasks = Vec::with_capacity(receivers.len());
    for (receiver, subprocedure) in receivers.iter_mut() {
        tasks.push(async move {
            watcher::wait(receiver).await.inspect_err(|e| {
                error!(e; "inflight subprocedure failed, parent procedure_id: {}, procedure: {}", procedure_ctx.procedure_id, subprocedure);
            })
        });
    }

    if fail_fast {
        try_join_all(tasks).await.context(WaitProcedureSnafu)?;
        return Ok(WaitForInflightSubproceduresResult::Success(
            subprocedures.iter().collect(),
        ));
    }

    // If fail_fast is false, we need to wait for all subprocedures to complete.
    let results = join_all(tasks).await;
    let failed_procedures_num = results.iter().filter(|r| r.is_err()).count();
    if failed_procedures_num == 0 {
        return Ok(WaitForInflightSubproceduresResult::Success(
            subprocedures.iter().collect(),
        ));
    }
    warn!(
        "{} inflight subprocedures failed, total: {}, parent procedure_id: {}",
        failed_procedures_num,
        subprocedures.len(),
        procedure_ctx.procedure_id
    );

    let mut failed_procedures = Vec::with_capacity(failed_procedures_num);
    let mut success_procedures = Vec::with_capacity(subprocedures.len() - failed_procedures_num);
    for (result, subprocedure) in results.into_iter().zip(subprocedures) {
        if result.is_err() {
            failed_procedures.push(subprocedure);
        } else {
            success_procedures.push(subprocedure);
        }
    }

    Ok(WaitForInflightSubproceduresResult::PartialSuccess(
        PartialSuccessResult {
            failed_procedures,
            success_procedures,
        },
    ))
}

#[derive(Clone)]
pub struct Context {
    pub node_manager: NodeManagerRef,
    pub table_metadata_manager: TableMetadataManagerRef,
    pub cache_invalidator: CacheInvalidatorRef,
}

/// Metadata for an inflight physical table subprocedure.
pub struct PhysicalTableMeta {
    pub procedure_id: ProcedureId,
    pub table_id: TableId,
    pub table_name: TableName,
}

/// Metadata for an inflight logical table subprocedure.
pub struct LogicalTableMeta {
    pub procedure_id: ProcedureId,
    pub physical_table_id: TableId,
    pub physical_table_name: TableName,
    pub logical_tables: Vec<(TableId, TableName)>,
}

/// Metadata for an inflight database subprocedure.
pub struct ReconcileDatabaseMeta {
    pub procedure_id: ProcedureId,
    pub catalog: String,
    pub schema: String,
}

/// The inflight subprocedure metadata.
pub enum SubprocedureMeta {
    PhysicalTable(PhysicalTableMeta),
    LogicalTable(LogicalTableMeta),
    Database(ReconcileDatabaseMeta),
}

impl Display for SubprocedureMeta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SubprocedureMeta::PhysicalTable(meta) => {
                write!(
                    f,
                    "ReconcilePhysicalTable(procedure_id: {}, table_id: {}, table_name: {})",
                    meta.procedure_id, meta.table_id, meta.table_name
                )
            }
            SubprocedureMeta::LogicalTable(meta) => {
                write!(
                    f,
                    "ReconcileLogicalTable(procedure_id: {}, physical_table_id: {}, physical_table_name: {}, logical_tables: {:?})",
                    meta.procedure_id,
                    meta.physical_table_id,
                    meta.physical_table_name,
                    meta.logical_tables
                )
            }
            SubprocedureMeta::Database(meta) => {
                write!(
                    f,
                    "ReconcileDatabase(procedure_id: {}, catalog: {}, schema: {})",
                    meta.procedure_id, meta.catalog, meta.schema
                )
            }
        }
    }
}

impl SubprocedureMeta {
    /// Creates a new logical table subprocedure metadata.
    pub fn new_logical_table(
        procedure_id: ProcedureId,
        physical_table_id: TableId,
        physical_table_name: TableName,
        logical_tables: Vec<(TableId, TableName)>,
    ) -> Self {
        Self::LogicalTable(LogicalTableMeta {
            procedure_id,
            physical_table_id,
            physical_table_name,
            logical_tables,
        })
    }

    /// Creates a new physical table subprocedure metadata.
    pub fn new_physical_table(
        procedure_id: ProcedureId,
        table_id: TableId,
        table_name: TableName,
    ) -> Self {
        Self::PhysicalTable(PhysicalTableMeta {
            procedure_id,
            table_id,
            table_name,
        })
    }

    /// Creates a new reconcile database subprocedure metadata.
    pub fn new_reconcile_database(
        procedure_id: ProcedureId,
        catalog: String,
        schema: String,
    ) -> Self {
        Self::Database(ReconcileDatabaseMeta {
            procedure_id,
            catalog,
            schema,
        })
    }

    /// Returns the procedure id of the subprocedure.
    pub fn procedure_id(&self) -> ProcedureId {
        match self {
            SubprocedureMeta::PhysicalTable(meta) => meta.procedure_id,
            SubprocedureMeta::LogicalTable(meta) => meta.procedure_id,
            SubprocedureMeta::Database(meta) => meta.procedure_id,
        }
    }

    /// Returns the number of tables will be reconciled.
    pub fn table_num(&self) -> usize {
        match self {
            SubprocedureMeta::PhysicalTable(_) => 1,
            SubprocedureMeta::LogicalTable(meta) => meta.logical_tables.len(),
            SubprocedureMeta::Database(_) => 0,
        }
    }

    /// Returns the number of databases will be reconciled.
    pub fn database_num(&self) -> usize {
        match self {
            SubprocedureMeta::Database(_) => 1,
            _ => 0,
        }
    }
}

/// The metrics of reconciling catalog.
#[derive(Clone, Default)]
pub struct ReconcileCatalogMetrics {
    pub succeeded_databases: usize,
    pub failed_databases: usize,
}

impl AddAssign for ReconcileCatalogMetrics {
    fn add_assign(&mut self, other: Self) {
        self.succeeded_databases += other.succeeded_databases;
        self.failed_databases += other.failed_databases;
    }
}

impl Display for ReconcileCatalogMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "succeeded_databases: {}, failed_databases: {}",
            self.succeeded_databases, self.failed_databases
        )
    }
}

impl From<WaitForInflightSubproceduresResult<'_>> for ReconcileCatalogMetrics {
    fn from(result: WaitForInflightSubproceduresResult<'_>) -> Self {
        match result {
            WaitForInflightSubproceduresResult::Success(subprocedures) => ReconcileCatalogMetrics {
                succeeded_databases: subprocedures.len(),
                failed_databases: 0,
            },
            WaitForInflightSubproceduresResult::PartialSuccess(PartialSuccessResult {
                failed_procedures,
                success_procedures,
            }) => {
                let succeeded_databases = success_procedures
                    .iter()
                    .map(|subprocedure| subprocedure.database_num())
                    .sum();
                let failed_databases = failed_procedures
                    .iter()
                    .map(|subprocedure| subprocedure.database_num())
                    .sum();
                ReconcileCatalogMetrics {
                    succeeded_databases,
                    failed_databases,
                }
            }
        }
    }
}

/// The metrics of reconciling database.
#[derive(Clone, Default)]
pub struct ReconcileDatabaseMetrics {
    pub succeeded_tables: usize,
    pub failed_tables: usize,
    pub succeeded_procedures: usize,
    pub failed_procedures: usize,
}

impl Display for ReconcileDatabaseMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "succeeded_tables: {}, failed_tables: {}, succeeded_procedures: {}, failed_procedures: {}",
            self.succeeded_tables,
            self.failed_tables,
            self.succeeded_procedures,
            self.failed_procedures
        )
    }
}

impl AddAssign for ReconcileDatabaseMetrics {
    fn add_assign(&mut self, other: Self) {
        self.succeeded_tables += other.succeeded_tables;
        self.failed_tables += other.failed_tables;
        self.succeeded_procedures += other.succeeded_procedures;
        self.failed_procedures += other.failed_procedures;
    }
}

impl From<WaitForInflightSubproceduresResult<'_>> for ReconcileDatabaseMetrics {
    fn from(result: WaitForInflightSubproceduresResult<'_>) -> Self {
        match result {
            WaitForInflightSubproceduresResult::Success(subprocedures) => {
                let table_num = subprocedures
                    .iter()
                    .map(|subprocedure| subprocedure.table_num())
                    .sum();
                ReconcileDatabaseMetrics {
                    succeeded_procedures: subprocedures.len(),
                    failed_procedures: 0,
                    succeeded_tables: table_num,
                    failed_tables: 0,
                }
            }
            WaitForInflightSubproceduresResult::PartialSuccess(PartialSuccessResult {
                failed_procedures,
                success_procedures,
            }) => {
                let succeeded_tables = success_procedures
                    .iter()
                    .map(|subprocedure| subprocedure.table_num())
                    .sum();
                let failed_tables = failed_procedures
                    .iter()
                    .map(|subprocedure| subprocedure.table_num())
                    .sum();
                ReconcileDatabaseMetrics {
                    succeeded_procedures: success_procedures.len(),
                    failed_procedures: failed_procedures.len(),
                    succeeded_tables,
                    failed_tables,
                }
            }
        }
    }
}

/// The metrics of reconciling logical tables.
#[derive(Clone)]
pub struct ReconcileLogicalTableMetrics {
    pub start_time: Instant,
    pub update_table_info_count: usize,
    pub create_tables_count: usize,
    pub column_metadata_consistent_count: usize,
    pub column_metadata_inconsistent_count: usize,
}

impl Default for ReconcileLogicalTableMetrics {
    fn default() -> Self {
        Self {
            start_time: Instant::now(),
            update_table_info_count: 0,
            create_tables_count: 0,
            column_metadata_consistent_count: 0,
            column_metadata_inconsistent_count: 0,
        }
    }
}

const CREATE_TABLES: &str = "create_tables";
const UPDATE_TABLE_INFO: &str = "update_table_info";
const COLUMN_METADATA_CONSISTENT: &str = "column_metadata_consistent";
const COLUMN_METADATA_INCONSISTENT: &str = "column_metadata_inconsistent";

impl ReconcileLogicalTableMetrics {
    /// The total number of tables that have been reconciled.
    pub fn total_table_count(&self) -> usize {
        self.create_tables_count
            + self.column_metadata_consistent_count
            + self.column_metadata_inconsistent_count
    }
}

impl Drop for ReconcileLogicalTableMetrics {
    fn drop(&mut self) {
        let procedure_name = ReconcileLogicalTablesProcedure::TYPE_NAME;
        metrics::METRIC_META_RECONCILIATION_STATS
            .with_label_values(&[procedure_name, metrics::TABLE_TYPE_LOGICAL, CREATE_TABLES])
            .inc_by(self.create_tables_count as u64);
        metrics::METRIC_META_RECONCILIATION_STATS
            .with_label_values(&[
                procedure_name,
                metrics::TABLE_TYPE_LOGICAL,
                UPDATE_TABLE_INFO,
            ])
            .inc_by(self.update_table_info_count as u64);
        metrics::METRIC_META_RECONCILIATION_STATS
            .with_label_values(&[
                procedure_name,
                metrics::TABLE_TYPE_LOGICAL,
                COLUMN_METADATA_CONSISTENT,
            ])
            .inc_by(self.column_metadata_consistent_count as u64);
        metrics::METRIC_META_RECONCILIATION_STATS
            .with_label_values(&[
                procedure_name,
                metrics::TABLE_TYPE_LOGICAL,
                COLUMN_METADATA_INCONSISTENT,
            ])
            .inc_by(self.column_metadata_inconsistent_count as u64);
    }
}

impl Display for ReconcileLogicalTableMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let elapsed = self.start_time.elapsed();
        if self.create_tables_count > 0 {
            write!(f, "create_tables_count: {}, ", self.create_tables_count)?;
        }
        if self.update_table_info_count > 0 {
            write!(
                f,
                "update_table_info_count: {}, ",
                self.update_table_info_count
            )?;
        }
        if self.column_metadata_consistent_count > 0 {
            write!(
                f,
                "column_metadata_consistent_count: {}, ",
                self.column_metadata_consistent_count
            )?;
        }
        if self.column_metadata_inconsistent_count > 0 {
            write!(
                f,
                "column_metadata_inconsistent_count: {}, ",
                self.column_metadata_inconsistent_count
            )?;
        }

        write!(
            f,
            "total_table_count: {}, elapsed: {:?}",
            self.total_table_count(),
            elapsed
        )
    }
}

/// The result of resolving column metadata.
#[derive(Clone, Copy)]
pub enum ResolveColumnMetadataResult {
    Consistent,
    Inconsistent(ResolveStrategy),
}

impl Display for ResolveColumnMetadataResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ResolveColumnMetadataResult::Consistent => write!(f, "Consistent"),
            ResolveColumnMetadataResult::Inconsistent(strategy) => {
                let strategy_str = strategy.as_ref();
                write!(f, "Inconsistent({})", strategy_str)
            }
        }
    }
}

/// The metrics of reconciling physical tables.
#[derive(Clone)]
pub struct ReconcileTableMetrics {
    /// The start time of the reconciliation.
    pub start_time: Instant,
    /// The result of resolving column metadata.
    pub resolve_column_metadata_result: Option<ResolveColumnMetadataResult>,
    /// Whether the table info has been updated.
    pub update_table_info: bool,
}

impl Drop for ReconcileTableMetrics {
    fn drop(&mut self) {
        if let Some(resolve_column_metadata_result) = self.resolve_column_metadata_result {
            match resolve_column_metadata_result {
                ResolveColumnMetadataResult::Consistent => {
                    metrics::METRIC_META_RECONCILIATION_STATS
                        .with_label_values(&[
                            ReconcileTableProcedure::TYPE_NAME,
                            metrics::TABLE_TYPE_PHYSICAL,
                            COLUMN_METADATA_CONSISTENT,
                        ])
                        .inc();
                }
                ResolveColumnMetadataResult::Inconsistent(strategy) => {
                    metrics::METRIC_META_RECONCILIATION_STATS
                        .with_label_values(&[
                            ReconcileTableProcedure::TYPE_NAME,
                            metrics::TABLE_TYPE_PHYSICAL,
                            COLUMN_METADATA_INCONSISTENT,
                        ])
                        .inc();
                    metrics::METRIC_META_RECONCILIATION_RESOLVED_COLUMN_METADATA
                        .with_label_values(&[strategy.as_ref()])
                        .inc();
                }
            }
        }
        if self.update_table_info {
            metrics::METRIC_META_RECONCILIATION_STATS
                .with_label_values(&[
                    ReconcileTableProcedure::TYPE_NAME,
                    metrics::TABLE_TYPE_PHYSICAL,
                    UPDATE_TABLE_INFO,
                ])
                .inc();
        }
    }
}

impl Default for ReconcileTableMetrics {
    fn default() -> Self {
        Self {
            start_time: Instant::now(),
            resolve_column_metadata_result: None,
            update_table_info: false,
        }
    }
}

impl Display for ReconcileTableMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let elapsed = self.start_time.elapsed();
        if let Some(resolve_column_metadata_result) = self.resolve_column_metadata_result {
            write!(
                f,
                "resolve_column_metadata_result: {}, ",
                resolve_column_metadata_result
            )?;
        }
        write!(
            f,
            "update_table_info: {}, elapsed: {:?}",
            self.update_table_info, elapsed
        )
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::collections::HashMap;
    use std::sync::Arc;

    use api::v1::SemanticType;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema, SchemaBuilder};
    use store_api::metadata::ColumnMetadata;
    use store_api::storage::RegionId;
    use table::metadata::TableMetaBuilder;
    use table::table_reference::TableReference;

    use super::*;
    use crate::ddl::test_util::region_metadata::build_region_metadata;
    use crate::error::Error;
    use crate::reconciliation::utils::check_column_metadatas_consistent;

    fn new_test_schema() -> Schema {
        let column_schemas = vec![
            ColumnSchema::new("col1", ConcreteDataType::int32_datatype(), true),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new("col2", ConcreteDataType::int32_datatype(), true),
        ];
        SchemaBuilder::try_from(column_schemas)
            .unwrap()
            .version(123)
            .build()
            .unwrap()
    }

    fn new_test_column_metadatas() -> Vec<ColumnMetadata> {
        vec![
            ColumnMetadata {
                column_schema: ColumnSchema::new("col1", ConcreteDataType::int32_datatype(), true),
                semantic_type: SemanticType::Tag,
                column_id: 0,
            },
            ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                )
                .with_time_index(true),
                semantic_type: SemanticType::Timestamp,
                column_id: 1,
            },
            ColumnMetadata {
                column_schema: ColumnSchema::new("col2", ConcreteDataType::int32_datatype(), true),
                semantic_type: SemanticType::Field,
                column_id: 2,
            },
        ]
    }

    fn new_test_raw_table_info() -> TableMeta {
        let mut table_meta_builder = TableMetaBuilder::empty();
        table_meta_builder
            .schema(Arc::new(new_test_schema()))
            .primary_key_indices(vec![0])
            .partition_key_indices(vec![2])
            .next_column_id(4)
            .build()
            .unwrap()
    }

    #[test]
    fn test_build_table_info_from_column_metadatas_identical() {
        let column_metadatas = new_test_column_metadatas();
        let table_id = 1;
        let table_ref = TableReference::full("test_catalog", "test_schema", "test_table");
        let mut table_meta = new_test_raw_table_info();
        table_meta.column_ids = vec![0, 1, 2];
        let name_to_ids = HashMap::from([
            ("col1".to_string(), 0),
            ("ts".to_string(), 1),
            ("col2".to_string(), 2),
        ]);

        let new_table_meta = build_table_meta_from_column_metadatas(
            table_id,
            table_ref,
            &table_meta,
            Some(name_to_ids),
            &column_metadatas,
        )
        .unwrap();
        assert_eq!(new_table_meta, table_meta);
    }

    #[test]
    fn test_build_table_info_from_column_metadatas() {
        let mut column_metadatas = new_test_column_metadatas();
        column_metadatas.push(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "__table_id",
                ConcreteDataType::string_datatype(),
                true,
            ),
            semantic_type: SemanticType::Tag,
            column_id: ReservedColumnId::table_id(),
        });

        let table_id = 1;
        let table_ref = TableReference::full("test_catalog", "test_schema", "test_table");
        let table_meta = new_test_raw_table_info();
        let name_to_ids = HashMap::from([
            ("col1".to_string(), 0),
            ("ts".to_string(), 1),
            ("col2".to_string(), 2),
        ]);

        let new_table_meta = build_table_meta_from_column_metadatas(
            table_id,
            table_ref,
            &table_meta,
            Some(name_to_ids),
            &column_metadatas,
        )
        .unwrap();

        assert_eq!(new_table_meta.primary_key_indices, vec![0, 3]);
        assert_eq!(new_table_meta.partition_key_indices, vec![2]);
        assert_eq!(new_table_meta.value_indices, vec![1, 2]);
        assert_eq!(new_table_meta.schema.timestamp_index(), Some(1));
        assert_eq!(
            new_table_meta.column_ids,
            vec![0, 1, 2, ReservedColumnId::table_id()]
        );
        assert_eq!(new_table_meta.next_column_id, table_meta.next_column_id);
    }

    #[test]
    fn test_build_table_info_from_column_metadatas_with_incorrect_name_to_ids() {
        let column_metadatas = new_test_column_metadatas();
        let table_id = 1;
        let table_ref = TableReference::full("test_catalog", "test_schema", "test_table");
        let table_meta = new_test_raw_table_info();
        let name_to_ids = HashMap::from([
            ("col1".to_string(), 0),
            ("ts".to_string(), 1),
            // Change column id of col2 to 3.
            ("col2".to_string(), 3),
        ]);

        let err = build_table_meta_from_column_metadatas(
            table_id,
            table_ref,
            &table_meta,
            Some(name_to_ids),
            &column_metadatas,
        )
        .unwrap_err();

        assert_matches!(err, Error::MismatchColumnId { .. });
    }

    #[test]
    fn test_build_table_info_from_column_metadatas_with_missing_time_index() {
        let mut column_metadatas = new_test_column_metadatas();
        column_metadatas.retain(|c| c.semantic_type != SemanticType::Timestamp);
        let table_id = 1;
        let table_ref = TableReference::full("test_catalog", "test_schema", "test_table");
        let table_meta = new_test_raw_table_info();
        let name_to_ids = HashMap::from([
            ("col1".to_string(), 0),
            ("ts".to_string(), 1),
            ("col2".to_string(), 2),
        ]);

        let err = build_table_meta_from_column_metadatas(
            table_id,
            table_ref,
            &table_meta,
            Some(name_to_ids),
            &column_metadatas,
        )
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("Missing table index in column metadata"),
            "err: {}",
            err
        );
    }

    #[test]
    fn test_build_table_info_from_column_metadatas_with_missing_column() {
        let mut column_metadatas = new_test_column_metadatas();
        // Remove primary key column.
        column_metadatas.retain(|c| c.column_id != 0);
        let table_id = 1;
        let table_ref = TableReference::full("test_catalog", "test_schema", "test_table");
        let table_meta = new_test_raw_table_info();
        let name_to_ids = HashMap::from([
            ("col1".to_string(), 0),
            ("ts".to_string(), 1),
            ("col2".to_string(), 2),
        ]);

        let err = build_table_meta_from_column_metadatas(
            table_id,
            table_ref,
            &table_meta,
            Some(name_to_ids.clone()),
            &column_metadatas,
        )
        .unwrap_err();
        assert_matches!(err, Error::MissingColumnInColumnMetadata { .. });

        let mut column_metadatas = new_test_column_metadatas();
        // Remove partition key column.
        column_metadatas.retain(|c| c.column_id != 2);

        let err = build_table_meta_from_column_metadatas(
            table_id,
            table_ref,
            &table_meta,
            Some(name_to_ids),
            &column_metadatas,
        )
        .unwrap_err();
        assert_matches!(err, Error::MissingColumnInColumnMetadata { .. });
    }

    #[test]
    fn test_check_column_metadatas_consistent() {
        let column_metadatas = new_test_column_metadatas();
        let region_metadata1 = build_region_metadata(RegionId::new(1024, 0), &column_metadatas);
        let region_metadata2 = build_region_metadata(RegionId::new(1024, 1), &column_metadatas);
        let result =
            check_column_metadatas_consistent(&[region_metadata1, region_metadata2]).unwrap();
        assert_eq!(result, column_metadatas);

        let region_metadata1 = build_region_metadata(RegionId::new(1025, 0), &column_metadatas);
        let region_metadata2 = build_region_metadata(RegionId::new(1024, 1), &column_metadatas);
        let result = check_column_metadatas_consistent(&[region_metadata1, region_metadata2]);
        assert!(result.is_none());
    }

    #[test]
    fn test_check_column_metadata_invariants() {
        let column_metadatas = new_test_column_metadatas();
        let mut new_column_metadatas = column_metadatas.clone();
        new_column_metadatas.push(ColumnMetadata {
            column_schema: ColumnSchema::new("col3", ConcreteDataType::int32_datatype(), true),
            semantic_type: SemanticType::Field,
            column_id: 3,
        });
        check_column_metadata_invariants(&new_column_metadatas, &column_metadatas).unwrap();
    }

    #[test]
    fn test_check_column_metadata_invariants_missing_primary_key_column_or_ts_column() {
        let column_metadatas = new_test_column_metadatas();
        let mut new_column_metadatas = column_metadatas.clone();
        new_column_metadatas.retain(|c| c.semantic_type != SemanticType::Timestamp);
        check_column_metadata_invariants(&new_column_metadatas, &column_metadatas).unwrap_err();

        let column_metadatas = new_test_column_metadatas();
        let mut new_column_metadatas = column_metadatas.clone();
        new_column_metadatas.retain(|c| c.semantic_type != SemanticType::Tag);
        check_column_metadata_invariants(&new_column_metadatas, &column_metadatas).unwrap_err();
    }

    #[test]
    fn test_check_column_metadata_invariants_mismatch_column_id() {
        let column_metadatas = new_test_column_metadatas();
        let mut new_column_metadatas = column_metadatas.clone();
        if let Some(col) = new_column_metadatas
            .iter_mut()
            .find(|c| c.semantic_type == SemanticType::Timestamp)
        {
            col.column_id = 100;
        }
        check_column_metadata_invariants(&new_column_metadatas, &column_metadatas).unwrap_err();

        let column_metadatas = new_test_column_metadatas();
        let mut new_column_metadatas = column_metadatas.clone();
        if let Some(col) = new_column_metadatas
            .iter_mut()
            .find(|c| c.semantic_type == SemanticType::Tag)
        {
            col.column_id = 100;
        }
        check_column_metadata_invariants(&new_column_metadatas, &column_metadatas).unwrap_err();
    }

    #[test]
    fn test_resolve_column_metadatas_with_use_metasrv_strategy() {
        let column_metadatas = new_test_column_metadatas();
        let region_metadata1 = build_region_metadata(RegionId::new(1024, 0), &column_metadatas);
        let mut metasrv_column_metadatas = region_metadata1.column_metadatas.clone();
        metasrv_column_metadatas.push(ColumnMetadata {
            column_schema: ColumnSchema::new("col3", ConcreteDataType::int32_datatype(), true),
            semantic_type: SemanticType::Field,
            column_id: 3,
        });
        let result =
            resolve_column_metadatas_with_metasrv(&metasrv_column_metadatas, &[region_metadata1])
                .unwrap();

        assert_eq!(result, vec![RegionId::new(1024, 0)]);
    }

    #[test]
    fn test_resolve_column_metadatas_with_use_latest_strategy() {
        let column_metadatas = new_test_column_metadatas();
        let region_metadata1 = build_region_metadata(RegionId::new(1024, 0), &column_metadatas);
        let mut new_column_metadatas = column_metadatas.clone();
        new_column_metadatas.push(ColumnMetadata {
            column_schema: ColumnSchema::new("col3", ConcreteDataType::int32_datatype(), true),
            semantic_type: SemanticType::Field,
            column_id: 3,
        });

        let mut region_metadata2 =
            build_region_metadata(RegionId::new(1024, 1), &new_column_metadatas);
        region_metadata2.schema_version = 2;

        let (resolved_column_metadatas, region_ids) =
            resolve_column_metadatas_with_latest(&[region_metadata1, region_metadata2]).unwrap();
        assert_eq!(region_ids, vec![RegionId::new(1024, 0)]);
        assert_eq!(resolved_column_metadatas, new_column_metadatas);
    }
}
