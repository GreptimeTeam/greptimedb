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

//! [ExtensionPlanner] implementation for distributed planner

use std::sync::Arc;

use ahash::HashMap;
use arrow_schema::SortOptions;
use async_trait::async_trait;
use catalog::CatalogManagerRef;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_telemetry::debug;
use datafusion::common::Result;
use datafusion::datasource::DefaultTableSource;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion_common::{DataFusionError, TableReference};
use datafusion_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr};
use datatypes::prelude::ConcreteDataType;
use partition::manager::{PartitionRuleManagerRef, create_partitions_from_region_routes};
use session::context::QueryContext;
use snafu::{OptionExt, ResultExt};
use store_api::storage::RegionId;
use table::metadata::TableInfo;
pub use table::metadata::TableType;
use table::table::adapter::DfTableProviderAdapter;
use table::table_name::TableName;

use crate::dist_plan::PredicateExtractor;
use crate::dist_plan::merge_scan::{MergeScanExec, MergeScanLogicalPlan};
use crate::dist_plan::merge_sort::{MergeSortExec, MergeSortLogicalPlan};
use crate::dist_plan::region_pruner::ConstraintPruner;
use crate::error::{CatalogSnafu, PartitionRuleManagerSnafu, TableNotFoundSnafu};
use crate::region_query::RegionQueryHandlerRef;

/// Planner for converting merge sort logical plan to physical plan.
///
/// `MergeSortExec` always represents the distributed merge stage. It declares
/// the required input ordering to DataFusion, so `EnforceSorting` inserts a
/// `SortExec` below it when the input `MergeScanExec` cannot preserve per-region
/// ordering, for example when one output partition may merge multiple region
/// streams.
pub struct MergeSortExtensionPlanner {}

impl MergeSortExtensionPlanner {
    fn ordering(
        session_state: &SessionState,
        merge_sort: &MergeSortLogicalPlan,
    ) -> Result<LexOrdering> {
        let ordering = merge_sort
            .expr
            .iter()
            .map(|sort_expr| {
                let physical_expr = session_state
                    .create_physical_expr(sort_expr.expr.clone(), merge_sort.input.schema())?;
                Ok(PhysicalSortExpr::new(
                    physical_expr,
                    SortOptions {
                        descending: !sort_expr.asc,
                        nulls_first: sort_expr.nulls_first,
                    },
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        LexOrdering::new(ordering).ok_or_else(|| {
            DataFusionError::Internal(
                "Expect MergeSort to have non-empty sort expressions".to_string(),
            )
        })
    }
}

#[async_trait]
impl ExtensionPlanner for MergeSortExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(merge_sort) = node.as_any().downcast_ref::<MergeSortLogicalPlan>() {
            if let LogicalPlan::Extension(ext) = &merge_sort.input.as_ref()
                && ext
                    .node
                    .as_any()
                    .downcast_ref::<MergeScanLogicalPlan>()
                    .is_some()
            {
                let input = physical_inputs.first().cloned().ok_or_else(|| {
                    DataFusionError::Internal(
                        "Expect MergeSort to have one physical input".to_string(),
                    )
                })?;
                if input.as_any().downcast_ref::<MergeScanExec>().is_none() {
                    return Err(DataFusionError::Internal(format!(
                        "Expect MergeSort's input is a MergeScanExec, found {:?}",
                        physical_inputs
                    )));
                }

                let ordering = Self::ordering(session_state, merge_sort)?;
                Ok(Some(Arc::new(MergeSortExec::new(
                    ordering,
                    input,
                    merge_sort.fetch,
                ))))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
}

pub struct DistExtensionPlanner {
    catalog_manager: CatalogManagerRef,
    partition_rule_manager: PartitionRuleManagerRef,
    region_query_handler: RegionQueryHandlerRef,
    enable_per_region_metrics: bool,
}

impl DistExtensionPlanner {
    pub fn new(
        catalog_manager: CatalogManagerRef,
        partition_rule_manager: PartitionRuleManagerRef,
        region_query_handler: RegionQueryHandlerRef,
        enable_per_region_metrics: bool,
    ) -> Self {
        Self {
            catalog_manager,
            partition_rule_manager,
            region_query_handler,
            enable_per_region_metrics,
        }
    }
}

#[async_trait]
impl ExtensionPlanner for DistExtensionPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(merge_scan) = node.as_any().downcast_ref::<MergeScanLogicalPlan>() else {
            return Ok(None);
        };

        let input_plan = merge_scan.input();
        let fallback = |logical_plan| async move {
            let optimized_plan = self.optimize_input_logical_plan(session_state, logical_plan)?;
            planner
                .create_physical_plan(&optimized_plan, session_state)
                .await
                .map(Some)
        };

        if merge_scan.is_placeholder() {
            // ignore placeholder
            return fallback(input_plan).await;
        }

        let optimized_plan = input_plan;
        let Some(table_name) = Self::extract_full_table_name(input_plan)? else {
            // no relation found in input plan, going to execute them locally
            return fallback(optimized_plan).await;
        };

        let Ok(regions) = self.get_regions(&table_name, input_plan).await else {
            // no peers found, going to execute them locally
            return fallback(optimized_plan).await;
        };

        // TODO(ruihang): generate different execution plans for different variant merge operation
        let schema = optimized_plan.schema().as_arrow();
        let query_ctx = session_state
            .config()
            .get_extension()
            .unwrap_or_else(QueryContext::arc);
        let merge_scan_plan = MergeScanExec::new(
            session_state,
            table_name,
            regions,
            input_plan.clone(),
            schema,
            self.region_query_handler.clone(),
            query_ctx,
            session_state.config().target_partitions(),
            merge_scan.partition_cols().clone(),
            merge_scan.remote_dyn_filter_producer_id(),
            self.enable_per_region_metrics,
        )?;
        Ok(Some(Arc::new(merge_scan_plan) as _))
    }
}

impl DistExtensionPlanner {
    /// Extract fully resolved table name from logical plan
    fn extract_full_table_name(plan: &LogicalPlan) -> Result<Option<TableName>> {
        let mut extractor = TableNameExtractor::default();
        let _ = plan.visit(&mut extractor)?;
        Ok(extractor.table_name)
    }

    async fn get_regions(
        &self,
        table_name: &TableName,
        logical_plan: &LogicalPlan,
    ) -> Result<Vec<RegionId>> {
        let table = self
            .catalog_manager
            .table(
                &table_name.catalog_name,
                &table_name.schema_name,
                &table_name.table_name,
                None,
            )
            .await
            .context(CatalogSnafu)?
            .with_context(|| TableNotFoundSnafu {
                table: table_name.to_string(),
            })?;

        let table_info = table.table_info();
        let (physical_table_id, physical_table_route) = self
            .partition_rule_manager
            .find_physical_table_route_with_id(table_info.table_id())
            .await
            .context(PartitionRuleManagerSnafu)?;
        let all_regions = physical_table_route
            .region_routes
            .iter()
            .map(|r| RegionId::new(table_info.table_id(), r.region.id.region_number()))
            .collect::<Vec<_>>();
        let Some(logical_partition_columns) = partition_column_types(&table_info) else {
            debug!(
                "DistExtensionPlanner: invalid partition metadata for table {}, using all regions: {:?}",
                table_name, all_regions
            );
            return Ok(all_regions);
        };
        let partition_columns = logical_partition_columns
            .iter()
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();
        debug!(
            "DistExtensionPlanner: loaded table partition metadata, table: {}, table_id: {}, partition_key_indices: {:?}, partition_columns: {:?}, all_regions: {:?}",
            table_name,
            table_info.table_id(),
            table_info.meta.partition_key_indices,
            partition_columns,
            all_regions,
        );
        if partition_columns.is_empty() {
            return Ok(all_regions);
        }
        // Extract predicates from logical plan
        let partition_expressions = match PredicateExtractor::extract_partition_expressions(
            logical_plan,
            &partition_columns,
        ) {
            Ok(expressions) => expressions,
            Err(err) => {
                common_telemetry::debug!(
                    "Failed to extract partition expressions for table {} (id: {}), using all regions: {:?}",
                    table_name,
                    table.table_info().table_id(),
                    err
                );
                return Ok(all_regions);
            }
        };

        if partition_expressions.is_empty() {
            return Ok(all_regions);
        }

        let physical_table_info = if physical_table_id == table_info.table_id() {
            table_info.clone()
        } else {
            match self
                .catalog_manager
                .table_info_by_id(physical_table_id)
                .await
            {
                Ok(Some(table_info)) if table_info.table_id() == physical_table_id => table_info,
                Ok(Some(table_info)) => {
                    debug!(
                        "DistExtensionPlanner: physical table info id mismatch for table {}: expected {}, got {}, using all regions: {:?}",
                        table_name,
                        physical_table_id,
                        table_info.table_id(),
                        all_regions
                    );
                    return Ok(all_regions);
                }
                Ok(None) => {
                    debug!(
                        "DistExtensionPlanner: physical table info not found for table {} (id: {}), using all regions: {:?}",
                        table_name, physical_table_id, all_regions
                    );
                    return Ok(all_regions);
                }
                Err(err) => {
                    debug!(
                        "DistExtensionPlanner: failed to load physical table info for table {} (id: {}): {}, using all regions: {:?}",
                        table_name, physical_table_id, err, all_regions
                    );
                    return Ok(all_regions);
                }
            }
        };
        let Some(physical_partition_columns) = partition_column_types(&physical_table_info) else {
            debug!(
                "DistExtensionPlanner: invalid physical partition metadata for table {} (id: {}), using all regions: {:?}",
                table_name, physical_table_id, all_regions
            );
            return Ok(all_regions);
        };
        let partition_column_types = physical_partition_columns
            .iter()
            .cloned()
            .collect::<HashMap<_, _>>();
        let logical_partition_column_types = logical_partition_columns
            .iter()
            .cloned()
            .collect::<HashMap<_, _>>();
        let mut predicate_column_names = std::collections::HashSet::new();
        for expression in &partition_expressions {
            expression.collect_column_names(&mut predicate_column_names);
        }
        if predicate_column_names.iter().any(|name| {
            logical_partition_column_types.get(name) != partition_column_types.get(name)
        }) {
            debug!(
                "DistExtensionPlanner: logical and physical partition metadata mismatch for table {} (physical id: {}), using all regions: {:?}",
                table_name, physical_table_id, all_regions
            );
            return Ok(all_regions);
        }

        // Get partition information for the table if partition rule manager is available
        let partitions = match create_partitions_from_region_routes(
            table_info.table_id(),
            &physical_table_route.region_routes,
        ) {
            Ok(partitions) => partitions,
            Err(err) => {
                common_telemetry::debug!(
                    "Failed to get partition information for table {}, using all regions: {:?}",
                    table_name,
                    err
                );
                return Ok(all_regions);
            }
        };
        if partitions.is_empty() {
            return Ok(all_regions);
        }
        if partitions
            .iter()
            .any(|partition| partition.partition_expr.is_none())
        {
            debug!(
                "DistExtensionPlanner: route metadata contains a missing partition expression for table {}, using all regions: {:?}",
                table_name, all_regions
            );
            return Ok(all_regions);
        }

        // Apply region pruning based on partition rules
        let pruned_regions = match ConstraintPruner::prune_regions(
            &partition_expressions,
            &partitions,
            partition_column_types,
        ) {
            Ok(regions) => regions,
            Err(err) => {
                common_telemetry::debug!(
                    "Failed to prune regions for table {}, using all regions: {:?}",
                    table_name,
                    err
                );
                return Ok(all_regions);
            }
        };

        common_telemetry::debug!(
            "Region pruning for table {}: {} partition expressions applied, pruned from {} to {} regions",
            table_name,
            partition_expressions.len(),
            all_regions.len(),
            pruned_regions.len()
        );

        Ok(pruned_regions)
    }

    /// Input logical plan is analyzed. Thus only call logical optimizer to optimize it.
    fn optimize_input_logical_plan(
        &self,
        session_state: &SessionState,
        plan: &LogicalPlan,
    ) -> Result<LogicalPlan> {
        let state = session_state.clone();
        state.optimizer().optimize(plan.clone(), &state, |_, _| {})
    }
}

fn partition_column_types(table_info: &TableInfo) -> Option<Vec<(String, ConcreteDataType)>> {
    let column_schemas = table_info.meta.schema.column_schemas();
    let mut names =
        std::collections::HashSet::with_capacity(table_info.meta.partition_key_indices.len());
    table_info
        .meta
        .partition_key_indices
        .iter()
        .map(|index| {
            let column = column_schemas.get(*index)?;
            if !names.insert(column.name.clone()) {
                return None;
            }
            Some((column.name.clone(), column.data_type.clone()))
        })
        .collect()
}

/// Visitor to extract table name from logical plan (TableScan node)
#[derive(Default)]
struct TableNameExtractor {
    pub table_name: Option<TableName>,
}

impl TreeNodeVisitor<'_> for TableNameExtractor {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: &Self::Node) -> Result<TreeNodeRecursion> {
        match node {
            LogicalPlan::TableScan(scan) => {
                if let Some(source) = scan.source.as_any().downcast_ref::<DefaultTableSource>()
                    && let Some(provider) = source
                        .table_provider
                        .as_any()
                        .downcast_ref::<DfTableProviderAdapter>()
                {
                    if provider.table().table_type() == TableType::Base {
                        let info = provider.table().table_info();
                        self.table_name = Some(TableName::new(
                            info.catalog_name.clone(),
                            info.schema_name.clone(),
                            info.name.clone(),
                        ));
                    }
                    return Ok(TreeNodeRecursion::Stop);
                }
                match &scan.table_name {
                    TableReference::Full {
                        catalog,
                        schema,
                        table,
                    } => {
                        self.table_name = Some(TableName::new(
                            catalog.to_string(),
                            schema.to_string(),
                            table.to_string(),
                        ));
                        Ok(TreeNodeRecursion::Stop)
                    }
                    // TODO(ruihang): Maybe the following two cases should not be valid
                    TableReference::Partial { schema, table } => {
                        self.table_name = Some(TableName::new(
                            DEFAULT_CATALOG_NAME.to_string(),
                            schema.to_string(),
                            table.to_string(),
                        ));
                        Ok(TreeNodeRecursion::Stop)
                    }
                    TableReference::Bare { table } => {
                        self.table_name = Some(TableName::new(
                            DEFAULT_CATALOG_NAME.to_string(),
                            DEFAULT_SCHEMA_NAME.to_string(),
                            table.to_string(),
                        ));
                        Ok(TreeNodeRecursion::Stop)
                    }
                }
            }
            _ => Ok(TreeNodeRecursion::Continue),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use api::v1::region::{RemoteDynFilterUnregister, RemoteDynFilterUpdate};
    use async_trait::async_trait;
    use catalog::memory::MemoryCatalogManager;
    use catalog::{CatalogManagerRef, RegisterTableRequest};
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use common_meta::cache::new_table_route_cache;
    use common_meta::key::TableMetadataManager;
    use common_meta::key::table_route::TableRouteValue;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::rpc::router::{Region, RegionRoute};
    use common_query::request::QueryRequest;
    use common_recordbatch::SendableRecordBatchStream;
    use datafusion::datasource::DefaultTableSource;
    use datafusion_expr::{LogicalPlan, LogicalPlanBuilder, col as df_col, lit};
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::value::Value;
    use moka::future::CacheBuilder;
    use partition::cache::new_partition_info_cache;
    use partition::expr::{PartitionExpr, col as partition_col};
    use partition::manager::PartitionRuleManager;
    use session::ReadPreference;
    use store_api::storage::RegionId;
    use table::metadata::{TableInfo, TableInfoBuilder, TableMeta, TableType};
    use table::table::adapter::DfTableProviderAdapter;
    use table::table_name::TableName;
    use table::test_util::EmptyTable;

    use super::DistExtensionPlanner;
    use crate::region_query::RegionQueryHandler;

    const LOGICAL_TABLE_ID: u32 = 1024;
    const PHYSICAL_TABLE_ID: u32 = 2048;

    struct UnusedRegionQueryHandler;

    #[async_trait]
    impl RegionQueryHandler for UnusedRegionQueryHandler {
        async fn do_get(
            &self,
            _read_preference: ReadPreference,
            _request: QueryRequest,
        ) -> crate::error::Result<SendableRecordBatchStream> {
            unreachable!("get_regions does not query regions")
        }

        async fn handle_remote_dyn_filter_update(
            &self,
            _region_id: RegionId,
            _query_id: String,
            _update: RemoteDynFilterUpdate,
        ) -> crate::error::Result<()> {
            unreachable!("get_regions does not update dynamic filters")
        }

        async fn handle_remote_dyn_filter_unregister(
            &self,
            _region_id: RegionId,
            _query_id: String,
            _unregister: RemoteDynFilterUnregister,
        ) -> crate::error::Result<()> {
            unreachable!("get_regions does not unregister dynamic filters")
        }
    }

    fn table_info(
        table_id: u32,
        name: &str,
        columns: &[&str],
        partition_keys: Vec<usize>,
    ) -> TableInfo {
        let schema = Arc::new(Schema::new(
            columns
                .iter()
                .map(|name| ColumnSchema::new(*name, ConcreteDataType::string_datatype(), true))
                .collect(),
        ));
        let meta = TableMeta {
            schema,
            primary_key_indices: vec![],
            value_indices: vec![],
            engine: "metric".to_string(),
            next_column_id: columns.len() as u32,
            options: Default::default(),
            created_on: Default::default(),
            updated_on: Default::default(),
            partition_key_indices: partition_keys,
            column_ids: (0..columns.len() as u32).collect(),
        };
        TableInfoBuilder::default()
            .table_id(table_id)
            .table_version(0)
            .name(name.to_string())
            .catalog_name(DEFAULT_CATALOG_NAME.to_string())
            .schema_name(DEFAULT_SCHEMA_NAME.to_string())
            .desc(None)
            .table_type(TableType::Base)
            .meta(meta)
            .build()
            .unwrap()
    }

    fn region_route(region_number: u32, expression: Option<PartitionExpr>) -> RegionRoute {
        RegionRoute {
            region: Region {
                id: RegionId::new(PHYSICAL_TABLE_ID, region_number),
                partition_expr: expression
                    .map(|expression| expression.as_json_str().unwrap())
                    .unwrap_or_default(),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    async fn planner_and_plan(
        physical_partition_keys: Vec<usize>,
        expressions: Vec<Option<PartitionExpr>>,
    ) -> (DistExtensionPlanner, LogicalPlan, TableName) {
        let logical_info = table_info(LOGICAL_TABLE_ID, "logical", &["host"], vec![0]);
        let physical_info = table_info(
            PHYSICAL_TABLE_ID,
            "physical",
            &["host", "rack"],
            physical_partition_keys,
        );
        let logical_table = EmptyTable::from_table_info(&logical_info);
        let physical_table = EmptyTable::from_table_info(&physical_info);
        let catalog_manager = MemoryCatalogManager::with_default_setup();
        for table in [&logical_table, &physical_table] {
            let info = table.table_info();
            catalog_manager
                .register_table_sync(RegisterTableRequest {
                    catalog: info.catalog_name.clone(),
                    schema: info.schema_name.clone(),
                    table_name: info.name.clone(),
                    table_id: info.table_id(),
                    table: table.clone(),
                })
                .unwrap();
        }

        let backend = Arc::new(MemoryKvBackend::default());
        let metadata_manager = TableMetadataManager::new(backend.clone());
        let routes = expressions
            .into_iter()
            .enumerate()
            .map(|(index, expression)| region_route(index as u32 + 1, expression))
            .collect();
        metadata_manager
            .create_table_metadata(
                physical_info,
                TableRouteValue::physical(routes),
                HashMap::new(),
            )
            .await
            .unwrap();
        metadata_manager
            .create_table_metadata(
                logical_info,
                TableRouteValue::logical(PHYSICAL_TABLE_ID),
                HashMap::new(),
            )
            .await
            .unwrap();

        let table_route_cache = Arc::new(new_table_route_cache(
            "planner-test-routes".to_string(),
            CacheBuilder::new(16).build(),
            backend.clone(),
        ));
        let partition_info_cache = Arc::new(new_partition_info_cache(
            "planner-test-partitions".to_string(),
            CacheBuilder::new(16).build(),
            table_route_cache.clone(),
        ));
        let partition_rule_manager = Arc::new(PartitionRuleManager::new(
            backend,
            table_route_cache,
            partition_info_cache,
        ));
        let (resolved_physical_id, physical_route) = partition_rule_manager
            .find_physical_table_route_with_id(PHYSICAL_TABLE_ID)
            .await
            .unwrap();
        assert_eq!(PHYSICAL_TABLE_ID, resolved_physical_id);
        let (resolved_logical_id, logical_route) = partition_rule_manager
            .find_physical_table_route_with_id(LOGICAL_TABLE_ID)
            .await
            .unwrap();
        assert_eq!(PHYSICAL_TABLE_ID, resolved_logical_id);
        assert_eq!(physical_route.region_routes, logical_route.region_routes);
        let catalog_manager: CatalogManagerRef = catalog_manager;
        let planner = DistExtensionPlanner::new(
            catalog_manager,
            partition_rule_manager,
            Arc::new(UnusedRegionQueryHandler),
            false,
        );
        let table_source = Arc::new(DefaultTableSource::new(Arc::new(
            DfTableProviderAdapter::new(logical_table),
        )));
        let plan = LogicalPlanBuilder::scan_with_filters("logical", table_source, None, vec![])
            .unwrap()
            .filter(df_col("host").eq(lit("a")))
            .unwrap()
            .build()
            .unwrap();
        (
            planner,
            plan,
            TableName::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, "logical"),
        )
    }

    fn physical_partition_expressions() -> Vec<Option<PartitionExpr>> {
        vec![
            Some(partition_col("host").lt(Value::String("m".into()))),
            Some(
                partition_col("host")
                    .gt_eq(Value::String("m".into()))
                    .and(partition_col("rack").lt(Value::String("n".into()))),
            ),
            Some(
                partition_col("host")
                    .gt_eq(Value::String("m".into()))
                    .and(partition_col("rack").gt_eq(Value::String("n".into()))),
            ),
        ]
    }

    #[tokio::test]
    async fn logical_table_pruning_uses_physical_partition_datatypes() {
        let (planner, plan, table_name) =
            planner_and_plan(vec![0, 1], physical_partition_expressions()).await;

        assert_eq!(
            vec![RegionId::new(LOGICAL_TABLE_ID, 1)],
            planner.get_regions(&table_name, &plan).await.unwrap()
        );
    }

    #[tokio::test]
    async fn missing_physical_partition_datatype_falls_back_to_all_logical_regions() {
        let (planner, plan, table_name) =
            planner_and_plan(vec![0], physical_partition_expressions()).await;

        assert_all_logical_regions(planner.get_regions(&table_name, &plan).await.unwrap());
    }

    #[tokio::test]
    async fn missing_route_partition_expression_falls_back_to_all_logical_regions() {
        let mut expressions = physical_partition_expressions();
        expressions[1] = None;
        let (planner, plan, table_name) = planner_and_plan(vec![0, 1], expressions).await;

        assert_all_logical_regions(planner.get_regions(&table_name, &plan).await.unwrap());
    }

    fn assert_all_logical_regions(mut regions: Vec<RegionId>) {
        regions.sort_unstable();
        assert_eq!(
            vec![
                RegionId::new(LOGICAL_TABLE_ID, 1),
                RegionId::new(LOGICAL_TABLE_ID, 2),
                RegionId::new(LOGICAL_TABLE_ID, 3),
            ],
            regions
        );
    }
}
