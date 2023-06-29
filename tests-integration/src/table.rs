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

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::sync::Arc;

    use api::v1::column::SemanticType;
    use api::v1::{
        column, Column, ColumnDataType, InsertRequest as GrpcInsertRequest, InsertRequests,
    };
    use common_meta::table_name::TableName;
    use common_query::logical_plan::Expr;
    use common_query::physical_plan::DfPhysicalPlanAdapter;
    use common_query::{DfPhysicalPlan, Output};
    use common_recordbatch::adapter::RecordBatchStreamAdapter;
    use common_recordbatch::RecordBatches;
    use datafusion::datasource::TableProvider;
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::expressions::{col as physical_col, PhysicalSortExpr};
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::expr_fn::{and, binary_expr, col};
    use datafusion_expr::{lit, Operator};
    use datanode::instance::Instance;
    use datatypes::arrow::compute::SortOptions;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use frontend::catalog::FrontendCatalogManager;
    use frontend::table::DistTable;
    use itertools::Itertools;
    use servers::query_handler::sql::SqlQueryHandler;
    use session::context::QueryContext;
    use store_api::storage::{RegionNumber, ScanRequest};
    use table::metadata::{TableInfoBuilder, TableMetaBuilder};
    use table::table::adapter::DfTableProviderAdapter;
    use table::TableRef;

    async fn new_dist_table(test_name: &str) -> DistTable {
        let column_schemas = vec![
            ColumnSchema::new("ts", ConcreteDataType::int64_datatype(), false),
            ColumnSchema::new("a", ConcreteDataType::int32_datatype(), true),
            ColumnSchema::new("row_id", ConcreteDataType::int32_datatype(), true),
        ];
        let schema = Arc::new(Schema::new(column_schemas.clone()));

        let instance = crate::tests::create_distributed_instance(test_name).await;
        let frontend = instance.frontend();
        let catalog_manager = frontend
            .catalog_manager()
            .as_any()
            .downcast_ref::<FrontendCatalogManager>()
            .unwrap();
        let partition_manager = catalog_manager.partition_manager();

        let table_name = TableName::new("greptime", "public", "dist_numbers");

        let sql = "
            CREATE TABLE greptime.public.dist_numbers (
                ts BIGINT,
                a INT,
                row_id INT,
                TIME INDEX (ts),
            )
            PARTITION BY RANGE COLUMNS (a) (
                PARTITION r0 VALUES LESS THAN (10),
                PARTITION r1 VALUES LESS THAN (20),
                PARTITION r2 VALUES LESS THAN (50),
                PARTITION r3 VALUES LESS THAN (MAXVALUE),
            )
            ENGINE=mito";

        let _result = frontend
            .do_query(sql, QueryContext::arc())
            .await
            .remove(0)
            .unwrap();

        let table_route = partition_manager
            .find_table_route(&table_name)
            .await
            .unwrap();

        let mut region_to_datanode_mapping = HashMap::new();
        for region_route in table_route.region_routes.iter() {
            let region_id = region_route.region.id as u32;
            let datanode_id = region_route.leader_peer.as_ref().unwrap().id;
            let _ = region_to_datanode_mapping.insert(region_id, datanode_id);
        }

        let mut global_start_ts = 1;
        let regional_numbers = vec![
            (0, (0..5).collect::<Vec<i32>>()),
            (1, (10..15).collect::<Vec<i32>>()),
            (2, (30..35).collect::<Vec<i32>>()),
            (3, (100..105).collect::<Vec<i32>>()),
        ];
        for (region_number, numbers) in regional_numbers {
            let datanode_id = *region_to_datanode_mapping.get(&region_number).unwrap();
            let instance = instance.datanodes().get(&datanode_id).unwrap().clone();

            let start_ts = global_start_ts;
            global_start_ts += numbers.len() as i64;

            insert_testing_data(
                &table_name,
                instance.clone(),
                numbers,
                start_ts,
                region_number,
            )
            .await;
        }

        let meta = TableMetaBuilder::default()
            .schema(schema)
            .primary_key_indices(vec![])
            .next_column_id(1)
            .build()
            .unwrap();
        let table_info = TableInfoBuilder::default()
            .name(&table_name.table_name)
            .meta(meta)
            .build()
            .unwrap();
        DistTable::new(
            table_name,
            Arc::new(table_info),
            Arc::new(catalog_manager.clone()),
        )
    }

    async fn insert_testing_data(
        table_name: &TableName,
        dn_instance: Arc<Instance>,
        data: Vec<i32>,
        start_ts: i64,
        region_number: RegionNumber,
    ) {
        let row_count = data.len() as u32;
        let columns = vec![
            Column {
                column_name: "ts".to_string(),
                values: Some(column::Values {
                    i64_values: (start_ts..start_ts + row_count as i64).collect::<Vec<i64>>(),
                    ..Default::default()
                }),
                datatype: ColumnDataType::Int64 as i32,
                semantic_type: SemanticType::Timestamp as i32,
                ..Default::default()
            },
            Column {
                column_name: "a".to_string(),
                values: Some(column::Values {
                    i32_values: data,
                    ..Default::default()
                }),
                datatype: ColumnDataType::Int32 as i32,
                ..Default::default()
            },
            Column {
                column_name: "row_id".to_string(),
                values: Some(column::Values {
                    i32_values: (1..=row_count as i32).collect::<Vec<i32>>(),
                    ..Default::default()
                }),
                datatype: ColumnDataType::Int32 as i32,
                ..Default::default()
            },
        ];
        let request = GrpcInsertRequest {
            table_name: table_name.table_name.clone(),
            columns,
            row_count,
            region_number,
        };
        let requests = InsertRequests {
            inserts: vec![request],
        };
        let Output::AffectedRows(x) = dn_instance
            .handle_inserts(requests, &QueryContext::arc())
            .await
            .unwrap() else { unreachable!() };
        assert_eq!(x as u32, row_count);
    }
}
