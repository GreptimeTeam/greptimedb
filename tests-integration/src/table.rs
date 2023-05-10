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
    use api::v1::{column, Column, ColumnDataType, InsertRequest as GrpcInsertRequest};
    use common_query::logical_plan::Expr;
    use common_query::physical_plan::DfPhysicalPlanAdapter;
    use common_query::DfPhysicalPlan;
    use common_recordbatch::adapter::RecordBatchStreamAdapter;
    use common_recordbatch::RecordBatches;
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
    use meta_client::rpc::TableName;
    use servers::query_handler::sql::SqlQueryHandler;
    use session::context::QueryContext;
    use store_api::storage::RegionNumber;
    use table::metadata::{TableInfoBuilder, TableMetaBuilder};
    use table::TableRef;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_dist_table_scan() {
        common_telemetry::init_default_ut_logging();
        let table = Arc::new(new_dist_table("test_dist_table_scan").await);
        // should scan all regions
        // select a, row_id from numbers
        let projection = Some(vec![1, 2]);
        let filters = vec![];
        let expected_output = vec![
            "+-----+--------+",
            "| a   | row_id |",
            "+-----+--------+",
            "| 0   | 1      |",
            "| 1   | 2      |",
            "| 2   | 3      |",
            "| 3   | 4      |",
            "| 4   | 5      |",
            "| 10  | 1      |",
            "| 11  | 2      |",
            "| 12  | 3      |",
            "| 13  | 4      |",
            "| 14  | 5      |",
            "| 30  | 1      |",
            "| 31  | 2      |",
            "| 32  | 3      |",
            "| 33  | 4      |",
            "| 34  | 5      |",
            "| 100 | 1      |",
            "| 101 | 2      |",
            "| 102 | 3      |",
            "| 103 | 4      |",
            "| 104 | 5      |",
            "+-----+--------+",
        ];
        exec_table_scan(table.clone(), projection, filters, 4, expected_output).await;

        // should scan only region 1
        // select a, row_id from numbers where a < 10
        let projection = Some(vec![1, 2]);
        let filters = vec![binary_expr(col("a"), Operator::Lt, lit(10)).into()];
        let expected_output = vec![
            "+---+--------+",
            "| a | row_id |",
            "+---+--------+",
            "| 0 | 1      |",
            "| 1 | 2      |",
            "| 2 | 3      |",
            "| 3 | 4      |",
            "| 4 | 5      |",
            "+---+--------+",
        ];
        exec_table_scan(table.clone(), projection, filters, 1, expected_output).await;

        // should scan region 1 and 2
        // select a, row_id from numbers where a < 15
        let projection = Some(vec![1, 2]);
        let filters = vec![binary_expr(col("a"), Operator::Lt, lit(15)).into()];
        let expected_output = vec![
            "+----+--------+",
            "| a  | row_id |",
            "+----+--------+",
            "| 0  | 1      |",
            "| 1  | 2      |",
            "| 2  | 3      |",
            "| 3  | 4      |",
            "| 4  | 5      |",
            "| 10 | 1      |",
            "| 11 | 2      |",
            "| 12 | 3      |",
            "| 13 | 4      |",
            "| 14 | 5      |",
            "+----+--------+",
        ];
        exec_table_scan(table.clone(), projection, filters, 2, expected_output).await;

        // should scan region 2 and 3
        // select a, row_id from numbers where a < 40 and a >= 10
        let projection = Some(vec![1, 2]);
        let filters = vec![and(
            binary_expr(col("a"), Operator::Lt, lit(40)),
            binary_expr(col("a"), Operator::GtEq, lit(10)),
        )
        .into()];
        let expected_output = vec![
            "+----+--------+",
            "| a  | row_id |",
            "+----+--------+",
            "| 10 | 1      |",
            "| 11 | 2      |",
            "| 12 | 3      |",
            "| 13 | 4      |",
            "| 14 | 5      |",
            "| 30 | 1      |",
            "| 31 | 2      |",
            "| 32 | 3      |",
            "| 33 | 4      |",
            "| 34 | 5      |",
            "+----+--------+",
        ];
        exec_table_scan(table.clone(), projection, filters, 2, expected_output).await;

        // should scan all regions
        // select a, row_id from numbers where a < 1000 and row_id == 1
        let projection = Some(vec![1, 2]);
        let filters = vec![and(
            binary_expr(col("a"), Operator::Lt, lit(1000)),
            binary_expr(col("row_id"), Operator::Eq, lit(1)),
        )
        .into()];
        let expected_output = vec![
            "+-----+--------+",
            "| a   | row_id |",
            "+-----+--------+",
            "| 0   | 1      |",
            "| 10  | 1      |",
            "| 30  | 1      |",
            "| 100 | 1      |",
            "+-----+--------+",
        ];
        exec_table_scan(table.clone(), projection, filters, 4, expected_output).await;
    }

    async fn exec_table_scan(
        table: TableRef,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
        expected_partitions: usize,
        expected_output: Vec<&str>,
    ) {
        let expected_output = expected_output.into_iter().join("\n");
        let table_scan = table
            .scan(projection.as_ref(), filters.as_slice(), None)
            .await
            .unwrap();
        assert_eq!(
            table_scan.output_partitioning().partition_count(),
            expected_partitions
        );

        let merge =
            CoalescePartitionsExec::new(Arc::new(DfPhysicalPlanAdapter(table_scan.clone())));

        let sort = SortExec::new(
            vec![PhysicalSortExpr {
                expr: physical_col("a", table_scan.schema().arrow_schema()).unwrap(),
                options: SortOptions::default(),
            }],
            Arc::new(merge),
        )
        .with_fetch(None);
        assert_eq!(sort.output_partitioning().partition_count(), 1);

        let session_ctx = SessionContext::new();
        let stream = sort.execute(0, session_ctx.task_ctx()).unwrap();
        let stream = Box::pin(RecordBatchStreamAdapter::try_new(stream).unwrap());

        let recordbatches = RecordBatches::try_collect(stream).await.unwrap();
        assert_eq!(recordbatches.pretty_print().unwrap(), expected_output);
    }

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
        let datanode_clients = catalog_manager.datanode_clients();

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
            region_to_datanode_mapping.insert(region_id, datanode_id);
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
            partition_manager,
            datanode_clients,
            catalog_manager.backend(),
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
        dn_instance
            .handle_insert(request, QueryContext::arc())
            .await
            .unwrap();
    }
}
