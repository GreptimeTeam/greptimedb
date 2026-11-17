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

//! Regression tests for the Prometheus HTTP API conversion of `count_values` results.
//!
//! `count_values` turns a sample value into a label, so the generated column must be a
//! *string* column holding Prometheus' textual form of the value: the conversion infers
//! labels from string columns and the sample value from the first numeric column. A numeric
//! label column is reported as the sample value of the series instead of a label.

use std::sync::Arc;

use catalog::RegisterTableRequest;
use catalog::memory::MemoryCatalogManager;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_query::prelude::greptime_value;
use common_recordbatch::RecordBatch;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::vectors::{Float64Vector, StringVector, TimestampMillisecondVector};
use promql_parser::parser::value::ValueType;
use query::options::QueryOptions;
use query::parser::{PromQuery, QueryLanguageParser};
use query::query_engine::QueryEngineFactory;
use servers::http::prometheus::{
    PromData, PromQueryResult, PrometheusJsonResponse, PrometheusResponse,
};
use session::context::QueryContext;
use table::Table;
use table::metadata::{FilterPushDownType, TableInfoBuilder, TableMetaBuilder};
use table::test_util::MemTable;

/// Catalog with a single metric `cv_metric` holding one series per value at `ts=1000ms`.
fn metric_catalog_manager(values: &[f64]) -> Arc<MemoryCatalogManager> {
    let catalog_list = MemoryCatalogManager::with_default_setup();
    let columns = vec![
        ColumnSchema::new("k".to_string(), ConcreteDataType::string_datatype(), false),
        ColumnSchema::new(
            "timestamp".to_string(),
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        )
        .with_time_index(true),
        ColumnSchema::new(
            greptime_value().to_string(),
            ConcreteDataType::float64_datatype(),
            true,
        ),
    ];
    let schema = Arc::new(Schema::new(columns));
    let table_meta = TableMetaBuilder::empty()
        .schema(schema.clone())
        .primary_key_indices(vec![0])
        .value_indices(vec![2])
        .next_column_id(1024)
        .build()
        .unwrap();
    let table_info = Arc::new(
        TableInfoBuilder::default()
            .table_id(3_001)
            .name("cv_metric")
            .meta(table_meta)
            .build()
            .unwrap(),
    );
    let batch = RecordBatch::new(
        schema.clone(),
        vec![
            Arc::new(StringVector::from(
                (0..values.len())
                    .map(|index| format!("k{index}"))
                    .collect::<Vec<_>>(),
            )) as _,
            Arc::new(TimestampMillisecondVector::from_vec(vec![
                1_000;
                values.len()
            ])) as _,
            Arc::new(Float64Vector::from_vec(values.to_vec())) as _,
        ],
    )
    .unwrap();
    let backing = MemTable::new_with_catalog(
        "cv_metric",
        batch,
        3_001,
        DEFAULT_CATALOG_NAME.to_string(),
        DEFAULT_SCHEMA_NAME.to_string(),
    );
    let table = Arc::new(Table::new(
        table_info,
        FilterPushDownType::Unsupported,
        backing.data_source(),
    ));

    assert!(
        catalog_list
            .register_table_sync(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: "cv_metric".to_string(),
                table_id: 3_001,
                table,
            })
            .is_ok()
    );

    catalog_list
}

#[tokio::test]
async fn count_values_generated_label_is_reported_as_a_prometheus_label() {
    // Two samples carry the value 5.0, one carries 0.5 and one carries 200.0. The query
    // evaluates at `ts=1s`, the only timestamp holding samples.
    let catalog_list = metric_catalog_manager(&[5.0, 5.0, 0.5, 200.0]);
    let query_engine = QueryEngineFactory::new(
        catalog_list,
        None,
        None,
        None,
        None,
        false,
        QueryOptions::default(),
    )
    .query_engine();

    let query_ctx = QueryContext::arc();
    let prom_query = PromQuery {
        query: r#"count_values("v", cv_metric) + 1"#.to_string(),
        start: "1".to_string(),
        end: "1".to_string(),
        step: "5s".to_string(),
        lookback: "5m".to_string(),
        alias: None,
    };
    let statement = QueryLanguageParser::parse_promql(&prom_query, &query_ctx).unwrap();
    let plan = query_engine
        .planner()
        .plan(&statement, query_ctx.clone())
        .await
        .unwrap();
    let output = query_engine.execute(plan, query_ctx).await.unwrap();

    let response =
        PrometheusJsonResponse::from_query_result(Ok(output), None, ValueType::Vector, None).await;
    let PrometheusResponse::PromData(PromData {
        result: PromQueryResult::Vector(series),
        ..
    }) = response.data
    else {
        panic!("expected a vector response");
    };

    // `v` is the generated label, so it must be reported as a label holding Prometheus'
    // textual form of the sample value (`5`, not `5.0`), and the series carries the
    // aggregation result (`count_values("v", cv_metric) + 1`) as its sample value.
    let mut actual = series
        .into_iter()
        .map(|series| {
            assert_eq!(
                series.metric.keys().collect::<Vec<_>>(),
                vec!["v"],
                "`v` must be the only label of the series"
            );
            let label = series.metric["v"].clone();
            let (timestamp, value) = series
                .value
                .expect("every series of a vector result carries a sample");
            (label, timestamp, value)
        })
        .collect::<Vec<_>>();
    actual.sort_by(|left, right| left.0.cmp(&right.0));

    assert_eq!(
        actual,
        vec![
            ("0.5".to_string(), 1.0, "2.0".to_string()),
            ("200".to_string(), 1.0, "2.0".to_string()),
            ("5".to_string(), 1.0, "3.0".to_string()),
        ]
    );
}
