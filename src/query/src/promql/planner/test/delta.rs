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

use common_query::logical_plan::SubstraitPlanDecoder;
use common_query::prelude::set_default_prefix;
use datafusion::catalog::SchemaProvider;

use super::*;
use crate::query_engine::DefaultPlanDecoder;

fn delta_temporality_table_provider() -> (DfTableSourceProvider, QueryEngineState, Arc<MemTable>) {
    let catalog = MemoryCatalogManager::with_default_setup();
    let schema = Arc::new(Schema::new(vec![
        ColumnSchema::new(
            "series".to_string(),
            ConcreteDataType::string_datatype(),
            false,
        ),
        ColumnSchema::new(
            greptime_temporality_label().to_string(),
            ConcreteDataType::string_datatype(),
            true,
        ),
        ColumnSchema::new(
            greptime_timestamp().to_string(),
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        )
        .with_time_index(true),
        ColumnSchema::new(
            greptime_value().to_string(),
            ConcreteDataType::float64_datatype(),
            true,
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema.arrow_schema().clone(),
        vec![
            Arc::new(StringArray::from(vec![
                "delta",
                "delta",
                "delta",
                "single",
                "stale",
                "stale",
                "cumulative",
                "cumulative",
                "cumulative",
            ])),
            Arc::new(StringArray::from(vec![
                Some(GREPTIME_TEMPORALITY_DELTA),
                Some(GREPTIME_TEMPORALITY_DELTA),
                Some(GREPTIME_TEMPORALITY_DELTA),
                Some(GREPTIME_TEMPORALITY_DELTA),
                Some(GREPTIME_TEMPORALITY_DELTA),
                Some(GREPTIME_TEMPORALITY_DELTA),
                None,
                None,
                None,
            ])),
            Arc::new(TimestampMillisecondArray::from(vec![
                60_000, 120_000, 180_000, 180_000, 120_000, 180_000, 60_000, 120_000, 180_000,
            ])),
            Arc::new(Float64Array::from(vec![
                10.0,
                20.0,
                15.0,
                7.0,
                7.0,
                f64::from_bits(PROMETHEUS_STALE_NAN_BITS),
                10.0,
                20.0,
                30.0,
            ])),
        ],
    )
    .unwrap();
    let datafusion_table =
        Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch.clone()]]).unwrap());
    let table_meta = TableMetaBuilder::empty()
        .schema(schema.clone())
        .primary_key_indices(vec![0, 1])
        .value_indices(vec![3])
        .next_column_id(4)
        .build()
        .unwrap();
    let table_info = Arc::new(
        TableInfoBuilder::default()
            .name("delta_metric")
            .meta(table_meta)
            .build()
            .unwrap(),
    );
    let backing = GreptimeMemTable::new_with_catalog(
        "delta_metric",
        GreptimeRecordBatch::from_df_record_batch(schema, batch),
        4_001,
        DEFAULT_CATALOG_NAME.to_string(),
        DEFAULT_SCHEMA_NAME.to_string(),
    );
    let table = Arc::new(Table::new(
        table_info,
        FilterPushDownType::Unsupported,
        backing.data_source(),
    ));
    catalog
        .register_table_sync(RegisterTableRequest {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: "delta_metric".to_string(),
            table_id: 4_001,
            table,
        })
        .unwrap();
    let state = QueryEngineState::new(
        catalog.clone(),
        None,
        None,
        None,
        None,
        None,
        false,
        Plugins::default(),
        QueryOptions::default(),
    );
    let provider = DfTableSourceProvider::new(
        catalog,
        false,
        QueryContext::arc(),
        DummyDecoder::arc(),
        false,
    );
    (provider, state, datafusion_table)
}

#[tokio::test]
async fn rate_and_increase_select_raw_delta_math_per_series() {
    set_default_prefix(Some("custom")).unwrap();
    assert_eq!(greptime_temporality_label(), "__custom_temporality__");

    let eval_time = UNIX_EPOCH.checked_add(Duration::from_secs(180)).unwrap();
    for (function, expected_delta, expected_single) in
        [("increase", 45.0, 7.0), ("rate", 0.25, 7.0 / 180.0)]
    {
        let eval_stmt = EvalStmt {
            expr: parser::parse(&format!("{function}(delta_metric[3m])")).unwrap(),
            start: eval_time,
            end: eval_time,
            interval: Duration::from_secs(60),
            lookback_delta: Duration::from_secs(300),
        };
        let (provider, state, datafusion_table) = delta_temporality_table_provider();
        let raw = PromPlanner::stmt_to_plan(provider, &eval_stmt, &state)
            .await
            .unwrap();
        let plan = raw.display_indent_schema().to_string();
        assert!(plan.contains("prom_sum_over_time"), "{plan}");
        assert!(plan.contains(greptime_temporality_label()), "{plan}");
        let value_field = raw
            .schema()
            .fields()
            .iter()
            .find(|field| field.data_type() == &ArrowDataType::Float64)
            .unwrap()
            .name()
            .clone();
        assert!(value_field.starts_with(&format!("prom_{function}")));

        let executable = if function == "increase" {
            let context = SessionContext::new_with_state(state.session_state());
            let catalog = Arc::new(MemoryCatalogProvider::new());
            let schema = Arc::new(MemorySchemaProvider::new());
            schema
                .register_table("delta_metric".to_string(), datafusion_table)
                .unwrap();
            catalog
                .register_schema(DEFAULT_SCHEMA_NAME, schema)
                .unwrap();
            context.register_catalog("datafusion", catalog);
            let decoder = DefaultPlanDecoder::new(context.state(), &QueryContext::arc()).unwrap();
            decoder
                .decode(
                    DFLogicalSubstraitConvertor
                        .encode(&raw, DefaultSerializer)
                        .unwrap(),
                    context.state().catalog_list().clone(),
                    false,
                )
                .await
                .unwrap()
        } else {
            raw
        };
        let (_, batches) = execute(executable, &state).await;
        let mut results = HashMap::new();
        for batch in batches {
            let series = batch
                .column_by_name("series")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let temporality = batch
                .column_by_name(greptime_temporality_label())
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let values = batch
                .column_by_name(&value_field)
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                results.insert(
                    series.value(row).to_string(),
                    (
                        (!temporality.is_null(row)).then(|| temporality.value(row).to_string()),
                        values.value(row),
                    ),
                );
            }
        }
        assert_eq!(
            Some(&(Some(GREPTIME_TEMPORALITY_DELTA.to_string()), expected_delta)),
            results.get("delta")
        );
        assert_eq!(
            Some(&(
                Some(GREPTIME_TEMPORALITY_DELTA.to_string()),
                expected_single
            )),
            results.get("single")
        );
        assert_eq!(
            Some(&(
                Some(GREPTIME_TEMPORALITY_DELTA.to_string()),
                expected_single
            )),
            results.get("stale")
        );
        assert!(results.contains_key("cumulative"));
    }
}

#[tokio::test]
async fn timestamp_binary_join_normalizes_default_matching_on_mismatched_labels() {
    let eval_stmt = build_eval_stmt("timestamp(left_host_job) / right_by_job");

    let table_provider = build_test_table_provider_with_tsid_tag_fields(&[
        (
            (DEFAULT_SCHEMA_NAME.to_string(), "left_host_job".to_string()),
            2,
            1,
        ),
        (
            (DEFAULT_SCHEMA_NAME.to_string(), "right_by_job".to_string()),
            1,
            1,
        ),
    ])
    .await;
    let plan = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
        .await
        .unwrap();
    let plan_str = plan.display_indent_schema().to_string();

    assert!(plan_str.contains("__promql_match_0"), "{plan_str}");
    assert!(plan_str.contains("__promql_match_1"), "{plan_str}");
    assert!(!plan_str.contains("Boolean(false)"), "{plan_str}");
}

#[tokio::test]
async fn native_histogram_rate_ignores_delta_marker() {
    let provider =
        build_test_native_histogram_table_provider_with_marker("native_delta_metric", true).await;
    let plan = PromPlanner::stmt_to_plan(
        provider,
        &build_eval_stmt("rate(native_delta_metric[5m])"),
        &build_query_engine_state(),
    )
    .await
    .unwrap()
    .display_indent_schema()
    .to_string();

    assert!(plan.contains("prom_native_histogram_rate"), "{plan}");
    assert!(!plan.contains("prom_sum_over_time"), "{plan}");
    assert!(!plan.contains("CASE WHEN"), "{plan}");
}

#[tokio::test]
async fn mixed_range_rate_selects_delta_math_only_for_float_samples() {
    let plan = PromPlanner::stmt_to_plan(
        build_test_mixed_native_histogram_table_provider_with_marker("some_metric", true).await,
        &build_eval_stmt("rate(some_metric[5m])"),
        &build_query_engine_state(),
    )
    .await
    .unwrap()
    .display_indent_schema()
    .to_string();

    assert!(plan.contains("CASE WHEN"), "{plan}");
    assert!(plan.contains("prom_sum_over_time"), "{plan}");
    assert!(plan.contains("prom_mixed_range_float"), "{plan}");
    assert!(plan.contains("prom_mixed_range_histogram"), "{plan}");
    assert!(plan.contains(greptime_temporality_label()), "{plan}");
}

#[tokio::test]
async fn nullable_string_matchers_treat_null_as_absent() {
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "label",
        ArrowDataType::Utf8,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![
            None,
            Some(""),
            Some("delta"),
            Some("other"),
        ]))],
    )
    .unwrap();
    let table = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap());

    for (query, expected) in [
        (r#"metric{label=""}"#, vec![None, Some("")]),
        (
            r#"metric{label!="delta"}"#,
            vec![None, Some(""), Some("other")],
        ),
        (
            r#"metric{label=~".*"}"#,
            vec![None, Some(""), Some("delta"), Some("other")],
        ),
        (
            r#"metric{label!~"delta"}"#,
            vec![None, Some(""), Some("other")],
        ),
        (r#"metric{label="delta"}"#, vec![Some("delta")]),
    ] {
        let scan = LogicalPlanBuilder::scan("labels", provider_as_source(table.clone()), None)
            .unwrap()
            .build()
            .unwrap();
        let PromExpr::VectorSelector(selector) = parser::parse(query).unwrap() else {
            unreachable!()
        };
        let expressions = PromPlanner::matchers_to_expr(selector.matchers, scan.schema()).unwrap();
        let display = expressions.iter().map(ToString::to_string).join(" AND ");
        if query == r#"metric{label="delta"}"# {
            assert!(!display.contains("coalesce"), "{display}");
        } else if !expressions.is_empty() {
            assert!(display.contains("coalesce"), "{display}");
        }
        let plan = if let Some(filter) = conjunction(expressions) {
            LogicalPlanBuilder::from(scan)
                .filter(filter)
                .unwrap()
                .build()
                .unwrap()
        } else {
            scan
        };
        let (_, batches) = execute(plan, &build_query_engine_state()).await;
        let actual = batches
            .iter()
            .flat_map(|batch| {
                let labels = batch
                    .column_by_name("label")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                (0..batch.num_rows())
                    .map(move |row| (!labels.is_null(row)).then(|| labels.value(row).to_string()))
            })
            .collect::<Vec<_>>();
        assert_eq!(
            expected
                .into_iter()
                .map(|value| value.map(str::to_string))
                .collect::<Vec<_>>(),
            actual,
            "{query}"
        );
    }
}

#[tokio::test]
async fn binary_joins_normalize_missing_and_null_labels() {
    for (left_k, expected_rows) in [(Some(Some("delta")), 0), (Some(None), 1)] {
        let left = matrix_source("lhs", left_k, 1, 1.0);
        let right = matrix_source("rhs", None, 1, 2.0);
        let left_context = matrix_context("lhs", left_k);
        let right_context = matrix_context("rhs", None);
        let planner = PromPlanner {
            table_provider: build_test_table_provider_with_fields(
                &[(DEFAULT_SCHEMA_NAME.to_string(), "dummy".to_string())],
                &[],
            )
            .await,
            ctx: PromPlannerContext::default(),
            promql_annotations: None,
        };
        let joined = planner
            .join_on_non_field_columns(
                scan(&left),
                scan(&right),
                TableReference::bare("lhs"),
                TableReference::bare("rhs"),
                Some("ts".to_string()),
                Some("ts".to_string()),
                false,
                &None,
                &left_context,
                &right_context,
            )
            .unwrap();
        let (_, batches) = execute(joined, &build_query_engine_state()).await;
        assert_eq!(
            expected_rows,
            batches.iter().map(RecordBatch::num_rows).sum::<usize>()
        );

        let PromExpr::Binary(and_expr) = parser::parse("lhs and rhs").unwrap() else {
            unreachable!()
        };
        let mut planner = PromPlanner {
            table_provider: build_test_table_provider_with_fields(
                &[(DEFAULT_SCHEMA_NAME.to_string(), "dummy".to_string())],
                &[],
            )
            .await,
            ctx: PromPlannerContext::default(),
            promql_annotations: None,
        };
        let set = planner
            .set_op_on_non_field_columns(
                scan(&left),
                scan(&right),
                left_context,
                right_context,
                and_expr.op,
                &and_expr.modifier,
            )
            .unwrap();
        assert!(
            set.schema()
                .fields()
                .iter()
                .all(|field| !field.name().starts_with("__promql_match_"))
        );
        let (_, batches) = execute(set, &build_query_engine_state()).await;
        assert_eq!(
            expected_rows,
            batches.iter().map(RecordBatch::num_rows).sum::<usize>()
        );
    }
}
