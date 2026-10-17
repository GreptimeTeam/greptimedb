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

use common_query::OutputData;
use common_recordbatch::RecordBatches;
use datafusion::arrow::array::{Array, Float64Array, StringArray, TimestampMillisecondArray};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use frontend::instance::Instance;
use query::options::QueryOptions;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::QueryContext;

use super::GreptimeDbStandaloneBuilder;
use crate::test_util::execute_sql;

const QUERY: &str = "TQL EVAL (1, 3, '1s') baseline_native";
const FILTERED_QUERY: &str = "TQL EVAL (1, 3, '1s') baseline_native > 6";
const CREATE_TABLE: &str = "CREATE TABLE baseline_native (ts TIMESTAMP(3), host STRING, val DOUBLE, TIME INDEX (ts), PRIMARY KEY (host)) ENGINE=mito";
const INSERT_ROWS: &str = "INSERT INTO baseline_native VALUES (1000, 'alpha', 1.5), (2000, 'alpha', NULL), (3000, 'alpha', 3.5), (1000, '', 4.5), (2000, '', 5.5), (3000, '', 6.5), (1000, NULL, 10.5), (2000, NULL, 11.5), (3000, NULL, 12.5), (1000, 'beta', 7.5), (3000, 'beta', 9.5)";

async fn run_query(instance: &Arc<Instance>, sql: &str) -> String {
    let output = SqlQueryHandler::do_query(instance.as_ref(), sql, QueryContext::arc())
        .await
        .remove(0)
        .unwrap();
    output.data.pretty_print().await
}

async fn query_batches(instance: &Arc<Instance>, sql: &str) -> RecordBatches {
    let output = SqlQueryHandler::do_query(instance.as_ref(), sql, QueryContext::arc())
        .await
        .remove(0)
        .unwrap();
    match output.data {
        OutputData::RecordBatches(batches) => batches,
        OutputData::Stream(stream) => RecordBatches::try_collect(stream).await.unwrap(),
        OutputData::AffectedRows(_) => panic!("expected query batches"),
    }
}

async fn create_seeded_instance(
    name: &str,
    options: QueryOptions,
) -> crate::standalone::GreptimeDbStandalone {
    let standalone = GreptimeDbStandaloneBuilder::new(name)
        .with_query_options(options)
        .build()
        .await;
    let instance = &standalone.frontend.instance;
    execute_sql(instance, CREATE_TABLE).await;
    execute_sql(instance, INSERT_ROWS).await;
    standalone
}

fn rows(batches: &RecordBatches) -> Vec<(i64, Option<String>, Option<f64>)> {
    let mut rows = Vec::new();
    for batch in batches.iter() {
        let batch = batch.df_record_batch();
        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        let host = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let val = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for index in 0..batch.num_rows() {
            rows.push((
                ts.value(index),
                (!host.is_null(index)).then(|| host.value(index).to_string()),
                (!val.is_null(index)).then(|| val.value(index)),
            ));
        }
    }
    rows.sort_by(|left, right| {
        left.0.cmp(&right.0).then_with(|| {
            left.1
                .is_none()
                .cmp(&right.1.is_none())
                .then_with(|| left.1.cmp(&right.1))
        })
    });
    rows
}

#[tokio::test(flavor = "multi_thread")]
async fn test_native_promql_standalone_executes_with_real_tql_query() {
    let disabled_options = QueryOptions {
        parallelism: 1,
        ..Default::default()
    };
    let enabled_options = QueryOptions {
        experimental_native_promql: true,
        ..disabled_options.clone()
    };
    let enabled = create_seeded_instance("native_promql_enabled", enabled_options).await;
    let ordinary = create_seeded_instance("native_promql_disabled", disabled_options).await;
    let instance = &enabled.frontend.instance;
    let baseline_instance = &ordinary.frontend.instance;

    let analyzed = run_query(instance, "TQL ANALYZE VERBOSE (1, 3, '1s') baseline_native").await;
    assert!(
        analyzed.contains("NativeInstantManipulateExec"),
        "experimental plan did not execute via native instant manipulation:\n{analyzed}"
    );
    assert!(
        analyzed.contains("SeriesScanExec: source=primary_key"),
        "native plan omitted the primary-key raw-series scan:\n{analyzed}"
    );
    let ordinary_plan = run_query(
        baseline_instance,
        "TQL ANALYZE VERBOSE (1, 3, '1s') baseline_native",
    )
    .await;
    assert!(
        !ordinary_plan.contains("NativeInstantManipulateExec"),
        "{ordinary_plan}"
    );

    let enabled_batches = query_batches(instance, QUERY).await;
    let ordinary_batches = query_batches(baseline_instance, QUERY).await;
    assert_eq!(enabled_batches.schema(), ordinary_batches.schema());
    let arrow_schema = enabled_batches.schema().arrow_schema().clone();
    assert_eq!(
        arrow_schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        vec!["ts", "host", "val"]
    );
    assert_eq!(
        arrow_schema.field(0).data_type(),
        &DataType::Timestamp(TimeUnit::Millisecond, None)
    );
    assert_eq!(arrow_schema.field(1).data_type(), &DataType::Utf8);
    assert_eq!(arrow_schema.field(2).data_type(), &DataType::Float64);
    assert_eq!(rows(&enabled_batches), rows(&ordinary_batches));
    assert_eq!(
        vec![
            (1_000, Some(String::new()), Some(4.5)),
            (1_000, Some("alpha".to_string()), Some(1.5)),
            (1_000, Some("beta".to_string()), Some(7.5)),
            (1_000, None, Some(10.5)),
            (2_000, Some(String::new()), Some(5.5)),
            (2_000, Some("alpha".to_string()), None),
            (2_000, Some("beta".to_string()), Some(7.5)),
            (2_000, None, Some(11.5)),
            (3_000, Some(String::new()), Some(6.5)),
            (3_000, Some("alpha".to_string()), Some(3.5)),
            (3_000, Some("beta".to_string()), Some(9.5)),
            (3_000, None, Some(12.5)),
        ],
        rows(&enabled_batches)
    );

    let single_eval = "TQL ANALYZE VERBOSE (1, 1, '1s') baseline_native";
    let unsupported_plan = run_query(instance, single_eval).await;
    assert!(
        !unsupported_plan.contains("NativeInstantManipulateExec"),
        "{unsupported_plan}"
    );
    let unsupported_result = query_batches(instance, "TQL EVAL (1, 1, '1s') baseline_native").await;
    assert_eq!(
        vec![
            (1_000, Some(String::new()), Some(4.5)),
            (1_000, Some("alpha".to_string()), Some(1.5)),
            (1_000, Some("beta".to_string()), Some(7.5)),
            (1_000, None, Some(10.5)),
        ],
        rows(&unsupported_result)
    );

    for test_instance in [instance, baseline_instance] {
        execute_sql(
            test_instance,
            "CREATE TABLE native_tag_parts (ts TIMESTAMP(3), host STRING, val DOUBLE, TIME INDEX (ts), PRIMARY KEY (host)) PARTITION ON COLUMNS (host) (host < 'm', host >= 'm') ENGINE=mito",
        )
        .await;
        execute_sql(
            test_instance,
            "INSERT INTO native_tag_parts VALUES (1000, 'alpha', 1), (3000, 'alpha', 3), (1000, 'zeta', 10), (3000, 'zeta', 30)",
        )
        .await;
        let table = test_instance
            .catalog_manager()
            .table("greptime", "public", "native_tag_parts", None)
            .await
            .unwrap()
            .unwrap();
        let table_id = table.table_info().ident.table_id;
        let partitions = test_instance
            .partition_manager()
            .batch_find_table_partitions(&[table_id])
            .await
            .unwrap()[&table_id]
            .clone();
        assert_eq!(2, partitions.len(), "native_tag_parts");
        execute_sql(
            test_instance,
            "CREATE TABLE native_time_parts (ts TIMESTAMP(3), host STRING, val DOUBLE, TIME INDEX (ts), PRIMARY KEY (host)) PARTITION ON COLUMNS (ts) (ts < '1970-01-01 00:00:02', ts >= '1970-01-01 00:00:02') ENGINE=mito",
        )
        .await;
        execute_sql(
            test_instance,
            "INSERT INTO native_time_parts VALUES (1000, 'alpha', 1), (3000, 'alpha', 3)",
        )
        .await;
        let table = test_instance
            .catalog_manager()
            .table("greptime", "public", "native_time_parts", None)
            .await
            .unwrap()
            .unwrap();
        let table_id = table.table_info().ident.table_id;
        let partitions = test_instance
            .partition_manager()
            .batch_find_table_partitions(&[table_id])
            .await
            .unwrap()[&table_id]
            .clone();
        assert_eq!(2, partitions.len(), "native_time_parts");
    }

    let tag_query = "TQL EVAL (1, 3, '1s') native_tag_parts";
    let tag_plan = run_query(
        instance,
        "TQL ANALYZE VERBOSE (1, 3, '1s') native_tag_parts",
    )
    .await;
    assert!(
        tag_plan.contains("NativeInstantManipulateExec"),
        "tag-partitioned plan did not use native instant manipulation:\n{tag_plan}"
    );
    let tag_enabled = query_batches(instance, tag_query).await;
    let tag_ordinary =
        query_batches(baseline_instance, "TQL EVAL (1, 3, '1s') native_tag_parts").await;
    assert_eq!(tag_enabled.schema(), tag_ordinary.schema());
    assert_eq!(rows(&tag_enabled), rows(&tag_ordinary));
    assert_eq!(
        vec![
            (1_000, Some("alpha".to_string()), Some(1.0)),
            (1_000, Some("zeta".to_string()), Some(10.0)),
            (2_000, Some("alpha".to_string()), Some(1.0)),
            (2_000, Some("zeta".to_string()), Some(10.0)),
            (3_000, Some("alpha".to_string()), Some(3.0)),
            (3_000, Some("zeta".to_string()), Some(30.0)),
        ],
        rows(&tag_enabled)
    );

    let time_query = "TQL EVAL (1, 3, '1s') native_time_parts";
    let time_plan = run_query(
        instance,
        "TQL ANALYZE VERBOSE (1, 3, '1s') native_time_parts",
    )
    .await;
    assert!(
        time_plan.contains("NativeInstantManipulateExec"),
        "time-partitioned plan did not use native instant manipulation:\n{time_plan}"
    );
    assert!(
        time_plan.contains("NativeRawMergeExec"),
        "time-partitioned plan did not merge the native raw series of its regions:\n{time_plan}"
    );
    let time_enabled = query_batches(instance, time_query).await;
    let time_ordinary =
        query_batches(baseline_instance, "TQL EVAL (1, 3, '1s') native_time_parts").await;
    assert_eq!(time_enabled.schema(), time_ordinary.schema());
    assert_eq!(rows(&time_enabled), rows(&time_ordinary));
    assert_eq!(
        vec![
            (1_000, Some("alpha".to_string()), Some(1.0)),
            (2_000, Some("alpha".to_string()), Some(1.0)),
            (3_000, Some("alpha".to_string()), Some(3.0)),
        ],
        rows(&time_enabled)
    );

    let filtered_plan = run_query(
        instance,
        "TQL ANALYZE VERBOSE (1, 3, '1s') baseline_native > 6",
    )
    .await;
    assert!(
        !filtered_plan.contains("NativeInstantManipulateExec"),
        "unsupported filtered plan unexpectedly used native instant manipulation:\n{filtered_plan}"
    );
    let filtered_enabled = query_batches(instance, FILTERED_QUERY).await;
    let filtered_ordinary = query_batches(baseline_instance, FILTERED_QUERY).await;
    assert_eq!(filtered_enabled.schema(), filtered_ordinary.schema());
    assert_eq!(rows(&filtered_enabled), rows(&filtered_ordinary));
    assert_eq!(
        vec![
            (1_000, Some("beta".to_string()), Some(7.5)),
            (1_000, None, Some(10.5)),
            (2_000, Some("beta".to_string()), Some(7.5)),
            (2_000, None, Some(11.5)),
            (3_000, Some(String::new()), Some(6.5)),
            (3_000, Some("beta".to_string()), Some(9.5)),
            (3_000, None, Some(12.5)),
        ],
        rows(&filtered_enabled)
    );
}

/// The time-partitioned case at parallelism 4: the frontend plans more than one target partition,
/// and the native instant manipulation must still consume the merged raw series of the two
/// regions that hold the one series of the table.
///
/// An ordinary instance built with the same options and seeded with the same fixture is the schema
/// oracle of this case, so the complete public schema of the result is compared instead of only
/// the plan of the enabled instance.
#[tokio::test(flavor = "multi_thread")]
async fn test_native_promql_standalone_time_partitioned_parallelism_4() {
    const CREATE_TIME_PARTS: &str = "CREATE TABLE native_time_parts (ts TIMESTAMP(3), host STRING, val DOUBLE, TIME INDEX (ts), PRIMARY KEY (host)) PARTITION ON COLUMNS (ts) (ts < '1970-01-01 00:00:02', ts >= '1970-01-01 00:00:02') ENGINE=mito";
    const INSERT_TIME_PARTS: &str =
        "INSERT INTO native_time_parts VALUES (1000, 'alpha', 1), (3000, 'alpha', 3)";
    const TIME_PARTS_QUERY: &str = "TQL EVAL (1, 3, '1s') native_time_parts";
    const TIME_PARTS_PLAN: &str = "TQL ANALYZE VERBOSE (1, 3, '1s') native_time_parts";

    let enabled = create_seeded_instance(
        "native_promql_time_parts_p4",
        QueryOptions {
            parallelism: 4,
            experimental_native_promql: true,
            ..Default::default()
        },
    )
    .await;
    let ordinary = create_seeded_instance(
        "native_promql_time_parts_p4_ordinary",
        QueryOptions {
            parallelism: 4,
            ..Default::default()
        },
    )
    .await;
    let instance = &enabled.frontend.instance;
    let ordinary_instance = &ordinary.frontend.instance;
    for target in [instance, ordinary_instance] {
        execute_sql(target, CREATE_TIME_PARTS).await;
        execute_sql(target, INSERT_TIME_PARTS).await;
    }

    let plan = run_query(instance, TIME_PARTS_PLAN).await;
    assert!(
        plan.contains("NativeInstantManipulateExec"),
        "time-partitioned plan at parallelism 4 did not use native instant manipulation:\n{plan}"
    );
    assert!(
        plan.contains("NativeRawMergeExec"),
        "time-partitioned plan at parallelism 4 did not merge the native raw series of its \
         regions:\n{plan}"
    );

    let time_batches = query_batches(instance, TIME_PARTS_QUERY).await;
    let ordinary_batches = query_batches(ordinary_instance, TIME_PARTS_QUERY).await;
    // The complete public schema, including nullability and field and schema metadata: the
    // frontend has already expanded scan-time dictionaries, so a dictionary-shaped native schema
    // must fail here instead of being normalized away.
    assert_eq!(time_batches.schema(), ordinary_batches.schema());
    assert_eq!(
        time_batches.schema().arrow_schema(),
        ordinary_batches.schema().arrow_schema()
    );
    assert_eq!(rows(&time_batches), rows(&ordinary_batches));
    assert_eq!(
        vec![
            (1_000, Some("alpha".to_string()), Some(1.0)),
            (2_000, Some("alpha".to_string()), Some(1.0)),
            (3_000, Some("alpha".to_string()), Some(3.0)),
        ],
        rows(&time_batches)
    );
}
