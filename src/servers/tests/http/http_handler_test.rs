use std::collections::HashMap;

use axum::body::Body;
use axum::extract::{Json, Query, RawBody, State};
use common_telemetry::metric;
use metrics::counter;
use servers::http::handler as http_handler;
use servers::http::JsonOutput;
use table::test_util::MemTable;

use crate::create_testing_sql_query_handler;

#[tokio::test]
async fn test_sql_not_provided() {
    let query_handler = create_testing_sql_query_handler(MemTable::default_numbers_table());
    let Json(json) = http_handler::sql(
        State(query_handler),
        Query(http_handler::SqlQuery::default()),
    )
    .await;
    assert!(!json.success());
    assert_eq!(
        Some(&"sql parameter is required.".to_string()),
        json.error()
    );
    assert!(json.output().is_none());
}

#[tokio::test]
async fn test_sql_output_rows() {
    common_telemetry::init_default_ut_logging();

    let query = create_query();
    let query_handler = create_testing_sql_query_handler(MemTable::default_numbers_table());

    let Json(json) = http_handler::sql(State(query_handler), query).await;
    assert!(json.success(), "{:?}", json);
    assert!(json.error().is_none());
    match json.output().expect("assertion failed") {
        JsonOutput::Records(records) => {
            assert_eq!(1, records.num_rows());
        }
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn test_metrics() {
    metric::init_default_metrics_recorder();

    counter!("test_metrics", 1);

    let text = http_handler::metrics(Query(HashMap::default())).await;
    assert!(text.contains("test_metrics counter"));
}

#[tokio::test]
async fn test_scripts() {
    common_telemetry::init_default_ut_logging();

    let script = r#"
@copr(sql='select uint32s as number from numbers', args=['number'], returns=['n'])
def test(n):
    return n;
"#
    .to_string();

    let query_handler = create_testing_sql_query_handler(MemTable::default_numbers_table());
    let body = RawBody(Body::from(script.clone()));
    let invalid_query = create_invalid_script_query();
    let Json(json) = http_handler::scripts(State(query_handler.clone()), invalid_query, body).await;
    assert!(!json.success(), "{:?}", json);
    assert_eq!(json.error().unwrap(), "Invalid argument: invalid name");

    let body = RawBody(Body::from(script));
    let exec = create_script_query();
    let Json(json) = http_handler::scripts(State(query_handler), exec, body).await;
    assert!(json.success(), "{:?}", json);
    assert!(json.error().is_none());
    assert!(json.output().is_none());
}

fn create_script_query() -> Query<http_handler::ScriptQuery> {
    Query(http_handler::ScriptQuery {
        name: Some("test".to_string()),
    })
}

fn create_invalid_script_query() -> Query<http_handler::ScriptQuery> {
    Query(http_handler::ScriptQuery { name: None })
}

fn create_query() -> Query<http_handler::SqlQuery> {
    Query(http_handler::SqlQuery {
        sql: Some("select sum(uint32s) from numbers limit 20".to_string()),
        database: None,
    })
}
