use std::collections::HashMap;

use axum::extract::Query;
use axum::Extension;
use common_telemetry::metric;
use metrics::counter;
use servers::http::handler as http_handler;
use servers::http::{HttpResponse, JsonOutput};
use test_util::MemTable;

use crate::create_testing_sql_query_handler;

#[tokio::test]
async fn test_sql_not_provided() {
    let query_handler = create_testing_sql_query_handler(MemTable::default_numbers_table());
    let extension = Extension(query_handler);

    let json = http_handler::sql(extension, Query(HashMap::default())).await;
    match json {
        HttpResponse::Json(json) => {
            assert!(!json.success());
            assert_eq!(
                Some(&"sql parameter is required.".to_string()),
                json.error()
            );
            assert!(json.output().is_none());
        }
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn test_sql_output_rows() {
    common_telemetry::init_default_ut_logging();

    let query = create_query();
    let query_handler = create_testing_sql_query_handler(MemTable::default_numbers_table());
    let extension = Extension(query_handler);

    let json = http_handler::sql(extension, query).await;
    match json {
        HttpResponse::Json(json) => {
            assert!(json.success(), "{:?}", json);
            assert!(json.error().is_none());
            match json.output().expect("assertion failed") {
                JsonOutput::Rows(rows) => {
                    assert_eq!(1, rows.len());
                }
                _ => unreachable!(),
            }
        }
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn test_metrics() {
    metric::init_default_metrics_recorder();

    counter!("test_metrics", 1);

    let query = create_query();
    let query_handler = create_testing_sql_query_handler(MemTable::default_numbers_table());
    let extension = Extension(query_handler);

    let text = http_handler::metrics(extension, query).await;
    match text {
        HttpResponse::Text(s) => assert!(s.contains("test_metrics counter")),
        _ => unreachable!(),
    }
}

fn create_query() -> Query<HashMap<String, String>> {
    Query(HashMap::from([(
        "sql".to_string(),
        "select sum(uint32s) from numbers limit 20".to_string(),
    )]))
}
