// http handlers

use std::collections::HashMap;

use axum::extract::{Extension, Query};
use common_telemetry::{metric, timer};

use crate::instance::InstanceRef;
use crate::metric::METRIC_HANDLE_SQL_ELAPSED;
use crate::server::http::{HttpResponse, JsonResponse};

/// Handler to execute sql
#[axum_macros::debug_handler]
pub async fn sql(
    Extension(instance): Extension<InstanceRef>,
    Query(params): Query<HashMap<String, String>>,
) -> HttpResponse {
    let _timer = timer!(METRIC_HANDLE_SQL_ELAPSED);
    if let Some(sql) = params.get("sql") {
        HttpResponse::Json(JsonResponse::from_output(instance.execute_sql(sql).await).await)
    } else {
        HttpResponse::Json(JsonResponse::with_error(Some(
            "sql parameter is required.".to_string(),
        )))
    }
}

/// Handler to export metrics
#[axum_macros::debug_handler]
pub async fn metrics(
    Extension(_instance): Extension<InstanceRef>,
    Query(_params): Query<HashMap<String, String>>,
) -> HttpResponse {
    if let Some(handle) = metric::try_handle() {
        HttpResponse::Text(handle.render())
    } else {
        HttpResponse::Text("Prometheus handle not initialized.".to_string())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use metrics::counter;

    use super::*;
    use crate::instance::Instance;
    use crate::server::http::JsonOutput;
    use crate::test_util;

    fn create_params() -> Query<HashMap<String, String>> {
        let mut map = HashMap::new();
        map.insert(
            "sql".to_string(),
            "select sum(number) from numbers limit 20".to_string(),
        );
        Query(map)
    }

    async fn create_extension() -> Extension<InstanceRef> {
        let (opts, _wal_dir, _data_dir) = test_util::create_tmp_dir_and_datanode_opts();
        let instance = Arc::new(Instance::new(&opts).await.unwrap());
        instance.start().await.unwrap();
        Extension(instance)
    }

    #[tokio::test]
    async fn test_sql_not_provided() {
        let extension = create_extension().await;

        let json = sql(extension, Query(HashMap::default())).await;
        match json {
            HttpResponse::Json(json) => {
                assert!(!json.success);
                assert_eq!(Some("sql parameter is required.".to_string()), json.error);
                assert!(json.output.is_none());
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn test_sql_output_rows() {
        common_telemetry::init_default_ut_logging();
        let query = create_params();
        let extension = create_extension().await;

        let json = sql(extension, query).await;

        match json {
            HttpResponse::Json(json) => {
                assert!(json.success, "{:?}", json);
                assert!(json.error.is_none());
                assert!(json.output.is_some());

                match json.output.unwrap() {
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

        let query = create_params();
        let extension = create_extension().await;
        let text = metrics(extension, query).await;

        match text {
            HttpResponse::Text(s) => assert!(s.contains("test_metrics counter")),
            _ => unreachable!(),
        }
    }
}
