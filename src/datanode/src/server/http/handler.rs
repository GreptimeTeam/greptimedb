// http handlers

use std::collections::HashMap;

use axum::extract::{Extension, Query};

use crate::instance::InstanceRef;
use crate::server::http::JsonResponse;

/// Handler to execute sql
#[axum_macros::debug_handler]
pub async fn sql(
    Extension(instance): Extension<InstanceRef>,
    Query(params): Query<HashMap<String, String>>,
) -> JsonResponse {
    if let Some(sql) = params.get("sql") {
        JsonResponse::from(instance.execute_sql(sql).await).await
    } else {
        JsonResponse::with_error(Some("sql parameter is required.".to_string()))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use query::catalog::memory;

    use super::*;
    use crate::instance::Instance;
    use crate::server::http::JsonOutput;

    fn create_params() -> Query<HashMap<String, String>> {
        let mut map = HashMap::new();
        map.insert(
            "sql".to_string(),
            "select sum(number) from numbers limit 20".to_string(),
        );
        Query(map)
    }

    fn create_extension() -> Extension<InstanceRef> {
        let catalog_list = memory::new_memory_catalog_list().unwrap();
        let instance = Arc::new(Instance::new(catalog_list));
        Extension(instance)
    }

    #[tokio::test]
    async fn test_sql_not_provided() {
        let extension = create_extension();

        let json = sql(extension, Query(HashMap::default())).await;
        assert!(!json.success);
        assert_eq!(Some("sql parameter is required.".to_string()), json.error);
        assert!(json.output.is_none());
    }

    #[tokio::test]
    async fn test_sql_output_rows() {
        let query = create_params();
        let extension = create_extension();

        let json = sql(extension, query).await;
        assert!(json.success);
        assert!(json.error.is_none());
        assert!(json.output.is_some());

        match json.output.unwrap() {
            JsonOutput::Rows(rows) => {
                assert_eq!(1, rows.len());
            }
            _ => unreachable!(),
        }
    }
}
