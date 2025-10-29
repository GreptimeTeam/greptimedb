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

use std::collections::HashMap;

use axum::Json;
use axum::extract::{Query, State};
use axum::response::{IntoResponse, Response};
use common_meta::datanode::DatanodeStatValue;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use tonic::codegen::http;

use crate::cluster::MetaPeerClientRef;
use crate::error::{self, Result};
use crate::service::admin::util::ErrorHandler;
use crate::service::admin::{HttpHandler, util};

#[derive(Clone)]
pub struct HeartBeatHandler {
    pub meta_peer_client: MetaPeerClientRef,
}

impl HeartBeatHandler {
    async fn get_heartbeat(&self, filter: Option<&str>) -> Result<StatValues> {
        let stat_kvs = self.meta_peer_client.get_all_dn_stat_kvs().await?;
        let mut stat_vals: Vec<DatanodeStatValue> = stat_kvs.into_values().collect();

        if let Some(addr) = filter {
            stat_vals = filter_by_addr(stat_vals, addr);
        }
        Ok(StatValues { stat_vals })
    }
}

fn find_addr_filter_from_params(params: &HashMap<String, String>) -> Option<&str> {
    params.get("addr").map(|s| s.as_str())
}

/// Get the heartbeat handler.
#[axum_macros::debug_handler]
pub(crate) async fn get(
    State(handler): State<HeartBeatHandler>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let filter = find_addr_filter_from_params(&params);
    handler
        .get_heartbeat(filter)
        .await
        .map_err(ErrorHandler::new)
        .map(Json)
        .into_response()
}

/// Get the heartbeat help handler.
#[axum_macros::debug_handler]
pub(crate) async fn help() -> Response {
    r#"
    - GET /heartbeat
    - GET /heartbeat?addr=127.0.0.1:3001
    "#
    .into_response()
}

#[async_trait::async_trait]
impl HttpHandler for HeartBeatHandler {
    async fn handle(
        &self,
        path: &str,
        _: http::Method,
        params: &HashMap<String, String>,
    ) -> Result<http::Response<String>> {
        if path.ends_with("/help") {
            return util::to_text_response(
                r#"
            - GET /heartbeat
            - GET /heartbeat?addr=127.0.0.1:3001
            "#,
            );
        }

        let filter = find_addr_filter_from_params(params);
        let result = self.get_heartbeat(filter).await?.try_into()?;

        http::Response::builder()
            .status(http::StatusCode::OK)
            .body(result)
            .context(error::InvalidHttpBodySnafu)
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StatValues {
    pub stat_vals: Vec<DatanodeStatValue>,
}

impl TryFrom<StatValues> for String {
    type Error = error::Error;

    fn try_from(vals: StatValues) -> Result<Self> {
        serde_json::to_string(&vals).context(error::SerializeToJsonSnafu {
            input: format!("{vals:?}"),
        })
    }
}

fn filter_by_addr(stat_vals: Vec<DatanodeStatValue>, addr: &str) -> Vec<DatanodeStatValue> {
    stat_vals
        .into_iter()
        .filter(|stat_val| stat_val.stats.iter().any(|stat| stat.addr == addr))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::body::{Body, to_bytes};
    use axum::http::{self, Request};
    use common_meta::datanode::{DatanodeStatKey, DatanodeStatValue, Stat};
    use common_meta::kv_backend::KvBackendRef;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::rpc::store::PutRequest;
    use tower::ServiceExt;

    use crate::cluster::MetaPeerClientBuilder;
    use crate::service::admin::heartbeat::{self, HeartBeatHandler, filter_by_addr};

    #[tokio::test]
    async fn test_filter_by_addr() {
        let stat_value1 = DatanodeStatValue {
            stats: vec![
                Stat {
                    addr: "127.0.0.1:3001".to_string(),
                    timestamp_millis: 1,
                    ..Default::default()
                },
                Stat {
                    addr: "127.0.0.1:3001".to_string(),
                    timestamp_millis: 2,
                    ..Default::default()
                },
            ],
        };

        let stat_value2 = DatanodeStatValue {
            stats: vec![
                Stat {
                    addr: "127.0.0.1:3002".to_string(),
                    timestamp_millis: 3,
                    ..Default::default()
                },
                Stat {
                    addr: "127.0.0.1:3002".to_string(),
                    timestamp_millis: 4,
                    ..Default::default()
                },
                Stat {
                    addr: "127.0.0.1:3002".to_string(),
                    timestamp_millis: 5,
                    ..Default::default()
                },
            ],
        };

        let mut stat_vals = vec![stat_value1, stat_value2];
        stat_vals = filter_by_addr(stat_vals, "127.0.0.1:3002");
        assert_eq!(stat_vals.len(), 1);
        assert_eq!(stat_vals.first().unwrap().stats.len(), 3);
        assert_eq!(
            stat_vals
                .first()
                .unwrap()
                .stats
                .first()
                .unwrap()
                .timestamp_millis,
            3
        );
    }

    async fn get_body_string(resp: axum::response::Response) -> String {
        let body_bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        String::from_utf8_lossy(&body_bytes).to_string()
    }

    async fn put_stat_value(kv_backend: &KvBackendRef, node_id: u64, addr: &str) {
        let value: Vec<u8> = DatanodeStatValue {
            stats: vec![Stat {
                addr: addr.to_string(),
                timestamp_millis: 3,
                ..Default::default()
            }],
        }
        .try_into()
        .unwrap();
        let put = PutRequest::new()
            .with_key(DatanodeStatKey { node_id })
            .with_value(value);
        kv_backend.put(put).await.unwrap();
    }

    #[tokio::test]
    async fn test_get_heartbeat_with_filter() {
        common_telemetry::init_default_ut_logging();
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let meta_peer_client = MetaPeerClientBuilder::default()
            .election(None)
            .in_memory(kv_backend.clone())
            .build()
            .map(Arc::new)
            .unwrap();
        let kv_backend = kv_backend as _;
        put_stat_value(&kv_backend, 0, "127.0.0.1:3000").await;
        put_stat_value(&kv_backend, 1, "127.0.0.1:3001").await;
        put_stat_value(&kv_backend, 2, "127.0.0.1:3002").await;

        let handler = HeartBeatHandler {
            meta_peer_client: meta_peer_client.clone(),
        };
        let app = axum::Router::new()
            .route("/", axum::routing::get(heartbeat::get))
            .with_state(handler);

        let req = Request::builder()
            .uri("/?addr=127.0.0.1:3003")
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        let status = resp.status();
        let body = get_body_string(resp).await;
        assert_eq!(status, http::StatusCode::OK);
        assert_eq!(body, r#"[]"#);

        let req = Request::builder()
            .uri("/?addr=127.0.0.1:3001")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        let status = resp.status();
        let body = get_body_string(resp).await;
        assert_eq!(status, http::StatusCode::OK);
        assert_eq!(
            body,
            "[[{\"timestamp_millis\":3,\"id\":0,\"addr\":\"127.0.0.1:3001\",\"rcus\":0,\"wcus\":0,\"region_num\":0,\"region_stats\":[],\"topic_stats\":[],\"node_epoch\":0,\"datanode_workloads\":{\"types\":[]},\"gc_stat\":null}]]"
        );
    }
}
