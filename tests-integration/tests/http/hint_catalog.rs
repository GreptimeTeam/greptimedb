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

use std::collections::BTreeSet;

use axum::http::StatusCode;
use rstest_reuse::apply;
use serde_json::{Value, json};
use servers::http::test_helpers::TestClient;
use servers::http::{HttpOptions, HttpServerBuilder};
use session::{hints, query_options};
use store_api::mito_engine_options;
use table::requests;
use tests_integration::standalone::GreptimeDbStandaloneBuilder;
use tests_integration::test_util::MockInstanceImpl;
use urlencoding::encode;

use crate::both_deployment_cases;
use crate::http::execute_sql;

#[apply(both_deployment_cases)]
async fn test_http_hint_catalog_defaults_match_show(distributed: bool) {
    let (mut instance, client) = setup_catalog_client("hint_catalog_defaults", distributed).await;
    let catalog = fetch_catalog(&client).await;
    assert_eq!(
        find_hint(&catalog, "datafusion.optimizer.filter_null_join_keys")["default"],
        true,
        "both deployment fixtures enable the distributed analyzer"
    );
    let parallelism = find_hint(&catalog, query_options::QUERY_PARALLELISM);
    assert!(parallelism["default"].as_u64().unwrap() > 0);
    assert_eq!(
        find_hint(&catalog, requests::AUTO_CREATE_TABLE_KEY)["default"],
        true
    );

    // SHOW executes against the real frontend and query engine in both deployments.
    for name in [
        query_options::QUERY_PARALLELISM,
        query_options::QUERY_ALLOW_QUERY_FALLBACK,
        query_options::QUERY_ENABLE_REMOTE_DYNAMIC_FILTER_PUSHDOWN,
        "datafusion.optimizer.filter_null_join_keys",
        "datafusion.optimizer.max_passes",
    ] {
        let response = execute_sql(&client, &format!("SHOW {name}")).await;
        assert_eq!(response.status(), StatusCode::OK, "SHOW {name}");
        let body = response.json::<Value>().await;
        assert_eq!(
            single_cell(&body, 0),
            &json!(find_hint(&catalog, name)["default"].to_string()),
            "SHOW {name}: {body}"
        );
    }
    // An explicit request option must override the distributed analyzer default.
    let response = client
        .get("/v1/sql?sql=SHOW%20datafusion.optimizer.filter_null_join_keys")
        .header(
            hints::HINTS_KEY,
            "datafusion.optimizer.filter_null_join_keys=false",
        )
        .send()
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.json::<Value>().await;
    assert_eq!(
        single_cell(&body, 0),
        "false",
        "explicit optimizer override: {body}"
    );
    instance.shutdown().await;
}

#[apply(both_deployment_cases)]
async fn test_http_hint_catalog_names_and_values_are_accepted(distributed: bool) {
    let (mut instance, client) = setup_catalog_client("hint_catalog_inputs", distributed).await;
    let catalog = fetch_catalog(&client).await;
    for name in [
        query_options::QUERY_PARALLELISM,
        query_options::QUERY_ALLOW_QUERY_FALLBACK,
        query_options::QUERY_ENABLE_REMOTE_DYNAMIC_FILTER_PUSHDOWN,
    ] {
        let entry = find_hint(&catalog, name);
        let names = std::iter::once(name).chain(
            entry["aliases"]
                .as_array()
                .unwrap()
                .iter()
                .map(|alias| alias.as_str().unwrap()),
        );
        let values = entry.get("values").expect("values field");
        let values = if name == query_options::QUERY_PARALLELISM {
            assert!(values.is_null(), "{name}: {entry}");
            vec!["1", "2"]
        } else {
            assert_eq!(entry["type"], "boolean", "{name}");
            assert_eq!(values, &json!(["true", "false"]), "{name}");
            values
                .as_array()
                .unwrap()
                .iter()
                .map(|value| value.as_str().unwrap())
                .collect()
        };
        for input_name in names {
            for value in &values {
                let sql = format!("SHOW {name}");
                let response = client
                    .get(&format!("/v1/sql?sql={}", encode(&sql)))
                    .header(hints::HINTS_KEY, format!("{input_name}={value}"))
                    .send()
                    .await;
                assert_eq!(
                    response.status(),
                    StatusCode::OK,
                    "hint {input_name}={value}"
                );
                let body = response.json::<Value>().await;
                assert_eq!(
                    single_cell(&body, 0),
                    *value,
                    "hint {input_name}={value}; SHOW {name}: {body}"
                );

                let sql = format!("SET {input_name} = {value}; SHOW {name}");
                let response = execute_sql(&client, &sql).await;
                assert_eq!(response.status(), StatusCode::OK, "{sql}");
                let body = response.json::<Value>().await;
                assert_eq!(single_cell(&body, 1), *value, "{sql}: {body}");
            }
        }
    }
    instance.shutdown().await;
}

#[apply(both_deployment_cases)]
async fn test_http_hint_catalog_metadata_and_enterprise_visibility(distributed: bool) {
    let (mut instance, client) = setup_catalog_client("hint_catalog_metadata", distributed).await;
    let catalog = fetch_catalog(&client).await;
    let entries: Vec<_> = catalog_entries(&catalog).collect();
    let names: BTreeSet<_> = entries
        .iter()
        .map(|entry| entry["name"].as_str().unwrap())
        .collect();
    assert_eq!(entries.len(), names.len(), "duplicate hint names");
    assert!(catalog.get("legacy_headers_deprecation").is_none());
    assert_eq!(
        catalog["groups"]
            .as_array()
            .unwrap()
            .iter()
            .map(|group| group["name"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["query", "optimizer", "ingestion"]
    );
    for (name, group) in [
        (servers::http::jaeger::JAEGER_QUERY_TABLE_NAME_KEY, "query"),
        (requests::TTL_KEY, "ingestion"),
        (mito_engine_options::MERGE_MODE_KEY, "ingestion"),
        ("datafusion.optimizer.max_passes", "optimizer"),
    ] {
        assert_group(&catalog, name, group);
    }
    if cfg!(feature = "enterprise") {
        assert_group(&catalog, hints::READ_PREFERENCE_HINT, "query");
    }
    for entry in &entries {
        assert!(
            entry["applies_to"]
                .as_array()
                .is_some_and(|protocols| !protocols.is_empty()),
            "{entry}"
        );
        assert!(entry.get("legacy_headers").is_none());
    }
    for (name, values) in [
        (
            mito_engine_options::MERGE_MODE_KEY,
            json!(["last_row", "last_non_null"]),
        ),
        (mito_engine_options::COMPACTION_TYPE, json!(["twcs"])),
    ] {
        let entry = find_hint(&catalog, name);
        assert_eq!(entry["type"], "enum", "{name}");
        assert_eq!(entry["values"], values, "{name}");
    }
    let pattern_names: Vec<_> = entries
        .iter()
        .filter(|entry| entry["name_is_pattern"] == true)
        .map(|entry| entry["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        pattern_names,
        [
            "greptime.semantic.entity.<type>.id",
            "greptime.semantic.entity.<type>.descriptive",
            "greptime.semantic.entity.<type>.scope",
        ]
    );
    assert!(!names.contains(requests::SEMANTIC_ENTITY_SERVICE_ID));
    for name in pattern_names {
        let entry = find_hint(&catalog, name);
        assert_eq!(entry["type"], "string");
        assert!(entry["values"].is_null());
        assert!(
            entry["applies_to"]
                .as_array()
                .unwrap()
                .contains(&json!("influxdb"))
        );
    }
    assert_eq!(
        find_hint(&catalog, requests::TTL_KEY)["name_is_pattern"],
        false
    );
    let ttl = find_hint(&catalog, requests::TTL_KEY);
    assert!(ttl.get("values").expect("TTL values field").is_null());
    assert!(ttl.get("default").expect("TTL default field").is_null());
    assert!(
        ttl["default_description"]
            .as_str()
            .is_some_and(|text| !text.is_empty())
    );

    // Checking this crate's feature also catches missing frontend-to-server forwarding.
    assert_eq!(
        names.contains(hints::READ_PREFERENCE_HINT),
        cfg!(feature = "enterprise")
    );
    if cfg!(feature = "enterprise") {
        let entry = find_hint(&catalog, hints::READ_PREFERENCE_HINT);
        assert_eq!(entry["type"], "enum");
        assert_eq!(entry["default"], "leader");
        for value in entry["values"].as_array().unwrap() {
            value
                .as_str()
                .unwrap()
                .parse::<session::ReadPreference>()
                .unwrap();
        }
        assert_eq!(entry["values"], json!(session::READ_PREFERENCE_VALUES));
    } else {
        let body = catalog.to_string();
        assert!(!body.contains(hints::READ_PREFERENCE_HINT));
        assert!(!body.contains("read replicas"));
        assert!(!body.contains("x-greptime-read-preference"));
    }
    instance.shutdown().await;
}

#[apply(both_deployment_cases)]
async fn test_http_hint_catalog_request_hints_do_not_change_defaults(distributed: bool) {
    let (mut instance, client) = setup_catalog_client("hint_catalog_overrides", distributed).await;
    let expected = catalog_defaults(&fetch_catalog(&client).await);
    let response = client
        .get("/v1/hints")
        .header(
            hints::HINTS_KEY,
            format!(
                "{}=1,{}=false",
                query_options::QUERY_PARALLELISM,
                requests::AUTO_CREATE_TABLE_KEY
            ),
        )
        .send()
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.json::<Value>().await;
    assert_eq!(catalog_defaults(&body), expected);
    instance.shutdown().await;
}

#[apply(both_deployment_cases)]
async fn test_http_hint_catalog_rejects_invalid_query_hints(distributed: bool) {
    let (mut instance, client) = setup_catalog_client("hint_catalog_invalid", distributed).await;
    for input in [
        format!("{}=0", query_options::QUERY_PARALLELISM),
        format!(
            "{}=not_a_boolean",
            query_options::QUERY_ALLOW_QUERY_FALLBACK
        ),
        "query.unknown_option=true".to_string(),
        "datafusion.optimizer.unknown_option=true".to_string(),
        "datafusion.optimizer.max_passes=not_an_integer".to_string(),
    ] {
        let response = client
            .get("/v1/sql?sql=SELECT%201")
            .header(hints::HINTS_KEY, &input)
            .send()
            .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "hint {input}");
    }
    instance.shutdown().await;
}

#[apply(both_deployment_cases)]
async fn test_http_hint_catalog_conflicting_aliases_prevent_execution(distributed: bool) {
    let (mut instance, client) = setup_catalog_client("hint_catalog_conflict", distributed).await;
    let catalog = fetch_catalog(&client).await;
    let entry = find_hint(&catalog, query_options::QUERY_PARALLELISM);
    let alias = entry["aliases"][0].as_str().expect("parallelism alias");
    let sql = "CREATE TABLE rejected_hints (ts TIMESTAMP TIME INDEX)";
    let input = format!("{}=1,{alias}=2", query_options::QUERY_PARALLELISM);
    let response = client
        .get(&format!("/v1/sql?sql={}", encode(sql)))
        .header(hints::HINTS_KEY, input)
        .send()
        .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    // Request validation must finish before the statement can mutate the database.
    let response = execute_sql(&client,
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'rejected_hints'"
    ).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.json::<Value>().await;
    assert_eq!(
        single_cell(&body, 0),
        &json!(0),
        "rejected request executed DDL: {body}"
    );
    let response = execute_sql(
        &client,
        &format!("SHOW {}", query_options::QUERY_PARALLELISM),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.json::<Value>().await;
    assert_eq!(
        single_cell(&body, 0),
        &json!(entry["default"].to_string()),
        "default changed after rejected request"
    );
    instance.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_http_hint_catalog_auto_create_default() {
    common_telemetry::init_default_ut_logging();
    // Only the configured default changes; the HTTP request and table are identical.
    for auto_create_table in [false, true] {
        let mut instance = GreptimeDbStandaloneBuilder::new("hint_catalog_auto_create")
            .with_auto_create_table(auto_create_table)
            .build()
            .await;
        let frontend = instance.fe_instance().clone();
        let server = HttpServerBuilder::new(HttpOptions::default())
            .with_sql_handler(frontend.clone())
            .with_influxdb_handler(frontend)
            .build();
        let client = TestClient::new(server.build(server.make_app()).unwrap()).await;
        let catalog = fetch_catalog(&client).await;
        let entry = find_hint(&catalog, requests::AUTO_CREATE_TABLE_KEY);
        assert_eq!(entry["default"], auto_create_table);
        assert_eq!(entry["values"], json!(["true", "false"]));

        let response = client
            .post("/v1/influxdb/write?db=public")
            .body("hint_catalog_auto_create,host=a value=1 1700000000000000000")
            .send()
            .await;
        assert_eq!(response.status().is_success(), auto_create_table);
        if !auto_create_table {
            let response = client
                .post("/v1/influxdb/write?db=public")
                .header(
                    hints::HINTS_KEY,
                    format!("{}=true", requests::AUTO_CREATE_TABLE_KEY),
                )
                .body("hint_catalog_auto_create,host=a value=1 1700000000000000000")
                .send()
                .await;
            assert!(!response.status().is_success());
            assert!(
                response
                    .text()
                    .await
                    .contains("auto-create table is disabled by frontend config")
            );
        }
        instance.guard.remove_all().await;
    }
}

#[apply(both_deployment_cases)]
async fn test_show_keeps_explicit_remote_filter_override(distributed: bool) {
    let (mut instance, client) = setup_catalog_client("hint_remote_filter", distributed).await;
    let name = query_options::QUERY_ENABLE_REMOTE_DYNAMIC_FILTER_PUSHDOWN;
    assert_eq!(
        find_hint(&fetch_catalog(&client).await, name)["default"],
        true
    );
    for value in ["false", "true"] {
        let response = client
            .get(&format!("/v1/sql?sql={}", encode(&format!("SHOW {name}"))))
            .header(hints::HINTS_KEY, format!("{name}={value}"))
            .send()
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(single_cell(&response.json::<Value>().await, 0), value);
    }
    instance.shutdown().await;
}

fn assert_group(catalog: &Value, name: &str, expected: &str) {
    let group = catalog["groups"]
        .as_array()
        .unwrap()
        .iter()
        .find(|group| {
            group["hints"]
                .as_array()
                .unwrap()
                .iter()
                .any(|entry| entry["name"] == name)
        })
        .unwrap_or_else(|| panic!("missing hint {name}"));
    assert_eq!(group["name"], expected, "{name}");
}

async fn setup_catalog_client(name: &str, distributed: bool) -> (MockInstanceImpl, TestClient) {
    common_telemetry::init_default_ut_logging();
    let instance = MockInstanceImpl::new(name, distributed).await;
    let server = HttpServerBuilder::new(HttpOptions::default())
        .with_sql_handler(instance.frontend())
        .with_influxdb_handler(instance.frontend())
        .build();
    let client = TestClient::new(server.build(server.make_app()).unwrap()).await;
    (instance, client)
}

async fn fetch_catalog(client: &TestClient) -> Value {
    let response = client.get("/v1/hints").send().await;
    assert_eq!(response.status(), StatusCode::OK);
    response.json::<Value>().await
}

fn find_hint<'a>(catalog: &'a Value, name: &str) -> &'a Value {
    catalog_entries(catalog)
        .find(|entry| entry["name"] == name)
        .unwrap_or_else(|| panic!("missing hint {name}"))
}

fn single_cell(body: &Value, output_index: usize) -> &Value {
    let output = body["output"]
        .as_array()
        .unwrap_or_else(|| panic!("SQL output: {body}"));
    let rows = output[output_index]["records"]["rows"]
        .as_array()
        .unwrap_or_else(|| panic!("SQL rows at output {output_index}: {body}"));
    assert_eq!(rows.len(), 1, "expected a single row: {body}");
    let row = rows[0].as_array().expect("SQL row");
    assert_eq!(row.len(), 1, "expected a single column: {body}");
    &row[0]
}

fn catalog_defaults(catalog: &Value) -> Vec<(String, Value)> {
    catalog_entries(catalog)
        .map(|entry| {
            let name = entry["name"].as_str().expect("hint name");
            let default = entry
                .get("default")
                .unwrap_or_else(|| panic!("missing default for {name}"));
            (name.to_string(), default.clone())
        })
        .collect()
}

fn catalog_entries(catalog: &Value) -> impl Iterator<Item = &Value> {
    catalog["groups"]
        .as_array()
        .expect("hint catalog groups")
        .iter()
        .flat_map(|group| group["hints"].as_array().expect("hint group entries"))
}
