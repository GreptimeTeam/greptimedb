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

use std::time::Instant;

use reqwest::Client;
use serde_json::{Value, json};

use crate::query_regression_runner::Result;

pub(super) fn sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

pub(super) fn extract_rows(body: &Value) -> Vec<Value> {
    fn visit(body: &Value, rows: &mut Vec<Value>) {
        match body {
            Value::Object(object) => {
                for key in ["data", "rows", "records", "output"] {
                    let Some(value) = object.get(key) else {
                        continue;
                    };
                    if matches!(key, "data" | "rows") && value.is_array() {
                        rows.extend(value.as_array().unwrap().iter().cloned());
                    } else {
                        visit(value, rows);
                    }
                }
            }
            Value::Array(values) => {
                if !values.is_empty()
                    && values
                        .iter()
                        .all(|value| !value.is_object() && !value.is_array())
                {
                    rows.push(body.clone());
                } else {
                    for value in values {
                        visit(value, rows);
                    }
                }
            }
            _ => {}
        }
    }

    let mut rows = Vec::new();
    visit(body, &mut rows);
    rows
}

pub(super) fn row_value<'a>(row: &'a Value, index: usize, name: &str) -> Option<&'a Value> {
    match row {
        Value::Object(values) => [
            name.to_string(),
            name.to_ascii_uppercase(),
            name.to_ascii_lowercase(),
        ]
        .into_iter()
        .find_map(|key| values.get(&key)),
        Value::Array(values) => values.get(index),
        _ => Some(row),
    }
}

pub(super) fn row_u64(row: &Value, index: usize, name: &str) -> Result<u64> {
    let value =
        row_value(row, index, name).ok_or_else(|| format!("missing {name} in row {row}"))?;
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
        .ok_or_else(|| format!("invalid {name} in row {row}").into())
}

pub(super) fn value_text(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::Null => "None".to_string(),
        _ => value.to_string(),
    }
}

pub(super) fn extract_count_value(result: &Value) -> Option<u64> {
    let row = result
        .get("response")?
        .get("data")?
        .as_array()?
        .first()?
        .as_object()?;
    row.iter()
        .find(|(key, _)| {
            let key = key.to_ascii_lowercase();
            key == "count(*)" || key.starts_with("count(")
        })
        .and_then(|(_, value)| value_u64(Some(value)))
}

pub(super) fn value_u64(value: Option<&Value>) -> Option<u64> {
    value?.as_u64().or_else(|| value?.as_str()?.parse().ok())
}

pub(super) fn value_f64(value: Option<&Value>) -> Option<f64> {
    value?.as_f64().or_else(|| value?.as_str()?.parse().ok())
}

pub(super) async fn http_post_sql(client: &Client, port: u16, sql: &str, db: &str) -> Value {
    let started = Instant::now();
    let request = client
        .post(format!("http://127.0.0.1:{port}/v1/sql"))
        .form(&[("sql", sql), ("db", db), ("format", "json")]);
    match request.send().await {
        Ok(response) => {
            let status = response.status().as_u16();
            match response.text().await {
                Ok(raw) => {
                    let body = serde_json::from_str(&raw).unwrap_or_else(|_| json!({"raw": raw}));
                    let ok = status < 400 && !response_has_error(&body);
                    let mut sample = json!({
                        "ok": ok,
                        "status": status,
                        "latency_ms": started.elapsed().as_secs_f64() * 1000.0,
                        "response": body,
                        "sql": sql,
                    });
                    if status >= 400 {
                        sample
                            .as_object_mut()
                            .expect("HTTP samples are objects")
                            .insert("error".to_string(), Value::String(format!("HTTP {status}")));
                    }
                    sample
                }
                Err(error) => json!({
                    "ok": false,
                    "status": status,
                    "latency_ms": started.elapsed().as_secs_f64() * 1000.0,
                    "error": error.to_string(),
                    "sql": sql,
                }),
            }
        }
        Err(error) => json!({
            "ok": false,
            "status": Value::Null,
            "latency_ms": started.elapsed().as_secs_f64() * 1000.0,
            "error": error.to_string(),
            "sql": sql,
        }),
    }
}

fn response_has_error(body: &Value) -> bool {
    let Some(body) = body.as_object() else {
        return false;
    };
    ["error", "err_msg", "error_msg"]
        .into_iter()
        .any(|key| body.get(key).is_some_and(is_truthy))
        || body
            .get("error_code")
            .is_some_and(|value| !is_success_code(value))
        || (!body.contains_key("output")
            && body
                .get("code")
                .is_some_and(|value| !is_success_code(value)))
}

fn is_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64().is_none_or(|value| value != 0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(value) => !value.is_empty(),
        Value::Object(value) => !value.is_empty(),
    }
}

fn is_success_code(value: &Value) -> bool {
    let code = match value {
        Value::String(value) => value.clone(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::Null => "None".to_string(),
        _ => value.to_string(),
    };
    matches!(code.to_lowercase().as_str(), "" | "0" | "success")
}

pub(super) fn sql_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn top_level_errors_do_not_inspect_rows() {
        assert!(response_has_error(&json!({"error_code": 7})));
        assert!(response_has_error(&json!({"code": "bad"})));
        assert!(!response_has_error(
            &json!({"output": [{"code": 7, "error": "row value"}]})
        ));
        assert!(!response_has_error(
            &json!({"error_code": "success", "output": []})
        ));
    }

    #[test]
    fn extracts_remote_write_count_from_data_map() {
        assert_eq!(
            extract_count_value(&json!({"response": {"data": [{"COUNT(*)": "12"}]}})),
            Some(12)
        );
        assert_eq!(
            extract_count_value(&json!({"response": {"data": [{"count(value)": 7}]}})),
            Some(7)
        );
        assert_eq!(
            extract_count_value(&json!({"response": {"data": [[]]}})),
            None
        );
    }
}
