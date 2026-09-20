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

#[cfg(test)]
mod tests;

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::BooleanArray;
use arrow::datatypes::DataType;
use async_trait::async_trait;
use datafusion_common::cast::{as_float64_array, as_string_view_array};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, exec_datafusion_err, exec_err, not_impl_err};
use datafusion_expr::async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use futures::{StreamExt, TryStreamExt, stream};
use reqwest::Client;
use serde_json::{Value, json};

use crate::function_factory::ScalarFunctionFactory;
use crate::function_registry::FunctionRegistry;

/// Experimental natural-language predicate: `jev(text, statement, threshold)`.
/// Each non-null row is a Noul question; its probability is compared with `>=`.
#[derive(PartialEq, Eq, Hash)]
pub(crate) struct JevFunction {
    signature: Signature,
    enabled: bool,
    api_key: Option<String>,
    endpoint: String,
    model: String,
}

impl JevFunction {
    pub(crate) fn register(registry: &FunctionRegistry) {
        registry.register(ScalarFunctionFactory {
            name: "jev".to_string(),
            factory: Arc::new(|_| AsyncScalarUDF::new(Arc::new(Self::default())).into_scalar_udf()),
        });
    }

    async fn evaluate(&self, client: &Client, text: &str, statement: &str) -> Result<f64> {
        let response: Value = client
            .post(&self.endpoint)
            .json(&json!({
                "model": self.model,
                "state": text,
                "questions": {
                    "matches": { "type": "noul", "instructions": statement }
                }
            }))
            .send()
            .await
            .and_then(|response| response.error_for_status())
            .map_err(|e| exec_datafusion_err!("jev request failed: {e}"))?
            .json()
            .await
            .map_err(|e| exec_datafusion_err!("jev response is not valid JSON: {e}"))?;

        let answer = &response["answers"]["matches"];
        let probability = answer["noul"].as_f64().filter(|p| (0.0..=1.0).contains(p));
        match (answer["type"].as_str(), probability) {
            (Some("noul"), Some(probability)) => Ok(probability),
            _ => exec_err!(
                "jev response must contain answers.matches with type noul and a probability in [0, 1]"
            ),
        }
    }

    fn client(&self) -> Result<Client> {
        if !self.enabled {
            return exec_err!(
                "jev is experimental; set GREPTIMEDB_EXPERIMENTAL_JEV=true to enable it"
            );
        }
        let key = self
            .api_key
            .as_deref()
            .filter(|key| !key.trim().is_empty())
            .ok_or_else(|| {
                exec_datafusion_err!("jev requires the JEV_API_KEY environment variable")
            })?;
        let mut authorization = reqwest::header::HeaderValue::from_str(&format!("Bearer {key}"))
            .map_err(|_| exec_datafusion_err!("JEV_API_KEY is not a valid HTTP header value"))?;
        authorization.set_sensitive(true);
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(reqwest::header::AUTHORIZATION, authorization);
        Client::builder()
            .default_headers(headers)
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| exec_datafusion_err!("failed to create jev HTTP client: {e}"))
    }
}

impl Default for JevFunction {
    fn default() -> Self {
        Self {
            // External model evaluations must not be constant-folded during planning.
            signature: Signature::exact(
                vec![DataType::Utf8View, DataType::Utf8View, DataType::Float64],
                Volatility::Volatile,
            ),
            enabled: std::env::var("GREPTIMEDB_EXPERIMENTAL_JEV").as_deref() == Ok("true"),
            api_key: std::env::var("JEV_API_KEY").ok(),
            endpoint: std::env::var("JEV_ENDPOINT")
                .unwrap_or_else(|_| "https://api.typesafe.ai/v1/systemone".to_string()),
            model: std::env::var("JEV_MODEL").unwrap_or_else(|_| "jev-latest".to_string()),
        }
    }
}

impl fmt::Debug for JevFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Credentials must never appear in plans or diagnostic output.
        f.debug_struct("JevFunction")
            .field("signature", &self.signature)
            .field("enabled", &self.enabled)
            .field("model", &self.model)
            .finish_non_exhaustive()
    }
}

impl ScalarUDFImpl for JevFunction {
    fn name(&self) -> &str {
        "jev"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        not_impl_err!("jev can only be called from async contexts")
    }
}

#[async_trait]
impl AsyncScalarUDFImpl for JevFunction {
    async fn invoke_async_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = args
            .args
            .into_iter()
            .map(|arg| arg.into_array(args.number_rows))
            .collect::<Result<Vec<_>>>()?;
        let [texts, statements, thresholds] = take_function_args(self.name(), arrays)?;
        let texts = as_string_view_array(&texts)?;
        let statements = as_string_view_array(&statements)?;
        let thresholds = as_float64_array(&thresholds)?;
        let rows: Vec<_> = texts
            .iter()
            .zip(statements.iter())
            .zip(thresholds.iter())
            .map(|((text, statement), threshold)| Some((text?, statement?, threshold?)))
            .collect();

        // Validate the whole batch before making any billable requests.
        for (_, _, threshold) in rows.iter().flatten() {
            if !(0.0..=1.0).contains(threshold) {
                return exec_err!("jev threshold must be finite and in [0, 1]");
            }
        }
        if rows.iter().all(Option::is_none) {
            return Ok(ColumnarValue::Array(Arc::new(BooleanArray::new_null(
                args.number_rows,
            ))));
        }

        // Reuse connections within the batch; bound in-flight requests and preserve row order.
        let client = self.client()?;
        let requests: Vec<_> = rows
            .into_iter()
            .map(|row| {
                let client = &client;
                async move {
                    match row {
                        Some((text, statement, threshold)) => self
                            .evaluate(client, text, statement)
                            .await
                            .map(|p| Some(p >= threshold)),
                        None => Ok(None),
                    }
                }
            })
            .collect();
        // This limit is per expression/batch invocation, not per query or process.
        // Concurrent partitions and queries can each have their own in-flight requests.
        let matches: Vec<Option<bool>> = stream::iter(requests).buffered(8).try_collect().await?;
        Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(matches))))
    }
}
