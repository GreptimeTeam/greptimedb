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
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, StringViewArray, new_null_array};
use arrow::datatypes::DataType;
use async_trait::async_trait;
use datafusion_common::cast::as_string_view_array;
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, exec_err, not_impl_err};
use datafusion_expr::async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use futures::{StreamExt, TryStreamExt, stream};
use reqwest::Client;
use serde::Serialize;
use serde_json::{Map, Value, json};

use crate::function_factory::ScalarFunctionFactory;
use crate::function_registry::FunctionRegistry;

/// Registers the experimental AI matching, classification, and rating functions.
pub(crate) fn register(registry: &FunctionRegistry) {
    AiFunction::<Noul>::register(registry);
    AiFunction::<Choice>::register(registry);
    AiFunction::<Score>::register(registry);
}

/// Shared asynchronous execution for AI functions using the Jev backend.
#[derive(PartialEq, Eq, Hash)]
struct AiFunction<Q> {
    signature: Signature,
    question: PhantomData<Q>,
    enabled: bool,
    api_key: Option<String>,
    endpoint: String,
    model: String,
}

struct AiRequest<'a, Q: AiQuestion> {
    text: &'a str,
    prompt: &'a str,
    criteria: Arc<Q::Criteria>,
}

/// Borrows string arguments without expanding constants to the batch size.
enum StringArgument<'a> {
    Scalar(Option<&'a str>),
    Array(&'a StringViewArray),
}

impl<'a> StringArgument<'a> {
    fn try_new(arg: &'a ColumnarValue) -> Result<Self> {
        match arg {
            ColumnarValue::Scalar(ScalarValue::Utf8View(value)) => {
                Ok(Self::Scalar(value.as_deref()))
            }
            ColumnarValue::Array(array) => Ok(Self::Array(as_string_view_array(array)?)),
            _ => exec_err!("AI functions expect Utf8View arguments"),
        }
    }

    fn value(&self, row: usize) -> Option<&'a str> {
        match self {
            Self::Scalar(value) => *value,
            Self::Array(array) => {
                let array = *array;
                array.is_valid(row).then(|| array.value(row))
            }
        }
    }
}

impl<Q: AiQuestion> AiFunction<Q> {
    fn register(registry: &FunctionRegistry) {
        registry.register(ScalarFunctionFactory {
            name: Q::NAME.to_string(),
            factory: Arc::new(|_| AsyncScalarUDF::new(Arc::new(Self::default())).into_scalar_udf()),
        });
    }

    async fn evaluate(&self, client: &Client, request: AiRequest<'_, Q>) -> Result<ScalarValue> {
        let mut question = json!({ "type": Q::TYPE, "instructions": request.prompt });
        if Q::ARG_COUNT == 3 {
            question["criteria"] = json!(request.criteria.as_ref());
        }
        let response: Value = client
            .post(&self.endpoint)
            .json(&json!({
                "model": self.model,
                "state": request.text,
                "questions": { "matches": question }
            }))
            .send()
            .await
            .and_then(|response| response.error_for_status())
            .map_err(|e| exec_datafusion_err!("{} request failed: {e}", Q::NAME))?
            .json()
            .await
            .map_err(|e| exec_datafusion_err!("{} response is not valid JSON: {e}", Q::NAME))?;

        let answer = &response["answers"]["matches"];
        if answer["type"].as_str() != Some(Q::TYPE) {
            return exec_err!(
                "{} response must contain answers.matches with type {}",
                Q::NAME,
                Q::TYPE
            );
        }
        Q::parse_answer(answer, request.criteria.as_ref())
    }

    fn prepare_requests<'a>(
        &self,
        args: &'a [ColumnarValue],
        number_rows: usize,
    ) -> Result<Vec<Option<AiRequest<'a, Q>>>> {
        if args.len() != Q::ARG_COUNT {
            return exec_err!("{} requires {} arguments", Q::NAME, Q::ARG_COUNT);
        }
        let args = args
            .iter()
            .map(StringArgument::try_new)
            .collect::<Result<Vec<_>>>()?;
        let scalar_criteria = args[2..]
            .iter()
            .all(|arg| matches!(arg, StringArgument::Scalar(_)));
        let mut shared_criteria: Option<Arc<Q::Criteria>> = None;

        // Validate all non-null rows before making any billable requests.
        (0..number_rows)
            .map(|row| {
                let values: Option<Vec<_>> = args.iter().map(|arg| arg.value(row)).collect();
                values
                    .map(|values| {
                        // Initialize lazily so NULL rows never validate otherwise invalid criteria.
                        let criteria = match &shared_criteria {
                            Some(criteria) => Arc::clone(criteria),
                            None => {
                                let criteria = Arc::new(Q::parse_criteria(&values[2..])?);
                                if scalar_criteria {
                                    shared_criteria = Some(Arc::clone(&criteria));
                                }
                                criteria
                            }
                        };
                        Ok(AiRequest {
                            text: values[0],
                            prompt: values[1],
                            criteria,
                        })
                    })
                    .transpose()
            })
            .collect()
    }

    fn client(&self) -> Result<Client> {
        if !self.enabled {
            return exec_err!(
                "{} is experimental; set GREPTIMEDB_EXPERIMENTAL_JEV=true to enable it",
                Q::NAME
            );
        }
        let key = self
            .api_key
            .as_deref()
            .filter(|key| !key.trim().is_empty())
            .ok_or_else(|| {
                exec_datafusion_err!("{} requires the JEV_API_KEY environment variable", Q::NAME)
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
            .map_err(|e| exec_datafusion_err!("failed to create {} HTTP client: {e}", Q::NAME))
    }
}

impl<Q: AiQuestion> Default for AiFunction<Q> {
    fn default() -> Self {
        Self {
            // External model evaluations must not be constant-folded during planning.
            signature: Signature::exact(
                vec![DataType::Utf8View; Q::ARG_COUNT],
                Volatility::Volatile,
            ),
            question: PhantomData,
            enabled: std::env::var("GREPTIMEDB_EXPERIMENTAL_JEV").as_deref() == Ok("true"),
            api_key: std::env::var("JEV_API_KEY").ok(),
            endpoint: std::env::var("JEV_ENDPOINT")
                .unwrap_or_else(|_| "https://api.typesafe.ai/v1/systemone".to_string()),
            model: std::env::var("JEV_MODEL").unwrap_or_else(|_| "jev-latest".to_string()),
        }
    }
}

impl<Q: AiQuestion> fmt::Debug for AiFunction<Q> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Credentials must never appear in plans or diagnostic output.
        f.debug_struct("AiFunction")
            .field("name", &Q::NAME)
            .field("signature", &self.signature)
            .field("enabled", &self.enabled)
            .field("model", &self.model)
            .finish_non_exhaustive()
    }
}

impl<Q: AiQuestion> ScalarUDFImpl for AiFunction<Q> {
    fn name(&self) -> &str {
        Q::NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Q::return_type())
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        not_impl_err!("{} can only be called from async contexts", Q::NAME)
    }
}

#[async_trait]
impl<Q: AiQuestion> AsyncScalarUDFImpl for AiFunction<Q> {
    async fn invoke_async_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = self.prepare_requests(&args.args, args.number_rows)?;

        if rows.iter().all(Option::is_none) {
            return Ok(ColumnarValue::Array(new_null_array(
                &Q::return_type(),
                args.number_rows,
            )));
        }

        // Reuse connections within the batch; bound in-flight requests and preserve row order.
        let client = self.client()?;
        // Collect futures to avoid async_trait's higher-ranked lifetime inference issue.
        let requests: Vec<_> = rows
            .into_iter()
            .map(|row| {
                let client = &client;
                async move {
                    match row {
                        Some(request) => self.evaluate(client, request).await,
                        None => ScalarValue::try_from(&Q::return_type()),
                    }
                }
            })
            .collect();
        // This limit is per expression/batch invocation, not per query or process.
        // Concurrent partitions and queries can each have their own in-flight requests.
        let answers: Vec<ScalarValue> = stream::iter(requests).buffered(8).try_collect().await?;
        Ok(ColumnarValue::Array(ScalarValue::iter_to_array(answers)?))
    }
}

/// The request criteria and scalar answer contract for an AI question type.
trait AiQuestion: fmt::Debug + Eq + Hash + Send + Sync + 'static {
    type Criteria: Serialize + Send + Sync;

    const NAME: &'static str;
    const TYPE: &'static str;
    const ARG_COUNT: usize;

    fn return_type() -> DataType;
    fn parse_criteria(args: &[&str]) -> Result<Self::Criteria>;
    fn parse_answer(answer: &Value, criteria: &Self::Criteria) -> Result<ScalarValue>;
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Noul;

impl AiQuestion for Noul {
    type Criteria = ();

    const NAME: &'static str = "ai_match";
    const TYPE: &'static str = "noul";
    const ARG_COUNT: usize = 2;

    fn return_type() -> DataType {
        DataType::Float64
    }

    fn parse_criteria(_args: &[&str]) -> Result<()> {
        Ok(())
    }

    fn parse_answer(answer: &Value, _criteria: &()) -> Result<ScalarValue> {
        numeric_answer(Self::NAME, &answer["noul"], "noul", 1.0)
            .map(|probability| ScalarValue::Float64(Some(probability)))
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Choice;

impl AiQuestion for Choice {
    type Criteria = Map<String, Value>;

    const NAME: &'static str = "ai_choose";
    const TYPE: &'static str = "choice";
    const ARG_COUNT: usize = 3;

    fn return_type() -> DataType {
        DataType::Utf8
    }

    fn parse_criteria(args: &[&str]) -> Result<Self::Criteria> {
        let criteria: Value = serde_json::from_str(args[0])
            .map_err(|e| exec_datafusion_err!("ai_choose criteria is not valid JSON: {e}"))?;
        match criteria {
            Value::Object(options)
                if (1..=255).contains(&options.len())
                    && options.values().all(|v| v.is_null() || is_description(v)) =>
            {
                Ok(options)
            }
            _ => exec_err!(
                "ai_choose criteria must be a JSON object with 1 to 255 options; descriptions must be strings, objects, arrays, or null"
            ),
        }
    }

    fn parse_answer(answer: &Value, criteria: &Self::Criteria) -> Result<ScalarValue> {
        let choice = answer["choice"]
            .as_str()
            .filter(|choice| criteria.contains_key(*choice))
            .ok_or_else(|| {
                exec_datafusion_err!("ai_choose response must contain a choice from the criteria")
            })?;
        Ok(ScalarValue::Utf8(Some(choice.to_string())))
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Score;

impl AiQuestion for Score {
    type Criteria = Vec<Value>;

    const NAME: &'static str = "ai_score";
    const TYPE: &'static str = "score";
    const ARG_COUNT: usize = 3;

    fn return_type() -> DataType {
        DataType::BinaryView
    }

    fn parse_criteria(args: &[&str]) -> Result<Self::Criteria> {
        let criteria: Value = serde_json::from_str(args[0])
            .map_err(|e| exec_datafusion_err!("ai_score criteria is not valid JSON: {e}"))?;
        match criteria {
            Value::Array(levels)
                if (2..=10).contains(&levels.len()) && levels.iter().all(is_description) =>
            {
                Ok(levels)
            }
            _ => exec_err!(
                "ai_score criteria must be a JSON array with 2 to 10 levels; descriptions must be strings, objects, or arrays"
            ),
        }
    }

    fn parse_answer(answer: &Value, criteria: &Self::Criteria) -> Result<ScalarValue> {
        let score = numeric_answer(
            Self::NAME,
            &answer["score"],
            "score",
            (criteria.len() - 1) as f64,
        )?;
        let confidence = numeric_answer(Self::NAME, &answer["confidence"], "confidence", 1.0)?;
        let probabilities = score_probabilities(&answer["probabilities"], criteria.len())?;
        let object = jsonb::Object::from([
            ("score".to_string(), jsonb::Value::from(score)),
            ("confidence".to_string(), jsonb::Value::from(confidence)),
            (
                "probabilities".to_string(),
                jsonb::Value::Array(probabilities.into_iter().map(jsonb::Value::from).collect()),
            ),
        ]);
        Ok(ScalarValue::BinaryView(Some(
            jsonb::Value::Object(object).to_vec(),
        )))
    }
}

fn score_probabilities(distribution: &Value, level_count: usize) -> Result<Vec<f64>> {
    if distribution
        .as_object()
        .is_none_or(|probabilities| probabilities.len() != level_count)
    {
        return exec_err!(
            "ai_score response must contain probabilities for all {level_count} levels"
        );
    }
    let probabilities = (0..level_count)
        .map(|level| {
            numeric_answer(
                Score::NAME,
                &distribution[level.to_string()],
                "probability",
                1.0,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    // Allow small rounding differences without renormalizing the provider's distribution.
    if (probabilities.iter().sum::<f64>() - 1.0).abs() > 1e-6 {
        return exec_err!("ai_score response probabilities must sum to 1 within 1e-6");
    }
    Ok(probabilities)
}

fn is_description(description: &Value) -> bool {
    matches!(
        description,
        Value::String(_) | Value::Object(_) | Value::Array(_)
    )
}

fn numeric_answer(name: &str, answer: &Value, field: &str, max: f64) -> Result<f64> {
    answer
        .as_f64()
        .filter(|score| (0.0..=max).contains(score))
        .ok_or_else(|| {
            exec_datafusion_err!("{name} response must contain a finite {field} in [0, {max}]")
        })
}
