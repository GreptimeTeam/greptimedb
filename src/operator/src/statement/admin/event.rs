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

use std::any::Any;

use api::v1::value::ValueData;
use api::v1::{ColumnSchema, Row};
use common_event_recorder::Event;
use common_event_recorder::error::Result as EventResult;
use common_event_recorder::event_table::{
    ADMIN_FUNCTION_NAME_COLUMN, ADMIN_FUNCTION_OUTPUT_COLUMN, ADMIN_FUNCTION_STATUS_COLUMN,
    column_schemas, jsonb_value,
};
use datatypes::value::Value;
use serde_json::{Value as JsonValue, json};
use sql::ast::{Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Value as SqlValue};
use sql::statements::admin::Admin;

use crate::error::Error;
use crate::statement::admin::AdminFunctionRequest;

/// Event type emitted for an ADMIN function execution.
pub(crate) const ADMIN_FUNCTION_EVENT_TYPE: &str = "admin_function";

const PAYLOAD_VERSION: u8 = 1;
const VERSION_KEY: &str = "version";
const ARGUMENTS_KEY: &str = "arguments";
const RESULT_KEY: &str = "result";
const ERROR_KEY: &str = "error";
const SUCCEEDED_STATUS: &str = "Succeeded";
const FAILED_STATUS: &str = "Failed";
const UNSUPPORTED: &str = "<unsupported>";

/// The ADMIN function input captured before execution.
#[derive(Debug, Clone)]
pub(crate) struct AdminFunctionEventInput {
    function: String,
    arguments: Vec<JsonValue>,
}

impl AdminFunctionEventInput {
    /// Creates an event input from an ADMIN function request.
    pub(crate) fn from_request(request: &AdminFunctionRequest) -> Self {
        let Admin::Func(function) = &request.statement;
        let function_name = function.name.to_string().to_lowercase();

        let arguments = match &function.args {
            FunctionArguments::List(arguments) => {
                arguments.args.iter().map(argument_to_json).collect()
            }
            _ => vec![JsonValue::String(UNSUPPORTED.to_string())],
        };

        Self {
            function: function_name,
            arguments,
        }
    }
}

/// An event describing the outcome of an ADMIN function execution.
#[derive(Debug)]
pub(crate) struct AdminFunctionEvent {
    function_name: String,
    status: &'static str,
    output: JsonValue,
    payload: JsonValue,
}

impl AdminFunctionEvent {
    /// Creates a successful ADMIN function event.
    pub(crate) fn success(input: AdminFunctionEventInput, result: &Value) -> Self {
        Self {
            function_name: input.function,
            status: SUCCEEDED_STATUS,
            output: json!({
                (RESULT_KEY): value_to_json(result),
            }),
            payload: json!({
                (VERSION_KEY): PAYLOAD_VERSION,
                (ARGUMENTS_KEY): input.arguments,
            }),
        }
    }

    /// Creates a failed ADMIN function event with the debug representation of the error.
    pub(crate) fn failure(input: AdminFunctionEventInput, error: &Error) -> Self {
        Self {
            function_name: input.function,
            status: FAILED_STATUS,
            output: json!({
                (ERROR_KEY): format!("{error:?}"),
            }),
            payload: json!({
                (VERSION_KEY): PAYLOAD_VERSION,
                (ARGUMENTS_KEY): input.arguments,
            }),
        }
    }
}

impl Event for AdminFunctionEvent {
    fn event_type(&self) -> &str {
        ADMIN_FUNCTION_EVENT_TYPE
    }

    fn json_payload(&self) -> EventResult<JsonValue> {
        Ok(self.payload.clone())
    }

    fn extra_schema(&self) -> Vec<ColumnSchema> {
        column_schemas([
            &ADMIN_FUNCTION_NAME_COLUMN,
            &ADMIN_FUNCTION_STATUS_COLUMN,
            &ADMIN_FUNCTION_OUTPUT_COLUMN,
        ])
    }

    fn extra_rows(&self) -> EventResult<Vec<Row>> {
        Ok(vec![Row {
            values: vec![
                ValueData::StringValue(self.function_name.clone()).into(),
                ValueData::StringValue(self.status.to_string()).into(),
                jsonb_value(&self.output),
            ],
        }])
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn argument_to_json(argument: &FunctionArg) -> JsonValue {
    let FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(value))) = argument else {
        return JsonValue::String(UNSUPPORTED.to_string());
    };

    sql_value_to_json(&value.value)
}

fn sql_value_to_json(value: &SqlValue) -> JsonValue {
    match value {
        SqlValue::Number(value, _) => {
            serde_json::from_str(value).unwrap_or_else(|_| JsonValue::String(value.clone()))
        }
        SqlValue::Boolean(value) => JsonValue::Bool(*value),
        SqlValue::Null => JsonValue::Null,
        SqlValue::SingleQuotedString(value) | SqlValue::DoubleQuotedString(value) => {
            JsonValue::String(value.clone())
        }
        SqlValue::HexStringLiteral(_) => JsonValue::String(UNSUPPORTED.to_string()),
        _ => JsonValue::String(UNSUPPORTED.to_string()),
    }
}

fn value_to_json(value: &Value) -> JsonValue {
    match value {
        Value::Null => JsonValue::Null,
        Value::Boolean(value) => JsonValue::Bool(*value),
        Value::UInt8(value) => JsonValue::from(*value),
        Value::UInt16(value) => JsonValue::from(*value),
        Value::UInt32(value) => JsonValue::from(*value),
        Value::UInt64(value) => JsonValue::from(*value),
        Value::Int8(value) => JsonValue::from(*value),
        Value::Int16(value) => JsonValue::from(*value),
        Value::Int32(value) => JsonValue::from(*value),
        Value::Int64(value) => JsonValue::from(*value),
        Value::Float32(value) => serde_json::Number::from_f64(value.0 as f64)
            .map(JsonValue::Number)
            .unwrap_or_else(|| JsonValue::String(UNSUPPORTED.to_string())),
        Value::Float64(value) => serde_json::Number::from_f64(value.0)
            .map(JsonValue::Number)
            .unwrap_or_else(|| JsonValue::String(UNSUPPORTED.to_string())),
        Value::String(value) => JsonValue::String(value.as_utf8().to_string()),
        _ => JsonValue::String(UNSUPPORTED.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use api::v1::Row;
    use api::v1::value::ValueData;
    use common_event_recorder::Event;
    use common_event_recorder::event_table::{
        ADMIN_FUNCTION_NAME_COLUMN, ADMIN_FUNCTION_OUTPUT_COLUMN, ADMIN_FUNCTION_STATUS_COLUMN,
        column_schemas, jsonb_value,
    };
    use datatypes::value::Value;
    use serde_json::json;
    use session::context::QueryContext;
    use sql::dialect::GreptimeDbDialect;
    use sql::parser::{ParseOptions, ParserContext};
    use sql::statements::statement::Statement;

    use crate::error::Error;
    use crate::statement::admin::AdminFunctionRequest;
    use crate::statement::admin::event::{
        ADMIN_FUNCTION_EVENT_TYPE, AdminFunctionEvent, AdminFunctionEventInput,
    };

    fn request(sql: &str) -> AdminFunctionRequest {
        let Statement::Admin(statement) =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap()
                .remove(0)
        else {
            panic!("expected ADMIN statement")
        };
        AdminFunctionRequest {
            statement,
            query_ctx: QueryContext::arc(),
        }
    }

    fn assert_admin_columns(
        event: &AdminFunctionEvent,
        function_name: &str,
        status: &str,
        output: serde_json::Value,
    ) {
        assert_eq!(event.event_type(), ADMIN_FUNCTION_EVENT_TYPE);
        assert_eq!(
            event.extra_schema(),
            column_schemas([
                &ADMIN_FUNCTION_NAME_COLUMN,
                &ADMIN_FUNCTION_STATUS_COLUMN,
                &ADMIN_FUNCTION_OUTPUT_COLUMN,
            ])
        );
        assert_eq!(
            event.extra_rows().unwrap(),
            vec![Row {
                values: vec![
                    ValueData::StringValue(function_name.to_string()).into(),
                    ValueData::StringValue(status.to_string()).into(),
                    jsonb_value(&output),
                ],
            }]
        );
    }

    #[test]
    fn success_uses_separate_output_columns() {
        let input = AdminFunctionEventInput::from_request(&request(
            "ADMIN flush_table('greptime.public.demo')",
        ));
        let event = AdminFunctionEvent::success(input, &Value::UInt64(3));

        assert_admin_columns(&event, "flush_table", "Succeeded", json!({"result": 3}));
        assert_eq!(
            event.json_payload().unwrap(),
            json!({
                "version": 1,
                "arguments": ["greptime.public.demo"],
            })
        );
    }

    #[test]
    fn successful_null_is_explicit() {
        let input = AdminFunctionEventInput::from_request(&request(
            "ADMIN migrate_region(NULL, NULL, NULL)",
        ));
        let event = AdminFunctionEvent::success(input, &Value::Null);

        assert_admin_columns(
            &event,
            "migrate_region",
            "Succeeded",
            json!({"result": null}),
        );
        assert_eq!(
            event.json_payload().unwrap(),
            json!({
                "version": 1,
                "arguments": [null, null, null],
            })
        );
    }

    #[test]
    fn unknown_function_values_are_recorded() {
        let input = AdminFunctionEventInput::from_request(&request(
            "ADMIN plugin_function('plugin-value', NULL)",
        ));
        let event = AdminFunctionEvent::success(input, &Value::from("plugin-result"));

        assert_admin_columns(
            &event,
            "plugin_function",
            "Succeeded",
            json!({"result": "plugin-result"}),
        );
        assert_eq!(
            event.json_payload().unwrap(),
            json!({
                "version": 1,
                "arguments": ["plugin-value", null],
            })
        );
    }

    #[test]
    fn failure_payload_has_error_only() {
        let input = AdminFunctionEventInput::from_request(&request(
            "ADMIN flush_table('greptime.public.demo')",
        ));
        let error = Error::BuildAdminFunctionArgs {
            msg: "invalid input".to_string(),
        };
        let expected_error = format!("{error:?}");
        let event = AdminFunctionEvent::failure(input, &error);

        assert_admin_columns(
            &event,
            "flush_table",
            "Failed",
            json!({"error": expected_error}),
        );
        assert_eq!(
            event.json_payload().unwrap(),
            json!({
                "version": 1,
                "arguments": ["greptime.public.demo"],
            })
        );
    }
}
