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
    ACTOR_COLUMN, ADMIN_FUNCTION_NAME_COLUMN, ADMIN_FUNCTION_OUTPUT_COLUMN,
    ADMIN_FUNCTION_STATUS_COLUMN, column_schemas, jsonb_value,
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
const ARGUMENT_TYPE_KEY: &str = "type";
const ARGUMENT_VALUE_KEY: &str = "value";
const LIST_ARGUMENT_TYPE: &str = "list";
const SUBQUERY_ARGUMENT_TYPE: &str = "subquery";
const RESULT_KEY: &str = "result";
const ERROR_KEY: &str = "error";
const SUCCEEDED_STATUS: &str = "Succeeded";
const FAILED_STATUS: &str = "Failed";
const UNSUPPORTED: &str = "<unsupported>";

/// The ADMIN function input captured before execution.
#[derive(Debug, Clone)]
pub(crate) struct AdminFunctionEventInput {
    actor: String,
    function: String,
    arguments: Option<JsonValue>,
}

impl AdminFunctionEventInput {
    /// Creates an event input from an ADMIN function request.
    pub(crate) fn from_request(request: &AdminFunctionRequest) -> Self {
        let Admin::Func(function) = &request.statement;
        let function_name = function.name.to_string().to_lowercase();

        let arguments = match &function.args {
            FunctionArguments::List(arguments) => Some(json!({
                (ARGUMENT_TYPE_KEY): LIST_ARGUMENT_TYPE,
                (ARGUMENT_VALUE_KEY): arguments.args.iter().map(argument_to_json).collect::<Vec<_>>(),
            })),
            FunctionArguments::Subquery(query) => Some(json!({
                (ARGUMENT_TYPE_KEY): SUBQUERY_ARGUMENT_TYPE,
                (ARGUMENT_VALUE_KEY): query.to_string(),
            })),
            FunctionArguments::None => None,
        };

        Self {
            actor: request.query_ctx.current_user().username().to_string(),
            function: function_name,
            arguments,
        }
    }
}

/// An event describing the outcome of an ADMIN function execution.
#[derive(Debug)]
pub(crate) struct AdminFunctionEvent {
    actor: String,
    function_name: String,
    status: &'static str,
    output: JsonValue,
    payload: JsonValue,
}

impl AdminFunctionEvent {
    /// Creates a successful ADMIN function event.
    pub(crate) fn success(input: AdminFunctionEventInput, result: Option<&Value>) -> Self {
        let AdminFunctionEventInput {
            actor,
            function,
            arguments,
        } = input;
        Self {
            actor,
            function_name: function,
            status: SUCCEEDED_STATUS,
            output: result.map_or_else(
                || json!({}),
                |result| {
                    json!({
                        (RESULT_KEY): value_to_json(result),
                    })
                },
            ),
            payload: input_payload(arguments),
        }
    }

    /// Creates a failed ADMIN function event with the debug representation of the error.
    pub(crate) fn failure(input: AdminFunctionEventInput, error: &Error) -> Self {
        let AdminFunctionEventInput {
            actor,
            function,
            arguments,
        } = input;
        Self {
            actor,
            function_name: function,
            status: FAILED_STATUS,
            output: json!({
                (ERROR_KEY): format!("{error:?}"),
            }),
            payload: input_payload(arguments),
        }
    }

    /// Creates a failed ADMIN function event for a cancelled execution.
    pub(crate) fn cancelled(input: AdminFunctionEventInput) -> Self {
        Self::failure(input, &Error::AdminFunctionCancelled)
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
            &ACTOR_COLUMN,
            &ADMIN_FUNCTION_NAME_COLUMN,
            &ADMIN_FUNCTION_STATUS_COLUMN,
            &ADMIN_FUNCTION_OUTPUT_COLUMN,
        ])
    }

    fn extra_rows(&self) -> EventResult<Vec<Row>> {
        Ok(vec![Row {
            values: vec![
                ValueData::StringValue(self.actor.clone()).into(),
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

fn input_payload(arguments: Option<JsonValue>) -> JsonValue {
    let mut payload = json!({ VERSION_KEY: PAYLOAD_VERSION });
    if let Some(arguments) = arguments {
        payload[ARGUMENTS_KEY] = arguments;
    }
    payload
}

fn argument_to_json(argument: &FunctionArg) -> JsonValue {
    match argument {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(value))) => {
            sql_value_to_json(&value.value)
        }
        _ => JsonValue::String(argument.to_string()),
    }
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
        SqlValue::Placeholder(value) => JsonValue::String(value.clone()),
        _ => value
            .clone()
            .into_string()
            .map(JsonValue::String)
            .unwrap_or_else(|| JsonValue::String(value.to_string())),
    }
}

fn value_to_json(value: &Value) -> JsonValue {
    match value {
        Value::Float32(value) if !value.0.is_finite() => JsonValue::String(value.to_string()),
        Value::Float64(value) if !value.0.is_finite() => JsonValue::String(value.to_string()),
        _ => JsonValue::try_from(value.clone())
            .unwrap_or_else(|_| JsonValue::String(UNSUPPORTED.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use api::v1::Row;
    use api::v1::value::ValueData;
    use common_event_recorder::Event;
    use common_event_recorder::event_table::{
        ACTOR_COLUMN, ADMIN_FUNCTION_NAME_COLUMN, ADMIN_FUNCTION_OUTPUT_COLUMN,
        ADMIN_FUNCTION_STATUS_COLUMN, column_schemas, jsonb_value,
    };
    use datatypes::value::Value;
    use serde_json::json;
    use session::context::QueryContext;
    use sql::ast::{
        Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident, Value as SqlValue,
    };
    use sql::dialect::GreptimeDbDialect;
    use sql::parser::{ParseOptions, ParserContext};
    use sql::statements::admin::Admin;
    use sql::statements::statement::Statement;
    use sqlparser::ast::{DollarQuotedString, FunctionArgOperator, FunctionArgumentList};

    use crate::error::Error;
    use crate::statement::admin::AdminFunctionRequest;
    use crate::statement::admin::event::{
        ADMIN_FUNCTION_EVENT_TYPE, AdminFunctionEvent, AdminFunctionEventInput, sql_value_to_json,
        value_to_json,
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

    fn request_with_arguments(arguments: FunctionArguments) -> AdminFunctionRequest {
        let mut request = request("ADMIN plugin_function()");
        let Admin::Func(function) = &mut request.statement;
        function.args = arguments;
        request
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
                &ACTOR_COLUMN,
                &ADMIN_FUNCTION_NAME_COLUMN,
                &ADMIN_FUNCTION_STATUS_COLUMN,
                &ADMIN_FUNCTION_OUTPUT_COLUMN,
            ])
        );
        assert_eq!(
            event.extra_rows().unwrap(),
            vec![Row {
                values: vec![
                    ValueData::StringValue("greptime".to_string()).into(),
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
        let event = AdminFunctionEvent::success(input, Some(&Value::UInt64(3)));

        assert_admin_columns(&event, "flush_table", "Succeeded", json!({"result": 3}));
        assert_eq!(
            event.json_payload().unwrap(),
            json!({
                "version": 1,
                "arguments": {"type": "list", "value": ["greptime.public.demo"]},
            })
        );
    }

    #[test]
    fn successful_null_is_explicit() {
        let input = AdminFunctionEventInput::from_request(&request(
            "ADMIN migrate_region(NULL, NULL, NULL)",
        ));
        let event = AdminFunctionEvent::success(input, Some(&Value::Null));

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
                "arguments": {"type": "list", "value": [null, null, null]},
            })
        );
    }

    #[test]
    fn unknown_function_values_are_recorded() {
        let input = AdminFunctionEventInput::from_request(&request(
            "ADMIN plugin_function('plugin-value', NULL)",
        ));
        let event = AdminFunctionEvent::success(input, Some(&Value::from("plugin-result")));

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
                "arguments": {"type": "list", "value": ["plugin-value", null]},
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
                "arguments": {"type": "list", "value": ["greptime.public.demo"]},
            })
        );
    }

    #[test]
    fn cancellation_uses_debug_error_format() {
        let input = AdminFunctionEventInput::from_request(&request("ADMIN flush_table('demo')"));
        let event = AdminFunctionEvent::cancelled(input);

        assert_admin_columns(
            &event,
            "flush_table",
            "Failed",
            json!({"error": format!("{:?}", Error::AdminFunctionCancelled)}),
        );
    }

    #[test]
    fn subquery_arguments_use_display() {
        let Statement::Query(query) = ParserContext::create_with_dialect(
            "SELECT 1",
            &GreptimeDbDialect {},
            ParseOptions::default(),
        )
        .unwrap()
        .remove(0) else {
            panic!("expected query statement")
        };
        let input = AdminFunctionEventInput::from_request(&request_with_arguments(
            FunctionArguments::Subquery(Box::new(query.inner)),
        ));
        let event = AdminFunctionEvent::failure(
            input,
            &Error::BuildAdminFunctionArgs {
                msg: "subquery is not executable".to_string(),
            },
        );

        assert_eq!(
            event.json_payload().unwrap(),
            json!({
                "version": 1,
                "arguments": {"type": "subquery", "value": "SELECT 1"},
            })
        );
    }

    #[test]
    fn no_arguments_omits_arguments_field_and_empty_result() {
        let input =
            AdminFunctionEventInput::from_request(&request_with_arguments(FunctionArguments::None));
        let event = AdminFunctionEvent::success(input, None);

        assert_admin_columns(&event, "plugin_function", "Succeeded", json!({}));
        assert_eq!(event.json_payload().unwrap(), json!({"version": 1}));
    }

    #[test]
    fn empty_argument_list_is_recorded() {
        let input = AdminFunctionEventInput::from_request(&request("ADMIN plugin_function()"));
        let event = AdminFunctionEvent::success(input, Some(&Value::UInt64(0)));

        assert_eq!(
            event.json_payload().unwrap(),
            json!({
                "version": 1,
                "arguments": {"type": "list", "value": []},
            })
        );
    }

    #[test]
    fn records_extended_sql_values_and_non_literal_arguments() {
        let input = AdminFunctionEventInput::from_request(&request(
            "ADMIN plugin_function(1, true, NULL, X'48656c6c6f', table_name)",
        ));
        let event = AdminFunctionEvent::success(input, Some(&Value::UInt64(0)));

        assert_eq!(
            event.json_payload().unwrap(),
            json!({
                "version": 1,
                "arguments": {
                    "type": "list",
                    "value": [1, true, null, "48656c6c6f", "table_name"],
                },
            })
        );
    }

    #[test]
    fn string_literals_preserve_logical_content() {
        let values = [
            SqlValue::SingleQuotedString("single".to_string()),
            SqlValue::DoubleQuotedString("double".to_string()),
            SqlValue::DollarQuotedString(DollarQuotedString {
                value: "dollar".to_string(),
                tag: None,
            }),
            SqlValue::SingleQuotedRawStringLiteral("raw".to_string()),
            SqlValue::SingleQuotedByteStringLiteral("bytes".to_string()),
            SqlValue::NationalStringLiteral("national".to_string()),
            SqlValue::UnicodeStringLiteral("unicode".to_string()),
            SqlValue::HexStringLiteral("48656c6c6f".to_string()),
        ];

        let expected = [
            "single",
            "double",
            "dollar",
            "raw",
            "bytes",
            "national",
            "unicode",
            "48656c6c6f",
        ];
        for (value, expected) in values.iter().zip(expected) {
            assert_eq!(sql_value_to_json(value), json!(expected));
        }
    }

    #[test]
    fn non_literal_arguments_use_sql_display() {
        let arguments = FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![
                FunctionArg::Named {
                    name: Ident::new("named"),
                    arg: FunctionArgExpr::Expr(Expr::Identifier(Ident::new("value"))),
                    operator: FunctionArgOperator::RightArrow,
                },
                FunctionArg::Unnamed(FunctionArgExpr::Wildcard),
            ],
            clauses: vec![],
        };
        let input = AdminFunctionEventInput::from_request(&request_with_arguments(
            FunctionArguments::List(arguments),
        ));
        let event = AdminFunctionEvent::success(input, Some(&Value::UInt64(0)));

        assert_eq!(
            event.json_payload().unwrap(),
            json!({
                "version": 1,
                "arguments": {"type": "list", "value": ["named => value", "*"]},
            })
        );
        assert_eq!(
            sql_value_to_json(&SqlValue::Placeholder("$1".to_string())),
            json!("$1")
        );
    }

    #[test]
    fn serializes_binary_result() {
        assert_eq!(
            value_to_json(&Value::from(vec![1_u8, 2, 3])),
            json!([1, 2, 3])
        );
    }

    #[test]
    fn serializes_non_finite_float_results_as_strings() {
        assert_eq!(
            value_to_json(&Value::Float32(f32::NAN.into())),
            json!("NaN")
        );
        assert_eq!(
            value_to_json(&Value::Float32(f32::INFINITY.into())),
            json!("inf")
        );
        assert_eq!(
            value_to_json(&Value::Float32(f32::NEG_INFINITY.into())),
            json!("-inf")
        );
        assert_eq!(
            value_to_json(&Value::Float64(f64::NAN.into())),
            json!("NaN")
        );
        assert_eq!(
            value_to_json(&Value::Float64(f64::INFINITY.into())),
            json!("inf")
        );
        assert_eq!(
            value_to_json(&Value::Float64(f64::NEG_INFINITY.into())),
            json!("-inf")
        );
    }
}
