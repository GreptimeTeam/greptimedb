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

//! Concretizes the result-type argument of JSON2 `json_get` expressions.
//!
//! This rule runs before distributed planning so remote plans carry explicit result types. A
//! matching JSON2 type hint is used; otherwise it injects `STRING`. Existing explicit result
//! types are preserved.

use std::collections::HashMap;

use arrow_schema::extension::ExtensionType;
use common_function::scalars::json::json_get::JsonGetWithType;
use datafusion::config::ConfigOptions;
use datafusion::datasource::DefaultTableSource;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Result, ScalarValue, plan_datafusion_err};
use datafusion_expr::{Expr, LogicalPlan};
use datafusion_optimizer::analyzer::AnalyzerRule;
use datafusion_optimizer::utils::NamePreserver;
use datatypes::extension::json::{
    Json2ExtensionType, is_json2_extension_type, parse_legacy_json2_settings,
};
use datatypes::json::JsonSettings;
use datatypes::types::json_type::JsonNativeType;

/// Makes the result type of untyped JSON2 `json_get` expressions explicit.
#[derive(Debug)]
pub(crate) struct JsonGetResultTypeRule;

impl AnalyzerRule for JsonGetResultTypeRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        inject_json_get_result_types(plan).map(|transformed| transformed.data)
    }

    fn name(&self) -> &str {
        "JsonGetResultTypeRule"
    }
}

/// Adds the result-type argument to untyped JSON2 path accesses.
///
/// The third `json_get` argument is the expression result type as well as the storage read type.
/// Never replace an existing argument: it represents an explicit SQL cast (or another prior type
/// coercion) and must take precedence over a JSON2 type hint. A path without a matching hint uses
/// `STRING`.
pub(crate) fn inject_json_get_result_types(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let json_type_hints = collect_json_type_hints(&plan)?;
    plan.transform_up(|plan| {
        let mut changed = false;
        let name_preserver = NamePreserver::new(&plan);
        let expressions = plan
            .expressions()
            .into_iter()
            .map(|expr| {
                let original_name = name_preserver.save(&expr);
                let transformed = expr.transform_up(|mut expr| {
                    let Expr::ScalarFunction(function) = &mut expr else {
                        return Ok(Transformed::no(expr));
                    };
                    if !function.name().eq_ignore_ascii_case(JsonGetWithType::NAME)
                        || function.args.len() != 2
                    {
                        return Ok(Transformed::no(expr));
                    }

                    let Some(Expr::Column(column)) = function.args.first() else {
                        return Ok(Transformed::no(expr));
                    };
                    let Some(path) = json_get_path(function) else {
                        return Ok(Transformed::no(expr));
                    };
                    if !json_type_hints.contains_key(&column.name) {
                        return Ok(Transformed::no(expr));
                    }
                    let json_type = json_type_from_hint(&json_type_hints, &column.name, path)
                        .unwrap_or(JsonNativeType::String);

                    let type_arg = ScalarValue::try_new_null(&json_type.as_arrow_type())?;
                    function.args.push(Expr::Literal(type_arg, None));
                    Ok(Transformed::yes(expr))
                })?;
                changed |= transformed.transformed;
                Ok(original_name.restore(transformed.data))
            })
            .collect::<Result<Vec<_>>>()?;

        if changed {
            let inputs = plan.inputs().into_iter().cloned().collect();
            Ok(Transformed::yes(plan.with_new_exprs(expressions, inputs)?))
        } else {
            Ok(Transformed::no(plan))
        }
    })
}

/// Collects JSON2 type hints from table-scan schemas.
pub(crate) fn collect_json_type_hints(plan: &LogicalPlan) -> Result<HashMap<String, JsonSettings>> {
    let mut json_type_hints = HashMap::new();

    plan.apply(|plan| {
        let LogicalPlan::TableScan(table_scan) = plan else {
            return Ok(TreeNodeRecursion::Continue);
        };
        let Some(source) = table_scan.source.downcast_ref::<DefaultTableSource>() else {
            return Ok(TreeNodeRecursion::Continue);
        };

        for field in source.table_provider.schema().fields() {
            if !is_json2_extension_type(field) {
                continue;
            }
            let settings = if field.extension_type_name() == Some(Json2ExtensionType::NAME) {
                let extension = field
                    .try_extension_type::<Json2ExtensionType>()
                    .map_err(|e| plan_datafusion_err!("invalid JSON2 extension metadata: {e}"))?;
                Some(extension.metadata().json_settings().clone())
            } else {
                parse_legacy_json2_settings(field.metadata())
                    .map_err(|e| plan_datafusion_err!("invalid JSON2 extension metadata: {e}"))?
            };
            json_type_hints.insert(field.name().clone(), settings.unwrap_or_default());
        }
        Ok(TreeNodeRecursion::Continue)
    })?;

    Ok(json_type_hints)
}

/// Returns the literal JSON path argument of a `json_get` call.
pub(crate) fn json_get_path(function: &datafusion_expr::expr::ScalarFunction) -> Option<&str> {
    function
        .args
        .get(1)
        .and_then(|expr| expr.as_literal())
        .and_then(|value| value.try_as_str())
        .flatten()
}

/// Returns the configured type for an exact JSON2 hint path.
pub(crate) fn json_type_from_hint(
    json_type_hints: &HashMap<String, JsonSettings>,
    column: &str,
    path: &str,
) -> Option<JsonNativeType> {
    if path.contains('[') {
        return None;
    }

    json_type_hints.get(column).and_then(|settings| {
        settings
            .type_hints()
            .iter()
            .find(|hint| hint.path.iter().map(String::as_str).eq(path.split('.')))
            .map(|hint| JsonNativeType::from(&hint.data_type))
    })
}
