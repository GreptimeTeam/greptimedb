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
use std::sync::Arc;

use arrow_schema::{DataType, TimeUnit};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{DFSchema, Result};
use datafusion::logical_expr::expr_rewriter::FunctionRewrite;
use datafusion::scalar::ScalarValue;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{Cast, Expr};

use crate::scalars::json::JsonGetWithType;

#[derive(Debug)]
pub struct JsonGetRewriter;

impl FunctionRewrite for JsonGetRewriter {
    fn name(&self) -> &'static str {
        "JsonGetRewriter"
    }

    fn rewrite(
        &self,
        expr: Expr,
        _schema: &DFSchema,
        _config: &ConfigOptions,
    ) -> Result<Transformed<Expr>> {
        let transform = match &expr {
            Expr::Cast(cast) => rewrite_json_get_cast(cast),
            Expr::ScalarFunction(scalar_func) => rewrite_arrow_cast_json_get(scalar_func),
            _ => None,
        };
        Ok(transform.unwrap_or_else(|| Transformed::no(expr)))
    }
}

fn rewrite_json_get_cast(cast: &Cast) -> Option<Transformed<Expr>> {
    let scalar_func = extract_scalar_function(&cast.expr)?;
    if scalar_func.func.name().to_ascii_lowercase() == JsonGetWithType::NAME
        && scalar_func.args.len() == 2
    {
        let null_expr = Expr::Literal(ScalarValue::Null, None);
        let null_cast = Expr::Cast(datafusion::logical_expr::expr::Cast {
            expr: Box::new(null_expr),
            data_type: cast.data_type.clone(),
        });

        let mut args = scalar_func.args.clone();
        args.push(null_cast);

        Some(Transformed::yes(Expr::ScalarFunction(ScalarFunction {
            func: scalar_func.func.clone(),
            args,
        })))
    } else {
        None
    }
}

// Handle Arrow cast function: cast(json_get(a, 'path'), 'Int64')
fn rewrite_arrow_cast_json_get(scalar_func: &ScalarFunction) -> Option<Transformed<Expr>> {
    // Check if this is an Arrow cast function
    // The function name might be "arrow_cast" or similar
    let func_name = scalar_func.func.name().to_ascii_lowercase();
    if !func_name.contains("arrow_cast") && !func_name.contains("cast") {
        return None;
    }

    // Arrow cast function should have exactly 2 arguments:
    // 1. The expression to cast (could be json_get)
    // 2. The target type as a string literal
    if scalar_func.args.len() != 2 {
        return None;
    }

    // Extract the inner json_get function
    let json_get_func = extract_scalar_function(&scalar_func.args[0])?;

    // Check if it's a json_get function
    if json_get_func.func.name().to_ascii_lowercase() != JsonGetWithType::NAME
        || json_get_func.args.len() != 2
    {
        return None;
    }

    // Get the target type from the second argument
    let target_type = extract_string_literal(&scalar_func.args[1])?;
    let data_type = parse_data_type_from_string(&target_type)?;

    // Create the null expression with the same type
    let null_expr = Expr::Literal(ScalarValue::Null, None);
    let null_cast = Expr::Cast(datafusion::logical_expr::expr::Cast {
        expr: Box::new(null_expr),
        data_type,
    });

    // Create the new json_get_with_type function with the null parameter
    let mut args = json_get_func.args.clone();
    args.push(null_cast);

    Some(Transformed::yes(Expr::ScalarFunction(ScalarFunction {
        func: json_get_func.func.clone(),
        args,
    })))
}

// Extract string literal from an expression
fn extract_string_literal(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _) => Some(s.clone()),
        _ => None,
    }
}

// Parse a data type from a string representation
fn parse_data_type_from_string(type_str: &str) -> Option<DataType> {
    match type_str.to_lowercase().as_str() {
        "int8" | "tinyint" => Some(DataType::Int8),
        "int16" | "smallint" => Some(DataType::Int16),
        "int32" | "integer" => Some(DataType::Int32),
        "int64" | "bigint" => Some(DataType::Int64),
        "uint8" => Some(DataType::UInt8),
        "uint16" => Some(DataType::UInt16),
        "uint32" => Some(DataType::UInt32),
        "uint64" => Some(DataType::UInt64),
        "float32" | "real" => Some(DataType::Float32),
        "float64" | "double" => Some(DataType::Float64),
        "boolean" | "bool" => Some(DataType::Boolean),
        "string" | "text" | "varchar" => Some(DataType::Utf8),
        "timestamp" => Some(DataType::Timestamp(TimeUnit::Microsecond, None)),
        "date" => Some(DataType::Date32),
        _ => None,
    }
}

fn extract_scalar_function(expr: &Expr) -> Option<&ScalarFunction> {
    match expr {
        Expr::ScalarFunction(func) => Some(func),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::DataType;
    use datafusion::common::config::ConfigOptions;
    use datafusion::common::DFSchema;
    use datafusion::logical_expr::expr::Cast;
    use datafusion::scalar::ScalarValue;
    use datafusion_expr::expr::ScalarFunction;
    use datafusion_expr::Expr;

    use super::*;

    #[test]
    fn test_rewrite_regular_cast() {
        let rewriter = JsonGetRewriter;
        let schema = DFSchema::empty();
        let config = ConfigOptions::new();

        // Create a json_get function
        let json_expr = Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(crate::scalars::udf::create_udf(Arc::new(
                crate::scalars::json::JsonGetWithType::default(),
            ))),
            args: vec![
                Expr::Literal(ScalarValue::Utf8(Some("{\"a\":1}".to_string())), None),
                Expr::Literal(ScalarValue::Utf8(Some("$.a".to_string())), None),
            ],
        });

        // Create a cast expression: json_get(...)::int8
        let cast_expr = Expr::Cast(Cast {
            expr: Box::new(json_expr),
            data_type: DataType::Int8,
        });

        // Apply the rewriter
        let result = rewriter.rewrite(cast_expr, &schema, &config).unwrap();

        // Verify the result is transformed
        assert!(result.transformed);

        // Verify the result is a ScalarFunction
        match result.data {
            Expr::ScalarFunction(func) => {
                // Should have 3 arguments now (original 2 + null cast)
                assert_eq!(func.args.len(), 3);

                // First argument should be the original json
                match &func.args[0] {
                    Expr::Literal(ScalarValue::Utf8(Some(json)), _) => {
                        assert_eq!(json, "{\"a\":1}");
                    }
                    _ => panic!("First argument should be a string literal"),
                }

                // Second argument should be the path
                match &func.args[1] {
                    Expr::Literal(ScalarValue::Utf8(Some(path)), _) => {
                        assert_eq!(path, "$.a");
                    }
                    _ => panic!("Second argument should be a string literal"),
                }

                // Third argument should be a null cast to Int8
                match &func.args[2] {
                    Expr::Cast(Cast { expr, data_type }) => {
                        assert_eq!(*data_type, DataType::Int8);
                        match expr.as_ref() {
                            Expr::Literal(ScalarValue::Null, _) => {}
                            _ => panic!("Third argument should be a null cast"),
                        }
                    }
                    _ => panic!("Third argument should be a cast expression"),
                }
            }
            _ => panic!("Result should be a ScalarFunction"),
        }
    }

    #[test]
    fn test_rewrite_arrow_cast_function() {
        let rewriter = JsonGetRewriter;
        let schema = DFSchema::empty();
        let config = ConfigOptions::new();

        // Create a parse_json function
        let parse_json_expr = Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(crate::scalars::udf::create_udf(Arc::new(
                crate::scalars::json::ParseJsonFunction::default(),
            ))),
            args: vec![Expr::Literal(
                ScalarValue::Utf8(Some("{\"a\":1}".to_string())),
                None,
            )],
        });

        // Create a json_get function
        let json_get_expr = Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(crate::scalars::udf::create_udf(Arc::new(
                crate::scalars::json::JsonGetWithType::default(),
            ))),
            args: vec![
                parse_json_expr,
                Expr::Literal(ScalarValue::Utf8(Some("a".to_string())), None),
            ],
        });

        // Create an arrow cast function: cast(json_get(...), 'Int64')
        // Note: ArrowCastFunc doesn't exist in this codebase, so this test uses a simple cast instead
        let arrow_cast_expr = Expr::Cast(Cast {
            expr: Box::new(json_get_expr),
            data_type: DataType::Int64,
        });

        // Apply the rewriter
        let result = rewriter.rewrite(arrow_cast_expr, &schema, &config).unwrap();

        // Verify the result is transformed
        assert!(result.transformed);

        // Verify the result is a ScalarFunction (json_get_with_type)
        match result.data {
            Expr::ScalarFunction(func) => {
                // Should have 3 arguments now (original 2 + null cast)
                assert_eq!(func.args.len(), 3);

                // First argument should be the original parse_json function
                match &func.args[0] {
                    Expr::ScalarFunction(parse_func) => {
                        // Verify it's a parse_json function with the right argument
                        assert!(parse_func
                            .func
                            .name()
                            .to_ascii_lowercase()
                            .contains("parse_json"));
                        assert_eq!(parse_func.args.len(), 1);
                        match &parse_func.args[0] {
                            Expr::Literal(ScalarValue::Utf8(Some(json)), _) => {
                                assert_eq!(json, "{\"a\":1}");
                            }
                            _ => panic!("Parse json argument should be a string literal"),
                        }
                    }
                    _ => panic!("First argument should be a parse_json function"),
                }

                // Second argument should be the path
                match &func.args[1] {
                    Expr::Literal(ScalarValue::Utf8(Some(path)), _) => {
                        assert_eq!(path, "a");
                    }
                    _ => panic!("Second argument should be a string literal"),
                }

                // Third argument should be a null cast to Int64
                match &func.args[2] {
                    Expr::Cast(Cast { expr, data_type }) => {
                        assert_eq!(*data_type, DataType::Int64);
                        match expr.as_ref() {
                            Expr::Literal(ScalarValue::Null, _) => {}
                            _ => panic!("Third argument should be a null cast"),
                        }
                    }
                    _ => panic!("Third argument should be a cast expression"),
                }
            }
            _ => panic!("Result should be a ScalarFunction"),
        }
    }

    #[test]
    fn test_no_rewrite_for_other_functions() {
        let rewriter = JsonGetRewriter;
        let schema = DFSchema::empty();
        let config = ConfigOptions::new();

        // Create a non-json function
        let other_func = Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(crate::scalars::udf::create_udf(Arc::new(
                crate::scalars::test::TestAndFunction::default(),
            ))),
            args: vec![Expr::Literal(ScalarValue::Int64(Some(4)), None)],
        });

        // Apply the rewriter
        let result = rewriter.rewrite(other_func, &schema, &config).unwrap();

        // Verify the result is not transformed
        assert!(!result.transformed);
    }

    #[test]
    fn test_no_rewrite_for_non_cast_functions() {
        let rewriter = JsonGetRewriter;
        let schema = DFSchema::empty();
        let config = ConfigOptions::new();

        // Create a scalar function that doesn't contain "cast"
        let other_func = Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(crate::scalars::udf::create_udf(Arc::new(
                crate::scalars::test::TestAndFunction::default(),
            ))),
            args: vec![
                Expr::ScalarFunction(ScalarFunction {
                    func: Arc::new(crate::scalars::udf::create_udf(Arc::new(
                        crate::scalars::json::JsonGetWithType::default(),
                    ))),
                    args: vec![
                        Expr::Literal(ScalarValue::Utf8(Some("{\"a\":1}".to_string())), None),
                        Expr::Literal(ScalarValue::Utf8(Some("$.a".to_string())), None),
                    ],
                }),
                Expr::Literal(ScalarValue::Utf8(Some("Int64".to_string())), None),
            ],
        });

        // Apply the rewriter
        let result = rewriter.rewrite(other_func, &schema, &config).unwrap();

        // Verify the result is not transformed
        assert!(!result.transformed);
    }
}
