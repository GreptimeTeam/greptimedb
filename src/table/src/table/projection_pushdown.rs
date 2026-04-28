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

#![allow(dead_code)]

use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::physical_plan::expressions::{Column, Literal};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion_common::ScalarValue;
use store_api::storage::{NestedPath, ProjectionInput};

const JSON_GET_FUNCTIONS: &[&str] = &[
    "json_get",
    "json_get_int",
    "json_get_float",
    "json_get_bool",
    "json_get_string",
    "json_get_object",
];

pub(crate) fn extract_nested_paths(projection: &ProjectionExec) -> BTreeSet<NestedPath> {
    let mut nested_paths = BTreeSet::new();
    for expr in projection.expr() {
        collect_nested_paths_from_expr(expr.expr.as_ref(), &mut nested_paths);
    }
    nested_paths
}

pub(crate) fn apply_nested_paths(
    base: &ProjectionInput,
    nested_paths: impl IntoIterator<Item = NestedPath>,
) -> Option<ProjectionInput> {
    let mut merged = base.nested_paths.iter().cloned().collect::<BTreeSet<_>>();
    let original_len = merged.len();
    merged.extend(nested_paths);

    if merged.len() == original_len {
        return None;
    }

    Some(ProjectionInput {
        projection: base.projection.clone(),
        nested_paths: merged.into_iter().collect(),
    })
}

fn collect_nested_paths_from_expr(
    expr: &dyn PhysicalExpr,
    nested_paths: &mut BTreeSet<NestedPath>,
) {
    if let Some(path) = extract_json_get_path(expr) {
        let _ = nested_paths.insert(path);
    }

    for child in expr.children() {
        collect_nested_paths_from_expr(child.as_ref(), nested_paths);
    }
}

fn extract_json_get_path(expr: &dyn PhysicalExpr) -> Option<NestedPath> {
    let func = expr.as_any().downcast_ref::<ScalarFunctionExpr>()?;
    let name = func.name().to_ascii_lowercase();
    if !JSON_GET_FUNCTIONS
        .iter()
        .any(|candidate| *candidate == name)
    {
        return None;
    }

    let column = func.args().first()?.as_any().downcast_ref::<Column>()?;
    let path = func.args().get(1).and_then(extract_string_literal)?;
    let mut nested_path = vec![column.name().to_string()];
    nested_path.extend(parse_json_get_path(path)?);
    Some(nested_path)
}

fn extract_string_literal(expr: &Arc<dyn PhysicalExpr>) -> Option<&str> {
    let literal = expr.as_any().downcast_ref::<Literal>()?;
    match literal.value() {
        ScalarValue::Utf8(Some(path))
        | ScalarValue::LargeUtf8(Some(path))
        | ScalarValue::Utf8View(Some(path)) => Some(path.as_str()),
        _ => None,
    }
}

fn parse_json_get_path(path: &str) -> Option<Vec<String>> {
    let path = path.trim();
    if path.is_empty() {
        return None;
    }

    let path = if let Some(rest) = path.strip_prefix("$.") {
        rest
    } else if let Some(rest) = path.strip_prefix('$') {
        rest
    } else {
        path
    };

    let mut segments = Vec::new();
    let bytes = path.as_bytes();
    let mut idx = 0;
    while idx < bytes.len() {
        match bytes[idx] {
            b'.' => idx += 1,
            b'[' => {
                let (segment, next) = parse_bracket_segment(path, idx)?;
                segments.push(segment);
                idx = next;
            }
            _ => {
                let start = idx;
                while idx < bytes.len() && bytes[idx] != b'.' && bytes[idx] != b'[' {
                    idx += 1;
                }
                let segment = path[start..idx].trim();
                if segment.is_empty() {
                    return None;
                }
                segments.push(segment.to_string());
            }
        }
    }

    (!segments.is_empty()).then_some(segments)
}

fn parse_bracket_segment(path: &str, start: usize) -> Option<(String, usize)> {
    let bytes = path.as_bytes();
    let quote = *bytes.get(start + 1)?;
    if quote != b'"' && quote != b'\'' {
        return None;
    }

    let mut idx = start + 2;
    let mut escaped = false;
    let mut segment = String::new();
    while idx < bytes.len() {
        let ch = bytes[idx] as char;
        if escaped {
            segment.push(ch);
            escaped = false;
            idx += 1;
            continue;
        }
        match bytes[idx] {
            b'\\' => {
                escaped = true;
                idx += 1;
            }
            current if current == quote => {
                if *bytes.get(idx + 1)? != b']' {
                    return None;
                }
                return Some((segment, idx + 2));
            }
            _ => {
                segment.push(ch);
                idx += 1;
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::common::config::ConfigOptions;
    use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl};
    use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::expressions::{Column, Literal};
    use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
    use datafusion_common::{DataFusionError, ScalarValue};
    use datafusion_expr::{Signature, Volatility};

    use super::*;

    #[test]
    fn collects_nested_paths_from_json_get_projection() {
        let projection = projection_exec(vec![ProjectionExpr::new(
            json_get_expr("json_get", vec!["data".into(), "day".into(), "INT".into()]),
            "day",
        )]);

        assert_eq!(
            extract_nested_paths(&projection),
            BTreeSet::from([vec!["data".to_string(), "day".to_string()]])
        );
    }

    #[test]
    fn collects_nested_paths_recursively_and_dedups() {
        let nested = json_get_expr("json_get", vec!["data".into(), "a.b".into(), "INT".into()]);
        let wrapped = scalar_function_expr(
            "outer_wrapper",
            vec![
                nested.clone(),
                Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
            ],
        );
        let projection = projection_exec(vec![
            ProjectionExpr::new(wrapped, "sum"),
            ProjectionExpr::new(
                json_get_expr("json_get_int", vec!["data".into(), "a.b".into()]),
                "again",
            ),
        ]);

        assert_eq!(
            extract_nested_paths(&projection),
            BTreeSet::from([vec!["data".to_string(), "a".to_string(), "b".to_string()]])
        );
    }

    #[test]
    fn parses_bracket_quoted_json_path_segments() {
        let projection = projection_exec(vec![ProjectionExpr::new(
            json_get_expr("json_get_string", vec!["data".into(), "a[\"b.c\"]".into()]),
            "quoted",
        )]);

        assert_eq!(
            extract_nested_paths(&projection),
            BTreeSet::from([vec!["data".to_string(), "a".to_string(), "b.c".to_string()]])
        );
    }

    #[test]
    fn ignores_non_column_and_non_literal_json_get_args() {
        let non_column_base = scalar_function_expr(
            "inner_wrapper",
            vec![
                Arc::new(Column::new("data", 0)),
                Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
            ],
        );
        let non_column = scalar_function_expr(
            "json_get",
            vec![
                non_column_base,
                Arc::new(Literal::new(ScalarValue::Utf8(Some("day".into())))),
                Arc::new(Literal::new(ScalarValue::Utf8(Some("INT".into())))),
            ],
        );
        let non_literal = scalar_function_expr(
            "json_get",
            vec![
                Arc::new(Column::new("data", 0)),
                Arc::new(Column::new("path", 1)),
                Arc::new(Literal::new(ScalarValue::Utf8(Some("INT".into())))),
            ],
        );
        let projection = projection_exec(vec![
            ProjectionExpr::new(non_column, "non_column"),
            ProjectionExpr::new(non_literal, "non_literal"),
        ]);

        assert!(extract_nested_paths(&projection).is_empty());
    }

    #[test]
    fn merges_nested_paths_into_projection_input() {
        let base = ProjectionInput::new(vec![2])
            .with_nested_paths(vec![vec!["data".to_string(), "month".to_string()]]);

        let merged = apply_nested_paths(
            &base,
            vec![
                vec!["data".to_string(), "day".to_string()],
                vec!["data".to_string(), "month".to_string()],
            ],
        )
        .unwrap();

        assert_eq!(merged.projection, vec![2]);
        assert_eq!(
            merged.nested_paths,
            vec![
                vec!["data".to_string(), "day".to_string()],
                vec!["data".to_string(), "month".to_string()],
            ]
        );
    }

    #[test]
    fn returns_none_when_projection_input_is_unchanged() {
        let base = ProjectionInput::new(vec![2])
            .with_nested_paths(vec![vec!["data".to_string(), "day".to_string()]]);

        assert!(
            apply_nested_paths(&base, vec![vec!["data".to_string(), "day".to_string()]]).is_none()
        );
    }

    fn projection_exec(exprs: Vec<ProjectionExpr>) -> ProjectionExec {
        ProjectionExec::try_new(exprs, Arc::new(EmptyExec::new(test_schema()))).unwrap()
    }

    fn json_get_expr(name: &'static str, args: Vec<String>) -> Arc<dyn PhysicalExpr> {
        let mut physical_args: Vec<Arc<dyn PhysicalExpr>> = Vec::with_capacity(args.len());
        physical_args.push(Arc::new(Column::new(&args[0], 0)));
        for arg in args.into_iter().skip(1) {
            physical_args.push(Arc::new(Literal::new(ScalarValue::Utf8(Some(arg)))));
        }
        scalar_function_expr(name, physical_args)
    }

    fn scalar_function_expr(
        name: &'static str,
        args: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Arc<dyn PhysicalExpr> {
        let udf = ScalarUDF::new_from_impl(TestJsonFunction::new(name, args.len()));
        Arc::new(
            ScalarFunctionExpr::try_new(
                Arc::new(udf),
                args,
                test_schema().as_ref(),
                Arc::new(ConfigOptions::default()),
            )
            .unwrap(),
        )
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("data", DataType::Utf8, true),
            Field::new("path", DataType::Utf8, true),
            Field::new("lhs", DataType::Int32, true),
            Field::new("rhs", DataType::Int32, true),
        ]))
    }

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct TestJsonFunction {
        name: &'static str,
        signature: Signature,
    }

    impl TestJsonFunction {
        fn new(name: &'static str, num_args: usize) -> Self {
            Self {
                name,
                signature: Signature::any(num_args, Volatility::Immutable),
            }
        }
    }

    impl std::fmt::Display for TestJsonFunction {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.name)
        }
    }

    impl ScalarUDFImpl for TestJsonFunction {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            self.name
        }

        fn return_type(&self, _input_types: &[DataType]) -> datafusion_common::Result<DataType> {
            Ok(DataType::Utf8View)
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn invoke_with_args(
            &self,
            _args: ScalarFunctionArgs,
        ) -> datafusion_common::Result<ColumnarValue> {
            Err(DataFusionError::Execution(
                "test json function should not be invoked".to_string(),
            ))
        }
    }
}
