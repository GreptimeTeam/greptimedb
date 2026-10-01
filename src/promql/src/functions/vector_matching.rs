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

//! Cardinality checks for PromQL vector matching.

use datafusion::arrow::array::{Array, Int64Array};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::{ScalarUDF, Volatility};
use datafusion::physical_plan::ColumnarValue;
use datafusion_common::ScalarValue;
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl, Signature};
use datatypes::arrow::datatypes::DataType;

use crate::functions::extract_array;

/// The rule a repeated match group violates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MatchGroupViolation {
    /// Several series share a match group on the side that must hold one series per group.
    DuplicateOnOneSide { one_side_is_left: bool },
    /// One-to-one matching found several matches for the same group.
    ImplicitManyToOne,
    /// A group modifier left several matches with the same result label set.
    AmbiguousGroupLabels,
    /// A function rewrote labels so that several series share the same label set.
    DuplicateLabelSet,
}

impl MatchGroupViolation {
    pub const fn code(self) -> i64 {
        match self {
            Self::DuplicateOnOneSide {
                one_side_is_left: true,
            } => 0,
            Self::DuplicateOnOneSide {
                one_side_is_left: false,
            } => 1,
            Self::ImplicitManyToOne => 2,
            Self::AmbiguousGroupLabels => 3,
            Self::DuplicateLabelSet => 4,
        }
    }

    fn from_code(code: i64) -> DfResult<Self> {
        match code {
            0 => Ok(Self::DuplicateOnOneSide {
                one_side_is_left: true,
            }),
            1 => Ok(Self::DuplicateOnOneSide {
                one_side_is_left: false,
            }),
            2 => Ok(Self::ImplicitManyToOne),
            3 => Ok(Self::AmbiguousGroupLabels),
            4 => Ok(Self::DuplicateLabelSet),
            _ => Err(DataFusionError::Execution(format!(
                "invalid match group violation code: {code}"
            ))),
        }
    }

    fn message(&self, group: &str) -> String {
        match self {
            Self::DuplicateOnOneSide { one_side_is_left } => {
                let side = if *one_side_is_left { "left" } else { "right" };
                format!(
                    "found duplicate series for the match group {group} on the {side} hand-side \
                     of the operation; many-to-many matching not allowed: matching labels must \
                     be unique on one side"
                )
            }
            Self::ImplicitManyToOne => format!(
                "multiple matches for labels {group}: many-to-one matching must be explicit \
                 (group_left/group_right)"
            ),
            Self::AmbiguousGroupLabels => format!(
                "multiple matches for labels {group}: grouping labels must ensure unique matches"
            ),
            Self::DuplicateLabelSet => {
                "vector cannot contain metrics with the same labelset".to_string()
            }
        }
    }
}

/// Rejects a vector matching whose match groups are not unique.
///
/// Takes the per-group row count, violation code, then alternating label names and values.
/// Keeping the configuration in the arguments lets serialized plans reconstruct the function.
pub struct UniqueMatchGroup;

impl UniqueMatchGroup {
    pub const fn name() -> &'static str {
        "prom_assert_unique_match_group"
    }

    pub fn scalar_udf() -> ScalarUDF {
        ScalarUDF::new_from_impl(AssertUniqueMatchGroup {
            signature: Signature::variadic_any(Volatility::Volatile),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct AssertUniqueMatchGroup {
    signature: Signature,
}

impl AssertUniqueMatchGroup {
    fn scalar_value(arg: &ColumnarValue) -> DfResult<ScalarValue> {
        match arg {
            ColumnarValue::Scalar(value) => Ok(value.clone()),
            ColumnarValue::Array(array) if !array.is_empty() => {
                ScalarValue::try_from_array(array, 0)
            }
            _ => Err(DataFusionError::Execution(format!(
                "{} expects a scalar configuration argument",
                UniqueMatchGroup::name()
            ))),
        }
    }

    fn render_group(args: &[ColumnarValue], row: usize) -> DfResult<String> {
        let mut rendered = Vec::with_capacity(args.len() / 2);
        for pair in args.chunks_exact(2) {
            let label = match Self::scalar_value(&pair[0])? {
                ScalarValue::Utf8(Some(label)) => label,
                value => {
                    return Err(DataFusionError::Execution(format!(
                        "{} expects a string label name, found {value}",
                        UniqueMatchGroup::name()
                    )));
                }
            };
            let arg = &pair[1];
            let array = extract_array(arg)?;
            // A scalar argument was expanded to a single row above.
            let row = if array.len() == 1 { 0 } else { row };
            if row >= array.len() || array.is_null(row) {
                continue;
            }
            let value = array_value_to_string(&array, row)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
            if value.is_empty() {
                continue;
            }
            rendered.push(format!("{label}=\"{value}\""));
        }
        Ok(format!("{{{}}}", rendered.join(", ")))
    }
}

impl ScalarUDFImpl for AssertUniqueMatchGroup {
    fn name(&self) -> &str {
        UniqueMatchGroup::name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let Some((counts, remaining)) = args.args.split_first() else {
            return Err(DataFusionError::Execution(format!(
                "{} expects the match group row count as first argument",
                UniqueMatchGroup::name()
            )));
        };
        let Some((violation, labels)) = remaining.split_first() else {
            return Err(DataFusionError::Execution(format!(
                "{} expects a match group violation code",
                UniqueMatchGroup::name()
            )));
        };
        if labels.len() % 2 != 0 {
            return Err(DataFusionError::Execution(format!(
                "{} expects alternating label names and values",
                UniqueMatchGroup::name()
            )));
        }
        let violation = match Self::scalar_value(violation)? {
            ScalarValue::Int64(Some(code)) => MatchGroupViolation::from_code(code)?,
            value => {
                return Err(DataFusionError::Execution(format!(
                    "{} expects an Int64 violation code, found {value}",
                    UniqueMatchGroup::name()
                )));
            }
        };
        let counts = extract_array(counts)?;
        let counts = counts
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "{} expects an Int64 match group row count, found {}",
                    UniqueMatchGroup::name(),
                    counts.data_type()
                ))
            })?;

        if let Some(row) =
            (0..counts.len()).find(|row| !counts.is_null(*row) && counts.value(*row) > 1)
        {
            let group = Self::render_group(labels, row)?;
            return Err(DataFusionError::Execution(violation.message(&group)));
        }

        Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::StringArray;
    use datafusion::arrow::datatypes::Field;

    use super::*;

    fn invoke_with_violation(
        counts: Vec<i64>,
        hosts: Vec<Option<&str>>,
        violation: MatchGroupViolation,
    ) -> DfResult<ColumnarValue> {
        let udf = UniqueMatchGroup::scalar_udf();
        let number_rows = counts.len();
        udf.invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(Int64Array::from(counts))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(violation.code()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("host".to_string()))),
                ColumnarValue::Array(Arc::new(StringArray::from(hosts))),
            ],
            arg_fields: vec![
                Arc::new(Field::new("count", DataType::Int64, true)),
                Arc::new(Field::new("violation", DataType::Int64, false)),
                Arc::new(Field::new("label_name", DataType::Utf8, false)),
                Arc::new(Field::new("host", DataType::Utf8, true)),
            ],
            number_rows,
            return_field: Arc::new(Field::new("assert", DataType::Boolean, false)),
            config_options: Arc::new(Default::default()),
        })
    }

    fn invoke(counts: Vec<i64>, hosts: Vec<Option<&str>>) -> DfResult<ColumnarValue> {
        invoke_with_violation(counts, hosts, MatchGroupViolation::ImplicitManyToOne)
    }

    #[test]
    fn unique_groups_pass() {
        let result = invoke(vec![1, 1], vec![Some("a"), Some("b")]).unwrap();
        assert!(matches!(
            result,
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(true)))
        ));
    }

    #[test]
    fn duplicate_group_reports_its_labels() {
        let err = invoke(vec![1, 2], vec![Some("a"), Some("b")]).unwrap_err();
        assert!(
            err.to_string()
                .contains("multiple matches for labels {host=\"b\"}"),
            "{err}"
        );
    }

    #[test]
    fn null_label_is_omitted_from_the_group() {
        let err = invoke(vec![2], vec![None]).unwrap_err();
        assert!(err.to_string().contains("labels {}"), "{err}");
    }

    #[test]
    fn duplicate_label_set_uses_its_error_message() {
        let err = invoke_with_violation(
            vec![2],
            vec![Some("a")],
            MatchGroupViolation::DuplicateLabelSet,
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("vector cannot contain metrics with the same labelset"),
            "{err}"
        );
    }
}
