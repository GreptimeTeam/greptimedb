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

//! PromQL comparison operators and `sqrt()`, which follow IEEE 754.
//!
//! Arrow compares floats by total order, where NaN equals NaN and is greater than `+Inf`, and
//! DataFusion's `sqrt` rejects negative input. In PromQL every comparison involving NaN is false
//! except `!=`, and the square root of a negative number is NaN. NULL input gives NULL output.
//!
//! The functions report the column name of the expression they replace (`a < b`, `sqrt(a)`), so
//! query output columns are unchanged.

use std::sync::Arc;

use datafusion::arrow::array::{AsArray, BooleanArray, Float64Array};
use datafusion::arrow::datatypes::{DataType, Float64Type};
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::{
    ColumnarValue, Operator, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_expr::Expr;

/// A PromQL comparison operator on two float operands.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct IeeeComparison {
    op: Operator,
    signature: Signature,
}

impl IeeeComparison {
    /// The comparison UDF for `op`, which must be a comparison operator.
    pub fn scalar_udf(op: Operator) -> ScalarUDF {
        ScalarUDF::new_from_impl(Self {
            op,
            signature: Signature::uniform(2, vec![DataType::Float64], Volatility::Immutable),
        })
    }

    /// All comparison UDFs, for registering them where plans are decoded.
    pub fn all() -> Vec<ScalarUDF> {
        [
            Operator::Eq,
            Operator::NotEq,
            Operator::Lt,
            Operator::LtEq,
            Operator::Gt,
            Operator::GtEq,
        ]
        .into_iter()
        .map(Self::scalar_udf)
        .collect()
    }

    fn compare(&self, lhs: f64, rhs: f64) -> bool {
        match self.op {
            Operator::Eq => lhs == rhs,
            Operator::NotEq => lhs != rhs,
            Operator::Lt => lhs < rhs,
            Operator::LtEq => lhs <= rhs,
            Operator::Gt => lhs > rhs,
            Operator::GtEq => lhs >= rhs,
            _ => unreachable!("not a comparison operator: {}", self.op),
        }
    }
}

impl ScalarUDFImpl for IeeeComparison {
    fn name(&self) -> &str {
        match self.op {
            Operator::Eq => "prom_eq",
            Operator::NotEq => "prom_ne",
            Operator::Lt => "prom_lt",
            Operator::LtEq => "prom_le",
            Operator::Gt => "prom_gt",
            Operator::GtEq => "prom_ge",
            _ => unreachable!("not a comparison operator: {}", self.op),
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn schema_name(&self, args: &[Expr]) -> DfResult<String> {
        Ok(format!(
            "{} {} {}",
            args[0].schema_name(),
            self.op,
            args[1].schema_name()
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let lhs = arrays[0].as_primitive::<Float64Type>();
        let rhs = arrays[1].as_primitive::<Float64Type>();
        let result: BooleanArray = lhs
            .iter()
            .zip(rhs.iter())
            .map(|(lhs, rhs)| Some(self.compare(lhs?, rhs?)))
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

/// PromQL `sqrt()`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct IeeeSqrt {
    signature: Signature,
}

impl IeeeSqrt {
    pub fn scalar_udf() -> ScalarUDF {
        ScalarUDF::new_from_impl(Self {
            signature: Signature::uniform(1, vec![DataType::Float64], Volatility::Immutable),
        })
    }
}

impl ScalarUDFImpl for IeeeSqrt {
    fn name(&self) -> &str {
        "prom_sqrt"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Float64)
    }

    fn schema_name(&self, args: &[Expr]) -> DfResult<String> {
        Ok(format!("sqrt({})", args[0].schema_name()))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let input = arrays[0].as_primitive::<Float64Type>();
        let result: Float64Array = input.unary(f64::sqrt);
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Array, ArrayRef};
    use datafusion::arrow::datatypes::Field;
    use datafusion_common::config::ConfigOptions;

    use super::*;

    fn invoke(udf: &ScalarUDF, args: Vec<ArrayRef>, return_type: DataType) -> ArrayRef {
        let number_rows = args[0].len();
        let arg_fields = args
            .iter()
            .map(|array| Arc::new(Field::new("a", array.data_type().clone(), true)))
            .collect();
        let result = udf
            .invoke_with_args(ScalarFunctionArgs {
                args: args.into_iter().map(ColumnarValue::Array).collect(),
                arg_fields,
                number_rows,
                return_field: Arc::new(Field::new("r", return_type, true)),
                config_options: Arc::new(ConfigOptions::default()),
            })
            .unwrap();
        match result {
            ColumnarValue::Array(array) => array,
            ColumnarValue::Scalar(_) => unreachable!(),
        }
    }

    #[test]
    fn comparisons_follow_ieee_and_propagate_null() {
        let lhs: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(1.0),
            Some(f64::NAN),
            Some(f64::NAN),
            None,
            Some(f64::INFINITY),
        ]));
        let rhs: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(1.0),
            Some(f64::NAN),
            Some(1.0),
            Some(f64::NAN),
            Some(f64::NAN),
        ]));
        let expected = [
            (
                Operator::Eq,
                [Some(true), Some(false), Some(false), None, Some(false)],
            ),
            (
                Operator::NotEq,
                [Some(false), Some(true), Some(true), None, Some(true)],
            ),
            (
                Operator::Lt,
                [Some(false), Some(false), Some(false), None, Some(false)],
            ),
            (
                Operator::LtEq,
                [Some(true), Some(false), Some(false), None, Some(false)],
            ),
            (
                Operator::Gt,
                [Some(false), Some(false), Some(false), None, Some(false)],
            ),
            (
                Operator::GtEq,
                [Some(true), Some(false), Some(false), None, Some(false)],
            ),
        ];
        for (op, expected) in expected {
            let result = invoke(
                &IeeeComparison::scalar_udf(op),
                vec![lhs.clone(), rhs.clone()],
                DataType::Boolean,
            );
            let result: Vec<_> = result.as_boolean().iter().collect();
            assert_eq!(result, expected, "{op}");
        }
    }

    #[test]
    fn sqrt_of_negative_is_nan() {
        let input: ArrayRef = Arc::new(Float64Array::from(vec![Some(4.0), Some(-1.0), None]));
        let result = invoke(&IeeeSqrt::scalar_udf(), vec![input], DataType::Float64);
        let result = result.as_primitive::<Float64Type>();
        assert_eq!(result.value(0), 2.0);
        assert!(result.value(1).is_nan());
        assert!(result.is_null(2));
    }

    #[test]
    fn column_names_match_the_replaced_expressions() {
        let a = datafusion_expr::col("a");
        let b = datafusion_expr::lit(1.5_f64);
        let comparison =
            IeeeComparison::scalar_udf(Operator::LtEq).call(vec![a.clone(), b.clone()]);
        assert_eq!(
            comparison.schema_name().to_string(),
            a.clone().lt_eq(b).schema_name().to_string()
        );
        let sqrt = IeeeSqrt::scalar_udf().call(vec![a.clone()]);
        assert_eq!(sqrt.schema_name().to_string(), "sqrt(a)");
    }
}
