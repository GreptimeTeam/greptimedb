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

//! SQL forms of the hash used by `iceberg_hash` partition rules.
//!
//! `iceberg_hash(col)` is the same function partition rules evaluate, exposed so
//! that users can measure a distribution or verify which bucket a value falls
//! into. It must never diverge from the rule form: both are built from
//! [`partition::function::iceberg_hash_udf`].
//!
//! `iceberg_hash_bounds(n)` prints the breakpoints of an equal-width n-bucket
//! split, ready to paste into `PARTITION ON COLUMNS`. It only formats the
//! numbers; it does not create or change partitions.

use std::fmt;

use datafusion::arrow::datatypes::DataType;
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, Volatility};
use partition::function::{IcebergHash, iceberg_hash_udf};

use crate::function::Function;
use crate::function_registry::FunctionRegistry;

/// `iceberg_hash_bounds` hard limit. Each region carries its own WAL, memtable,
/// manifest and compaction stream, so four-digit bucket counts are already far
/// beyond any practical table.
const MAX_BUCKETS: i64 = 1024;

/// `iceberg_hash(col)`: the 32-bit Murmur3 that partition rules persist.
#[derive(Debug)]
pub(crate) struct IcebergHashFunction {
    signature: Signature,
}

impl Default for IcebergHashFunction {
    fn default() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl IcebergHashFunction {
    pub fn register(registry: &FunctionRegistry) {
        registry.register_scalar(Self::default());
    }
}

impl Function for IcebergHashFunction {
    fn name(&self) -> &str {
        IcebergHash::NAME
    }

    fn return_type(&self, _input_types: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Int32)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        iceberg_hash_udf().invoke_with_args(args)
    }
}

impl fmt::Display for IcebergHashFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ICEBERG_HASH")
    }
}

/// `iceberg_hash_bounds(n)`: the `n - 1` equal-width breakpoints of the hash
/// space, as comma separated `INT32` literals.
#[derive(Debug)]
pub(crate) struct IcebergHashBoundsFunction {
    signature: Signature,
}

impl Default for IcebergHashBoundsFunction {
    fn default() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl IcebergHashBoundsFunction {
    pub fn register(registry: &FunctionRegistry) {
        registry.register_scalar(Self::default());
    }
}

impl Function for IcebergHashBoundsFunction {
    fn name(&self) -> &str {
        "iceberg_hash_bounds"
    }

    fn return_type(&self, _input_types: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [arg] = args.args.as_slice() else {
            return Err(DataFusionError::Execution(format!(
                "{} expects one argument, got {}",
                self.name(),
                args.args.len()
            )));
        };
        let value = match arg {
            ColumnarValue::Scalar(scalar) => scalar,
            ColumnarValue::Array(_) => {
                return Err(DataFusionError::Execution(format!(
                    "{} expects a constant bucket count, not a column",
                    self.name()
                )));
            }
        };
        if value.is_null() {
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
        }
        let buckets = match value {
            ScalarValue::Int64(Some(v)) => *v,
            ScalarValue::Int32(Some(v)) => i64::from(*v),
            ScalarValue::Int16(Some(v)) => i64::from(*v),
            ScalarValue::Int8(Some(v)) => i64::from(*v),
            ScalarValue::UInt64(Some(v)) => i64::try_from(*v).map_err(|_| {
                DataFusionError::Execution(format!(
                    "{} bucket count is too large: {v}",
                    self.name()
                ))
            })?,
            ScalarValue::UInt32(Some(v)) => i64::from(*v),
            ScalarValue::UInt16(Some(v)) => i64::from(*v),
            ScalarValue::UInt8(Some(v)) => i64::from(*v),
            other => {
                return Err(DataFusionError::Execution(format!(
                    "{} expects an integer bucket count, got {other:?}",
                    self.name()
                )));
            }
        };
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            equal_split_bounds(buckets)?,
        ))))
    }
}

impl fmt::Display for IcebergHashBoundsFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ICEBERG_HASH_BOUNDS")
    }
}

/// Formats the breakpoints of `buckets` equal-width ranges over `INT32`:
/// `bi = i32::MIN + floor(i * 2^32 / buckets)`.
///
/// This is the same definition the `PARTITION BY HASH ... RANGES` design uses,
/// so a printed list can be pasted into `PARTITION ON COLUMNS` unchanged.
fn equal_split_bounds(buckets: i64) -> datafusion_common::Result<String> {
    if !(2..=MAX_BUCKETS).contains(&buckets) {
        return Err(DataFusionError::Execution(format!(
            "iceberg_hash_bounds expects a bucket count in [2, {MAX_BUCKETS}], got {buckets}"
        )));
    }

    let mut bounds = String::new();
    for i in 1..buckets {
        if i > 1 {
            bounds.push_str(", ");
        }
        let offset = ((i as i128) << 32) / buckets as i128;
        let bound = i32::MIN as i64 + offset as i64;
        bounds.push_str(&bound.to_string());
    }
    Ok(bounds)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Array, AsArray, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Int32Type};

    use super::*;
    use crate::function_registry::FUNCTION_REGISTRY;

    fn invoke(
        function: &impl Function,
        args: Vec<ColumnarValue>,
        rows: usize,
        return_type: DataType,
    ) -> ColumnarValue {
        function
            .invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields: vec![],
                number_rows: rows,
                return_field: Arc::new(Field::new("result", return_type, true)),
                config_options: Arc::new(Default::default()),
            })
            .unwrap()
    }

    fn into_scalar(value: ColumnarValue) -> ScalarValue {
        match value {
            ColumnarValue::Scalar(scalar) => scalar,
            ColumnarValue::Array(_) => panic!("expected a scalar result"),
        }
    }

    #[test]
    fn test_hash_matches_partition_rule() {
        let function = IcebergHashFunction::default();
        assert_eq!(function.name(), "iceberg_hash");
        assert_eq!(
            function.return_type(&[DataType::Utf8]).unwrap(),
            DataType::Int32
        );

        // Iceberg table spec, appendix B.
        let hashed = invoke(
            &function,
            vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                "iceberg".to_string(),
            )))],
            1,
            DataType::Int32,
        );
        assert_eq!(into_scalar(hashed), ScalarValue::Int32(Some(1210000089)));

        // An untyped NULL reaches the UDF as ScalarValue::Null.
        let untyped_null = invoke(
            &function,
            vec![ColumnarValue::Scalar(ScalarValue::Null)],
            1,
            DataType::Int32,
        );
        assert_eq!(into_scalar(untyped_null), ScalarValue::Int32(None));

        // Integers hash as Iceberg longs, and NULL hashes to NULL.
        let array = invoke(
            &function,
            vec![ColumnarValue::Array(Arc::new(Int64Array::from(vec![
                Some(34),
                None,
            ])))],
            2,
            DataType::Int32,
        )
        .to_array(2)
        .unwrap();
        let array = array.as_primitive::<Int32Type>();
        assert_eq!(array.value(0), 2017239379);
        assert!(array.is_null(1));
    }

    #[test]
    fn test_bounds_equal_split() {
        let function = IcebergHashBoundsFunction::default();
        assert_eq!(function.name(), "iceberg_hash_bounds");
        assert_eq!(
            function.return_type(&[DataType::Int64]).unwrap(),
            DataType::Utf8
        );

        let cases = [
            (2, "0"),
            (3, "-715827883, 715827882"),
            (4, "-1073741824, 0, 1073741824"),
            (
                8,
                "-1610612736, -1073741824, -536870912, 0, 536870912, \
                 1073741824, 1610612736",
            ),
        ];
        for (buckets, expected) in cases {
            let bounds = invoke(
                &function,
                vec![ColumnarValue::Scalar(ScalarValue::Int64(Some(buckets)))],
                1,
                DataType::Utf8,
            );
            assert_eq!(
                into_scalar(bounds),
                ScalarValue::Utf8(Some(expected.to_string())),
                "buckets: {buckets}"
            );
        }

        // Every integer width is a constant count; small widths must not be
        // rejected as non-integer input.
        for buckets in [
            ScalarValue::Int8(Some(4)),
            ScalarValue::Int16(Some(4)),
            ScalarValue::UInt8(Some(4)),
            ScalarValue::UInt16(Some(4)),
            ScalarValue::UInt32(Some(4)),
            ScalarValue::UInt64(Some(4)),
        ] {
            let label = format!("{buckets:?}");
            let bounds = invoke(
                &function,
                vec![ColumnarValue::Scalar(buckets)],
                1,
                DataType::Utf8,
            );
            assert_eq!(
                into_scalar(bounds),
                ScalarValue::Utf8(Some("-1073741824, 0, 1073741824".to_string())),
                "scalar: {label}"
            );
        }
    }

    #[test]
    fn test_bounds_rejects_invalid_input() {
        let function = IcebergHashBoundsFunction::default();
        let call = |arg: ColumnarValue| {
            function.invoke_with_args(ScalarFunctionArgs {
                args: vec![arg],
                arg_fields: vec![],
                number_rows: 1,
                return_field: Arc::new(Field::new("result", DataType::Utf8, true)),
                config_options: Arc::new(Default::default()),
            })
        };

        for buckets in [i64::MIN, -1, 0, 1, MAX_BUCKETS + 1] {
            let err = call(ColumnarValue::Scalar(ScalarValue::Int64(Some(buckets))))
                .unwrap_err()
                .to_string();
            assert!(
                err.contains("bucket count in [2, 1024]"),
                "unexpected error for {buckets}: {err}"
            );
        }

        let err = call(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "4".to_string(),
        ))))
        .unwrap_err()
        .to_string();
        assert!(err.contains("integer bucket count"), "{err}");

        let err = call(ColumnarValue::Array(Arc::new(Int64Array::from(vec![
            4_i64,
        ]))))
        .unwrap_err()
        .to_string();
        assert!(err.contains("constant bucket count"), "{err}");

        // A NULL bucket count propagates instead of failing.
        let null = call(ColumnarValue::Scalar(ScalarValue::Int64(None))).unwrap();
        assert_eq!(into_scalar(null), ScalarValue::Utf8(None));
    }

    /// Both functions must resolve and run through a plain SQL session, not just
    /// through the [Function] trait.
    #[tokio::test]
    async fn test_functions_run_through_sql() {
        use datafusion::prelude::SessionContext;

        use crate::scalars::udf::create_udf;

        let ctx = SessionContext::new();
        ctx.register_udf(create_udf(Arc::new(IcebergHashFunction::default())));
        ctx.register_udf(create_udf(Arc::new(IcebergHashBoundsFunction::default())));

        let batches = ctx
            .sql(
                "SELECT iceberg_hash('iceberg') AS h, iceberg_hash(34) AS i, \
                 iceberg_hash(NULL) AS n, iceberg_hash_bounds(4) AS b, \
                 iceberg_hash_bounds(CAST(4 AS SMALLINT)) AS sb",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(
            batches[0].column(0).as_primitive::<Int32Type>().value(0),
            1210000089
        );
        assert_eq!(
            batches[0].column(1).as_primitive::<Int32Type>().value(0),
            2017239379
        );
        assert!(batches[0].column(2).as_primitive::<Int32Type>().is_null(0));
        assert_eq!(
            batches[0].column(3).as_string::<i32>().value(0),
            "-1073741824, 0, 1073741824"
        );
        assert_eq!(
            batches[0].column(4).as_string::<i32>().value(0),
            "-1073741824, 0, 1073741824"
        );
    }

    #[test]
    fn test_hash_functions_are_registered() {
        for name in ["iceberg_hash", "iceberg_hash_bounds"] {
            assert!(
                FUNCTION_REGISTRY.get_function(name).is_some(),
                "{name} is not registered"
            );
        }
    }
}
