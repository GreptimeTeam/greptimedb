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
//!
//! `iceberg_hash_breakpoints(v1, v2, ...)` hashes the given constant values
//! (any order, one shared hashable type) and prints them sorted, for building
//! buckets around known key values instead of around equal widths.
//!
//! `iceberg_hash_ranges_sql(column, n)` renders the whole rule list for
//! `PARTITION ON COLUMNS` from an equal-width split, so it can be pasted
//! without expanding the breakpoints by hand.

use std::fmt;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, Volatility};
use partition::function::{IcebergHash, iceberg_hash_udf, iceberg_hash_values};

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

    /// Returns the bucket count a constant scalar names, or an error naming
    /// the rejected input.
    fn bucket_count(scalar: &ScalarValue) -> datafusion_common::Result<i64> {
        let count = match scalar {
            ScalarValue::Int64(Some(v)) => *v,
            ScalarValue::Int32(Some(v)) => i64::from(*v),
            ScalarValue::Int16(Some(v)) => i64::from(*v),
            ScalarValue::Int8(Some(v)) => i64::from(*v),
            ScalarValue::UInt64(Some(v)) => i64::try_from(*v).map_err(|_| {
                DataFusionError::Execution(format!(
                    "iceberg_hash_bounds bucket count is too large: {v}"
                ))
            })?,
            ScalarValue::UInt32(Some(v)) => i64::from(*v),
            ScalarValue::UInt16(Some(v)) => i64::from(*v),
            ScalarValue::UInt8(Some(v)) => i64::from(*v),
            other => {
                return Err(DataFusionError::Execution(format!(
                    "iceberg_hash_bounds expects an integer bucket count, got {other:?}"
                )));
            }
        };
        Ok(count)
    }

    /// Returns the `n - 1` equal-width breakpoints a constant bucket count
    /// names, or an error when the count is not a constant integer in
    /// `[2, MAX_BUCKETS]`. Shared with [`IcebergHashRangesSqlFunction`].
    fn breakpoints_of(&self, scalar: &ScalarValue) -> datafusion_common::Result<Vec<i32>> {
        equal_split_breakpoints(Self::bucket_count(scalar)?)
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
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            self.breakpoints_of(value)?
                .iter()
                .map(i32::to_string)
                .collect::<Vec<_>>()
                .join(", "),
        ))))
    }
}

impl fmt::Display for IcebergHashBoundsFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ICEBERG_HASH_BOUNDS")
    }
}

/// `iceberg_hash_breakpoints(v1, v2, ...)`: the sorted, comma separated hash
/// values of the given constant values, as `INT32` literals.
///
/// The values do not need to be sorted; the output is. Every value must be a
/// hashable constant of one shared type, exactly like the values a partition
/// rule compares the hashed column against. NULL values hash to NULL and are
/// dropped, and so is an empty argument list, so the output never contains a
/// NULL entry: those inputs are rejected as unusable breakpoints.
#[derive(Debug)]
pub(crate) struct IcebergHashBreakpointsFunction {
    signature: Signature,
}

impl Default for IcebergHashBreakpointsFunction {
    fn default() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl IcebergHashBreakpointsFunction {
    pub fn register(registry: &FunctionRegistry) {
        registry.register_scalar(Self::default());
    }

    /// Hashes the scalar arguments into sorted breakpoints, or an error when
    /// the arguments cannot form a usable breakpoint list.
    fn breakpoints(&self, args: &ScalarFunctionArgs) -> datafusion_common::Result<String> {
        if args.args.is_empty() {
            return Err(DataFusionError::Execution(format!(
                "{} expects at least one value, got none",
                self.name()
            )));
        }
        let mut scalars: Vec<i32> = Vec::with_capacity(args.args.len());
        for (position, arg) in args.args.iter().enumerate() {
            let scalar = match arg {
                ColumnarValue::Scalar(scalar) => scalar,
                ColumnarValue::Array(_) => {
                    return Err(DataFusionError::Execution(format!(
                        "{} expects constant values, not a column (argument {})",
                        self.name(),
                        position + 1
                    )));
                }
            };
            let hashed = if scalar.is_null() {
                // A NULL hash can never match a comparison bound, so a NULL
                // value cannot be a breakpoint. Reject it instead of silently
                // dropping it: the user asked for that value's breakpoint.
                return Err(DataFusionError::Execution(format!(
                    "{} cannot use NULL as a breakpoint (argument {}): NULL rows fall in the lowest bucket",
                    self.name(),
                    position + 1
                )));
            } else {
                into_hashed_scalar(self.name(), position + 1, scalar)?
            };
            scalars.push(hashed);
        }
        scalars.sort_unstable();
        scalars.dedup();
        Ok(scalars
            .iter()
            .map(i32::to_string)
            .collect::<Vec<_>>()
            .join(", "))
    }
}

impl Function for IcebergHashBreakpointsFunction {
    fn name(&self) -> &str {
        "iceberg_hash_breakpoints"
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
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            self.breakpoints(&args)?,
        ))))
    }
}

impl fmt::Display for IcebergHashBreakpointsFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ICEBERG_HASH_BREAKPOINTS")
    }
}

/// `iceberg_hash_ranges_sql(column, n)`: the ready-to-paste rule list for
/// `PARTITION ON COLUMNS`, built from an equal-width `n`-bucket split.
///
/// The first argument is the column name as a constant string, the second the
/// constant bucket count accepted by [`IcebergHashBoundsFunction`]. The output
/// is the complete expression list: `iceberg_hash(c) < b1`,
/// `iceberg_hash(c) >= b1 AND iceberg_hash(c) < b2`, ..., one rule per bucket,
/// with the same breakpoints `iceberg_hash_bounds(n)` prints.
///
/// It only renders text; it does not create or change any partition.
#[derive(Debug)]
pub(crate) struct IcebergHashRangesSqlFunction {
    signature: Signature,
}

impl Default for IcebergHashRangesSqlFunction {
    fn default() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl IcebergHashRangesSqlFunction {
    pub fn register(registry: &FunctionRegistry) {
        registry.register_scalar(Self::default());
    }

    /// Renders the rule list from the hashed column name and the bucket count.
    fn ranges_sql(&self, args: &ScalarFunctionArgs) -> datafusion_common::Result<Option<String>> {
        if args.args.len() != 2 {
            return Err(DataFusionError::Execution(format!(
                "{} expects (column, buckets), got {} arguments",
                self.name(),
                args.args.len()
            )));
        }
        let [name, buckets] = args.args.as_slice() else {
            unreachable!("length checked above");
        };
        let (name, buckets) = match (name, buckets) {
            (ColumnarValue::Scalar(name), ColumnarValue::Scalar(buckets)) => (name, buckets),
            (ColumnarValue::Array(_), _) | (_, ColumnarValue::Array(_)) => {
                return Err(DataFusionError::Execution(format!(
                    "{} expects constant arguments (column name, bucket count)",
                    self.name()
                )));
            }
        };
        if name.is_null() || buckets.is_null() {
            return Ok(None);
        }
        let column = match name {
            ScalarValue::Utf8(Some(column)) | ScalarValue::LargeUtf8(Some(column)) => column,
            other => {
                return Err(DataFusionError::Execution(format!(
                    "{} expects a column name string, got {other:?}",
                    self.name()
                )));
            }
        };
        if column.is_empty() {
            return Err(DataFusionError::Execution(format!(
                "{} expects a non-empty column name",
                self.name()
            )));
        }
        let bounds = IcebergHashBoundsFunction::default().breakpoints_of(buckets)?;
        let mut rules = Vec::with_capacity(bounds.len() + 1);
        for (index, bound) in bounds.iter().copied().enumerate() {
            if index == 0 {
                // First rule: only the upper bound; NULL rows land here.
                rules.push(format!("iceberg_hash({column}) < {bound}"));
            } else {
                let lower = bounds[index - 1];
                rules.push(format!(
                    "iceberg_hash({column}) >= {lower} AND iceberg_hash({column}) < {bound}"
                ));
            }
        }
        // Last rule: only the lower bound.
        rules.push(format!(
            "iceberg_hash({column}) >= {}",
            bounds[bounds.len() - 1]
        ));
        Ok(Some(rules.join(",\n")))
    }
}

impl Function for IcebergHashRangesSqlFunction {
    fn name(&self) -> &str {
        "iceberg_hash_ranges_sql"
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
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
            self.ranges_sql(&args)?,
        )))
    }
}

impl fmt::Display for IcebergHashRangesSqlFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ICEBERG_HASH_RANGES_SQL")
    }
}

/// Hashes one constant by evaluating it through the shared array form, so the
/// value goes through exactly the code a partition rule evaluates.
fn into_hashed_scalar(
    name: &str,
    position: usize,
    scalar: &ScalarValue,
) -> datafusion_common::Result<i32> {
    let array: ArrayRef = scalar.to_array()?;
    let hashed = iceberg_hash_values(array.as_ref()).map_err(|error| {
        DataFusionError::Execution(format!("{name} argument {position}: {error}"))
    })?;
    let value = ScalarValue::try_from_array(&hashed, 0)?;
    match value {
        ScalarValue::Int32(Some(hashed)) => Ok(hashed),
        ScalarValue::Int32(None) => Err(DataFusionError::Execution(format!(
            "{name} argument {position}: NULL hash"
        ))),
        other => Err(DataFusionError::Execution(format!(
            "{name} argument {position}: unexpected hash {other:?}"
        ))),
    }
}

/// Returns the `n - 1` equal-width breakpoints of `buckets` over `INT32`,
/// following `bi = i32::MIN + floor(i * 2^32 / n)`.
fn equal_split_breakpoints(buckets: i64) -> datafusion_common::Result<Vec<i32>> {
    if !(2..=MAX_BUCKETS).contains(&buckets) {
        return Err(DataFusionError::Execution(format!(
            "iceberg_hash_bounds expects a bucket count in [2, {MAX_BUCKETS}], got {buckets}"
        )));
    }

    Ok((1..buckets)
        .map(|i| {
            let offset = ((i as i128) << 32) / buckets as i128;
            i32::MIN as i64 + offset as i64
        })
        .map(|bound| bound as i32)
        .collect())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Array, AsArray, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Int32Type};
    use datatypes::value::Value;

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
    fn test_breakpoints_sorts_and_dedups() {
        let function = IcebergHashBreakpointsFunction::default();
        assert_eq!(function.name(), "iceberg_hash_breakpoints");
        assert_eq!(
            function.return_type(&[DataType::Utf8]).unwrap(),
            DataType::Utf8
        );

        // Unsorted strings: 'iceberg', 'a', and 'iceberg' again (dedup).
        let bounds = invoke(
            &function,
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("iceberg".to_string()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("a".to_string()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("iceberg".to_string()))),
            ],
            1,
            DataType::Utf8,
        );
        let iceberg = IcebergHash::evaluate_value(&Value::String("iceberg".into()))
            .unwrap()
            .as_i64()
            .unwrap() as i32;
        let a = IcebergHash::evaluate_value(&Value::String("a".into()))
            .unwrap()
            .as_i64()
            .unwrap() as i32;
        let expected = match (iceberg, a) {
            (min, max) if min < max => format!("{min}, {max}"),
            (min, max) => format!("{max}, {min}"),
        };
        assert_eq!(into_scalar(bounds), ScalarValue::Utf8(Some(expected)));

        // Integers hash as longs and mix with the same rules.
        let bounds = invoke(
            &function,
            vec![
                ColumnarValue::Scalar(ScalarValue::Int64(Some(34))),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(-1))),
                ColumnarValue::Scalar(ScalarValue::UInt8(Some(0))),
            ],
            1,
            DataType::Utf8,
        );
        assert_eq!(
            into_scalar(bounds),
            ScalarValue::Utf8(Some(format!(
                "{}, {}, {}",
                IcebergHash::evaluate_value(&Value::Int32(-1))
                    .unwrap()
                    .as_i64()
                    .unwrap() as i32,
                IcebergHash::evaluate_value(&Value::UInt8(0))
                    .unwrap()
                    .as_i64()
                    .unwrap() as i32,
                IcebergHash::evaluate_value(&Value::Int64(34))
                    .unwrap()
                    .as_i64()
                    .unwrap() as i32,
            )))
        );
    }

    #[test]
    fn test_breakpoints_rejects_invalid_input() {
        let function = IcebergHashBreakpointsFunction::default();
        let call = |args: Vec<ColumnarValue>| {
            function.invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields: vec![],
                number_rows: 1,
                return_field: Arc::new(Field::new("result", DataType::Utf8, true)),
                config_options: Arc::new(Default::default()),
            })
        };

        // Empty list cannot form a breakpoint list.
        let err = call(vec![]).unwrap_err().to_string();
        assert!(err.contains("at least one value"), "{err}");

        // NULL can never be a breakpoint: it falls in the lowest bucket.
        let err = call(vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("a".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Null),
        ])
        .unwrap_err()
        .to_string();
        assert!(err.contains("cannot use NULL as a breakpoint"), "{err}");

        // A column cannot be hashed at planning time.
        let err = call(vec![ColumnarValue::Array(Arc::new(Int64Array::from(
            vec![34_i64],
        )))])
        .unwrap_err()
        .to_string();
        assert!(err.contains("constant values, not a column"), "{err}");

        // Unsupported value type names the shared hash error.
        let err = call(vec![ColumnarValue::Scalar(ScalarValue::Float64(Some(1.0)))])
            .unwrap_err()
            .to_string();
        assert!(err.contains("does not support type"), "{err}");
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

    #[test]
    fn test_ranges_sql_renders_rule_list() {
        let function = IcebergHashRangesSqlFunction::default();
        assert_eq!(function.name(), "iceberg_hash_ranges_sql");
        assert_eq!(
            function
                .return_type(&[DataType::Utf8, DataType::Int64])
                .unwrap(),
            DataType::Utf8
        );

        let render = |name: &str, buckets: i64| {
            let rendered = invoke(
                &function,
                vec![
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(name.to_string()))),
                    ColumnarValue::Scalar(ScalarValue::Int64(Some(buckets))),
                ],
                1,
                DataType::Utf8,
            );
            into_scalar(rendered)
        };

        let four_buckets = [
            "iceberg_hash(host) < -1073741824",
            "iceberg_hash(host) >= -1073741824 AND iceberg_hash(host) < 0",
            "iceberg_hash(host) >= 0 AND iceberg_hash(host) < 1073741824",
            "iceberg_hash(host) >= 1073741824",
        ]
        .join(",\n");
        assert_eq!(render("host", 4), ScalarValue::Utf8(Some(four_buckets)));
        assert_eq!(
            render("tenant", 2),
            ScalarValue::Utf8(Some(
                "iceberg_hash(tenant) < 0,\niceberg_hash(tenant) >= 0".to_string()
            ))
        );

        // A NULL name or bucket count propagates.
        let call = |args: Vec<ColumnarValue>| {
            let rendered = function
                .invoke_with_args(ScalarFunctionArgs {
                    args,
                    arg_fields: vec![],
                    number_rows: 1,
                    return_field: Arc::new(Field::new("result", DataType::Utf8, true)),
                    config_options: Arc::new(Default::default()),
                })
                .unwrap();
            into_scalar(rendered)
        };
        assert_eq!(
            call(vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(None)),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(4))),
            ]),
            ScalarValue::Utf8(None)
        );
        assert_eq!(
            call(vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("host".to_string()))),
                ColumnarValue::Scalar(ScalarValue::Int64(None)),
            ]),
            ScalarValue::Utf8(None)
        );
    }

    #[test]
    fn test_ranges_sql_rejects_invalid_input() {
        let function = IcebergHashRangesSqlFunction::default();
        let call = |args: Vec<ColumnarValue>| {
            function
                .invoke_with_args(ScalarFunctionArgs {
                    args,
                    arg_fields: vec![],
                    number_rows: 1,
                    return_field: Arc::new(Field::new("result", DataType::Utf8, true)),
                    config_options: Arc::new(Default::default()),
                })
                .unwrap_err()
                .to_string()
        };

        // Bucket count outside the accepted range names the shared limit.
        let err = call(vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("host".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
        ]);
        assert!(err.contains("bucket count in [2, 1024]"), "{err}");

        // Non-integer bucket count names the shared type error.
        let err = call(vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("host".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("4".to_string()))),
        ]);
        assert!(err.contains("integer bucket count"), "{err}");

        // Non-string column name.
        let err = call(vec![
            ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(4))),
        ]);
        assert!(err.contains("column name string"), "{err}");

        // Empty column name.
        let err = call(vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::new()))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(4))),
        ]);
        assert!(err.contains("non-empty column name"), "{err}");

        // Columns cannot be hashed at planning time.
        let err = call(vec![
            ColumnarValue::Array(Arc::new(Int64Array::from(vec![1_i64]))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(4))),
        ]);
        assert!(err.contains("constant arguments"), "{err}");

        // Wrong arity.
        let err = call(vec![ColumnarValue::Scalar(ScalarValue::Int64(Some(4)))]);
        assert!(err.contains("expects (column, buckets)"), "{err}");
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
        ctx.register_udf(create_udf(Arc::new(
            IcebergHashBreakpointsFunction::default(),
        )));
        ctx.register_udf(create_udf(
            Arc::new(IcebergHashRangesSqlFunction::default()),
        ));

        let batches = ctx
            .sql(
                "SELECT iceberg_hash('iceberg') AS h, iceberg_hash(34) AS i, \
                 iceberg_hash(NULL) AS n, iceberg_hash_bounds(4) AS b, \
                 iceberg_hash_bounds(CAST(4 AS SMALLINT)) AS sb, \
                 iceberg_hash_breakpoints('iceberg', 34) AS bp, \
                 iceberg_hash_ranges_sql('host', 2) AS rs",
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
        // Sorted hash values of 'iceberg' (1210000089) and 34 (2017239379).
        assert_eq!(
            batches[0].column(5).as_string::<i32>().value(0),
            "1210000089, 2017239379"
        );
        // Ready-to-paste rule list; a newline separates one rule per bucket.
        assert_eq!(
            batches[0].column(6).as_string::<i32>().value(0),
            "iceberg_hash(host) < 0,\niceberg_hash(host) >= 0"
        );
    }

    #[test]
    fn test_hash_functions_are_registered() {
        for name in [
            "iceberg_hash",
            "iceberg_hash_bounds",
            "iceberg_hash_breakpoints",
            "iceberg_hash_ranges_sql",
        ] {
            assert!(
                FUNCTION_REGISTRY.get_function(name).is_some(),
                "{name} is not registered"
            );
        }
    }
}
