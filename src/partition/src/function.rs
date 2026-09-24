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

use std::sync::{Arc, LazyLock};

use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datatypes::arrow::array::StringBuilder;
use datatypes::arrow::datatypes::DataType;
use datatypes::data_type::DataType as _;
use datatypes::value::Value;
use serde::{Deserialize, Serialize};

/// Functions whose evaluation is part of the persisted partition routing contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum PartitionFunction {
    Substring,
    Hash,
}

impl PartitionFunction {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Substring => "substring",
            Self::Hash => "hash",
        }
    }

    /// Checks types without implicit casts, which could change persisted routing.
    pub fn validate(self, types: &[DataType]) -> Result<()> {
        let mut unpacked = types.iter().map(|t| match t {
            DataType::Dictionary(_, value) => value.as_ref(),
            _ => t,
        });
        let string = |t: &DataType| {
            matches!(
                t,
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null
            )
        };
        let valid = match self {
            Self::Hash => !types.is_empty() && unpacked.all(|t| string(t) || t.is_integer()),
            Self::Substring => {
                (2..=3).contains(&types.len())
                    && unpacked.next().is_some_and(string)
                    && unpacked.all(|t| t.is_integer() || *t == DataType::Null)
            }
        };
        if !valid {
            return exec_err!(
                "Invalid argument types for partition function {}: {types:?}",
                self.name()
            );
        }
        Ok(())
    }

    /// Evaluates both row routing and the batch UDF with identical semantics.
    pub(crate) fn evaluate(self, args: &[Value]) -> Result<Value> {
        self.validate(
            &args
                .iter()
                .map(|v| v.data_type().as_arrow_type())
                .collect::<Vec<_>>(),
        )?;
        self.evaluate_validated(args)
    }

    // Types are checked once per batch; value-dependent errors must still be
    // checked for every evaluated row, including scalar arguments.
    fn evaluate_validated(self, args: &[Value]) -> Result<Value> {
        if args.iter().any(|v| matches!(v, Value::Null)) {
            return Ok(Value::Null);
        }
        match self {
            Self::Substring => {
                let Value::String(value) = &args[0] else {
                    unreachable!()
                };
                let Some(start) = args[1]
                    .as_i64()
                    .or_else(|| args[1].as_u64().and_then(|n| i64::try_from(n).ok()))
                else {
                    return exec_err!("substring start is outside the signed 64-bit range");
                };
                let length = if let Some(length) = args.get(2) {
                    let Some(length) = length
                        .as_i64()
                        .or_else(|| length.as_u64().and_then(|n| i64::try_from(n).ok()))
                        .filter(|v| *v >= 0)
                    else {
                        return exec_err!(
                            "substring length must be a non-negative signed 64-bit integer"
                        );
                    };
                    Some(length)
                } else {
                    None
                };
                // SQL positions count Unicode characters, including positions before 1.
                let skip = (i128::from(start) - 1).max(0);
                let take =
                    length.map(|length| (i128::from(start) - 1 + i128::from(length) - skip).max(0));
                let chars = value
                    .as_utf8()
                    .chars()
                    .skip(usize::try_from(skip).unwrap_or(usize::MAX));
                let result: String = chars
                    .take(
                        take.and_then(|n| usize::try_from(n).ok())
                            .unwrap_or(usize::MAX),
                    )
                    .collect();
                Ok(Value::String(result.into()))
            }
            Self::Hash => {
                // Concatenate arguments using this persisted routing encoding:
                // 0x01 | u64 big-endian byte length | UTF-8 bytes
                // 0x02 | i64 big-endian two's complement
                // 0x03 | u64 big-endian
                // Integer widths normalize within each signedness category.
                // These tags, encodings and MD5 must remain stable across versions.
                let mut context = md5::Context::new();
                for arg in args {
                    let (tag, bytes): (u8, [u8; 8]) = match arg {
                        Value::String(value) => {
                            let bytes = value.as_utf8().as_bytes();
                            context.consume([0x01]);
                            context.consume((bytes.len() as u64).to_be_bytes());
                            context.consume(bytes);
                            continue;
                        }
                        Value::Int8(value) => (0x02, i64::from(*value).to_be_bytes()),
                        Value::Int16(value) => (0x02, i64::from(*value).to_be_bytes()),
                        Value::Int32(value) => (0x02, i64::from(*value).to_be_bytes()),
                        Value::Int64(value) => (0x02, value.to_be_bytes()),
                        Value::UInt8(value) => (0x03, u64::from(*value).to_be_bytes()),
                        Value::UInt16(value) => (0x03, u64::from(*value).to_be_bytes()),
                        Value::UInt32(value) => (0x03, u64::from(*value).to_be_bytes()),
                        Value::UInt64(value) => (0x03, value.to_be_bytes()),
                        _ => unreachable!(),
                    };
                    context.consume([tag]);
                    context.consume(bytes);
                }
                Ok(Value::String(format!("{:x}", context.finalize()).into()))
            }
        }
    }
}

impl ScalarUDFImpl for PartitionFunction {
    fn name(&self) -> &str {
        (*self).name()
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: LazyLock<Signature> =
            LazyLock::new(|| Signature::variadic_any(Volatility::Immutable));
        &SIGNATURE
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        self.validate(types)?;
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.validate(
            &args
                .args
                .iter()
                .map(ColumnarValue::data_type)
                .collect::<Vec<_>>(),
        )?;
        let all_scalar = args
            .args
            .iter()
            .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
        let rows = if all_scalar && args.number_rows > 0 {
            1
        } else {
            args.number_rows
        };
        let mut values = Vec::with_capacity(args.args.len());
        let mut results = StringBuilder::with_capacity(rows, 0);
        for row in 0..rows {
            values.clear();
            for arg in &args.args {
                let scalar = match arg {
                    ColumnarValue::Scalar(value) => value.clone(),
                    ColumnarValue::Array(array) => ScalarValue::try_from_array(array, row)?,
                };
                values.push(
                    Value::try_from(scalar)
                        .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?,
                );
            }
            let value = self.evaluate_validated(&values)?;
            if all_scalar {
                return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(match value {
                    Value::String(value) => Some(value.as_utf8().to_owned()),
                    Value::Null => None,
                    _ => unreachable!(),
                })));
            }
            match value {
                Value::String(value) => results.append_value(value.as_utf8()),
                Value::Null => results.append_null(),
                _ => unreachable!(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(results.finish())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_and_scalar_results() {
        use datatypes::arrow::array::{ArrayRef, StringArray};
        use datatypes::arrow::datatypes::Field;

        let invoke = |function: PartitionFunction, args: Vec<ColumnarValue>, number_rows| {
            let arg_fields = args
                .iter()
                .map(|arg| Arc::new(Field::new("arg", arg.data_type(), true)))
                .collect();
            function
                .invoke_with_args(ScalarFunctionArgs {
                    args,
                    arg_fields,
                    number_rows,
                    return_field: Arc::new(Field::new("result", DataType::Utf8, true)),
                    config_options: Arc::new(Default::default()),
                })
                .unwrap()
                .into_array(number_rows)
                .unwrap()
        };
        let values = Arc::new(StringArray::from(vec![Some("abc"), None, Some("中🙂")])) as ArrayRef;
        let actual = invoke(
            PartitionFunction::Substring,
            vec![
                ColumnarValue::Array(values),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
            ],
            3,
        );
        assert_eq!(
            actual.as_any().downcast_ref::<StringArray>().unwrap(),
            &StringArray::from(vec![Some("b"), None, Some("🙂")])
        );
        let actual = invoke(
            PartitionFunction::Hash,
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("a".into()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("bc".into()))),
            ],
            3,
        );
        assert_eq!(
            actual.as_any().downcast_ref::<StringArray>().unwrap(),
            &StringArray::from(vec!["8f867eea8fef54c5b939e98da8815f16"; 3])
        );
        let actual = invoke(
            PartitionFunction::Substring,
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("abc".into()))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(-1))),
            ],
            0,
        );
        assert_eq!(actual.len(), 0);
    }

    #[test]
    fn test_substring_unicode_and_bounds() {
        for (start, length, expected) in [
            (1, Some(2), "a中"),
            (2, Some(2), "中🙂"),
            (2, None, "中🙂z"),
            (0, Some(2), "a"),
            (-2, Some(4), "a"),
            (-2, None, "a中🙂z"),
            (1, Some(0), ""),
            (99, None, ""),
            (i64::MIN, Some(i64::MAX), ""),
            (i64::MAX, Some(i64::MAX), ""),
        ] {
            let mut args = vec![Value::from("a中🙂z"), Value::Int64(start)];
            if let Some(length) = length {
                args.push(Value::Int64(length));
            }
            assert_eq!(
                PartitionFunction::Substring.evaluate(&args).unwrap(),
                Value::from(expected)
            );
        }
        assert!(
            PartitionFunction::Substring
                .evaluate(&[Value::from("abc"), Value::Int64(1), Value::Int64(-1)])
                .is_err()
        );
        assert!(
            PartitionFunction::Substring
                .evaluate(&[Value::from("abc"), Value::UInt64(u64::MAX)])
                .is_err()
        );
        assert_eq!(
            PartitionFunction::Substring
                .evaluate(&[Value::Null, Value::Int64(1)])
                .unwrap(),
            Value::Null
        );
    }

    #[test]
    fn test_hash_integer_encoding() {
        for (values, expected) in [
            (
                vec![
                    Value::Int8(42),
                    Value::Int16(42),
                    Value::Int32(42),
                    Value::Int64(42),
                ],
                "1b445199c0b8e60824f46c7d86e180ac",
            ),
            (
                vec![
                    Value::UInt8(42),
                    Value::UInt16(42),
                    Value::UInt32(42),
                    Value::UInt64(42),
                ],
                "0a4a10125f3a80f43632ed08f971e0c1",
            ),
            (
                vec![
                    Value::Int8(-1),
                    Value::Int16(-1),
                    Value::Int32(-1),
                    Value::Int64(-1),
                ],
                "a47c749bf46e53ff2f7c5ce57a2d5df3",
            ),
            (
                vec![Value::Int64(i64::MIN)],
                "3c1056e64c7f1b90f975993d9233065e",
            ),
            (
                vec![Value::Int64(i64::MAX)],
                "47e2d97c5bcda90e8c26ff875d8b9c8d",
            ),
            (
                vec![Value::UInt64(u64::MAX)],
                "bdc3b69973c1a29c29e05e79e380949b",
            ),
        ] {
            for value in values {
                assert_eq!(
                    PartitionFunction::Hash.evaluate(&[value]).unwrap(),
                    Value::from(expected)
                );
            }
        }
        assert_eq!(
            PartitionFunction::Hash
                .evaluate(&[Value::Int64(42), Value::from("host"), Value::UInt64(42)])
                .unwrap(),
            Value::from("e7ea8e6691cb06826c104e062afb327d")
        );
        assert_eq!(
            PartitionFunction::Hash
                .evaluate(&[Value::Int64(42), Value::Null])
                .unwrap(),
            Value::Null
        );
        for data_type in [
            DataType::Boolean,
            DataType::Float32,
            DataType::Float64,
            DataType::Decimal128(20, 4),
            DataType::Date32,
            DataType::Timestamp(datatypes::arrow::datatypes::TimeUnit::Millisecond, None),
        ] {
            assert!(
                PartitionFunction::Hash
                    .validate(&[data_type, DataType::Null])
                    .is_err()
            );
        }
    }

    #[test]
    fn test_hash_routing_contract() {
        assert_eq!(
            PartitionFunction::Hash
                .evaluate(&[Value::from("")])
                .unwrap(),
            Value::from("53e0f9e230926c8fa946e76e40b78c84")
        );
        assert_eq!(
            PartitionFunction::Hash
                .evaluate(&[Value::from("abc")])
                .unwrap(),
            Value::from("771ab001e65f604d17f50242b0eee2aa")
        );
        assert_eq!(
            PartitionFunction::Hash
                .evaluate(&[Value::from("a"), Value::from("bc")])
                .unwrap(),
            Value::from("8f867eea8fef54c5b939e98da8815f16")
        );
        assert_eq!(
            PartitionFunction::Hash
                .evaluate(&[Value::from("ab"), Value::from("c")])
                .unwrap(),
            Value::from("674a210d202926e0fa536e96e3303d9d")
        );
        assert_eq!(
            PartitionFunction::Hash
                .evaluate(&[Value::from("中"), Value::from("🙂")])
                .unwrap(),
            Value::from("e492b54b0d91d20fc5b82858ec8ee754")
        );
        assert_eq!(
            PartitionFunction::Hash
                .evaluate(&[Value::from("a"), Value::Null])
                .unwrap(),
            Value::Null
        );
        assert!(PartitionFunction::Hash.evaluate(&[]).is_err());
        assert!(
            PartitionFunction::Hash
                .evaluate(&[Value::from(1.0_f64)])
                .is_err()
        );
    }
}
