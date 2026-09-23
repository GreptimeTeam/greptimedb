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
use datatypes::arrow::array::StringArray;
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
        let types = types
            .iter()
            .map(|t| match t {
                DataType::Dictionary(_, value) => value.as_ref(),
                _ => t,
            })
            .collect::<Vec<_>>();
        let string = |t: &&DataType| {
            matches!(
                t,
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null
            )
        };
        let valid = match self {
            Self::Hash => !types.is_empty() && types.iter().all(string),
            Self::Substring => {
                (2..=3).contains(&types.len())
                    && string(&types[0])
                    && types[1..]
                        .iter()
                        .all(|t| t.is_integer() || **t == DataType::Null)
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
                // Length-prefixed UTF-8 keeps argument boundaries unambiguous. The
                // encoding, byte order and MD5 algorithm must remain stable across versions.
                let mut context = md5::Context::new();
                for arg in args {
                    let Value::String(value) = arg else {
                        unreachable!()
                    };
                    let bytes = value.as_utf8().as_bytes();
                    context.consume((bytes.len() as u64).to_be_bytes());
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
        let all_scalar = args
            .args
            .iter()
            .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
        let rows = if all_scalar { 1 } else { args.number_rows };
        let mut values = Vec::with_capacity(args.args.len());
        let mut results = Vec::with_capacity(rows);
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
            let value = self.evaluate(&values)?;
            results.push(match value {
                Value::String(value) => Some(value.as_utf8().to_owned()),
                Value::Null => None,
                _ => unreachable!(),
            });
        }
        if all_scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                results.pop().flatten(),
            )))
        } else {
            Ok(ColumnarValue::Array(Arc::new(StringArray::from(results))))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_hash_routing_contract() {
        assert_eq!(
            PartitionFunction::Hash
                .evaluate(&[Value::from("")])
                .unwrap(),
            Value::from("7dea362b3fac8e00956a4952a3d4f474")
        );
        assert_eq!(
            PartitionFunction::Hash
                .evaluate(&[Value::from("abc")])
                .unwrap(),
            Value::from("28f2a06351900a402769c453a5b8b051")
        );
        assert_eq!(
            PartitionFunction::Hash
                .evaluate(&[Value::from("a"), Value::from("bc")])
                .unwrap(),
            Value::from("b6d6f72a44aa4f71d6e041d1c9750933")
        );
        assert_eq!(
            PartitionFunction::Hash
                .evaluate(&[Value::from("ab"), Value::from("c")])
                .unwrap(),
            Value::from("fa4f70db448765d5fa0c807a97ae7749")
        );
        assert_eq!(
            PartitionFunction::Hash
                .evaluate(&[Value::from("中"), Value::from("🙂")])
                .unwrap(),
            Value::from("5f5571be5e0507619c0b3cdea3e9fc58")
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
                .evaluate(&[Value::Int64(1)])
                .is_err()
        );
    }
}
