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
use datafusion_functions::unicode::substr::get_true_start_end;
use datatypes::arrow::array::{BooleanArray, StringBuilder};
use datatypes::arrow::buffer::NullBuffer;
use datatypes::arrow::compute::{CastOptions, cast_with_options, nullif};
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
                    else {
                        return exec_err!("substring length is outside the signed 64-bit range");
                    };
                    Some(length)
                } else {
                    None
                };
                let value = value.as_utf8();
                let (start, end) = get_true_start_end(value, start, length, false)?;
                Ok(Value::from(&value[start..end]))
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
        if *self == Self::Substring && !all_scalar && rows > 0 {
            return invoke_substring(args);
        }
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

fn invoke_substring(mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    // NULL propagation precedes range checks, including UInt64 -> Int64 casts.
    let nulls = arrays.iter().fold(None, |nulls, array| {
        NullBuffer::union(nulls.as_ref(), array.logical_nulls().as_ref())
    });
    let null_mask = nulls.map(|nulls| BooleanArray::new(!nulls.inner(), None));
    let options = CastOptions {
        safe: false,
        ..Default::default()
    };
    args.args = arrays
        .into_iter()
        .enumerate()
        .map(|(index, array)| {
            let data_type = if index == 0 {
                DataType::Utf8
            } else {
                DataType::Int64
            };
            let array = if array.data_type() == &data_type {
                array
            } else {
                // Decode before narrowing: Arrow casts every dictionary value,
                // including entries unused by the selected rows.
                let array = if let DataType::Dictionary(_, value_type) = array.data_type() {
                    cast_with_options(array.as_ref(), value_type, &options)?
                } else {
                    array
                };
                let array = if let Some(mask) = &null_mask {
                    nullif(array.as_ref(), mask)?
                } else {
                    array
                };
                cast_with_options(array.as_ref(), &data_type, &options)?
            };
            Ok(ColumnarValue::Array(array))
        })
        .collect::<Result<Vec<_>>>()?;
    args.arg_fields = args
        .arg_fields
        .iter()
        .zip(&args.args)
        .map(|(field, value)| Arc::new(field.as_ref().clone().with_data_type(value.data_type())))
        .collect();
    datafusion_functions::unicode::substr().invoke_with_args(args)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn invoke(
        function: PartitionFunction,
        args: Vec<ColumnarValue>,
        number_rows: usize,
    ) -> Result<datatypes::arrow::array::ArrayRef> {
        use datatypes::arrow::datatypes::Field;
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
            })?
            .into_array(number_rows)
    }

    #[test]
    fn test_scalar_arguments_and_empty_batch() {
        use datatypes::arrow::array::StringArray;

        let actual = invoke(
            PartitionFunction::Hash,
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("a".into()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("bc".into()))),
            ],
            3,
        )
        .unwrap();
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
        )
        .unwrap();
        assert_eq!(actual.len(), 0);
    }

    #[test]
    fn test_substring_row_batch_semantics() {
        use datatypes::arrow::array::{ArrayRef, Int64Array, StringArray, UInt64Array};

        let text = ScalarValue::Utf8(Some("a中🙂z".into()));
        let mut cases = Vec::new();
        for integer in [
            ScalarValue::Int8(Some(2)),
            ScalarValue::Int16(Some(2)),
            ScalarValue::Int32(Some(2)),
            ScalarValue::Int64(Some(2)),
            ScalarValue::UInt8(Some(2)),
            ScalarValue::UInt16(Some(2)),
            ScalarValue::UInt32(Some(2)),
            ScalarValue::UInt64(Some(2)),
        ] {
            cases.push((vec![text.clone(), integer.clone(), integer], false));
        }
        for args in [
            vec![text.clone(), ScalarValue::Int64(Some(i64::MIN))],
            vec![
                text.clone(),
                ScalarValue::Int64(Some(1)),
                ScalarValue::Int64(Some(-1)),
            ],
            vec![text.clone(), ScalarValue::UInt64(Some(u64::MAX))],
            vec![
                text.clone(),
                ScalarValue::Int64(Some(1)),
                ScalarValue::UInt64(Some(u64::MAX)),
            ],
        ] {
            cases.push((args, true));
        }
        for args in [
            vec![ScalarValue::Utf8(None), ScalarValue::UInt64(Some(u64::MAX))],
            vec![
                text.clone(),
                ScalarValue::UInt64(Some(u64::MAX)),
                ScalarValue::Null,
            ],
            vec![
                text.clone(),
                ScalarValue::Null,
                ScalarValue::UInt64(Some(u64::MAX)),
            ],
            vec![ScalarValue::Utf8(None), ScalarValue::Int64(Some(i64::MIN))],
            vec![
                ScalarValue::Utf8(None),
                ScalarValue::Int64(Some(1)),
                ScalarValue::Int64(Some(-1)),
            ],
            vec![
                ScalarValue::Utf8View(Some("a中🙂z".into())),
                ScalarValue::Int64(Some(2)),
            ],
            vec![
                ScalarValue::LargeUtf8(Some("a中🙂z".into())),
                ScalarValue::Int64(Some(2)),
            ],
        ] {
            cases.push((args, false));
        }
        for (args, should_error) in cases {
            let values = args
                .iter()
                .cloned()
                .map(Value::try_from)
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap();
            let row = PartitionFunction::Substring.evaluate(&values);
            assert_eq!(row.is_err(), should_error, "{args:?}");
            for array_index in 0..args.len() {
                let batch_args = args
                    .iter()
                    .enumerate()
                    .map(|(index, value)| {
                        if index == array_index {
                            ColumnarValue::Array(value.to_array_of_size(2).unwrap())
                        } else {
                            ColumnarValue::Scalar(value.clone())
                        }
                    })
                    .collect();
                let batch = invoke(PartitionFunction::Substring, batch_args, 2);
                assert_eq!(batch.is_err(), should_error, "{args:?}");
                if let Ok(expected) = &row {
                    let batch = batch.unwrap();
                    for index in 0..2 {
                        let actual =
                            Value::try_from(ScalarValue::try_from_array(&batch, index).unwrap())
                                .unwrap();
                        assert_eq!(&actual, expected, "{args:?}");
                    }
                }
            }
        }
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![Some("a中🙂z"), None, Some("abc")])),
            Arc::new(UInt64Array::from(vec![2, u64::MAX, 1])),
            Arc::new(Int64Array::from(vec![2, -1, 0])),
        ];
        for offset in [0, 1] {
            let args = arrays
                .iter()
                .map(|array| ColumnarValue::Array(array.slice(offset, 3 - offset)))
                .collect();
            let actual = invoke(PartitionFunction::Substring, args, 3 - offset).unwrap();
            let expected = StringArray::from(vec![Some("中🙂"), None, Some("")]);
            assert_eq!(actual.as_ref(), &expected.slice(offset, 3 - offset));
        }
    }

    #[test]
    fn test_substring_dictionary_integer_bounds() {
        use datatypes::arrow::array::{DictionaryArray, StringArray, UInt32Array, UInt64Array};
        use datatypes::arrow::datatypes::UInt32Type;

        let dictionary = DictionaryArray::<UInt32Type>::try_new(
            UInt32Array::from(vec![0, 1]),
            Arc::new(UInt64Array::from(vec![1, u64::MAX])),
        )
        .unwrap();
        for argument_index in [1, 2] {
            for (offset, host, expected) in [
                (0, Some("abc"), Some("abc")),
                (1, None, None),
                (1, Some("abc"), None),
            ] {
                let mut args = vec![
                    ColumnarValue::Scalar(ScalarValue::Utf8(host.map(str::to_owned))),
                    ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
                ];
                if argument_index == 2 {
                    args.push(ColumnarValue::Scalar(ScalarValue::Int64(Some(1))));
                }
                args[argument_index] = ColumnarValue::Array(Arc::new(dictionary.slice(offset, 1)));
                let actual = invoke(PartitionFunction::Substring, args, 1);
                if offset == 1 && host.is_some() {
                    assert!(actual.is_err());
                } else {
                    let expected = if argument_index == 2 {
                        expected.map(|_| "a")
                    } else {
                        expected
                    };
                    assert_eq!(actual.unwrap().as_ref(), &StringArray::from(vec![expected]));
                }
            }
        }
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
