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

//! `iceberg_hash`, the hash allowed on the column side of a partition rule.
//!
//! The hash decides which region a row is written to and is persisted in
//! partition metadata, so its result for a given input must never change across
//! versions. A different algorithm needs a new type.
//!
//! Rule simplification and coverage checks treat the hash as an independent
//! dimension, which holds because it is defined for every value of the accepted
//! column types.

use std::fmt::{Display, Formatter};
use std::sync::Arc;

use datafusion_common::arrow::array::{Array, ArrayRef, AsArray, Int32Array};
use datafusion_common::arrow::compute::take;
use datafusion_common::arrow::datatypes::{
    DataType, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type,
    UInt64Type,
};
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datatypes::prelude::ConcreteDataType;
use datatypes::value::Value;
use serde::{Deserialize, Serialize};

use crate::error::{self, Result};

/// `iceberg_hash(column)`: the 32-bit Murmur3 hash defined by the Iceberg table
/// spec (appendix B) over a string or integer column, as an `INT`.
///
/// Integers of every width hash as Iceberg longs. `UINT64` values hash their bits
/// as a long, so values up to `i64::MAX` match Iceberg. `NULL` hashes to `NULL`.
/// The field is the hashed column.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct IcebergHash(String);

impl IcebergHash {
    pub const NAME: &str = "iceberg_hash";

    pub fn new(column: impl Into<String>) -> Self {
        Self(column.into())
    }

    pub fn column(&self) -> &str {
        &self.0
    }

    pub fn accepts(data_type: &ConcreteDataType) -> bool {
        data_type.is_string() || data_type.is_unsigned() || is_signed_integer(data_type)
    }

    /// The type of the result, which is also the type of the literals it is compared with.
    pub fn output_type() -> ConcreteDataType {
        ConcreteDataType::int32_datatype()
    }

    pub fn evaluate_value(value: &Value) -> Result<Value> {
        let hash = match value {
            Value::Null => return Ok(Value::Null),
            Value::String(v) => murmur3_32(v.as_utf8().as_bytes()),
            Value::Int8(v) => hash_long(*v as i64),
            Value::Int16(v) => hash_long(*v as i64),
            Value::Int32(v) => hash_long(*v as i64),
            Value::Int64(v) => hash_long(*v),
            Value::UInt8(v) => hash_long(*v as i64),
            Value::UInt16(v) => hash_long(*v as i64),
            Value::UInt32(v) => hash_long(*v as i64),
            Value::UInt64(v) => hash_long(*v as i64),
            other => {
                return error::UnexpectedSnafu {
                    err_msg: format!("Unsupported value for {}: {other:?}", Self::NAME),
                }
                .fail();
            }
        };
        Ok(Value::Int32(hash))
    }

    pub fn evaluate_array(array: &dyn Array) -> datafusion_common::Result<ArrayRef> {
        iceberg_hash_array(array)
    }

    pub fn to_logical_expr(&self) -> Expr {
        let column = Expr::Column(datafusion_common::Column::from_name(self.column()));
        ScalarUDF::new_from_impl(IcebergHashUdf::default()).call(vec![column])
    }
}

impl Display for IcebergHash {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}({})", Self::NAME, self.column())
    }
}

fn is_signed_integer(data_type: &ConcreteDataType) -> bool {
    matches!(
        data_type,
        ConcreteDataType::Int8(_)
            | ConcreteDataType::Int16(_)
            | ConcreteDataType::Int32(_)
            | ConcreteDataType::Int64(_)
    )
}

fn iceberg_hash_array(array: &dyn Array) -> datafusion_common::Result<ArrayRef> {
    let hashes: Int32Array = match array.data_type() {
        DataType::Utf8 => array
            .as_string::<i32>()
            .iter()
            .map(|v| v.map(|v| murmur3_32(v.as_bytes())))
            .collect(),
        DataType::LargeUtf8 => array
            .as_string::<i64>()
            .iter()
            .map(|v| v.map(|v| murmur3_32(v.as_bytes())))
            .collect(),
        DataType::Utf8View => array
            .as_string_view()
            .iter()
            .map(|v| v.map(|v| murmur3_32(v.as_bytes())))
            .collect(),
        DataType::Int8 => array
            .as_primitive::<Int8Type>()
            .unary(|v| hash_long(v as i64)),
        DataType::Int16 => array
            .as_primitive::<Int16Type>()
            .unary(|v| hash_long(v as i64)),
        DataType::Int32 => array
            .as_primitive::<Int32Type>()
            .unary(|v| hash_long(v as i64)),
        DataType::Int64 => array.as_primitive::<Int64Type>().unary(hash_long),
        DataType::UInt8 => array
            .as_primitive::<UInt8Type>()
            .unary(|v| hash_long(v as i64)),
        DataType::UInt16 => array
            .as_primitive::<UInt16Type>()
            .unary(|v| hash_long(v as i64)),
        DataType::UInt32 => array
            .as_primitive::<UInt32Type>()
            .unary(|v| hash_long(v as i64)),
        DataType::UInt64 => array
            .as_primitive::<UInt64Type>()
            .unary(|v| hash_long(v as i64)),
        DataType::Dictionary(_, _) => {
            let dictionary = array.as_any_dictionary();
            let values = iceberg_hash_array(dictionary.values().as_ref())?;
            return Ok(take(values.as_ref(), dictionary.keys(), None)?);
        }
        other => {
            return Err(DataFusionError::Execution(format!(
                "{} does not support type {other}",
                IcebergHash::NAME
            )));
        }
    };
    Ok(Arc::new(hashes))
}

fn hash_long(value: i64) -> i32 {
    murmur3_32(&value.to_le_bytes())
}

/// MurmurHash3 x86 32-bit with seed 0, returned as a signed integer like Iceberg.
fn murmur3_32(data: &[u8]) -> i32 {
    const C1: u32 = 0xcc9e_2d51;
    const C2: u32 = 0x1b87_3593;

    fn mix_k(k: u32) -> u32 {
        k.wrapping_mul(C1).rotate_left(15).wrapping_mul(C2)
    }

    let mut h = 0u32;
    let (blocks, tail) = data.as_chunks::<4>();
    for block in blocks {
        h = (h ^ mix_k(u32::from_le_bytes(*block)))
            .rotate_left(13)
            .wrapping_mul(5)
            .wrapping_add(0xe654_6b64);
    }
    if !tail.is_empty() {
        let k = tail
            .iter()
            .rev()
            .fold(0u32, |k, byte| (k << 8) | *byte as u32);
        h ^= mix_k(k);
    }

    // Murmur3 mixes in the length modulo 2^32.
    h ^= data.len() as u32;
    h ^= h >> 16;
    h = h.wrapping_mul(0x85eb_ca6b);
    h ^= h >> 13;
    h = h.wrapping_mul(0xc2b2_ae35);
    h ^= h >> 16;
    h as i32
}

/// DataFusion form of [`IcebergHash`], so storage engines
/// can evaluate a region's partition expression as a filter.
#[derive(Debug, PartialEq, Eq, Hash)]
struct IcebergHashUdf {
    signature: Signature,
}

impl Default for IcebergHashUdf {
    fn default() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for IcebergHashUdf {
    fn name(&self) -> &str {
        IcebergHash::NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Int32)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [arg] = args.args.as_slice() else {
            return Err(DataFusionError::Internal(format!(
                "{} expects one argument, got {}",
                IcebergHash::NAME,
                args.args.len()
            )));
        };
        match arg {
            ColumnarValue::Array(array) => {
                Ok(ColumnarValue::Array(iceberg_hash_array(array.as_ref())?))
            }
            ColumnarValue::Scalar(scalar) => {
                let hashes = iceberg_hash_array(scalar.to_array()?.as_ref())?;
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &hashes, 0,
                )?))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion_common::arrow::array::{
        Int64Array, StringArray, StringDictionaryBuilder, StringViewArray, UInt64Array,
    };
    use datafusion_common::arrow::datatypes::UInt16Type;

    use super::*;

    #[test]
    fn test_iceberg_spec_vectors() {
        // Iceberg table spec, appendix B.
        assert_eq!(hash_long(34), 2017239379);
        // date 2017-11-16 hashes as int 17486.
        assert_eq!(hash_long(17486), -653330422);
        // time 22:31:08 hashes as long 81068000000 micros.
        assert_eq!(hash_long(81068000000), -662762989);
        assert_eq!(murmur3_32("iceberg".as_bytes()), 1210000089);
        assert_eq!(murmur3_32(&[0, 1, 2, 3]), -188683207);
    }

    #[test]
    fn test_unsigned_policy() {
        assert_eq!(
            IcebergHash::evaluate_value(&Value::UInt64(34)).unwrap(),
            Value::Int32(2017239379)
        );
        assert_eq!(
            IcebergHash::evaluate_value(&Value::UInt64(u64::MAX)).unwrap(),
            Value::Int32(hash_long(-1))
        );
    }

    #[test]
    fn test_array_matches_value() {
        let strings = [Some("iceberg"), None, Some(""), Some("中🙂")];
        let mut dictionary = StringDictionaryBuilder::<UInt16Type>::new();
        for v in strings {
            dictionary.append_option(v);
        }
        let string_arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(strings.to_vec())),
            Arc::new(StringViewArray::from(strings.to_vec())),
            Arc::new(dictionary.finish()),
        ];
        let string_values = strings.map(|v| v.map_or(Value::Null, Value::from));

        let ints = [Some(34), None, Some(i64::MIN), Some(-1)];
        let unsigned = [Some(34), None, Some(u64::MAX), Some(0)];
        let cases = string_arrays
            .into_iter()
            .map(|array| (array, string_values.to_vec()))
            .chain([
                (
                    Arc::new(Int64Array::from(ints.to_vec())) as ArrayRef,
                    ints.map(|v| v.map_or(Value::Null, Value::Int64)).to_vec(),
                ),
                (
                    Arc::new(UInt64Array::from(unsigned.to_vec())) as ArrayRef,
                    unsigned
                        .map(|v| v.map_or(Value::Null, Value::UInt64))
                        .to_vec(),
                ),
            ]);
        for (array, values) in cases {
            let hashes = IcebergHash::evaluate_array(array.as_ref()).unwrap();
            let hashes = hashes.as_primitive::<Int32Type>();
            for (row, value) in values.iter().enumerate() {
                let expected = IcebergHash::evaluate_value(value).unwrap();
                let actual = if hashes.is_null(row) {
                    Value::Null
                } else {
                    Value::Int32(hashes.value(row))
                };
                assert_eq!(actual, expected, "{:?} row {row}", array.data_type());
            }
        }
    }
}
