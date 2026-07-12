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

use std::sync::Arc;

use arrow::datatypes::DataType as ArrowDataType;
use num_traits::Num;
use serde::{Deserialize, Serialize};

use crate::data_type::{DataType, DataTypeRef};
use crate::scalars::ScalarVectorBuilder;
use crate::type_id::LogicalTypeId;
use crate::value::Value;
use crate::vectors::{BooleanVectorBuilder, MutableVector};

#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BooleanType;

impl BooleanType {
    pub fn arc() -> DataTypeRef {
        Arc::new(Self)
    }
}

impl DataType for BooleanType {
    fn name(&self) -> String {
        "Boolean".to_string()
    }

    fn logical_type_id(&self) -> LogicalTypeId {
        LogicalTypeId::Boolean
    }

    fn default_value(&self) -> Value {
        bool::default().into()
    }

    fn as_arrow_type(&self) -> ArrowDataType {
        ArrowDataType::Boolean
    }

    fn create_mutable_vector(&self, capacity: usize) -> Box<dyn MutableVector> {
        Box::new(BooleanVectorBuilder::with_capacity(capacity))
    }

    fn try_cast(&self, from: Value) -> Option<Value> {
        match from {
            Value::Boolean(v) => Some(Value::Boolean(v)),
            Value::UInt8(v) => numeric_to_bool(v),
            Value::UInt16(v) => numeric_to_bool(v),
            Value::UInt32(v) => numeric_to_bool(v),
            Value::UInt64(v) => numeric_to_bool(v),
            Value::Int8(v) => numeric_to_bool(v),
            Value::Int16(v) => numeric_to_bool(v),
            Value::Int32(v) => numeric_to_bool(v),
            Value::Int64(v) => numeric_to_bool(v),
            Value::Float32(v) => numeric_to_bool(v),
            Value::Float64(v) => numeric_to_bool(v),
            Value::String(v) => v.as_utf8().parse::<bool>().ok().map(Value::Boolean),
            _ => None,
        }
    }
}

pub fn numeric_to_bool<T>(num: T) -> Option<Value>
where
    T: Num + Default,
{
    if num != T::default() {
        Some(Value::Boolean(true))
    } else {
        Some(Value::Boolean(false))
    }
}

pub fn bool_to_numeric<T>(value: bool) -> Option<T>
where
    T: Num,
{
    if value {
        Some(T::one())
    } else {
        Some(T::zero())
    }
}

#[cfg(test)]
mod tests {

    use ordered_float::OrderedFloat;

    use super::*;
    use crate::data_type::ConcreteDataType;

    macro_rules! test_cast_to_bool {
        ($value: expr, $expected: expr) => {
            let val = $value;
            let b = ConcreteDataType::boolean_datatype().try_cast(val).unwrap();
            assert_eq!(b, Value::Boolean($expected));
        };
    }

    macro_rules! test_cast_from_bool {
        ($value: expr, $datatype: expr, $expected: expr) => {
            let val = Value::Boolean($value);
            let b = $datatype.try_cast(val).unwrap();
            assert_eq!(b, $expected);
        };
    }

    #[test]
    fn test_other_type_cast_to_bool() {
        // false cases
        test_cast_to_bool!(Value::UInt8(0), false);
        test_cast_to_bool!(Value::UInt16(0), false);
        test_cast_to_bool!(Value::UInt32(0), false);
        test_cast_to_bool!(Value::UInt64(0), false);
        test_cast_to_bool!(Value::Int8(0), false);
        test_cast_to_bool!(Value::Int16(0), false);
        test_cast_to_bool!(Value::Int32(0), false);
        test_cast_to_bool!(Value::Int64(0), false);
        test_cast_to_bool!(Value::Float32(OrderedFloat(0.0)), false);
        test_cast_to_bool!(Value::Float64(OrderedFloat(0.0)), false);
        // true cases
        test_cast_to_bool!(Value::UInt8(1), true);
        test_cast_to_bool!(Value::UInt16(2), true);
        test_cast_to_bool!(Value::UInt32(3), true);
        test_cast_to_bool!(Value::UInt64(4), true);
        test_cast_to_bool!(Value::Int8(5), true);
        test_cast_to_bool!(Value::Int16(6), true);
        test_cast_to_bool!(Value::Int32(7), true);
        test_cast_to_bool!(Value::Int64(8), true);
        test_cast_to_bool!(Value::Float32(OrderedFloat(1.0)), true);
        test_cast_to_bool!(Value::Float64(OrderedFloat(2.0)), true);
    }

    #[test]
    fn test_bool_cast_to_other_type() {
        // false cases
        test_cast_from_bool!(false, ConcreteDataType::uint8_datatype(), Value::UInt8(0));
        test_cast_from_bool!(false, ConcreteDataType::uint16_datatype(), Value::UInt16(0));
        test_cast_from_bool!(false, ConcreteDataType::uint32_datatype(), Value::UInt32(0));
        test_cast_from_bool!(false, ConcreteDataType::uint64_datatype(), Value::UInt64(0));
        test_cast_from_bool!(false, ConcreteDataType::int8_datatype(), Value::Int8(0));
        test_cast_from_bool!(false, ConcreteDataType::int16_datatype(), Value::Int16(0));
        test_cast_from_bool!(false, ConcreteDataType::int32_datatype(), Value::Int32(0));
        test_cast_from_bool!(false, ConcreteDataType::int64_datatype(), Value::Int64(0));
        test_cast_from_bool!(
            false,
            ConcreteDataType::float32_datatype(),
            Value::Float32(OrderedFloat(0.0))
        );
        test_cast_from_bool!(
            false,
            ConcreteDataType::float64_datatype(),
            Value::Float64(OrderedFloat(0.0))
        );
        // true cases
        test_cast_from_bool!(true, ConcreteDataType::uint8_datatype(), Value::UInt8(1));
        test_cast_from_bool!(true, ConcreteDataType::uint16_datatype(), Value::UInt16(1));
        test_cast_from_bool!(true, ConcreteDataType::uint32_datatype(), Value::UInt32(1));
        test_cast_from_bool!(true, ConcreteDataType::uint64_datatype(), Value::UInt64(1));
        test_cast_from_bool!(true, ConcreteDataType::int8_datatype(), Value::Int8(1));
        test_cast_from_bool!(true, ConcreteDataType::int16_datatype(), Value::Int16(1));
        test_cast_from_bool!(true, ConcreteDataType::int32_datatype(), Value::Int32(1));
        test_cast_from_bool!(true, ConcreteDataType::int64_datatype(), Value::Int64(1));
        test_cast_from_bool!(
            true,
            ConcreteDataType::float32_datatype(),
            Value::Float32(OrderedFloat(1.0))
        );
        test_cast_from_bool!(
            true,
            ConcreteDataType::float64_datatype(),
            Value::Float64(OrderedFloat(1.0))
        );
    }
}
