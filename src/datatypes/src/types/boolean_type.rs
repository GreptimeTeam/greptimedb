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
    fn name(&self) -> &str {
        "Boolean"
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

    fn is_timestamp_compatible(&self) -> bool {
        false
    }

    fn cast(&self, from: Value) -> Option<Value> {
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
            _ => None,
        }
    }
}

fn numeric_to_bool<T>(num: T) -> Option<Value>
where
    T: Num + Default,
{
    if num != T::default() {
        Some(Value::Boolean(true))
    } else {
        Some(Value::Boolean(false))
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::data_type::ConcreteDataType;

    macro_rules! test_bool_conversion {
        ($value: expr, $expected: expr) => {
            let val = $value;
            let b = ConcreteDataType::boolean_datatype().cast(val).unwrap();
            assert_eq!(b, Value::Boolean($expected));
        };
    }

    #[test]
    fn test_bool_cast() {
        // false cases
        test_bool_conversion!(Value::UInt8(0), false);
        test_bool_conversion!(Value::UInt16(0), false);
        test_bool_conversion!(Value::UInt32(0), false);
        test_bool_conversion!(Value::UInt64(0), false);
        test_bool_conversion!(Value::Int8(0), false);
        test_bool_conversion!(Value::Int16(0), false);
        test_bool_conversion!(Value::Int32(0), false);
        test_bool_conversion!(Value::Int64(0), false);

        // true cases
        test_bool_conversion!(Value::UInt8(1), true);
        test_bool_conversion!(Value::UInt16(1), true);
        test_bool_conversion!(Value::UInt32(1), true);
        test_bool_conversion!(Value::UInt64(1), true);
        test_bool_conversion!(Value::Int8(1), true);
        test_bool_conversion!(Value::Int16(1), true);
        test_bool_conversion!(Value::Int32(1), true);
        test_bool_conversion!(Value::Int64(1), true);
    }
}
