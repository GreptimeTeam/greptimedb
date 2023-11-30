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

use arrow_schema::DataType as ArrowDataType;
use common_decimal::decimal128::DECIMAL128_MAX_PRECISION;
use common_decimal::Decimal128;
use serde::{Deserialize, Serialize};

use crate::prelude::{DataType, ScalarVectorBuilder};
use crate::type_id::LogicalTypeId;
use crate::value::Value;
use crate::vectors::{Decimal128VectorBuilder, MutableVector};

/// Decimal type with precision and scale information.
#[derive(
    Debug, Default, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct Decimal128Type {
    precision: u8,
    scale: i8,
}

impl Decimal128Type {
    pub fn new(precision: u8, scale: i8) -> Self {
        // assert precision and scale is valid
        assert!(
            precision > 0 && precision <= DECIMAL128_MAX_PRECISION,
            "precision should be in [1, {}]",
            DECIMAL128_MAX_PRECISION
        );
        assert!(
            scale >= 0 && scale <= precision as i8,
            "scale should be in [0, precision]"
        );
        Decimal128Type { precision, scale }
    }

    pub fn precision(&self) -> u8 {
        self.precision
    }

    pub fn scale(&self) -> i8 {
        self.scale
    }
}

impl DataType for Decimal128Type {
    fn name(&self) -> String {
        format!("Decimal({}, {})", self.precision, self.scale)
    }

    fn logical_type_id(&self) -> LogicalTypeId {
        LogicalTypeId::Decimal128
    }

    fn default_value(&self) -> Value {
        Value::Decimal128(Decimal128::default())
    }

    fn as_arrow_type(&self) -> ArrowDataType {
        ArrowDataType::Decimal128(self.precision, self.scale)
    }

    fn create_mutable_vector(&self, capacity: usize) -> Box<dyn MutableVector> {
        Box::new(
            Decimal128VectorBuilder::with_capacity(capacity)
                .with_precision_and_scale(self.precision, self.scale)
                // safe to unwrap because we have validated the precision and scale in new()
                .unwrap(),
        )
    }

    fn try_cast(&self, val: Value) -> Option<Value> {
        match val {
            Value::Null => Some(Value::Null),
            Value::Decimal128(_) => Some(val),
            _ => None,
        }
    }
}
