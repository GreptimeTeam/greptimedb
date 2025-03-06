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

//! Implementation of the scalar function `hll_count`.

use std::fmt;
use std::fmt::Display;
use std::sync::Arc;

use common_query::error::{DowncastVectorSnafu, InvalidFuncArgsSnafu, Result};
use common_query::prelude::{Signature, Volatility};
use datatypes::data_type::ConcreteDataType;
use datatypes::prelude::Vector;
use datatypes::scalars::{ScalarVector, ScalarVectorBuilder};
use datatypes::vectors::{BinaryVector, MutableVector, UInt64VectorBuilder, VectorRef};
use hyperloglogplus::HyperLogLog;
use snafu::OptionExt;

use crate::aggr::HllStateType;
use crate::function::{Function, FunctionContext};
use crate::function_registry::FunctionRegistry;

const NAME: &str = "hll_count";

/// HllCalcFunction implements the scalar function `hll_count`.
///
/// It accepts one argument:
/// 1. The serialized HyperLogLogPlus state, as produced by the aggregator (binary).
///
/// For each row, it deserializes the sketch and returns the estimated cardinality.
#[derive(Debug, Default)]
pub struct HllCalcFunction;

impl HllCalcFunction {
    pub fn register(registry: &FunctionRegistry) {
        registry.register(Arc::new(HllCalcFunction));
    }
}

impl Display for HllCalcFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", NAME.to_ascii_uppercase())
    }
}

impl Function for HllCalcFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::uint64_datatype())
    }

    fn signature(&self) -> Signature {
        // Only argument: HyperLogLogPlus state (binary)
        Signature::exact(
            vec![ConcreteDataType::binary_datatype()],
            Volatility::Immutable,
        )
    }

    fn eval(&self, _func_ctx: &FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        if columns.len() != 1 {
            return InvalidFuncArgsSnafu {
                err_msg: format!("hll_count expects 1 argument, got {}", columns.len()),
            }
            .fail();
        }

        let hll_vec = columns[0]
            .as_any()
            .downcast_ref::<BinaryVector>()
            .with_context(|| DowncastVectorSnafu {
                err_msg: format!("expect BinaryVector, got {}", columns[0].vector_type_name()),
            })?;
        let len = hll_vec.len();
        let mut builder = UInt64VectorBuilder::with_capacity(len);

        for i in 0..len {
            let hll_opt = hll_vec.get_data(i);

            if hll_opt.is_none() {
                builder.push_null();
                continue;
            }

            let hll_bytes = hll_opt.unwrap();

            // Deserialize the HyperLogLogPlus from its bincode representation
            let mut hll: HllStateType = match bincode::deserialize(hll_bytes) {
                Ok(h) => h,
                Err(e) => {
                    common_telemetry::trace!("Failed to deserialize HyperLogLogPlus: {}", e);
                    builder.push_null();
                    continue;
                }
            };

            builder.push(Some(hll.count().round() as u64));
        }

        Ok(builder.to_vector())
    }
}

#[cfg(test)]
mod tests {
    use datatypes::vectors::BinaryVector;

    use super::*;
    use crate::utils::FixedRandomState;

    #[test]
    fn test_hll_count_function() {
        let function = HllCalcFunction;
        assert_eq!("hll_count", function.name());
        assert_eq!(
            ConcreteDataType::uint64_datatype(),
            function
                .return_type(&[ConcreteDataType::uint64_datatype()])
                .unwrap()
        );

        // Create a test HLL
        let mut hll = HllStateType::new(14, FixedRandomState::new()).unwrap();
        for i in 1..=10 {
            hll.insert(&i.to_string());
        }

        let serialized_bytes = bincode::serialize(&hll).unwrap();
        let args: Vec<VectorRef> = vec![Arc::new(BinaryVector::from(vec![Some(serialized_bytes)]))];

        let result = function.eval(&FunctionContext::default(), &args).unwrap();
        assert_eq!(result.len(), 1);

        // Test cardinality estimate
        if let datatypes::value::Value::UInt64(v) = result.get(0) {
            assert_eq!(v, 10);
        } else {
            panic!("Expected uint64 value");
        }
    }

    #[test]
    fn test_hll_count_function_errors() {
        let function = HllCalcFunction;

        // Test with invalid number of arguments
        let args: Vec<VectorRef> = vec![];
        let result = function.eval(&FunctionContext::default(), &args);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("hll_count expects 1 argument"));

        // Test with invalid binary data
        let args: Vec<VectorRef> = vec![Arc::new(BinaryVector::from(vec![Some(vec![1, 2, 3])]))]; // Invalid binary data
        let result = function.eval(&FunctionContext::default(), &args).unwrap();
        assert_eq!(result.len(), 1);
        assert!(matches!(result.get(0), datatypes::value::Value::Null));
    }
}
