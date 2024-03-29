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

use std::fmt;
use std::sync::Arc;

use common_query::error::Result;
use common_query::prelude::{Signature, Volatility};
use datatypes::data_type::DataType;
use datatypes::prelude::ConcreteDataType;
use datatypes::types::LogicalPrimitiveType;
use datatypes::vectors::VectorRef;
use datatypes::with_match_primitive_type_id;
use num::traits::Pow;
use num_traits::AsPrimitive;

use crate::function::{Function, FunctionContext};
use crate::scalars::expression::{scalar_binary_op, EvalContext};

#[derive(Clone, Debug, Default)]
pub struct PowFunction;

impl Function for PowFunction {
    fn name(&self) -> &str {
        "pow"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::float64_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::uniform(2, ConcreteDataType::numerics(), Volatility::Immutable)
    }

    fn eval(&self, _func_ctx: FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        with_match_primitive_type_id!(columns[0].data_type().logical_type_id(), |$S| {
            with_match_primitive_type_id!(columns[1].data_type().logical_type_id(), |$T| {
                let col = scalar_binary_op::<<$S as LogicalPrimitiveType>::Native, <$T as LogicalPrimitiveType>::Native, f64, _>(&columns[0], &columns[1], scalar_pow, &mut EvalContext::default())?;
                Ok(Arc::new(col))
            },{
                unreachable!()
            })
        },{
            unreachable!()
        })
    }
}

#[inline]
fn scalar_pow<S, T>(value: Option<S>, base: Option<T>, _ctx: &mut EvalContext) -> Option<f64>
where
    S: AsPrimitive<f64>,
    T: AsPrimitive<f64>,
{
    match (value, base) {
        (Some(value), Some(base)) => Some(value.as_().pow(base.as_())),
        _ => None,
    }
}

impl fmt::Display for PowFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "POW")
    }
}

#[cfg(test)]
mod tests {
    use common_query::prelude::TypeSignature;
    use datatypes::value::Value;
    use datatypes::vectors::{Float32Vector, Int8Vector};

    use super::*;
    use crate::function::FunctionContext;
    #[test]
    fn test_pow_function() {
        let pow = PowFunction;

        assert_eq!("pow", pow.name());
        assert_eq!(
            ConcreteDataType::float64_datatype(),
            pow.return_type(&[]).unwrap()
        );

        assert!(matches!(pow.signature(),
                         Signature {
                             type_signature: TypeSignature::Uniform(2, valid_types),
                             volatility: Volatility::Immutable
                         } if  valid_types == ConcreteDataType::numerics()
        ));

        let values = vec![1.0, 2.0, 3.0];
        let bases = vec![0i8, -1i8, 3i8];

        let args: Vec<VectorRef> = vec![
            Arc::new(Float32Vector::from_vec(values.clone())),
            Arc::new(Int8Vector::from_vec(bases.clone())),
        ];

        let vector = pow.eval(FunctionContext::default(), &args).unwrap();
        assert_eq!(3, vector.len());

        for i in 0..3 {
            let p: f64 = (values[i] as f64).pow(bases[i] as f64);
            assert!(matches!(vector.get(i), Value::Float64(v) if v == p));
        }
    }
}
