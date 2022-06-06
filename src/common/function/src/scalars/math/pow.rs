use std::fmt;
use std::sync::Arc;

use common_query::prelude::{Signature, Volatility};
use datatypes::data_type::DataType;
use datatypes::prelude::ConcreteDataType;
use datatypes::type_id::LogicalTypeId;
use datatypes::vectors::VectorRef;
use datatypes::with_match_primitive_type_id;
use num::traits::Pow;
use num_traits::AsPrimitive;

use crate::error::Result;
use crate::scalars::expression::{scalar_binary_op, EvalContext};
use crate::scalars::function::{Function, FunctionContext};
use crate::scalars::numerics;

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
        Signature::uniform(2, numerics(), Volatility::Immutable)
    }

    fn eval(&self, _func_ctx: FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        with_match_primitive_type_id!(columns[0].data_type().logical_type_id(), |$S| {
            with_match_primitive_type_id!(columns[1].data_type().logical_type_id(), |$T| {
                let col = scalar_binary_op::<$S, $T, f64, _>(&columns[0], &columns[1], scalar_pow, &mut EvalContext::default())?;
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
    use datatypes::vectors::{ConstantVector, Float32Vector, Int8Vector};

    use super::*;
    #[test]
    fn test_pow_function() {
        let pow = PowFunction::default();

        assert_eq!("pow", pow.name());
        assert_eq!(
            ConcreteDataType::float64_datatype(),
            pow.return_type(&[]).unwrap()
        );

        assert!(matches!(pow.signature(),
                         Signature {
                             type_signature: TypeSignature::Uniform(2, valid_types),
                             volatility: Volatility::Immutable
                         } if  valid_types == numerics()
        ));

        let args: Vec<VectorRef> = vec![
            Arc::new(ConstantVector::new(
                Arc::new(Float32Vector::from_vec(vec![
                    std::f32::consts::PI,
                    -1.42,
                    2.0,
                ])),
                3,
            )),
            Arc::new(Int8Vector::from_vec(vec![-1i8, 3, 123])),
        ];

        let vector = pow.eval(FunctionContext::default(), &args).unwrap();
        assert_eq!(3, vector.len());

        for i in 0..3 {
            match i {
                0 => assert!(
                    matches!(vector.get_unchecked(i), Value::Float64(v) if v == 0.31830987732601135)
                ),
                1 => assert!(
                    matches!(vector.get_unchecked(i), Value::Float64(v) if v == 31.006279268784656)
                ),
                2 => assert!(
                    matches!(vector.get_unchecked(i), Value::Float64(v) if v == 1.4107037729615416e61)
                ),
                _ => unreachable!(),
            }
        }
    }
}
