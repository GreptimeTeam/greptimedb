use std::fmt;
use std::sync::Arc;

use common_query::prelude::{Signature, Volatility};
use datatypes::data_type::ConcreteDataType;
use datatypes::data_type::DataType;
use datatypes::type_id::LogicalTypeId;
use datatypes::vectors::VectorRef;
use datatypes::with_match_primitive_type_id;
use num_traits::AsPrimitive;

use crate::error::Result;
use crate::scalars::expression::{scalar_binary_op, EvalContext};
use crate::scalars::function::{Function, FunctionContext};
use crate::scalars::numerics;

/// numpy.clip function, https://numpy.org/doc/stable/reference/generated/numpy.clip.html
#[derive(Clone, Debug, Default)]
pub struct ClipFuncton;

impl Function for ClipFuncton {
    fn name(&self) -> &str {
        "clip"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::float64_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::uniform(3, numerics(), Volatility::Immutable)
    }

    fn eval(&self, _func_ctx: FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        with_match_primitive_type_id!(columns[0].data_type().logical_type_id(), |$S| {
            with_match_primitive_type_id!(columns[1].data_type().logical_type_id(), |$T| {
                with_match_primitive_type_id!(columns[2].data_type().logical_type_id(), |$R| {
                    // clip(a, min, max)
                    // Equivalent to minimum(max, maximum(a, min))
                    let col: VectorRef = Arc::new(scalar_binary_op::<$S, $T, f64, _>(&columns[0], &columns[1], scalar_max, &mut EvalContext::default())?);
                    let col = scalar_binary_op::<f64, $R, f64, _>(&col, &columns[2], scalar_min, &mut EvalContext::default())?;
                    Ok(Arc::new(col))
                }, {
                    unreachable!()
                })
            }, {
                unreachable!()
            })
        }, {
            unreachable!()
        })
    }
}

#[inline]
fn scalar_min<S, T>(value: Option<S>, base: Option<T>, _ctx: &mut EvalContext) -> Option<f64>
where
    S: AsPrimitive<f64>,
    T: AsPrimitive<f64>,
{
    match (value, base) {
        (Some(value), Some(base)) => Some(f64::min(value.as_(), base.as_())),
        _ => None,
    }
}

#[inline]
fn scalar_max<S, T>(value: Option<S>, base: Option<T>, _ctx: &mut EvalContext) -> Option<f64>
where
    S: AsPrimitive<f64>,
    T: AsPrimitive<f64>,
{
    match (value, base) {
        (Some(value), Some(base)) => Some(f64::max(value.as_(), base.as_())),
        _ => None,
    }
}

impl fmt::Display for ClipFuncton {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CLIP")
    }
}

#[cfg(test)]
mod tests {
    use common_query::prelude::TypeSignature;
    use datatypes::value::Value;
    use datatypes::vectors::{ConstantVector, Int32Vector};

    use super::*;
    #[test]
    fn test_clip_function() {
        let clip = ClipFuncton::default();

        assert_eq!("clip", clip.name());
        assert_eq!(
            ConcreteDataType::float64_datatype(),
            clip.return_type(&[]).unwrap()
        );

        assert!(matches!(clip.signature(),
                         Signature {
                             type_signature: TypeSignature::Uniform(3, valid_types),
                             volatility: Volatility::Immutable
                         } if  valid_types == numerics()
        ));

        let args: Vec<VectorRef> = vec![
            Arc::new(Int32Vector::from_values(0..10)),
            Arc::new(ConstantVector::new(
                Arc::new(Int32Vector::from_vec(vec![3])),
                10,
            )),
            Arc::new(ConstantVector::new(
                Arc::new(Int32Vector::from_vec(vec![6])),
                10,
            )),
        ];

        let vector = clip.eval(FunctionContext::default(), &args).unwrap();
        assert_eq!(10, vector.len());

        // clip([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], 3, 6) = [3, 3, 3, 3, 4, 5, 6, 6, 6, 6]
        for i in 0..10 {
            if i <= 3 {
                assert!(matches!(vector.get_unchecked(i), Value::Float64(v) if v == 3.0));
            } else if i <= 6 {
                assert!(matches!(vector.get_unchecked(i), Value::Float64(v) if v == (i as f64)));
            } else {
                assert!(matches!(vector.get_unchecked(i), Value::Float64(v) if v == 6.0));
            }
        }
    }
}
