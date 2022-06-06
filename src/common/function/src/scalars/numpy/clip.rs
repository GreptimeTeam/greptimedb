use std::fmt;
use std::sync::Arc;

use common_query::prelude::{Signature, Volatility};
use datatypes::data_type::ConcreteDataType;
use datatypes::data_type::DataType;
use datatypes::prelude::{Scalar, VectorRef};
use datatypes::type_id::LogicalTypeId;
use datatypes::with_match_primitive_type_id;
use num_traits::AsPrimitive;
use paste::paste;

use crate::error::Result;
use crate::scalars::expression::{scalar_binary_op, EvalContext};
use crate::scalars::function::{Function, FunctionContext};
use crate::scalars::numerics;

/// numpy.clip function, https://numpy.org/doc/stable/reference/generated/numpy.clip.html
#[derive(Clone, Debug, Default)]
pub struct ClipFuncton;

fn is_float_type(ty: &ConcreteDataType) -> bool {
    matches!(ty, ConcreteDataType::Float64(_)) || matches!(ty, ConcreteDataType::Float32(_))
}

macro_rules! define_eval {
    ($O: ident) => {
        paste! {
            fn [<eval_ $O>](columns: &[VectorRef]) -> Result<VectorRef> {
                with_match_primitive_type_id!(columns[0].data_type().logical_type_id(), |$S| {
                    with_match_primitive_type_id!(columns[1].data_type().logical_type_id(), |$T| {
                        with_match_primitive_type_id!(columns[2].data_type().logical_type_id(), |$R| {
                            // clip(a, min, max) is equals to min(max(a, min), max)
                            let col: VectorRef = Arc::new(scalar_binary_op::<$S, $T, $O, _>(&columns[0], &columns[1], scalar_max, &mut EvalContext::default())?);
                            let col = scalar_binary_op::<$O, $R, $O, _>(&col, &columns[2], scalar_min, &mut EvalContext::default())?;
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
    }
}

define_eval!(i64);
define_eval!(f64);

impl Function for ClipFuncton {
    fn name(&self) -> &str {
        "clip"
    }

    fn return_type(&self, input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        if input_types.iter().any(is_float_type) {
            Ok(ConcreteDataType::float64_datatype())
        } else {
            Ok(ConcreteDataType::int64_datatype())
        }
    }

    fn signature(&self) -> Signature {
        Signature::uniform(3, numerics(), Volatility::Immutable)
    }

    fn eval(&self, _func_ctx: FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        if columns.iter().any(|v| is_float_type(&v.data_type())) {
            eval_f64(columns)
        } else {
            eval_i64(columns)
        }
    }
}

#[inline]
pub fn min<T: PartialOrd>(input: T, min: T) -> T {
    if input < min {
        input
    } else {
        min
    }
}

#[inline]
pub fn max<T: PartialOrd>(input: T, max: T) -> T {
    if input > max {
        input
    } else {
        max
    }
}

#[inline]
fn scalar_min<S, T, O>(left: Option<S>, right: Option<T>, _ctx: &mut EvalContext) -> Option<O>
where
    S: AsPrimitive<O>,
    T: AsPrimitive<O>,
    O: Scalar + Copy + PartialOrd,
{
    match (left, right) {
        (Some(left), Some(right)) => Some(min(left.as_(), right.as_())),
        _ => None,
    }
}

#[inline]
fn scalar_max<S, T, O>(left: Option<S>, right: Option<T>, _ctx: &mut EvalContext) -> Option<O>
where
    S: AsPrimitive<O>,
    T: AsPrimitive<O>,
    O: Scalar + Copy + PartialOrd,
{
    match (left, right) {
        (Some(left), Some(right)) => Some(max(left.as_(), right.as_())),
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
    use datatypes::vectors::{ConstantVector, Float32Vector, Int32Vector};

    use super::*;
    #[test]
    fn test_clip_function() {
        let clip = ClipFuncton::default();

        assert_eq!("clip", clip.name());
        assert_eq!(
            ConcreteDataType::int64_datatype(),
            clip.return_type(&[]).unwrap()
        );

        assert!(matches!(clip.signature(),
                         Signature {
                             type_signature: TypeSignature::Uniform(3, valid_types),
                             volatility: Volatility::Immutable
                         } if  valid_types == numerics()
        ));

        // eval with integers
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
                assert!(matches!(vector.get_unchecked(i), Value::Int64(v) if v == 3));
            } else if i <= 6 {
                assert!(matches!(vector.get_unchecked(i), Value::Int64(v) if v == (i as i64)));
            } else {
                assert!(matches!(vector.get_unchecked(i), Value::Int64(v) if v == 6));
            }
        }

        // eval with floats
        let args: Vec<VectorRef> = vec![
            Arc::new(Int32Vector::from_values(0..10)),
            Arc::new(ConstantVector::new(
                Arc::new(Int32Vector::from_vec(vec![3])),
                10,
            )),
            Arc::new(ConstantVector::new(
                Arc::new(Float32Vector::from_vec(vec![6f32])),
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
