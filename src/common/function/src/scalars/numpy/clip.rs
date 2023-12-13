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
use datatypes::arrow::compute;
use datatypes::arrow::datatypes::ArrowPrimitiveType;
use datatypes::data_type::ConcreteDataType;
use datatypes::prelude::*;
use datatypes::vectors::PrimitiveVector;
use paste::paste;

use crate::function::{Function, FunctionContext};
use crate::scalars::expression::{scalar_binary_op, EvalContext};

/// numpy.clip function, <https://numpy.org/doc/stable/reference/generated/numpy.clip.html>
#[derive(Clone, Debug, Default)]
pub struct ClipFunction;

macro_rules! define_eval {
    ($O: ident) => {
        paste! {
            fn [<eval_ $O>](columns: &[VectorRef]) -> Result<VectorRef> {
                fn cast_vector(input: &VectorRef) -> VectorRef {
                    Arc::new(PrimitiveVector::<<$O as WrapperType>::LogicalType>::try_from_arrow_array(
                        compute::cast(&input.to_arrow_array(), &<<<$O as WrapperType>::LogicalType as LogicalPrimitiveType>::ArrowPrimitive as ArrowPrimitiveType>::DATA_TYPE).unwrap()
                    ).unwrap()) as _
                }
                let operator_1 = cast_vector(&columns[0]);
                let operator_2 = cast_vector(&columns[1]);
                let operator_3 = cast_vector(&columns[2]);

                // clip(a, min, max) is equals to min(max(a, min), max)
                let col: VectorRef = Arc::new(scalar_binary_op::<$O, $O, $O, _>(
                    &operator_1,
                    &operator_2,
                    scalar_max,
                    &mut EvalContext::default(),
                )?);
                let col = scalar_binary_op::<$O, $O, $O, _>(
                    &col,
                    &operator_3,
                    scalar_min,
                    &mut EvalContext::default(),
                )?;
                Ok(Arc::new(col))
            }
        }
    };
}

define_eval!(i64);
define_eval!(u64);
define_eval!(f64);

impl Function for ClipFunction {
    fn name(&self) -> &str {
        "clip"
    }

    fn return_type(&self, input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        if input_types.iter().all(ConcreteDataType::is_signed) {
            Ok(ConcreteDataType::int64_datatype())
        } else if input_types.iter().all(ConcreteDataType::is_unsigned) {
            Ok(ConcreteDataType::uint64_datatype())
        } else {
            Ok(ConcreteDataType::float64_datatype())
        }
    }

    fn signature(&self) -> Signature {
        Signature::uniform(3, ConcreteDataType::numerics(), Volatility::Immutable)
    }

    fn eval(&self, _func_ctx: FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        if columns.iter().all(|v| v.data_type().is_signed()) {
            eval_i64(columns)
        } else if columns.iter().all(|v| v.data_type().is_unsigned()) {
            eval_u64(columns)
        } else {
            eval_f64(columns)
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
fn scalar_min<O>(left: Option<O>, right: Option<O>, _ctx: &mut EvalContext) -> Option<O>
where
    O: Scalar + Copy + PartialOrd,
{
    match (left, right) {
        (Some(left), Some(right)) => Some(min(left, right)),
        _ => None,
    }
}

#[inline]
fn scalar_max<O>(left: Option<O>, right: Option<O>, _ctx: &mut EvalContext) -> Option<O>
where
    O: Scalar + Copy + PartialOrd,
{
    match (left, right) {
        (Some(left), Some(right)) => Some(max(left, right)),
        _ => None,
    }
}

impl fmt::Display for ClipFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CLIP")
    }
}

#[cfg(test)]
mod tests {
    use common_query::prelude::TypeSignature;
    use datatypes::value::Value;
    use datatypes::vectors::{
        ConstantVector, Float32Vector, Int16Vector, Int32Vector, Int8Vector, UInt16Vector,
        UInt32Vector, UInt8Vector,
    };

    use super::*;

    #[test]
    fn test_clip_signature() {
        let clip = ClipFunction;

        assert_eq!("clip", clip.name());
        assert_eq!(
            ConcreteDataType::int64_datatype(),
            clip.return_type(&[]).unwrap()
        );

        assert_eq!(
            ConcreteDataType::int64_datatype(),
            clip.return_type(&[
                ConcreteDataType::int16_datatype(),
                ConcreteDataType::int64_datatype(),
                ConcreteDataType::int8_datatype()
            ])
            .unwrap()
        );
        assert_eq!(
            ConcreteDataType::uint64_datatype(),
            clip.return_type(&[
                ConcreteDataType::uint16_datatype(),
                ConcreteDataType::uint64_datatype(),
                ConcreteDataType::uint8_datatype()
            ])
            .unwrap()
        );
        assert_eq!(
            ConcreteDataType::float64_datatype(),
            clip.return_type(&[
                ConcreteDataType::uint16_datatype(),
                ConcreteDataType::int64_datatype(),
                ConcreteDataType::uint8_datatype()
            ])
            .unwrap()
        );

        assert!(matches!(clip.signature(),
                         Signature {
                             type_signature: TypeSignature::Uniform(3, valid_types),
                             volatility: Volatility::Immutable
                         } if  valid_types == ConcreteDataType::numerics()
        ));
    }

    #[test]
    fn test_clip_fn_signed() {
        // eval with signed integers
        let args: Vec<VectorRef> = vec![
            Arc::new(Int32Vector::from_values(0..10)),
            Arc::new(ConstantVector::new(
                Arc::new(Int8Vector::from_vec(vec![3])),
                10,
            )),
            Arc::new(ConstantVector::new(
                Arc::new(Int16Vector::from_vec(vec![6])),
                10,
            )),
        ];

        let vector = ClipFunction
            .eval(FunctionContext::default(), &args)
            .unwrap();
        assert_eq!(10, vector.len());

        // clip([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], 3, 6) = [3, 3, 3, 3, 4, 5, 6, 6, 6, 6]
        for i in 0..10 {
            if i <= 3 {
                assert!(matches!(vector.get(i), Value::Int64(v) if v == 3));
            } else if i <= 6 {
                assert!(matches!(vector.get(i), Value::Int64(v) if v == (i as i64)));
            } else {
                assert!(matches!(vector.get(i), Value::Int64(v) if v == 6));
            }
        }
    }

    #[test]
    fn test_clip_fn_unsigned() {
        // eval with unsigned integers
        let args: Vec<VectorRef> = vec![
            Arc::new(UInt8Vector::from_values(0..10)),
            Arc::new(ConstantVector::new(
                Arc::new(UInt32Vector::from_vec(vec![3])),
                10,
            )),
            Arc::new(ConstantVector::new(
                Arc::new(UInt16Vector::from_vec(vec![6])),
                10,
            )),
        ];

        let vector = ClipFunction
            .eval(FunctionContext::default(), &args)
            .unwrap();
        assert_eq!(10, vector.len());

        // clip([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], 3, 6) = [3, 3, 3, 3, 4, 5, 6, 6, 6, 6]
        for i in 0..10 {
            if i <= 3 {
                assert!(matches!(vector.get(i), Value::UInt64(v) if v == 3));
            } else if i <= 6 {
                assert!(matches!(vector.get(i), Value::UInt64(v) if v == (i as u64)));
            } else {
                assert!(matches!(vector.get(i), Value::UInt64(v) if v == 6));
            }
        }
    }

    #[test]
    fn test_clip_fn_float() {
        // eval with floats
        let args: Vec<VectorRef> = vec![
            Arc::new(Int8Vector::from_values(0..10)),
            Arc::new(ConstantVector::new(
                Arc::new(UInt32Vector::from_vec(vec![3])),
                10,
            )),
            Arc::new(ConstantVector::new(
                Arc::new(Float32Vector::from_vec(vec![6f32])),
                10,
            )),
        ];

        let vector = ClipFunction
            .eval(FunctionContext::default(), &args)
            .unwrap();
        assert_eq!(10, vector.len());

        // clip([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], 3, 6) = [3, 3, 3, 3, 4, 5, 6, 6, 6, 6]
        for i in 0..10 {
            if i <= 3 {
                assert!(matches!(vector.get(i), Value::Float64(v) if v == 3.0));
            } else if i <= 6 {
                assert!(matches!(vector.get(i), Value::Float64(v) if v == (i as f64)));
            } else {
                assert!(matches!(vector.get(i), Value::Float64(v) if v == 6.0));
            }
        }
    }
}
