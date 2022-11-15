use std::fmt;

use arrow::array::Array;
use common_query::error::{FromArrowArraySnafu, Result, TypeCastSnafu};
use common_query::prelude::{Signature, Volatility};
use datatypes::prelude::*;
use datatypes::vectors::{Helper, VectorRef};
use snafu::ResultExt;

use crate::scalars::function::{Function, FunctionContext};

/// generates rates from a sequence of adjacent data points.
#[derive(Clone, Debug, Default)]
pub struct RateFunction;

impl fmt::Display for RateFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RATE")
    }
}

impl Function for RateFunction {
    fn name(&self) -> &str {
        "rate"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::float64_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::uniform(2, ConcreteDataType::numerics(), Volatility::Immutable)
    }

    fn eval(&self, _func_ctx: FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        let val = &columns[0].to_arrow_array();
        let val_0 = val.slice(0, val.len() - 1);
        let val_1 = val.slice(1, val.len() - 1);
        let dv = arrow::compute::arithmetics::sub(&*val_1, &*val_0);
        let ts = &columns[1].to_arrow_array();
        let ts_0 = ts.slice(0, ts.len() - 1);
        let ts_1 = ts.slice(1, ts.len() - 1);
        let dt = arrow::compute::arithmetics::sub(&*ts_1, &*ts_0);
        fn all_to_f64(array: &dyn Array) -> Result<Box<dyn Array>> {
            Ok(arrow::compute::cast::cast(
                array,
                &arrow::datatypes::DataType::Float64,
                arrow::compute::cast::CastOptions {
                    wrapped: true,
                    partial: true,
                },
            )
            .context(TypeCastSnafu {
                typ: arrow::datatypes::DataType::Float64,
            })?)
        }
        let dv = all_to_f64(&*dv)?;
        let dt = all_to_f64(&*dt)?;
        let rate = arrow::compute::arithmetics::div(&*dv, &*dt);
        let v = Helper::try_into_vector(&rate).context(FromArrowArraySnafu)?;
        Ok(v)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Float64Array;
    use common_query::prelude::TypeSignature;
    use datatypes::vectors::{Float32Vector, Int64Vector};

    use super::*;
    #[test]
    fn test_rate_function() {
        let rate = RateFunction::default();
        assert_eq!("rate", rate.name());
        assert_eq!(
            ConcreteDataType::float64_datatype(),
            rate.return_type(&[]).unwrap()
        );
        assert!(matches!(rate.signature(),
                         Signature {
                             type_signature: TypeSignature::Uniform(2, valid_types),
                             volatility: Volatility::Immutable
                         } if  valid_types == ConcreteDataType::numerics()
        ));
        let values = vec![1.0, 3.0, 6.0];
        let ts = vec![0, 1, 2];

        let args: Vec<VectorRef> = vec![
            Arc::new(Float32Vector::from_vec(values)),
            Arc::new(Int64Vector::from_vec(ts)),
        ];
        let vector = rate.eval(FunctionContext::default(), &args).unwrap();
        let arr = vector.to_arrow_array();
        let expect = Arc::new(Float64Array::from_vec(vec![2.0, 3.0]));
        let res = arrow::compute::comparison::eq(&*arr, &*expect);
        res.iter().for_each(|x| assert!(matches!(x, Some(true))));
    }
}
