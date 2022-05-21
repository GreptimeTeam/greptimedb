use std::sync::Arc;

use datatypes::prelude::ConcreteDataType;
use datatypes::vectors::VectorRef;
use snafu::ResultExt;

use crate::error::{ExecuteFunctionSnafu, Result};
use crate::prelude::{ColumnarValue, ScalarValue};

/// Scalar function
///
/// The Fn param is the wrapped function but be aware that the function will
/// be passed with the slice / vec of columnar values (either scalar or array)
/// with the exception of zero param function, where a singular element vec
/// will be passed. In that case the single element is a null array to indicate
/// the batch's row count (so that the generative zero-argument function can know
/// the result array size).
pub type ScalarFunctionImplementation =
    Arc<dyn Fn(&[ColumnarValue]) -> Result<ColumnarValue> + Send + Sync>;

/// A function's return type
pub type ReturnTypeFunction =
    Arc<dyn Fn(&[ConcreteDataType]) -> Result<Arc<ConcreteDataType>> + Send + Sync>;

/// This signature corresponds to which types an aggregator serializes
/// its state, given its return datatype.
pub type StateTypeFunction =
    Arc<dyn Fn(&ConcreteDataType) -> Result<Arc<Vec<ConcreteDataType>>> + Send + Sync>;

/// decorates a function to handle [`ScalarValue`]s by converting them to arrays before calling the function
/// and vice-versa after evaluation.
pub fn make_scalar_function<F>(inner: F) -> ScalarFunctionImplementation
where
    F: Fn(&[VectorRef]) -> Result<VectorRef> + Sync + Send + 'static,
{
    Arc::new(move |args: &[ColumnarValue]| {
        // first, identify if any of the arguments is an vector. If yes, store its `len`,
        // as any scalar will need to be converted to an vector of len `len`.
        let len = args
            .iter()
            .fold(Option::<usize>::None, |acc, arg| match arg {
                ColumnarValue::Scalar(_) => acc,
                ColumnarValue::Vector(v) => Some(v.len()),
            });

        // to array
        let args: Result<Vec<_>> = if let Some(len) = len {
            args.iter()
                .map(|arg| arg.clone().try_into_vector(len))
                .collect()
        } else {
            args.iter()
                .map(|arg| arg.clone().try_into_vector(1))
                .collect()
        };

        let result = (inner)(&args?);

        // maybe back to scalar
        if len.is_some() {
            result.map(ColumnarValue::Vector)
        } else {
            ScalarValue::try_from_array(&result?.to_arrow_array(), 0)
                .map(ColumnarValue::Scalar)
                .context(ExecuteFunctionSnafu)
        }
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::prelude::ScalarVector;
    use datatypes::prelude::Vector;
    use datatypes::vectors::BooleanVector;

    use super::*;

    #[test]
    fn test_make_scalar_function() {
        let and_fun = |args: &[VectorRef]| -> Result<VectorRef> {
            let left = &args[0]
                .as_any()
                .downcast_ref::<BooleanVector>()
                .expect("cast failed");
            let right = &args[1]
                .as_any()
                .downcast_ref::<BooleanVector>()
                .expect("cast failed");

            let result = left
                .iter_data()
                .zip(right.iter_data())
                .map(|(left, right)| match (left, right) {
                    (Some(left), Some(right)) => Some(left && right),
                    _ => None,
                })
                .collect::<BooleanVector>();
            Ok(Arc::new(result) as VectorRef)
        };

        let and_fun = make_scalar_function(and_fun);

        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))),
            ColumnarValue::Vector(Arc::new(BooleanVector::from(vec![
                true, false, false, true,
            ]))),
        ];

        let vec = (and_fun)(&args).unwrap();

        match vec {
            ColumnarValue::Vector(vec) => {
                let vec = vec.as_any().downcast_ref::<BooleanVector>().unwrap();

                assert_eq!(4, vec.len());
                for i in 0..4 {
                    assert_eq!(
                        i == 0 || i == 3,
                        vec.get_data(i).unwrap(),
                        "failed at {}",
                        i
                    )
                }
            }
            _ => unreachable!(),
        }
    }
}
