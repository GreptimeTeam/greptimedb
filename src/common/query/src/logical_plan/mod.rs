mod expr;
mod udf;

use std::sync::Arc;

use datatypes::prelude::ConcreteDataType;

pub use self::expr::Expr;
pub use self::udf::ScalarUDF;
use crate::function::{ReturnTypeFunction, ScalarFunctionImplementation};
use crate::signature::{Signature, Volatility};

/// Creates a new UDF with a specific signature and specific return type.
/// This is a helper function to create a new UDF.
/// The function `create_udf` returns a subset of all possible `ScalarFunction`:
/// * the UDF has a fixed return type
/// * the UDF has a fixed signature (e.g. [f64, f64])
pub fn create_udf(
    name: &str,
    input_types: Vec<ConcreteDataType>,
    return_type: Arc<ConcreteDataType>,
    volatility: Volatility,
    fun: ScalarFunctionImplementation,
) -> ScalarUDF {
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(return_type.clone()));
    ScalarUDF::new(
        name,
        &Signature::exact(input_types, volatility),
        &return_type,
        &fun,
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::BooleanArray;
    use arrow::datatypes::DataType;
    use datafusion_expr::ColumnarValue as DfColumnarValue;
    use datafusion_expr::ScalarUDF as DfScalarUDF;
    use datafusion_expr::TypeSignature as DfTypeSignature;
    use datatypes::prelude::ScalarVector;
    use datatypes::vectors::BooleanVector;
    use datatypes::vectors::VectorRef;

    use super::*;
    use crate::error::Result;
    use crate::function::make_scalar_function;
    use crate::prelude::ScalarValue;
    use crate::signature::TypeSignature;

    #[test]
    fn test_create_udf() {
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

        let input_types = vec![
            ConcreteDataType::boolean_datatype(),
            ConcreteDataType::boolean_datatype(),
        ];

        let return_type = Arc::new(ConcreteDataType::boolean_datatype());

        let udf = create_udf(
            "and",
            input_types.clone(),
            return_type.clone(),
            Volatility::Immutable,
            and_fun.clone(),
        );

        assert_eq!("and", udf.name);
        assert!(
            matches!(&udf.signature.type_signature, TypeSignature::Exact(ts) if ts.clone() == input_types)
        );
        assert_eq!(return_type, (udf.return_type)(&[]).unwrap());

        // test into_df_udf
        let df_udf: DfScalarUDF = udf.into_df_udf();
        assert_eq!("and", df_udf.name);

        let types = vec![DataType::Boolean, DataType::Boolean];
        assert!(
            matches!(&df_udf.signature.type_signature, DfTypeSignature::Exact(ts) if ts.clone() == types)
        );
        assert_eq!(
            Arc::new(DataType::Boolean),
            (df_udf.return_type)(&[]).unwrap()
        );

        let args = vec![
            DfColumnarValue::Scalar(ScalarValue::Boolean(Some(true))),
            DfColumnarValue::Array(Arc::new(BooleanArray::from_slice(vec![
                true, false, false, true,
            ]))),
        ];

        // call the function
        let result = (df_udf.fun)(&args).unwrap();

        match result {
            DfColumnarValue::Array(arr) => {
                let arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
                for i in 0..4 {
                    assert_eq!(i == 0 || i == 3, arr.value(i));
                }
            }
            _ => unreachable!(),
        }
    }
}
