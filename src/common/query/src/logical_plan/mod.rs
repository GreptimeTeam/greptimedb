mod accumulator;
mod expr;
mod udaf;
mod udf;

use std::sync::Arc;

use datatypes::prelude::ConcreteDataType;

pub use self::accumulator::Accumulator;
pub use self::expr::Expr;
pub use self::udaf::AggregateUdf;
pub use self::udf::ScalarUdf;
use crate::function::{
    AccumulatorFunctionImplementation, ReturnTypeFunction, ScalarFunctionImplementation,
    StateTypeFunction,
};
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
) -> ScalarUdf {
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(return_type.clone()));
    ScalarUdf::new(
        name,
        &Signature::exact(input_types, volatility),
        &return_type,
        &fun,
    )
}

/// Creates a new UDAF with a specific signature, state type and return type.
/// The signature and state type must match the `Accumulator's implementation`.
#[allow(clippy::rc_buffer)]
pub fn create_udaf(
    name: &str,
    input_type: ConcreteDataType,
    return_type: Arc<ConcreteDataType>,
    volatility: Volatility,
    accumulator: AccumulatorFunctionImplementation,
    state_type: Arc<Vec<ConcreteDataType>>,
) -> AggregateUdf {
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(return_type.clone()));
    let state_type: StateTypeFunction = Arc::new(move |_| Ok(state_type.clone()));
    AggregateUdf::new(
        name,
        &Signature::exact(vec![input_type], volatility),
        &return_type,
        &accumulator,
        &state_type,
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

    #[derive(Debug)]
    struct DummyAccumulator;

    impl Accumulator for DummyAccumulator {
        fn state(&self) -> Result<Vec<ScalarValue>> {
            Ok(vec![])
        }

        fn update_batch(&mut self, _values: &[VectorRef]) -> Result<()> {
            Ok(())
        }

        fn merge_batch(&mut self, _states: &[VectorRef]) -> Result<()> {
            Ok(())
        }

        fn evaluate(&self) -> Result<ScalarValue> {
            Ok(ScalarValue::Int32(Some(0)))
        }
    }

    #[tokio::test]
    async fn test_create_udaf() -> Result<()> {
        let input_type = ConcreteDataType::float64_datatype();
        let return_type = ConcreteDataType::float64_datatype();
        let state_type = vec![
            ConcreteDataType::float64_datatype(),
            ConcreteDataType::uint32_datatype(),
        ];
        let udaf = create_udaf(
            "dummy_udaf",
            input_type.clone(),
            Arc::new(return_type.clone()),
            Volatility::Immutable,
            Arc::new(|| Ok(Box::new(DummyAccumulator))),
            Arc::new(state_type.clone()),
        );
        assert_eq!("dummy_udaf", udaf.name);

        let signature = udaf.signature;
        assert_eq!(
            TypeSignature::Exact(vec![input_type]),
            signature.type_signature
        );
        assert_eq!(Volatility::Immutable, signature.volatility);

        assert_eq!(Arc::new(return_type), (udaf.return_type)(&[]).unwrap());
        assert_eq!(
            Arc::new(state_type),
            (udaf.state_type)(&ConcreteDataType::float64_datatype()).unwrap()
        );
        Ok(())
    }
}
