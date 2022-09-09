use std::sync::Arc;

use common_function_macro::{as_aggr_func_creator, AggrFuncTypeStore};
use common_query::error::{
    self, BadAccumulatorImplSnafu, CreateAccumulatorSnafu, DowncastVectorSnafu,
    FromScalarValueSnafu, GenerateFunctionSnafu, InvalidInputColSnafu, Result,
};
use common_query::logical_plan::{Accumulator, AggregateFunctionCreator};
use common_query::prelude::*;
use datatypes::prelude::*;
use datatypes::value::{ListValue, OrderedFloat};
use datatypes::vectors::{ConstantVector, Float64Vector, ListVector};
use datatypes::with_match_primitive_type_id;
use num_traits::AsPrimitive;
use snafu::{ensure, OptionExt, ResultExt};
use statrs::distribution::{Continuous, Normal};
use statrs::statistics::Statistics;

// https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.norm.html

#[derive(Debug, Default)]
pub struct ScipyStatsNormPdf<T>
where
    T: Primitive + AsPrimitive<f64> + std::iter::Sum<T>,
{
    values: Vec<T>,
    x: Option<f64>,
}

impl<T> ScipyStatsNormPdf<T>
where
    T: Primitive + AsPrimitive<f64> + std::iter::Sum<T>,
{
    fn push(&mut self, value: T) {
        self.values.push(value);
    }
}

impl<T> Accumulator for ScipyStatsNormPdf<T>
where
    T: Primitive + AsPrimitive<f64> + std::iter::Sum<T>,
    for<'a> T: Scalar<RefType<'a> = T>,
{
    fn state(&self) -> Result<Vec<Value>> {
        let nums = self
            .values
            .iter()
            .map(|&x| x.into())
            .collect::<Vec<Value>>();
        Ok(vec![
            Value::List(ListValue::new(
                Some(Box::new(nums)),
                T::default().into().data_type(),
            )),
            self.x.into(),
        ])
    }

    fn update_batch(&mut self, values: &[VectorRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        ensure!(values.len() == 2, InvalidInputStateSnafu);
        ensure!(values[1].len() == values[0].len(), InvalidInputStateSnafu);

        if values[0].len() == 0 {
            return Ok(());
        }

        let column = &values[0];
        let mut len = 1;
        let column: &<T as Scalar>::VectorType = if column.is_const() {
            len = column.len();
            let column: &ConstantVector = unsafe { VectorHelper::static_cast(column) };
            unsafe { VectorHelper::static_cast(column.inner()) }
        } else {
            unsafe { VectorHelper::static_cast(column) }
        };

        let x = &values[1];
        let x = VectorHelper::check_get_scalar::<f64>(x).context(error::InvalidInputsSnafu {
            err_msg: "expecting \"SCIPYSTATSNORMPDF\" function's second argument to be a positive integer",
        })?;
        let first = x.get(0);
        ensure!(!first.is_null(), InvalidInputColSnafu);
        let first = match first {
            Value::Float64(OrderedFloat(v)) => v,
            // unreachable because we have checked `first` is not null and is i64 above
            _ => unreachable!(),
        };
        if let Some(x) = self.x {
            ensure!(x == first, InvalidInputColSnafu);
        } else {
            self.x = Some(first);
        };

        (0..len).for_each(|_| {
            for v in column.iter_data().flatten() {
                self.push(v);
            }
        });
        Ok(())
    }

    fn merge_batch(&mut self, states: &[VectorRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        ensure!(
            states.len() == 2,
            BadAccumulatorImplSnafu {
                err_msg: "expect 2 states in `merge_batch`",
            }
        );

        let x = &states[1];
        let x = x
            .as_any()
            .downcast_ref::<Float64Vector>()
            .with_context(|| DowncastVectorSnafu {
                err_msg: format!(
                    "expect Float64Vector, got vector type {}",
                    x.vector_type_name()
                ),
            })?;
        let x = x.get(0);
        if x.is_null() {
            return Ok(());
        }
        let x = match x {
            Value::Float64(OrderedFloat(x)) => x,
            _ => unreachable!(),
        };
        self.x = Some(x);

        let values = &states[0];
        let values = values
            .as_any()
            .downcast_ref::<ListVector>()
            .with_context(|| DowncastVectorSnafu {
                err_msg: format!(
                    "expect ListVector, got vector type {}",
                    values.vector_type_name()
                ),
            })?;
        for value in values.values_iter() {
            let value = value.context(FromScalarValueSnafu)?;
            let column: &<T as Scalar>::VectorType = unsafe { VectorHelper::static_cast(&value) };
            for v in column.iter_data().flatten() {
                self.push(v);
            }
        }
        Ok(())
    }

    fn evaluate(&self) -> Result<Value> {
        let values = self.values.iter().map(|&v| v.as_()).collect::<Vec<_>>();
        let mean = values.clone().mean();
        let std_dev = values.std_dev();
        if mean.is_nan() || std_dev.is_nan() {
            Ok(Value::Null)
        } else {
            let x = if let Some(x) = self.x {
                x
            } else {
                return Ok(Value::Null);
            };
            let n = Normal::new(mean, std_dev).context(GenerateFunctionSnafu)?;
            Ok(n.pdf(x).into())
        }
    }
}

#[as_aggr_func_creator]
#[derive(Debug, Default, AggrFuncTypeStore)]
pub struct ScipyStatsNormPdfAccumulatorCreator {}

impl AggregateFunctionCreator for ScipyStatsNormPdfAccumulatorCreator {
    fn creator(&self) -> AccumulatorCreatorFunction {
        let creator: AccumulatorCreatorFunction = Arc::new(move |types: &[ConcreteDataType]| {
            let input_type = &types[0];
            with_match_primitive_type_id!(
                input_type.logical_type_id(),
                |$S| {
                    Ok(Box::new(ScipyStatsNormPdf::<$S>::default()))
                },
                {
                    let err_msg = format!(
                        "\"SCIPYSTATSNORMpdf\" aggregate function not support data type {:?}",
                        input_type.logical_type_id(),
                    );
                    CreateAccumulatorSnafu { err_msg }.fail()?
                }
            )
        });
        creator
    }

    fn output_type(&self) -> Result<ConcreteDataType> {
        let input_types = self.input_types()?;
        ensure!(input_types.len() == 2, InvalidInputStateSnafu);
        Ok(ConcreteDataType::float64_datatype())
    }

    fn state_types(&self) -> Result<Vec<ConcreteDataType>> {
        let input_types = self.input_types()?;
        ensure!(input_types.len() == 2, InvalidInputStateSnafu);
        Ok(vec![
            ConcreteDataType::list_datatype(input_types[0].clone()),
            ConcreteDataType::float64_datatype(),
        ])
    }
}

#[cfg(test)]
mod test {
    use datatypes::vectors::PrimitiveVector;

    use super::*;
    #[test]
    fn test_update_batch() {
        // test update empty batch, expect not updating anything
        let mut scipy_stats_norm_pdf = ScipyStatsNormPdf::<i32>::default();
        assert!(scipy_stats_norm_pdf.update_batch(&[]).is_ok());
        assert!(scipy_stats_norm_pdf.values.is_empty());
        assert_eq!(Value::Null, scipy_stats_norm_pdf.evaluate().unwrap());

        // test update no null-value batch
        let mut scipy_stats_norm_pdf = ScipyStatsNormPdf::<i32>::default();
        let v: Vec<VectorRef> = vec![
            Arc::new(PrimitiveVector::<i32>::from(vec![
                Some(-1i32),
                Some(1),
                Some(2),
            ])),
            Arc::new(PrimitiveVector::<f64>::from(vec![
                Some(2.0_f64),
                Some(2.0_f64),
                Some(2.0_f64),
            ])),
        ];
        assert!(scipy_stats_norm_pdf.update_batch(&v).is_ok());
        assert_eq!(
            Value::from(0.17843340219081558),
            scipy_stats_norm_pdf.evaluate().unwrap()
        );

        // test update null-value batch
        let mut scipy_stats_norm_pdf = ScipyStatsNormPdf::<i32>::default();
        let v: Vec<VectorRef> = vec![
            Arc::new(PrimitiveVector::<i32>::from(vec![
                Some(-2i32),
                None,
                Some(3),
                Some(4),
            ])),
            Arc::new(PrimitiveVector::<f64>::from(vec![
                Some(2.0_f64),
                None,
                Some(2.0_f64),
                Some(2.0_f64),
            ])),
        ];
        assert!(scipy_stats_norm_pdf.update_batch(&v).is_ok());
        assert_eq!(
            Value::from(0.12343972049858312),
            scipy_stats_norm_pdf.evaluate().unwrap()
        );
    }
}
