use std::sync::Arc;

use arc_swap::ArcSwapOption;
use common_query::error::{
    CreateAccumulatorSnafu, DowncastVectorSnafu, ExecuteFunctionSnafu, FromScalarValueSnafu,
    GenerateFunctionSnafu, Result,
};
use common_query::logical_plan::{Accumulator, AggregateFunctionCreator};
use common_query::prelude::*;
use datafusion_common::DataFusionError;
use datatypes::prelude::*;
use datatypes::value::ListValue;
use datatypes::vectors::{ConstantVector, ListVector};
use datatypes::with_match_ordered_primitive_type_id;
use num_traits::AsPrimitive;
use snafu::{OptionExt, ResultExt};
use statrs::distribution::{Continuous, Normal};

// https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.norm.html
#[derive(Debug, Default)]
pub struct ScipyStatsNormPdf<T>
where
    T: Primitive + AsPrimitive<f64> + std::iter::Sum<T>,
{
    values: Vec<T>,
    x: f64,
}

impl<T> ScipyStatsNormPdf<T>
where
    T: Primitive + AsPrimitive<f64> + std::iter::Sum<T>,
{
    fn push(&mut self, value: T) {
        self.values.push(value);
    }

    fn mean(&self) -> Option<f64> {
        let sum: T = self.values.clone().into_iter().sum();
        let count = self.values.len();

        match count {
            positive if positive > 0 => Some(sum.as_() / count as f64),
            _ => None,
        }
    }

    fn std_deviation(&self) -> Option<f64> {
        match (self.mean(), self.values.len()) {
            (Some(value_mean), count) if count > 0 => {
                let variance = self
                    .values
                    .iter()
                    .map(|value| {
                        let diff = value_mean - (*value).as_();
                        diff * diff
                    })
                    .sum::<f64>()
                    / count as f64;

                Some(variance.sqrt())
            }
            _ => None,
        }
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
        Ok(vec![Value::List(ListValue::new(
            Some(Box::new(nums)),
            T::default().into().data_type(),
        ))])
    }

    fn update_batch(&mut self, values: &[VectorRef]) -> Result<()> {
        if let Some(column) = values.get(0) {
            let column: &<T as Scalar>::VectorType = if column.is_const() {
                let column: &ConstantVector = unsafe { VectorHelper::static_cast(column) };
                unsafe { VectorHelper::static_cast(column.inner()) }
            } else {
                unsafe { VectorHelper::static_cast(column) }
            };
            for v in column.iter_data().flatten() {
                self.push(v);
            }
        };
        Ok(())
    }

    fn merge_batch(&mut self, states: &[VectorRef]) -> Result<()> {
        if let Some(states) = states.get(0) {
            let states = states
                .as_any()
                .downcast_ref::<ListVector>()
                .with_context(|| DowncastVectorSnafu {
                    err_msg: format!(
                        "expect ListVector, got vector type {}",
                        states.vector_type_name()
                    ),
                })?;
            for state in states.values_iter() {
                let state = state.context(FromScalarValueSnafu)?;
                self.update_batch(&[state])?
            }
        }
        Ok(())
    }

    fn evaluate(&self) -> Result<Value> {
        if let (Some(mean), Some(std_dev)) = (self.mean(), self.std_deviation()) {
            let n = Normal::new(mean, std_dev).context(GenerateFunctionSnafu)?;
            Ok(n.pdf(self.x).into())
        } else {
            Ok(Value::Null)
        }
    }
}

#[derive(Debug, Default)]
pub struct ScipyStatsNormPdfAccumulatorCreator {
    input_types: ArcSwapOption<Vec<ConcreteDataType>>,
}

impl AggregateFunctionCreator for ScipyStatsNormPdfAccumulatorCreator {
    fn creator(&self) -> AccumulatorCreatorFunction {
        let creator: AccumulatorCreatorFunction = Arc::new(move |types: &[ConcreteDataType]| {
            let input_type = &types[0];
            with_match_ordered_primitive_type_id!(
                input_type.logical_type_id(),
                |$S| {
                    Ok(Box::new(ScipyStatsNormPdf::<$S>::default()))
                },
                {
                    let err_msg = format!(
                        "\"SCIPYSTATSNORMPDF\" aggregate function not support data type {:?}",
                        input_type.logical_type_id(),
                    );
                    CreateAccumulatorSnafu { err_msg }.fail()?
                }
            )
        });
        creator
    }

    fn input_types(&self) -> Result<Vec<ConcreteDataType>> {
        let input_types = self.input_types.load();
        if input_types.is_none() {
            return Err(datafusion_internal_error()).context(ExecuteFunctionSnafu)?;
        }
        Ok(input_types.as_ref().unwrap().as_ref().clone())
    }

    fn set_input_types(&self, input_types: Vec<ConcreteDataType>) -> Result<()> {
        let old = self.input_types.swap(Some(Arc::new(input_types.clone())));
        if let Some(old) = old {
            if old.len() != input_types.len() {
                return Err(datafusion_internal_error()).context(ExecuteFunctionSnafu)?;
            }
            for (x, y) in old.iter().zip(input_types.iter()) {
                if x != y {
                    return Err(datafusion_internal_error()).context(ExecuteFunctionSnafu)?;
                }
            }
        }
        Ok(())
    }

    fn output_type(&self) -> Result<ConcreteDataType> {
        let input_types = self.input_types()?;
        if input_types.len() != 1 {
            return Err(datafusion_internal_error()).context(ExecuteFunctionSnafu)?;
        }
        Ok(ConcreteDataType::float64_datatype())
    }

    fn state_types(&self) -> Result<Vec<ConcreteDataType>> {
        Ok(vec![ConcreteDataType::list_datatype(self.output_type()?)])
    }
}

fn datafusion_internal_error() -> DataFusionError {
    DataFusionError::Internal(
        "Illegal input_types status, check if DataFusion has changed its UDAF execution logic."
            .to_string(),
    )
}

#[cfg(test)]
mod test {
    use datatypes::vectors::PrimitiveVector;
    use statrs::statistics::Distribution;

    use super::*;
    #[test]
    fn test_update_batch() {
        // test update empty batch, expect not updating anything
        let mut scipy_stats_norm_pdf = ScipyStatsNormPdf::<i32> {
            x: 0.0,
            ..Default::default()
        };
        assert!(scipy_stats_norm_pdf.update_batch(&[]).is_ok());
        assert!(scipy_stats_norm_pdf.values.is_empty());
        assert_eq!(Value::Null, scipy_stats_norm_pdf.evaluate().unwrap());

        // test update no null-value batch
        let mut scipy_stats_norm_pdf = ScipyStatsNormPdf::<i32> {
            x: 0.0,
            ..Default::default()
        };
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![
            Some(-1i32),
            Some(1),
            Some(0),
        ]))];
        assert!(scipy_stats_norm_pdf.update_batch(&v).is_ok());
        let n = Normal::new(0.0, 0.816496580927726).unwrap();
        assert_eq!(n.mean(), scipy_stats_norm_pdf.mean());
        assert_eq!(n.std_dev(), scipy_stats_norm_pdf.std_deviation());
        assert_eq!(
            Value::from(n.pdf(0.0)),
            scipy_stats_norm_pdf.evaluate().unwrap()
        );

        // test update null-value batch
        let mut scipy_stats_norm_pdf = ScipyStatsNormPdf::<i32> {
            x: 1.0,
            ..Default::default()
        };
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![
            Some(-2i32),
            None,
            Some(2),
            Some(0),
        ]))];
        assert!(scipy_stats_norm_pdf.update_batch(&v).is_ok());
        let n = Normal::new(0.0, 1.632993161855452).unwrap();
        assert_eq!(scipy_stats_norm_pdf.mean(), n.mean());
        assert_eq!(scipy_stats_norm_pdf.std_deviation(), n.std_dev());
        assert_eq!(
            Value::from(n.pdf(1.0)),
            scipy_stats_norm_pdf.evaluate().unwrap()
        );
    }
}
