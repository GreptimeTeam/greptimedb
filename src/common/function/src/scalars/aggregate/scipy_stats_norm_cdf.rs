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
use paste::paste;
use snafu::{OptionExt, ResultExt};
use statrs::distribution::{ContinuousCDF, Normal};

use crate::scipy_stats_norm_codec;

// https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.norm.html
scipy_stats_norm_codec!(cdf, ScipyStatsNormCdf);

#[cfg(test)]
mod test {
    use datatypes::vectors::PrimitiveVector;
    use statrs::statistics::Distribution;

    use super::*;
    #[test]
    fn test_update_batch() {
        // test update empty batch, expect not updating anything
        let mut scipy_stats_norm_cdf = ScipyStatsNormCdf::<i32> {
            x: 0.0,
            ..Default::default()
        };
        assert!(scipy_stats_norm_cdf.update_batch(&[]).is_ok());
        assert!(scipy_stats_norm_cdf.values.is_empty());
        assert_eq!(Value::Null, scipy_stats_norm_cdf.evaluate().unwrap());

        // test update no null-value batch
        let mut scipy_stats_norm_cdf = ScipyStatsNormCdf::<i32> {
            x: 0.0,
            ..Default::default()
        };
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![
            Some(-1i32),
            Some(1),
            Some(0),
        ]))];
        assert!(scipy_stats_norm_cdf.update_batch(&v).is_ok());
        let n = Normal::new(0.0, 0.816496580927726).unwrap();
        assert_eq!(n.mean(), scipy_stats_norm_cdf.mean());
        assert_eq!(n.std_dev(), scipy_stats_norm_cdf.std_deviation());
        assert_eq!(
            Value::from(n.cdf(0.0)),
            scipy_stats_norm_cdf.evaluate().unwrap()
        );

        // test update null-value batch
        let mut scipy_stats_norm_cdf = ScipyStatsNormCdf::<i32> {
            x: 1.0,
            ..Default::default()
        };
        let v: Vec<VectorRef> = vec![Arc::new(PrimitiveVector::<i32>::from(vec![
            Some(-2i32),
            None,
            Some(2),
            Some(0),
        ]))];
        assert!(scipy_stats_norm_cdf.update_batch(&v).is_ok());
        let n = Normal::new(0.0, 1.632993161855452).unwrap();
        assert_eq!(scipy_stats_norm_cdf.mean(), n.mean());
        assert_eq!(scipy_stats_norm_cdf.std_deviation(), n.std_dev());
        assert_eq!(
            Value::from(n.cdf(1.0)),
            scipy_stats_norm_cdf.evaluate().unwrap()
        );
    }
}
