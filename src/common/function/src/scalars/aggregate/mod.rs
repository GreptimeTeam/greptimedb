mod argmax;
mod argmin;
mod diff;
mod mean;
mod median;
mod percentile;
mod polyval;
mod scipy_stats_norm_cdf;
mod scipy_stats_norm_pdf;

use std::sync::Arc;

pub use argmax::ArgmaxAccumulatorCreator;
pub use argmin::ArgminAccumulatorCreator;
use common_query::logical_plan::AggregateFunctionCreatorRef;
pub use diff::DiffAccumulatorCreator;
pub use mean::MeanAccumulatorCreator;
pub use median::MedianAccumulatorCreator;
pub use percentile::PercentileAccumulatorCreator;
pub use polyval::PolyvalAccumulatorCreator;
pub use scipy_stats_norm_cdf::ScipyStatsNormCdfAccumulatorCreator;
pub use scipy_stats_norm_pdf::ScipyStatsNormPdfAccumulatorCreator;

use crate::scalars::FunctionRegistry;

/// A function creates `AggregateFunctionCreator`.
/// "Aggregator" *is* AggregatorFunction. Since the later one is long, we named an short alias for it.
/// The two names might be used interchangeably.
type AggregatorCreatorFunction = Arc<dyn Fn() -> AggregateFunctionCreatorRef + Send + Sync>;

/// `AggregateFunctionMeta` dynamically creates AggregateFunctionCreator.
#[derive(Clone)]
pub struct AggregateFunctionMeta {
    name: String,
    args_count: u8,
    creator: AggregatorCreatorFunction,
}

pub type AggregateFunctionMetaRef = Arc<AggregateFunctionMeta>;

impl AggregateFunctionMeta {
    pub fn new(name: &str, args_count: u8, creator: AggregatorCreatorFunction) -> Self {
        Self {
            name: name.to_string(),
            args_count,
            creator,
        }
    }

    pub fn name(&self) -> String {
        self.name.to_string()
    }

    pub fn args_count(&self) -> u8 {
        self.args_count
    }

    pub fn create(&self) -> AggregateFunctionCreatorRef {
        (self.creator)()
    }
}

pub(crate) struct AggregateFunctions;

impl AggregateFunctions {
    pub fn register(registry: &FunctionRegistry) {
        registry.register_aggregate_function(Arc::new(AggregateFunctionMeta::new(
            "median",
            1,
            Arc::new(|| Arc::new(MedianAccumulatorCreator::default())),
        )));
        registry.register_aggregate_function(Arc::new(AggregateFunctionMeta::new(
            "diff",
            1,
            Arc::new(|| Arc::new(DiffAccumulatorCreator::default())),
        )));
        registry.register_aggregate_function(Arc::new(AggregateFunctionMeta::new(
            "mean",
            1,
            Arc::new(|| Arc::new(MeanAccumulatorCreator::default())),
        )));
        registry.register_aggregate_function(Arc::new(AggregateFunctionMeta::new(
            "polyval",
            2,
            Arc::new(|| Arc::new(PolyvalAccumulatorCreator::default())),
        )));
        registry.register_aggregate_function(Arc::new(AggregateFunctionMeta::new(
            "argmax",
            1,
            Arc::new(|| Arc::new(ArgmaxAccumulatorCreator::default())),
        )));
        registry.register_aggregate_function(Arc::new(AggregateFunctionMeta::new(
            "argmin",
            1,
            Arc::new(|| Arc::new(ArgminAccumulatorCreator::default())),
        )));
        registry.register_aggregate_function(Arc::new(AggregateFunctionMeta::new(
            "diff",
            1,
            Arc::new(|| Arc::new(DiffAccumulatorCreator::default())),
        )));
        registry.register_aggregate_function(Arc::new(AggregateFunctionMeta::new(
            "percentile",
            2,
            Arc::new(|| Arc::new(PercentileAccumulatorCreator::default())),
        )));
        registry.register_aggregate_function(Arc::new(AggregateFunctionMeta::new(
            "scipystatsnormcdf",
            2,
            Arc::new(|| Arc::new(ScipyStatsNormCdfAccumulatorCreator::default())),
        )));
        registry.register_aggregate_function(Arc::new(AggregateFunctionMeta::new(
            "scipystatsnormpdf",
            2,
            Arc::new(|| Arc::new(ScipyStatsNormPdfAccumulatorCreator::default())),
        )));
    }
}
