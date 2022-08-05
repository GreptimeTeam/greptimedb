mod argmax;
mod argmin;
mod diff;
mod mean;
mod median;
mod percentile;
mod polyval;
mod scipy_stats_norm_cdf;
mod scipy_stats_norm_pdf;
mod stddev;

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
pub use stddev::StddevAccumulatorCreator;

use crate::scalars::FunctionRegistry;

/// A function creates `AggregateFunctionCreator`.
/// "Aggregator" *is* AggregatorFunction. Since the later one is long, we named an short alias for it.
/// The two names might be used interchangeably.
type AggregatorCreatorFunction = Arc<dyn Fn() -> AggregateFunctionCreatorRef + Send + Sync>;

/// `AggregateFunctionMeta` dynamically creates AggregateFunctionCreator.
#[derive(Clone)]
pub struct AggregateFunctionMeta {
    name: String,
    creator: AggregatorCreatorFunction,
}

pub type AggregateFunctionMetaRef = Arc<AggregateFunctionMeta>;

impl AggregateFunctionMeta {
    pub fn new(name: &str, creator: AggregatorCreatorFunction) -> Self {
        Self {
            name: name.to_string(),
            creator,
        }
    }

    pub fn name(&self) -> String {
        self.name.to_string()
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
            Arc::new(|| Arc::new(MedianAccumulatorCreator::default())),
        )));
        registry.register_aggregate_function(Arc::new(AggregateFunctionMeta::new(
            "diff",
            Arc::new(|| Arc::new(DiffAccumulatorCreator::default())),
        )));
        registry.register_aggregate_function(Arc::new(AggregateFunctionMeta::new(
            "mean",
            Arc::new(|| Arc::new(MeanAccumulatorCreator::default())),
        )));
        registry.register_aggregate_function(Arc::new(AggregateFunctionMeta::new(
            "polyval",
            Arc::new(|| Arc::new(PolyvalAccumulatorCreator::default())),
        )));
        registry.register_aggregate_function(Arc::new(AggregateFunctionMeta::new(
            "argmax",
            Arc::new(|| Arc::new(ArgmaxAccumulatorCreator::default())),
        )));
        registry.register_aggregate_function(Arc::new(AggregateFunctionMeta::new(
            "argmin",
            Arc::new(|| Arc::new(ArgminAccumulatorCreator::default())),
        )));
        registry.register_aggregate_function(Arc::new(AggregateFunctionMeta::new(
            "percentile",
            Arc::new(|| Arc::new(PercentileAccumulatorCreator::default())),
        )));
        registry.register_aggregate_function(Arc::new(AggregateFunctionMeta::new(
            "scipy_stats_norm_cdf",
            Arc::new(|| Arc::new(ScipyStatsNormCdfAccumulatorCreator::default())),
        )));
        registry.register_aggregate_function(Arc::new(AggregateFunctionMeta::new(
            "scipy_stats_norm_pdf",
            Arc::new(|| Arc::new(ScipyStatsNormPdfAccumulatorCreator::default())),
        )));
        registry.register_aggregate_function(Arc::new(AggregateFunctionMeta::new(
            "stddev",
            Arc::new(|| Arc::new(StddevAccumulatorCreator::default())),
        )));
    }
}
