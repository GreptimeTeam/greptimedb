mod macros;
mod median;
mod scipy_stats_norm_cdf;
mod scipy_stats_norm_pdf;

use std::sync::Arc;

use common_query::logical_plan::AggregateFunctionCreatorRef;
pub use median::MedianAccumulatorCreator;
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
            "scipy_stats_norm_cdf",
            Arc::new(|| Arc::new(ScipyStatsNormCdfAccumulatorCreator::default())),
        )));
        registry.register_aggregate_function(Arc::new(AggregateFunctionMeta::new(
            "scipy_stats_norm_pdf",
            Arc::new(|| Arc::new(ScipyStatsNormPdfAccumulatorCreator::default())),
        )));
    }
}
