mod mean;
mod median;

use std::sync::Arc;

use common_query::logical_plan::AggregateFunctionCreatorRef;
pub use mean::MeanAccumulatorCreator;
pub use median::MedianAccumulatorCreator;

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
            "mean",
            Arc::new(|| Arc::new(MeanAccumulatorCreator::default())),
        )));
    }
}
