mod median;

use std::sync::Arc;

use common_query::logical_plan::AggregateFunctionCreatorRef;
pub use median::MedianAccumulatorCreator;

use crate::scalars::FunctionRegistry;

/// `AggregateFunctionMeta` dynamically creates AggregateFunctionCreator.
#[derive(Clone)]
pub struct AggregateFunctionMeta {
    name: String,
    creator: Arc<dyn Fn() -> AggregateFunctionCreatorRef + Send + Sync>,
}

pub type AggregateFunctionMetaRef = Arc<AggregateFunctionMeta>;

impl AggregateFunctionMeta {
    pub fn new(
        name: &str,
        creator: Arc<dyn Fn() -> AggregateFunctionCreatorRef + Send + Sync>,
    ) -> Self {
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
    }
}
