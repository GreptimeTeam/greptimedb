registry.register_aggregate_function(Arc::new(AggregateFunctionMeta::new(
    "median",
    Arc::new(|| Arc::new(MedianAccumulatorCreator::default())),
)));