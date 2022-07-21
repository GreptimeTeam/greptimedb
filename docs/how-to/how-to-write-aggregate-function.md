Currently, our query engine is based on DataFusion, so all aggregate function is executed by DataFusion, through its UDAF interface. You can find DataFusion's UDAF example [here](https://github.com/apache/arrow-datafusion/blob/arrow2/datafusion-examples/examples/simple_udaf.rs). Basically, we provide the same way as DataFusion to write aggregate functions: both are centered in a struct called "Accumulator" to accumulates states along the way in aggregation.

However, DataFusion's UDAF implementation has a huge restriction, that it requires user to provide a concrete "Accumulator". Take `Median` aggregate function for example, to aggregate a `u32` datatype column, you have to write a `MedianU32`, and use `SELECT MEDIANU32(x)` in SQL. `MedianU32` cannot be used to aggregate a `i32` datatype column. Or, there's another way: you can use a special type that can hold all kinds of data (like our `Value` enum or Arrow's `ScalarValue`), and `match` all the way up to do aggregate calculations. It might work, though rather tedious. (But I think it's DataFusion's prefer way to write UDAF.)

So is there a way we can make an aggregate function that automatically match the input data's type? For example, a `Median` aggregator that can work on both `u32` column and `i32`? The answer is yes until we found a way to bypassing DataFusion's restriction, a restriction that DataFusion simply don't pass the input data's type when creating an Accumulator.

> There's an example in `my_sum_udaf_example.rs`, take that as quick start.

# 1. Impl `AggregateFunctionCreator` trait for your accumulator creator.

You must first define a struct that can store the input data's type. For example,

```Rust
struct MySumAccumulatorCreator {
    input_types: ArcSwapOption<Vec<ConcreteDataType>>,
}
```

Then impl `AggregateFunctionCreator` trait on it. The definition of the trait is:

```Rust
pub trait AggregateFunctionCreator: Send + Sync + Debug {
    fn creator(&self) -> AccumulatorCreatorFunction;
    fn input_types(&self) -> Vec<ConcreteDataType>;
    fn set_input_types(&self, input_types: Vec<ConcreteDataType>);
    fn output_type(&self) -> ConcreteDataType;
    fn state_types(&self) -> Vec<ConcreteDataType>;
}
```

our query engine will call `set_input_types` the very first, so you can use input data's type in methods that return output type and state types.

The output type is aggregate function's output data's type. For example, `SUM` aggregate function's output type is `u64` for a `u32` datatype column. The state types are accumulator's internal states' types. Take `AVG` aggregate function on a `i32` column as example, it's state types are `i64` (for sum) and `u64` (for count).

The `creator` function is where you define how an accumulator (that will be used in DataFusion) is created. You define "how" to create the accumulator (instead of "what" to create), using the input data's type as arguments. With input datatype known, you can create accumulator generically.

# 2. Impl `Accumulator` trait for you accumulator.

The accumulator is where you store the aggregate calculation states and evaluate a result. You must impl `Accumulator` trait for it. The trait's definition is:

```Rust
pub trait Accumulator: Send + Sync + Debug {
    fn state(&self) -> Result<Vec<Value>>;
    fn update_batch(&mut self, values: &[VectorRef]) -> Result<()>;
    fn merge_batch(&mut self, states: &[VectorRef]) -> Result<()>;
    fn evaluate(&self) -> Result<Value>;
}
```

The DataFusion basically execute aggregate like this:

1. Partitioning all input data for aggregate. Create an accumulator for each part.
2. Call `update_batch` on each accumulator with partitioned data, to let you update your aggregate calculation.
3. Call `state` to get each accumulator's internal state, the medial calculation result.
4. Call `merge_batch` to merge all accumulator's internal state to one.
5. Execute `evalute` on the chosen one to get the final calculation result.

Once you know the meaning of each method, you can easily write your accumulator. You can refer to `Median` accumulator or `SUM` accumulator defined in  file `my_sum_udaf_example.rs` for more details.

# 3. Register your aggregate function to our query engine.

You can call `register_aggregate_function` method in query engine to register your aggregate function. To do that, you have to new an instance of struct `AggregateFunctionMeta`. The struct has two fields, first is the name of your aggregate function's name. The function name is case-sensitive due to DataFusion's restriction. We strongly recommend using lowercase for your name. If you have to use uppercase name, wrap your aggregate function with quotation marks. For example, if you define an aggregate function named "my_aggr", you can use "`SELECT MY_AGGR(x)`"; if you define "my_AGGR", you have to use "`SELECT "my_AGGR"(x)`".

The second field is a function about how to create your accumulator creator that you defined in step 1 above. Create creator, that's a bit intertwined, but it is how we make DataFusion use a newly created aggregate function each time it executes a SQL, preventing the stored input types from affecting each other. The key detail can be starting looking at our `DfContextProviderAdapter` struct's `get_aggregate_meta` method.

# (Optional) 4. Make your aggregate function automatically registered.

If you've written a great aggregate function that want to let everyone use it, you can make it automatically registered to our query engine at start time. It's quick simple, just refer to the `AggregateFunctions::register` function in `common/function/src/scalars/aggregate/mod.rs`.
