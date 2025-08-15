// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Wrapper for making aggregate functions out of state/merge functions of original aggregate functions.
//!
//! i.e. for a aggregate function `foo`, we will have a state function `foo_state` and a merge function `foo_merge`.
//!
//! `foo_state`'s input args is the same as `foo`'s, and its output is a state object.
//! Note that `foo_state` might have multiple output columns, so it's a struct array
//! that each output column is a struct field.
//! `foo_merge`'s input arg is the same as `foo_state`'s output, and its output is the same as `foo`'s input.
//!

use std::sync::Arc;

use arrow::array::StructArray;
use arrow_schema::{FieldRef, Fields};
use common_telemetry::debug;
use datafusion::functions_aggregate::all_default_aggregate_functions;
use datafusion::optimizer::analyzer::type_coercion::TypeCoercion;
use datafusion::optimizer::AnalyzerRule;
use datafusion::physical_planner::create_aggregate_expr_and_maybe_filter;
use datafusion_common::{Column, ScalarValue};
use datafusion_expr::expr::{AggregateFunction, AggregateFunctionParams};
use datafusion_expr::function::StateFieldsArgs;
use datafusion_expr::{
    Accumulator, Aggregate, AggregateUDF, AggregateUDFImpl, Expr, ExprSchemable, LogicalPlan,
    Signature,
};
use datafusion_physical_expr::aggregate::AggregateFunctionExpr;
use datatypes::arrow::datatypes::{DataType, Field};

use crate::function_registry::FunctionRegistry;

/// Returns the name of the state function for the given aggregate function name.
/// The state function is used to compute the state of the aggregate function.
/// The state function's name is in the format `__<aggr_name>_state
pub fn aggr_state_func_name(aggr_name: &str) -> String {
    format!("__{}_state", aggr_name)
}

/// Returns the name of the merge function for the given aggregate function name.
/// The merge function is used to merge the states of the state functions.
/// The merge function's name is in the format `__<aggr_name>_merge
pub fn aggr_merge_func_name(aggr_name: &str) -> String {
    format!("__{}_merge", aggr_name)
}

/// A wrapper to make an aggregate function out of the state and merge functions of the original aggregate function.
/// It contains the original aggregate function, the state functions, and the merge function.
///
/// Notice state functions may have multiple output columns, so it's return type is always a struct array, and the merge function is used to merge the states of the state functions.
#[derive(Debug, Clone)]
pub struct StateMergeHelper;

/// A struct to hold the two aggregate plans, one for the state function(lower) and one for the merge function(upper).
#[allow(unused)]
#[derive(Debug, Clone)]
pub struct StepAggrPlan {
    /// Upper merge plan, which is the aggregate plan that merges the states of the state function.
    pub upper_merge: LogicalPlan,
    /// Lower state plan, which is the aggregate plan that computes the state of the aggregate function.
    pub lower_state: LogicalPlan,
}

pub fn get_aggr_func(expr: &Expr) -> Option<&datafusion_expr::expr::AggregateFunction> {
    let mut expr_ref = expr;
    while let Expr::Alias(alias) = expr_ref {
        expr_ref = &alias.expr;
    }
    if let Expr::AggregateFunction(aggr_func) = expr_ref {
        Some(aggr_func)
    } else {
        None
    }
}

impl StateMergeHelper {
    /// Register all the `state` function of supported aggregate functions.
    /// Note that can't register `merge` function here, as it needs to be created from the original aggregate function with given input types.
    pub fn register(registry: &FunctionRegistry) {
        let all_default = all_default_aggregate_functions();
        let greptime_custom_aggr_functions = registry.aggregate_functions();

        // if our custom aggregate function have the same name as the default aggregate function, we will override it.
        let supported = all_default
            .into_iter()
            .chain(greptime_custom_aggr_functions.into_iter().map(Arc::new))
            .collect::<Vec<_>>();
        debug!(
            "Registering state functions for supported: {:?}",
            supported.iter().map(|f| f.name()).collect::<Vec<_>>()
        );

        let state_func = supported.into_iter().filter_map(|f| {
            StateWrapper::new((*f).clone())
                .inspect_err(
                    |e| common_telemetry::error!(e; "Failed to register state function for {:?}", f),
                )
                .ok()
                .map(AggregateUDF::new_from_impl)
        });

        for func in state_func {
            registry.register_aggr(func);
        }
    }

    /// Split an aggregate plan into two aggregate plans, one for the state function and one for the merge function.
    pub fn split_aggr_node(aggr_plan: Aggregate) -> datafusion_common::Result<StepAggrPlan> {
        let aggr = {
            // certain aggr func need type coercion to work correctly, so we need to analyze the plan first.
            let aggr_plan = TypeCoercion::new().analyze(
                LogicalPlan::Aggregate(aggr_plan).clone(),
                &Default::default(),
            )?;
            if let LogicalPlan::Aggregate(aggr) = aggr_plan {
                aggr
            } else {
                return Err(datafusion_common::DataFusionError::Internal(format!(
                    "Failed to coerce expressions in aggregate plan, expected Aggregate, got: {:?}",
                    aggr_plan
                )));
            }
        };
        let mut lower_aggr_exprs = vec![];
        let mut upper_aggr_exprs = vec![];

        for aggr_expr in aggr.aggr_expr.iter() {
            let Some(aggr_func) = get_aggr_func(aggr_expr) else {
                return Err(datafusion_common::DataFusionError::NotImplemented(format!(
                    "Unsupported aggregate expression for step aggr optimize: {:?}",
                    aggr_expr
                )));
            };

            let original_input_types = aggr_func
                .params
                .args
                .iter()
                .map(|e| e.get_type(&aggr.input.schema()))
                .collect::<Result<Vec<_>, _>>()?;

            // first create the state function from the original aggregate function.
            let state_func = StateWrapper::new((*aggr_func.func).clone())?;

            let expr = AggregateFunction {
                func: Arc::new(state_func.into()),
                params: aggr_func.params.clone(),
            };
            let expr = Expr::AggregateFunction(expr);
            let lower_state_output_col_name = expr.schema_name().to_string();

            lower_aggr_exprs.push(expr);

            let (original_phy_expr, _filter, _ordering) = create_aggregate_expr_and_maybe_filter(
                aggr_expr,
                aggr.input.schema(),
                aggr.input.schema().as_arrow(),
                &Default::default(),
            )?;

            let merge_func = MergeWrapper::new(
                (*aggr_func.func).clone(),
                original_phy_expr,
                original_input_types,
            )?;
            let arg = Expr::Column(Column::new_unqualified(lower_state_output_col_name));
            let expr = AggregateFunction {
                func: Arc::new(merge_func.into()),
                params: AggregateFunctionParams {
                    args: vec![arg],
                    ..aggr_func.params.clone()
                },
            };

            // alias to the original aggregate expr's schema name, so parent plan can refer to it
            // correctly.
            let expr = Expr::AggregateFunction(expr).alias(aggr_expr.schema_name().to_string());
            upper_aggr_exprs.push(expr);
        }

        let mut lower = aggr.clone();
        lower.aggr_expr = lower_aggr_exprs;
        let lower_plan = LogicalPlan::Aggregate(lower);

        // update aggregate's output schema
        let lower_plan = lower_plan.recompute_schema()?;

        let mut upper = aggr.clone();
        let aggr_plan = LogicalPlan::Aggregate(aggr);
        upper.aggr_expr = upper_aggr_exprs;
        upper.input = Arc::new(lower_plan.clone());
        // upper schema's output schema should be the same as the original aggregate plan's output schema
        let upper_check = upper;
        let upper_plan = LogicalPlan::Aggregate(upper_check).recompute_schema()?;
        if *upper_plan.schema() != *aggr_plan.schema() {
            return Err(datafusion_common::DataFusionError::Internal(format!(
                 "Upper aggregate plan's schema is not the same as the original aggregate plan's schema: \n[transformed]:{}\n[original]:{}",
                upper_plan.schema(), aggr_plan.schema()
            )));
        }

        Ok(StepAggrPlan {
            lower_state: lower_plan,
            upper_merge: upper_plan,
        })
    }
}

/// Wrapper to make an aggregate function out of a state function.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateWrapper {
    inner: AggregateUDF,
    name: String,
}

impl StateWrapper {
    /// `state_index`: The index of the state in the output of the state function.
    pub fn new(inner: AggregateUDF) -> datafusion_common::Result<Self> {
        let name = aggr_state_func_name(inner.name());
        Ok(Self { inner, name })
    }

    pub fn inner(&self) -> &AggregateUDF {
        &self.inner
    }

    /// Deduce the return type of the original aggregate function
    /// based on the accumulator arguments.
    ///
    pub fn deduce_aggr_return_type(
        &self,
        acc_args: &datafusion_expr::function::AccumulatorArgs,
    ) -> datafusion_common::Result<FieldRef> {
        self.inner.return_field(acc_args.schema.fields())
    }
}

impl AggregateUDFImpl for StateWrapper {
    fn accumulator<'a, 'b>(
        &'a self,
        acc_args: datafusion_expr::function::AccumulatorArgs<'b>,
    ) -> datafusion_common::Result<Box<dyn Accumulator>> {
        // fix and recover proper acc args for the original aggregate function.
        let state_type = acc_args.return_type().clone();
        let inner = {
            let acc_args = datafusion_expr::function::AccumulatorArgs {
                return_field: self.deduce_aggr_return_type(&acc_args)?,
                schema: acc_args.schema,
                ignore_nulls: acc_args.ignore_nulls,
                order_bys: acc_args.order_bys,
                is_reversed: acc_args.is_reversed,
                name: acc_args.name,
                is_distinct: acc_args.is_distinct,
                exprs: acc_args.exprs,
            };
            self.inner.accumulator(acc_args)?
        };
        Ok(Box::new(StateAccum::new(inner, state_type)?))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn is_nullable(&self) -> bool {
        self.inner.is_nullable()
    }

    /// Return state_fields as the output struct type.
    ///
    fn return_type(&self, arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        let input_fields = &arg_types
            .iter()
            .map(|x| Arc::new(Field::new("x", x.clone(), false)))
            .collect::<Vec<_>>();

        let state_fields_args = StateFieldsArgs {
            name: self.inner().name(),
            input_fields,
            return_field: self.inner.return_field(input_fields)?,
            // TODO(discord9): how to get this?, probably ok?
            ordering_fields: &[],
            is_distinct: false,
        };
        let state_fields = self.inner.state_fields(state_fields_args)?;
        let struct_field = DataType::Struct(state_fields.into());
        Ok(struct_field)
    }

    /// The state function's output fields are the same as the original aggregate function's state fields.
    fn state_fields(
        &self,
        args: datafusion_expr::function::StateFieldsArgs,
    ) -> datafusion_common::Result<Vec<FieldRef>> {
        let state_fields_args = StateFieldsArgs {
            name: args.name,
            input_fields: args.input_fields,
            return_field: self.inner.return_field(args.input_fields)?,
            ordering_fields: args.ordering_fields,
            is_distinct: args.is_distinct,
        };
        self.inner.state_fields(state_fields_args)
    }

    /// The state function's signature is the same as the original aggregate function's signature,
    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    /// Coerce types also do nothing, as optimzer should be able to already make struct types
    fn coerce_types(&self, arg_types: &[DataType]) -> datafusion_common::Result<Vec<DataType>> {
        self.inner.coerce_types(arg_types)
    }
}

/// The wrapper's input is the same as the original aggregate function's input,
/// and the output is the state function's output.
#[derive(Debug)]
pub struct StateAccum {
    inner: Box<dyn Accumulator>,
    state_fields: Fields,
}

impl StateAccum {
    pub fn new(
        inner: Box<dyn Accumulator>,
        state_type: DataType,
    ) -> datafusion_common::Result<Self> {
        let DataType::Struct(fields) = state_type else {
            return Err(datafusion_common::DataFusionError::Internal(format!(
                "Expected a struct type for state, got: {:?}",
                state_type
            )));
        };
        Ok(Self {
            inner,
            state_fields: fields,
        })
    }
}

impl Accumulator for StateAccum {
    fn evaluate(&mut self) -> datafusion_common::Result<ScalarValue> {
        let state = self.inner.state()?;

        let array = state
            .iter()
            .map(|s| s.to_array())
            .collect::<Result<Vec<_>, _>>()?;
        let struct_array = StructArray::try_new(self.state_fields.clone(), array, None)?;
        Ok(ScalarValue::Struct(Arc::new(struct_array)))
    }

    fn merge_batch(
        &mut self,
        states: &[datatypes::arrow::array::ArrayRef],
    ) -> datafusion_common::Result<()> {
        self.inner.merge_batch(states)
    }

    fn update_batch(
        &mut self,
        values: &[datatypes::arrow::array::ArrayRef],
    ) -> datafusion_common::Result<()> {
        self.inner.update_batch(values)
    }

    fn size(&self) -> usize {
        self.inner.size()
    }

    fn state(&mut self) -> datafusion_common::Result<Vec<ScalarValue>> {
        self.inner.state()
    }
}

/// TODO(discord9): mark this function as non-ser/de able
///
/// This wrapper shouldn't be register as a udaf, as it contain extra data that is not serializable.
/// and changes for different logical plans.
#[derive(Debug, Clone)]
pub struct MergeWrapper {
    inner: AggregateUDF,
    name: String,
    merge_signature: Signature,
    /// The original physical expression of the aggregate function, can't store the original aggregate function directly, as PhysicalExpr didn't implement Any
    original_phy_expr: Arc<AggregateFunctionExpr>,
    original_input_types: Vec<DataType>,
}
impl MergeWrapper {
    pub fn new(
        inner: AggregateUDF,
        original_phy_expr: Arc<AggregateFunctionExpr>,
        original_input_types: Vec<DataType>,
    ) -> datafusion_common::Result<Self> {
        let name = aggr_merge_func_name(inner.name());
        // the input type is actually struct type, which is the state fields of the original aggregate function.
        let merge_signature = Signature::user_defined(datafusion_expr::Volatility::Immutable);

        Ok(Self {
            inner,
            name,
            merge_signature,
            original_phy_expr,
            original_input_types,
        })
    }

    pub fn inner(&self) -> &AggregateUDF {
        &self.inner
    }
}

impl AggregateUDFImpl for MergeWrapper {
    fn accumulator<'a, 'b>(
        &'a self,
        acc_args: datafusion_expr::function::AccumulatorArgs<'b>,
    ) -> datafusion_common::Result<Box<dyn Accumulator>> {
        if acc_args.exprs.len() != 1
            || !matches!(
                acc_args.exprs[0].data_type(acc_args.schema)?,
                DataType::Struct(_)
            )
        {
            return Err(datafusion_common::DataFusionError::Internal(format!(
                "Expected one struct type as input, got: {:?}",
                acc_args.schema
            )));
        }
        let input_type = acc_args.exprs[0].data_type(acc_args.schema)?;
        let DataType::Struct(fields) = input_type else {
            return Err(datafusion_common::DataFusionError::Internal(format!(
                "Expected a struct type for input, got: {:?}",
                input_type
            )));
        };

        let inner_accum = self.original_phy_expr.create_accumulator()?;
        Ok(Box::new(MergeAccum::new(inner_accum, &fields)))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn is_nullable(&self) -> bool {
        self.inner.is_nullable()
    }

    /// Notice here the `arg_types` is actually the `state_fields`'s data types,
    /// so return fixed return type instead of using `arg_types` to determine the return type.
    fn return_type(&self, _arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        // The return type is the same as the original aggregate function's return type.
        let ret_type = self.inner.return_type(&self.original_input_types)?;
        Ok(ret_type)
    }
    fn signature(&self) -> &Signature {
        &self.merge_signature
    }

    /// Coerce types also do nothing, as optimzer should be able to already make struct types
    fn coerce_types(&self, arg_types: &[DataType]) -> datafusion_common::Result<Vec<DataType>> {
        // just check if the arg_types are only one and is struct array
        if arg_types.len() != 1 || !matches!(arg_types.first(), Some(DataType::Struct(_))) {
            return Err(datafusion_common::DataFusionError::Internal(format!(
                "Expected one struct type as input, got: {:?}",
                arg_types
            )));
        }
        Ok(arg_types.to_vec())
    }

    /// Just return the original aggregate function's state fields.
    fn state_fields(
        &self,
        _args: datafusion_expr::function::StateFieldsArgs,
    ) -> datafusion_common::Result<Vec<FieldRef>> {
        self.original_phy_expr.state_fields()
    }
}

/// The merge accumulator, which modify `update_batch`'s behavior to accept one struct array which
/// include the state fields of original aggregate function, and merge said states into original accumulator
/// the output is the same as original aggregate function
#[derive(Debug)]
pub struct MergeAccum {
    inner: Box<dyn Accumulator>,
    state_fields: Fields,
}

impl MergeAccum {
    pub fn new(inner: Box<dyn Accumulator>, state_fields: &Fields) -> Self {
        Self {
            inner,
            state_fields: state_fields.clone(),
        }
    }
}

impl Accumulator for MergeAccum {
    fn evaluate(&mut self) -> datafusion_common::Result<ScalarValue> {
        self.inner.evaluate()
    }

    fn merge_batch(&mut self, states: &[arrow::array::ArrayRef]) -> datafusion_common::Result<()> {
        self.inner.merge_batch(states)
    }

    fn update_batch(&mut self, values: &[arrow::array::ArrayRef]) -> datafusion_common::Result<()> {
        let value = values.first().ok_or_else(|| {
            datafusion_common::DataFusionError::Internal("No values provided for merge".to_string())
        })?;
        // The input values are states from other accumulators, so we merge them.
        let struct_arr = value
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(format!(
                    "Expected StructArray, got: {:?}",
                    value.data_type()
                ))
            })?;
        let fields = struct_arr.fields();
        if fields != &self.state_fields {
            return Err(datafusion_common::DataFusionError::Internal(format!(
                "Expected state fields: {:?}, got: {:?}",
                self.state_fields, fields
            )));
        }

        // now fields should be the same, so we can merge the batch
        // by pass the columns as order should be the same
        let state_columns = struct_arr.columns();
        self.inner.merge_batch(state_columns)
    }

    fn size(&self) -> usize {
        self.inner.size()
    }

    fn state(&mut self) -> datafusion_common::Result<Vec<ScalarValue>> {
        self.inner.state()
    }
}

#[cfg(test)]
mod tests;
