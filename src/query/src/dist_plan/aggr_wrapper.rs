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
//! `foo_state` i's input args is the same as `foo`'s, and its output is a state object.
//! Note that `foo_state` might have multiple output columns(might need special handling in the future).
//! `foo_merge`'s input args is the same as `foo_state`'s, and its output is the same as `foo`'s.
//!

use std::sync::Arc;

use arrow_schema::Fields;
use datafusion_common::{DFSchema, ScalarValue};
use datafusion_expr::{Accumulator, AggregateUDF, AggregateUDFImpl};
use datatypes::arrow::datatypes::{DataType, Field};
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};

/// Wrappr to make an aggregate function out of a state function.
#[derive(Debug)]
pub struct AggregateStateFunctionWrapper {
    inner: AggregateUDF,
    name: String,
}

impl AggregateStateFunctionWrapper {
    pub fn new(inner: AggregateUDF) -> Self {
        let name = format!("__{}_state_udaf", inner.name());
        Self { inner, name }
    }

    pub fn inner(&self) -> &AggregateUDF {
        &self.inner
    }
}

impl AggregateUDFImpl for AggregateStateFunctionWrapper {
    fn accumulator(
        &self,
        acc_args: datafusion_expr::function::AccumulatorArgs,
    ) -> datafusion_common::Result<Box<dyn Accumulator>> {
        let inner = self.inner.accumulator(acc_args)?;
        Ok(Box::new(AggregateStateAccumulatorWrapper::new(inner)))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self.inner.inner().as_any()
    }
    fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Return state as a binary, in case the state function has multiple output columns,
    /// we will serialize the state into a binary format.
    fn return_type(&self, arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Binary)
    }
    fn signature(&self) -> &datafusion_expr::Signature {
        self.inner.signature()
    }

    fn state_fields(
        &self,
        args: datafusion_expr::function::StateFieldsArgs,
    ) -> datafusion_common::Result<Vec<Field>> {
        self.inner.state_fields(args)
    }
}

/// The wrapper's input is the same as the original aggregate function's input,
/// and the output is the state function's output.
#[derive(Debug)]
pub struct AggregateStateAccumulatorWrapper {
    inner: Box<dyn Accumulator>,
}

impl AggregateStateAccumulatorWrapper {
    pub fn new(inner: Box<dyn Accumulator>) -> Self {
        Self { inner }
    }
}

impl Accumulator for AggregateStateAccumulatorWrapper {
    fn evaluate(&mut self) -> datafusion_common::Result<ScalarValue> {
        let state = self.inner.state()?;

        // encode states into LogicalPlan::Values then into substrait plan.
        // This is a workaround to serialize the state into a binary format.

        // it's either this or have to recompute the state multiple times for aggr functions
        // that have multiple output columns(i.e. `avg`).
        let tys = state
            .iter()
            .enumerate()
            .map(|(idx, s)| Field::new(format!("{idx}"), s.data_type(), true))
            .collect::<Vec<_>>();
        let fields = Fields::from(tys);
        let df_schema = DFSchema::from_unqualified_fields(fields, Default::default())?;
        let exprs = state
            .into_iter()
            .map(|s| datafusion_expr::Expr::Literal(s))
            .collect::<Vec<_>>();
        let values = datafusion_expr::Values {
            schema: Arc::new(df_schema),
            values: vec![exprs],
        };
        let plan = datafusion_expr::LogicalPlan::Values(values);
        let substrait_plan = DFLogicalSubstraitConvertor {}
            .encode(&plan, DefaultSerializer)
            .map_err(|e| datafusion_common::DataFusionError::Execution(e.to_string()))?;
        // TODO: serialize the state into a binary format(maybe protobuf or something else).
        let bytes = substrait_plan.to_vec();
        Ok(ScalarValue::Binary(Some(bytes)))
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
