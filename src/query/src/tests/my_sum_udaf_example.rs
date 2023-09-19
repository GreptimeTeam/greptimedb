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

use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use common_function::scalars::aggregate::AggregateFunctionMeta;
use common_macro::{as_aggr_func_creator, AggrFuncTypeStore};
use common_query::error::{CreateAccumulatorSnafu, Result as QueryResult};
use common_query::logical_plan::{Accumulator, AggregateFunctionCreator};
use common_query::prelude::*;
use common_recordbatch::{RecordBatch, RecordBatches};
use datatypes::prelude::*;
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::types::{LogicalPrimitiveType, WrapperType};
use datatypes::vectors::Helper;
use datatypes::with_match_primitive_type_id;
use num_traits::AsPrimitive;
use table::test_util::MemTable;

use crate::error::Result;
use crate::tests::{exec_selection, new_query_engine_with_table};

#[derive(Debug, Default)]
struct MySumAccumulator<T, SumT> {
    sum: SumT,
    _phantom: PhantomData<T>,
}

impl<T, SumT> MySumAccumulator<T, SumT>
where
    T: WrapperType,
    SumT: WrapperType,
    T::Native: AsPrimitive<SumT::Native>,
    SumT::Native: std::ops::AddAssign,
{
    #[inline(always)]
    fn add(&mut self, v: T) {
        let mut sum_native = self.sum.into_native();
        sum_native += v.into_native().as_();
        self.sum = SumT::from_native(sum_native);
    }

    #[inline(always)]
    fn merge(&mut self, s: SumT) {
        let mut sum_native = self.sum.into_native();
        sum_native += s.into_native();
        self.sum = SumT::from_native(sum_native);
    }
}

#[as_aggr_func_creator]
#[derive(Debug, Default, AggrFuncTypeStore)]
struct MySumAccumulatorCreator {}

impl AggregateFunctionCreator for MySumAccumulatorCreator {
    fn creator(&self) -> AccumulatorCreatorFunction {
        let creator: AccumulatorCreatorFunction = Arc::new(move |types: &[ConcreteDataType]| {
            let input_type = &types[0];
            with_match_primitive_type_id!(
                input_type.logical_type_id(),
                |$S| {
                    Ok(Box::new(MySumAccumulator::<<$S as LogicalPrimitiveType>::Wrapper, <<$S as LogicalPrimitiveType>::LargestType as LogicalPrimitiveType>::Wrapper>::default()))
                },
                {
                    let err_msg = format!(
                        "\"MY_SUM\" aggregate function not support data type {:?}",
                        input_type.logical_type_id(),
                    );
                    CreateAccumulatorSnafu { err_msg }.fail()?
                }
            )
        });
        creator
    }

    fn output_type(&self) -> QueryResult<ConcreteDataType> {
        let input_type = &self.input_types()?[0];
        with_match_primitive_type_id!(
            input_type.logical_type_id(),
            |$S| {
                Ok(<<$S as LogicalPrimitiveType>::LargestType>::build_data_type())
            },
            {
                unreachable!()
            }
        )
    }

    fn state_types(&self) -> QueryResult<Vec<ConcreteDataType>> {
        Ok(vec![self.output_type()?])
    }
}

impl<T, SumT> Accumulator for MySumAccumulator<T, SumT>
where
    T: WrapperType,
    SumT: WrapperType,
    T::Native: AsPrimitive<SumT::Native>,
    SumT::Native: std::ops::AddAssign,
{
    fn state(&self) -> QueryResult<Vec<Value>> {
        Ok(vec![self.sum.into()])
    }

    fn update_batch(&mut self, values: &[VectorRef]) -> QueryResult<()> {
        if values.is_empty() {
            return Ok(());
        };
        let column = &values[0];
        let column: &<T as Scalar>::VectorType = unsafe { Helper::static_cast(column) };
        for v in column.iter_data().flatten() {
            self.add(v)
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[VectorRef]) -> QueryResult<()> {
        if states.is_empty() {
            return Ok(());
        };
        let states = &states[0];
        let states: &<SumT as Scalar>::VectorType = unsafe { Helper::static_cast(states) };
        for s in states.iter_data().flatten() {
            self.merge(s)
        }
        Ok(())
    }

    fn evaluate(&self) -> QueryResult<Value> {
        Ok(self.sum.into())
    }
}

#[tokio::test]
async fn test_my_sum() -> Result<()> {
    common_telemetry::init_default_ut_logging();

    test_my_sum_with(
        (1..=10).collect::<Vec<u32>>(),
        r#"+--------+
| my_sum |
+--------+
| 55     |
+--------+"#,
    )
    .await?;
    test_my_sum_with(
        (-10..=11).collect::<Vec<i32>>(),
        r#"+--------+
| my_sum |
+--------+
| 11     |
+--------+"#,
    )
    .await?;
    test_my_sum_with(
        vec![-1.0f32, 1.0, 2.0, 3.0, 4.0],
        r#"+--------+
| my_sum |
+--------+
| 9.0    |
+--------+"#,
    )
    .await?;
    test_my_sum_with(
        vec![u32::MAX, u32::MAX],
        r#"+------------+
| my_sum     |
+------------+
| 8589934590 |
+------------+"#,
    )
    .await?;
    Ok(())
}

async fn test_my_sum_with<T>(numbers: Vec<T>, expected: &str) -> Result<()>
where
    T: WrapperType,
{
    let table_name = format!("{}_numbers", std::any::type_name::<T>());
    let column_name = format!("{}_number", std::any::type_name::<T>());

    let column_schemas = vec![ColumnSchema::new(
        column_name.clone(),
        T::LogicalType::build_data_type(),
        true,
    )];
    let schema = Arc::new(Schema::new(column_schemas.clone()));
    let column: VectorRef = Arc::new(T::VectorType::from_vec(numbers));
    let recordbatch = RecordBatch::new(schema, vec![column]).unwrap();
    let testing_table = MemTable::table(&table_name, recordbatch);

    let engine = new_query_engine_with_table(testing_table);

    engine.register_aggregate_function(Arc::new(AggregateFunctionMeta::new(
        "my_sum",
        1,
        Arc::new(|| Arc::new(MySumAccumulatorCreator::default())),
    )));

    let sql = format!("select MY_SUM({column_name}) as my_sum from {table_name}");
    let batches = exec_selection(engine, &sql).await;
    let batches = RecordBatches::try_new(batches.first().unwrap().schema.clone(), batches).unwrap();

    let pretty_print = batches.pretty_print().unwrap();
    assert_eq!(expected, pretty_print);
    Ok(())
}
