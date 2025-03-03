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

use std::sync::Arc;

use datafusion_expr::ReturnTypeFunction as DfReturnTypeFunction;
use datatypes::arrow::datatypes::DataType as ArrowDataType;
use datatypes::prelude::{ConcreteDataType, DataType};

use crate::error::Result;
use crate::logical_plan::Accumulator;

/// A function's return type
pub type ReturnTypeFunction =
    Arc<dyn Fn(&[ConcreteDataType]) -> Result<Arc<ConcreteDataType>> + Send + Sync>;

/// Accumulator creator that will be used by DataFusion
pub type AccumulatorFunctionImpl = Arc<dyn Fn() -> Result<Box<dyn Accumulator>> + Send + Sync>;

/// Create Accumulator with the data type of input columns.
pub type AccumulatorCreatorFunction =
    Arc<dyn Fn(&[ConcreteDataType]) -> Result<Box<dyn Accumulator>> + Sync + Send>;

/// This signature corresponds to which types an aggregator serializes
/// its state, given its return datatype.
pub type StateTypeFunction =
    Arc<dyn Fn(&ConcreteDataType) -> Result<Arc<Vec<ConcreteDataType>>> + Send + Sync>;

pub fn to_df_return_type(func: ReturnTypeFunction) -> DfReturnTypeFunction {
    let df_func = move |data_types: &[ArrowDataType]| {
        // DataFusion DataType -> ConcreteDataType
        let concrete_data_types = data_types
            .iter()
            .map(ConcreteDataType::from_arrow_type)
            .collect::<Vec<_>>();

        // evaluate ConcreteDataType
        let eval_result = (func)(&concrete_data_types);

        // ConcreteDataType -> DataFusion DataType
        eval_result
            .map(|t| Arc::new(t.as_arrow_type()))
            .map_err(|e| e.into())
    };
    Arc::new(df_func)
}
