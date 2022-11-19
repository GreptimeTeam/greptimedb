// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Accumulator module contains the trait definition for aggregation function's accumulators.

use std::fmt::Debug;
use std::sync::Arc;

use common_time::timestamp::TimeUnit;
use datafusion_common::Result as DfResult;
use datafusion_expr::Accumulator as DfAccumulator;
use datatypes::arrow::array::ArrayRef;
use datatypes::prelude::*;
use datatypes::value::ListValue;
use datatypes::vectors::{Helper as VectorHelper, VectorRef};
use snafu::ResultExt;

use crate::error::{self, Error, FromScalarValueSnafu, IntoVectorSnafu, Result};
use crate::prelude::*;

pub type AggregateFunctionCreatorRef = Arc<dyn AggregateFunctionCreator>;

/// An accumulator represents a stateful object that lives throughout the evaluation of multiple rows and
/// generically accumulates values.
///
/// An accumulator knows how to:
/// * update its state from inputs via `update_batch`
/// * convert its internal state to a vector of scalar values
/// * update its state from multiple accumulators' states via `merge_batch`
/// * compute the final value from its internal state via `evaluate`
///
/// Modified from DataFusion.
pub trait Accumulator: Send + Sync + Debug {
    /// Returns the state of the accumulator at the end of the accumulation.
    // in the case of an average on which we track `sum` and `n`, this function should return a vector
    // of two values, sum and n.
    fn state(&self) -> Result<Vec<Value>>;

    /// updates the accumulator's state from a vector of arrays.
    fn update_batch(&mut self, values: &[VectorRef]) -> Result<()>;

    /// updates the accumulator's state from a vector of states.
    fn merge_batch(&mut self, states: &[VectorRef]) -> Result<()>;

    /// returns its value based on its current state.
    fn evaluate(&self) -> Result<Value>;
}

/// An `AggregateFunctionCreator` dynamically creates `Accumulator`.
///
/// An `AggregateFunctionCreator` often has a companion struct, that
/// can store the input data types (impl [AggrFuncTypeStore]), and knows the output and states
/// types of an Accumulator.
pub trait AggregateFunctionCreator: AggrFuncTypeStore {
    /// Create a function that can create a new accumulator with some input data type.
    fn creator(&self) -> AccumulatorCreatorFunction;

    /// Get the Accumulator's output data type.
    fn output_type(&self) -> Result<ConcreteDataType>;

    /// Get the Accumulator's state data types.
    fn state_types(&self) -> Result<Vec<ConcreteDataType>>;
}

/// `AggrFuncTypeStore` stores the aggregate function's input data's types.
///
/// When creating Accumulator generically, we have to know the input data's types.
/// However, DataFusion does not provide the input data's types at the time of creating Accumulator.
/// To solve the problem, we store the datatypes upfront here.
pub trait AggrFuncTypeStore: Send + Sync + Debug {
    /// Get the input data types of the Accumulator.
    fn input_types(&self) -> Result<Vec<ConcreteDataType>>;

    /// Store the input data types that are provided by DataFusion at runtime (when it is evaluating
    /// return type function).
    fn set_input_types(&self, input_types: Vec<ConcreteDataType>) -> Result<()>;
}

pub fn make_accumulator_function(
    creator: Arc<dyn AggregateFunctionCreator>,
) -> AccumulatorFunctionImpl {
    Arc::new(move || {
        let input_types = creator.input_types()?;
        let creator = creator.creator();
        creator(&input_types)
    })
}

pub fn make_return_function(creator: Arc<dyn AggregateFunctionCreator>) -> ReturnTypeFunction {
    Arc::new(move |input_types| {
        creator.set_input_types(input_types.to_vec())?;

        let output_type = creator.output_type()?;
        Ok(Arc::new(output_type))
    })
}

pub fn make_state_function(creator: Arc<dyn AggregateFunctionCreator>) -> StateTypeFunction {
    Arc::new(move |_| Ok(Arc::new(creator.state_types()?)))
}

/// A wrapper type for our Accumulator to DataFusion's Accumulator,
/// so to make our Accumulator able to be executed by DataFusion query engine.
#[derive(Debug)]
pub struct DfAccumulatorAdaptor {
    accumulator: Box<dyn Accumulator>,
    creator: AggregateFunctionCreatorRef,
}

impl DfAccumulatorAdaptor {
    pub fn new(accumulator: Box<dyn Accumulator>, creator: AggregateFunctionCreatorRef) -> Self {
        Self {
            accumulator,
            creator,
        }
    }
}

impl DfAccumulator for DfAccumulatorAdaptor {
    fn state(&self) -> DfResult<Vec<ScalarValue>> {
        let state_values = self.accumulator.state()?;
        let state_types = self.creator.state_types()?;
        if state_values.len() != state_types.len() {
            return error::BadAccumulatorImplSnafu {
                err_msg: format!("Accumulator {:?} returned state values size do not match its state types size.", self),
            }
            .fail()
            .map_err(Error::from)?;
        }
        Ok(state_values
            .into_iter()
            .zip(state_types.iter())
            .map(|(v, t)| try_into_scalar_value(v, t))
            .collect::<Result<Vec<_>>>()
            .map_err(Error::from)?)
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        let vectors = VectorHelper::try_into_vectors(values)
            .context(FromScalarValueSnafu)
            .map_err(Error::from)?;
        self.accumulator
            .update_batch(&vectors)
            .map_err(|e| e.into())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        let mut vectors = Vec::with_capacity(states.len());
        for array in states.iter() {
            vectors.push(
                VectorHelper::try_into_vector(array)
                    .context(IntoVectorSnafu {
                        data_type: array.data_type().clone(),
                    })
                    .map_err(Error::from)?,
            );
        }
        self.accumulator.merge_batch(&vectors).map_err(|e| e.into())
    }

    fn evaluate(&self) -> DfResult<ScalarValue> {
        let value = self.accumulator.evaluate()?;
        let output_type = self.creator.output_type()?;
        Ok(try_into_scalar_value(value, &output_type)?)
    }
}

fn try_into_scalar_value(value: Value, datatype: &ConcreteDataType) -> Result<ScalarValue> {
    if !matches!(value, Value::Null) && datatype != &value.data_type() {
        return error::BadAccumulatorImplSnafu {
            err_msg: format!(
                "expect value to return datatype {:?}, actual: {:?}",
                datatype,
                value.data_type()
            ),
        }
        .fail()?;
    }

    Ok(match value {
        Value::Boolean(v) => ScalarValue::Boolean(Some(v)),
        Value::UInt8(v) => ScalarValue::UInt8(Some(v)),
        Value::UInt16(v) => ScalarValue::UInt16(Some(v)),
        Value::UInt32(v) => ScalarValue::UInt32(Some(v)),
        Value::UInt64(v) => ScalarValue::UInt64(Some(v)),
        Value::Int8(v) => ScalarValue::Int8(Some(v)),
        Value::Int16(v) => ScalarValue::Int16(Some(v)),
        Value::Int32(v) => ScalarValue::Int32(Some(v)),
        Value::Int64(v) => ScalarValue::Int64(Some(v)),
        Value::Float32(v) => ScalarValue::Float32(Some(v.0)),
        Value::Float64(v) => ScalarValue::Float64(Some(v.0)),
        Value::String(v) => ScalarValue::Utf8(Some(v.as_utf8().to_string())),
        Value::Binary(v) => ScalarValue::LargeBinary(Some(v.to_vec())),
        Value::Date(v) => ScalarValue::Date32(Some(v.val())),
        Value::DateTime(v) => ScalarValue::Date64(Some(v.val())),
        Value::Null => try_convert_null_value(datatype)?,
        Value::List(list) => try_convert_list_value(list)?,
        Value::Timestamp(t) => timestamp_to_scalar_value(t.unit(), Some(t.value())),
    })
}

fn timestamp_to_scalar_value(unit: TimeUnit, val: Option<i64>) -> ScalarValue {
    match unit {
        TimeUnit::Second => ScalarValue::TimestampSecond(val, None),
        TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(val, None),
        TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(val, None),
        TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(val, None),
    }
}

fn try_convert_null_value(datatype: &ConcreteDataType) -> Result<ScalarValue> {
    Ok(match datatype {
        ConcreteDataType::Boolean(_) => ScalarValue::Boolean(None),
        ConcreteDataType::Int8(_) => ScalarValue::Int8(None),
        ConcreteDataType::Int16(_) => ScalarValue::Int16(None),
        ConcreteDataType::Int32(_) => ScalarValue::Int32(None),
        ConcreteDataType::Int64(_) => ScalarValue::Int64(None),
        ConcreteDataType::UInt8(_) => ScalarValue::UInt8(None),
        ConcreteDataType::UInt16(_) => ScalarValue::UInt16(None),
        ConcreteDataType::UInt32(_) => ScalarValue::UInt32(None),
        ConcreteDataType::UInt64(_) => ScalarValue::UInt64(None),
        ConcreteDataType::Float32(_) => ScalarValue::Float32(None),
        ConcreteDataType::Float64(_) => ScalarValue::Float64(None),
        ConcreteDataType::Binary(_) => ScalarValue::LargeBinary(None),
        ConcreteDataType::String(_) => ScalarValue::Utf8(None),
        ConcreteDataType::Timestamp(t) => timestamp_to_scalar_value(t.unit, None),
        _ => {
            return error::BadAccumulatorImplSnafu {
                err_msg: format!(
                    "undefined transition from null value to datatype {:?}",
                    datatype
                ),
            }
            .fail()?
        }
    })
}

fn try_convert_list_value(list: ListValue) -> Result<ScalarValue> {
    let vs = if let Some(items) = list.items() {
        Some(Box::new(
            items
                .iter()
                .map(|v| try_into_scalar_value(v.clone(), list.datatype()))
                .collect::<Result<Vec<_>>>()?,
        ))
    } else {
        None
    };
    Ok(ScalarValue::List(
        vs,
        Box::new(list.datatype().as_arrow_type()),
    ))
}

#[cfg(test)]
mod tests {
    use common_base::bytes::{Bytes, StringBytes};
    use datafusion_common::ScalarValue;
    use datatypes::arrow::datatypes::DataType;
    use datatypes::value::{ListValue, OrderedFloat};

    use super::*;

    #[test]
    fn test_not_null_value_to_scalar_value() {
        assert_eq!(
            ScalarValue::Boolean(Some(true)),
            try_into_scalar_value(Value::Boolean(true), &ConcreteDataType::boolean_datatype())
                .unwrap()
        );
        assert_eq!(
            ScalarValue::Boolean(Some(false)),
            try_into_scalar_value(Value::Boolean(false), &ConcreteDataType::boolean_datatype())
                .unwrap()
        );
        assert_eq!(
            ScalarValue::UInt8(Some(u8::MIN + 1)),
            try_into_scalar_value(
                Value::UInt8(u8::MIN + 1),
                &ConcreteDataType::uint8_datatype()
            )
            .unwrap()
        );
        assert_eq!(
            ScalarValue::UInt16(Some(u16::MIN + 2)),
            try_into_scalar_value(
                Value::UInt16(u16::MIN + 2),
                &ConcreteDataType::uint16_datatype()
            )
            .unwrap()
        );
        assert_eq!(
            ScalarValue::UInt32(Some(u32::MIN + 3)),
            try_into_scalar_value(
                Value::UInt32(u32::MIN + 3),
                &ConcreteDataType::uint32_datatype()
            )
            .unwrap()
        );
        assert_eq!(
            ScalarValue::UInt64(Some(u64::MIN + 4)),
            try_into_scalar_value(
                Value::UInt64(u64::MIN + 4),
                &ConcreteDataType::uint64_datatype()
            )
            .unwrap()
        );
        assert_eq!(
            ScalarValue::Int8(Some(i8::MIN + 4)),
            try_into_scalar_value(Value::Int8(i8::MIN + 4), &ConcreteDataType::int8_datatype())
                .unwrap()
        );
        assert_eq!(
            ScalarValue::Int16(Some(i16::MIN + 5)),
            try_into_scalar_value(
                Value::Int16(i16::MIN + 5),
                &ConcreteDataType::int16_datatype()
            )
            .unwrap()
        );
        assert_eq!(
            ScalarValue::Int32(Some(i32::MIN + 6)),
            try_into_scalar_value(
                Value::Int32(i32::MIN + 6),
                &ConcreteDataType::int32_datatype()
            )
            .unwrap()
        );
        assert_eq!(
            ScalarValue::Int64(Some(i64::MIN + 7)),
            try_into_scalar_value(
                Value::Int64(i64::MIN + 7),
                &ConcreteDataType::int64_datatype()
            )
            .unwrap()
        );
        assert_eq!(
            ScalarValue::Float32(Some(8.0f32)),
            try_into_scalar_value(
                Value::Float32(OrderedFloat(8.0f32)),
                &ConcreteDataType::float32_datatype()
            )
            .unwrap()
        );
        assert_eq!(
            ScalarValue::Float64(Some(9.0f64)),
            try_into_scalar_value(
                Value::Float64(OrderedFloat(9.0f64)),
                &ConcreteDataType::float64_datatype()
            )
            .unwrap()
        );
        assert_eq!(
            ScalarValue::Utf8(Some("hello".to_string())),
            try_into_scalar_value(
                Value::String(StringBytes::from("hello")),
                &ConcreteDataType::string_datatype(),
            )
            .unwrap()
        );
        assert_eq!(
            ScalarValue::LargeBinary(Some("world".as_bytes().to_vec())),
            try_into_scalar_value(
                Value::Binary(Bytes::from("world".as_bytes())),
                &ConcreteDataType::binary_datatype()
            )
            .unwrap()
        );
    }

    #[test]
    fn test_null_value_to_scalar_value() {
        assert_eq!(
            ScalarValue::Boolean(None),
            try_into_scalar_value(Value::Null, &ConcreteDataType::boolean_datatype()).unwrap()
        );
        assert_eq!(
            ScalarValue::UInt8(None),
            try_into_scalar_value(Value::Null, &ConcreteDataType::uint8_datatype()).unwrap()
        );
        assert_eq!(
            ScalarValue::UInt16(None),
            try_into_scalar_value(Value::Null, &ConcreteDataType::uint16_datatype()).unwrap()
        );
        assert_eq!(
            ScalarValue::UInt32(None),
            try_into_scalar_value(Value::Null, &ConcreteDataType::uint32_datatype()).unwrap()
        );
        assert_eq!(
            ScalarValue::UInt64(None),
            try_into_scalar_value(Value::Null, &ConcreteDataType::uint64_datatype()).unwrap()
        );
        assert_eq!(
            ScalarValue::Int8(None),
            try_into_scalar_value(Value::Null, &ConcreteDataType::int8_datatype()).unwrap()
        );
        assert_eq!(
            ScalarValue::Int16(None),
            try_into_scalar_value(Value::Null, &ConcreteDataType::int16_datatype()).unwrap()
        );
        assert_eq!(
            ScalarValue::Int32(None),
            try_into_scalar_value(Value::Null, &ConcreteDataType::int32_datatype()).unwrap()
        );
        assert_eq!(
            ScalarValue::Int64(None),
            try_into_scalar_value(Value::Null, &ConcreteDataType::int64_datatype()).unwrap()
        );
        assert_eq!(
            ScalarValue::Float32(None),
            try_into_scalar_value(Value::Null, &ConcreteDataType::float32_datatype()).unwrap()
        );
        assert_eq!(
            ScalarValue::Float64(None),
            try_into_scalar_value(Value::Null, &ConcreteDataType::float64_datatype()).unwrap()
        );
        assert_eq!(
            ScalarValue::Utf8(None),
            try_into_scalar_value(Value::Null, &ConcreteDataType::string_datatype()).unwrap()
        );
        assert_eq!(
            ScalarValue::LargeBinary(None),
            try_into_scalar_value(Value::Null, &ConcreteDataType::binary_datatype()).unwrap()
        );
    }

    #[test]
    fn test_list_value_to_scalar_value() {
        let items = Some(Box::new(vec![Value::Int32(-1), Value::Null]));
        let list = Value::List(ListValue::new(items, ConcreteDataType::int32_datatype()));
        let df_list = try_into_scalar_value(
            list,
            &ConcreteDataType::list_datatype(ConcreteDataType::int32_datatype()),
        )
        .unwrap();
        assert!(matches!(df_list, ScalarValue::List(_, _)));
        match df_list {
            ScalarValue::List(vs, datatype) => {
                assert_eq!(*datatype, DataType::Int32);

                assert!(vs.is_some());
                let vs = *vs.unwrap();
                assert_eq!(
                    vs,
                    vec![ScalarValue::Int32(Some(-1)), ScalarValue::Int32(None)]
                );
            }
            _ => unreachable!(),
        }
    }

    #[test]
    pub fn test_timestamp_to_scalar_value() {
        assert_eq!(
            ScalarValue::TimestampSecond(Some(1), None),
            timestamp_to_scalar_value(TimeUnit::Second, Some(1))
        );
        assert_eq!(
            ScalarValue::TimestampMillisecond(Some(1), None),
            timestamp_to_scalar_value(TimeUnit::Millisecond, Some(1))
        );
        assert_eq!(
            ScalarValue::TimestampMicrosecond(Some(1), None),
            timestamp_to_scalar_value(TimeUnit::Microsecond, Some(1))
        );
        assert_eq!(
            ScalarValue::TimestampNanosecond(Some(1), None),
            timestamp_to_scalar_value(TimeUnit::Nanosecond, Some(1))
        );
    }
}
