use datatypes::prelude::ConcreteDataType;
use datatypes::value::{OrderedF32, OrderedF64, Value};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub enum AggregateFunc {
    MaxInt16,
    MaxInt32,
    MaxInt64,
    MaxUInt16,
    MaxUInt32,
    MaxUInt64,
    MaxFloat32,
    MaxFloat64,
    MaxBool,
    MaxString,
    MaxDate,
    MaxTimestamp,
    MaxTimestampTz,
    MinInt16,
    MinInt32,
    MinInt64,
    MinUInt16,
    MinUInt32,
    MinUInt64,
    MinFloat32,
    MinFloat64,
    MinBool,
    MinString,
    MinDate,
    MinTimestamp,
    MinTimestampTz,
    SumInt16,
    SumInt32,
    SumInt64,
    SumUInt16,
    SumUInt32,
    SumUInt64,
    SumFloat32,
    SumFloat64,
    Count,
    Any,
    All,
}

impl AggregateFunc {
    pub fn eval<I>(&self, values: I) -> Value
    where
        I: IntoIterator<Item = Value>,
    {
        match self {
            AggregateFunc::MaxInt16 => max_value::<I, i16>(values),
            AggregateFunc::MaxInt32 => max_value::<I, i32>(values),
            AggregateFunc::MaxInt64 => max_value::<I, i64>(values),
            AggregateFunc::MaxUInt16 => max_value::<I, u16>(values),
            AggregateFunc::MaxUInt32 => max_value::<I, u32>(values),
            AggregateFunc::MaxUInt64 => max_value::<I, u64>(values),
            AggregateFunc::MaxFloat32 => max_value::<I, OrderedF32>(values),
            AggregateFunc::MaxFloat64 => max_value::<I, OrderedF64>(values),
            _ => todo!(),
        }
    }
}

fn max_value<I, TypedValue>(values: I) -> Value
where
    I: IntoIterator<Item = Value>,
    TypedValue: TryFrom<Value> + Ord,
    <TypedValue as TryFrom<Value>>::Error: std::fmt::Debug,
    Value: From<Option<TypedValue>>,
{
    let x: Option<TypedValue> = values
        .into_iter()
        .filter(|v| !v.is_null())
        .map(|v| TypedValue::try_from(v).expect("unexpected type"))
        .max();
    x.into()
}

fn min_value<I, TypedValue>(values: I) -> Value
where
    I: IntoIterator<Item = Value>,
    TypedValue: TryFrom<Value> + Ord,
    <TypedValue as TryFrom<Value>>::Error: std::fmt::Debug,
    Value: From<Option<TypedValue>>,
{
    let x: Option<TypedValue> = values
        .into_iter()
        .filter(|v| !v.is_null())
        .map(|v| TypedValue::try_from(v).expect("unexpected type"))
        .min();
    x.into()
}
