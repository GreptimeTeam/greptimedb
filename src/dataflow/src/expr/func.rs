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

use common_time::DateTime;
use datatypes::data_type::ConcreteDataType;
use datatypes::types::cast::CastOption;
use datatypes::types::cast_with_opt;
use datatypes::value::Value;
use hydroflow::bincode::Error;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use super::ScalarExpr;
use crate::expr::error::CastValueSnafu;
// TODO(discord9): more function & eval
use crate::{
    expr::error::{EvalError, TryFromValueSnafu, TypeMismatchSnafu},
    repr::Row,
};

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum UnmaterializableFunc {
    Now,
    CurrentDatabase,
    CurrentSchema,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize, Hash)]
pub enum UnaryFunc {
    Not,
    IsNull,
    IsTrue,
    IsFalse,
    StepTimestamp,
    Cast(ConcreteDataType),
}

impl UnaryFunc {
    pub fn eval(&self, values: &[Value], expr: &ScalarExpr) -> Result<Value, EvalError> {
        let arg = expr.eval(values)?;
        match self {
            Self::Not => {
                let bool = if let Value::Boolean(bool) = arg {
                    Ok(bool)
                } else {
                    TypeMismatchSnafu {
                        expected: ConcreteDataType::boolean_datatype(),
                        actual: arg.data_type(),
                    }
                    .fail()?
                }?;
                Ok(Value::from(!bool))
            }
            Self::IsNull => Ok(Value::from(arg.is_null())),
            Self::IsTrue => {
                let bool = if let Value::Boolean(bool) = arg {
                    Ok(bool)
                } else {
                    TypeMismatchSnafu {
                        expected: ConcreteDataType::boolean_datatype(),
                        actual: arg.data_type(),
                    }
                    .fail()?
                }?;
                Ok(Value::from(bool))
            }
            Self::IsFalse => {
                let bool = if let Value::Boolean(bool) = arg {
                    Ok(bool)
                } else {
                    TypeMismatchSnafu {
                        expected: ConcreteDataType::boolean_datatype(),
                        actual: arg.data_type(),
                    }
                    .fail()?
                }?;
                Ok(Value::from(!bool))
            }
            Self::StepTimestamp => {
                if let Value::DateTime(datetime) = arg {
                    let datetime = DateTime::from(datetime.val() + 1);
                    Ok(Value::from(datetime))
                } else {
                    TypeMismatchSnafu {
                        expected: ConcreteDataType::datetime_datatype(),
                        actual: arg.data_type(),
                    }
                    .fail()?
                }
            }
            Self::Cast(to) => {
                let arg_ty = arg.data_type();
                let res = cast_with_opt(arg, to, &CastOption { strict: true }).map_err(|e| {
                    CastValueSnafu {
                        from: arg_ty,
                        to: to.clone(),
                        msg: e.to_string(),
                    }
                    .build()
                })?;
                Ok(res)
            }
        }
    }
}

/// TODO(discord9): support more binary functions for more types
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize, Hash)]
pub enum BinaryFunc {
    Eq,
    NotEq,
    Lt,
    Lte,
    Gt,
    Gte,
    AddInt16,
    AddInt32,
    AddInt64,
    AddUInt16,
    AddUInt32,
    AddUInt64,
    AddFloat32,
    AddFloat64,
    SubInt16,
    SubInt32,
    SubInt64,
    SubUInt16,
    SubUInt32,
    SubUInt64,
    SubFloat32,
    SubFloat64,
    MulInt16,
    MulInt32,
    MulInt64,
    MulUInt16,
    MulUInt32,
    MulUInt64,
    MulFloat32,
    MulFloat64,
    DivInt16,
    DivInt32,
    DivInt64,
    DivUInt16,
    DivUInt32,
    DivUInt64,
    DivFloat32,
    DivFloat64,
    ModInt16,
    ModInt32,
    ModInt64,
    ModUInt16,
    ModUInt32,
    ModUInt64,
}

impl BinaryFunc {
    pub fn eval(
        &self,
        values: &[Value],
        expr1: &ScalarExpr,
        expr2: &ScalarExpr,
    ) -> Result<Value, EvalError> {
        let left = expr1.eval(values)?;
        let right = expr2.eval(values)?;
        match self {
            Self::Eq => Ok(Value::from(left == right)),
            Self::NotEq => Ok(Value::from(left != right)),
            Self::Lt => Ok(Value::from(left < right)),
            Self::Lte => Ok(Value::from(left <= right)),
            Self::Gt => Ok(Value::from(left > right)),
            Self::Gte => Ok(Value::from(left >= right)),
            Self::AddInt16 => Ok(add::<i16>(left, right)?),
            Self::AddInt32 => Ok(add::<i32>(left, right)?),
            Self::AddInt64 => Ok(add::<i64>(left, right)?),
            Self::AddUInt16 => Ok(add::<u16>(left, right)?),
            Self::AddUInt32 => Ok(add::<u32>(left, right)?),
            Self::AddUInt64 => Ok(add::<u64>(left, right)?),
            Self::AddFloat32 => Ok(add::<f32>(left, right)?),
            Self::AddFloat64 => Ok(add::<f64>(left, right)?),

            Self::SubInt16 => Ok(sub::<i16>(left, right)?),
            Self::SubInt32 => Ok(sub::<i32>(left, right)?),
            Self::SubInt64 => Ok(sub::<i64>(left, right)?),
            Self::SubUInt16 => Ok(sub::<u16>(left, right)?),
            Self::SubUInt32 => Ok(sub::<u32>(left, right)?),
            Self::SubUInt64 => Ok(sub::<u64>(left, right)?),
            Self::SubFloat32 => Ok(sub::<f32>(left, right)?),
            Self::SubFloat64 => Ok(sub::<f64>(left, right)?),

            Self::MulInt16 => Ok(mul::<i16>(left, right)?),
            Self::MulInt32 => Ok(mul::<i32>(left, right)?),
            Self::MulInt64 => Ok(mul::<i64>(left, right)?),
            Self::MulUInt16 => Ok(mul::<u16>(left, right)?),
            Self::MulUInt32 => Ok(mul::<u32>(left, right)?),
            Self::MulUInt64 => Ok(mul::<u64>(left, right)?),
            Self::MulFloat32 => Ok(mul::<f32>(left, right)?),
            Self::MulFloat64 => Ok(mul::<f64>(left, right)?),

            Self::DivInt16 => Ok(div::<i16>(left, right)?),
            Self::DivInt32 => Ok(div::<i32>(left, right)?),
            Self::DivInt64 => Ok(div::<i64>(left, right)?),
            Self::DivUInt16 => Ok(div::<u16>(left, right)?),
            Self::DivUInt32 => Ok(div::<u32>(left, right)?),
            Self::DivUInt64 => Ok(div::<u64>(left, right)?),
            Self::DivFloat32 => Ok(div::<f32>(left, right)?),
            Self::DivFloat64 => Ok(div::<f64>(left, right)?),

            Self::ModInt16 => Ok(rem::<i16>(left, right)?),
            Self::ModInt32 => Ok(rem::<i32>(left, right)?),
            Self::ModInt64 => Ok(rem::<i64>(left, right)?),
            Self::ModUInt16 => Ok(rem::<u16>(left, right)?),
            Self::ModUInt32 => Ok(rem::<u32>(left, right)?),
            Self::ModUInt64 => Ok(rem::<u64>(left, right)?),

            _ => todo!(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize, Hash)]
pub enum VariadicFunc {}

impl VariadicFunc {
    pub fn name_to_func(name: &str) -> Option<Self> {
        todo!("No variadic function yet")
    }
    pub fn eval(&self, values: &[Value], exprs: &[ScalarExpr]) -> Result<Value, EvalError> {
        todo!()
    }
}

fn add<T>(left: Value, right: Value) -> Result<Value, EvalError>
where
    T: TryFrom<Value, Error = datatypes::Error> + std::ops::Add<Output = T>,
    Value: From<T>,
{
    let left = T::try_from(left)
        .map_err(|e| e.to_string())
        .map_err(|e| TryFromValueSnafu { msg: e }.build())?;
    let right = T::try_from(right)
        .map_err(|e| e.to_string())
        .map_err(|e| TryFromValueSnafu { msg: e }.build())?;
    Ok(Value::from(left + right))
}

fn sub<T>(left: Value, right: Value) -> Result<Value, EvalError>
where
    T: TryFrom<Value, Error = datatypes::Error> + std::ops::Sub<Output = T>,
    Value: From<T>,
{
    let left = T::try_from(left)
        .map_err(|e| e.to_string())
        .map_err(|e| TryFromValueSnafu { msg: e }.build())?;
    let right = T::try_from(right)
        .map_err(|e| e.to_string())
        .map_err(|e| TryFromValueSnafu { msg: e }.build())?;
    Ok(Value::from(left - right))
}

fn mul<T>(left: Value, right: Value) -> Result<Value, EvalError>
where
    T: TryFrom<Value, Error = datatypes::Error> + std::ops::Mul<Output = T>,
    Value: From<T>,
{
    let left = T::try_from(left)
        .map_err(|e| e.to_string())
        .map_err(|e| TryFromValueSnafu { msg: e }.build())?;
    let right = T::try_from(right)
        .map_err(|e| e.to_string())
        .map_err(|e| TryFromValueSnafu { msg: e }.build())?;
    Ok(Value::from(left * right))
}

fn div<T>(left: Value, right: Value) -> Result<Value, EvalError>
where
    T: TryFrom<Value, Error = datatypes::Error> + std::ops::Div<Output = T>,
    <T as TryFrom<Value>>::Error: std::fmt::Debug,
    Value: From<T>,
{
    let left = T::try_from(left)
        .map_err(|e| e.to_string())
        .map_err(|e| TryFromValueSnafu { msg: e }.build())?;
    let right = T::try_from(right)
        .map_err(|e| e.to_string())
        .map_err(|e| TryFromValueSnafu { msg: e }.build())?;
    Ok(Value::from(left / right))
}

fn rem<T>(left: Value, right: Value) -> Result<Value, EvalError>
where
    T: TryFrom<Value, Error = datatypes::Error> + std::ops::Rem<Output = T>,
    <T as TryFrom<Value>>::Error: std::fmt::Debug,
    Value: From<T>,
{
    let left = T::try_from(left)
        .map_err(|e| e.to_string())
        .map_err(|e| TryFromValueSnafu { msg: e }.build())?;
    let right = T::try_from(right)
        .map_err(|e| e.to_string())
        .map_err(|e| TryFromValueSnafu { msg: e }.build())?;
    Ok(Value::from(left % right))
}
