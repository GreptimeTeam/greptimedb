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

use datatypes::value::Value;
use serde::{Deserialize, Serialize};

use super::ScalarExpr;
// TODO(discord9): more function & eval
use crate::{adapter::error::EvalError, repr::Row};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize, Hash)]
pub enum UnaryFunc {
    Not,
    IsNull,
    IsTrue,
    IsFalse,
    // TODO: spec treat of cast
    CastDatetimeToInt64,
    CastInt64ToFloat32,
}

impl UnaryFunc {
    pub fn name_to_func(name: &str) -> Option<Self> {
        match name {
            "not" => Some(Self::Not),
            "is_null" => Some(Self::IsNull),
            "is_true" => Some(Self::IsTrue),
            "is_false" => Some(Self::IsFalse),
            "cast_datetime_to_int64" => Some(Self::CastDatetimeToInt64),
            "cast_int64_to_float32" => Some(Self::CastInt64ToFloat32),
            _ => None,
        }
    }
    pub fn eval(&self, values: &[Value], expr: &ScalarExpr) -> Result<Value, EvalError> {
        let arg = expr.eval(values)?;
        match self {
            Self::CastDatetimeToInt64 => {
                let datetime = if let Value::DateTime(datetime) = arg {
                    Ok(datetime.val())
                } else {
                    Err(EvalError::TypeMismatch(format!(
                        "cannot cast {:?} to datetime",
                        arg
                    )))
                }?;
                Ok(Value::from(datetime))
            }
            Self::CastInt64ToFloat32 => {
                let int64 = if let Value::Int64(int64) = arg {
                    Ok(int64)
                } else {
                    Err(EvalError::TypeMismatch(format!(
                        "cannot cast {:?} to int64",
                        arg
                    )))
                }?;
                Ok(Value::from(int64 as f32))
            }
            _ => todo!(),
        }
    }
}

/// TODO: support more binary functions for more types
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
    pub fn name_to_func(name: &str) -> Option<Self> {
        match name {
            "eq" => Some(Self::Eq),
            "not_eq" => Some(Self::NotEq),
            "lt" => Some(Self::Lt),
            "lte" => Some(Self::Lte),
            "gt" => Some(Self::Gt),
            "gte" => Some(Self::Gte),
            "add_int16" => Some(Self::AddInt16),
            "add_int32" => Some(Self::AddInt32),
            "add_int64" => Some(Self::AddInt64),
            "add_uint16" => Some(Self::AddUInt16),
            "add_uint32" => Some(Self::AddUInt32),
            "add_uint64" => Some(Self::AddUInt64),
            "add_float32" => Some(Self::AddFloat32),
            "add_float64" => Some(Self::AddFloat64),
            "sub_int16" => Some(Self::SubInt16),
            "sub_int32" => Some(Self::SubInt32),
            "sub_int64" => Some(Self::SubInt64),
            "sub_uint16" => Some(Self::SubUInt16),
            "sub_uint32" => Some(Self::SubUInt32),
            "sub_uint64" => Some(Self::SubUInt64),
            "sub_float32" => Some(Self::SubFloat32),
            "sub_float64" => Some(Self::SubFloat64),
            "mul_int16" => Some(Self::MulInt16),
            "mul_int32" => Some(Self::MulInt32),
            "mul_int64" => Some(Self::MulInt64),
            "mul_uint16" => Some(Self::MulUInt16),
            "mul_uint32" => Some(Self::MulUInt32),
            "mul_uint64" => Some(Self::MulUInt64),
            "mul_float32" => Some(Self::MulFloat32),
            "mul_float64" => Some(Self::MulFloat64),
            "div_int16" => Some(Self::DivInt16),
            "div_int32" => Some(Self::DivInt32),
            "div_int64" => Some(Self::DivInt64),
            "div_uint16" => Some(Self::DivUInt16),
            "div_uint32" => Some(Self::DivUInt32),
            "div_uint64" => Some(Self::DivUInt64),
            "div_float32" => Some(Self::DivFloat32),
            "div_float64" => Some(Self::DivFloat64),
            "mod_int16" => Some(Self::ModInt16),
            "mod_int32" => Some(Self::ModInt32),
            "mod_int64" => Some(Self::ModInt64),
            "mod_uint16" => Some(Self::ModUInt16),
            "mod_uint32" => Some(Self::ModUInt32),
            "mod_uint64" => Some(Self::ModUInt64),
            _ => None,
        }
    }
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
    T: TryFrom<Value> + std::ops::Add<Output = T>,
    <T as TryFrom<Value>>::Error: std::fmt::Debug,
    Value: From<T>,
{
    let left = T::try_from(left).map_err(|e| EvalError::TypeMismatch(format!("{:?}", e)))?;
    let right = T::try_from(right).map_err(|e| EvalError::TypeMismatch(format!("{:?}", e)))?;
    Ok(Value::from(left + right))
}

fn sub<T>(left: Value, right: Value) -> Result<Value, EvalError>
where
    T: TryFrom<Value> + std::ops::Sub<Output = T>,
    <T as TryFrom<Value>>::Error: std::fmt::Debug,
    Value: From<T>,
{
    let left = T::try_from(left).map_err(|e| EvalError::TypeMismatch(format!("{:?}", e)))?;
    let right = T::try_from(right).map_err(|e| EvalError::TypeMismatch(format!("{:?}", e)))?;
    Ok(Value::from(left - right))
}

fn mul<T>(left: Value, right: Value) -> Result<Value, EvalError>
where
    T: TryFrom<Value> + std::ops::Mul<Output = T>,
    <T as TryFrom<Value>>::Error: std::fmt::Debug,
    Value: From<T>,
{
    let left = T::try_from(left).map_err(|e| EvalError::TypeMismatch(format!("{:?}", e)))?;
    let right = T::try_from(right).map_err(|e| EvalError::TypeMismatch(format!("{:?}", e)))?;
    Ok(Value::from(left * right))
}

fn div<T>(left: Value, right: Value) -> Result<Value, EvalError>
where
    T: TryFrom<Value> + std::ops::Div<Output = T>,
    <T as TryFrom<Value>>::Error: std::fmt::Debug,
    Value: From<T>,
{
    let left = T::try_from(left).map_err(|e| EvalError::TypeMismatch(format!("{:?}", e)))?;
    let right = T::try_from(right).map_err(|e| EvalError::TypeMismatch(format!("{:?}", e)))?;
    Ok(Value::from(left / right))
}

fn rem<T>(left: Value, right: Value) -> Result<Value, EvalError>
where
    T: TryFrom<Value> + std::ops::Rem<Output = T>,
    <T as TryFrom<Value>>::Error: std::fmt::Debug,
    Value: From<T>,
{
    let left = T::try_from(left).map_err(|e| EvalError::TypeMismatch(format!("{:?}", e)))?;
    let right = T::try_from(right).map_err(|e| EvalError::TypeMismatch(format!("{:?}", e)))?;
    Ok(Value::from(left % right))
}
