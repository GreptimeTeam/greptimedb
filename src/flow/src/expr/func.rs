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

//! This module contains the definition of functions that can be used in expressions.

use std::collections::HashMap;
use std::sync::OnceLock;

use common_telemetry::debug;
use common_time::DateTime;
use datafusion_expr::Operator;
use datatypes::data_type::ConcreteDataType;
use datatypes::types::cast;
use datatypes::types::cast::CastOption;
use datatypes::value::Value;
use serde::{Deserialize, Serialize};
use smallvec::smallvec;
use snafu::{ensure, OptionExt, ResultExt};
use strum::{EnumIter, IntoEnumIterator};
use substrait::df_logical_plan::consumer::name_to_op;

use crate::adapter::error::{Error, InvalidQuerySnafu, PlanSnafu};
use crate::expr::error::{
    CastValueSnafu, DivisionByZeroSnafu, EvalError, InternalSnafu, TryFromValueSnafu,
    TypeMismatchSnafu,
};
use crate::expr::signature::{GenericFn, Signature};
use crate::expr::{InvalidArgumentSnafu, ScalarExpr};
use crate::repr::{value_to_internal_ts, Row};

/// UnmaterializableFunc is a function that can't be eval independently,
/// and require special handling
#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum UnmaterializableFunc {
    Now,
    CurrentSchema,
}

impl UnmaterializableFunc {
    /// Return the signature of the function
    pub fn signature(&self) -> Signature {
        match self {
            Self::Now => Signature {
                input: smallvec![],
                output: ConcreteDataType::datetime_datatype(),
                generic_fn: GenericFn::Now,
            },
            Self::CurrentSchema => Signature {
                input: smallvec![],
                output: ConcreteDataType::string_datatype(),
                generic_fn: GenericFn::CurrentSchema,
            },
        }
    }

    /// Create a UnmaterializableFunc from a string of the function name
    pub fn from_str(name: &str) -> Result<Self, Error> {
        match name {
            "now" => Ok(Self::Now),
            "current_schema" => Ok(Self::CurrentSchema),
            _ => InvalidQuerySnafu {
                reason: format!("Unknown unmaterializable function: {}", name),
            }
            .fail(),
        }
    }
}

/// UnaryFunc is a function that takes one argument. Also notice this enum doesn't contain function arguments,
/// because the arguments are stored in the expression. (except `cast` function, which requires a type argument)
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
    /// Return the signature of the function
    pub fn signature(&self) -> Signature {
        match self {
            Self::IsNull => Signature {
                input: smallvec![ConcreteDataType::null_datatype()],
                output: ConcreteDataType::boolean_datatype(),
                generic_fn: GenericFn::IsNull,
            },
            Self::Not | Self::IsTrue | Self::IsFalse => Signature {
                input: smallvec![ConcreteDataType::boolean_datatype()],
                output: ConcreteDataType::boolean_datatype(),
                generic_fn: match self {
                    Self::Not => GenericFn::Not,
                    Self::IsTrue => GenericFn::IsTrue,
                    Self::IsFalse => GenericFn::IsFalse,
                    _ => unreachable!(),
                },
            },
            Self::StepTimestamp => Signature {
                input: smallvec![ConcreteDataType::datetime_datatype()],
                output: ConcreteDataType::datetime_datatype(),
                generic_fn: GenericFn::StepTimestamp,
            },
            Self::Cast(to) => Signature {
                input: smallvec![ConcreteDataType::null_datatype()],
                output: to.clone(),
                generic_fn: GenericFn::Cast,
            },
        }
    }

    /// Create a UnaryFunc from a string of the function name and given argument type(optional)
    pub fn from_str_and_type(
        name: &str,
        arg_type: Option<ConcreteDataType>,
    ) -> Result<Self, Error> {
        match name {
            "not" => Ok(Self::Not),
            "is_null" => Ok(Self::IsNull),
            "is_true" => Ok(Self::IsTrue),
            "is_false" => Ok(Self::IsFalse),
            "step_timestamp" => Ok(Self::StepTimestamp),
            "cast" => {
                let arg_type = arg_type.with_context(|| InvalidQuerySnafu {
                    reason: "cast function requires a type argument".to_string(),
                })?;
                Ok(UnaryFunc::Cast(arg_type))
            }
            _ => InvalidQuerySnafu {
                reason: format!("Unknown unary function: {}", name),
            }
            .fail(),
        }
    }

    /// Evaluate the function with given values and expression
    ///
    /// # Arguments
    ///
    /// - `values`: The values to be used in the evaluation
    ///
    /// - `expr`: The expression to be evaluated and use as argument, will extract the value from the `values` and evaluate the expression
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
            Self::IsTrue | Self::IsFalse => {
                let bool = if let Value::Boolean(bool) = arg {
                    Ok(bool)
                } else {
                    TypeMismatchSnafu {
                        expected: ConcreteDataType::boolean_datatype(),
                        actual: arg.data_type(),
                    }
                    .fail()?
                }?;
                if matches!(self, Self::IsTrue) {
                    Ok(Value::from(bool))
                } else {
                    Ok(Value::from(!bool))
                }
            }
            Self::StepTimestamp => {
                let ty = arg.data_type();
                if let Value::DateTime(datetime) = arg {
                    let datetime = DateTime::from(datetime.val() + 1);
                    Ok(Value::from(datetime))
                } else if let Ok(v) = value_to_internal_ts(arg) {
                    let datetime = DateTime::from(v + 1);
                    Ok(Value::from(datetime))
                } else {
                    TypeMismatchSnafu {
                        expected: ConcreteDataType::datetime_datatype(),
                        actual: ty,
                    }
                    .fail()?
                }
            }
            Self::Cast(to) => {
                let arg_ty = arg.data_type();
                let res = cast(arg, to).context({
                    CastValueSnafu {
                        from: arg_ty,
                        to: to.clone(),
                    }
                });
                debug!("Cast to type: {to:?}, result: {:?}", res);
                res
            }
        }
    }
}

/// BinaryFunc is a function that takes two arguments.
/// Also notice this enum doesn't contain function arguments, since the arguments are stored in the expression.
///
/// TODO(discord9): support more binary functions for more types
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize, Hash, EnumIter,
)]
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

/// Generate binary function signature based on the function and the input types
/// The user can provide custom signature for some functions in the form of a regular match arm,
/// and the rest will be generated according to the provided list of functions like this:
/// ```ignore
/// AddInt16=>(int16_datatype,Add),
/// ```
/// which expand to:
/// ```ignore, rust
/// Self::AddInt16 => Signature {
///    input: smallvec![
///       ConcreteDataType::int16_datatype(),
///      ConcreteDataType::int16_datatype(),
///    ],
///    output: ConcreteDataType::int16_datatype(),
///    generic_fn: GenericFn::Add,
/// },
/// ````
macro_rules! generate_binary_signature {
    ($value:ident, { $($user_arm:tt)* },
    [ $(
        $auto_arm:ident=>($con_type:ident,$generic:ident)
        ),*
    ]) => {
        match $value {
            $($user_arm)*,
            $(
                Self::$auto_arm => Signature {
                    input: smallvec![
                        ConcreteDataType::$con_type(),
                        ConcreteDataType::$con_type(),
                    ],
                    output: ConcreteDataType::$con_type(),
                    generic_fn: GenericFn::$generic,
                },
            )*
        }
    };
}

static SPECIALIZATION: OnceLock<HashMap<(GenericFn, ConcreteDataType), BinaryFunc>> =
    OnceLock::new();

impl BinaryFunc {
    /// Use null type to ref to any type
    pub fn signature(&self) -> Signature {
        generate_binary_signature!(self, {
                Self::Eq | Self::NotEq | Self::Lt | Self::Lte | Self::Gt | Self::Gte => Signature {
                    input: smallvec![
                        ConcreteDataType::null_datatype(),
                        ConcreteDataType::null_datatype()
                    ],
                    output: ConcreteDataType::boolean_datatype(),
                    generic_fn: match self {
                        Self::Eq => GenericFn::Eq,
                        Self::NotEq => GenericFn::NotEq,
                        Self::Lt => GenericFn::Lt,
                        Self::Lte => GenericFn::Lte,
                        Self::Gt => GenericFn::Gt,
                        Self::Gte => GenericFn::Gte,
                        _ => unreachable!(),
                    },
                }
            },
            [
                AddInt16=>(int16_datatype,Add),
                AddInt32=>(int32_datatype,Add),
                AddInt64=>(int64_datatype,Add),
                AddUInt16=>(uint16_datatype,Add),
                AddUInt32=>(uint32_datatype,Add),
                AddUInt64=>(uint64_datatype,Add),
                AddFloat32=>(float32_datatype,Add),
                AddFloat64=>(float64_datatype,Add),
                SubInt16=>(int16_datatype,Sub),
                SubInt32=>(int32_datatype,Sub),
                SubInt64=>(int64_datatype,Sub),
                SubUInt16=>(uint16_datatype,Sub),
                SubUInt32=>(uint32_datatype,Sub),
                SubUInt64=>(uint64_datatype,Sub),
                SubFloat32=>(float32_datatype,Sub),
                SubFloat64=>(float64_datatype,Sub),
                MulInt16=>(int16_datatype,Mul),
                MulInt32=>(int32_datatype,Mul),
                MulInt64=>(int64_datatype,Mul),
                MulUInt16=>(uint16_datatype,Mul),
                MulUInt32=>(uint32_datatype,Mul),
                MulUInt64=>(uint64_datatype,Mul),
                MulFloat32=>(float32_datatype,Mul),
                MulFloat64=>(float64_datatype,Mul),
                DivInt16=>(int16_datatype,Div),
                DivInt32=>(int32_datatype,Div),
                DivInt64=>(int64_datatype,Div),
                DivUInt16=>(uint16_datatype,Div),
                DivUInt32=>(uint32_datatype,Div),
                DivUInt64=>(uint64_datatype,Div),
                DivFloat32=>(float32_datatype,Div),
                DivFloat64=>(float64_datatype,Div),
                ModInt16=>(int16_datatype,Mod),
                ModInt32=>(int32_datatype,Mod),
                ModInt64=>(int64_datatype,Mod),
                ModUInt16=>(uint16_datatype,Mod),
                ModUInt32=>(uint32_datatype,Mod),
                ModUInt64=>(uint64_datatype,Mod)
            ]
        )
    }

    pub fn add(input_type: ConcreteDataType) -> Result<Self, Error> {
        Self::specialization(GenericFn::Add, input_type)
    }

    pub fn sub(input_type: ConcreteDataType) -> Result<Self, Error> {
        Self::specialization(GenericFn::Sub, input_type)
    }

    pub fn mul(input_type: ConcreteDataType) -> Result<Self, Error> {
        Self::specialization(GenericFn::Mul, input_type)
    }

    pub fn div(input_type: ConcreteDataType) -> Result<Self, Error> {
        Self::specialization(GenericFn::Div, input_type)
    }

    /// Get the specialization of the binary function based on the generic function and the input type
    pub fn specialization(generic: GenericFn, input_type: ConcreteDataType) -> Result<Self, Error> {
        let rule = SPECIALIZATION.get_or_init(|| {
            let mut spec = HashMap::new();
            for func in BinaryFunc::iter() {
                let sig = func.signature();
                spec.insert((sig.generic_fn, sig.input[0].clone()), func);
            }
            spec
        });
        rule.get(&(generic, input_type.clone()))
            .cloned()
            .with_context(|| InvalidQuerySnafu {
                reason: format!(
                    "No specialization found for binary function {:?} with input type {:?}",
                    generic, input_type
                ),
            })
    }

    /// try it's best to infer types from the input types and expressions
    ///
    /// if it can't found out types, will return None
    pub(crate) fn infer_type_from(
        generic: GenericFn,
        arg_exprs: &[ScalarExpr],
        arg_types: &[Option<ConcreteDataType>],
    ) -> Result<ConcreteDataType, Error> {
        let ret = match (arg_types[0].as_ref(), arg_types[1].as_ref()) {
            (Some(t1), Some(t2)) => {
                ensure!(
                    t1 == t2,
                    InvalidQuerySnafu {
                        reason: format!(
                            "Binary function {:?} requires both arguments to have the same type",
                            generic
                        ),
                    }
                );
                t1.clone()
            }
            (Some(t), None) | (None, Some(t)) => t.clone(),
            _ => arg_exprs[0]
                .as_literal()
                .map(|lit| lit.data_type())
                .or_else(|| arg_exprs[1].as_literal().map(|lit| lit.data_type()))
                .with_context(|| InvalidQuerySnafu {
                    reason: format!(
                        "Binary function {:?} requires at least one argument with known type",
                        generic
                    ),
                })?,
        };
        Ok(ret)
    }

    /// choose the appropriate specialization based on the input types
    /// return a specialization of the binary function and it's actual input and output type(so no null type present)
    ///
    /// will try it best to extract from `arg_types` and `arg_exprs` to get the input types
    /// if `arg_types` is not enough, it will try to extract from `arg_exprs` if `arg_exprs` is literal with known type
    pub fn from_str_expr_and_type(
        name: &str,
        arg_exprs: &[ScalarExpr],
        arg_types: &[Option<ConcreteDataType>],
    ) -> Result<(Self, Signature), Error> {
        // this `name_to_op` if error simply return a similar message of `unsupported function xxx` so
        let op = name_to_op(name).or_else(|err| {
            if let datafusion_common::DataFusionError::NotImplemented(msg) = err {
                InvalidQuerySnafu {
                    reason: format!("Unsupported binary function: {}", msg),
                }
                .fail()
            } else {
                InvalidQuerySnafu {
                    reason: format!("Error when parsing binary function: {:?}", err),
                }
                .fail()
            }
        })?;

        // get first arg type and make sure if both is some, they are the same
        let generic_fn = {
            match op {
                Operator::Eq => GenericFn::Eq,
                Operator::NotEq => GenericFn::NotEq,
                Operator::Lt => GenericFn::Lt,
                Operator::LtEq => GenericFn::Lte,
                Operator::Gt => GenericFn::Gt,
                Operator::GtEq => GenericFn::Gte,
                Operator::Plus => GenericFn::Add,
                Operator::Minus => GenericFn::Sub,
                Operator::Multiply => GenericFn::Mul,
                Operator::Divide => GenericFn::Div,
                Operator::Modulo => GenericFn::Mod,
                _ => {
                    return InvalidQuerySnafu {
                        reason: format!("Unsupported binary function: {}", name),
                    }
                    .fail();
                }
            }
        };
        let need_type = matches!(
            generic_fn,
            GenericFn::Add | GenericFn::Sub | GenericFn::Mul | GenericFn::Div | GenericFn::Mod
        );

        ensure!(
            arg_exprs.len() == 2 && arg_types.len() == 2,
            PlanSnafu {
                reason: "Binary function requires exactly 2 arguments".to_string()
            }
        );

        let arg_type = Self::infer_type_from(generic_fn, arg_exprs, arg_types)?;

        // if type is not needed, we can erase input type to null to find correct functions for
        // functions that do not need type
        let query_input_type = if need_type {
            arg_type.clone()
        } else {
            ConcreteDataType::null_datatype()
        };

        let spec_fn = Self::specialization(generic_fn, query_input_type)?;

        let signature = Signature {
            input: smallvec![arg_type.clone(), arg_type],
            output: spec_fn.signature().output,
            generic_fn,
        };

        Ok((spec_fn, signature))
    }

    /// Evaluate the function with given values and expression
    ///
    /// # Arguments
    ///
    /// - `values`: The values to be used in the evaluation
    ///
    /// - `expr1`: The first arg to be evaluated, will extract the value from the `values` and evaluate the expression
    ///
    /// - `expr2`: The second arg to be evaluated
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
        }
    }

    /// Reverse the comparison operator, i.e. `a < b` becomes `b > a`,
    /// equal and not equal are unchanged.
    pub fn reverse_compare(&self) -> Result<Self, Error> {
        let ret = match &self {
            BinaryFunc::Eq => BinaryFunc::Eq,
            BinaryFunc::NotEq => BinaryFunc::NotEq,
            BinaryFunc::Lt => BinaryFunc::Gt,
            BinaryFunc::Lte => BinaryFunc::Gte,
            BinaryFunc::Gt => BinaryFunc::Lt,
            BinaryFunc::Gte => BinaryFunc::Lte,
            _ => {
                return InvalidQuerySnafu {
                    reason: format!("Expect a comparison operator, found {:?}", self),
                }
                .fail();
            }
        };
        Ok(ret)
    }
}

/// VariadicFunc is a function that takes a variable number of arguments.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize, Hash)]
pub enum VariadicFunc {
    And,
    Or,
}

impl VariadicFunc {
    /// Return the signature of the function
    pub fn signature(&self) -> Signature {
        Signature {
            input: smallvec![ConcreteDataType::boolean_datatype()],
            output: ConcreteDataType::boolean_datatype(),
            generic_fn: match self {
                Self::And => GenericFn::And,
                Self::Or => GenericFn::Or,
            },
        }
    }

    /// Create a VariadicFunc from a string of the function name and given argument types(optional)
    pub fn from_str_and_types(
        name: &str,
        arg_types: &[Option<ConcreteDataType>],
    ) -> Result<Self, Error> {
        // TODO: future variadic funcs to be added might need to check arg_types
        let _ = arg_types;
        match name {
            "and" => Ok(Self::And),
            "or" => Ok(Self::Or),
            _ => InvalidQuerySnafu {
                reason: format!("Unknown variadic function: {}", name),
            }
            .fail(),
        }
    }

    /// Evaluate the function with given values and expressions
    pub fn eval(&self, values: &[Value], exprs: &[ScalarExpr]) -> Result<Value, EvalError> {
        match self {
            VariadicFunc::And => and(values, exprs),
            VariadicFunc::Or => or(values, exprs),
        }
    }
}

fn and(values: &[Value], exprs: &[ScalarExpr]) -> Result<Value, EvalError> {
    // If any is false, then return false. Else, if any is null, then return null. Else, return true.
    let mut null = false;
    for expr in exprs {
        match expr.eval(values) {
            Ok(Value::Boolean(true)) => {}
            Ok(Value::Boolean(false)) => return Ok(Value::Boolean(false)), // short-circuit
            Ok(Value::Null) => null = true,
            Err(this_err) => {
                return Err(this_err);
            } // retain first error encountered
            Ok(x) => InvalidArgumentSnafu {
                reason: format!(
                    "`and()` only support boolean type, found value {:?} of type {:?}",
                    x,
                    x.data_type()
                ),
            }
            .fail()?,
        }
    }
    match null {
        true => Ok(Value::Null),
        false => Ok(Value::Boolean(true)),
    }
}

fn or(values: &[Value], exprs: &[ScalarExpr]) -> Result<Value, EvalError> {
    // If any is false, then return false. Else, if any is null, then return null. Else, return true.
    let mut null = false;
    for expr in exprs {
        match expr.eval(values) {
            Ok(Value::Boolean(true)) => return Ok(Value::Boolean(true)), // short-circuit
            Ok(Value::Boolean(false)) => {}
            Ok(Value::Null) => null = true,
            Err(this_err) => {
                return Err(this_err);
            } // retain first error encountered
            Ok(x) => InvalidArgumentSnafu {
                reason: format!(
                    "`or()` only support boolean type, found value {:?} of type {:?}",
                    x,
                    x.data_type()
                ),
            }
            .fail()?,
        }
    }
    match null {
        true => Ok(Value::Null),
        false => Ok(Value::Boolean(false)),
    }
}

fn add<T>(left: Value, right: Value) -> Result<Value, EvalError>
where
    T: TryFrom<Value, Error = datatypes::Error> + num_traits::Num,
    Value: From<T>,
{
    let left = T::try_from(left).map_err(|e| TryFromValueSnafu { msg: e.to_string() }.build())?;
    let right = T::try_from(right).map_err(|e| TryFromValueSnafu { msg: e.to_string() }.build())?;
    Ok(Value::from(left + right))
}

fn sub<T>(left: Value, right: Value) -> Result<Value, EvalError>
where
    T: TryFrom<Value, Error = datatypes::Error> + num_traits::Num,
    Value: From<T>,
{
    let left = T::try_from(left).map_err(|e| TryFromValueSnafu { msg: e.to_string() }.build())?;
    let right = T::try_from(right).map_err(|e| TryFromValueSnafu { msg: e.to_string() }.build())?;
    Ok(Value::from(left - right))
}

fn mul<T>(left: Value, right: Value) -> Result<Value, EvalError>
where
    T: TryFrom<Value, Error = datatypes::Error> + num_traits::Num,
    Value: From<T>,
{
    let left = T::try_from(left).map_err(|e| TryFromValueSnafu { msg: e.to_string() }.build())?;
    let right = T::try_from(right).map_err(|e| TryFromValueSnafu { msg: e.to_string() }.build())?;
    Ok(Value::from(left * right))
}

fn div<T>(left: Value, right: Value) -> Result<Value, EvalError>
where
    T: TryFrom<Value, Error = datatypes::Error> + num_traits::Num,
    <T as TryFrom<Value>>::Error: std::fmt::Debug,
    Value: From<T>,
{
    let left = T::try_from(left).map_err(|e| TryFromValueSnafu { msg: e.to_string() }.build())?;
    let right = T::try_from(right).map_err(|e| TryFromValueSnafu { msg: e.to_string() }.build())?;
    if right.is_zero() {
        return Err(DivisionByZeroSnafu {}.build());
    }
    Ok(Value::from(left / right))
}

fn rem<T>(left: Value, right: Value) -> Result<Value, EvalError>
where
    T: TryFrom<Value, Error = datatypes::Error> + num_traits::Num,
    <T as TryFrom<Value>>::Error: std::fmt::Debug,
    Value: From<T>,
{
    let left = T::try_from(left).map_err(|e| TryFromValueSnafu { msg: e.to_string() }.build())?;
    let right = T::try_from(right).map_err(|e| TryFromValueSnafu { msg: e.to_string() }.build())?;
    Ok(Value::from(left % right))
}

#[test]
fn test_num_ops() {
    let left = Value::from(10);
    let right = Value::from(3);
    let res = add::<i32>(left.clone(), right.clone()).unwrap();
    assert_eq!(res, Value::from(13));
    let res = sub::<i32>(left.clone(), right.clone()).unwrap();
    assert_eq!(res, Value::from(7));
    let res = mul::<i32>(left.clone(), right.clone()).unwrap();
    assert_eq!(res, Value::from(30));
    let res = div::<i32>(left.clone(), right.clone()).unwrap();
    assert_eq!(res, Value::from(3));
    let res = rem::<i32>(left, right).unwrap();
    assert_eq!(res, Value::from(1));

    let values = vec![Value::from(true), Value::from(false)];
    let exprs = vec![ScalarExpr::Column(0), ScalarExpr::Column(1)];
    let res = and(&values, &exprs).unwrap();
    assert_eq!(res, Value::from(false));
    let res = or(&values, &exprs).unwrap();
    assert_eq!(res, Value::from(true));
}

/// test if the binary function specialization works
/// whether from direct type or from the expression that is literal
#[test]
fn test_binary_func_spec() {
    assert_eq!(
        BinaryFunc::from_str_expr_and_type(
            "add",
            &[ScalarExpr::Column(0), ScalarExpr::Column(0)],
            &[
                Some(ConcreteDataType::int32_datatype()),
                Some(ConcreteDataType::int32_datatype())
            ]
        )
        .unwrap(),
        (BinaryFunc::AddInt32, BinaryFunc::AddInt32.signature())
    );

    assert_eq!(
        BinaryFunc::from_str_expr_and_type(
            "add",
            &[ScalarExpr::Column(0), ScalarExpr::Column(0)],
            &[Some(ConcreteDataType::int32_datatype()), None]
        )
        .unwrap(),
        (BinaryFunc::AddInt32, BinaryFunc::AddInt32.signature())
    );

    assert_eq!(
        BinaryFunc::from_str_expr_and_type(
            "add",
            &[ScalarExpr::Column(0), ScalarExpr::Column(0)],
            &[Some(ConcreteDataType::int32_datatype()), None]
        )
        .unwrap(),
        (BinaryFunc::AddInt32, BinaryFunc::AddInt32.signature())
    );

    assert_eq!(
        BinaryFunc::from_str_expr_and_type(
            "add",
            &[ScalarExpr::Column(0), ScalarExpr::Column(0)],
            &[Some(ConcreteDataType::int32_datatype()), None]
        )
        .unwrap(),
        (BinaryFunc::AddInt32, BinaryFunc::AddInt32.signature())
    );

    assert_eq!(
        BinaryFunc::from_str_expr_and_type(
            "add",
            &[
                ScalarExpr::Literal(Value::from(1i32), ConcreteDataType::int32_datatype()),
                ScalarExpr::Column(0)
            ],
            &[None, None]
        )
        .unwrap(),
        (BinaryFunc::AddInt32, BinaryFunc::AddInt32.signature())
    );

    // this testcase make sure the specialization can find actual type from expression and fill in signature
    assert_eq!(
        BinaryFunc::from_str_expr_and_type(
            "equal",
            &[
                ScalarExpr::Literal(Value::from(1i32), ConcreteDataType::int32_datatype()),
                ScalarExpr::Column(0)
            ],
            &[None, None]
        )
        .unwrap(),
        (
            BinaryFunc::Eq,
            Signature {
                input: smallvec![
                    ConcreteDataType::int32_datatype(),
                    ConcreteDataType::int32_datatype()
                ],
                output: ConcreteDataType::boolean_datatype(),
                generic_fn: GenericFn::Eq
            }
        )
    );

    matches!(
        BinaryFunc::from_str_expr_and_type(
            "add",
            &[ScalarExpr::Column(0), ScalarExpr::Column(0)],
            &[None, None]
        ),
        Err(Error::InvalidQuery { .. })
    );
}
