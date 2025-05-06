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

//! Function signature, useful for type checking and function resolution.

use datatypes::data_type::ConcreteDataType;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

/// Function signature
///
/// TODO(discord9): use `common_query::signature::Signature` crate
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize, Hash)]
pub struct Signature {
    /// the input types, usually not great than two input arg
    pub input: SmallVec<[ConcreteDataType; 2]>,
    /// Output type
    pub output: ConcreteDataType,
    /// Generic function
    pub generic_fn: GenericFn,
}

/// Generic function category
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize, Hash)]
pub enum GenericFn {
    // aggregate func
    Max,
    Min,
    Sum,
    Count,
    Any,
    All,
    // unary func
    Not,
    IsNull,
    IsTrue,
    IsFalse,
    StepTimestamp,
    Cast,
    // binary func
    Eq,
    NotEq,
    Lt,
    Lte,
    Gt,
    Gte,
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    // variadic func
    And,
    Or,
    // unmaterized func
    Now,
    CurrentSchema,
    TumbleWindow,
}
