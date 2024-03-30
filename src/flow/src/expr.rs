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

//! for declare Expression in dataflow, including map, reduce, id and join(TODO!) etc.

pub(crate) mod error;
mod func;
mod id;
mod linear;
mod relation;
mod scalar;

pub(crate) use error::{EvalError, InvalidArgumentSnafu, OptimizeSnafu};
pub(crate) use func::{BinaryFunc, UnaryFunc, UnmaterializableFunc, VariadicFunc};
pub(crate) use id::{GlobalId, Id, LocalId};
pub(crate) use linear::{MapFilterProject, MfpPlan, SafeMfpPlan};
pub(crate) use relation::{AggregateExpr, AggregateFunc};
pub(crate) use scalar::ScalarExpr;
