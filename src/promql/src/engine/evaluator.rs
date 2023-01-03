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

use promql_parser::parser::{Expr, Value};

use crate::engine::Context;
use crate::error::Result;

/// An evaluator evaluates given expressions over given fixed timestamps.
pub struct Evaluator {}

impl Evaluator {
    pub fn eval(_ctx: &Context, _expr: &Expr) -> Result<Arc<dyn Value>> {
        unimplemented!();
    }
}
