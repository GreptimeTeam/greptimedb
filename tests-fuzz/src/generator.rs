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

pub mod alter_expr;
pub mod create_expr;

use std::fmt;

use crate::error::Error;
use crate::ir::{AlterTableExpr, CreateTableExpr};

pub type CreateTableExprGenerator =
    Box<dyn Generator<CreateTableExpr, Error = Error> + Sync + Send>;

pub type AlterTableExprGenerator = Box<dyn Generator<AlterTableExpr, Error = Error> + Sync + Send>;

pub(crate) trait Generator<T> {
    type Error: Sync + Send + fmt::Debug;

    fn generate(&self) -> Result<T, Self::Error>;
}
