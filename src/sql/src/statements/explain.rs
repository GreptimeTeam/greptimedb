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

use sqlparser::ast::Statement;

/// Explain statement instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Explain {
    pub inner: Statement,
}

use crate::error::Error;

impl TryFrom<Statement> for Explain {
    type Error = Error;

    fn try_from(value: Statement) -> Result<Self, Self::Error> {
        Ok(Explain { inner: value })
    }
}
