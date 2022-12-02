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

use sqlparser::ast::Query as SpQuery;

use crate::errors::ParserError;

/// Query statement instance.
#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    pub inner: SpQuery,
}

/// Automatically converts from sqlparser Query instance to SqlQuery.
impl TryFrom<SpQuery> for Query {
    type Error = ParserError;

    fn try_from(q: SpQuery) -> Result<Self, Self::Error> {
        Ok(Query { inner: q })
    }
}

impl TryFrom<Query> for SpQuery {
    type Error = ParserError;

    fn try_from(value: Query) -> Result<Self, Self::Error> {
        Ok(value.inner)
    }
}
