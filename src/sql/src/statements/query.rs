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

use std::fmt;

use datatypes::prelude::ConcreteDataType;
use sqlparser::ast::Query as SpQuery;

use crate::error::Error;

/// Query statement instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Query {
    pub inner: SpQuery,
    pub param_types: Vec<ConcreteDataType>,
}

/// Automatically converts from sqlparser Query instance to SqlQuery.
impl TryFrom<SpQuery> for Query {
    type Error = Error;

    fn try_from(q: SpQuery) -> Result<Self, Self::Error> {
        Ok(Query {
            inner: q,
            param_types: vec![],
        })
    }
}

impl TryFrom<Query> for SpQuery {
    type Error = Error;

    fn try_from(value: Query) -> Result<Self, Self::Error> {
        Ok(value.inner)
    }
}

impl Query {
    pub fn param_types(&self) -> &Vec<ConcreteDataType> {
        &self.param_types
    }

    pub fn param_types_mut(&mut self) -> &mut Vec<ConcreteDataType> {
        &mut self.param_types
    }
}

impl fmt::Display for Query {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ", self.inner)?;
        write!(f, "[")?;
        for i in 0..self.param_types.len() {
            write!(f, "{}", self.param_types[i])?;
            if i != self.param_types.len() - 1 {
                write!(f, ",")?;
            }
        }
        write!(f, "]")?;
        Ok(())
    }
}
