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

use std::collections::HashMap;
use std::fmt::Display;

use datatypes::value::Value;
use derive_builder::Builder;
use partition::expr::PartitionExpr;
use serde::{Deserialize, Serialize};

use crate::ir::{Column, Ident};

/// The column options
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum ColumnOption {
    Null,
    NotNull,
    DefaultValue(Value),
    DefaultFn(String),
    TimeIndex,
    PrimaryKey,
}

impl Display for ColumnOption {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ColumnOption::Null => write!(f, "NULL"),
            ColumnOption::NotNull => write!(f, "NOT NULL"),
            ColumnOption::DefaultFn(s) => write!(f, "DEFAULT {}", s),
            ColumnOption::DefaultValue(s) => match s {
                Value::String(value) => {
                    write!(f, "DEFAULT \'{}\'", value.as_utf8())
                }
                _ => write!(f, "DEFAULT {}", s),
            },
            ColumnOption::TimeIndex => write!(f, "TIME INDEX"),
            ColumnOption::PrimaryKey => write!(f, "PRIMARY KEY"),
        }
    }
}

/// A naive create table expr builder.
#[derive(Debug, Builder, Clone, Serialize, Deserialize)]
pub struct CreateTableExpr {
    #[builder(setter(into))]
    pub table_name: Ident,
    pub columns: Vec<Column>,
    #[builder(default)]
    pub if_not_exists: bool,

    // GreptimeDB specific options
    #[builder(default, setter(into))]
    pub partition: Option<PartitionDef>,
    #[builder(default, setter(into))]
    pub engine: String,
    #[builder(default, setter(into))]
    pub options: HashMap<String, Value>,
    pub primary_keys: Vec<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionDef {
    pub columns: Vec<Ident>,
    pub exprs: Vec<PartitionExpr>,
}

#[derive(Debug, Builder, Clone, Serialize, Deserialize)]
pub struct CreateDatabaseExpr {
    #[builder(setter(into))]
    pub database_name: Ident,
    #[builder(default)]
    pub if_not_exists: bool,
}
