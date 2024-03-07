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

use partition::partition::PartitionDef;

use crate::ir::{Column, CreateTableExpr, Ident};

pub type TableContextRef = Arc<TableContext>;

/// TableContext stores table info.
pub struct TableContext {
    pub name: Ident,
    pub columns: Vec<Column>,

    // GreptimeDB specific options
    pub partition: Option<PartitionDef>,
    pub primary_keys: Vec<usize>,
}

impl From<&CreateTableExpr> for TableContext {
    fn from(
        CreateTableExpr {
            table_name: name,
            columns,
            partition,
            primary_keys,
            ..
        }: &CreateTableExpr,
    ) -> Self {
        Self {
            name: name.clone(),
            columns: columns.clone(),
            partition: partition.clone(),
            primary_keys: primary_keys.clone(),
        }
    }
}
