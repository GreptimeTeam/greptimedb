// Copyright 2023-2026 GrepTime Inc.
//
// This file is part of the GreptimeDB Enterprise Edition and is licensed under
// the GreptimeDB Enterprise License. You may not use this file except in
// compliance with that license. A copy of the license is available at the root
// of this repository in the file LICENSE-ENTERPRISE.
//
// Unless required by applicable law or agreed to in writing, this software is
// distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied.

use std::fmt::{Display, Formatter};

use serde::Serialize;
use sqlparser_derive::{Visit, VisitMut};

use crate::ast::{Ident, ObjectName};
use crate::statements::OptionMap;

/// `CREATE BULK LOAD` statement.
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut, Serialize)]
pub struct CreateBulkLoad {
    pub job_id: Ident,
    pub table_name: ObjectName,
    pub staging_uri: String,
    pub options: OptionMap,
}

impl Display for CreateBulkLoad {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CREATE BULK LOAD {} INTO {} FROM '{}'",
            self.job_id,
            self.table_name,
            self.staging_uri.replace('\'', "''")
        )?;
        if !self.options.is_empty() {
            write!(f, " WITH ({})", self.options.kv_pairs().join(", "))?;
        }
        Ok(())
    }
}
