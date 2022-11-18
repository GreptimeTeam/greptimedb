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

use api::helper::ColumnDataTypeWrapper;
use api::v1::ColumnDef;
use datatypes::schema::{ColumnDefaultConstraint, ColumnSchema};
use snafu::ResultExt;

use crate::error::{ColumnDataTypeSnafu, ColumnDefaultConstraintSnafu, Result};

pub fn create_column_schema(column_def: &ColumnDef) -> Result<ColumnSchema> {
    let data_type =
        ColumnDataTypeWrapper::try_new(column_def.datatype).context(ColumnDataTypeSnafu)?;
    let default_constraint = match &column_def.default_constraint {
        None => None,
        Some(v) => {
            Some(ColumnDefaultConstraint::try_from(&v[..]).context(ColumnDefaultConstraintSnafu)?)
        }
    };
    ColumnSchema::new(
        column_def.name.clone(),
        data_type.into(),
        column_def.is_nullable,
    )
    .with_default_constraint(default_constraint)
    .context(ColumnDefaultConstraintSnafu)
}
