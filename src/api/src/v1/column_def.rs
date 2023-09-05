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

use datatypes::schema::{ColumnDefaultConstraint, ColumnSchema};
use snafu::ResultExt;

use crate::error::{self, Result};
use crate::helper::ColumnDataTypeWrapper;
use crate::v1::ColumnDef;

pub fn try_as_column_schema(column_def: &ColumnDef) -> Result<ColumnSchema> {
    let data_type = ColumnDataTypeWrapper::try_new(column_def.data_type)?;

    let constraint = if column_def.default_constraint.is_empty() {
        None
    } else {
        Some(
            ColumnDefaultConstraint::try_from(column_def.default_constraint.as_slice()).context(
                error::ConvertColumnDefaultConstraintSnafu {
                    column: &column_def.name,
                },
            )?,
        )
    };

    ColumnSchema::new(&column_def.name, data_type.into(), column_def.is_nullable)
        .with_default_constraint(constraint)
        .context(error::InvalidColumnDefaultConstraintSnafu {
            column: &column_def.name,
        })
}
