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

/// Convert [`ColumnDef`] to [`ColumnSchema`].
/// This function returns an error when failed to convert column data type or column default constraint.  
pub fn column_def_to_column_schema(column_def: &ColumnDef) -> Result<ColumnSchema> {
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

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use api::v1::ColumnDataType;
    use datatypes::prelude::ConcreteDataType;

    use super::*;

    #[test]
    fn test_column_def_to_column_schema() {
        let col_name = "mem_usage";
        let def = ColumnDef {
            name: col_name.to_string(),
            datatype: ColumnDataType::Float64 as i32,
            is_nullable: true,
            default_constraint: None,
        };
        let schema = column_def_to_column_schema(&def).unwrap();
        assert_eq!(col_name, schema.name);
        assert_eq!(ConcreteDataType::float64_datatype(), schema.data_type);
    }

    #[test]
    fn test_column_def_to_column_schema_with_default_constraint() {
        let col_name = "mem_usage";
        let def = ColumnDef {
            name: col_name.to_string(),
            datatype: ColumnDataType::Float64 as i32,
            is_nullable: true,
            default_constraint: Some(
                Vec::<u8>::try_from(ColumnDefaultConstraint::null_value()).unwrap(),
            ),
        };
        let schema = column_def_to_column_schema(&def).unwrap();
        assert_eq!(col_name, schema.name);
        assert_eq!(ConcreteDataType::float64_datatype(), schema.data_type);
        let default_constraint = schema.default_constraint().unwrap();
        assert_matches!(
            default_constraint,
            ColumnDefaultConstraint::Value(datatypes::value::Value::Null)
        )
    }
}
