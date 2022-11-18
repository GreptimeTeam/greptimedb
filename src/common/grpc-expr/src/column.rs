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
