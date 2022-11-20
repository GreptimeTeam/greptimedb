use datatypes::schema::{ColumnDefaultConstraint, ColumnSchema};
use snafu::ResultExt;

use crate::error::{self, Result};
use crate::helper::ColumnDataTypeWrapper;
use crate::v1::ColumnDef;

impl ColumnDef {
    pub fn try_as_column_schema(&self) -> Result<ColumnSchema> {
        let data_type = ColumnDataTypeWrapper::try_new(self.datatype)?;

        let constraint = match &self.default_constraint {
            None => None,
            Some(v) => Some(
                ColumnDefaultConstraint::try_from(&v[..])
                    .context(error::ConvertColumnDefaultConstraintSnafu { column: &self.name })?,
            ),
        };

        ColumnSchema::new(&self.name, data_type.into(), self.is_nullable)
            .with_default_constraint(constraint)
            .context(error::InvalidColumnDefaultConstraintSnafu { column: &self.name })
    }
}
