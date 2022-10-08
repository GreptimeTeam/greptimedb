use std::sync::Arc;

use common_time::{util, Timestamp};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::data_type::{ConcreteDataType, DataType};
use crate::error::{self, Result};
use crate::scalars::ScalarVector;
use crate::value::Value;
use crate::vectors::{ConstantVector, TimestampVector, VectorRef};

/// Column's default constraint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColumnDefaultConstraint {
    // A function invocation
    // TODO(dennis): we save the function expression here, maybe use a struct in future.
    Function(String),
    // A value
    Value(Value),
}

impl TryFrom<&[u8]> for ColumnDefaultConstraint {
    type Error = error::Error;

    fn try_from(bytes: &[u8]) -> Result<Self> {
        let json = String::from_utf8_lossy(bytes);
        serde_json::from_str(&json).context(error::DeserializeSnafu { json })
    }
}

impl TryInto<Vec<u8>> for ColumnDefaultConstraint {
    type Error = error::Error;

    fn try_into(self) -> Result<Vec<u8>> {
        let s = serde_json::to_string(&self).context(error::SerializeSnafu)?;
        Ok(s.into_bytes())
    }
}

impl ColumnDefaultConstraint {
    /// Create a vector that contains `num_rows` default values for given `data_type`.
    ///
    /// # Panics
    /// Panics if `num_rows == 0`.
    pub fn create_default_vector(
        &self,
        data_type: &ConcreteDataType,
        num_rows: usize,
    ) -> Result<VectorRef> {
        assert!(num_rows > 0);

        match self {
            ColumnDefaultConstraint::Function(expr) => {
                match &expr[..] {
                    // TODO(dennis): we only supports current_timestamp right now,
                    //   it's better to use a expression framework in future.
                    "current_timestamp()" => {
                        let vector =
                            Arc::new(TimestampVector::from_slice(&[Timestamp::from_millis(
                                util::current_time_millis(),
                            )]));
                        Ok(Arc::new(ConstantVector::new(vector, num_rows)))
                    }
                    _ => error::UnsupportedDefaultExprSnafu { expr }.fail(),
                }
            }
            ColumnDefaultConstraint::Value(v) => {
                let mut mutable_vector = data_type.create_mutable_vector(1);
                mutable_vector.push_value_ref(v.as_value_ref())?;
                let vector = Arc::new(ConstantVector::new(mutable_vector.to_vector(), num_rows));
                Ok(vector)
            }
        }
    }
}
