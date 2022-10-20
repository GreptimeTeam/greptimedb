use std::sync::Arc;

use common_time::{util, Timestamp};
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};

use crate::data_type::{ConcreteDataType, DataType};
use crate::error::{self, Result};
use crate::scalars::ScalarVector;
use crate::value::Value;
use crate::vectors::{ConstantVector, TimestampVector, VectorRef};

const CURRENT_TIMESTAMP: &str = "current_timestamp()";

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
    /// Returns a default null constraint.
    pub fn null_value() -> ColumnDefaultConstraint {
        ColumnDefaultConstraint::Value(Value::Null)
    }

    /// Check whether the constraint is valid for columns with given `data_type`
    /// and `is_nullable` attributes.
    pub fn validate(&self, data_type: &ConcreteDataType, is_nullable: bool) -> Result<()> {
        ensure!(is_nullable || !self.maybe_null(), error::NullDefaultSnafu);

        match self {
            ColumnDefaultConstraint::Function(expr) => {
                ensure!(
                    expr == CURRENT_TIMESTAMP,
                    error::UnsupportedDefaultExprSnafu { expr }
                );
                ensure!(
                    data_type.is_timestamp(),
                    error::DefaultValueTypeSnafu {
                        reason: "return value of the function must has timestamp type",
                    }
                );
            }
            ColumnDefaultConstraint::Value(v) => {
                if !v.is_null() {
                    // Whether the value could be nullable has been checked before, only need
                    // to check the type compatibility here.
                    ensure!(
                        data_type.logical_type_id() == v.logical_type_id(),
                        error::DefaultValueTypeSnafu {
                            reason: format!(
                                "column has type {:?} but default value has type {:?}",
                                data_type.logical_type_id(),
                                v.logical_type_id()
                            ),
                        }
                    );
                }
            }
        }

        Ok(())
    }

    /// Create a vector that contains `num_rows` default values for given `data_type`.
    ///
    /// If `is_nullable` is `true`, then this method would returns error if the created
    /// default value is null.
    ///
    /// # Panics
    /// Panics if `num_rows == 0`.
    pub fn create_default_vector(
        &self,
        data_type: &ConcreteDataType,
        is_nullable: bool,
        num_rows: usize,
    ) -> Result<VectorRef> {
        assert!(num_rows > 0);

        match self {
            ColumnDefaultConstraint::Function(expr) => {
                // Functions should also ensure its return value is not null when
                // is_nullable is true.
                match &expr[..] {
                    // TODO(dennis): we only supports current_timestamp right now,
                    //   it's better to use a expression framework in future.
                    CURRENT_TIMESTAMP => {
                        // TODO(yingwen): We should coerce the type to the physical type of
                        // input `data_type`.
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
                ensure!(is_nullable || !v.is_null(), error::NullDefaultSnafu);

                // TODO(yingwen):
                // 1. For null value, we could use NullVector once it supports custom logical type.
                // 2. For non null value, we could use ConstantVector, but it would cause all codes
                //  attempt to downcast the vector fail if they don't check whether the vector is const
                //  first.
                let mut mutable_vector = data_type.create_mutable_vector(1);
                mutable_vector.push_value_ref(v.as_value_ref())?;
                let base_vector = mutable_vector.to_vector();
                Ok(base_vector.replicate(&[num_rows]))
            }
        }
    }

    /// Returns true if this constraint might creates NULL.
    fn maybe_null(&self) -> bool {
        // Once we support more functions, we may return true if given function
        // could return null.
        matches!(self, ColumnDefaultConstraint::Value(Value::Null))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vectors::Int32Vector;

    #[test]
    fn test_null_default_constraint() {
        let constraint = ColumnDefaultConstraint::null_value();
        assert!(constraint.maybe_null());
        let constraint = ColumnDefaultConstraint::Value(Value::Int32(10));
        assert!(!constraint.maybe_null());
    }

    #[test]
    fn test_validate_null_constraint() {
        let constraint = ColumnDefaultConstraint::null_value();
        let data_type = ConcreteDataType::int32_datatype();
        constraint.validate(&data_type, false).unwrap_err();
        constraint.validate(&data_type, true).unwrap();
    }

    #[test]
    fn test_validate_value_constraint() {
        let constraint = ColumnDefaultConstraint::Value(Value::Int32(10));
        let data_type = ConcreteDataType::int32_datatype();
        constraint.validate(&data_type, false).unwrap();
        constraint.validate(&data_type, true).unwrap();

        constraint
            .validate(&ConcreteDataType::uint32_datatype(), true)
            .unwrap_err();
    }

    #[test]
    fn test_validate_function_constraint() {
        let constraint = ColumnDefaultConstraint::Function(CURRENT_TIMESTAMP.to_string());
        constraint
            .validate(&ConcreteDataType::timestamp_millis_datatype(), false)
            .unwrap();
        constraint
            .validate(&ConcreteDataType::boolean_datatype(), false)
            .unwrap_err();

        let constraint = ColumnDefaultConstraint::Function("hello()".to_string());
        constraint
            .validate(&ConcreteDataType::timestamp_millis_datatype(), false)
            .unwrap_err();
    }

    #[test]
    fn test_create_default_vector_by_null() {
        let constraint = ColumnDefaultConstraint::null_value();
        let data_type = ConcreteDataType::int32_datatype();
        constraint
            .create_default_vector(&data_type, false, 10)
            .unwrap_err();

        let constraint = ColumnDefaultConstraint::null_value();
        let v = constraint
            .create_default_vector(&data_type, true, 3)
            .unwrap();
        assert_eq!(3, v.len());
        for i in 0..v.len() {
            assert_eq!(Value::Null, v.get(i));
        }
    }

    #[test]
    fn test_create_default_vector_by_value() {
        let constraint = ColumnDefaultConstraint::Value(Value::Int32(10));
        let data_type = ConcreteDataType::int32_datatype();
        let v = constraint
            .create_default_vector(&data_type, false, 4)
            .unwrap();
        let expect: VectorRef = Arc::new(Int32Vector::from_values(vec![10; 4]));
        assert_eq!(expect, v);
    }

    #[test]
    fn test_create_default_vector_by_func() {
        let constraint = ColumnDefaultConstraint::Function(CURRENT_TIMESTAMP.to_string());
        let data_type = ConcreteDataType::timestamp_millis_datatype();
        let v = constraint
            .create_default_vector(&data_type, false, 4)
            .unwrap();
        assert_eq!(4, v.len());
        assert!(
            matches!(v.get(0), Value::Timestamp(_)),
            "v {:?} is not timestamp",
            v.get(0)
        );

        let constraint = ColumnDefaultConstraint::Function("no".to_string());
        let data_type = ConcreteDataType::timestamp_millis_datatype();
        constraint
            .create_default_vector(&data_type, false, 4)
            .unwrap_err();
    }
}
