use datafusion_expr::ColumnarValue as DfColumnarValue;
use datatypes::prelude::ConcreteDataType;
use datatypes::vectors;
use datatypes::vectors::VectorRef;
use snafu::ResultExt;

use crate::error::{IntoVectorSnafu, Result};
use crate::prelude::ScalarValue;

/// Represents the result from an expression
#[derive(Clone)]
pub enum ColumnarValue {
    Vector(VectorRef),
    /// A single value
    Scalar(ScalarValue),
}

impl ColumnarValue {
    pub fn data_type(&self) -> ConcreteDataType {
        match self {
            ColumnarValue::Vector(vector) => vector.data_type(),
            ColumnarValue::Scalar(scalar_value) => {
                ConcreteDataType::from_arrow_type(&scalar_value.get_datatype())
            }
        }
    }

    /// Convert a columnar value into an VectorRef
    pub fn try_into_vector(self, num_rows: usize) -> Result<VectorRef> {
        Ok(match self {
            ColumnarValue::Vector(v) => v,
            ColumnarValue::Scalar(s) => {
                let v = s.to_array_of_size(num_rows);
                let data_type = v.data_type().clone();
                vectors::try_into_vector(v).with_context(|_| IntoVectorSnafu { data_type })?
            }
        })
    }

    pub fn try_from_df_columnar_value(value: &DfColumnarValue) -> Result<ColumnarValue> {
        Ok(match value {
            DfColumnarValue::Scalar(v) => ColumnarValue::Scalar(v.clone()),
            DfColumnarValue::Array(v) => {
                ColumnarValue::Vector(vectors::try_into_vector(v.clone()).with_context(|_| {
                    IntoVectorSnafu {
                        data_type: v.data_type().clone(),
                    }
                })?)
            }
        })
    }
}

impl From<ColumnarValue> for DfColumnarValue {
    fn from(columnar_value: ColumnarValue) -> Self {
        match columnar_value {
            ColumnarValue::Scalar(v) => DfColumnarValue::Scalar(v),
            ColumnarValue::Vector(v) => DfColumnarValue::Array(v.to_arrow_array()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::DataType as ArrowDataType;
    use datatypes::vectors::BooleanVector;

    use super::*;

    #[test]
    fn test_scalar_columnar_value() {
        let value = ColumnarValue::Scalar(ScalarValue::Boolean(Some(true)));

        assert_eq!(ConcreteDataType::boolean_datatype(), value.data_type());
        let v = value.clone().try_into_vector(1).unwrap();
        assert_eq!(1, v.len());

        let df_value = DfColumnarValue::from(value);
        assert_eq!(ArrowDataType::Boolean, df_value.data_type());
    }

    #[test]
    fn test_vector_columnar_value() {
        let value = ColumnarValue::Vector(Arc::new(BooleanVector::from(vec![true, false, true])));

        assert_eq!(ConcreteDataType::boolean_datatype(), value.data_type());
        let v = value.clone().try_into_vector(1).unwrap();
        assert_eq!(3, v.len());

        let df_value = DfColumnarValue::from(value);
        assert_eq!(ArrowDataType::Boolean, df_value.data_type());
    }
}
