pub use datafusion_common::ScalarValue;
use datatypes::prelude::ConcreteDataType;
use datatypes::vectors;
use datatypes::vectors::VectorRef;

/// Represents the result from an expression
/// TODO(dennis): move to sub mod columnar_value
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
    pub fn into_array(self, num_rows: usize) -> VectorRef {
        match self {
            ColumnarValue::Vector(v) => v,
            ColumnarValue::Scalar(s) => vectors::try_into_vector(s.to_array_of_size(num_rows))
                .expect("fail to cast into vector"),
        }
    }
}
