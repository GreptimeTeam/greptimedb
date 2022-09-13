use std::sync::Arc;

use arrow::datatypes::DataType as ArrowDataType;
use serde::{Deserialize, Serialize};

use crate::data_type::{DataType, DataTypeRef};
use crate::type_id::LogicalTypeId;
use crate::value::Value;
use crate::vectors::{MutableVector, NullVectorBuilder};

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NullType;

impl NullType {
    pub fn arc() -> DataTypeRef {
        Arc::new(Self)
    }
}

impl DataType for NullType {
    fn name(&self) -> &str {
        "Null"
    }

    fn logical_type_id(&self) -> LogicalTypeId {
        LogicalTypeId::Null
    }

    fn default_value(&self) -> Value {
        Value::Null
    }

    fn as_arrow_type(&self) -> ArrowDataType {
        ArrowDataType::Null
    }

    fn create_mutable_vector(&self, _capacity: usize) -> Box<dyn MutableVector> {
        Box::new(NullVectorBuilder::default())
    }
}
