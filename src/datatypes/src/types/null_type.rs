use std::sync::Arc;

use arrow::datatypes::DataType as ArrowDataType;

use crate::data_type::{DataType, DataTypeRef};
use crate::type_id::LogicalTypeId;
use crate::value::Value;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
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
}
