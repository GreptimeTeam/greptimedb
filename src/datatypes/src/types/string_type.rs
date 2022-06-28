use std::sync::Arc;

use arrow::datatypes::DataType as ArrowDataType;
use common_base::bytes::StringBytes;

use crate::data_type::DataType;
use crate::prelude::{DataTypeRef, LogicalTypeId, Value};

#[derive(Debug, Default, Clone, PartialEq)]
pub struct StringType;

impl StringType {
    pub fn arc() -> DataTypeRef {
        Arc::new(Self)
    }
}

impl DataType for StringType {
    fn name(&self) -> &str {
        "String"
    }

    fn logical_type_id(&self) -> LogicalTypeId {
        LogicalTypeId::String
    }

    fn default_value(&self) -> Value {
        StringBytes::default().into()
    }

    fn as_arrow_type(&self) -> ArrowDataType {
        ArrowDataType::LargeUtf8
    }
}
