use std::sync::Arc;

use arrow2::datatypes::DataType as ArrowDataType;
use common::bytes::StringBytes;

use crate::data_type::{DataType, DataTypeRef};
use crate::type_id::LogicalTypeId;
use crate::value::Value;

#[derive(Debug, Default)]
pub struct BinaryType;

impl BinaryType {
    pub fn arc() -> DataTypeRef {
        Arc::new(Self)
    }
}

impl DataType for BinaryType {
    fn name(&self) -> &str {
        "Binary"
    }

    fn logical_type_id(&self) -> LogicalTypeId {
        LogicalTypeId::String
    }

    fn default_value(&self) -> Value {
        StringBytes::default().into()
    }

    fn as_arrow_type(&self) -> ArrowDataType {
        ArrowDataType::LargeBinary
    }
}
