use std::sync::Arc;

use arrow::datatypes::DataType as ArrowDataType;
use serde::{Deserialize, Serialize};

use crate::data_type::DataType;
use crate::prelude::{DataTypeRef, LogicalTypeId, Value};

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, Eq)]
pub struct DateType;

impl DataType for DateType {
    fn name(&self) -> &str {
        "Date"
    }

    fn logical_type_id(&self) -> LogicalTypeId {
        LogicalTypeId::Date
    }

    fn default_value(&self) -> Value {
        Value::Date(Default::default())
    }

    fn as_arrow_type(&self) -> ArrowDataType {
        ArrowDataType::Date32
    }
}

impl DateType {
    pub fn arc() -> DataTypeRef {
        Arc::new(Self)
    }
}
