use std::sync::Arc;

use arrow::datatypes::DataType as ArrowDataType;
use serde::{Deserialize, Serialize};

use crate::data_type::{DataType, DataTypeRef};
use crate::prelude::{LogicalTypeId, Value};

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DateTimeType;

const DATE_TIME_TYPE_NAME: &str = "DateTime";

/// [DateTimeType] represents the seconds elapsed since UNIX EPOCH.
impl DataType for DateTimeType {
    fn name(&self) -> &str {
        DATE_TIME_TYPE_NAME
    }

    fn logical_type_id(&self) -> LogicalTypeId {
        LogicalTypeId::DateTime
    }

    fn default_value(&self) -> Value {
        Value::DateTime(Default::default())
    }

    fn as_arrow_type(&self) -> ArrowDataType {
        ArrowDataType::Date64
    }
}

impl DateTimeType {
    pub fn arc() -> DataTypeRef {
        Arc::new(Self)
    }

    pub fn name() -> &'static str {
        DATE_TIME_TYPE_NAME
    }
}
