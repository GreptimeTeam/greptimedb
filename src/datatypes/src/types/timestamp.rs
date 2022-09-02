use arrow::datatypes::{DataType as ArrowDataType, TimeUnit as ArrowTimeUnit};
use common_time::timestamp::{TimeUnit, Timestamp};
use serde::{Deserialize, Serialize};

use crate::data_type::DataType;
use crate::prelude::{LogicalTypeId, Value};

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimestampType {
    pub unit: TimeUnit,
}

impl TimestampType {
    pub fn new(unit: TimeUnit) -> Self {
        Self { unit }
    }
}

impl DataType for TimestampType {
    fn name(&self) -> &str {
        "Timestamp"
    }

    fn logical_type_id(&self) -> LogicalTypeId {
        LogicalTypeId::Timestamp(self.unit)
    }

    fn default_value(&self) -> Value {
        Value::Timestamp(Timestamp::default())
    }

    fn as_arrow_type(&self) -> ArrowDataType {
        match self.unit {
            TimeUnit::Second => ArrowDataType::Timestamp(ArrowTimeUnit::Second, None),
            TimeUnit::Millisecond => ArrowDataType::Timestamp(ArrowTimeUnit::Millisecond, None),
            TimeUnit::Microsecond => ArrowDataType::Timestamp(ArrowTimeUnit::Microsecond, None),
            TimeUnit::Nanosecond => ArrowDataType::Timestamp(ArrowTimeUnit::Nanosecond, None),
        }
    }
}
