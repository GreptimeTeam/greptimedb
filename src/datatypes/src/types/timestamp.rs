use arrow::datatypes::{DataType as ArrowDataType, TimeUnit as ArrowTimeUnit};
use common_time::timestamp::{TimeUnit, Timestamp};
use serde::{Deserialize, Serialize};

use crate::data_type::DataType;
use crate::prelude::{LogicalTypeId, MutableVector, ScalarVectorBuilder, Value};
use crate::vectors::TimestampVectorBuilder;

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
        LogicalTypeId::Timestamp
    }

    fn default_value(&self) -> Value {
        Value::Timestamp(Timestamp::new(0, self.unit))
    }

    fn as_arrow_type(&self) -> ArrowDataType {
        match self.unit {
            TimeUnit::Second => ArrowDataType::Timestamp(ArrowTimeUnit::Second, None),
            TimeUnit::Millisecond => ArrowDataType::Timestamp(ArrowTimeUnit::Millisecond, None),
            TimeUnit::Microsecond => ArrowDataType::Timestamp(ArrowTimeUnit::Microsecond, None),
            TimeUnit::Nanosecond => ArrowDataType::Timestamp(ArrowTimeUnit::Nanosecond, None),
        }
    }

    fn create_mutable_vector(&self, capacity: usize) -> Box<dyn MutableVector> {
        Box::new(TimestampVectorBuilder::with_capacity(capacity))
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::TimeUnit as ArrowTimeUnit;
    use common_time::timestamp::TimeUnit::Microsecond;

    use super::*;
    use crate::prelude::{ConcreteDataType, ValueRef};

    #[test]
    pub fn test_timestamp_type() {
        assert_eq!(
            LogicalTypeId::Timestamp,
            TimestampType::new(TimeUnit::Microsecond).logical_type_id()
        );
    }

    #[test]
    pub fn test_as_arrow_type() {
        assert_eq!(
            ArrowDataType::Timestamp(ArrowTimeUnit::Nanosecond, None),
            TimestampType::new(TimeUnit::Nanosecond).as_arrow_type()
        );
        assert_eq!(
            ArrowDataType::Timestamp(ArrowTimeUnit::Microsecond, None),
            TimestampType::new(TimeUnit::Microsecond).as_arrow_type()
        );
        assert_eq!(
            ArrowDataType::Timestamp(ArrowTimeUnit::Millisecond, None),
            TimestampType::new(TimeUnit::Millisecond).as_arrow_type()
        );
        assert_eq!(
            ArrowDataType::Timestamp(ArrowTimeUnit::Second, None),
            TimestampType::new(TimeUnit::Second).as_arrow_type()
        );
    }

    #[test]
    pub fn test_default_value() {
        assert_eq!(
            Value::Timestamp(Timestamp::new(0, Microsecond)),
            TimestampType::new(TimeUnit::Microsecond).default_value()
        );
    }

    #[test]
    pub fn test_create_mutable_vector() {
        let mut builder = TimestampType::new(TimeUnit::Microsecond).create_mutable_vector(10);
        builder
            .push_value_ref(ValueRef::Timestamp(Timestamp::new(
                42,
                TimeUnit::Millisecond,
            )))
            .unwrap();
        builder.push_value_ref(ValueRef::Null).unwrap();
        builder
            .push_value_ref(ValueRef::Timestamp(Timestamp::new(96, TimeUnit::Second)))
            .unwrap();
        let v = builder.to_vector();
        assert_eq!(ConcreteDataType::timestamp_millis_datatype(), v.data_type());
        assert_eq!(Value::Timestamp(Timestamp::from_millis(42)), v.get(0));
        assert_eq!(Value::Null, v.get(1));
        // Push a timestamp with different unit will convert the value to value with time unit millisecond.
        assert_eq!(Value::Timestamp(Timestamp::from_millis(96_000)), v.get(2));
    }
}
