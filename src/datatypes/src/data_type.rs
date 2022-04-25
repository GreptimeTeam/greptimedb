use std::sync::Arc;

use arrow2::datatypes::DataType as ArrowDataType;

use crate::type_id::LogicalTypeId;
use crate::value::Value;

/// Data type abstraction.
pub trait DataType: std::fmt::Debug + Send + Sync {
    /// Name of this data type.
    fn name(&self) -> &str;

    /// Returns id of the Logical data type.
    fn logical_type_id(&self) -> LogicalTypeId;

    /// Returns the default value of this type.
    fn default_value(&self) -> Value;

    /// Convert this type as [arrow2::datatypes::DataType].
    fn as_arrow_type(&self) -> ArrowDataType;
}

pub type DataTypeRef = Arc<dyn DataType>;
