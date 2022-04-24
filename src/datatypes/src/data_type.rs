use std::sync::Arc;

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
}

pub type DataTypeRef = Arc<dyn DataType>;
