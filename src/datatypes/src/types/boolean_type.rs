use std::sync::Arc;

use arrow::datatypes::DataType as ArrowDataType;
use serde::{Deserialize, Serialize};

use crate::data_type::{DataType, DataTypeRef};
use crate::scalars::ScalarVectorBuilder;
use crate::type_id::LogicalTypeId;
use crate::value::Value;
use crate::vectors::{BooleanVectorBuilder, MutableVector};

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BooleanType;

impl BooleanType {
    pub fn arc() -> DataTypeRef {
        Arc::new(Self)
    }
}

impl DataType for BooleanType {
    fn name(&self) -> &str {
        "Boolean"
    }

    fn logical_type_id(&self) -> LogicalTypeId {
        LogicalTypeId::Boolean
    }

    fn default_value(&self) -> Value {
        bool::default().into()
    }

    fn as_arrow_type(&self) -> ArrowDataType {
        ArrowDataType::Boolean
    }

    fn create_mutable_vector(&self, capacity: usize) -> Box<dyn MutableVector> {
        Box::new(BooleanVectorBuilder::with_capacity(capacity))
    }
}
