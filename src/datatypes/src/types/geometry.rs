use serde::{Deserialize, Serialize};

use crate::data_type::DataType;
use crate::prelude::{DataTypeRef, LogicalTypeId, Value};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GeometryType {
    Point,
}

impl DataType for GeometryType {
    fn name(&self) -> &str {
        "Geometry"
    }

    fn logical_type_id(&self) -> crate::type_id::LogicalTypeId {
        LogicalTypeId::Geometry
    }

    fn default_value(&self) -> crate::value::Value {
        todo!()
    }

    fn as_arrow_type(&self) -> arrow::datatypes::DataType {
        unimplemented!()
    }

    fn create_mutable_vector(&self, capacity: usize) -> Box<dyn crate::vectors::MutableVector> {
        todo!()
    }
}
