use serde::{Deserialize, Serialize};

use crate::data_type::DataType;
use crate::prelude::{DataTypeRef, LogicalTypeId, Value};
use crate::value::GeometryValue;
use crate::vectors::geometry::GeometryVectorBuilder;

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
        match self {
            GeometryType::Point => GeometryValue::new_point(0.0, 0.0).to_value(),
        }
    }

    fn as_arrow_type(&self) -> arrow::datatypes::DataType {
        unreachable!()
    }

    fn create_mutable_vector(&self, capacity: usize) -> Box<dyn crate::vectors::MutableVector> {
        match self {
            GeometryType::Point => Box::new(
                GeometryVectorBuilder::with_capacity_point_vector_builder(capacity),
            ),
        }
    }
}
