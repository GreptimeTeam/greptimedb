use arrow::datatypes::Field;
use serde::{Deserialize, Serialize};

use crate::arrow::datatypes::DataType::Float64;
use crate::data_type::DataType;
use crate::prelude::LogicalTypeId;
use crate::value::GeometryValue;
use crate::vectors::geometry::GeometryVectorBuilder;

const GEOMETRY_TYPE_NAME: &str = "Geometry";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GeometryType {
    Point,
}

impl Default for GeometryType {
    fn default() -> Self {
        Self::Point
    }
}

impl DataType for GeometryType {
    fn name(&self) -> &str {
        GEOMETRY_TYPE_NAME
    }

    fn logical_type_id(&self) -> crate::type_id::LogicalTypeId {
        LogicalTypeId::Geometry
    }

    fn default_value(&self) -> crate::value::Value {
        match self {
            GeometryType::Point => GeometryValue::new_point(0.0, 0.0).to_value(),
        }
    }

    // TODO: check if unreachable
    fn as_arrow_type(&self) -> arrow::datatypes::DataType {
        let fields = vec![
            Field::new("x", Float64, true),
            Field::new("y", Float64, true),
        ];
        arrow::datatypes::DataType::Struct(fields)
    }

    fn create_mutable_vector(&self, capacity: usize) -> Box<dyn crate::vectors::MutableVector> {
        match self {
            GeometryType::Point => Box::new(
                GeometryVectorBuilder::with_capacity_point_vector_builder(capacity),
            ),
        }
    }
}

impl GeometryType {
    pub fn name() -> &'static str {
        GEOMETRY_TYPE_NAME
    }
}
