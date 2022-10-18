use arrow::array::{Array, Float64Vec, MutableArray, PrimitiveArray, StructArray};
use arrow::datatypes::DataType::{self, Float64};
use arrow::datatypes::Field;

use crate::prelude::Validity;
use crate::value::{GeometryValue, Value};
use crate::vectors::impl_validity_for_vector;
#[derive(Debug, Clone, PartialEq)]
pub struct PointVector {
    pub array: StructArray,
}

impl PointVector {
    pub fn memory_size(&self) -> usize {
        2 * self.array.len() * std::mem::size_of::<f64>()
    }

    pub fn validity(&self) -> Validity {
        impl_validity_for_vector!(self.array)
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        return Self {
            array: self.array.slice(offset, length),
        };
    }

    pub fn get(&self, index: usize) -> Value {
        if self.array.is_null(index) {
            return Value::Null;
        }
        let ref_x_array = self
            .array
            .values()
            .get(0)
            .unwrap()
            .as_any()
            .downcast_ref::<PrimitiveArray<f64>>()
            .unwrap();

        let ref_y_array = self
            .array
            .values()
            .get(1)
            .unwrap()
            .as_any()
            .downcast_ref::<PrimitiveArray<f64>>()
            .unwrap();

        let (x, y) = (ref_x_array.value(index), ref_y_array.value(index));
        GeometryValue::new_point(x, y).to_value()
    }
}

pub struct PointVectorBuilder {
    //pub array: MutableFixedSizeListArray,
    pub array_x: Float64Vec,
    pub array_y: Float64Vec,
}

impl PointVectorBuilder {
    pub fn new() -> Self {
        Self {
            array_x: Float64Vec::new(),
            array_y: Float64Vec::new(),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            array_x: Float64Vec::with_capacity(capacity),
            array_y: Float64Vec::with_capacity(capacity),
        }
    }

    pub fn push(&mut self, value: Option<GeometryValue>) {
        match value {
            Some(val) => match val {
                GeometryValue::Point(xy) => {
                    self.array_x.push(Some(*xy.x()));
                    self.array_y.push(Some(*xy.y()));
                }
            },
            None => {
                self.array_x.push_null();
                self.array_y.push_null();
            }
        }
    }
    pub fn finish(&mut self) -> PointVector {
        let (x, y) = (self.array_x.as_arc(), self.array_y.as_arc());
        let fields = vec![
            Field::new("x", Float64, true),
            Field::new("y", Float64, true),
        ];
        let validity = x.validity().map(|validity| validity.clone());
        let array = StructArray::new(DataType::Struct(fields), vec![x, y], validity);

        PointVector { array }
    }
}
