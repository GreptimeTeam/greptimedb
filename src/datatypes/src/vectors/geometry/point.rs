use std::sync::Arc;

use arrow::array::{
    Array, FixedSizeListArray, Float64Vec, ListArray, MutableArray, MutableFixedSizeListArray,
    PrimitiveArray, StructArray,
};
use arrow::datatypes::DataType::{self, Float64, List};
use arrow::datatypes::Field;
use geo::Point;

use crate::value::{GeometryValue, OrderedF64, Value, ValueRef};
use crate::vectors::impl_validity_for_vector;
use crate::{
    prelude::{ScalarVector, ScalarVectorBuilder, Validity, Vector},
    vectors::MutableVector,
};
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
        let ref_x_array = self
            .array
            .values()
            .get(0)
            .unwrap()
            .as_any()
            .downcast_ref::<PrimitiveArray<f64>>()
            .unwrap();

        let validity = ref_x_array.validity();

        if let Some(bm) = validity {
            if !bm.get_bit(index) {
                return Value::Null;
            }
        }

        let ref_y_array = self
            .array
            .values()
            .get(1)
            .unwrap()
            .as_any()
            .downcast_ref::<PrimitiveArray<f64>>()
            .unwrap();

        let (x, y) = (ref_x_array.value(index), ref_x_array.value(index));
        let geo_value = GeometryValue::Point(Point::<OrderedF64>::new(x.into(), y.into()));
        Value::Geometry(geo_value)
    }

    pub fn get_ref(&self, index: usize) -> ValueRef {
        if let Value::Geometry(geo_value) = self.get(index) {
            ValueRef::Geometry(geo_value)
        } else {
            //NUll case
            ValueRef::Null
        }
    }
}

pub struct PointVectorBuilder {
    //pub array: MutableFixedSizeListArray,
    pub array_x: Float64Vec,
    pub array_y: Float64Vec,
}

impl PointVectorBuilder {
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

        let array = StructArray::new(DataType::Struct(fields), vec![x, y], None);
        //how to get validity of struct?

        PointVector { array }
    }
}
