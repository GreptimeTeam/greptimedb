use arrow::array::{
    Array, FixedSizeListArray, Float64Vec, ListArray, MutableArray, MutableFixedSizeListArray,
    StructArray,
};
use arrow::datatypes::DataType::{self, Float64, List};
use arrow::datatypes::Field;
use geo::Point;

use crate::value::{GeometryValue, ValueRef};
use crate::{
    prelude::{ScalarVector, ScalarVectorBuilder, Vector},
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
}

pub struct PointVectorBuilder {
    //pub array: MutableFixedSizeListArray,
    pub array_x: Float64Vec,
    pub array_y: Float64Vec,
}

impl PointVectorBuilder {
    pub fn push(&mut self, value: Option<&GeometryValue>) {
        match value {
            Some(val) => match *val {
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
