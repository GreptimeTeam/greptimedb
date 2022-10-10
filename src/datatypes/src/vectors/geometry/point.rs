use arrow::array::{Array, FixedSizeListArray, ListArray, MutableFixedSizeListArray, StructArray};
use arrow::datatypes::DataType::List;
use geo::Point;

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
}
