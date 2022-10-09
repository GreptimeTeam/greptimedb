// use arrow::array::{FixedSizeListArray, MutableFixedSizeListArray};
// use geo::Point;

// use crate::{
//     prelude::{ScalarVector, ScalarVectorBuilder, Vector},
//     vectors::MutableVector,
// };
// #[derive(Debug, Clone, PartialEq)]
// struct PointVector {
//     array: FixedSizeListArray,
// }

// impl Vector for PointVector {
//     fn data_type(&self) -> crate::data_type::ConcreteDataType {
//         todo!()
//     }

//     fn vector_type_name(&self) -> String {
//         todo!()
//     }

//     fn as_any(&self) -> &dyn std::any::Any {
//         todo!()
//     }

//     fn len(&self) -> usize {
//         todo!()
//     }

//     fn to_arrow_array(&self) -> arrow::array::ArrayRef {
//         todo!()
//     }

//     fn to_boxed_arrow_array(&self) -> Box<dyn arrow::array::Array> {
//         todo!()
//     }

//     fn validity(&self) -> crate::vectors::Validity {
//         todo!()
//     }

//     fn memory_size(&self) -> usize {
//         todo!()
//     }

//     fn is_null(&self, row: usize) -> bool {
//         todo!()
//     }

//     fn slice(&self, offset: usize, length: usize) -> crate::vectors::VectorRef {
//         todo!()
//     }

//     fn get(&self, index: usize) -> crate::value::Value {
//         todo!()
//     }

//     fn get_ref(&self, index: usize) -> crate::value::ValueRef {
//         todo!()
//     }

//     fn is_empty(&self) -> bool {
//         self.len() == 0
//     }

//     fn null_count(&self) -> usize {
//         match self.validity() {
//             crate::vectors::Validity::Slots(bitmap) => bitmap.null_count(),
//             crate::vectors::Validity::AllValid => 0,
//             crate::vectors::Validity::AllNull => self.len(),
//         }
//     }

//     fn is_const(&self) -> bool {
//         false
//     }

//     fn only_null(&self) -> bool {
//         self.null_count() == self.len()
//     }

//     fn try_get(&self, index: usize) -> crate::Result<crate::value::Value> {
//         ensure!(
//             index < self.len(),
//             error::BadArrayAccessSnafu {
//                 index,
//                 size: self.len()
//             }
//         );
//         Ok(self.get(index))
//     }
// }

// impl ScalarVector for PointVector {
//     type OwnedItem;

//     type RefItem<'a>
//     where
//         Self: 'a;

//     type Iter<'a>
//     where
//         Self: 'a;

//     type Builder;

//     fn get_data(&self, idx: usize) -> Option<Self::RefItem<'_>> {
//         todo!()
//     }

//     fn iter_data(&self) -> Self::Iter<'_> {
//         todo!()
//     }

//     fn from_slice(data: &[Self::RefItem<'_>]) -> Self {
//         let mut builder = Self::Builder::with_capacity(data.len());
//         for item in data {
//             builder.push(Some(*item));
//         }
//         builder.finish()
//     }

//     fn from_iterator<'a>(it: impl Iterator<Item = Self::RefItem<'a>>) -> Self {
//         let mut builder = Self::Builder::with_capacity(get_iter_capacity(&it));
//         for item in it {
//             builder.push(Some(item));
//         }
//         builder.finish()
//     }

//     fn from_owned_iterator(it: impl Iterator<Item = Option<Self::OwnedItem>>) -> Self {
//         let mut builder = Self::Builder::with_capacity(get_iter_capacity(&it));
//         for item in it {
//             match item {
//                 Some(item) => builder.push(Some(item.as_scalar_ref())),
//                 None => builder.push(None),
//             }
//         }
//         builder.finish()
//     }

//     fn from_vec<I: Into<Self::OwnedItem>>(values: Vec<I>) -> Self {
//         let it = values.into_iter();
//         let mut builder = Self::Builder::with_capacity(get_iter_capacity(&it));
//         for item in it {
//             builder.push(Some(item.into().as_scalar_ref()));
//         }
//         builder.finish()
//     }
// }

// struct PointVectorBuilder {
//     buffer: MutableFixedSizeListArray<f64>,
// }

// impl MutableVector for PointVectorBuilder {}

// impl ScalarVectorBuilder for PointVectorBuilder {
//     type VectorType = PointVector;

//     fn with_capacity(capacity: usize) -> Self {
//         todo!()
//     }

//     fn push(
//         &mut self,
//         value: Option<<Self::VectorType as crate::prelude::ScalarVector>::RefItem<'_>>,
//     ) {
//     }

//     fn finish(&mut self) -> Self::VectorType {
//         todo!()
//     }
// }
