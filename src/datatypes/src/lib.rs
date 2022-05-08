#![feature(generic_associated_types)]

mod data_type;
pub mod prelude;
mod scalars;
pub mod type_id;
mod types;
pub mod value;
pub mod vectors;

use arrow::array::{BinaryArray, MutableBinaryArray};

pub type LargeBinaryArray = BinaryArray<i64>;
pub type MutableLargeBinaryArray = MutableBinaryArray<i64>;
pub mod schema;
