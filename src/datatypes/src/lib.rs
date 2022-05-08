#![feature(generic_associated_types)]

use arrow::array::{BinaryArray, MutableBinaryArray};

mod data_type;
pub mod prelude;
mod scalars;
pub mod type_id;
mod types;
pub mod value;
pub mod vectors;

pub type LargeBinaryArray = BinaryArray<i64>;
pub type MutableLargeBinaryArray = MutableBinaryArray<i64>;
pub mod schema;

pub mod deserialize;
pub mod serialize;

pub mod errors;
