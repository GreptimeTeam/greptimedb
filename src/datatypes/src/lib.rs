mod data_type;
pub mod prelude;
mod schema;
pub mod type_id;
mod types;
pub mod value;
pub mod vectors;

use arrow2::array::BinaryArray;

pub type LargeBinaryArray = BinaryArray<i64>;
