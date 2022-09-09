#![feature(generic_associated_types)]
#![feature(assert_matches)]

pub mod arrow_array;
pub mod compute;
pub mod data_type;
pub mod deserialize;
pub mod error;
pub mod macros;
pub mod prelude;
mod scalars;
pub mod schema;
pub mod serialize;
pub mod type_id;
pub mod types;
pub mod value;
pub mod vectors;

pub use arrow;
pub use error::{Error, Result};
