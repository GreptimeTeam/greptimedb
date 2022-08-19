//! Python script coprocessor

mod builtins;
mod coprocessor;
mod engine;
mod error;
#[cfg(test)]
mod test;
pub(crate) mod utils;
mod vector;

use self::coprocessor::AnnotationInfo;
pub use self::coprocessor::{exec_copr_print, exec_coprocessor};
pub use self::vector::PyVector;
