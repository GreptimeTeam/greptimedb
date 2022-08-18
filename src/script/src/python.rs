//! Python script coprocessor

mod coprocessor;
mod error;
mod modules;
pub(crate) mod py_utils;
#[cfg(test)]
mod test;
mod vector;

pub(crate) use py_utils::is_instance;

use self::coprocessor::AnnotationInfo;
pub use self::coprocessor::CoprEngine;
pub use self::coprocessor::{exec_copr_print, exec_coprocessor};
pub use self::vector::PyVector;
