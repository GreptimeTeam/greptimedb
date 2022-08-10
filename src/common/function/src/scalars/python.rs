//! python udf supports
//! use the function [`exec_coprocessor`](exec_coprocessor) to parse and run a python function with arguments from recordBatch, and return a newly assembled RecordBatch
mod copr_parse;
mod coprocessor;
mod error;
#[cfg(test)]
mod test;
mod type_;
mod copr_engine;

use coprocessor::AnnotationInfo;
pub use coprocessor::{exec_copr_print, exec_coprocessor};
