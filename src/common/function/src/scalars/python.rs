//! python udf supports
//! use the function `coprocessor` to parse and run a python function with arguments from recordBatch, and return a newly assembled RecordBatch
mod copr_parse;
mod coprocessor;
mod error;
#[cfg(test)]
mod test;
mod type_;

pub use coprocessor::exec_coprocessor;
use coprocessor::AnnotationInfo;