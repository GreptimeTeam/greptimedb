//! basically a wrapper around the `datatype` crate
//! for basic Data Representation
use datatypes::value::Value;

/// A row is a vector of values.
///
/// TODO: use a more efficient representation
/// i.e. more compact like raw u8 of \[tag0, value0, tag1, value1, ...\]
pub type Row = Vec<Value>;
