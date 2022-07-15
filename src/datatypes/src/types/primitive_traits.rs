use arrow::compute::arithmetics::basic::NativeArithmetics;
use arrow::types::NativeType;
use num::NumCast;

use crate::value::Value;

/// Primitive type.
pub trait Primitive:
    PartialOrd
    + Default
    + Clone
    + Copy
    + Into<Value>
    + NativeType
    + serde::Serialize
    + NativeArithmetics
    + NumCast
{
    /// Largest numeric type this primitive type can be cast to.
    type LargestType: Primitive;
}

macro_rules! impl_primitive {
    ($Type:ident, $LargestType: ident) => {
        impl Primitive for $Type {
            type LargestType = $LargestType;
        }
    };
}

impl_primitive!(u8, u64);
impl_primitive!(u16, u64);
impl_primitive!(u32, u64);
impl_primitive!(u64, u64);
impl_primitive!(i8, i64);
impl_primitive!(i16, i64);
impl_primitive!(i32, i64);
impl_primitive!(i64, i64);
impl_primitive!(f32, f64);
impl_primitive!(f64, f64);
