use std::cmp::Ordering;

use arrow::compute::arithmetics::basic::NativeArithmetics;
use arrow::types::NativeType;
use num::NumCast;

use crate::prelude::Scalar;
use crate::value::{IntoValueRef, Value};

/// Primitive type.
pub trait Primitive:
    PartialOrd
    + Default
    + Clone
    + Copy
    + Into<Value>
    + IntoValueRef<'static>
    + NativeType
    + serde::Serialize
    + NativeArithmetics
    + NumCast
    + Scalar
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

/// A new type for [Primitive], complement the `Ord` feature for it. Wrapping not ordered
/// primitive types like `f32` and `f64` in `OrdPrimitive` can make them be used in places that
/// require `Ord`. For example, in `Median` or `Percentile` UDAFs.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OrdPrimitive<T: Primitive>(pub T);

impl<T: Primitive> OrdPrimitive<T> {
    pub fn as_primitive(&self) -> T {
        self.0
    }
}

impl<T: Primitive> Eq for OrdPrimitive<T> {}

impl<T: Primitive> PartialOrd for OrdPrimitive<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Primitive> Ord for OrdPrimitive<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.into().cmp(&other.0.into())
    }
}

impl<T: Primitive> From<OrdPrimitive<T>> for Value {
    fn from(p: OrdPrimitive<T>) -> Self {
        p.0.into()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BinaryHeap;

    use super::*;

    #[test]
    fn test_ord_primitive() {
        struct Foo<T>
        where
            T: Primitive,
        {
            heap: BinaryHeap<OrdPrimitive<T>>,
        }

        impl<T> Foo<T>
        where
            T: Primitive,
        {
            fn push(&mut self, value: T) {
                let value = OrdPrimitive::<T>(value);
                self.heap.push(value);
            }
        }

        macro_rules! test {
            ($Type:ident) => {
                let mut foo = Foo::<$Type> {
                    heap: BinaryHeap::new(),
                };
                foo.push($Type::default());
            };
        }

        test!(u8);
        test!(u16);
        test!(u32);
        test!(u64);
        test!(i8);
        test!(i16);
        test!(i32);
        test!(i64);
        test!(f32);
        test!(f64);
    }
}
