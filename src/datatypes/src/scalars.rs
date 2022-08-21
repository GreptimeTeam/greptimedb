use std::any::Any;

use crate::prelude::*;
use crate::vectors::date::DateVector;
use crate::vectors::*;

pub mod common;

fn get_iter_capacity<T, I: Iterator<Item = T>>(iter: &I) -> usize {
    match iter.size_hint() {
        (_lower, Some(upper)) => upper,
        (0, None) => 1024,
        (lower, None) => lower,
    }
}

/// Owned scalar value
/// primitive types, bool, Vec<u8> ...
pub trait Scalar: 'static + Sized + Default + Any
where
    for<'a> Self::VectorType: ScalarVector<RefItem<'a> = Self::RefType<'a>>,
{
    type VectorType: ScalarVector<OwnedItem = Self>;
    type RefType<'a>: ScalarRef<'a, ScalarType = Self, VectorType = Self::VectorType>
    where
        Self: 'a;
    /// Get a reference of the current value.
    fn as_scalar_ref(&self) -> Self::RefType<'_>;

    /// Upcast GAT type's lifetime.
    fn upcast_gat<'short, 'long: 'short>(long: Self::RefType<'long>) -> Self::RefType<'short>;
}

pub trait ScalarRef<'a>: std::fmt::Debug + Clone + Copy + Send + 'a {
    type VectorType: ScalarVector<RefItem<'a> = Self>;
    /// The corresponding [`Scalar`] type.
    type ScalarType: Scalar<RefType<'a> = Self>;

    /// Convert the reference into an owned value.
    fn to_owned_scalar(&self) -> Self::ScalarType;
}

/// A sub trait of Vector to add scalar operation support.
// This implementation refers to Datebend's [ScalarColumn](https://github.com/datafuselabs/databend/blob/main/common/datavalues/src/scalars/type_.rs)
// and skyzh's [type-exercise-in-rust](https://github.com/skyzh/type-exercise-in-rust).
pub trait ScalarVector: Vector + Send + Sync + Sized + 'static
where
    for<'a> Self::OwnedItem: Scalar<RefType<'a> = Self::RefItem<'a>>,
{
    type OwnedItem: Scalar<VectorType = Self>;
    /// The reference item of this vector.
    type RefItem<'a>: ScalarRef<'a, ScalarType = Self::OwnedItem, VectorType = Self>
    where
        Self: 'a;

    /// Iterator type of this vector.
    type Iter<'a>: Iterator<Item = Option<Self::RefItem<'a>>>
    where
        Self: 'a;

    /// Builder type to build this vector.
    type Builder: ScalarVectorBuilder<VectorType = Self>;

    /// Returns the reference to an element at given position.
    ///
    /// Note: `get()` has bad performance, avoid call this function inside loop.
    ///
    /// # Panics
    /// Panics if `idx >= self.len()`.
    fn get_data(&self, idx: usize) -> Option<Self::RefItem<'_>>;

    /// Returns iterator of current vector.
    fn iter_data(&self) -> Self::Iter<'_>;

    fn from_slice(data: &[Self::RefItem<'_>]) -> Self {
        let mut builder = Self::Builder::with_capacity(data.len());
        for item in data {
            builder.push(Some(*item));
        }
        builder.finish()
    }

    fn from_iterator<'a>(it: impl Iterator<Item = Self::RefItem<'a>>) -> Self {
        let mut builder = Self::Builder::with_capacity(get_iter_capacity(&it));
        for item in it {
            builder.push(Some(item));
        }
        builder.finish()
    }

    fn from_owned_iterator(it: impl Iterator<Item = Option<Self::OwnedItem>>) -> Self {
        let mut builder = Self::Builder::with_capacity(get_iter_capacity(&it));
        for item in it {
            match item {
                Some(item) => builder.push(Some(item.as_scalar_ref())),
                None => builder.push(None),
            }
        }
        builder.finish()
    }

    fn from_vecs(values: Vec<Self::OwnedItem>) -> Self {
        let it = values.iter();
        let mut builder = Self::Builder::with_capacity(get_iter_capacity(&it));
        for item in it {
            builder.push(Some(item.as_scalar_ref()));
        }
        builder.finish()
    }
}

/// A trait over all vector builders.
pub trait ScalarVectorBuilder: MutableVector {
    type VectorType: ScalarVector<Builder = Self>;

    /// Create a new builder with initial `capacity`.
    fn with_capacity(capacity: usize) -> Self;

    /// Push a value into the builder.
    fn push(&mut self, value: Option<<Self::VectorType as ScalarVector>::RefItem<'_>>);

    /// Finish build and return a new vector.
    fn finish(&mut self) -> Self::VectorType;
}

macro_rules! impl_primitive_scalar_type {
    ($native:ident) => {
        impl Scalar for $native {
            type VectorType = PrimitiveVector<$native>;
            type RefType<'a> = $native;

            #[inline]
            fn as_scalar_ref(&self) -> $native {
                *self
            }

            #[allow(clippy::needless_lifetimes)]
            #[inline]
            fn upcast_gat<'short, 'long: 'short>(long: $native) -> $native {
                long
            }
        }

        /// Implement [`ScalarRef`] for primitive types. Note that primitive types are both [`Scalar`] and [`ScalarRef`].
        impl<'a> ScalarRef<'a> for $native {
            type VectorType = PrimitiveVector<$native>;
            type ScalarType = $native;

            #[inline]
            fn to_owned_scalar(&self) -> $native {
                *self
            }
        }
    };
}

impl_primitive_scalar_type!(u8);
impl_primitive_scalar_type!(u16);
impl_primitive_scalar_type!(u32);
impl_primitive_scalar_type!(u64);
impl_primitive_scalar_type!(i8);
impl_primitive_scalar_type!(i16);
impl_primitive_scalar_type!(i32);
impl_primitive_scalar_type!(i64);
impl_primitive_scalar_type!(f32);
impl_primitive_scalar_type!(f64);

impl Scalar for bool {
    type VectorType = BooleanVector;
    type RefType<'a> = bool;

    #[inline]
    fn as_scalar_ref(&self) -> bool {
        *self
    }

    #[allow(clippy::needless_lifetimes)]
    #[inline]
    fn upcast_gat<'short, 'long: 'short>(long: bool) -> bool {
        long
    }
}

impl<'a> ScalarRef<'a> for bool {
    type VectorType = BooleanVector;
    type ScalarType = bool;

    #[inline]
    fn to_owned_scalar(&self) -> bool {
        *self
    }
}

impl Scalar for String {
    type VectorType = StringVector;
    type RefType<'a> = &'a str;

    #[inline]
    fn as_scalar_ref(&self) -> &str {
        self
    }

    #[inline]
    fn upcast_gat<'short, 'long: 'short>(long: &'long str) -> &'short str {
        long
    }
}

impl<'a> ScalarRef<'a> for &'a str {
    type VectorType = StringVector;
    type ScalarType = String;

    #[inline]
    fn to_owned_scalar(&self) -> String {
        self.to_string()
    }
}

impl Scalar for Vec<u8> {
    type VectorType = BinaryVector;
    type RefType<'a> = &'a [u8];

    #[inline]
    fn as_scalar_ref(&self) -> &[u8] {
        self
    }

    #[inline]
    fn upcast_gat<'short, 'long: 'short>(long: &'long [u8]) -> &'short [u8] {
        long
    }
}

impl<'a> ScalarRef<'a> for &'a [u8] {
    type VectorType = BinaryVector;
    type ScalarType = Vec<u8>;

    #[inline]
    fn to_owned_scalar(&self) -> Vec<u8> {
        self.to_vec()
    }
}

impl Scalar for common_time::date::Date {
    type VectorType = DateVector;
    type RefType<'a> = common_time::date::Date;

    fn as_scalar_ref(&self) -> Self::RefType<'_> {
        self.clone()
    }

    fn upcast_gat<'short, 'long: 'short>(long: Self::RefType<'long>) -> Self::RefType<'short> {
        long
    }
}

impl<'a> ScalarRef<'a> for common_time::date::Date {
    type VectorType = DateVector;
    type ScalarType = common_time::date::Date;

    fn to_owned_scalar(&self) -> Self::ScalarType {
        self.clone()
    }
}

#[cfg(test)]
mod tests {
    use common_time::date::Date;

    use super::*;
    use crate::vectors::binary::BinaryVector;
    use crate::vectors::primitive::Int32Vector;

    fn build_vector_from_slice<T: ScalarVector>(items: &[Option<T::RefItem<'_>>]) -> T {
        let mut builder = T::Builder::with_capacity(items.len());
        for item in items {
            builder.push(*item);
        }
        builder.finish()
    }

    fn assert_vector_eq<'a, T: ScalarVector>(expect: &[Option<T::RefItem<'a>>], vector: &'a T)
    where
        T::RefItem<'a>: PartialEq + std::fmt::Debug,
    {
        for (a, b) in expect.iter().zip(vector.iter_data()) {
            assert_eq!(*a, b);
        }
    }

    #[test]
    fn test_build_i32_vector() {
        let expect = vec![Some(1), Some(2), Some(3), None, Some(5)];
        let vector: Int32Vector = build_vector_from_slice(&expect);
        assert_vector_eq(&expect, &vector);
    }

    #[test]
    fn test_build_binary_vector() {
        let expect: Vec<Option<&'static [u8]>> = vec![
            Some(b"a"),
            Some(b"b"),
            Some(b"c"),
            None,
            Some(b"e"),
            Some(b""),
        ];
        let vector: BinaryVector = build_vector_from_slice(&expect);
        assert_vector_eq(&expect, &vector);
    }

    #[test]
    pub fn test_build_date_vector() {
        let expect: Vec<Option<Date>> = vec![
            Some(Date::try_new(0).unwrap()),
            Some(Date::try_new(-1).unwrap()),
            Some(Date::try_new(1).unwrap()),
            None,
            Some(Date::MAX),
            Some(Date::MIN),
        ];
        let vector: DateVector = build_vector_from_slice(&expect);
        assert_vector_eq(&expect, &vector);
    }
}
