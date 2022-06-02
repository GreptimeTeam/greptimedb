use std::iter::TrustedLen;

use arrow::bitmap::Bitmap;
use arrow::bitmap::MutableBitmap;
use crate::error::Result;

use crate::prelude::*;

pub trait ScalarViewer<'a>: Clone + Sized {
    type ScalarItem: Scalar<Viewer<'a> = Self>;
    type Iterator: Iterator<Item = <Self::ScalarItem as Scalar>::RefType<'a>>
        + ExactSizeIterator
        + TrustedLen;

    fn try_create(col: &'a VectorRef) -> Result<Self>;

    fn value_at(&self, index: usize) -> <Self::ScalarItem as Scalar>::RefType<'a>;

    fn valid_at(&self, i: usize) -> bool;

    /// len is implemented in ExactSizeIterator
    fn size(&self) -> usize;

    fn null_at(&self, i: usize) -> bool {
        !self.valid_at(i)
    }

    fn is_empty(&self) -> bool {
        self.size() == 0
    }

    fn iter(&self) -> Self::Iterator;
}

#[derive(Clone)]
pub struct PrimitiveViewer<'a, T: PrimitiveType> {
    pub(crate) values: &'a [T],
    // for not nullable vector, it's 0. we only need keep one sign bit to tell `null_at` that it's not null.
    // for nullable vector, it's usize::max, validity will be cloned from nullable vector.
    pub(crate) null_mask: usize,
    // for const vector, it's 0, `value` function will fetch the first value of the vector.
    // for not const vector, it's usize::max, `value` function will fetch the value of the row in the vector.
    pub(crate) non_const_mask: usize,
    pub(crate) size: usize,
    pub(crate) pos: usize,
    pub(crate) validity: Bitmap,
}

impl<'a, T> ScalarViewer<'a> for PrimitiveViewer<'a, T>
where
    T: Scalar<Viewer<'a> = Self> + PrimitiveType,
    T: ScalarRef<'a, ScalarType = T>,
    T: Scalar<RefType<'a> = T>,
{
    type ScalarItem = T;
    type Iterator = Self;

    fn try_create(vector: &'a VectorRef) -> Result<Self> {
        let (inner, validity) = try_extract_inner(vector)?;
        let col: &PrimitiveVector<T> = Series::check_get(inner)?;
        let values = col.values();

        let null_mask = get_null_mask(vector);
        let non_const_mask = non_const_mask(vector);
        let size = vector.len();

        Ok(Self {
            values,
            null_mask,
            non_const_mask,
            validity,
            size,
            pos: 0,
        })
    }

    #[inline]
    fn value_at(&self, index: usize) -> T {
        self.values[index & self.non_const_mask]
    }

    #[inline]
    fn valid_at(&self, i: usize) -> bool {
        unsafe { self.validity.get_bit_unchecked(i & self.null_mask) }
    }

    #[inline]
    fn size(&self) -> usize {
        self.size
    }

    fn iter(&self) -> Self {
        let mut res = self.clone();
        res.pos = 0;
        res
    }
}

#[derive(Clone)]
pub struct BooleanViewer {
    pub(crate) values: Bitmap,
    pub(crate) null_mask: usize,
    pub(crate) non_const_mask: usize,
    pub(crate) size: usize,
    pub(crate) pos: usize,
    pub(crate) validity: Bitmap,
}

impl<'a> ScalarViewer<'a> for BooleanViewer {
    type ScalarItem = bool;
    type Iterator = Self;

    fn try_create(vector: &VectorRef) -> Result<Self> {
        debug_assert!(!vector.is_empty());
        let (inner, validity) = try_extract_inner(vector)?;
        let col: &BooleanVector = Series::check_get(inner)?;
        let values = col.values().clone();

        let null_mask = get_null_mask(vector);
        let non_const_mask = non_const_mask(vector);
        let size = vector.len();

        Ok(Self {
            values,
            null_mask,
            non_const_mask,
            validity,
            size,
            pos: 0,
        })
    }

    #[inline]
    fn value_at(&self, index: usize) -> bool {
        self.values.get_bit(index & self.non_const_mask)
    }

    #[inline]
    fn valid_at(&self, i: usize) -> bool {
        unsafe { self.validity.get_bit_unchecked(i & self.null_mask) }
    }

    #[inline]
    fn size(&self) -> usize {
        self.size
    }

    fn iter(&self) -> Self {
        let mut res = self.clone();
        res.pos = 0;
        res
    }
}

#[derive(Clone)]
pub struct StringViewer<'a> {
    pub(crate) col: &'a StringVector,
    pub(crate) null_mask: usize,
    pub(crate) non_const_mask: usize,
    pub(crate) size: usize,
    pub(crate) pos: usize,
    pub(crate) validity: Bitmap,
}

impl<'a> ScalarViewer<'a> for StringViewer<'a> {
    type ScalarItem = Vu8;
    type Iterator = Self;

    fn try_create(vector: &'a VectorRef) -> Result<Self> {
        let (inner, validity) = try_extract_inner(vector)?;
        let col: &'a StringVector = Series::check_get(inner)?;

        let null_mask = get_null_mask(vector);
        let non_const_mask = non_const_mask(vector);
        let size = vector.len();

        Ok(Self {
            col,
            null_mask,
            non_const_mask,
            validity,
            size,
            pos: 0,
        })
    }

    #[inline]
    fn value_at(&self, index: usize) -> &'a [u8] {
        unsafe { self.col.value_unchecked(index & self.non_const_mask) }
    }

    #[inline]
    fn valid_at(&self, i: usize) -> bool {
        unsafe { self.validity.get_bit_unchecked(i & self.null_mask) }
    }

    #[inline]
    fn size(&self) -> usize {
        self.size
    }

    fn iter(&self) -> Self {
        let mut res = self.clone();
        res.pos = 0;
        res
    }
}

#[derive(Clone)]
pub struct ObjectViewer<'a, T: ObjectType> {
    pub(crate) values: &'a [T],
    pub(crate) null_mask: usize,
    pub(crate) non_const_mask: usize,
    pub(crate) size: usize,
    pub(crate) pos: usize,
    pub(crate) validity: Bitmap,
}

impl<'a, T> ScalarViewer<'a> for ObjectViewer<'a, T>
where T: Scalar<Viewer<'a> = Self> + ObjectType
{
    type ScalarItem = T;
    type Iterator = Self;

    fn try_create(vector: &'a VectorRef) -> Result<Self> {
        let (inner, validity) = try_extract_inner(vector)?;
        let col: &ObjectVector<T> = Series::check_get(inner)?;
        let values = col.values();

        let null_mask = get_null_mask(vector);
        let non_const_mask = non_const_mask(vector);
        let size = vector.len();

        Ok(Self {
            values,
            null_mask,
            non_const_mask,
            validity,
            size,
            pos: 0,
        })
    }

    #[inline]
    fn value_at(&self, index: usize) -> <Self::ScalarItem as Scalar>::RefType<'a> {
        self.values[index & self.non_const_mask].as_scalar_ref()
    }

    #[inline]
    fn valid_at(&self, i: usize) -> bool {
        unsafe { self.validity.get_bit_unchecked(i & self.null_mask) }
    }

    #[inline]
    fn size(&self) -> usize {
        self.size
    }

    fn iter(&self) -> Self {
        let mut res = self.clone();
        res.pos = 0;
        res
    }
}

#[derive(Clone)]
pub struct ArrayViewer<'a> {
    pub(crate) col: &'a ArrayVector,
    pub(crate) null_mask: usize,
    pub(crate) non_const_mask: usize,
    pub(crate) size: usize,
    pub(crate) pos: usize,
    pub(crate) validity: Bitmap,
}

impl<'a> ScalarViewer<'a> for ArrayViewer<'a> {
    type ScalarItem = ArrayValue;
    type Iterator = Self;

    fn try_create(vector: &'a VectorRef) -> Result<Self> {
        let (inner, validity) = try_extract_inner(vector)?;
        let col: &'a ArrayVector = Series::check_get(inner)?;

        let null_mask = get_null_mask(vector);
        let non_const_mask = non_const_mask(vector);
        let size = vector.len();

        Ok(Self {
            col,
            null_mask,
            non_const_mask,
            validity,
            size,
            pos: 0,
        })
    }

    #[inline]
    fn value_at(&self, index: usize) -> <Self::ScalarItem as Scalar>::RefType<'a> {
        ArrayValueRef::Indexed {
            vector: self.col,
            idx: index,
        }
    }

    #[inline]
    fn valid_at(&self, i: usize) -> bool {
        unsafe { self.validity.get_bit_unchecked(i & self.null_mask) }
    }

    #[inline]
    fn size(&self) -> usize {
        self.size
    }

    fn iter(&self) -> Self {
        let mut res = self.clone();
        res.pos = 0;
        res
    }
}

#[inline]
fn try_extract_inner(vector: &VectorRef) -> Result<(&VectorRef, Bitmap)> {
    let (all_is_null, validity) = vector.validity();
    let first_flag = if all_is_null {
        false
    } else {
        validity.map(|c| c.get_bit(0)).unwrap_or(true)
    };

    let (vector, validity) = if vector.is_const() {
        let mut bitmap = MutableBitmap::with_capacity(1);
        bitmap.push(first_flag);
        let c: &ConstVector = unsafe { Series::static_cast(vector) };
        (c.inner(), bitmap.into())
    } else if vector.is_nullable() {
        let c: &NullableVector = unsafe { Series::static_cast(vector) };
        (c.inner(), c.ensure_validity().clone())
    } else {
        let mut bitmap = MutableBitmap::with_capacity(1);
        bitmap.push(first_flag);
        (vector, bitmap.into())
    };

    // apply these twice to cover the cases: const(nullable)
    let vector: &VectorRef = if vector.is_const() {
        let vector: &ConstVector = unsafe { Series::static_cast(vector) };
        vector.inner()
    } else if vector.is_nullable() {
        let vector: &NullableVector = unsafe { Series::static_cast(vector) };
        vector.inner()
    } else {
        vector
    };
    Ok((vector, validity))
}

#[inline]
fn get_null_mask(vector: &VectorRef) -> usize {
    if !vector.is_const() && !vector.only_null() && vector.is_nullable() {
        usize::MAX
    } else {
        0
    }
}

#[inline]
fn non_const_mask(vector: &VectorRef) -> usize {
    if !vector.is_const() && !vector.only_null() {
        usize::MAX
    } else {
        0
    }
}
