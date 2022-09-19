use std::any::Any;
use std::ops::Range;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, ListArray};
use arrow::bitmap::utils::ZipValidity;
use arrow::bitmap::MutableBitmap;
use arrow::datatypes::DataType as ArrowDataType;
use serde_json::Value as JsonValue;
use snafu::prelude::*;

use crate::error::Result;
use crate::prelude::*;
use crate::serialize::Serializable;
use crate::types::ListType;
use crate::value::{ListValue, ListValueRef};
use crate::vectors::{impl_try_from_arrow_array_for_vector, impl_validity_for_vector};

type ArrowListArray = ListArray<i32>;

/// Vector of Lists, basically backed by Arrow's `ListArray`.
#[derive(Debug, Clone, PartialEq)]
pub struct ListVector {
    array: ArrowListArray,
    inner_datatype: ConcreteDataType,
}

impl ListVector {
    /// Only iterate values in the [ListVector].
    ///
    /// Be careful to use this method as it would ignore validity and replace null
    /// by empty vector.
    pub fn values_iter(&self) -> Box<dyn Iterator<Item = Result<VectorRef>> + '_> {
        Box::new(self.array.values_iter().map(VectorHelper::try_into_vector))
    }

    pub(crate) fn as_arrow(&self) -> &dyn Array {
        &self.array
    }
}

impl Vector for ListVector {
    fn data_type(&self) -> ConcreteDataType {
        ConcreteDataType::List(ListType::new(self.inner_datatype.clone()))
    }

    fn vector_type_name(&self) -> String {
        "ListVector".to_string()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn len(&self) -> usize {
        self.array.len()
    }

    fn to_arrow_array(&self) -> ArrayRef {
        Arc::new(self.array.clone())
    }

    fn to_boxed_arrow_array(&self) -> Box<dyn Array> {
        Box::new(self.array.clone())
    }

    fn validity(&self) -> Validity {
        impl_validity_for_vector!(self.array)
    }

    fn memory_size(&self) -> usize {
        let offsets_bytes = self.array.offsets().len() * std::mem::size_of::<i64>();
        let value_refs_bytes = self.array.values().len() * std::mem::size_of::<Arc<dyn Array>>();
        offsets_bytes + value_refs_bytes
    }

    fn is_null(&self, row: usize) -> bool {
        self.array.is_null(row)
    }

    fn slice(&self, offset: usize, length: usize) -> VectorRef {
        Arc::new(ListVector::from(self.array.slice(offset, length)))
    }

    fn get(&self, index: usize) -> Value {
        if !self.array.is_valid(index) {
            return Value::Null;
        }

        let array = &self.array.value(index);
        let vector = VectorHelper::try_into_vector(array).unwrap_or_else(|_| {
            panic!(
                "arrow array with datatype {:?} cannot converted to our vector",
                array.data_type()
            )
        });
        let values = (0..vector.len())
            .map(|i| vector.get(i))
            .collect::<Vec<Value>>();
        Value::List(ListValue::new(
            Some(Box::new(values)),
            self.inner_datatype.clone(),
        ))
    }

    fn get_ref(&self, index: usize) -> ValueRef {
        ValueRef::List(ListValueRef::Indexed {
            vector: self,
            idx: index,
        })
    }
}

impl Serializable for ListVector {
    fn serialize_to_json(&self) -> Result<Vec<JsonValue>> {
        self.array
            .iter()
            .map(|v| match v {
                None => Ok(JsonValue::Null),
                Some(v) => VectorHelper::try_into_vector(v)
                    .and_then(|v| v.serialize_to_json())
                    .map(JsonValue::Array),
            })
            .collect()
    }
}

impl From<ArrowListArray> for ListVector {
    fn from(array: ArrowListArray) -> Self {
        let inner_datatype = ConcreteDataType::from_arrow_type(match array.data_type() {
            ArrowDataType::List(field) => &field.data_type,
            _ => unreachable!(),
        });
        Self {
            array,
            inner_datatype,
        }
    }
}

impl_try_from_arrow_array_for_vector!(ArrowListArray, ListVector);

pub struct ListVectorIter<'a> {
    vector: &'a ListVector,
    iter: ZipValidity<'a, usize, Range<usize>>,
}

impl<'a> ListVectorIter<'a> {
    pub fn new(vector: &'a ListVector) -> ListVectorIter<'a> {
        let iter = ZipValidity::new(
            0..vector.len(),
            vector.array.validity().as_ref().map(|x| x.iter()),
        );

        Self { vector, iter }
    }
}

impl<'a> Iterator for ListVectorIter<'a> {
    type Item = Option<ListValueRef<'a>>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|idx_opt| {
            idx_opt.map(|idx| ListValueRef::Indexed {
                vector: self.vector,
                idx,
            })
        })
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }

    #[inline]
    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.iter.nth(n).map(|idx_opt| {
            idx_opt.map(|idx| ListValueRef::Indexed {
                vector: self.vector,
                idx,
            })
        })
    }
}

impl ScalarVector for ListVector {
    type OwnedItem = ListValue;
    type RefItem<'a> = ListValueRef<'a>;
    type Iter<'a> = ListVectorIter<'a>;
    type Builder = ListVectorBuilder;

    fn get_data(&self, idx: usize) -> Option<Self::RefItem<'_>> {
        if self.array.is_valid(idx) {
            Some(ListValueRef::Indexed { vector: self, idx })
        } else {
            None
        }
    }

    fn iter_data(&self) -> Self::Iter<'_> {
        ListVectorIter::new(self)
    }
}

// Some codes are ported from arrow2's MutableListArray.
pub struct ListVectorBuilder {
    inner_type: ConcreteDataType,
    offsets: Vec<i32>,
    values: Box<dyn MutableVector>,
    validity: Option<MutableBitmap>,
}

impl ListVectorBuilder {
    pub fn with_type_capacity(inner_type: ConcreteDataType, capacity: usize) -> ListVectorBuilder {
        let mut offsets = Vec::with_capacity(capacity + 1);
        offsets.push(0);
        // The actual required capacity might greater than the capacity of the `ListVector`
        // if there exists child vector that has more than one element.
        let values = inner_type.create_mutable_vector(capacity);

        ListVectorBuilder {
            inner_type,
            offsets,
            values,
            validity: None,
        }
    }

    #[inline]
    fn last_offset(&self) -> i32 {
        *self.offsets.last().unwrap()
    }

    fn push_null(&mut self) {
        self.offsets.push(self.last_offset());
        match &mut self.validity {
            Some(validity) => validity.push(false),
            None => self.init_validity(),
        }
    }

    fn init_validity(&mut self) {
        let len = self.offsets.len() - 1;

        let mut validity = MutableBitmap::with_capacity(self.offsets.capacity());
        validity.extend_constant(len, true);
        validity.set(len - 1, false);
        self.validity = Some(validity)
    }

    fn push_list_value(&mut self, list_value: &ListValue) -> Result<()> {
        if let Some(items) = list_value.items() {
            for item in &**items {
                self.values.push_value_ref(item.as_value_ref())?;
            }
        }
        self.push_valid();
        Ok(())
    }

    /// Needs to be called when a valid value was extended to this builder.
    fn push_valid(&mut self) {
        let size = self.values.len();
        let size = i32::try_from(size).unwrap();
        assert!(size >= *self.offsets.last().unwrap());

        self.offsets.push(size);
        if let Some(validity) = &mut self.validity {
            validity.push(true)
        }
    }
}

impl MutableVector for ListVectorBuilder {
    fn data_type(&self) -> ConcreteDataType {
        ConcreteDataType::list_datatype(self.inner_type.clone())
    }

    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    fn to_vector(&mut self) -> VectorRef {
        Arc::new(self.finish())
    }

    fn push_value_ref(&mut self, value: ValueRef) -> Result<()> {
        if let Some(list_ref) = value.as_list()? {
            match list_ref {
                ListValueRef::Indexed { vector, idx } => match vector.get(idx).as_list()? {
                    Some(list_value) => self.push_list_value(list_value)?,
                    None => self.push_null(),
                },
                ListValueRef::Ref { val } => self.push_list_value(val)?,
            }
        } else {
            self.push_null();
        }

        Ok(())
    }

    fn extend_slice_of(&mut self, vector: &dyn Vector, offset: usize, length: usize) -> Result<()> {
        for idx in offset..offset + length {
            let value = vector.get_ref(idx);
            self.push_value_ref(value)?;
        }

        Ok(())
    }
}

impl ScalarVectorBuilder for ListVectorBuilder {
    type VectorType = ListVector;

    fn with_capacity(_capacity: usize) -> Self {
        panic!("Must use ListVectorBuilder::with_type_capacity()");
    }

    fn push(&mut self, value: Option<<Self::VectorType as ScalarVector>::RefItem<'_>>) {
        // We expect the input ListValue has the same inner type as the builder when using
        // push(), so just panic if `push_value_ref()` returns error, which indicate an
        // invalid input value type.
        self.push_value_ref(value.into()).unwrap_or_else(|e| {
            panic!(
                "Failed to push value, expect value type {:?}, err:{}",
                self.inner_type, e
            );
        });
    }

    fn finish(&mut self) -> Self::VectorType {
        let array = ArrowListArray::try_new(
            ConcreteDataType::list_datatype(self.inner_type.clone()).as_arrow_type(),
            std::mem::take(&mut self.offsets).into(),
            self.values.to_vector().to_arrow_array(),
            std::mem::take(&mut self.validity).map(|x| x.into()),
        )
        .unwrap(); // The `ListVectorBuilder` itself should ensure it always builds a valid array.

        ListVector {
            array,
            inner_datatype: self.inner_type.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{MutableListArray, MutablePrimitiveArray, TryExtend};
    use serde_json::json;

    use super::*;
    use crate::types::ListType;

    #[test]
    fn test_list_vector() {
        let data = vec![
            Some(vec![Some(1i32), Some(2), Some(3)]),
            None,
            Some(vec![Some(4), None, Some(6)]),
        ];

        let mut arrow_array = MutableListArray::<i32, MutablePrimitiveArray<i32>>::new();
        arrow_array.try_extend(data).unwrap();
        let arrow_array: ArrowListArray = arrow_array.into();

        let list_vector = ListVector {
            array: arrow_array.clone(),
            inner_datatype: ConcreteDataType::int32_datatype(),
        };
        assert_eq!(
            ConcreteDataType::List(ListType::new(ConcreteDataType::int32_datatype())),
            list_vector.data_type()
        );
        assert_eq!("ListVector", list_vector.vector_type_name());
        assert_eq!(3, list_vector.len());
        assert!(!list_vector.is_null(0));
        assert!(list_vector.is_null(1));
        assert!(!list_vector.is_null(2));

        assert_eq!(
            arrow_array,
            list_vector
                .to_arrow_array()
                .as_any()
                .downcast_ref::<ArrowListArray>()
                .unwrap()
                .clone()
        );
        assert_eq!(
            Validity::Slots(arrow_array.validity().unwrap()),
            list_vector.validity()
        );
        assert_eq!(
            arrow_array.offsets().len() * std::mem::size_of::<i64>()
                + arrow_array.values().len() * std::mem::size_of::<Arc<dyn Array>>(),
            list_vector.memory_size()
        );

        let slice = list_vector.slice(0, 2);
        assert_eq!(
            "ListArray[[1, 2, 3], None]",
            format!("{:?}", slice.to_arrow_array())
        );

        assert_eq!(
            Value::List(ListValue::new(
                Some(Box::new(vec![
                    Value::Int32(1),
                    Value::Int32(2),
                    Value::Int32(3)
                ])),
                ConcreteDataType::int32_datatype()
            )),
            list_vector.get(0)
        );
        let value_ref = list_vector.get_ref(0);
        assert!(matches!(
            value_ref,
            ValueRef::List(ListValueRef::Indexed { .. })
        ));
        let value_ref = list_vector.get_ref(1);
        if let ValueRef::List(ListValueRef::Indexed { idx, .. }) = value_ref {
            assert_eq!(1, idx);
        } else {
            unreachable!()
        }
        assert_eq!(Value::Null, list_vector.get(1));
        assert_eq!(
            Value::List(ListValue::new(
                Some(Box::new(vec![
                    Value::Int32(4),
                    Value::Null,
                    Value::Int32(6)
                ])),
                ConcreteDataType::int32_datatype()
            )),
            list_vector.get(2)
        );
    }

    #[test]
    fn test_from_arrow_array() {
        let data = vec![
            Some(vec![Some(1u32), Some(2), Some(3)]),
            None,
            Some(vec![Some(4), None, Some(6)]),
        ];

        let mut arrow_array = MutableListArray::<i32, MutablePrimitiveArray<u32>>::new();
        arrow_array.try_extend(data).unwrap();
        let arrow_array: ArrowListArray = arrow_array.into();
        let array_ref: ArrayRef = Arc::new(arrow_array);

        let list_vector = ListVector::try_from_arrow_array(array_ref).unwrap();
        assert_eq!(
            "ListVector { array: ListArray[[1, 2, 3], None, [4, None, 6]], inner_datatype: UInt32(UInt32) }",
            format!("{:?}", list_vector)
        );
    }

    #[test]
    fn test_iter_list_vector_values() {
        let data = vec![
            Some(vec![Some(1i64), Some(2), Some(3)]),
            None,
            Some(vec![Some(4), None, Some(6)]),
        ];

        let mut arrow_array = MutableListArray::<i32, MutablePrimitiveArray<i64>>::new();
        arrow_array.try_extend(data).unwrap();
        let arrow_array: ArrowListArray = arrow_array.into();

        let list_vector = ListVector::from(arrow_array);
        assert_eq!(
            ConcreteDataType::List(ListType::new(ConcreteDataType::int64_datatype())),
            list_vector.data_type()
        );
        let mut iter = list_vector.values_iter();
        assert_eq!(
            "Int64[1, 2, 3]",
            format!("{:?}", iter.next().unwrap().unwrap().to_arrow_array())
        );
        assert_eq!(
            "Int64[]",
            format!("{:?}", iter.next().unwrap().unwrap().to_arrow_array())
        );
        assert_eq!(
            "Int64[4, None, 6]",
            format!("{:?}", iter.next().unwrap().unwrap().to_arrow_array())
        );
        assert!(iter.next().is_none())
    }

    #[test]
    fn test_serialize_to_json() {
        let data = vec![
            Some(vec![Some(1i64), Some(2), Some(3)]),
            None,
            Some(vec![Some(4), None, Some(6)]),
        ];

        let mut arrow_array = MutableListArray::<i32, MutablePrimitiveArray<i64>>::new();
        arrow_array.try_extend(data).unwrap();
        let arrow_array: ArrowListArray = arrow_array.into();

        let list_vector = ListVector::from(arrow_array);
        assert_eq!(
            vec![json!([1, 2, 3]), json!(null), json!([4, null, 6]),],
            list_vector.serialize_to_json().unwrap()
        );
    }

    fn new_list_vector(data: Vec<Option<Vec<Option<i32>>>>) -> ListVector {
        let mut arrow_array = MutableListArray::<i32, MutablePrimitiveArray<i32>>::new();
        arrow_array.try_extend(data).unwrap();
        let arrow_array: ArrowListArray = arrow_array.into();

        ListVector::from(arrow_array)
    }

    #[test]
    fn test_list_vector_builder() {
        let mut builder =
            ListType::new(ConcreteDataType::int32_datatype()).create_mutable_vector(3);
        builder
            .push_value_ref(ValueRef::List(ListValueRef::Ref {
                val: &ListValue::new(
                    Some(Box::new(vec![
                        Value::Int32(4),
                        Value::Null,
                        Value::Int32(6),
                    ])),
                    ConcreteDataType::int32_datatype(),
                ),
            }))
            .unwrap();
        assert!(builder.push_value_ref(ValueRef::Int32(123)).is_err());

        let data = vec![
            Some(vec![Some(1), Some(2), Some(3)]),
            None,
            Some(vec![Some(7), Some(8), None]),
        ];
        let input = new_list_vector(data);
        builder.extend_slice_of(&input, 1, 2).unwrap();
        assert!(builder
            .extend_slice_of(&crate::vectors::Int32Vector::from_slice(&[13]), 0, 1)
            .is_err());
        let vector = builder.to_vector();

        let expect: VectorRef = Arc::new(new_list_vector(vec![
            Some(vec![Some(4), None, Some(6)]),
            None,
            Some(vec![Some(7), Some(8), None]),
        ]));
        assert_eq!(expect, vector);
    }

    #[test]
    fn test_list_vector_for_scalar() {
        let mut builder =
            ListVectorBuilder::with_type_capacity(ConcreteDataType::int32_datatype(), 2);
        builder.push(None);
        builder.push(Some(ListValueRef::Ref {
            val: &ListValue::new(
                Some(Box::new(vec![
                    Value::Int32(4),
                    Value::Null,
                    Value::Int32(6),
                ])),
                ConcreteDataType::int32_datatype(),
            ),
        }));
        let vector = builder.finish();

        let expect = new_list_vector(vec![None, Some(vec![Some(4), None, Some(6)])]);
        assert_eq!(expect, vector);

        assert!(vector.get_data(0).is_none());
        assert_eq!(
            ListValueRef::Indexed {
                vector: &vector,
                idx: 1
            },
            vector.get_data(1).unwrap()
        );
        assert_eq!(
            *vector.get(1).as_list().unwrap().unwrap(),
            vector.get_data(1).unwrap().to_owned_scalar()
        );

        let mut iter = vector.iter_data();
        assert!(iter.next().unwrap().is_none());
        assert_eq!(
            ListValueRef::Indexed {
                vector: &vector,
                idx: 1
            },
            iter.next().unwrap().unwrap()
        );
        assert!(iter.next().is_none());

        let mut iter = vector.iter_data();
        assert_eq!(2, iter.size_hint().0);
        assert_eq!(
            ListValueRef::Indexed {
                vector: &vector,
                idx: 1
            },
            iter.nth(1).unwrap().unwrap()
        );
    }
}
