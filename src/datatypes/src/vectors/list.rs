use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, ListArray};
use arrow::datatypes::DataType as ArrowDataType;
use serde_json::Value as JsonValue;
use snafu::prelude::*;

use crate::error::Result;
use crate::prelude::*;
use crate::serialize::Serializable;
use crate::types::ListType;
use crate::value::ListValue;
use crate::vectors::{impl_try_from_arrow_array_for_vector, impl_validity_for_vector};

type ArrowListArray = ListArray<i32>;

/// Vector of Lists, basically backed by Arrow's `ListArray`.
#[derive(Debug, Clone)]
pub struct ListVector {
    array: ArrowListArray,
    inner_data_type: ConcreteDataType,
}

impl ListVector {
    pub fn values_iter(&self) -> Box<dyn Iterator<Item = Result<VectorRef>> + '_> {
        Box::new(self.array.values_iter().map(VectorHelper::try_into_vector))
    }
}

impl Vector for ListVector {
    fn data_type(&self) -> ConcreteDataType {
        ConcreteDataType::List(ListType::new(self.inner_data_type.clone()))
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
            self.inner_data_type.clone(),
        ))
    }

    fn replicate(&self, _: &[usize]) -> VectorRef {
        // ListVector can be a scalar vector for implementing this `replicate` method. However,
        // that requires a lot of efforts, starting from not using Arrow's ListArray.
        // Refer to Databend's `ArrayColumn` for more details.
        unimplemented!()
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
        let inner_data_type = ConcreteDataType::from_arrow_type(match array.data_type() {
            ArrowDataType::List(field) => &field.data_type,
            _ => unreachable!(),
        });
        Self {
            array,
            inner_data_type,
        }
    }
}

impl_try_from_arrow_array_for_vector!(ArrowListArray, ListVector);

#[cfg(test)]
mod tests {
    use arrow::array::{MutableListArray, MutablePrimitiveArray, TryExtend};

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
            inner_data_type: ConcreteDataType::int32_datatype(),
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
        assert_eq!(
            Value::List(ListValue::new(
                Some(Box::new(vec![])),
                ConcreteDataType::int32_datatype()
            )),
            list_vector.get(1)
        );
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
            "ListVector { array: ListArray[[1, 2, 3], None, [4, None, 6]], inner_data_type: UInt32(UInt32) }",
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

        let list_vector = ListVector {
            array: arrow_array,
            inner_data_type: ConcreteDataType::int32_datatype(),
        };
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

        let list_vector = ListVector {
            array: arrow_array,
            inner_data_type: ConcreteDataType::int32_datatype(),
        };
        assert_eq!(
            "Ok([Array([Number(1), Number(2), Number(3)]), Null, Array([Number(4), Null, Number(6)])])",
            format!("{:?}", list_vector.serialize_to_json())
        );
    }
}
