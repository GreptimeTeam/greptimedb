// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::types::DateTimeType;
use crate::vectors::{PrimitiveVector, PrimitiveVectorBuilder};

/// Vector of [`DateTime`](common_time::Date)
pub type DateTimeVector = PrimitiveVector<DateTimeType>;
/// Builder for [`DateTimeVector`].
pub type DateTimeVectorBuilder = PrimitiveVectorBuilder<DateTimeType>;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, PrimitiveArray};
    use arrow_array::ArrayRef;
    use common_time::timezone::set_default_timezone;
    use common_time::DateTime;

    use super::*;
    use crate::data_type::DataType;
    use crate::prelude::{
        ConcreteDataType, ScalarVector, ScalarVectorBuilder, Value, ValueRef, Vector, VectorRef,
    };
    use crate::serialize::Serializable;

    #[test]
    fn test_datetime_vector() {
        set_default_timezone(Some("Asia/Shanghai")).unwrap();
        let v = DateTimeVector::new(PrimitiveArray::from(vec![1000, 2000, 3000]));
        assert_eq!(ConcreteDataType::datetime_datatype(), v.data_type());
        assert_eq!(3, v.len());
        assert_eq!("DateTimeVector", v.vector_type_name());
        assert_eq!(
            &arrow::datatypes::DataType::Date64,
            v.to_arrow_array().data_type()
        );

        assert_eq!(Some(DateTime::new(1000)), v.get_data(0));
        assert_eq!(Value::DateTime(DateTime::new(1000)), v.get(0));
        assert_eq!(ValueRef::DateTime(DateTime::new(1000)), v.get_ref(0));

        let mut iter = v.iter_data();
        assert_eq!(Some(DateTime::new(1000)), iter.next().unwrap());
        assert_eq!(Some(DateTime::new(2000)), iter.next().unwrap());
        assert_eq!(Some(DateTime::new(3000)), iter.next().unwrap());
        assert!(!v.is_null(0));
        assert_eq!(24, v.memory_size());

        if let Value::DateTime(d) = v.get(0) {
            assert_eq!(1000, d.val());
        } else {
            unreachable!()
        }
        assert_eq!(
            "[\"1970-01-01 08:00:01+0800\",\"1970-01-01 08:00:02+0800\",\"1970-01-01 08:00:03+0800\"]",
            serde_json::to_string(&v.serialize_to_json().unwrap()).unwrap()
        );
    }

    #[test]
    fn test_datetime_vector_builder() {
        let mut builder = DateTimeVectorBuilder::with_capacity(3);
        builder.push(Some(DateTime::new(1)));
        builder.push(None);
        builder.push(Some(DateTime::new(-1)));

        let v = builder.finish();
        assert_eq!(ConcreteDataType::datetime_datatype(), v.data_type());
        assert_eq!(Value::DateTime(DateTime::new(1)), v.get(0));
        assert_eq!(Value::Null, v.get(1));
        assert_eq!(Value::DateTime(DateTime::new(-1)), v.get(2));

        let input = DateTimeVector::from_wrapper_slice([
            DateTime::new(1),
            DateTime::new(2),
            DateTime::new(3),
        ]);

        let mut builder = DateTimeType.create_mutable_vector(3);
        builder.push_value_ref(ValueRef::DateTime(DateTime::new(5)));
        assert!(builder.try_push_value_ref(ValueRef::Int32(123)).is_err());
        builder.extend_slice_of(&input, 1, 2).unwrap();
        assert!(builder
            .extend_slice_of(&crate::vectors::Int32Vector::from_slice([13]), 0, 1)
            .is_err());
        let vector = builder.to_vector();

        let expect: VectorRef = Arc::new(DateTimeVector::from_wrapper_slice([
            DateTime::new(5),
            DateTime::new(2),
            DateTime::new(3),
        ]));
        assert_eq!(expect, vector);
    }

    #[test]
    fn test_datetime_from_arrow() {
        let vector = DateTimeVector::from_wrapper_slice([DateTime::new(1), DateTime::new(2)]);
        let arrow: ArrayRef = Arc::new(vector.as_arrow().slice(0, vector.len())) as _;
        let vector2 = DateTimeVector::try_from_arrow_array(arrow).unwrap();
        assert_eq!(vector, vector2);
    }
}
