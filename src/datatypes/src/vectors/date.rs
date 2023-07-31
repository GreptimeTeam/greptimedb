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

use crate::types::DateType;
use crate::vectors::{PrimitiveVector, PrimitiveVectorBuilder};

// Vector for [`Date`](common_time::Date).
pub type DateVector = PrimitiveVector<DateType>;
// Builder to build DateVector.
pub type DateVectorBuilder = PrimitiveVectorBuilder<DateType>;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::ArrayRef;
    use common_time::date::Date;

    use super::*;
    use crate::data_type::DataType;
    use crate::scalars::{ScalarVector, ScalarVectorBuilder};
    use crate::serialize::Serializable;
    use crate::types::DateType;
    use crate::value::{Value, ValueRef};
    use crate::vectors::{Vector, VectorRef};

    #[test]
    fn test_build_date_vector() {
        let mut builder = DateVectorBuilder::with_capacity(4);
        builder.push(Some(Date::new(1)));
        builder.push(None);
        builder.push(Some(Date::new(-1)));
        let vector = builder.finish();
        assert_eq!(3, vector.len());
        assert_eq!(Value::Date(Date::new(1)), vector.get(0));
        assert_eq!(ValueRef::Date(Date::new(1)), vector.get_ref(0));
        assert_eq!(Some(Date::new(1)), vector.get_data(0));
        assert_eq!(None, vector.get_data(1));
        assert_eq!(Value::Null, vector.get(1));
        assert_eq!(ValueRef::Null, vector.get_ref(1));
        assert_eq!(Some(Date::new(-1)), vector.get_data(2));
        let mut iter = vector.iter_data();
        assert_eq!(Some(Date::new(1)), iter.next().unwrap());
        assert_eq!(None, iter.next().unwrap());
        assert_eq!(Some(Date::new(-1)), iter.next().unwrap());
    }

    #[test]
    fn test_date_scalar() {
        let vector = DateVector::from_slice([1, 2]);
        assert_eq!(2, vector.len());
        assert_eq!(Some(Date::new(1)), vector.get_data(0));
        assert_eq!(Some(Date::new(2)), vector.get_data(1));
    }

    #[test]
    fn test_date_vector_builder() {
        let input = DateVector::from_slice([1, 2, 3]);

        let mut builder = DateType.create_mutable_vector(3);
        builder.push_value_ref(ValueRef::Date(Date::new(5)));
        assert!(builder.try_push_value_ref(ValueRef::Int32(123)).is_err());
        builder.extend_slice_of(&input, 1, 2).unwrap();
        assert!(builder
            .extend_slice_of(&crate::vectors::Int32Vector::from_slice([13]), 0, 1)
            .is_err());
        let vector = builder.to_vector();

        let expect: VectorRef = Arc::new(DateVector::from_slice([5, 2, 3]));
        assert_eq!(expect, vector);
    }

    #[test]
    fn test_date_from_arrow() {
        let vector = DateVector::from_slice([1, 2]);
        let arrow: ArrayRef = Arc::new(vector.as_arrow().slice(0, vector.len()));
        let vector2 = DateVector::try_from_arrow_array(&arrow).unwrap();
        assert_eq!(vector, vector2);
    }

    #[test]
    fn test_serialize_date_vector() {
        let vector = DateVector::from_slice([-1, 0, 1]);
        let serialized_json = serde_json::to_string(&vector.serialize_to_json().unwrap()).unwrap();
        assert_eq!(
            r#"["1969-12-31","1970-01-01","1970-01-02"]"#,
            serialized_json
        );
    }
}
