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

//! Exercise the library without cfg(test), including the release boundary-only path.

use datatypes::prelude::{ConcreteDataType, Value};
use mito_codec::row_converter::{DensePrimaryKeyCodec, PrimaryKeyCodec, SortField};

#[test]
fn prefix_count_rejects_partial_fields_in_library_builds() {
    let values = [
        (0, Value::from("abcdefghijk")),
        (1, Value::Int64(42)),
        (2, Value::Binary(vec![0, 255, 1].into())),
    ];
    let codec = DensePrimaryKeyCodec::with_fields(
        values
            .iter()
            .map(|(id, value)| (*id, SortField::new(value.data_type())))
            .collect(),
    );
    let mut boundaries = vec![0];
    let mut encoded = Vec::new();
    for count in 1..=values.len() {
        encoded.clear();
        codec.encode_values(&values[..count], &mut encoded).unwrap();
        boundaries.push(encoded.len());
    }
    for end in 0..=encoded.len() {
        let count = codec.decode_prefix_len(&encoded[..end]);
        match boundaries.binary_search(&end) {
            Ok(expected) => assert_eq!(expected, count.unwrap()),
            Err(_) => assert!(count.is_err(), "accepted a partial field ending at {end}"),
        }
    }
    encoded.push(0);
    assert!(codec.decode_prefix_len(&encoded).is_err());
    assert!(codec.decode_prefix_len(&[2]).is_err());
}

#[test]
fn prefix_count_only_validates_values_in_debug_library_builds() {
    let codec = DensePrimaryKeyCodec::with_fields(vec![(
        0,
        SortField::new(ConcreteDataType::boolean_datatype()),
    )]);
    // These bytes have a complete field boundary but an invalid boolean value.
    let encoded = [1, 2];
    assert!(codec.decode_dense(&encoded).is_err());
    let count = codec.decode_prefix_len(&encoded);
    // cfg(test) applies to this integration test, not the linked library.
    if cfg!(debug_assertions) {
        assert!(count.is_err());
    } else {
        assert_eq!(1, count.unwrap());
    }
}
