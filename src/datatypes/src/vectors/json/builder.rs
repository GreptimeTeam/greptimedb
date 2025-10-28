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

use std::any::Any;

use snafu::OptionExt;

use crate::data_type::ConcreteDataType;
use crate::error::{Result, TryFromValueSnafu, UnsupportedOperationSnafu};
use crate::prelude::{ValueRef, Vector, VectorRef};
use crate::types::JsonType;
use crate::value::StructValueRef;
use crate::vectors::{MutableVector, StructVectorBuilder};

struct JsonStructsBuilder {
    json_type: JsonType,
    inner: StructVectorBuilder,
}

impl JsonStructsBuilder {
    fn new(json_type: JsonType, capacity: usize) -> Self {
        let struct_type = json_type.as_struct_type();
        let inner = StructVectorBuilder::with_type_and_capacity(struct_type, capacity);
        Self { json_type, inner }
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn push(&mut self, value: &ValueRef) -> Result<()> {
        if self.json_type.is_plain_json() {
            let value = ValueRef::Struct(StructValueRef::RefList {
                val: vec![value.clone()],
                fields: self.json_type.as_struct_type(),
            });
            self.inner.try_push_value_ref(&value)
        } else {
            self.inner.try_push_value_ref(value)
        }
    }
}

/// The vector builder for json type values.
///
/// Json type are dynamic, to some degree (as long as they can be merged into each other). So are
/// json values. Json values are physically stored in struct vectors, which require the types of
/// struct values to be fixed inside a certain struct vector. So to resolve "dynamic" vs "fixed"
/// datatype problem, in this builder, each type of json value gets its own struct vector builder.
/// Once new json type value is pushing into this builder, it creates a new "child" builder for it.
///
/// Given the "mixed" nature of the values stored in this builder, to produce the json vector, a
/// "merge" operation is performed. The "merge" is to iterate over all the "child" builders, and fill
/// nulls for missing json fields. The final vector's json type is fixed to be the "merge" of all
/// pushed json types.
pub(crate) struct JsonVectorBuilder {
    merged_type: JsonType,
    capacity: usize,
    builders: Vec<JsonStructsBuilder>,
}

impl JsonVectorBuilder {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            merged_type: JsonType::empty(),
            capacity,
            builders: vec![],
        }
    }

    fn try_create_new_builder(&mut self, json_type: &JsonType) -> Result<&mut JsonStructsBuilder> {
        self.merged_type.merge(json_type)?;

        let builder = JsonStructsBuilder::new(json_type.clone(), self.capacity);
        self.builders.push(builder);

        let len = self.builders.len();
        Ok(&mut self.builders[len - 1])
    }
}

impl MutableVector for JsonVectorBuilder {
    fn data_type(&self) -> ConcreteDataType {
        ConcreteDataType::Json(self.merged_type.clone())
    }

    fn len(&self) -> usize {
        self.builders.iter().map(|x| x.len()).sum()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    fn to_vector(&mut self) -> VectorRef {
        // Fast path:
        if self.builders.len() == 1 {
            return self.builders[0].inner.to_vector();
        }

        let merged_type = self.merged_type.as_struct_type();
        let mut unified_jsons =
            StructVectorBuilder::with_type_and_capacity(merged_type.clone(), self.capacity);
        for builder in self.builders.iter_mut() {
            unified_jsons
                .try_merge(&mut builder.inner)
                // Safety: the "unified_jsons" has the merged json type from all the builders,
                // so it should merge them without errors.
                .unwrap_or_else(|e| panic!("failed to merge json builders, error: {e}"));
        }
        unified_jsons.to_vector()
    }

    fn to_vector_cloned(&self) -> VectorRef {
        // Fast path:
        if self.builders.len() == 1 {
            return self.builders[0].inner.to_vector_cloned();
        }

        let merged_type = self.merged_type.as_struct_type();
        let mut unified_jsons =
            StructVectorBuilder::with_type_and_capacity(merged_type.clone(), self.capacity);
        for builder in self.builders.iter() {
            unified_jsons
                .try_merge_cloned(&builder.inner)
                // Safety: the "unified_jsons" has the merged json type from all the builders,
                // so it should merge them without errors.
                .unwrap_or_else(|e| panic!("failed to merge json builders, error: {e}"));
        }
        unified_jsons.to_vector()
    }

    fn try_push_value_ref(&mut self, value: &ValueRef) -> Result<()> {
        let data_type = value.data_type();
        let json_type = data_type.as_json().with_context(|| TryFromValueSnafu {
            reason: format!("expected json value, got {value:?}"),
        })?;

        let builder = match self.builders.last_mut() {
            Some(last) => {
                if &last.json_type != json_type {
                    self.try_create_new_builder(json_type)?
                } else {
                    last
                }
            }
            None => self.try_create_new_builder(json_type)?,
        };

        let ValueRef::Json(value) = value else {
            // Safety: json datatype value must be the value of json.
            unreachable!()
        };
        builder.push(value)
    }

    fn push_null(&mut self) {
        let null_json_value = ValueRef::Json(Box::new(ValueRef::Null));
        self.try_push_value_ref(&null_json_value)
            // Safety: learning from the method "try_push_value_ref", a null json value should be
            // always able to push into any json vectors.
            .unwrap_or_else(|e| {
                panic!("failed to push null json value: {null_json_value:?}, error: {e}")
            });
    }

    fn extend_slice_of(&mut self, _: &dyn Vector, _: usize, _: usize) -> Result<()> {
        UnsupportedOperationSnafu {
            op: "extend_slice_of",
            vector_type: "JsonVector",
        }
        .fail()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_type::DataType;
    use crate::json::JsonStructureSettings;
    use crate::vectors::helper::pretty_print;

    fn push(json: &str, builder: &mut JsonVectorBuilder, expected: std::result::Result<(), &str>) {
        let settings = JsonStructureSettings::Structured(None);
        let json: serde_json::Value = serde_json::from_str(json).unwrap();
        let value = settings.encode(json).unwrap();

        let value = value.as_value_ref();
        let result = builder.try_push_value_ref(&value);
        match (result, expected) {
            (Ok(()), Ok(())) => (),
            (Err(e), Err(expected)) => assert_eq!(e.to_string(), expected),
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_push_plain_jsons() -> Result<()> {
        let jsons = vec!["1", "2", r#""s""#, "[true]"];
        let results = vec![
            Ok(()),
            Ok(()),
            Err(
                "Failed to merge JSON datatype: datatypes have conflict, this: Int64, that: String",
            ),
            Err(
                "Failed to merge JSON datatype: datatypes have conflict, this: Int64, that: List<Boolean>",
            ),
        ];
        let mut builder = JsonVectorBuilder::with_capacity(1);
        for (json, result) in jsons.into_iter().zip(results.into_iter()) {
            push(json, &mut builder, result);
        }
        let vector = builder.to_vector();
        let expected = r#"
+----------------+
| StructVector   |
+----------------+
| {__plain__: 1} |
| {__plain__: 2} |
+----------------+"#;
        assert_eq!(pretty_print(vector), expected.trim());
        Ok(())
    }

    #[test]
    fn test_push_json_objects() -> Result<()> {
        let jsons = vec![
            r#"{
            "s": "a",
            "list": [1, 2, 3]
        }"#,
            r#"{
            "list": [4],
            "s": "b"
        }"#,
            r#"{
            "s": "c",
            "float": 0.9
        }"#,
            r#"{
            "float": 0.8,
            "s": "d"
        }"#,
            r#"{
            "float": 0.7,
            "int": -1
        }"#,
            r#"{
            "int": 0,
            "float": 0.6
        }"#,
            r#"{
            "int": 1,
            "object": {"hello": "world", "timestamp": 1761523200000}
        }"#,
            r#"{
            "object": {"hello": "greptime", "timestamp": 1761523201000},
            "int": 2
        }"#,
            r#"{
            "object": {"timestamp": 1761523202000},
            "nested": {"a": {"b": {"b": {"a": "abba"}}}}
        }"#,
            r#"{
            "nested": {"a": {"b": {"a": {"b": "abab"}}}},
            "object": {"timestamp": 1761523203000}
        }"#,
        ];
        let mut builder = JsonVectorBuilder::with_capacity(1);
        for json in jsons {
            push(json, &mut builder, Ok(()));
        }
        assert_eq!(builder.len(), 10);

        // test children builders:
        assert_eq!(builder.builders.len(), 6);
        let expect_types = [
            r#"Json<Struct<"list": List<Int64>, "s": String>>"#,
            r#"Json<Struct<"float": Float64, "s": String>>"#,
            r#"Json<Struct<"float": Float64, "int": Int64>>"#,
            r#"Json<Struct<"int": Int64, "object": Struct<"hello": String, "timestamp": Int64>>>"#,
            r#"Json<Struct<"nested": Struct<"a": Struct<"b": Struct<"b": Struct<"a": String>>>>, "object": Struct<"timestamp": Int64>>>"#,
            r#"Json<Struct<"nested": Struct<"a": Struct<"b": Struct<"a": Struct<"b": String>>>>, "object": Struct<"timestamp": Int64>>>"#,
        ];
        let expect_vectors = [
            r#"
+-------------------------+
| StructVector            |
+-------------------------+
| {list: [1, 2, 3], s: a} |
| {list: [4], s: b}       |
+-------------------------+"#,
            r#"
+--------------------+
| StructVector       |
+--------------------+
| {float: 0.9, s: c} |
| {float: 0.8, s: d} |
+--------------------+"#,
            r#"
+-----------------------+
| StructVector          |
+-----------------------+
| {float: 0.7, int: -1} |
| {float: 0.6, int: 0}  |
+-----------------------+"#,
            r#"
+---------------------------------------------------------------+
| StructVector                                                  |
+---------------------------------------------------------------+
| {int: 1, object: {hello: world, timestamp: 1761523200000}}    |
| {int: 2, object: {hello: greptime, timestamp: 1761523201000}} |
+---------------------------------------------------------------+"#,
            r#"
+------------------------------------------------------------------------+
| StructVector                                                           |
+------------------------------------------------------------------------+
| {nested: {a: {b: {b: {a: abba}}}}, object: {timestamp: 1761523202000}} |
+------------------------------------------------------------------------+"#,
            r#"
+------------------------------------------------------------------------+
| StructVector                                                           |
+------------------------------------------------------------------------+
| {nested: {a: {b: {a: {b: abab}}}}, object: {timestamp: 1761523203000}} |
+------------------------------------------------------------------------+"#,
        ];
        for (builder, (expect_type, expect_vector)) in builder
            .builders
            .iter()
            .zip(expect_types.into_iter().zip(expect_vectors.into_iter()))
        {
            assert_eq!(builder.json_type.name(), expect_type);
            let vector = builder.inner.to_vector_cloned();
            assert_eq!(pretty_print(vector), expect_vector.trim());
        }

        // test final merged json type:
        let expected = r#"Json<Struct<"float": Float64, "int": Int64, "list": List<Int64>, "nested": Struct<"a": Struct<"b": Struct<"a": Struct<"b": String>, "b": Struct<"a": String>>>>, "object": Struct<"hello": String, "timestamp": Int64>, "s": String>>"#;
        assert_eq!(builder.data_type().to_string(), expected);

        // test final produced vector:
        let expected = r#"
+-------------------------------------------------------------------------------------------------------------------+
| StructVector                                                                                                      |
+-------------------------------------------------------------------------------------------------------------------+
| {float: , int: , list: [1, 2, 3], nested: , object: , s: a}                                                       |
| {float: , int: , list: [4], nested: , object: , s: b}                                                             |
| {float: 0.9, int: , list: , nested: , object: , s: c}                                                             |
| {float: 0.8, int: , list: , nested: , object: , s: d}                                                             |
| {float: 0.7, int: -1, list: , nested: , object: , s: }                                                            |
| {float: 0.6, int: 0, list: , nested: , object: , s: }                                                             |
| {float: , int: 1, list: , nested: , object: {hello: world, timestamp: 1761523200000}, s: }                        |
| {float: , int: 2, list: , nested: , object: {hello: greptime, timestamp: 1761523201000}, s: }                     |
| {float: , int: , list: , nested: {a: {b: {a: , b: {a: abba}}}}, object: {hello: , timestamp: 1761523202000}, s: } |
| {float: , int: , list: , nested: {a: {b: {a: {b: abab}, b: }}}, object: {hello: , timestamp: 1761523203000}, s: } |
+-------------------------------------------------------------------------------------------------------------------+"#;
        let vector = builder.to_vector_cloned();
        assert_eq!(pretty_print(vector), expected.trim());
        let vector = builder.to_vector();
        assert_eq!(pretty_print(vector), expected.trim());
        Ok(())
    }
}
