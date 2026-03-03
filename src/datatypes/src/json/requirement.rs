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

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::data_type::ConcreteDataType;
use crate::types::{StructField, StructType};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct JsonPathTarget {
    root: JsonPathTargetNode,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct JsonPathTargetNode {
    children: BTreeMap<String, JsonPathTargetNode>,
    leaf_type: Option<ConcreteDataType>,
}

impl JsonPathTarget {
    pub fn require_typed_path(&mut self, path: &str, data_type: ConcreteDataType) {
        let mut current = &mut self.root;
        for segment in path.split('.') {
            current = current.children.entry(segment.to_string()).or_default();
        }
        current.require_leaf_type(data_type);
    }

    pub fn is_empty(&self) -> bool {
        self.root.children.is_empty()
    }

    pub fn build_type(&self) -> Option<ConcreteDataType> {
        if self.is_empty() {
            None
        } else {
            Some(ConcreteDataType::Struct(self.root.build_struct_type()))
        }
    }
}

impl JsonPathTargetNode {
    fn require_leaf_type(&mut self, data_type: ConcreteDataType) {
        self.leaf_type = Some(data_type);
    }

    fn build_data_type(&self) -> ConcreteDataType {
        if self.children.is_empty() {
            self.leaf_type
                .clone()
                .unwrap_or_else(ConcreteDataType::string_datatype)
        } else {
            ConcreteDataType::Struct(self.build_struct_type())
        }
    }

    fn build_struct_type(&self) -> StructType {
        let fields = self
            .children
            .iter()
            .map(|(name, child)| StructField::new(name.clone(), child.build_data_type(), true))
            .collect::<Vec<_>>();
        StructType::new(Arc::new(fields))
    }
}
