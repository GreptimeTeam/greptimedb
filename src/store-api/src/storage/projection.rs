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

use std::fmt::{Display, Formatter};

/// A nested field access path.
///
/// Each path represents a field access on a nested column.
///
/// Example:
/// - `j.a.b` -> `["j", "a", "b"]`
pub type NestedPath = Vec<String>;

/// Projection information for a table scan.
#[derive(Default, Debug, Clone, PartialEq)]
pub struct ProjectionInput {
    /// Top-level column projection.
    ///
    /// The indices are based on the schema exposed by the table scan input,
    /// such as the schema passed to `TableProvider::scan`.
    ///
    /// Only the root columns with the specified schema indices are needed.
    pub projection: Vec<usize>,
    /// Nested field access paths used for sub-field projection.
    ///
    /// It extends and refines the top-level projection by specifying nested
    /// field accesses inside complex columns such as JSON or struct columns.
    ///
    /// In other words:
    /// - `projection` determines **which root columns are needed**
    /// - `nested_paths` further determines **which sub-fields inside those
    ///   columns are required**
    ///
    /// Each path starts with the root column name and continues with
    /// nested field names.
    pub nested_paths: Vec<NestedPath>,
}

impl ProjectionInput {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_projection(self, projection: Vec<usize>) -> Self {
        Self { projection, ..self }
    }

    pub fn with_nested_paths(self, nested_paths: Vec<NestedPath>) -> Self {
        Self {
            nested_paths,
            ..self
        }
    }
}

impl Display for ProjectionInput {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ProjectionInput {{ projection: {:?}, nested_paths: {:?} }}",
            self.projection, self.nested_paths
        )
    }
}
