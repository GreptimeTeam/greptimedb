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

use crate::extension::json;

/// Add some useful utilities upon Arrow's [Schema](arrow_schema::Schema).
pub trait ArrowSchemaExt {
    /// Check if this [Schema](arrow_schema::Schema) has JSON extension field.
    fn has_json_extension_field(&self) -> bool;
}

impl ArrowSchemaExt for arrow_schema::Schema {
    fn has_json_extension_field(&self) -> bool {
        self.fields().iter().any(json::is_json_extension_type)
    }
}
