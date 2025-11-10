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

pub(crate) mod attribute;
pub(crate) mod into_row;
pub(crate) mod schema;
pub(crate) mod to_row;
pub(crate) mod utils;

pub(crate) const META_KEY_COL: &str = "col";
pub(crate) const META_KEY_NAME: &str = "name";
pub(crate) const META_KEY_DATATYPE: &str = "datatype";
pub(crate) const META_KEY_SEMANTIC: &str = "semantic";
pub(crate) const META_KEY_SKIP: &str = "skip";
