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

use std::path::PathBuf;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use object_store::services::Fs;
use object_store::ObjectStore;

pub const TEST_BATCH_SIZE: usize = 100;

pub fn get_data_dir(path: &str) -> PathBuf {
    // https://doc.rust-lang.org/cargo/reference/environment-variables.html
    let dir = env!("CARGO_MANIFEST_DIR");

    PathBuf::from(dir).join(path)
}

pub fn format_schema(schema: SchemaRef) -> Vec<String> {
    schema
        .fields()
        .iter()
        .map(|f| {
            format!(
                "{}: {:?}: {}",
                f.name(),
                f.data_type(),
                if f.is_nullable() { "NULL" } else { "NOT NULL" }
            )
        })
        .collect()
}

pub fn test_store(root: &str) -> ObjectStore {
    let mut builder = Fs::default();
    builder.root(root);

    ObjectStore::new(builder).unwrap().finish()
}

pub fn test_basic_schema() -> SchemaRef {
    let schema = Schema::new(vec![
        Field::new("num", DataType::Int64, false),
        Field::new("str", DataType::Utf8, false),
    ]);
    Arc::new(schema)
}
