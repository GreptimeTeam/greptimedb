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

//! Pluggable vector index engine implementations.

mod usearch_impl;

use store_api::storage::{VectorIndexEngine, VectorIndexEngineType};
pub use usearch_impl::UsearchEngine;

use crate::error::Result;
use crate::sst::index::vector_index::creator::VectorIndexConfig;

/// Creates a new vector index engine based on the engine type.
pub fn create_engine(
    engine_type: VectorIndexEngineType,
    config: &VectorIndexConfig,
) -> Result<Box<dyn VectorIndexEngine>> {
    match engine_type {
        VectorIndexEngineType::Usearch => Ok(Box::new(UsearchEngine::create(config)?)),
    }
}

/// Loads a vector index engine from serialized data.
#[allow(unused)]
pub fn load_engine(
    engine_type: VectorIndexEngineType,
    config: &VectorIndexConfig,
    data: &[u8],
) -> Result<Box<dyn VectorIndexEngine>> {
    match engine_type {
        VectorIndexEngineType::Usearch => Ok(Box::new(UsearchEngine::load(config, data)?)),
    }
}
