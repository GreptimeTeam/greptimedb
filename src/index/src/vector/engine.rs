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

mod usearch;

use store_api::storage::{VectorIndexEngine, VectorIndexEngineType};
use usearch::MetricKind;
pub use usearch::UsearchEngine;

use super::VectorDistanceMetric;
use super::error::Result;

/// Configuration for creating a vector index.
#[derive(Debug, Clone)]
pub struct VectorIndexConfig {
    /// The vector index engine type.
    pub engine: VectorIndexEngineType,
    /// The dimension of vectors in this column.
    pub dim: usize,
    /// The distance metric to use (e.g., L2, Cosine, IP) - usearch format.
    pub metric: MetricKind,
    /// The original distance metric (for serialization).
    pub distance_metric: VectorDistanceMetric,
    /// HNSW connectivity parameter (M in the paper).
    /// Higher values give better recall but use more memory.
    pub connectivity: usize,
    /// Expansion factor during index construction (ef_construction).
    pub expansion_add: usize,
    /// Expansion factor during search (ef_search).
    pub expansion_search: usize,
}

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
