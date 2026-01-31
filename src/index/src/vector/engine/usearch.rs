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

//! USearch HNSW implementation of VectorIndexEngine.

use common_error::ext::BoxedError;
use store_api::storage::{VectorIndexEngine, VectorSearchMatches};
pub use usearch::MetricKind;
use usearch::{Index, IndexOptions, ScalarKind};

use super::VectorIndexConfig;
use crate::vector::error::{EngineSnafu, Result};

type EngineResult<T> = std::result::Result<T, BoxedError>;

/// USearch-based vector index engine using HNSW algorithm.
pub struct UsearchEngine {
    index: Index,
}

impl UsearchEngine {
    /// Creates a new USearch engine with the given configuration.
    pub fn create(config: &VectorIndexConfig) -> Result<Self> {
        let options = IndexOptions {
            dimensions: config.dim,
            metric: config.metric,
            quantization: ScalarKind::F32,
            connectivity: config.connectivity,
            expansion_add: config.expansion_add,
            expansion_search: config.expansion_search,
            multi: false,
        };

        let index = Index::new(&options).map_err(|e| {
            EngineSnafu {
                reason: format!("Failed to create USearch index: {}", e),
            }
            .build()
        })?;

        Ok(Self { index })
    }

    /// Loads a USearch engine from serialized data.
    #[allow(unused)]
    pub fn load(config: &VectorIndexConfig, data: &[u8]) -> Result<Self> {
        let options = IndexOptions {
            dimensions: config.dim,
            metric: config.metric,
            quantization: ScalarKind::F32,
            // These will be loaded from serialized data
            connectivity: 0,
            expansion_add: 0,
            expansion_search: 0,
            multi: false,
        };

        let index = Index::new(&options).map_err(|e| {
            EngineSnafu {
                reason: format!("Failed to create USearch index for loading: {}", e),
            }
            .build()
        })?;

        index.load_from_buffer(data).map_err(|e| {
            EngineSnafu {
                reason: format!("Failed to load USearch index from buffer: {}", e),
            }
            .build()
        })?;

        Ok(Self { index })
    }

    /// Helper to create a BoxedError from a reason string.
    fn boxed_engine_error(reason: String) -> BoxedError {
        BoxedError::new(EngineSnafu { reason }.build())
    }
}

impl VectorIndexEngine for UsearchEngine {
    fn add(&mut self, key: u64, vector: &[f32]) -> EngineResult<()> {
        // Reserve capacity if needed
        if self.index.size() >= self.index.capacity() {
            let new_capacity = std::cmp::max(1, self.index.capacity() * 2);
            self.index.reserve(new_capacity).map_err(|e| {
                Self::boxed_engine_error(format!("Failed to reserve capacity: {}", e))
            })?;
        }

        self.index
            .add(key, vector)
            .map_err(|e| Self::boxed_engine_error(format!("Failed to add vector: {}", e)))
    }

    fn search(&self, query: &[f32], k: usize) -> EngineResult<VectorSearchMatches> {
        let matches = self
            .index
            .search(query, k)
            .map_err(|e| Self::boxed_engine_error(format!("Failed to search: {}", e)))?;

        Ok(VectorSearchMatches {
            keys: matches.keys,
            distances: matches.distances,
        })
    }

    fn serialized_length(&self) -> usize {
        self.index.serialized_length()
    }

    fn save_to_buffer(&self, buffer: &mut [u8]) -> EngineResult<()> {
        self.index
            .save_to_buffer(buffer)
            .map_err(|e| Self::boxed_engine_error(format!("Failed to save to buffer: {}", e)))
    }

    fn reserve(&mut self, capacity: usize) -> EngineResult<()> {
        self.index
            .reserve(capacity)
            .map_err(|e| Self::boxed_engine_error(format!("Failed to reserve: {}", e)))
    }

    fn size(&self) -> usize {
        self.index.size()
    }

    fn capacity(&self) -> usize {
        self.index.capacity()
    }

    fn memory_usage(&self) -> usize {
        self.index.memory_usage()
    }
}

#[cfg(test)]
mod tests {
    use store_api::storage::VectorIndexEngineType;

    use super::*;
    use crate::vector::VectorDistanceMetric;

    fn test_config() -> VectorIndexConfig {
        VectorIndexConfig {
            engine: VectorIndexEngineType::Usearch,
            dim: 4,
            metric: MetricKind::L2sq,
            distance_metric: VectorDistanceMetric::L2sq,
            connectivity: 16,
            expansion_add: 128,
            expansion_search: 64,
        }
    }

    #[test]
    fn test_usearch_engine_create() {
        let config = test_config();
        let engine = UsearchEngine::create(&config).unwrap();
        assert_eq!(engine.size(), 0);
    }

    #[test]
    fn test_usearch_engine_add_and_search() {
        let config = test_config();
        let mut engine = UsearchEngine::create(&config).unwrap();

        // Add some vectors
        engine.add(0, &[1.0, 0.0, 0.0, 0.0]).unwrap();
        engine.add(1, &[0.0, 1.0, 0.0, 0.0]).unwrap();
        engine.add(2, &[0.0, 0.0, 1.0, 0.0]).unwrap();

        assert_eq!(engine.size(), 3);

        // Search
        let matches = engine.search(&[1.0, 0.0, 0.0, 0.0], 2).unwrap();
        assert_eq!(matches.keys.len(), 2);
        // First result should be the exact match (key 0)
        assert_eq!(matches.keys[0], 0);
    }

    #[test]
    fn test_usearch_engine_serialization() {
        let config = test_config();
        let mut engine = UsearchEngine::create(&config).unwrap();

        engine.add(0, &[1.0, 0.0, 0.0, 0.0]).unwrap();
        engine.add(1, &[0.0, 1.0, 0.0, 0.0]).unwrap();

        // Serialize
        let len = engine.serialized_length();
        let mut buffer = vec![0u8; len];
        engine.save_to_buffer(&mut buffer).unwrap();

        // Load
        let loaded = UsearchEngine::load(&config, &buffer).unwrap();
        assert_eq!(loaded.size(), 2);

        // Verify search works on loaded index
        let matches = loaded.search(&[1.0, 0.0, 0.0, 0.0], 1).unwrap();
        assert_eq!(matches.keys[0], 0);
    }
}
