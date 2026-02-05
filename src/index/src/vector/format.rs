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

//! Vector index blob format helpers.
//!
//! Blob layout (footer-last):
//! ```text
//! +----------------------+
//! |  Null Bitmap         |  <- RoaringBitmap serialized, size = meta.null_bitmap_size
//! +----------------------+
//! |  Index Data          |  <- engine-specific serialized, size = meta.index_size
//! +----------------------+
//! |  Meta (protobuf)     |  <- VectorIndexMeta encoded
//! +----------------------+
//! |  Meta Size (4B)      |  <- u32 LE
//! +----------------------+
//! ```

use greptime_proto::v1::index::{
    VectorDistanceMetric as ProtoVectorDistanceMetric, VectorIndexEngine as ProtoVectorIndexEngine,
};
use store_api::storage::{VectorDistanceMetric, VectorIndexEngineType};

/// Size of the meta length field in bytes (u32 LE).
pub const META_SIZE_LEN: usize = 4;

/// Converts Rust `VectorIndexEngineType` to proto `VectorIndexEngine`.
pub fn engine_type_to_proto(engine: VectorIndexEngineType) -> ProtoVectorIndexEngine {
    match engine {
        VectorIndexEngineType::Usearch => ProtoVectorIndexEngine::Usearch,
    }
}

/// Converts proto `VectorIndexEngine` to Rust `VectorIndexEngineType`.
pub fn engine_type_from_proto(engine: ProtoVectorIndexEngine) -> Option<VectorIndexEngineType> {
    match engine {
        ProtoVectorIndexEngine::Usearch => Some(VectorIndexEngineType::Usearch),
    }
}

/// Converts Rust `VectorDistanceMetric` to proto `VectorDistanceMetric`.
pub fn distance_metric_to_proto(metric: VectorDistanceMetric) -> ProtoVectorDistanceMetric {
    match metric {
        VectorDistanceMetric::L2sq => ProtoVectorDistanceMetric::L2sq,
        VectorDistanceMetric::Cosine => ProtoVectorDistanceMetric::Cosine,
        VectorDistanceMetric::InnerProduct => ProtoVectorDistanceMetric::InnerProduct,
    }
}

/// Converts proto `VectorDistanceMetric` to Rust `VectorDistanceMetric`.
pub fn distance_metric_from_proto(
    metric: ProtoVectorDistanceMetric,
) -> Option<VectorDistanceMetric> {
    match metric {
        ProtoVectorDistanceMetric::L2sq => Some(VectorDistanceMetric::L2sq),
        ProtoVectorDistanceMetric::Cosine => Some(VectorDistanceMetric::Cosine),
        ProtoVectorDistanceMetric::InnerProduct => Some(VectorDistanceMetric::InnerProduct),
    }
}

#[cfg(test)]
mod tests {
    use greptime_proto::v1::index::{VectorIndexMeta, VectorIndexStats};
    use prost::Message;
    use roaring::RoaringBitmap;

    use super::*;

    #[test]
    fn test_engine_type_roundtrip() {
        let engine = VectorIndexEngineType::Usearch;
        let proto = engine_type_to_proto(engine);
        let back = engine_type_from_proto(proto).unwrap();
        assert_eq!(engine, back);
    }

    #[test]
    fn test_distance_metric_roundtrip() {
        for metric in [
            VectorDistanceMetric::L2sq,
            VectorDistanceMetric::Cosine,
            VectorDistanceMetric::InnerProduct,
        ] {
            let proto = distance_metric_to_proto(metric);
            let back = distance_metric_from_proto(proto).unwrap();
            assert_eq!(metric, back);
        }
    }

    #[test]
    fn test_vector_index_meta_encode_decode() {
        let meta = VectorIndexMeta {
            engine: engine_type_to_proto(VectorIndexEngineType::Usearch).into(),
            dim: 128,
            metric: distance_metric_to_proto(VectorDistanceMetric::Cosine).into(),
            connectivity: 16,
            expansion_add: 128,
            expansion_search: 64,
            null_bitmap_size: 10,
            index_size: 1024,
            stats: Some(VectorIndexStats {
                total_row_count: 100,
                indexed_row_count: 95,
                null_count: 5,
            }),
        };

        let encoded = meta.encode_to_vec();
        let decoded = VectorIndexMeta::decode(encoded.as_slice()).unwrap();

        assert_eq!(decoded.engine, meta.engine);
        assert_eq!(decoded.dim, meta.dim);
        assert_eq!(decoded.metric, meta.metric);
        assert_eq!(decoded.connectivity, meta.connectivity);
        assert_eq!(decoded.expansion_add, meta.expansion_add);
        assert_eq!(decoded.expansion_search, meta.expansion_search);
        assert_eq!(decoded.null_bitmap_size, meta.null_bitmap_size);
        assert_eq!(decoded.index_size, meta.index_size);

        let stats = decoded.stats.unwrap();
        assert_eq!(stats.total_row_count, 100);
        assert_eq!(stats.indexed_row_count, 95);
        assert_eq!(stats.null_count, 5);
    }

    #[test]
    fn test_blob_format_roundtrip() {
        // Create test data
        let null_bitmap = RoaringBitmap::from_iter([1u32, 3, 5]);
        let mut null_bitmap_bytes = Vec::new();
        null_bitmap.serialize_into(&mut null_bitmap_bytes).unwrap();

        let index_data = b"fake_index_data_here";

        let meta = VectorIndexMeta {
            engine: engine_type_to_proto(VectorIndexEngineType::Usearch).into(),
            dim: 4,
            metric: distance_metric_to_proto(VectorDistanceMetric::L2sq).into(),
            connectivity: 16,
            expansion_add: 128,
            expansion_search: 64,
            null_bitmap_size: null_bitmap_bytes.len() as u32,
            index_size: index_data.len() as u64,
            stats: Some(VectorIndexStats {
                total_row_count: 10,
                indexed_row_count: 7,
                null_count: 3,
            }),
        };

        // Build blob: null_bitmap | index_data | meta | meta_size
        let mut blob = Vec::new();
        blob.extend_from_slice(&null_bitmap_bytes);
        blob.extend_from_slice(index_data);
        let meta_bytes = meta.encode_to_vec();
        blob.extend_from_slice(&meta_bytes);
        let meta_size = meta_bytes.len() as u32;
        blob.extend_from_slice(&meta_size.to_le_bytes());

        // Parse blob
        let blob_len = blob.len();
        let meta_size_read =
            u32::from_le_bytes(blob[blob_len - META_SIZE_LEN..].try_into().unwrap()) as usize;
        assert_eq!(meta_size_read, meta_bytes.len());

        let meta_start = blob_len - META_SIZE_LEN - meta_size_read;
        let decoded_meta =
            VectorIndexMeta::decode(&blob[meta_start..blob_len - META_SIZE_LEN]).unwrap();

        assert_eq!(
            decoded_meta.null_bitmap_size,
            null_bitmap_bytes.len() as u32
        );
        assert_eq!(decoded_meta.index_size, index_data.len() as u64);

        // Extract null bitmap
        let null_bitmap_end = decoded_meta.null_bitmap_size as usize;
        let restored_bitmap = RoaringBitmap::deserialize_from(&blob[..null_bitmap_end]).unwrap();
        assert_eq!(restored_bitmap.len(), 3);
        assert!(restored_bitmap.contains(1));
        assert!(restored_bitmap.contains(3));
        assert!(restored_bitmap.contains(5));

        // Extract index data
        let index_start = null_bitmap_end;
        let index_end = index_start + decoded_meta.index_size as usize;
        assert_eq!(&blob[index_start..index_end], index_data);
    }
}
