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

//! Vector index types and options.
//!
//! This module re-exports types from `datatypes` and provides conversions
//! to USearch types, as well as distance computation functions.

pub use datatypes::schema::{VectorDistanceMetric, VectorIndexOptions};
use nalgebra::DVectorView;
pub use usearch::MetricKind;

/// Converts a VectorDistanceMetric to a USearch MetricKind.
pub fn distance_metric_to_usearch(metric: VectorDistanceMetric) -> MetricKind {
    match metric {
        VectorDistanceMetric::L2sq => MetricKind::L2sq,
        VectorDistanceMetric::Cosine => MetricKind::Cos,
        VectorDistanceMetric::InnerProduct => MetricKind::IP,
    }
}

/// Computes distance between two vectors using the specified metric.
///
/// Uses SIMD-optimized implementations via nalgebra.
///
/// **Note:** The caller must ensure that the two vectors have the same length
/// and are non-empty. Empty vectors return 0.0 for all metrics.
pub fn compute_distance(v1: &[f32], v2: &[f32], metric: VectorDistanceMetric) -> f32 {
    // Empty vectors are degenerate; return 0.0 uniformly across all metrics.
    if v1.is_empty() || v2.is_empty() {
        return 0.0;
    }

    match metric {
        VectorDistanceMetric::L2sq => l2sq(v1, v2),
        VectorDistanceMetric::Cosine => cosine(v1, v2),
        VectorDistanceMetric::InnerProduct => -dot(v1, v2),
    }
}

/// Calculates the squared L2 distance between two vectors.
fn l2sq(lhs: &[f32], rhs: &[f32]) -> f32 {
    let lhs = DVectorView::from_slice(lhs, lhs.len());
    let rhs = DVectorView::from_slice(rhs, rhs.len());
    (lhs - rhs).norm_squared()
}

/// Calculates the cosine distance between two vectors.
///
/// Returns a value in `[0.0, 2.0]` where 0.0 means identical direction and 2.0 means
/// opposite direction. For degenerate cases (zero or near-zero magnitude vectors),
/// returns 1.0 (maximum uncertainty) to avoid NaN and ensure safe index operations.
fn cosine(lhs: &[f32], rhs: &[f32]) -> f32 {
    let lhs_vec = DVectorView::from_slice(lhs, lhs.len());
    let rhs_vec = DVectorView::from_slice(rhs, rhs.len());

    let dot_product = lhs_vec.dot(&rhs_vec);
    let lhs_norm = lhs_vec.norm();
    let rhs_norm = rhs_vec.norm();

    // Zero-magnitude vectors have undefined direction; return max distance as safe fallback.
    if dot_product.abs() < f32::EPSILON
        || lhs_norm.abs() < f32::EPSILON
        || rhs_norm.abs() < f32::EPSILON
    {
        return 1.0;
    }

    let cos_similar = dot_product / (lhs_norm * rhs_norm);
    let res = 1.0 - cos_similar;
    // Clamp near-zero results to exactly 0.0 to avoid floating-point artifacts.
    if res.abs() < f32::EPSILON { 0.0 } else { res }
}

/// Calculates the dot product between two vectors.
fn dot(lhs: &[f32], rhs: &[f32]) -> f32 {
    let lhs = DVectorView::from_slice(lhs, lhs.len());
    let rhs = DVectorView::from_slice(rhs, rhs.len());
    lhs.dot(&rhs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_distance_metric_to_usearch() {
        assert_eq!(
            distance_metric_to_usearch(VectorDistanceMetric::L2sq),
            MetricKind::L2sq
        );
        assert_eq!(
            distance_metric_to_usearch(VectorDistanceMetric::Cosine),
            MetricKind::Cos
        );
        assert_eq!(
            distance_metric_to_usearch(VectorDistanceMetric::InnerProduct),
            MetricKind::IP
        );
    }

    #[test]
    fn test_vector_index_options_default() {
        let options = VectorIndexOptions::default();
        assert_eq!(options.metric, VectorDistanceMetric::L2sq);
        assert_eq!(options.connectivity, 16);
        assert_eq!(options.expansion_add, 128);
        assert_eq!(options.expansion_search, 64);
    }

    #[test]
    fn test_compute_distance_l2sq() {
        let v1 = vec![1.0, 2.0, 3.0];
        let v2 = vec![4.0, 5.0, 6.0];
        // L2sq = (4-1)^2 + (5-2)^2 + (6-3)^2 = 9 + 9 + 9 = 27
        let dist = compute_distance(&v1, &v2, VectorDistanceMetric::L2sq);
        assert!((dist - 27.0).abs() < 1e-6);
    }

    #[test]
    fn test_compute_distance_cosine() {
        let v1 = vec![1.0, 0.0, 0.0];
        let v2 = vec![0.0, 1.0, 0.0];
        // Orthogonal vectors have cosine similarity of 0, distance of 1
        let dist = compute_distance(&v1, &v2, VectorDistanceMetric::Cosine);
        assert!((dist - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_compute_distance_inner_product() {
        let v1 = vec![1.0, 2.0, 3.0];
        let v2 = vec![4.0, 5.0, 6.0];
        // Inner product = 1*4 + 2*5 + 3*6 = 4 + 10 + 18 = 32
        // Distance is negated: -32
        let dist = compute_distance(&v1, &v2, VectorDistanceMetric::InnerProduct);
        assert!((dist - (-32.0)).abs() < 1e-6);
    }

    #[test]
    fn test_compute_distance_empty_vectors() {
        // Empty vectors should return 0.0 uniformly for all metrics
        assert_eq!(compute_distance(&[], &[], VectorDistanceMetric::L2sq), 0.0);
        assert_eq!(
            compute_distance(&[], &[], VectorDistanceMetric::Cosine),
            0.0
        );
        assert_eq!(
            compute_distance(&[], &[], VectorDistanceMetric::InnerProduct),
            0.0
        );
    }
}
