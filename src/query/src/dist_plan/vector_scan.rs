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

//! `VectorScanLogicalPlan` is a custom logical plan node that wraps a scan
//! with vector search hint. This node is serialized via Substrait extension
//! and transmitted to datanodes in distributed mode.

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion_common::{DFSchema, Result};
use datafusion_expr::{EmptyRelation, Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use greptime_proto::substrait_extension::VectorScan as PbVectorScan;
use prost::Message;
use snafu::ResultExt;
use store_api::storage::{ColumnId, VectorDistanceMetric, VectorSearchRequest};

use crate::error::{DeserializeSnafu, Result as QueryResult};

/// A custom logical plan node that wraps a scan with vector search hint.
///
/// In distributed mode, this node is serialized via Substrait extension
/// (using `PbVectorScan` proto message) and transmitted to datanodes.
/// On the datanode side, the hint is extracted and passed to the scan request.
#[derive(Debug, Clone)]
pub struct VectorScanLogicalPlan {
    /// The input logical plan (typically a table scan).
    input: LogicalPlan,
    /// Column ID of the vector column to search.
    column_id: ColumnId,
    /// The query vector to search for.
    query_vector: Vec<f32>,
    /// Number of nearest neighbors to return.
    k: usize,
    /// Distance metric to use.
    metric: VectorDistanceMetric,
}

impl PartialEq for VectorScanLogicalPlan {
    fn eq(&self, other: &Self) -> bool {
        self.input == other.input
            && self.column_id == other.column_id
            && self.k == other.k
            && self.metric == other.metric
            && self.query_vector.len() == other.query_vector.len()
            && self
                .query_vector
                .iter()
                .zip(other.query_vector.iter())
                .all(|(a, b)| a.to_bits() == b.to_bits())
    }
}

impl Eq for VectorScanLogicalPlan {}

impl PartialOrd for VectorScanLogicalPlan {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // Use Hash-based comparison for ordering since f32 doesn't have total ordering.
        // This is sufficient for DataFusion's requirements.
        let self_hash = {
            use std::hash::BuildHasher;
            let hasher = std::collections::hash_map::RandomState::new();
            hasher.hash_one(self)
        };
        let other_hash = {
            use std::hash::BuildHasher;
            let hasher = std::collections::hash_map::RandomState::new();
            hasher.hash_one(other)
        };
        self_hash.partial_cmp(&other_hash)
    }
}

impl Hash for VectorScanLogicalPlan {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.input.hash(state);
        self.column_id.hash(state);
        self.k.hash(state);
        // VectorDistanceMetric doesn't derive Hash, so convert to u32.
        metric_to_u32(self.metric).hash(state);
        for v in &self.query_vector {
            v.to_bits().hash(state);
        }
    }
}

/// Converts a `VectorDistanceMetric` to its proto representation (u32).
/// This matches the proto definition: 0=L2Squared, 1=Cosine, 2=Dot/InnerProduct.
pub fn metric_to_u32(metric: VectorDistanceMetric) -> u32 {
    match metric {
        VectorDistanceMetric::L2sq => 0,
        VectorDistanceMetric::Cosine => 1,
        VectorDistanceMetric::InnerProduct => 2,
    }
}

/// Converts a proto u32 to `VectorDistanceMetric`.
pub fn u32_to_metric(value: u32) -> VectorDistanceMetric {
    match value {
        1 => VectorDistanceMetric::Cosine,
        2 => VectorDistanceMetric::InnerProduct,
        _ => VectorDistanceMetric::L2sq, // Default to L2sq for 0 or unknown values
    }
}

impl UserDefinedLogicalNodeCore for VectorScanLogicalPlan {
    fn name(&self) -> &str {
        Self::name()
    }

    // Return the actual input so Substrait can properly serialize/deserialize it.
    // This makes VectorScanLogicalPlan an ExtensionSingleRel (with one child) instead of
    // ExtensionLeafRel (no children), which allows Substrait to handle the child plan automatically.
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &datafusion_common::DFSchemaRef {
        self.input.schema()
    }

    // Prevent further optimization on expressions.
    fn expressions(&self) -> Vec<datafusion_expr::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "VectorScan [column_id={}, k={}, metric={}, input=[\n{}\n]]",
            self.column_id, self.k, self.metric, self.input
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::prelude::Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        // Use the input from Substrait deserialization if provided.
        // This is called by Substrait consumer after deserializing the child plan.
        let input = inputs
            .into_iter()
            .next()
            .unwrap_or_else(|| self.input.clone());
        Ok(Self {
            input,
            column_id: self.column_id,
            query_vector: self.query_vector.clone(),
            k: self.k,
            metric: self.metric,
        })
    }
}

impl VectorScanLogicalPlan {
    /// Creates a new `VectorScanLogicalPlan`.
    pub fn new(
        input: LogicalPlan,
        column_id: ColumnId,
        query_vector: Vec<f32>,
        k: usize,
        metric: VectorDistanceMetric,
    ) -> Self {
        Self {
            input,
            column_id,
            query_vector,
            k,
            metric,
        }
    }

    /// Returns the static name of this plan node.
    pub fn name() -> &'static str {
        "VectorScan"
    }

    /// Creates a `LogicalPlan::Extension` node from this vector scan plan.
    pub fn into_logical_plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(self),
        })
    }

    /// Returns a reference to the input logical plan.
    pub fn input(&self) -> &LogicalPlan {
        &self.input
    }

    /// Returns the column ID of the vector column.
    pub fn column_id(&self) -> ColumnId {
        self.column_id
    }

    /// Returns the query vector.
    pub fn query_vector(&self) -> &[f32] {
        &self.query_vector
    }

    /// Returns the number of nearest neighbors (k).
    pub fn k(&self) -> usize {
        self.k
    }

    /// Returns the distance metric.
    pub fn metric(&self) -> VectorDistanceMetric {
        self.metric
    }

    /// Converts this plan to a `VectorSearchRequest` for the scan request.
    pub fn to_vector_search_request(&self) -> VectorSearchRequest {
        VectorSearchRequest {
            column_id: self.column_id,
            query_vector: self.query_vector.clone(),
            k: self.k,
            metric: self.metric,
        }
    }

    /// Deserializes a `VectorScanLogicalPlan` from protobuf bytes.
    ///
    /// Note: The input plan is deserialized as a placeholder (empty relation).
    /// The actual input plan should be patched in later by the Substrait decoder.
    pub fn deserialize(bytes: &[u8]) -> QueryResult<Self> {
        let pb = PbVectorScan::decode(bytes).context(DeserializeSnafu)?;

        // Create a placeholder input plan (will be replaced by Substrait decoder).
        let placeholder_plan = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        });

        Ok(Self {
            input: placeholder_plan,
            column_id: pb.column_id,
            query_vector: pb.query_vector,
            k: pb.k as usize,
            metric: u32_to_metric(pb.metric),
        })
    }
}
