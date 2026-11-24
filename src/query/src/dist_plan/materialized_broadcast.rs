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

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use datafusion_common::{DFSchemaRef, Result};
use datafusion_expr::{Extension, LogicalPlan, UserDefinedLogicalNodeCore};

#[derive(Debug, Clone)]
pub struct MaterializedBroadcastNode {
    schema: DFSchemaRef,
    batches: Vec<RecordBatch>,
    row_count: usize,
    /// A schema-only node inserted while analyzing a plan that is not executed
    /// yet. It keeps the materialization visible in the plan without evaluating
    /// the small side; the build plan runs when the query executes.
    placeholder: bool,
    /// For a placeholder, the build-side logical plan to evaluate at execution
    /// time. `None` for a materialized node.
    build_plan: Option<Arc<LogicalPlan>>,
}

impl PartialEq for MaterializedBroadcastNode {
    fn eq(&self, other: &Self) -> bool {
        self.schema == other.schema
            && self.row_count == other.row_count
            && self.placeholder == other.placeholder
            && self.batches == other.batches
    }
}
impl Eq for MaterializedBroadcastNode {}
impl Hash for MaterializedBroadcastNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.schema.hash(state);
        self.row_count.hash(state);
        self.placeholder.hash(state);
        for batch in &self.batches {
            batch.num_rows().hash(state);
            for array in batch.columns() {
                format!("{array:?}").hash(state);
            }
        }
    }
}
impl PartialOrd for MaterializedBroadcastNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(
            self.row_count
                .cmp(&other.row_count)
                .then_with(|| self.batches.len().cmp(&other.batches.len()))
                .then_with(|| self.placeholder.cmp(&other.placeholder)),
        )
    }
}

impl MaterializedBroadcastNode {
    pub const NAME: &'static str = "greptime.materialized_broadcast";
    /// Upper bound on the materialized rows. The row cap keeps the plan payload and the
    /// per-region transfer bounded for narrow rows; the byte cap below bounds wide rows.
    pub const MAX_ROWS: usize = 10_000;
    /// Upper bound on the decoded Arrow memory of the materialized batches. This is a
    /// conservative budget, not a derived value: it keeps one materialization from
    /// dominating frontend memory while the stream is collected.
    pub const MAX_BYTES: usize = 64 * 1024 * 1024;

    pub fn new(schema: DFSchemaRef, batches: Vec<RecordBatch>) -> Self {
        let row_count = batches.iter().map(RecordBatch::num_rows).sum();
        Self {
            schema,
            batches,
            row_count,
            placeholder: false,
            build_plan: None,
        }
    }

    /// A schema-only stand-in used when planning must stay side-effect free,
    /// e.g. for plain `EXPLAIN`.
    pub fn new_placeholder(schema: DFSchemaRef, build_plan: Arc<LogicalPlan>) -> Self {
        Self {
            schema,
            batches: Vec::new(),
            row_count: 0,
            placeholder: true,
            build_plan: Some(build_plan),
        }
    }

    pub fn is_placeholder(&self) -> bool {
        self.placeholder
    }

    pub fn build_plan(&self) -> Option<&LogicalPlan> {
        self.build_plan.as_deref()
    }

    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }
    pub fn arrow_schema(&self) -> arrow_schema::SchemaRef {
        Arc::new(self.schema.as_arrow().clone())
    }

    pub fn encode_batches(&self) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        {
            let schema = self
                .batches
                .first()
                .map(|batch| batch.schema())
                .unwrap_or_else(|| self.arrow_schema());
            let mut writer = StreamWriter::try_new(&mut bytes, &schema)?;
            if self.batches.is_empty() {
                writer.write(&RecordBatch::new_empty(schema.clone()))?;
            } else {
                for batch in &self.batches {
                    writer.write(batch)?;
                }
            }
            writer.finish()?;
        }
        Ok(bytes)
    }

    pub fn decode_batches(bytes: &[u8]) -> Result<Vec<RecordBatch>> {
        let reader = StreamReader::try_new(std::io::Cursor::new(bytes), None)?;
        reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn into_logical_plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(self),
        })
    }
}

/// Find the schema-only placeholder of a plan, if any.
pub fn find_placeholder(plan: &LogicalPlan) -> Option<&MaterializedBroadcastNode> {
    if let LogicalPlan::Extension(ext) = plan {
        if let Some(node) = ext
            .node
            .as_any()
            .downcast_ref::<MaterializedBroadcastNode>()
            && node.is_placeholder()
        {
            return Some(node);
        }
        // A `MergeScan` hides its remote input from the generic tree walk.
        if let Some(merge_scan) = ext
            .node
            .as_any()
            .downcast_ref::<crate::dist_plan::MergeScanLogicalPlan>()
        {
            return find_placeholder(merge_scan.input());
        }
    }
    plan.inputs().into_iter().find_map(find_placeholder)
}

/// Replace the schema-only placeholder with a materialized node holding `batches`.
pub fn materialize_placeholder(
    plan: LogicalPlan,
    batches: Vec<RecordBatch>,
) -> Result<LogicalPlan> {
    fn replace(plan: LogicalPlan, batches: &[RecordBatch]) -> Result<LogicalPlan> {
        if let LogicalPlan::Extension(ext) = &plan {
            if let Some(placeholder) = ext
                .node
                .as_any()
                .downcast_ref::<MaterializedBroadcastNode>()
                && placeholder.is_placeholder()
            {
                let materialized =
                    MaterializedBroadcastNode::new(placeholder.schema().clone(), batches.to_vec());
                return Ok(materialized.into_logical_plan());
            }
            // A `MergeScan` hides its remote input from the generic tree walk.
            if let Some(merge_scan) = ext
                .node
                .as_any()
                .downcast_ref::<crate::dist_plan::MergeScanLogicalPlan>()
            {
                let input = replace(merge_scan.input().clone(), batches)?;
                return Ok(LogicalPlan::Extension(Extension {
                    node: Arc::new(merge_scan.with_input(input)),
                }));
            }
        }
        let exprs = plan.expressions();
        let mut inputs = Vec::with_capacity(plan.inputs().len());
        for input in plan.inputs() {
            inputs.push(replace(input.clone(), batches)?);
        }
        plan.with_new_exprs(exprs, inputs)
    }
    replace(plan, &batches)
}

impl UserDefinedLogicalNodeCore for MaterializedBroadcastNode {
    fn name(&self) -> &str {
        Self::NAME
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }
    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }
    fn expressions(&self) -> Vec<datafusion_expr::Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if self.placeholder {
            write!(f, "MaterializedBroadcast (planned)")
        } else {
            write!(f, "MaterializedBroadcast rows={}", self.row_count)
        }
    }
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion_expr::Expr>,
        _inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        Ok(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::DFSchema;

    use super::*;

    #[test]
    fn ipc_round_trip_preserves_schema_and_data() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("a"), None])),
            ],
        )
        .unwrap();
        let df_schema = Arc::new(DFSchema::try_from(batch.schema().as_ref().clone()).unwrap());
        let node = MaterializedBroadcastNode::new(df_schema, vec![batch.clone()]);
        let decoded =
            MaterializedBroadcastNode::decode_batches(&node.encode_batches().unwrap()).unwrap();

        assert_eq!(decoded, vec![batch]);
        assert_eq!(decoded[0].schema(), node.arrow_schema());
    }
}
