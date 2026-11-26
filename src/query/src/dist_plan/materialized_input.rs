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
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DFSchemaRef, Result};
use datafusion_expr::{Extension, LogicalPlan, UserDefinedLogicalNodeCore};

/// The data slot of a remote join: the receiving end of the local
/// [`Materialize`](crate::dist_plan::MaterializeLogicalPlan) stage.
///
/// It is a leaf of the remote plan, so it carries the schema of the build side and the
/// batches the frontend sends along with the plan. Before the frontend binds the data the
/// batch list is empty; the node is then a schema-only slot that must not be executed,
/// i.e. encoded as an empty table.
#[derive(Debug, Clone)]
pub struct MaterializedInput {
    schema: DFSchemaRef,
    batches: Vec<RecordBatch>,
    row_count: usize,
}

impl PartialEq for MaterializedInput {
    fn eq(&self, other: &Self) -> bool {
        self.schema == other.schema
            && self.row_count == other.row_count
            && self.batches == other.batches
    }
}
impl Eq for MaterializedInput {}
impl Hash for MaterializedInput {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.schema.hash(state);
        self.row_count.hash(state);
        for batch in &self.batches {
            batch.num_rows().hash(state);
            for array in batch.columns() {
                format!("{array:?}").hash(state);
            }
        }
    }
}
impl PartialOrd for MaterializedInput {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(
            self.row_count
                .cmp(&other.row_count)
                .then_with(|| self.batches.len().cmp(&other.batches.len())),
        )
    }
}

impl MaterializedInput {
    pub const NAME: &'static str = "greptime.materialized_input";

    pub fn new(schema: DFSchemaRef, batches: Vec<RecordBatch>) -> Self {
        let row_count = batches.iter().map(RecordBatch::num_rows).sum();
        Self {
            schema,
            batches,
            row_count,
        }
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

    /// Binds `batches` into every data slot of `plan` without changing its shape.
    ///
    /// The remote join template is built by the analyzer with a schema-only slot; the
    /// physical layer fills it with the batches the local [`Materialize`](Materialize)
    /// stage produced. Only the node payload changes, so no re-planning happens here.
    pub fn bind_batches(plan: LogicalPlan, batches: Vec<RecordBatch>) -> Result<LogicalPlan> {
        let bound = plan.transform(|node| {
            let LogicalPlan::Extension(extension) = &node else {
                return Ok(Transformed::no(node));
            };
            let Some(slot) = extension.node.as_any().downcast_ref::<MaterializedInput>() else {
                return Ok(Transformed::no(node));
            };
            let slot = MaterializedInput::new(slot.schema.clone(), batches.clone());
            Ok(Transformed::yes(slot.into_logical_plan()))
        })?;
        Ok(bound.data)
    }
}

impl UserDefinedLogicalNodeCore for MaterializedInput {
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
        write!(f, "MaterializedInput rows={}", self.row_count)
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
    use datafusion_common::tree_node::TreeNodeRecursion;
    use datafusion_expr::{JoinType, LogicalPlanBuilder, col, lit};

    use super::*;

    /// The first data slot anywhere in `plan`.
    fn find_slot(plan: &LogicalPlan) -> Option<&MaterializedInput> {
        let mut slots = Vec::new();
        let _ = plan.apply(|node| {
            if let LogicalPlan::Extension(extension) = node
                && let Some(slot) = extension.node.as_any().downcast_ref::<MaterializedInput>()
            {
                slots.push(slot);
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        });
        slots.into_iter().next()
    }

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
        let node = MaterializedInput::new(df_schema, vec![batch.clone()]);
        let decoded = MaterializedInput::decode_batches(&node.encode_batches().unwrap()).unwrap();

        assert_eq!(decoded, vec![batch]);
        assert_eq!(decoded[0].schema(), node.arrow_schema());
    }

    #[test]
    fn bind_batches_fills_the_slot_without_changing_the_plan() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let df_schema = Arc::new(DFSchema::try_from(schema.as_ref().clone()).unwrap());
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2]))])
                .unwrap();
        let slot = MaterializedInput::new(df_schema.clone(), Vec::new());
        let probe = LogicalPlanBuilder::empty(true)
            .project(vec![lit(1i64).alias("probe_id")])
            .unwrap()
            .build()
            .unwrap();
        let remote_input = LogicalPlanBuilder::from(slot.into_logical_plan())
            .join_on(probe, JoinType::Inner, vec![col("id").eq(col("probe_id"))])
            .unwrap()
            .build()
            .unwrap();

        let bound =
            MaterializedInput::bind_batches(remote_input.clone(), vec![batch.clone()]).unwrap();

        assert_eq!(
            remote_input.schema(),
            bound.schema(),
            "the shape is unchanged"
        );
        assert!(
            !bound.display_indent().to_string().contains("rows=0"),
            "the data slot is filled: {}",
            bound.display_indent()
        );
        let bound_slot = find_slot(&bound).expect("the slot survives binding");
        assert_eq!(&[batch], bound_slot.batches());
        assert_eq!(df_schema, *bound_slot.schema());
        assert!(
            find_slot(&remote_input)
                .expect("the template starts with a slot")
                .batches()
                .is_empty()
        );
    }
}
