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

use std::sync::Arc;

use common_error::ext::BoxedError;
use common_query::logical_plan::SubstraitPlanDecoder;
use datafusion::catalog::CatalogProviderList;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{DFSchema, DFSchemaRef, DataFusionError};
use datafusion_expr::{LogicalPlan, UserDefinedLogicalNodeCore};
use snafu::ResultExt;

use crate::error::{DataFusionSnafu, Error, QueryPlanSnafu};
use crate::query_engine::DefaultPlanDecoder;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct UnexpandedNode {
    pub inner: Vec<u8>,
    pub schema: DFSchemaRef,
}

impl UnexpandedNode {
    pub fn new_no_schema(inner: Vec<u8>) -> Self {
        Self {
            inner,
            schema: Arc::new(DFSchema::empty()),
        }
    }
}

impl PartialOrd for UnexpandedNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.inner.partial_cmp(&other.inner)
    }
}

impl UnexpandedNode {
    const NAME: &'static str = "Unexpanded";
}

impl UserDefinedLogicalNodeCore for UnexpandedNode {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn with_exprs_and_inputs(
        &self,
        _: Vec<datafusion_expr::Expr>,
        _: Vec<LogicalPlan>,
    ) -> datafusion_common::Result<Self> {
        Ok(self.clone())
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", Self::NAME)
    }

    fn expressions(&self) -> Vec<datafusion_expr::Expr> {
        vec![]
    }
}

/// Rewrite decoded `LogicalPlan` so all `UnexpandedNode` are expanded
///
/// This is a hack to support decoded substrait plan using async functions
///
/// Corresponding encode method should put custom logical node's input plan into `UnexpandedNode` after encoding into bytes
pub struct UnexpandDecoder {
    pub default_decoder: DefaultPlanDecoder,
}

impl UnexpandDecoder {
    pub fn new(default_decoder: DefaultPlanDecoder) -> Self {
        Self { default_decoder }
    }
}

impl UnexpandDecoder {
    /// Decode substrait plan into `LogicalPlan` and recursively expand all unexpanded nodes
    ///
    /// supporting async functions so our custom logical plan's input can be decoded as well
    pub async fn decode(
        &self,
        message: bytes::Bytes,
        catalog_list: Arc<dyn CatalogProviderList>,
        optimize: bool,
    ) -> Result<LogicalPlan, Error> {
        let plan = self
            .default_decoder
            .decode(message, catalog_list.clone(), optimize)
            .await
            .map_err(BoxedError::new)
            .context(QueryPlanSnafu)?;
        self.expand(plan, catalog_list, optimize)
            .await
            .map_err(BoxedError::new)
            .context(QueryPlanSnafu)
    }

    /// Recursively expand all unexpanded nodes in the plan
    pub async fn expand(
        &self,
        plan: LogicalPlan,
        catalog_list: Arc<dyn CatalogProviderList>,
        optimize: bool,
    ) -> Result<LogicalPlan, Error> {
        let mut cur_unexpanded_node = None;
        let mut root_expanded_plan = plan.clone();
        loop {
            root_expanded_plan
                .apply(|p| {
                    if let LogicalPlan::Extension(node) = p {
                        if node.node.name() == UnexpandedNode::NAME {
                            let node = node.node.as_any().downcast_ref::<UnexpandedNode>().ok_or(
                                DataFusionError::Plan(
                                    "Failed to downcast to UnexpandedNode".to_string(),
                                ),
                            )?;
                            cur_unexpanded_node = Some(node.clone());
                            return Ok(TreeNodeRecursion::Stop);
                        }
                    }
                    Ok(TreeNodeRecursion::Continue)
                })
                .context(DataFusionSnafu)?;

            if let Some(unexpanded) = cur_unexpanded_node.take() {
                let decoded = self
                    .default_decoder
                    .decode(
                        unexpanded.inner.clone().into(),
                        catalog_list.clone(),
                        optimize,
                    )
                    .await
                    .map_err(BoxedError::new)
                    .context(QueryPlanSnafu)?;
                let mut decoded = Some(decoded);

                // replace it with decoded plan
                // since if unexpanded the first node we encountered is the same node
                root_expanded_plan = root_expanded_plan
                    .transform(|p| {
                        let Some(decoded) = decoded.take() else {
                            return Ok(Transformed::no(p));
                        };

                        if let LogicalPlan::Extension(node) = &p
                            && node.node.name() == UnexpandedNode::NAME
                        {
                            let _ = node.node.as_any().downcast_ref::<UnexpandedNode>().ok_or(
                                DataFusionError::Plan(
                                    "Failed to downcast to UnexpandedNode".to_string(),
                                ),
                            )?;
                            Ok(Transformed::yes(decoded))
                        } else {
                            Ok(Transformed::no(p))
                        }
                    })
                    .context(DataFusionSnafu)?
                    .data;
            } else {
                // all node are expanded
                break;
            }
        }

        Ok(root_expanded_plan)
    }
}
