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

use common_query::logical_plan::SubstraitPlanDecoder;
use datafusion::catalog::CatalogProviderList;
use datafusion::common::DataFusionError;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::execution::registry::SerializerRegistry;
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion_expr::UserDefinedLogicalNode;
use greptime_proto::substrait_extension::MergeScan as PbMergeScan;
use prost::Message;
use snafu::ResultExt;
use substrait::extension_serializer::ExtensionSerializer;
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};

use crate::dist_plan::{EncodedMergeScan, MergeScanLogicalPlan};
use crate::error::DataFusionSnafu;

/// Extended `[substrait::extension_serializer::ExtensionSerializer]` but supports `[MergeScanLogicalPlan]` serialization.
pub struct DefaultSerializer;

impl SerializerRegistry for DefaultSerializer {
    fn serialize_logical_plan(&self, node: &dyn UserDefinedLogicalNode) -> Result<Vec<u8>> {
        if node.name() == MergeScanLogicalPlan::name() {
            let merge_scan = node
                .as_any()
                .downcast_ref::<MergeScanLogicalPlan>()
                .expect("Failed to downcast to MergeScanLogicalPlan");

            let input = merge_scan.input();
            let is_placeholder = merge_scan.is_placeholder();
            let input = DFLogicalSubstraitConvertor
                .encode(input, DefaultSerializer)
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .to_vec();

            Ok(PbMergeScan {
                is_placeholder,
                input,
            }
            .encode_to_vec())
        } else {
            ExtensionSerializer.serialize_logical_plan(node)
        }
    }

    fn deserialize_logical_plan(
        &self,
        name: &str,
        bytes: &[u8],
    ) -> Result<Arc<dyn UserDefinedLogicalNode>> {
        if name == MergeScanLogicalPlan::name() {
            let pb_merge_scan =
                PbMergeScan::decode(bytes).map_err(|e| DataFusionError::External(Box::new(e)))?;

            let input = pb_merge_scan.input;
            let is_placeholder = pb_merge_scan.is_placeholder;

            // Use `EncodedMergeScan` as a temporary container,
            // it will be rewritten into `MergeScanLogicalPlan` by `SubstraitPlanDecoder`.
            // We can't decode the logical plan here because we don't have
            // the `SessionState` and `CatalogProviderList`.
            Ok(Arc::new(EncodedMergeScan::new(input, is_placeholder)))
        } else {
            ExtensionSerializer.deserialize_logical_plan(name, bytes)
        }
    }
}

/// The datafusion `[LogicalPlan]` decoder.
pub struct DefaultPlanDecoder {
    session_state: SessionState,
}

impl DefaultPlanDecoder {
    pub fn new(session_state: SessionState) -> Self {
        Self { session_state }
    }

    /// Rewrites `[EncodedMergeScan]` to `[MergeScanLogicalPlan]`.
    fn rewrite_merge_scan(
        &self,
        plan: LogicalPlan,
        catalog_list: Arc<dyn CatalogProviderList>,
    ) -> crate::error::Result<LogicalPlan> {
        let mut rewriter = MergeScanRewriter {
            session_state: self.session_state.clone(),
            catalog_list,
        };
        Ok(plan.rewrite(&mut rewriter).context(DataFusionSnafu)?.data)
    }
}

#[async_trait::async_trait]
impl SubstraitPlanDecoder for DefaultPlanDecoder {
    async fn decode(
        &self,
        message: bytes::Bytes,
        catalog_list: Arc<dyn CatalogProviderList>,
    ) -> common_query::error::Result<LogicalPlan> {
        // The session_state already has the `DefaultSerialzier` as `SerializerRegistry`.
        let logical_plan = DFLogicalSubstraitConvertor
            .decode(message, catalog_list.clone(), self.session_state.clone())
            .await
            .unwrap();

        Ok(self.rewrite_merge_scan(logical_plan, catalog_list).unwrap())
    }
}

struct MergeScanRewriter {
    catalog_list: Arc<dyn CatalogProviderList>,
    session_state: SessionState,
}

impl TreeNodeRewriter for MergeScanRewriter {
    type Node = LogicalPlan;

    /// descend
    fn f_down<'a>(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
        match node {
            LogicalPlan::Extension(Extension { node }) => {
                if node.name() == EncodedMergeScan::name() {
                    let encoded_merge_scan = node
                        .as_any()
                        .downcast_ref::<EncodedMergeScan>()
                        .expect("Failed to downcast to EncodedMergeScan");
                    let catalog_list = self.catalog_list.clone();
                    let session_state = self.session_state.clone();
                    let input = encoded_merge_scan.input.clone();

                    let input = std::thread::spawn(move || {
                        common_runtime::block_on_bg(async move {
                            DFLogicalSubstraitConvertor
                                .decode(&input[..], catalog_list, session_state)
                                .await
                                .map_err(|e| DataFusionError::External(Box::new(e)))
                        })
                    })
                    .join()
                    .unwrap()?;

                    Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                        node: Arc::new(MergeScanLogicalPlan::new(
                            input,
                            encoded_merge_scan.is_placeholder,
                        )),
                    })))
                } else {
                    Ok(Transformed::no(LogicalPlan::Extension(Extension { node })))
                }
            }
            node => Ok(Transformed::no(node)),
        }
    }
}
