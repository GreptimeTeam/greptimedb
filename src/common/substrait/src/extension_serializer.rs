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

use datafusion::error::Result;
use datafusion::execution::registry::SerializerRegistry;
use datafusion_common::DataFusionError;
use datafusion_expr::UserDefinedLogicalNode;
use promql::extension_plan::{
    EmptyMetric, InstantManipulate, RangeManipulate, SeriesDivide, SeriesNormalize,
};

pub struct ExtensionSerializer;

impl SerializerRegistry for ExtensionSerializer {
    /// Serialize this node to a byte array. This serialization should not include
    /// input plans.
    fn serialize_logical_plan(&self, node: &dyn UserDefinedLogicalNode) -> Result<Vec<u8>> {
        match node.name() {
            name if name == InstantManipulate::name() => {
                let instant_manipulate = node
                    .as_any()
                    .downcast_ref::<InstantManipulate>()
                    .expect("Failed to downcast to InstantManipulate");
                Ok(instant_manipulate.serialize())
            }
            name if name == SeriesNormalize::name() => {
                let series_normalize = node
                    .as_any()
                    .downcast_ref::<SeriesNormalize>()
                    .expect("Failed to downcast to SeriesNormalize");
                Ok(series_normalize.serialize())
            }
            name if name == RangeManipulate::name() => {
                let range_manipulate = node
                    .as_any()
                    .downcast_ref::<RangeManipulate>()
                    .expect("Failed to downcast to RangeManipulate");
                Ok(range_manipulate.serialize())
            }
            name if name == SeriesDivide::name() => {
                let series_divide = node
                    .as_any()
                    .downcast_ref::<SeriesDivide>()
                    .expect("Failed to downcast to SeriesDivide");
                Ok(series_divide.serialize())
            }
            name if name == EmptyMetric::name() => Err(DataFusionError::Substrait(
                "EmptyMetric should not be serialized".to_string(),
            )),
            "MergeScan" => Ok(vec![]),
            other => Err(DataFusionError::NotImplemented(format!(
                "Serizlize logical plan for {}",
                other
            ))),
        }
    }

    /// Deserialize user defined logical plan node ([UserDefinedLogicalNode]) from
    /// bytes.
    fn deserialize_logical_plan(
        &self,
        name: &str,
        bytes: &[u8],
    ) -> Result<Arc<dyn UserDefinedLogicalNode>> {
        match name {
            name if name == InstantManipulate::name() => {
                let instant_manipulate = InstantManipulate::deserialize(bytes)?;
                Ok(Arc::new(instant_manipulate))
            }
            name if name == SeriesNormalize::name() => {
                let series_normalize = SeriesNormalize::deserialize(bytes)?;
                Ok(Arc::new(series_normalize))
            }
            name if name == RangeManipulate::name() => {
                let range_manipulate = RangeManipulate::deserialize(bytes)?;
                Ok(Arc::new(range_manipulate))
            }
            name if name == SeriesDivide::name() => {
                let series_divide = SeriesDivide::deserialize(bytes)?;
                Ok(Arc::new(series_divide))
            }
            name if name == EmptyMetric::name() => Err(DataFusionError::Substrait(
                "EmptyMetric should not be deserialized".to_string(),
            )),
            other => Err(DataFusionError::NotImplemented(format!(
                "Deserialize logical plan for {}",
                other
            ))),
        }
    }
}
