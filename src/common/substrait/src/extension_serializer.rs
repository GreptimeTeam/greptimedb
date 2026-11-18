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
    Absent, EmptyMetric, FusedSeries, HistogramFold, InstantManipulate, RangeManipulate,
    RawSeriesSource, ScalarCalculate, SeriesDivide, SeriesNormalize, UnionDistinctOn,
};

#[derive(Debug)]
pub struct ExtensionSerializer;

impl SerializerRegistry for ExtensionSerializer {
    /// Serialize this node to a byte array. This serialization should not include
    /// input plans.
    fn serialize_logical_plan(&self, node: &dyn UserDefinedLogicalNode) -> Result<Vec<u8>> {
        match node.name() {
            name if name == FusedSeries::name() => {
                let fused = node
                    .as_any()
                    .downcast_ref::<FusedSeries>()
                    .expect("Failed to downcast to FusedSeries");
                fused.serialize()
            }
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
            name if name == ScalarCalculate::name() => {
                let scalar_calculate = node
                    .as_any()
                    .downcast_ref::<ScalarCalculate>()
                    .expect("Failed to downcast to ScalarCalculate");
                Ok(scalar_calculate.serialize())
            }
            name if name == SeriesDivide::name() => {
                let series_divide = node
                    .as_any()
                    .downcast_ref::<SeriesDivide>()
                    .expect("Failed to downcast to SeriesDivide");
                Ok(series_divide.serialize())
            }
            name if name == Absent::name() => {
                let absent = node
                    .as_any()
                    .downcast_ref::<Absent>()
                    .expect("Failed to downcast to Absent");
                Ok(absent.serialize())
            }
            name if name == HistogramFold::name() => {
                let histogram_fold = node
                    .as_any()
                    .downcast_ref::<HistogramFold>()
                    .expect("Failed to downcast to HistogramFold");
                histogram_fold.serialize()
            }
            name if name == UnionDistinctOn::name() => {
                let union_distinct_on = node
                    .as_any()
                    .downcast_ref::<UnionDistinctOn>()
                    .expect("Failed to downcast to UnionDistinctOn");
                Ok(union_distinct_on.serialize())
            }
            name if name == RawSeriesSource::name() => {
                let raw_series_source = node
                    .as_any()
                    .downcast_ref::<RawSeriesSource>()
                    .expect("Failed to downcast to RawSeriesSource");
                Ok(raw_series_source.serialize())
            }
            name if name == EmptyMetric::name() => Err(DataFusionError::Substrait(
                "EmptyMetric should not be serialized".to_string(),
            )),
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
            name if name == FusedSeries::name() => Ok(Arc::new(FusedSeries::deserialize(bytes)?)),
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
            name if name == ScalarCalculate::name() => {
                let scalar_calculate = ScalarCalculate::deserialize(bytes)?;
                Ok(Arc::new(scalar_calculate))
            }
            name if name == Absent::name() => {
                let absent = Absent::deserialize(bytes)?;
                Ok(Arc::new(absent))
            }
            name if name == HistogramFold::name() => {
                let histogram_fold = HistogramFold::deserialize(bytes)?;
                Ok(Arc::new(histogram_fold))
            }
            name if name == UnionDistinctOn::name() => {
                let union_distinct_on = UnionDistinctOn::deserialize(bytes)?;
                Ok(Arc::new(union_distinct_on))
            }
            name if name == RawSeriesSource::name() => {
                Ok(Arc::new(RawSeriesSource::deserialize(bytes)?))
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

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::datasource::{MemTable, provider_as_source};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::logical_expr::{Extension, LogicalPlan, LogicalPlanBuilder, col, lit};
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::{DFLogicalSubstraitConvertor, SubstraitPlan};

    #[tokio::test]
    async fn raw_series_source_substrait_roundtrip_restores_scan() {
        let table_name = "raw_source_input";
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("z", DataType::Utf8, false),
            Field::new("value_b", DataType::Float64, false),
            Field::new("value_a", DataType::Float64, false),
            Field::new("a", DataType::Utf8, false),
            Field::new("unused", DataType::Utf8, true),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
        ]));
        let provider = Arc::new(MemTable::try_new(input_schema, vec![vec![]]).unwrap());
        let filter = col("raw_source_input.z").eq(lit("web-1"));
        let input = LogicalPlanBuilder::scan_with_filters(
            table_name,
            provider_as_source(provider.clone()),
            Some(vec![0, 3, 2, 1, 5]),
            vec![filter],
        )
        .unwrap()
        .build()
        .unwrap();
        let LogicalPlan::TableScan(expected_scan) = &input else {
            panic!("expected table scan");
        };
        let expected_projection = expected_scan.projection.clone();
        let expected_schema = Arc::clone(&expected_scan.projected_schema);
        let expected_filters = expected_scan.filters.clone();
        let source = RawSeriesSource::try_new(
            Arc::from("http_requests_total"),
            vec!["z".into(), "a".into()],
            vec!["value_b".into(), "value_a".into()],
            input,
        )
        .unwrap();
        let expected_source_schema = Arc::clone(source.schema());
        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(source),
        });
        let converter = DFLogicalSubstraitConvertor;
        let bytes = converter.encode(&plan, ExtensionSerializer).unwrap();
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_serializer_registry(Arc::new(ExtensionSerializer))
            .build();
        let context = SessionContext::new_with_state(state);
        context.register_table(table_name, provider).unwrap();
        let decoded = converter.decode(bytes, context.state()).await.unwrap();

        let LogicalPlan::Extension(extension) = decoded else {
            panic!("expected restored RawSeriesSource extension");
        };
        let restored = extension
            .node
            .as_any()
            .downcast_ref::<RawSeriesSource>()
            .expect("expected RawSeriesSource");
        assert_eq!(restored.metric_name(), "http_requests_total");
        assert_eq!(restored.tags(), ["z", "a"]);
        assert_eq!(restored.fields(), ["value_b", "value_a"]);
        assert_eq!(restored.schema(), &expected_source_schema);
        let inputs = extension.node.inputs();
        assert_eq!(inputs.len(), 1);
        let LogicalPlan::TableScan(scan) = inputs[0] else {
            panic!("expected restored table scan child, got: {inputs:?}");
        };
        assert_eq!(scan.table_name.to_string(), table_name);
        assert_eq!(scan.projection, expected_projection);
        assert_eq!(scan.projected_schema, expected_schema);
        assert_eq!(scan.filters, expected_filters);
    }
}
