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

use std::any::Any;

use datafusion::config::{ConfigEntry, ConfigExtension, ExtensionOptions};
use datafusion::error::Result as DfResult;
use datafusion_common::DataFusionError;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_expr::{Extension, LogicalPlan};
use datafusion_optimizer::AnalyzerRule;
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};

use crate::query_engine::DefaultSerializer;
use crate::range_select::plan::RangeSelect;

#[derive(Debug, Clone, Default)]
pub struct RangeSelectOptions {
    pub experimental_enable_range_select_pushdown: bool,
}

impl ConfigExtension for RangeSelectOptions {
    const PREFIX: &'static str = "range_select";
}

impl ExtensionOptions for RangeSelectOptions {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, _value: &str) -> DfResult<()> {
        let message = if key == "experimental_enable_range_select_pushdown" {
            "range_select.experimental_enable_range_select_pushdown is an operator startup-only configuration"
                .to_string()
        } else {
            format!("RangeSelectOptions does not support set key: {key}")
        };
        Err(DataFusionError::NotImplemented(message))
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        vec![ConfigEntry {
            key: "range_select.experimental_enable_range_select_pushdown".to_string(),
            value: Some(self.experimental_enable_range_select_pushdown.to_string()),
            description: "Enable experimental RangeSelect pushdown planning",
        }]
    }
}

#[derive(Debug)]
pub struct RangeSelectLoweringAnalyzer;

impl AnalyzerRule for RangeSelectLoweringAnalyzer {
    fn name(&self) -> &str {
        "RangeSelectLoweringAnalyzer"
    }

    fn analyze(
        &self,
        plan: LogicalPlan,
        config: &datafusion::config::ConfigOptions,
    ) -> DfResult<LogicalPlan> {
        let enabled = config
            .extensions
            .get::<RangeSelectOptions>()
            .is_some_and(|options| options.experimental_enable_range_select_pushdown);
        if !enabled {
            return Ok(plan);
        }

        plan.transform_up(|plan| {
            let LogicalPlan::Extension(Extension { node }) = &plan else {
                return Ok(Transformed::no(plan));
            };
            let Some(range) = node.as_any().downcast_ref::<RangeSelect>() else {
                return Ok(Transformed::no(plan));
            };
            Ok(lower_if_encodable(range, |partial| {
                DFLogicalSubstraitConvertor
                    .encode(partial, DefaultSerializer)
                    .is_ok()
            })
            .map(Transformed::yes)
            .unwrap_or_else(|| Transformed::no(plan)))
        })
        .map(|result| result.data)
    }
}

fn lower_if_encodable(
    range: &RangeSelect,
    encode: impl FnOnce(&LogicalPlan) -> bool,
) -> Option<LogicalPlan> {
    let candidate = range.try_split_for_pushdown()?;
    let LogicalPlan::Extension(Extension { node }) = &candidate else {
        return None;
    };
    let final_range = node.as_any().downcast_ref::<RangeSelect>()?;
    let partial = final_range.input.as_ref();
    if !matches!(partial, LogicalPlan::Extension(_)) || !encode(partial) {
        return None;
    }
    Some(candidate)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use datafusion::config::ConfigOptions;
    use datafusion::datasource::DefaultTableSource;
    use datafusion::functions_aggregate::expr_fn::avg;
    use datafusion_expr::{Extension, LogicalPlanBuilder, col};

    use super::*;
    use crate::range_select::plan::{RangeFn, RangeSelect};

    fn eligible_range_plan() -> LogicalPlan {
        let schema = Arc::new(Schema::new(vec![
            Field::new("number", DataType::Int64, true),
            Field::new("host", DataType::Int64, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), true),
        ]));
        let source = Arc::new(DefaultTableSource::new(Arc::new(
            datafusion::datasource::empty::EmptyTable::new(schema),
        )));
        let input = LogicalPlanBuilder::scan("range_select_test", source, None)
            .unwrap()
            .build()
            .unwrap();
        let aggregate = avg(col("number"));
        let range = RangeSelect::try_new(
            Arc::new(input),
            vec![RangeFn {
                name: "avg(number) RANGE 5s".to_string(),
                data_type: DataType::Float64,
                expr: aggregate.clone(),
                range: Duration::from_secs(5),
                fill: None,
                need_cast: false,
            }],
            Duration::from_secs(5),
            0,
            col("ts"),
            vec![col("host")],
            &[aggregate, col("ts"), col("host")],
        )
        .unwrap();
        LogicalPlan::Extension(Extension {
            node: Arc::new(range),
        })
    }

    #[test]
    fn range_select_options_default_disabled() {
        assert!(!RangeSelectOptions::default().experimental_enable_range_select_pushdown);
    }

    #[test]
    fn range_select_options_rejects_sql_set_without_mutating_value() {
        let mut options = RangeSelectOptions::default();
        for value in ["true", "false"] {
            assert!(matches!(
                options.set("experimental_enable_range_select_pushdown", value),
                Err(DataFusionError::NotImplemented(_))
            ));
            assert!(!options.experimental_enable_range_select_pushdown);
        }
    }

    #[test]
    fn lowering_analyzer_is_identity_for_both_gate_states() {
        let plan = LogicalPlanBuilder::empty(false).build().unwrap();
        for enabled in [false, true] {
            let mut config = ConfigOptions::default();
            config.extensions.insert(RangeSelectOptions {
                experimental_enable_range_select_pushdown: enabled,
            });
            assert_eq!(
                RangeSelectLoweringAnalyzer
                    .analyze(plan.clone(), &config)
                    .unwrap(),
                plan
            );
        }
    }

    #[test]
    fn lowering_splits_eligible_range_select_only_when_enabled() {
        let plan = eligible_range_plan();
        let mut disabled = ConfigOptions::default();
        disabled.extensions.insert(RangeSelectOptions::default());
        assert_eq!(
            RangeSelectLoweringAnalyzer
                .analyze(plan.clone(), &disabled)
                .unwrap(),
            plan
        );

        let mut enabled = ConfigOptions::default();
        enabled.extensions.insert(RangeSelectOptions {
            experimental_enable_range_select_pushdown: true,
        });
        let split = RangeSelectLoweringAnalyzer.analyze(plan, &enabled).unwrap();
        let LogicalPlan::Extension(Extension { node }) = split else {
            panic!("expected final RangeSelect extension");
        };
        let final_range = node.as_any().downcast_ref::<RangeSelect>().unwrap();
        let LogicalPlan::Extension(Extension { node }) = final_range.input.as_ref() else {
            panic!("expected partial RangeSelect input");
        };
        assert!(matches!(
            node.as_any().downcast_ref::<RangeSelect>().unwrap().mode(),
            crate::range_select::plan::RangeSelectMode::Partial(_)
        ));
    }

    #[test]
    fn lowering_keeps_complete_plan_when_partial_encode_probe_fails() {
        let plan = eligible_range_plan();
        let LogicalPlan::Extension(Extension { node }) = &plan else {
            panic!("expected RangeSelect extension");
        };
        let range = node.as_any().downcast_ref::<RangeSelect>().unwrap();
        assert!(lower_if_encodable(range, |_| false).is_none());
    }
}
