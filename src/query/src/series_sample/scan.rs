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

//! The plan node that carries a sample fraction to the scan that honours it.
//!
//! A series sample is per scan, not per table: two scans of one table (a
//! self-join) may ask for different fractions, and a scan the optimizer derived
//! from another one must not inherit a fraction nobody asked for. The fraction
//! therefore travels in the plan, as a node over the scan it applies to, instead
//! of in a table-keyed out-of-band hint.
//!
//! The node is a plain extension node, so it crosses the region boundary the way
//! the PromQL nodes do: the region payload is a Substrait-encoded logical plan
//! and the extension serializer stores a node as arbitrary bytes. The payload is
//! JSON (`{"Series":{"num":1,"den":16}}`), so no protobuf message is needed for
//! it.
//!
//! The node is lowered where the scan request is built, in
//! [`SeriesSampleScanRule`]: the region turns it into
//! [`ScanRequest::sample`](store_api::storage::ScanRequest::sample) (series) or
//! [`ScanRequest::pk_sample`](store_api::storage::ScanRequest::pk_sample)
//! (primary key). Nothing above the region can honour a fraction, so the node
//! drops out of the plan there.

use std::fmt::{self, Formatter};
use std::sync::Arc;

use datafusion::common::tree_node::Transformed;
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::datasource::DefaultTableSource;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{
    EmptyRelation, Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore,
};
use datafusion_optimizer::{OptimizerConfig, OptimizerRule};
use serde::{Deserialize, Serialize};

use crate::dummy_catalog::DummyTableProvider;

/// Which dimension of a scan the fraction prunes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum SampleMethod {
    /// Keep a fraction of the *series* of a metric-engine table: a kept series
    /// returns every one of its rows, a dropped series returns none.
    Series { num: u32, den: u32 },
    /// Keep the rows of one contiguous interval of the encoded primary key of a
    /// table that stores rows in encoded-primary-key order.
    PrimaryKey { num: u32, den: u32 },
}

impl SampleMethod {
    /// The numerator of the kept fraction.
    pub fn num(&self) -> u32 {
        match self {
            Self::Series { num, .. } | Self::PrimaryKey { num, .. } => *num,
        }
    }

    /// The denominator of the kept fraction.
    pub fn den(&self) -> u32 {
        match self {
            Self::Series { den, .. } | Self::PrimaryKey { den, .. } => *den,
        }
    }

    /// The word that names the sampled dimension in an explain plan.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Series { .. } => "series",
            Self::PrimaryKey { .. } => "primary-key",
        }
    }

    /// Keeps a fraction of the series of a metric-engine table.
    pub fn series(num: u32, den: u32) -> Self {
        Self::Series { num, den }
    }
}

impl fmt::Display for SampleMethod {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{word} {num}/{den}",
            word = self.kind(),
            num = self.num(),
            den = self.den()
        )
    }
}

/// One sampled scan: `input`, with a fraction of `method` kept.
///
/// The node has no expressions and the schema of its input: it selects which
/// rows the scan below it reads, not which columns.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct SeriesSampleScan {
    input: LogicalPlan,
    method: SampleMethod,
}

impl SeriesSampleScan {
    pub const fn name() -> &'static str {
        "SeriesSampleScan"
    }

    pub fn new(input: LogicalPlan, method: SampleMethod) -> Self {
        Self { input, method }
    }

    /// The scanned plan.
    pub fn input(&self) -> &LogicalPlan {
        &self.input
    }

    /// The fraction and the dimension it samples.
    pub fn method(&self) -> SampleMethod {
        self.method
    }

    /// Encodes only the method; Substrait carries the input plan separately.
    pub fn serialize(&self) -> Vec<u8> {
        // A `SampleMethod` is two integers and a variant tag: JSON cannot fail here.
        serde_json::to_vec(&self.method).expect("SampleMethod is JSON serializable")
    }

    /// Decodes a method with a placeholder input, replaced when Substrait
    /// restores the child.
    pub fn deserialize(bytes: &[u8]) -> DataFusionResult<Self> {
        let method = serde_json::from_slice(bytes).map_err(|error| {
            DataFusionError::Substrait(format!("Failed to deserialize SeriesSampleScan: {error}"))
        })?;
        Ok(Self {
            input: placeholder_input(),
            method,
        })
    }

    /// The node as a plan.
    pub fn into_logical_plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(self),
        })
    }
}

/// The input a decoded node carries until Substrait restores its child.
fn placeholder_input() -> LogicalPlan {
    LogicalPlan::EmptyRelation(EmptyRelation {
        produce_one_row: false,
        schema: Arc::new(DFSchema::empty()),
    })
}

impl UserDefinedLogicalNodeCore for SeriesSampleScan {
    fn name(&self) -> &str {
        Self::name()
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}: {}", Self::name(), self.method)
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> DataFusionResult<Self> {
        if !exprs.is_empty() {
            return Err(DataFusionError::Internal(format!(
                "{} takes no expressions, got {exprs:?}",
                Self::name()
            )));
        }
        if inputs.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "{} takes exactly one input, got {}",
                Self::name(),
                inputs.len()
            )));
        }
        Ok(Self {
            input: inputs.pop().expect("checked one input"),
            method: self.method,
        })
    }
}

/// Lowers [`SeriesSampleScan`] into the scan request of the region it reads.
///
/// A fraction is only honoured by the region scan: the region builds the
/// [`ScanRequest`](store_api::storage::ScanRequest) that the storage engine
/// samples. The rule therefore runs where a region plan is planned, replaces a
/// node over a [`DummyTableProvider`] scan with the scan of a forked provider
/// carrying the fraction, and drops the node.
///
/// The node must read a bare table scan: the fraction describes the scan, and a
/// node over anything else could not be lowered into a scan request at all, so
/// that case fails loudly instead of planning an unsampled read.
#[derive(Debug)]
pub struct SeriesSampleScanRule {
    /// Whether this rule runs in the engine that executes region plans.
    ///
    /// A frontend plans the node over a catalog table and ships it inside the
    /// region payload, so only the region side can - and must - lower it.
    region_side: bool,
}

impl SeriesSampleScanRule {
    /// Creates the rule for the engine described by `with_dist_planner`.
    pub fn new(with_dist_planner: bool) -> Self {
        Self {
            region_side: !with_dist_planner,
        }
    }

    /// Lowers the sample node of `plan`, if it is one.
    pub fn lower(&self, plan: LogicalPlan) -> DataFusionResult<Transformed<LogicalPlan>> {
        let Some((method, child)) = sample_of(&plan) else {
            return Ok(Transformed::no(plan));
        };

        let LogicalPlan::TableScan(mut scan) = child else {
            return Err(DataFusionError::Plan(format!(
                "cannot apply {} to a plan that is not a table scan: its input is {child}",
                SeriesSampleScan::name()
            )));
        };

        let provider = scan
            .source
            .downcast_ref::<DefaultTableSource>()
            .and_then(|source| source.table_provider.downcast_ref::<DummyTableProvider>());
        let Some(provider) = provider else {
            return self.not_a_region_scan(plan, &scan);
        };

        // A provider can be shared by several scans, so the request is forked for
        // this use-site instead of mutating the catalog provider.
        let adapter = provider.clone_for_scan();
        adapter.with_sample(method);
        scan.source = Arc::new(DefaultTableSource::new(Arc::new(adapter)));
        Ok(Transformed::yes(LogicalPlan::TableScan(scan)))
    }

    /// Keeps the node when it still has a region to travel to, and rejects it
    /// otherwise: a sample that silently does nothing is the one outcome this
    /// design exists to prevent.
    fn not_a_region_scan(
        &self,
        plan: LogicalPlan,
        scan: &datafusion_expr::logical_plan::TableScan,
    ) -> DataFusionResult<Transformed<LogicalPlan>> {
        if !self.region_side {
            return Ok(Transformed::no(plan));
        }
        Err(DataFusionError::Plan(format!(
            "cannot apply {} to table `{}`: the scan reads a catalog table, not a region, so \
             nothing would sample it",
            SeriesSampleScan::name(),
            scan.table_name
        )))
    }
}

impl OptimizerRule for SeriesSampleScanRule {
    fn name(&self) -> &str {
        "SeriesSampleScanRule"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DataFusionResult<Transformed<LogicalPlan>> {
        self.lower(plan)
    }
}

/// Returns the method of `plan` and its input, when `plan` is a sample node.
fn sample_of(plan: &LogicalPlan) -> Option<(SampleMethod, LogicalPlan)> {
    let LogicalPlan::Extension(extension) = plan else {
        return None;
    };
    let node = extension.node.as_any().downcast_ref::<SeriesSampleScan>()?;
    Some((node.method(), node.input().clone()))
}

#[cfg(test)]
mod tests {
    use datafusion::datasource::{MemTable, provider_as_source};
    use datafusion::logical_expr::LogicalPlanBuilder;
    use datafusion::prelude::{col, lit};
    use datatypes::arrow::datatypes::{DataType, Field, Schema};
    use store_api::storage::{
        PkSample, RegionId, ScanRequest, SeriesSample, TimeSeriesDistribution,
    };

    use super::*;
    use crate::dist_plan::{Categorizer, Commutativity};
    use crate::optimizer::test_util::mock_table_provider;

    const FRACTION: SampleMethod = SampleMethod::Series { num: 1, den: 16 };

    fn region_plan(provider: Arc<DummyTableProvider>) -> LogicalPlan {
        LogicalPlanBuilder::scan("metrics", Arc::new(DefaultTableSource::new(provider)), None)
            .unwrap()
            .build()
            .unwrap()
    }

    fn mock_region_plan() -> LogicalPlan {
        region_plan(Arc::new(mock_table_provider(RegionId::new(1024, 1))))
    }

    fn sampled_plan(input: LogicalPlan, method: SampleMethod) -> LogicalPlan {
        SeriesSampleScan::new(input, method).into_logical_plan()
    }

    fn lower(plan: LogicalPlan) -> DataFusionResult<LogicalPlan> {
        // `new(false)` is the region-side engine: it lowers the node.
        let rule = SeriesSampleScanRule::new(false);
        let rewritten = rule.lower(plan)?;
        Ok(rewritten.data)
    }

    /// Returns the scan request of the only table scan of `plan`.
    fn scan_request(plan: &LogicalPlan) -> ScanRequest {
        let LogicalPlan::TableScan(scan) = plan else {
            panic!("expected a lowered table scan, got: {plan}");
        };
        let source = scan
            .source
            .downcast_ref::<DefaultTableSource>()
            .expect("expected the dummy catalog source");
        source
            .table_provider
            .downcast_ref::<DummyTableProvider>()
            .expect("expected a dummy table provider")
            .scan_request()
    }

    #[test]
    fn test_json_roundtrip() {
        for method in [
            SampleMethod::Series { num: 1, den: 16 },
            SampleMethod::PrimaryKey { num: 1, den: 4 },
        ] {
            let node = SeriesSampleScan::new(mock_region_plan(), method);
            let bytes = node.serialize();
            // The payload is JSON, so it needs no protobuf message.
            assert!(
                std::str::from_utf8(&bytes)
                    .unwrap()
                    .contains(&format!("\"num\":{}", method.num())),
                "{}",
                String::from_utf8_lossy(&bytes)
            );
            let restored = SeriesSampleScan::deserialize(&bytes).unwrap();
            assert_eq!(method, restored.method());
            // The input is restored by Substrait, so the payload does not carry it.
            let restored = restored
                .with_exprs_and_inputs(vec![], vec![node.input().clone()])
                .unwrap();
            assert_eq!(node, restored);
        }
    }

    #[test]
    fn test_explain_shows_the_fraction() {
        let plan = sampled_plan(mock_region_plan(), FRACTION);
        let display = plan.display_indent().to_string();
        assert!(
            display.contains("SeriesSampleScan: series 1/16"),
            "{display}"
        );

        let plan = sampled_plan(
            mock_region_plan(),
            SampleMethod::PrimaryKey { num: 1, den: 4 },
        );
        let display = plan.display_indent().to_string();
        assert!(
            display.contains("SeriesSampleScan: primary-key 1/4"),
            "{display}"
        );
    }

    #[test]
    fn test_commutativity() {
        let node = SeriesSampleScan::new(mock_region_plan(), FRACTION);
        assert!(matches!(
            Categorizer::check_extension_plan(&node, &Default::default()),
            Commutativity::Commutative
        ));

        // A sample of something that is not a scan cannot be pushed to a region.
        let input = LogicalPlanBuilder::from(sampled_plan(mock_region_plan(), FRACTION))
            .project(vec![col("v0")])
            .unwrap()
            .build()
            .unwrap();
        let node = SeriesSampleScan::new(input, FRACTION);
        assert!(matches!(
            Categorizer::check_extension_plan(&node, &Default::default()),
            Commutativity::Unsupported
        ));
    }

    #[test]
    fn test_lowering_sets_the_series_sample() {
        let plan = sampled_plan(mock_region_plan(), FRACTION);

        let lowered = lower(plan).unwrap();
        // The node is gone: the plan is the scan.
        assert!(matches!(lowered, LogicalPlan::TableScan(_)), "{lowered}");
        let request = scan_request(&lowered);
        assert_eq!(request.sample, Some(SeriesSample { num: 1, den: 16 }));
        assert_eq!(
            request.distribution,
            Some(TimeSeriesDistribution::PerSeries)
        );
        // A scan samples one dimension only.
        assert_eq!(request.pk_sample, None);
    }

    #[test]
    fn test_lowering_sets_the_primary_key_sample() {
        let plan = sampled_plan(
            mock_region_plan(),
            SampleMethod::PrimaryKey { num: 1, den: 4 },
        );

        let lowered = lower(plan).unwrap();
        let request = scan_request(&lowered);
        assert_eq!(request.pk_sample, Some(PkSample { num: 1, den: 4 }));
        assert_eq!(request.sample, None);
        // Primary-key sampling prunes rows of a key range, not series.
        assert_eq!(request.distribution, None);
    }

    #[test]
    fn test_lowering_rejects_a_non_scan_input() {
        let input = LogicalPlanBuilder::from(mock_region_plan())
            .filter(col("v0").gt(lit(0.0)))
            .unwrap()
            .build()
            .unwrap();
        let error = lower(sampled_plan(input, FRACTION))
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("cannot apply SeriesSampleScan to a plan that is not a table scan"),
            "{error}"
        );
    }

    #[test]
    fn test_lowering_rejects_a_non_region_scan() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "v0",
            DataType::Float64,
            false,
        )]));
        let provider = Arc::new(MemTable::try_new(schema, vec![vec![]]).unwrap());
        let input = LogicalPlanBuilder::scan("classic", provider_as_source(provider), None)
            .unwrap()
            .build()
            .unwrap();
        let plan = sampled_plan(input, FRACTION);

        // The region side only reads region scans, so a catalog scan is a bug.
        let error = lower(plan.clone()).unwrap_err().to_string();
        assert!(
            error.contains("the scan reads a catalog table, not a region"),
            "{error}"
        );

        // The frontend keeps the node: the region it is shipped to lowers it.
        let rule = SeriesSampleScanRule::new(true);
        let rewritten = rule.lower(plan).unwrap();
        assert!(matches!(rewritten.data, LogicalPlan::Extension(_)));
    }
}
