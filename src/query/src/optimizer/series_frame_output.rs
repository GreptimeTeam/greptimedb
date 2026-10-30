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

use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{DataFusionError, Result as DfResult};
use datatypes::series::frame::{FrameContent, FrameLabelMode, SeriesFrameDeclaration};
use promql::extension_plan::SeriesRepartitionExec;

/// Ensures a multi-partition series-frame *root* is emitted through one output partition.
///
/// The client receives one stream, and a series frame is not safe to coalesce: a stored series may
/// be split across partitions and a chunk's `is_last` closes only its own producer. The router
/// merges each complete identity first, so an evaluated or explicit-label raw root leaves here
/// through it.
///
/// The view is the root of a query-engine execution, which in a distributed statement is one
/// region's own root as well: a region answers its part of the statement through the same engine,
/// so the same coalescing would otherwise apply to the frames it hands upstream.
///
/// This rule normalizes the plan's root and nothing else. A single-partition root is left alone:
/// one producer's finality is the whole answer when that producer is the entire output, and the
/// router's job is reconciling several producers rather than raw content as such. Frame edges
/// below the root stay the responsibility of the operator that builds them - `FusedSeries` inserts
/// its own owner where a consumer needs one - so this is not a guard against every frame-shaped
/// subplan an arbitrary caller might execute directly.
#[derive(Debug, Default)]
pub struct SeriesFrameOutput;

impl PhysicalOptimizerRule for SeriesFrameOutput {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let schema = plan.schema();
        if !schema
            .metadata()
            .keys()
            .any(|key| key.starts_with("greptime.series."))
        {
            return Ok(plan);
        }

        let declaration = SeriesFrameDeclaration::parse(schema.as_ref()).map_err(|error| {
            DataFusionError::Plan(format!("invalid series frame declaration: {error}"))
        })?;
        if plan.properties().output_partitioning().partition_count() <= 1 {
            return Ok(plan);
        }
        // A raw frame reaches the client through the same single stream, and ordinary batch
        // coalescing cannot produce it: the chunks of one stored series may sit in different
        // partitions, and a per-stream `is_last` is not the series' global end. The router
        // reconstructs each complete primary key and recomputes finality, so it is the only
        // correct exit. It needs explicit labels to check that one key has one binding; a key-only
        // frame has to stay rejected rather than silently coalesced.
        // No production plan reaches the rejection below with a key-only frame: the only producer
        // is the region's series scan, and every plan that executes it puts `RawSeriesSourceExec`
        // or `FusedSeries` above it, both of which publish explicit labels. The rejection is what
        // keeps that true rather than a cost paid by a reachable query.
        if declaration.content() == FrameContent::Raw
            && declaration.labels() != FrameLabelMode::Explicit
        {
            return Err(DataFusionError::Plan(
                "a multi-partition raw series frame needs explicit labels to be merged into one \
                 output stream"
                    .into(),
            ));
        }

        Ok(Arc::new(SeriesRepartitionExec::try_new(plan, 1)?))
    }

    fn name(&self) -> &str {
        "SeriesFrameOutput"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::Schema;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datatypes::series::frame::raw_series_frame_schema;
    use datatypes::series::samples::{FieldSpec, SeriesSchema};
    use promql::extension_plan::SeriesRouting;

    use super::*;

    fn raw_frame_schema() -> Arc<Schema> {
        let series = SeriesSchema {
            fields: Arc::from([FieldSpec {
                name: Arc::from("value"),
            }]),
            eval_grid: None,
        };
        raw_series_frame_schema(&series, FrameLabelMode::Explicit)
            .expect("a raw frame declares its layout")
    }

    /// A plan declaring `partitions` empty explicit-label raw-frame partitions.
    fn raw_plan(partitions: usize) -> Arc<dyn ExecutionPlan> {
        let schema = raw_frame_schema();
        let batch = RecordBatch::new_empty(Arc::clone(&schema));
        let batches = (0..partitions)
            .map(|_| vec![batch.clone()])
            .collect::<Vec<_>>();
        let source = MemorySourceConfig::try_new(&batches, schema, None)
            .expect("the fixture batches match the frame schema");
        Arc::new(DataSourceExec::new(Arc::new(source)))
    }

    fn optimized(plan: Arc<dyn ExecutionPlan>) -> DfResult<Arc<dyn ExecutionPlan>> {
        SeriesFrameOutput.optimize(plan, &ConfigOptions::default())
    }

    #[test]
    fn a_multi_partition_raw_root_is_routed_into_one_output() {
        let plan = raw_plan(3);
        assert_eq!(3, plan.properties().output_partitioning().partition_count());

        let exec = optimized(plan).expect("an explicit-label raw root is supported");
        assert_eq!(
            1,
            exec.properties().output_partitioning().partition_count(),
            "the client sees one raw stream"
        );
        let router = exec
            .downcast_ref::<SeriesRepartitionExec>()
            .expect("the exit is the shared router");
        assert_eq!(
            &SeriesRouting::RawPrimaryKey,
            router.routing_policy(),
            "raw identities meet at their complete primary key"
        );
        assert_eq!(
            raw_frame_schema(),
            exec.schema(),
            "the frame schema is kept"
        );
    }

    #[test]
    fn a_single_partition_raw_root_is_left_alone() {
        let plan = raw_plan(1);
        let exec = optimized(Arc::clone(&plan)).expect("nothing to normalize");
        assert!(
            Arc::ptr_eq(&plan, &exec),
            "one producer is already the whole answer"
        );
    }

    #[test]
    fn the_rule_does_not_wrap_its_own_output_again() {
        let routed = optimized(raw_plan(3)).unwrap();
        let again = optimized(Arc::clone(&routed)).unwrap();
        assert!(
            Arc::ptr_eq(&routed, &again),
            "the exit declares one partition"
        );
    }

    #[test]
    fn a_plan_without_a_frame_declaration_is_untouched() {
        let schema = Arc::new(Schema::empty());
        let batch = RecordBatch::new_empty(Arc::clone(&schema));
        let source =
            MemorySourceConfig::try_new(&[vec![batch.clone()], vec![batch]], schema, None).unwrap();
        let plan: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(source)));
        let exec = optimized(Arc::clone(&plan)).expect("unrelated plans pass through");
        assert!(Arc::ptr_eq(&plan, &exec));
    }
}
