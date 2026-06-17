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
use std::sync::Arc;

use common_query::request::{
    DynFilterPayload, INITIAL_REMOTE_DYN_FILTER_REGISTRATIONS_EXTENSION_KEY, InitialDynFilterReg,
    InitialDynFilterRegs, InitialDynFilterSnapshot, REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES,
};
use datafusion_common::Result;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::DynamicFilterPhysicalExpr;
use session::context::{QueryContext, QueryContextRef};
use store_api::storage::RegionId;

use crate::dist_plan::filter_id::build_remote_dyn_filter_id;
use crate::dist_plan::{FilterId, QueryDynFilterRegistry, RemoteDynFilterProducerId, Subscriber};

#[derive(Debug, Clone)]
pub(crate) struct CapturedDynFilter {
    filter_id: FilterId,
    initial_registration: InitialDynFilterReg,
    pub(crate) alive_dyn_filter: Arc<DynamicFilterPhysicalExpr>,
}

#[derive(Debug, Clone)]
pub(crate) struct RemoteDynFilterPushdown {
    pub(crate) captured_dyn_filters: Vec<CapturedDynFilter>,
    /// Preflight result per parent filter.
    pub(crate) pushed_down: Vec<bool>,
}

pub(crate) fn capture_remote_dyn_filters_for_pushdown(
    remote_dyn_filter_producer_id: RemoteDynFilterProducerId,
    parent_filters: Vec<Arc<dyn datafusion::physical_plan::PhysicalExpr>>,
) -> RemoteDynFilterPushdown {
    let mut pushed_down = Vec::with_capacity(parent_filters.len());
    let mut captured_dyn_filters = Vec::new();

    for (producer_local_ordinal, filter) in parent_filters.into_iter().enumerate() {
        let Some(alive_dyn_filter) = downcast_dynamic_filter(filter) else {
            pushed_down.push(false);
            continue;
        };

        match build_captured_dyn_filter(
            remote_dyn_filter_producer_id,
            producer_local_ordinal,
            alive_dyn_filter,
        ) {
            Ok(captured_dyn_filter) => {
                pushed_down.push(true);
                captured_dyn_filters.push(captured_dyn_filter);
            }
            Err(error) => {
                common_telemetry::warn!(error; "Remote dyn filter is not pushed down because initial registration cannot be built");
                pushed_down.push(false);
            }
        }
    }

    if let Err(error) = validate_initial_registrations_for_pushdown(&captured_dyn_filters) {
        common_telemetry::warn!(error; "Remote dyn filters are not pushed down because initial registrations are invalid");
        return RemoteDynFilterPushdown {
            captured_dyn_filters: Vec::new(),
            pushed_down: vec![false; pushed_down.len()],
        };
    }

    RemoteDynFilterPushdown {
        captured_dyn_filters,
        pushed_down,
    }
}

fn downcast_dynamic_filter(
    expr: Arc<dyn datafusion::physical_plan::PhysicalExpr>,
) -> Option<Arc<DynamicFilterPhysicalExpr>> {
    (expr as Arc<dyn Any + Send + Sync + 'static>)
        .downcast::<DynamicFilterPhysicalExpr>()
        .ok()
}

pub(crate) fn register_dyn_filters_for_region(
    registry: &QueryDynFilterRegistry,
    region_id: RegionId,
    captured_dyn_filters: &[CapturedDynFilter],
) {
    for captured_dyn_filter in captured_dyn_filters {
        let _ = registry.register_remote_dyn_filter(
            captured_dyn_filter.filter_id.clone(),
            captured_dyn_filter.alive_dyn_filter.clone(),
        );
        let _ = registry
            .register_subscriber(&captured_dyn_filter.filter_id, Subscriber::new(region_id));
    }
}

fn build_captured_dyn_filter(
    remote_dyn_filter_producer_id: RemoteDynFilterProducerId,
    producer_local_ordinal: usize,
    alive_dyn_filter: Arc<DynamicFilterPhysicalExpr>,
) -> Result<CapturedDynFilter> {
    let children = alive_dyn_filter
        .children()
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();
    let filter_id = build_remote_dyn_filter_id(
        remote_dyn_filter_producer_id,
        producer_local_ordinal,
        &children,
    )?;
    let initial_registration =
        InitialDynFilterReg::from_filter_id_and_children(filter_id.to_string(), &children)?;

    Ok(CapturedDynFilter {
        filter_id,
        initial_registration: attach_initial_snapshot(initial_registration, &alive_dyn_filter),
        alive_dyn_filter,
    })
}

fn validate_initial_registrations_for_pushdown(
    captured_dyn_filters: &[CapturedDynFilter],
) -> std::result::Result<(), String> {
    let regs = build_initial_dyn_filter_regs_for_region(captured_dyn_filters);
    regs.validate_default_bounds()?;
    regs.to_extension_value()
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn attach_initial_snapshot(
    initial_registration: InitialDynFilterReg,
    alive_dyn_filter: &DynamicFilterPhysicalExpr,
) -> InitialDynFilterReg {
    let Some(initial_snapshot) = initial_snapshot(alive_dyn_filter) else {
        return initial_registration;
    };

    initial_registration.with_initial_snapshot(initial_snapshot)
}

fn initial_snapshot(
    alive_dyn_filter: &DynamicFilterPhysicalExpr,
) -> Option<InitialDynFilterSnapshot> {
    let generation = alive_dyn_filter.snapshot_generation();
    let current = match alive_dyn_filter.current() {
        Ok(current) => current,
        Err(error) => {
            common_telemetry::warn!(error; "Failed to read remote dyn filter initial snapshot");
            return None;
        }
    };

    let payload = match DynFilterPayload::from_datafusion_expr(
        &current,
        REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES,
    ) {
        Ok(payload) => payload,
        Err(error) => {
            common_telemetry::warn!(error; "Failed to encode remote dyn filter initial snapshot");
            return None;
        }
    };

    // Current DataFusion exposes `wait_complete()`, but no non-blocking completion getter.
    let is_complete = false;
    Some(InitialDynFilterSnapshot::new(
        payload,
        generation,
        is_complete,
    ))
}

fn build_initial_dyn_filter_regs_for_region(
    captured_dyn_filters: &[CapturedDynFilter],
) -> InitialDynFilterRegs {
    InitialDynFilterRegs::new(
        captured_dyn_filters
            .iter()
            .map(|captured| captured.initial_registration.clone())
            .collect(),
    )
}

pub(crate) fn query_context_with_initial_dyn_filter_regs(
    query_ctx: &QueryContextRef,
    region_id: RegionId,
    captured_dyn_filters: &[CapturedDynFilter],
) -> QueryContext {
    let mut region_query_ctx = query_ctx.as_ref().clone();
    let regs = build_initial_dyn_filter_regs_for_region(captured_dyn_filters);
    if regs.is_empty() {
        return region_query_ctx;
    }

    if let Err(error) = regs.validate_default_bounds() {
        common_telemetry::warn!(error; "Dropping initial remote dyn filter registrations for region {} that exceed configured bounds", region_id);
        return region_query_ctx;
    }

    match regs.to_extension_value() {
        Ok(serialized) => region_query_ctx.set_extension(
            INITIAL_REMOTE_DYN_FILTER_REGISTRATIONS_EXTENSION_KEY,
            serialized,
        ),
        Err(error) => {
            common_telemetry::warn!(error; "Failed to serialize initial remote dyn filter registrations");
        }
    }

    region_query_ctx
}

#[cfg(test)]
mod tests {
    use std::fmt;
    use std::hash::{Hash, Hasher};

    use datafusion::execution::TaskContext;
    use datafusion_common::ScalarValue;
    use datafusion_expr::ColumnarValue;
    use datafusion_physical_expr::expressions::{Column, lit};
    use session::query_id::QueryId;
    use uuid::Uuid;

    use super::*;

    #[derive(Debug)]
    struct UnserializableExpr;

    impl fmt::Display for UnserializableExpr {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "unserializable_expr")
        }
    }

    impl Hash for UnserializableExpr {
        fn hash<H: Hasher>(&self, state: &mut H) {
            "unserializable_expr".hash(state);
        }
    }

    impl PartialEq for UnserializableExpr {
        fn eq(&self, _other: &Self) -> bool {
            true
        }
    }

    impl Eq for UnserializableExpr {}

    impl datafusion_physical_expr::PhysicalExpr for UnserializableExpr {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn data_type(
            &self,
            _input_schema: &arrow_schema::Schema,
        ) -> datafusion_common::Result<arrow_schema::DataType> {
            Ok(arrow_schema::DataType::Boolean)
        }

        fn nullable(
            &self,
            _input_schema: &arrow_schema::Schema,
        ) -> datafusion_common::Result<bool> {
            Ok(false)
        }

        fn evaluate(
            &self,
            _batch: &common_recordbatch::DfRecordBatch,
        ) -> datafusion_common::Result<ColumnarValue> {
            Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))))
        }

        fn children(&self) -> Vec<&Arc<dyn datafusion_physical_expr::PhysicalExpr>> {
            Vec::new()
        }

        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn datafusion_physical_expr::PhysicalExpr>>,
        ) -> datafusion_common::Result<Arc<dyn datafusion_physical_expr::PhysicalExpr>> {
            Ok(self)
        }

        fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{self}")
        }
    }

    fn test_query_id(value: u128) -> QueryId {
        QueryId::from(Uuid::from_u128(value))
    }

    fn test_remote_dyn_filter_producer_id(value: u64) -> RemoteDynFilterProducerId {
        RemoteDynFilterProducerId::new(value)
    }

    fn test_captured_dyn_filter(
        remote_dyn_filter_producer_id: RemoteDynFilterProducerId,
        producer_local_ordinal: usize,
        column_name: &str,
        column_index: usize,
    ) -> CapturedDynFilter {
        build_captured_dyn_filter(
            remote_dyn_filter_producer_id,
            producer_local_ordinal,
            Arc::new(DynamicFilterPhysicalExpr::new(
                vec![Arc::new(Column::new(column_name, column_index)) as Arc<_>],
                lit(true) as _,
            )),
        )
        .unwrap()
    }

    fn test_dyn_filter_with_snapshot_payload(
        column_name: &str,
        column_index: usize,
        payload_bytes: usize,
    ) -> Arc<DynamicFilterPhysicalExpr> {
        let dyn_filter = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(Column::new(column_name, column_index)) as Arc<_>],
            lit(true) as _,
        ));
        dyn_filter
            .update(lit(ScalarValue::Utf8(Some("x".repeat(payload_bytes)))) as _)
            .unwrap();
        dyn_filter
    }

    #[test]
    fn capture_remote_dyn_filters_for_pushdown_preserves_parent_filter_ordinals() {
        let parent_filters = vec![
            Arc::new(Column::new("service", 0)) as Arc<dyn datafusion::physical_plan::PhysicalExpr>,
            Arc::new(DynamicFilterPhysicalExpr::new(
                vec![Arc::new(Column::new("host", 1)) as Arc<_>],
                lit(true) as _,
            )) as Arc<dyn datafusion::physical_plan::PhysicalExpr>,
            Arc::new(Column::new("zone", 2)) as Arc<dyn datafusion::physical_plan::PhysicalExpr>,
            Arc::new(DynamicFilterPhysicalExpr::new(
                vec![Arc::new(Column::new("pod", 3)) as Arc<_>],
                lit(true) as _,
            )) as Arc<dyn datafusion::physical_plan::PhysicalExpr>,
        ];

        let remote_dyn_filter_producer_id = test_remote_dyn_filter_producer_id(42);
        let captured =
            capture_remote_dyn_filters_for_pushdown(remote_dyn_filter_producer_id, parent_filters)
                .captured_dyn_filters;

        assert_eq!(captured.len(), 2);
        assert_eq!(
            captured[0].filter_id.remote_dyn_filter_producer_id(),
            remote_dyn_filter_producer_id
        );
        assert_eq!(
            captured[1].filter_id.remote_dyn_filter_producer_id(),
            remote_dyn_filter_producer_id
        );
        assert_eq!(captured[0].filter_id.producer_ordinal(), 1);
        assert_eq!(captured[1].filter_id.producer_ordinal(), 3);
    }

    #[test]
    fn capture_remote_dyn_filters_for_pushdown_marks_only_valid_initial_regs() {
        let parent_filters = vec![
            Arc::new(Column::new("service", 0)) as Arc<dyn datafusion::physical_plan::PhysicalExpr>,
            Arc::new(DynamicFilterPhysicalExpr::new(
                vec![Arc::new(Column::new("host", 1)) as Arc<_>],
                lit(true) as _,
            )) as Arc<dyn datafusion::physical_plan::PhysicalExpr>,
            Arc::new(Column::new("zone", 2)) as Arc<dyn datafusion::physical_plan::PhysicalExpr>,
        ];

        let remote_dyn_filter_producer_id = test_remote_dyn_filter_producer_id(42);
        let pushdown =
            capture_remote_dyn_filters_for_pushdown(remote_dyn_filter_producer_id, parent_filters);

        assert_eq!(pushdown.pushed_down, vec![false, true, false]);
        assert_eq!(pushdown.captured_dyn_filters.len(), 1);
        assert_eq!(
            pushdown.captured_dyn_filters[0]
                .filter_id
                .remote_dyn_filter_producer_id(),
            remote_dyn_filter_producer_id
        );
        assert_eq!(
            pushdown.captured_dyn_filters[0]
                .filter_id
                .producer_ordinal(),
            1
        );
        assert!(
            pushdown.captured_dyn_filters[0]
                .initial_registration
                .initial_snapshot
                .is_some()
        );
    }

    #[test]
    fn capture_remote_dyn_filters_for_pushdown_rejects_unencodable_registration() {
        let parent_filters = vec![Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(UnserializableExpr) as Arc<_>],
            lit(true) as _,
        ))
            as Arc<dyn datafusion::physical_plan::PhysicalExpr>];

        let pushdown = capture_remote_dyn_filters_for_pushdown(
            test_remote_dyn_filter_producer_id(42),
            parent_filters,
        );

        assert_eq!(pushdown.pushed_down, vec![false]);
        assert!(pushdown.captured_dyn_filters.is_empty());
    }

    #[test]
    fn capture_remote_dyn_filters_for_pushdown_attaches_initial_snapshot() {
        let parent_filters = vec![Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(Column::new("host", 1)) as Arc<_>],
            lit(true) as _,
        ))
            as Arc<dyn datafusion::physical_plan::PhysicalExpr>];

        let pushdown = capture_remote_dyn_filters_for_pushdown(
            test_remote_dyn_filter_producer_id(42),
            parent_filters,
        );

        assert_eq!(pushdown.pushed_down, vec![true]);
        assert!(
            pushdown.captured_dyn_filters[0]
                .initial_registration
                .initial_snapshot
                .is_some()
        );
    }

    #[test]
    fn capture_remote_dyn_filters_for_pushdown_attaches_initial_snapshot_after_update() {
        let dyn_filter = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(Column::new("host", 1)) as Arc<_>],
            lit(true) as _,
        ));
        dyn_filter.update(lit(false) as _).unwrap();
        let parent_filters = vec![dyn_filter as Arc<dyn datafusion::physical_plan::PhysicalExpr>];

        let pushdown = capture_remote_dyn_filters_for_pushdown(
            test_remote_dyn_filter_producer_id(42),
            parent_filters,
        );

        assert_eq!(pushdown.pushed_down, vec![true]);
        let snapshot = pushdown.captured_dyn_filters[0]
            .initial_registration
            .initial_snapshot
            .as_ref()
            .unwrap();
        assert_eq!(snapshot.generation, 2);
        assert!(!snapshot.is_complete);
        assert!(matches!(
            snapshot.payload,
            DynFilterPayload::Datafusion(ref bytes) if !bytes.is_empty()
        ));
    }

    #[test]
    fn capture_remote_dyn_filters_for_pushdown_rejects_oversized_snapshots() {
        let oversized_total_snapshot_bytes = REMOTE_DYN_FILTER_PAYLOAD_MAX_BYTES * 3 / 5;
        let parent_filters = vec![
            test_dyn_filter_with_snapshot_payload("host", 0, oversized_total_snapshot_bytes)
                as Arc<dyn datafusion::physical_plan::PhysicalExpr>,
            test_dyn_filter_with_snapshot_payload("pod", 1, oversized_total_snapshot_bytes)
                as Arc<dyn datafusion::physical_plan::PhysicalExpr>,
        ];

        let pushdown = capture_remote_dyn_filters_for_pushdown(
            test_remote_dyn_filter_producer_id(42),
            parent_filters,
        );

        assert_eq!(pushdown.pushed_down, vec![false, false]);
        assert!(pushdown.captured_dyn_filters.is_empty());
    }

    #[test]
    fn capture_remote_dyn_filters_for_pushdown_rejects_too_many_regs_with_snapshots() {
        const TOO_MANY_INITIAL_REGS: usize = 65;

        let parent_filters = (0..TOO_MANY_INITIAL_REGS)
            .map(|ordinal| {
                test_dyn_filter_with_snapshot_payload(&format!("host_{ordinal}"), ordinal, 1)
                    as Arc<dyn datafusion::physical_plan::PhysicalExpr>
            })
            .collect::<Vec<_>>();

        let pushdown = capture_remote_dyn_filters_for_pushdown(
            test_remote_dyn_filter_producer_id(42),
            parent_filters,
        );

        assert!(pushdown.captured_dyn_filters.is_empty());
        assert_eq!(pushdown.pushed_down, vec![false; TOO_MANY_INITIAL_REGS]);
    }

    #[test]
    fn capture_remote_dyn_filters_for_pushdown_rejects_regs_exceeding_bounds() {
        const TOO_MANY_INITIAL_REGS: usize = 65;

        let parent_filters = (0..TOO_MANY_INITIAL_REGS)
            .map(|_| {
                Arc::new(DynamicFilterPhysicalExpr::new(
                    vec![Arc::new(Column::new("host", 0)) as Arc<_>],
                    lit(true) as _,
                )) as Arc<dyn datafusion::physical_plan::PhysicalExpr>
            })
            .collect::<Vec<_>>();

        let pushdown = capture_remote_dyn_filters_for_pushdown(
            test_remote_dyn_filter_producer_id(42),
            parent_filters,
        );

        assert!(pushdown.captured_dyn_filters.is_empty());
        assert_eq!(pushdown.pushed_down, vec![false; TOO_MANY_INITIAL_REGS]);
    }

    #[test]
    fn register_dyn_filters_for_region_reuses_existing_entry() {
        let registry = QueryDynFilterRegistry::new(test_query_id(1));
        let captured_dyn_filters = vec![test_captured_dyn_filter(
            test_remote_dyn_filter_producer_id(42),
            2,
            "host",
            0,
        )];
        let first_region_id = RegionId::new(1024, 7);
        let second_region_id = RegionId::new(1024, 8);

        register_dyn_filters_for_region(&registry, first_region_id, &captured_dyn_filters);
        register_dyn_filters_for_region(&registry, second_region_id, &captured_dyn_filters);

        assert_eq!(registry.entry_count(), 1);
        let entry = registry.entries().pop().unwrap();
        assert_eq!(
            entry.filter_id().remote_dyn_filter_producer_id(),
            test_remote_dyn_filter_producer_id(42)
        );
        assert_eq!(entry.filter_id().producer_ordinal(), 2);
        let subscribers = entry.subscribers();
        assert_eq!(subscribers.len(), 2);
        assert!(
            subscribers
                .iter()
                .any(|subscriber| subscriber.region_id() == first_region_id)
        );
        assert!(
            subscribers
                .iter()
                .any(|subscriber| subscriber.region_id() == second_region_id)
        );
    }

    #[test]
    fn register_dyn_filters_for_region_keeps_independent_producer_ids_distinct() {
        let registry = QueryDynFilterRegistry::new(test_query_id(1));
        let region_id = RegionId::new(1024, 7);
        let make_filter = |remote_dyn_filter_producer_id| {
            test_captured_dyn_filter(remote_dyn_filter_producer_id, 2, "host", 0)
        };

        register_dyn_filters_for_region(
            &registry,
            region_id,
            &[make_filter(test_remote_dyn_filter_producer_id(42))],
        );
        register_dyn_filters_for_region(
            &registry,
            region_id,
            &[make_filter(test_remote_dyn_filter_producer_id(43))],
        );

        assert_eq!(registry.entry_count(), 2);
    }

    #[test]
    fn query_context_includes_region_initial_dyn_filter_regs() {
        let captured_dyn_filters = vec![test_captured_dyn_filter(
            test_remote_dyn_filter_producer_id(42),
            2,
            "host",
            0,
        )];
        let region_id = RegionId::new(1024, 7);
        let query_ctx = QueryContext::arc();

        let region_query_ctx = query_context_with_initial_dyn_filter_regs(
            &query_ctx,
            region_id,
            &captured_dyn_filters,
        );
        let extension = region_query_ctx
            .extension(INITIAL_REMOTE_DYN_FILTER_REGISTRATIONS_EXTENSION_KEY)
            .unwrap();
        let regs = InitialDynFilterRegs::from_extension_value(extension).unwrap();
        let decoded_children = regs.regs[0]
            .decode_children(
                &TaskContext::default(),
                &arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                    "host",
                    arrow_schema::DataType::Utf8,
                    false,
                )]),
                1024,
            )
            .unwrap();
        assert_eq!(regs.regs.len(), 1);
        assert_eq!(
            regs.regs[0].filter_id,
            captured_dyn_filters[0].filter_id.to_string()
        );
        assert_eq!(decoded_children.len(), 1);
        assert!(decoded_children[0].as_any().is::<Column>());
    }

    #[test]
    fn query_context_drops_initial_regs_when_duplicate_filter_ids_exceed_bounds() {
        let captured_dyn_filters = vec![
            test_captured_dyn_filter(test_remote_dyn_filter_producer_id(42), 2, "host", 0),
            test_captured_dyn_filter(test_remote_dyn_filter_producer_id(42), 2, "host", 0),
        ];
        let region_id = RegionId::new(1024, 7);
        let query_ctx = QueryContext::arc();

        let region_query_ctx = query_context_with_initial_dyn_filter_regs(
            &query_ctx,
            region_id,
            &captured_dyn_filters,
        );

        assert!(
            region_query_ctx
                .extension(INITIAL_REMOTE_DYN_FILTER_REGISTRATIONS_EXTENSION_KEY)
                .is_none()
        );
    }
}
