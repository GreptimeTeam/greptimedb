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
    INITIAL_REMOTE_DYN_FILTER_REGISTRATIONS_EXTENSION_KEY, InitialDynFilterReg,
    InitialDynFilterRegs,
};
use datafusion::execution::TaskContext;
use datafusion_common::Result;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::DynamicFilterPhysicalExpr;
#[cfg(test)]
use datafusion_physical_expr::expressions::{Column, lit};
use session::context::{QueryContext, QueryContextRef};
#[cfg(test)]
use session::query_id::QueryId;
use store_api::storage::RegionId;
#[cfg(test)]
use uuid::Uuid;

use crate::dist_plan::filter_id::build_remote_dyn_filter_id;
use crate::dist_plan::{FilterId, ProducerScopeId, QueryDynFilterRegistry, Subscriber};
use crate::query_engine::QueryEngineState;

#[derive(Debug, Clone)]
pub(crate) struct CapturedDynFilter {
    pub(crate) producer_scope_id: ProducerScopeId,
    pub(crate) producer_local_ordinal: usize,
    pub(crate) alive_dyn_filter: Arc<DynamicFilterPhysicalExpr>,
}

#[derive(Debug, Clone)]
struct ResolvedDynFilter {
    filter_id: FilterId,
    alive_dyn_filter: Arc<DynamicFilterPhysicalExpr>,
    children: Vec<Arc<dyn datafusion::physical_plan::PhysicalExpr>>,
}

pub(crate) fn capture_remote_dyn_filters(
    producer_scope_id: ProducerScopeId,
    parent_filters: Vec<Arc<dyn datafusion::physical_plan::PhysicalExpr>>,
) -> Vec<CapturedDynFilter> {
    parent_filters
        .into_iter()
        .enumerate()
        .filter_map(|(producer_local_ordinal, filter)| {
            downcast_dynamic_filter(filter).map(|alive_dyn_filter| CapturedDynFilter {
                producer_scope_id,
                producer_local_ordinal,
                alive_dyn_filter,
            })
        })
        .collect()
}

fn downcast_dynamic_filter(
    expr: Arc<dyn datafusion::physical_plan::PhysicalExpr>,
) -> Option<Arc<DynamicFilterPhysicalExpr>> {
    (expr as Arc<dyn Any + Send + Sync + 'static>)
        .downcast::<DynamicFilterPhysicalExpr>()
        .ok()
}

fn query_engine_state_from_task_context(context: &TaskContext) -> Option<Arc<QueryEngineState>> {
    context.session_config().get_extension()
}

pub(crate) fn register_dyn_filters_for_region(
    registry: &QueryDynFilterRegistry,
    region_id: RegionId,
    captured_dyn_filters: &[CapturedDynFilter],
) {
    for resolved_filter in resolved_dyn_filters(region_id, captured_dyn_filters) {
        let _ = registry.register_remote_dyn_filter(
            resolved_filter.filter_id.clone(),
            resolved_filter.alive_dyn_filter,
        );
        let _ =
            registry.register_subscriber(&resolved_filter.filter_id, Subscriber::new(region_id));
    }
}

pub(crate) fn bridge_dyn_filters_for_region(
    context: &TaskContext,
    query_ctx: &QueryContextRef,
    region_id: RegionId,
    captured_dyn_filters: &[CapturedDynFilter],
) {
    if captured_dyn_filters.is_empty() {
        return;
    }

    let Some(query_engine_state) = query_engine_state_from_task_context(context) else {
        return;
    };
    let Some(registry) = query_engine_state.get_or_init_remote_dyn_filter_registry(query_ctx)
    else {
        return;
    };

    register_dyn_filters_for_region(&registry, region_id, captured_dyn_filters);
}

fn resolve_dyn_filter(
    region_id: RegionId,
    captured_dyn_filter: &CapturedDynFilter,
) -> Result<ResolvedDynFilter> {
    let children = captured_dyn_filter
        .alive_dyn_filter
        .children()
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();
    let filter_id = build_remote_dyn_filter_id(
        region_id,
        captured_dyn_filter.producer_scope_id,
        captured_dyn_filter.producer_local_ordinal,
        &children,
    )?;

    Ok(ResolvedDynFilter {
        filter_id,
        alive_dyn_filter: captured_dyn_filter.alive_dyn_filter.clone(),
        children,
    })
}

fn resolved_dyn_filters(
    region_id: RegionId,
    captured_dyn_filters: &[CapturedDynFilter],
) -> Vec<ResolvedDynFilter> {
    captured_dyn_filters
        .iter()
        .filter_map(|captured_dyn_filter| resolve_dyn_filter(region_id, captured_dyn_filter).ok())
        .collect()
}

fn build_initial_dyn_filter_regs_for_region(
    region_id: RegionId,
    captured_dyn_filters: &[CapturedDynFilter],
) -> InitialDynFilterRegs {
    InitialDynFilterRegs::new(
        resolved_dyn_filters(region_id, captured_dyn_filters)
            .into_iter()
            .filter_map(|resolved_filter| {
                match InitialDynFilterReg::from_filter_id_and_children(
                    resolved_filter.filter_id.to_string(),
                    &resolved_filter.children,
                ) {
                    Ok(reg) => Some(reg),
                    Err(error) => {
                        common_telemetry::warn!(error; "Failed to encode initial remote dyn filter registration");
                        None
                    }
                }
            })
            .collect(),
    )
}

pub(crate) fn query_context_with_initial_dyn_filter_regs(
    query_ctx: &QueryContextRef,
    region_id: RegionId,
    captured_dyn_filters: &[CapturedDynFilter],
) -> QueryContext {
    let mut region_query_ctx = query_ctx.as_ref().clone();
    let regs = build_initial_dyn_filter_regs_for_region(region_id, captured_dyn_filters);
    if regs.is_empty() {
        return region_query_ctx;
    }

    if let Err(error) = regs.validate_default_bounds() {
        common_telemetry::warn!(error; "Dropping initial remote dyn filter registrations for region {} that exceed Task 03 bounds", region_id);
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
    use super::*;

    fn test_query_id(value: u128) -> QueryId {
        QueryId::from(Uuid::from_u128(value))
    }

    fn test_producer_scope(value: u64) -> ProducerScopeId {
        ProducerScopeId::new(value)
    }

    #[test]
    fn capture_remote_dyn_filters_preserves_parent_filter_ordinals() {
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

        let producer_scope_id = test_producer_scope(42);
        let captured = capture_remote_dyn_filters(producer_scope_id, parent_filters);

        assert_eq!(captured.len(), 2);
        assert_eq!(captured[0].producer_scope_id, producer_scope_id);
        assert_eq!(captured[1].producer_scope_id, producer_scope_id);
        assert_eq!(captured[0].producer_local_ordinal, 1);
        assert_eq!(captured[1].producer_local_ordinal, 3);
    }

    #[test]
    fn register_dyn_filters_for_region_reuses_existing_entry() {
        let registry = QueryDynFilterRegistry::new(test_query_id(1));
        let captured_dyn_filters = vec![CapturedDynFilter {
            producer_scope_id: test_producer_scope(42),
            producer_local_ordinal: 2,
            alive_dyn_filter: Arc::new(DynamicFilterPhysicalExpr::new(
                vec![Arc::new(Column::new("host", 0)) as Arc<_>],
                lit(true) as _,
            )),
        }];
        let region_id = RegionId::new(1024, 7);

        register_dyn_filters_for_region(&registry, region_id, &captured_dyn_filters);
        register_dyn_filters_for_region(&registry, region_id, &captured_dyn_filters);

        assert_eq!(registry.entry_count(), 1);
        let entry = registry.entries().pop().unwrap();
        assert_eq!(
            entry.filter_id().producer_scope_id(),
            test_producer_scope(42)
        );
        assert_eq!(entry.filter_id().producer_ordinal(), 2);
        assert_eq!(entry.subscribers().len(), 1);
        assert_eq!(entry.subscribers()[0].region_id(), region_id);
    }

    #[test]
    fn register_dyn_filters_for_region_keeps_independent_producer_scopes_distinct() {
        let registry = QueryDynFilterRegistry::new(test_query_id(1));
        let region_id = RegionId::new(1024, 7);
        let make_filter = |producer_scope_id| CapturedDynFilter {
            producer_scope_id,
            producer_local_ordinal: 2,
            alive_dyn_filter: Arc::new(DynamicFilterPhysicalExpr::new(
                vec![Arc::new(Column::new("host", 0)) as Arc<_>],
                lit(true) as _,
            )),
        };

        register_dyn_filters_for_region(
            &registry,
            region_id,
            &[make_filter(test_producer_scope(42))],
        );
        register_dyn_filters_for_region(
            &registry,
            region_id,
            &[make_filter(test_producer_scope(43))],
        );

        assert_eq!(registry.entry_count(), 2);
    }

    #[test]
    fn query_context_includes_region_initial_dyn_filter_regs() {
        let captured_dyn_filters = vec![CapturedDynFilter {
            producer_scope_id: test_producer_scope(42),
            producer_local_ordinal: 2,
            alive_dyn_filter: Arc::new(DynamicFilterPhysicalExpr::new(
                vec![Arc::new(Column::new("host", 0)) as Arc<_>],
                lit(true) as _,
            )),
        }];
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
        let expected_filter_id = build_remote_dyn_filter_id(
            region_id,
            captured_dyn_filters[0].producer_scope_id,
            captured_dyn_filters[0].producer_local_ordinal,
            &[Arc::new(Column::new("host", 0)) as Arc<_>],
        )
        .unwrap();

        assert_eq!(regs.regs.len(), 1);
        assert_eq!(regs.regs[0].filter_id, expected_filter_id.to_string());
        assert_eq!(decoded_children.len(), 1);
        assert!(decoded_children[0].as_any().is::<Column>());
    }

    #[test]
    fn query_context_drops_initial_regs_when_duplicate_filter_ids_exceed_bounds() {
        let captured_dyn_filters = vec![
            CapturedDynFilter {
                producer_scope_id: test_producer_scope(42),
                producer_local_ordinal: 2,
                alive_dyn_filter: Arc::new(DynamicFilterPhysicalExpr::new(
                    vec![Arc::new(Column::new("host", 0)) as Arc<_>],
                    lit(true) as _,
                )),
            },
            CapturedDynFilter {
                producer_scope_id: test_producer_scope(42),
                producer_local_ordinal: 2,
                alive_dyn_filter: Arc::new(DynamicFilterPhysicalExpr::new(
                    vec![Arc::new(Column::new("host", 0)) as Arc<_>],
                    lit(true) as _,
                )),
            },
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
