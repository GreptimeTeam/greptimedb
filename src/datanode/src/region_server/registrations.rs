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

use common_query::request::{
    INITIAL_REMOTE_DYN_FILTER_REGISTRATIONS_EXTENSION_KEY, InitialDynFilterRegs,
};
use common_telemetry::warn;
use dashmap::DashMap;
use session::context::QueryContextRef;
use store_api::storage::RegionId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RegisteredDynFilter {
    pub(super) filter_id: String,
    pub(super) child_exprs_datafusion_proto: Vec<Vec<u8>>,
    pub(super) subscriber_regions: Vec<RegionId>,
}

impl RegisteredDynFilter {
    fn new(
        filter_id: String,
        child_exprs_datafusion_proto: Vec<Vec<u8>>,
        region_id: RegionId,
    ) -> Self {
        Self {
            filter_id,
            child_exprs_datafusion_proto,
            subscriber_regions: vec![region_id],
        }
    }

    fn register_region(&mut self, region_id: RegionId) -> bool {
        if self.subscriber_regions.contains(&region_id) {
            return false;
        }

        self.subscriber_regions.push(region_id);
        true
    }

    fn remove_region(&mut self, region_id: RegionId) -> bool {
        let original_len = self.subscriber_regions.len();
        self.subscriber_regions
            .retain(|region| *region != region_id);
        original_len != self.subscriber_regions.len()
    }

    fn has_subscribers(&self) -> bool {
        !self.subscriber_regions.is_empty()
    }

    fn should_drop_after_remove(&mut self, region_id: RegionId) -> bool {
        self.remove_region(region_id) && !self.has_subscribers()
    }
}

pub(super) fn initial_dyn_filter_regs_from_query_ctx(
    query_ctx: &QueryContextRef,
) -> Option<InitialDynFilterRegs> {
    let registrations =
        query_ctx.extension(INITIAL_REMOTE_DYN_FILTER_REGISTRATIONS_EXTENSION_KEY)?;
    match InitialDynFilterRegs::from_extension_value(registrations) {
        Ok(registrations) => match registrations.validate_default_bounds() {
            Ok(()) => Some(registrations),
            Err(error) => {
                warn!(error; "Initial remote dyn filter registrations exceeded Task 03 bounds");
                None
            }
        },
        Err(error) => {
            warn!(error; "Failed to decode initial remote dyn filter registrations from query context");
            None
        }
    }
}

pub(super) fn register_initial_dyn_filter_regs(
    regs_by_query: &DashMap<String, DashMap<String, RegisteredDynFilter>>,
    query_id: &str,
    region_id: RegionId,
    regs: &InitialDynFilterRegs,
) {
    if regs.is_empty() {
        return;
    }

    if let Err(error) = regs.validate_default_bounds() {
        warn!(error; "Ignored invalid initial dyn filter registrations for query_id {} region_id {}", query_id, region_id);
        return;
    }

    let query_regs = regs_by_query
        .entry(query_id.to_string())
        .or_insert_with(DashMap::new);

    for reg in &regs.regs {
        if let Some(mut registered) = query_regs.get_mut(&reg.filter_id) {
            if registered.register_region(region_id) {
                continue;
            }

            warn!(
                query_id,
                filter_id = reg.filter_id,
                region_id = %region_id,
                "Duplicate initial dyn filter reg ignored"
            );
            continue;
        }

        query_regs.insert(
            reg.filter_id.clone(),
            RegisteredDynFilter::new(
                reg.filter_id.clone(),
                reg.child_exprs_datafusion_proto.clone(),
                region_id,
            ),
        );
    }
}

pub(super) fn remove_initial_dyn_filter_regs_for_region(
    regs_by_query: &DashMap<String, DashMap<String, RegisteredDynFilter>>,
    query_id: &str,
    region_id: RegionId,
) {
    let should_remove_query = {
        let Some(query_regs) = regs_by_query.get(query_id) else {
            return;
        };

        let filter_ids_to_remove = query_regs
            .iter_mut()
            .filter_map(|mut registered| {
                registered
                    .should_drop_after_remove(region_id)
                    .then(|| registered.filter_id.clone())
            })
            .collect::<Vec<_>>();

        for filter_id in filter_ids_to_remove {
            query_regs.remove(&filter_id);
        }

        query_regs.is_empty()
    };

    if should_remove_query {
        regs_by_query.remove(query_id);
    }
}
