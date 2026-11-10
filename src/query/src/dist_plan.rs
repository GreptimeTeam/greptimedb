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

mod analyzer;
mod commutativity;
mod dyn_filter_bridge;
mod filter_id;
mod grouped_read;
mod merge_scan;
mod merge_sort;
mod planner;
mod predicate_extractor;
mod region_pruner;
mod remote_dyn_filter_receiver;
mod remote_dyn_filter_registry;

pub use analyzer::{DistPlannerAnalyzer, DistPlannerOptions};
pub use filter_id::{FilterFingerprint, FilterId, ParseFilterIdError, RemoteDynFilterProducerId};
pub use grouped_read::{
    DatanodeReadGroup, GROUP_READ_CONTEXT_EXTENSION, GROUP_READ_NAME, GROUP_READ_VERSION,
    GroupReadEnvelopeError, GroupReadPayload, GroupReadPlan, MAX_GROUP_READ_PAYLOAD_BYTES,
    MAX_GROUP_READ_REGIONS, REGION_PLAN_NAME, RegionPlan, RegionPlanPayload, RegionPlanResolver,
    build_group_read_plan, decode_group_read_request, eligible_remote_plan, encode_nested_plan,
    nested_plan_table, peek_group_read_envelope, validate_region_plan,
};
pub use merge_scan::{MergeScanExec, MergeScanLogicalPlan};
pub(crate) use merge_sort::{MergeSortExec, MergeSortLogicalPlan};
pub use planner::{DistExtensionPlanner, MergeSortExtensionPlanner};
pub use predicate_extractor::PredicateExtractor;
pub use region_pruner::ConstraintPruner;
pub use remote_dyn_filter_receiver::{
    RemoteDynFilterReceiverExtensionPlanner, RemoteDynFilterReceiverInjector,
    RemoteDynFilterReceiverInjectorRef, RemoteDynFilterReceiverLogicalPlan,
};
pub use remote_dyn_filter_registry::{
    DynFilterEntry, DynFilterRegistryManager, EntryRegistration, QueryDynFilterRegistry,
    RemoteDynFilterRegistryLease, Subscriber, SubscriberRegistration,
};
