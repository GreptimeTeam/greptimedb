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

use std::fmt::{Display, Formatter};
use std::hash::{DefaultHasher, Hasher};
use std::num::ParseIntError;
use std::str::FromStr;
use std::sync::Arc;

use datafusion_common::{DataFusionError, Result};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::physical_plan::to_proto::serialize_physical_expr;
use prost::Message;
use store_api::storage::RegionId;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct FilterFingerprint(u64);

impl FilterFingerprint {
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

impl Display for FilterFingerprint {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

impl FromStr for FilterFingerprint {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(u64::from_str_radix(s, 16)?))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct FilterId {
    region_id: RegionId,
    producer_ordinal: u32,
    children_fingerprint: FilterFingerprint,
}

// NOTE(remote-dyn-filter): FilterId is generated once by the source-side planner/runtime and then
// propagated through the query lifecycle. Consumers should treat it as the canonical propagated
// identifier instead of independently recomputing it from local state.

impl FilterId {
    pub fn new(
        region_id: RegionId,
        producer_ordinal: u32,
        children_fingerprint: FilterFingerprint,
    ) -> Self {
        Self {
            region_id,
            producer_ordinal,
            children_fingerprint,
        }
    }

    pub fn region_id(&self) -> RegionId {
        self.region_id
    }

    pub fn producer_ordinal(&self) -> u32 {
        self.producer_ordinal
    }

    pub fn children_fingerprint(&self) -> FilterFingerprint {
        self.children_fingerprint
    }
}

impl Display for FilterId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}:{}",
            self.region_id.as_u64(),
            self.producer_ordinal,
            self.children_fingerprint
        )
    }
}

impl FromStr for FilterId {
    type Err = ParseFilterIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split(':');
        let region_id = parts
            .next()
            .ok_or(ParseFilterIdError)?
            .parse::<u64>()
            .map(RegionId::from_u64)
            .map_err(|_| ParseFilterIdError)?;
        let producer_ordinal = parts
            .next()
            .ok_or(ParseFilterIdError)?
            .parse::<u32>()
            .map_err(|_| ParseFilterIdError)?;
        let children_fingerprint = parts
            .next()
            .ok_or(ParseFilterIdError)?
            .parse::<FilterFingerprint>()
            .map_err(|_| ParseFilterIdError)?;
        if parts.next().is_some() {
            return Err(ParseFilterIdError);
        }

        Ok(Self::new(region_id, producer_ordinal, children_fingerprint))
    }
}

#[derive(Debug)]
pub struct ParseFilterIdError;

impl Display for ParseFilterIdError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid filter id")
    }
}

/// Builds the query-local remote dynamic filter identity.
///
/// The identity is `region_id + producer-local ordinal + canonicalized child fingerprint`.
/// Subscriber routing details such as `partition` stay outside this key so they can remain in
/// the later fanout/subscriber map instead of splitting one shared remote filter state.
///
/// NOTE(remote-dyn-filter): This id is generated once on the source side and then propagated.
/// Consumers should reuse the propagated `FilterId` instead of independently recomputing it from
/// local state.
#[allow(unused)]
pub(crate) fn build_remote_dyn_filter_id(
    region_id: RegionId,
    producer_local_ordinal: usize,
    children: &[Arc<dyn PhysicalExpr>],
) -> Result<FilterId> {
    let children_fingerprint = canonicalize_dyn_filter_children(children)?;
    let producer_local_ordinal = u32::try_from(producer_local_ordinal).map_err(|err| {
        let _ = err;
        DataFusionError::Execution("producer ordinal out of range for filter id".to_string())
    })?;
    Ok(FilterId::new(
        region_id,
        producer_local_ordinal,
        children_fingerprint,
    ))
}

fn canonicalize_dyn_filter_children(
    children: &[Arc<dyn PhysicalExpr>],
) -> Result<FilterFingerprint> {
    let codec = DefaultPhysicalExtensionCodec {};
    let mut hasher = DefaultHasher::new();
    hasher.write_usize(children.len());

    for child in children {
        let proto = serialize_physical_expr(child, &codec)?;
        let mut bytes = Vec::new();
        proto
            .encode(&mut bytes)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        hasher.write_usize(bytes.len());
        hasher.write(&bytes);
    }

    Ok(FilterFingerprint::new(hasher.finish()))
}

#[cfg(test)]
mod tests {
    use datafusion_physical_expr::expressions::Column;

    use super::*;

    fn test_children(names: &[&str]) -> Vec<Arc<dyn PhysicalExpr>> {
        names
            .iter()
            .enumerate()
            .map(|(index, name)| Arc::new(Column::new(name, index)) as Arc<dyn PhysicalExpr>)
            .collect()
    }

    #[test]
    fn filter_id_round_trips_through_string() {
        let filter_id = FilterId::new(RegionId::new(1024, 7), 3, FilterFingerprint::new(0xabc));
        let encoded = filter_id.to_string();

        assert_eq!(encoded.parse::<FilterId>().unwrap(), filter_id);
    }

    #[test]
    fn filter_id_rejects_malformed_strings() {
        assert!("".parse::<FilterId>().is_err());
        assert!("1024:3".parse::<FilterId>().is_err());
        assert!("1024:3:zzzz".parse::<FilterId>().is_err());
        assert!("1024:3:0000000000000abc:extra".parse::<FilterId>().is_err());
    }

    #[test]
    fn remote_dyn_filter_id_is_stable_for_equivalent_children() {
        let region_id = RegionId::new(1024, 7);
        let first =
            build_remote_dyn_filter_id(region_id, 3, &test_children(&["host", "pod"])).unwrap();
        let second =
            build_remote_dyn_filter_id(region_id, 3, &test_children(&["host", "pod"])).unwrap();

        assert_eq!(first, second);
    }

    #[test]
    fn remote_dyn_filter_id_changes_when_identity_inputs_change() {
        let children = test_children(&["host", "pod"]);
        let baseline = build_remote_dyn_filter_id(RegionId::new(1024, 7), 3, &children).unwrap();
        let different_region =
            build_remote_dyn_filter_id(RegionId::new(1024, 8), 3, &children).unwrap();
        let different_ordinal =
            build_remote_dyn_filter_id(RegionId::new(1024, 7), 4, &children).unwrap();
        let different_children =
            build_remote_dyn_filter_id(RegionId::new(1024, 7), 3, &test_children(&["pod", "host"]))
                .unwrap();

        assert_ne!(baseline, different_region);
        assert_ne!(baseline, different_ordinal);
        assert_ne!(baseline, different_children);
    }

    #[test]
    fn remote_dyn_filter_id_supports_empty_children() {
        let region_id = RegionId::new(4096, 2);
        let first = build_remote_dyn_filter_id(region_id, 1, &[]).unwrap();
        let second = build_remote_dyn_filter_id(region_id, 1, &[]).unwrap();

        assert_eq!(first, second);
        assert_eq!(first.region_id(), region_id);
        assert_eq!(first.producer_ordinal(), 1);
        assert_eq!(first.children_fingerprint(), second.children_fingerprint());
    }

    #[test]
    fn remote_dyn_filter_id_rejects_out_of_range_producer_ordinal() {
        let error = build_remote_dyn_filter_id(RegionId::new(1024, 7), usize::MAX, &[])
            .unwrap_err()
            .to_string();

        assert!(error.contains("producer ordinal out of range for filter id"));
    }
}
