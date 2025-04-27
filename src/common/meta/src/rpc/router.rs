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

use std::collections::{BTreeMap, HashMap, HashSet};

use api::v1::meta::{
    Partition as PbPartition, Peer as PbPeer, Region as PbRegion, Table as PbTable,
    TableRoute as PbTableRoute,
};
use common_time::util::current_time_millis;
use derive_builder::Builder;
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use snafu::OptionExt;
use store_api::storage::{RegionId, RegionNumber};
use strum::AsRefStr;
use table::table_name::TableName;

use crate::error::{self, Result};
use crate::key::RegionDistribution;
use crate::peer::Peer;
use crate::DatanodeId;

/// Returns the distribution of regions to datanodes.
///
/// The distribution is a map of datanode id to a list of region ids.
/// The list of region ids is sorted in ascending order.
pub fn region_distribution(region_routes: &[RegionRoute]) -> RegionDistribution {
    let mut regions_id_map = RegionDistribution::new();
    for route in region_routes.iter() {
        if let Some(peer) = route.leader_peer.as_ref() {
            let region_id = route.region.id.region_number();
            regions_id_map.entry(peer.id).or_default().push(region_id);
        }
        for peer in route.follower_peers.iter() {
            let region_id = route.region.id.region_number();
            regions_id_map.entry(peer.id).or_default().push(region_id);
        }
    }
    for (_, regions) in regions_id_map.iter_mut() {
        // id asc
        regions.sort()
    }
    regions_id_map
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct TableRoute {
    pub table: Table,
    pub region_routes: Vec<RegionRoute>,
    region_leaders: HashMap<RegionNumber, Option<Peer>>,
}

/// Returns the leader peers of the table.
pub fn find_leaders(region_routes: &[RegionRoute]) -> HashSet<Peer> {
    region_routes
        .iter()
        .flat_map(|x| &x.leader_peer)
        .cloned()
        .collect()
}

/// Returns the followers of the table.
pub fn find_followers(region_routes: &[RegionRoute]) -> HashSet<Peer> {
    region_routes
        .iter()
        .flat_map(|x| &x.follower_peers)
        .cloned()
        .collect()
}

/// Returns the operating leader regions with corresponding [DatanodeId].
pub fn operating_leader_regions(region_routes: &[RegionRoute]) -> Vec<(RegionId, DatanodeId)> {
    region_routes
        .iter()
        .filter_map(|route| {
            route
                .leader_peer
                .as_ref()
                .map(|leader| (route.region.id, leader.id))
        })
        .collect::<Vec<_>>()
}

/// Returns the HashMap<[RegionNumber], &[Peer]>;
///
/// If the region doesn't have a leader peer, the [Region] will be omitted.
pub fn convert_to_region_leader_map(region_routes: &[RegionRoute]) -> HashMap<RegionNumber, &Peer> {
    region_routes
        .iter()
        .filter_map(|x| {
            x.leader_peer
                .as_ref()
                .map(|leader| (x.region.id.region_number(), leader))
        })
        .collect::<HashMap<_, _>>()
}

pub fn find_region_leader(
    region_routes: &[RegionRoute],
    region_number: RegionNumber,
) -> Option<Peer> {
    region_routes
        .iter()
        .find(|x| x.region.id.region_number() == region_number)
        .and_then(|r| r.leader_peer.as_ref())
        .cloned()
}

/// Returns the region numbers of the leader regions on the target datanode.
pub fn find_leader_regions(region_routes: &[RegionRoute], datanode: &Peer) -> Vec<RegionNumber> {
    region_routes
        .iter()
        .filter_map(|x| {
            if let Some(peer) = &x.leader_peer {
                if peer == datanode {
                    return Some(x.region.id.region_number());
                }
            }
            None
        })
        .collect()
}

/// Returns the region numbers of the follower regions on the target datanode.
pub fn find_follower_regions(region_routes: &[RegionRoute], datanode: &Peer) -> Vec<RegionNumber> {
    region_routes
        .iter()
        .filter_map(|x| {
            if x.follower_peers.contains(datanode) {
                return Some(x.region.id.region_number());
            }
            None
        })
        .collect()
}

impl TableRoute {
    pub fn new(table: Table, region_routes: Vec<RegionRoute>) -> Self {
        let region_leaders = region_routes
            .iter()
            .map(|x| (x.region.id.region_number(), x.leader_peer.clone()))
            .collect::<HashMap<_, _>>();
        Self {
            table,
            region_routes,
            region_leaders,
        }
    }

    pub fn try_from_raw(peers: &[PbPeer], table_route: PbTableRoute) -> Result<Self> {
        let table = table_route
            .table
            .context(error::RouteInfoCorruptedSnafu {
                err_msg: "'table' is empty in table route",
            })?
            .try_into()?;

        let mut region_routes = Vec::with_capacity(table_route.region_routes.len());
        for region_route in table_route.region_routes.into_iter() {
            let region = region_route
                .region
                .context(error::RouteInfoCorruptedSnafu {
                    err_msg: "'region' is empty in region route",
                })?
                .into();

            let leader_peer = peers.get(region_route.leader_peer_index as usize).cloned();

            let follower_peers = region_route
                .follower_peer_indexes
                .into_iter()
                .filter_map(|x| peers.get(x as usize).cloned())
                .collect::<Vec<_>>();

            region_routes.push(RegionRoute {
                region,
                leader_peer,
                follower_peers,
                leader_state: None,
                leader_down_since: None,
            });
        }

        Ok(Self::new(table, region_routes))
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct Table {
    pub id: u64,
    pub table_name: TableName,
    #[serde(serialize_with = "as_utf8", deserialize_with = "from_utf8")]
    pub table_schema: Vec<u8>,
}

impl TryFrom<PbTable> for Table {
    type Error = error::Error;

    fn try_from(t: PbTable) -> Result<Self> {
        let table_name = t
            .table_name
            .context(error::RouteInfoCorruptedSnafu {
                err_msg: "table name required",
            })?
            .into();
        Ok(Self {
            id: t.id,
            table_name,
            table_schema: t.table_schema,
        })
    }
}

impl From<Table> for PbTable {
    fn from(table: Table) -> Self {
        PbTable {
            id: table.id,
            table_name: Some(table.table_name.into()),
            table_schema: table.table_schema,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Builder)]
pub struct RegionRoute {
    pub region: Region,
    #[builder(setter(into, strip_option))]
    pub leader_peer: Option<Peer>,
    #[builder(setter(into), default)]
    pub follower_peers: Vec<Peer>,
    /// `None` by default.
    #[builder(setter(into, strip_option), default)]
    #[serde(
        default,
        alias = "leader_status",
        skip_serializing_if = "Option::is_none"
    )]
    pub leader_state: Option<LeaderState>,
    /// The start time when the leader is in `Downgraded` state.
    #[serde(default)]
    #[builder(default = "self.default_leader_down_since()")]
    pub leader_down_since: Option<i64>,
}

impl RegionRouteBuilder {
    fn default_leader_down_since(&self) -> Option<i64> {
        match self.leader_state {
            Some(Some(LeaderState::Downgrading)) => Some(current_time_millis()),
            _ => None,
        }
    }
}

/// The State of the [`Region`] Leader.
/// TODO(dennis): It's better to add more fine-grained statuses such as `PENDING` etc.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, AsRefStr)]
#[strum(serialize_all = "UPPERCASE")]
pub enum LeaderState {
    /// The following cases in which the [`Region`] will be downgraded.
    ///
    /// - The [`Region`] may be unavailable (e.g., Crashed, Network disconnected).
    /// - The [`Region`] was planned to migrate to another [`Peer`].
    #[serde(alias = "Downgraded")]
    Downgrading,
}

impl RegionRoute {
    /// Returns true if the Leader [`Region`] is downgraded.
    ///
    /// The following cases in which the [`Region`] will be downgraded.
    ///
    /// - The [`Region`] is unavailable(e.g., Crashed, Network disconnected).
    /// - The [`Region`] was planned to migrate to another [`Peer`].
    ///
    pub fn is_leader_downgrading(&self) -> bool {
        matches!(self.leader_state, Some(LeaderState::Downgrading))
    }

    /// Marks the Leader [`Region`] as [`RegionState::Downgrading`].
    ///
    /// We should downgrade a [`Region`] before deactivating it:
    ///
    /// - During the [`Region`] Failover Procedure.
    /// - Migrating a [`Region`].
    ///
    /// **Notes:** Meta Server will renewing a special lease(`Downgrading`) for the downgrading [`Region`].
    ///
    /// A downgrading region will reject any write requests, and only allow memetable to be flushed to object storage
    ///
    pub fn downgrade_leader(&mut self) {
        self.leader_down_since = Some(current_time_millis());
        self.leader_state = Some(LeaderState::Downgrading)
    }

    /// Returns how long since the leader is in `Downgraded` state.
    pub fn leader_down_millis(&self) -> Option<i64> {
        self.leader_down_since
            .map(|start| current_time_millis() - start)
    }

    /// Sets the leader state.
    ///
    /// Returns true if updated.
    pub fn set_leader_state(&mut self, state: Option<LeaderState>) -> bool {
        let updated = self.leader_state != state;

        match (state, updated) {
            (Some(LeaderState::Downgrading), true) => {
                self.leader_down_since = Some(current_time_millis());
            }
            (Some(LeaderState::Downgrading), false) => {
                // Do nothing if leader is still in `Downgraded` state.
            }
            _ => {
                self.leader_down_since = None;
            }
        }

        self.leader_state = state;
        updated
    }
}

pub struct RegionRoutes(pub Vec<RegionRoute>);

impl RegionRoutes {
    pub fn region_leader_map(&self) -> HashMap<RegionNumber, &Peer> {
        convert_to_region_leader_map(&self.0)
    }

    pub fn find_region_leader(&self, region_number: RegionNumber) -> Option<&Peer> {
        self.region_leader_map().get(&region_number).copied()
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
pub struct Region {
    pub id: RegionId,
    pub name: String,
    pub partition: Option<Partition>,
    pub attrs: BTreeMap<String, String>,
}

impl Region {
    #[cfg(any(test, feature = "testing"))]
    pub fn new_test(id: RegionId) -> Self {
        Self {
            id,
            ..Default::default()
        }
    }
}

impl From<PbRegion> for Region {
    fn from(r: PbRegion) -> Self {
        Self {
            id: r.id.into(),
            name: r.name,
            partition: r.partition.map(Into::into),
            attrs: r.attrs.into_iter().collect::<BTreeMap<_, _>>(),
        }
    }
}

impl From<Region> for PbRegion {
    fn from(region: Region) -> Self {
        Self {
            id: region.id.into(),
            name: region.name,
            partition: region.partition.map(Into::into),
            attrs: region.attrs.into_iter().collect::<HashMap<_, _>>(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct Partition {
    #[serde(serialize_with = "as_utf8_vec", deserialize_with = "from_utf8_vec")]
    pub column_list: Vec<Vec<u8>>,
    #[serde(serialize_with = "as_utf8_vec", deserialize_with = "from_utf8_vec")]
    pub value_list: Vec<Vec<u8>>,
}

fn as_utf8<S: Serializer>(val: &[u8], serializer: S) -> std::result::Result<S::Ok, S::Error> {
    serializer.serialize_str(
        String::from_utf8(val.to_vec())
            .unwrap_or_else(|_| "<unknown-not-UTF8>".to_string())
            .as_str(),
    )
}

pub fn from_utf8<'de, D>(deserializer: D) -> std::result::Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;

    Ok(s.into_bytes())
}

fn as_utf8_vec<S: Serializer>(
    val: &[Vec<u8>],
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    let mut seq = serializer.serialize_seq(Some(val.len()))?;
    for v in val {
        seq.serialize_element(&String::from_utf8_lossy(v))?;
    }
    seq.end()
}

pub fn from_utf8_vec<'de, D>(deserializer: D) -> std::result::Result<Vec<Vec<u8>>, D::Error>
where
    D: Deserializer<'de>,
{
    let values = Vec::<String>::deserialize(deserializer)?;

    let values = values
        .into_iter()
        .map(|value| value.into_bytes())
        .collect::<Vec<_>>();
    Ok(values)
}

impl From<Partition> for PbPartition {
    fn from(p: Partition) -> Self {
        Self {
            column_list: p.column_list,
            value_list: p.value_list,
        }
    }
}

impl From<PbPartition> for Partition {
    fn from(p: PbPartition) -> Self {
        Self {
            column_list: p.column_list,
            value_list: p.value_list,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_leader_is_downgraded() {
        let mut region_route = RegionRoute {
            region: Region {
                id: 2.into(),
                name: "r2".to_string(),
                partition: None,
                attrs: BTreeMap::new(),
            },
            leader_peer: Some(Peer::new(1, "a1")),
            follower_peers: vec![Peer::new(2, "a2"), Peer::new(3, "a3")],
            leader_state: None,
            leader_down_since: None,
        };

        assert!(!region_route.is_leader_downgrading());

        region_route.downgrade_leader();

        assert!(region_route.is_leader_downgrading());
    }

    #[test]
    fn test_region_route_decode() {
        let region_route = RegionRoute {
            region: Region {
                id: 2.into(),
                name: "r2".to_string(),
                partition: None,
                attrs: BTreeMap::new(),
            },
            leader_peer: Some(Peer::new(1, "a1")),
            follower_peers: vec![Peer::new(2, "a2"), Peer::new(3, "a3")],
            leader_state: None,
            leader_down_since: None,
        };

        let input = r#"{"region":{"id":2,"name":"r2","partition":null,"attrs":{}},"leader_peer":{"id":1,"addr":"a1"},"follower_peers":[{"id":2,"addr":"a2"},{"id":3,"addr":"a3"}]}"#;

        let decoded: RegionRoute = serde_json::from_str(input).unwrap();

        assert_eq!(decoded, region_route);
    }

    #[test]
    fn test_region_route_compatibility() {
        let region_route = RegionRoute {
            region: Region {
                id: 2.into(),
                name: "r2".to_string(),
                partition: None,
                attrs: BTreeMap::new(),
            },
            leader_peer: Some(Peer::new(1, "a1")),
            follower_peers: vec![Peer::new(2, "a2"), Peer::new(3, "a3")],
            leader_state: Some(LeaderState::Downgrading),
            leader_down_since: None,
        };
        let input = r#"{"region":{"id":2,"name":"r2","partition":null,"attrs":{}},"leader_peer":{"id":1,"addr":"a1"},"follower_peers":[{"id":2,"addr":"a2"},{"id":3,"addr":"a3"}],"leader_state":"Downgraded","leader_down_since":null}"#;
        let decoded: RegionRoute = serde_json::from_str(input).unwrap();
        assert_eq!(decoded, region_route);

        let region_route = RegionRoute {
            region: Region {
                id: 2.into(),
                name: "r2".to_string(),
                partition: None,
                attrs: BTreeMap::new(),
            },
            leader_peer: Some(Peer::new(1, "a1")),
            follower_peers: vec![Peer::new(2, "a2"), Peer::new(3, "a3")],
            leader_state: Some(LeaderState::Downgrading),
            leader_down_since: None,
        };
        let input = r#"{"region":{"id":2,"name":"r2","partition":null,"attrs":{}},"leader_peer":{"id":1,"addr":"a1"},"follower_peers":[{"id":2,"addr":"a2"},{"id":3,"addr":"a3"}],"leader_status":"Downgraded","leader_down_since":null}"#;
        let decoded: RegionRoute = serde_json::from_str(input).unwrap();
        assert_eq!(decoded, region_route);

        let region_route = RegionRoute {
            region: Region {
                id: 2.into(),
                name: "r2".to_string(),
                partition: None,
                attrs: BTreeMap::new(),
            },
            leader_peer: Some(Peer::new(1, "a1")),
            follower_peers: vec![Peer::new(2, "a2"), Peer::new(3, "a3")],
            leader_state: Some(LeaderState::Downgrading),
            leader_down_since: None,
        };
        let input = r#"{"region":{"id":2,"name":"r2","partition":null,"attrs":{}},"leader_peer":{"id":1,"addr":"a1"},"follower_peers":[{"id":2,"addr":"a2"},{"id":3,"addr":"a3"}],"leader_state":"Downgrading","leader_down_since":null}"#;
        let decoded: RegionRoute = serde_json::from_str(input).unwrap();
        assert_eq!(decoded, region_route);

        let region_route = RegionRoute {
            region: Region {
                id: 2.into(),
                name: "r2".to_string(),
                partition: None,
                attrs: BTreeMap::new(),
            },
            leader_peer: Some(Peer::new(1, "a1")),
            follower_peers: vec![Peer::new(2, "a2"), Peer::new(3, "a3")],
            leader_state: Some(LeaderState::Downgrading),
            leader_down_since: None,
        };
        let input = r#"{"region":{"id":2,"name":"r2","partition":null,"attrs":{}},"leader_peer":{"id":1,"addr":"a1"},"follower_peers":[{"id":2,"addr":"a2"},{"id":3,"addr":"a3"}],"leader_status":"Downgrading","leader_down_since":null}"#;
        let decoded: RegionRoute = serde_json::from_str(input).unwrap();
        assert_eq!(decoded, region_route);
    }

    #[test]
    fn test_de_serialize_partition() {
        let p = Partition {
            column_list: vec![b"a".to_vec(), b"b".to_vec()],
            value_list: vec![b"hi".to_vec(), b",".to_vec()],
        };

        let output = serde_json::to_string(&p).unwrap();
        let got: Partition = serde_json::from_str(&output).unwrap();

        assert_eq!(got, p);
    }

    #[test]
    fn test_region_distribution() {
        let region_routes = vec![
            RegionRoute {
                region: Region {
                    id: RegionId::new(1, 1),
                    name: "r1".to_string(),
                    partition: None,
                    attrs: BTreeMap::new(),
                },
                leader_peer: Some(Peer::new(1, "a1")),
                follower_peers: vec![Peer::new(2, "a2"), Peer::new(3, "a3")],
                leader_state: None,
                leader_down_since: None,
            },
            RegionRoute {
                region: Region {
                    id: RegionId::new(1, 2),
                    name: "r2".to_string(),
                    partition: None,
                    attrs: BTreeMap::new(),
                },
                leader_peer: Some(Peer::new(2, "a2")),
                follower_peers: vec![Peer::new(1, "a1"), Peer::new(3, "a3")],
                leader_state: None,
                leader_down_since: None,
            },
        ];

        let distribution = region_distribution(&region_routes);
        assert_eq!(distribution.len(), 3);
        assert_eq!(distribution[&1], vec![1, 2]);
        assert_eq!(distribution[&2], vec![1, 2]);
        assert_eq!(distribution[&3], vec![1, 2]);
    }
}
