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
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use snafu::OptionExt;
use store_api::storage::{RegionId, RegionNumber};

use crate::error::{self, Result};
use crate::key::RegionDistribution;
use crate::peer::Peer;
use crate::table_name::TableName;

pub fn region_distribution(region_routes: &[RegionRoute]) -> Result<RegionDistribution> {
    let mut regions_id_map = RegionDistribution::new();
    for route in region_routes.iter() {
        if let Some(peer) = route.leader_peer.as_ref() {
            let region_id = route.region.id.region_number();
            regions_id_map.entry(peer.id).or_default().push(region_id);
        }
    }
    for (_, regions) in regions_id_map.iter_mut() {
        // id asc
        regions.sort()
    }
    Ok(regions_id_map)
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct TableRoute {
    pub table: Table,
    pub region_routes: Vec<RegionRoute>,
    region_leaders: HashMap<RegionNumber, Option<Peer>>,
}

pub fn find_leaders(region_routes: &[RegionRoute]) -> HashSet<Peer> {
    region_routes
        .iter()
        .flat_map(|x| &x.leader_peer)
        .cloned()
        .collect()
}

pub fn convert_to_region_map(region_routes: &[RegionRoute]) -> HashMap<u32, &Peer> {
    region_routes
        .iter()
        .filter_map(|x| {
            x.leader_peer
                .as_ref()
                .map(|leader| (x.region.id.region_number(), leader))
        })
        .collect::<HashMap<_, _>>()
}

pub fn find_region_leader(region_routes: &[RegionRoute], region_number: u32) -> Option<&Peer> {
    region_routes
        .iter()
        .find(|x| x.region.id.region_number() == region_number)
        .and_then(|r| r.leader_peer.as_ref())
}

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

pub fn extract_all_peers(region_routes: &[RegionRoute]) -> Vec<Peer> {
    let mut peers = region_routes
        .iter()
        .flat_map(|x| x.leader_peer.iter().chain(x.follower_peers.iter()))
        .collect::<HashSet<_>>()
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();
    peers.sort_by_key(|x| x.id);

    peers
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

            let leader_peer = peers
                .get(region_route.leader_peer_index as usize)
                .cloned()
                .map(Into::into);

            let follower_peers = region_route
                .follower_peer_indexes
                .into_iter()
                .filter_map(|x| peers.get(x as usize).cloned().map(Into::into))
                .collect::<Vec<_>>();

            region_routes.push(RegionRoute {
                region,
                leader_peer,
                follower_peers,
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

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
pub struct RegionRoute {
    pub region: Region,
    pub leader_peer: Option<Peer>,
    pub follower_peers: Vec<Peer>,
}

pub struct RegionRoutes(pub Vec<RegionRoute>);

impl RegionRoutes {
    pub fn region_map(&self) -> HashMap<u32, &Peer> {
        convert_to_region_map(&self.0)
    }

    pub fn find_region_leader(&self, region_number: u32) -> Option<&Peer> {
        self.region_map().get(&region_number).copied()
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
pub struct Region {
    pub id: RegionId,
    pub name: String,
    pub partition: Option<Partition>,
    pub attrs: BTreeMap<String, String>,
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
    fn test_de_serialize_partition() {
        let p = Partition {
            column_list: vec![b"a".to_vec(), b"b".to_vec()],
            value_list: vec![b"hi".to_vec(), b",".to_vec()],
        };

        let output = serde_json::to_string(&p).unwrap();
        let got: Partition = serde_json::from_str(&output).unwrap();

        assert_eq!(got, p);
    }
}
