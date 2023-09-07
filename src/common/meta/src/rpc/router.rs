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
    Partition as PbPartition, Peer as PbPeer, Region as PbRegion, RegionRoute as PbRegionRoute,
    RouteRequest as PbRouteRequest, RouteResponse as PbRouteResponse, Table as PbTable,
    TableId as PbTableId, TableRoute as PbTableRoute, TableRouteValue as PbTableRouteValue,
};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use snafu::OptionExt;
use store_api::storage::{RegionId, RegionNumber};
use table::metadata::TableId;

use crate::error::{self, Result};
use crate::key::RegionDistribution;
use crate::peer::Peer;
use crate::rpc::util;
use crate::table_name::TableName;

#[derive(Debug, Clone, Default)]
pub struct RouteRequest {
    pub table_ids: Vec<TableId>,
}

impl From<RouteRequest> for PbRouteRequest {
    fn from(mut req: RouteRequest) -> Self {
        Self {
            header: None,
            table_ids: req.table_ids.drain(..).map(|id| PbTableId { id }).collect(),
        }
    }
}

impl RouteRequest {
    #[inline]
    pub fn new(table_id: TableId) -> Self {
        Self {
            table_ids: vec![table_id],
        }
    }
}

#[derive(Debug, Clone)]
pub struct RouteResponse {
    pub table_routes: Vec<TableRoute>,
}

impl TryFrom<PbRouteResponse> for RouteResponse {
    type Error = error::Error;

    fn try_from(pb: PbRouteResponse) -> Result<Self> {
        util::check_response_header(pb.header.as_ref())?;

        let table_routes = pb
            .table_routes
            .into_iter()
            .map(|x| TableRoute::try_from_raw(&pb.peers, x))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { table_routes })
    }
}

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

    pub fn try_into_raw(self) -> Result<(Vec<PbPeer>, PbTableRoute)> {
        let mut peers = HashSet::new();
        self.region_routes
            .iter()
            .filter_map(|x| x.leader_peer.as_ref())
            .for_each(|p| {
                let _ = peers.insert(p.clone());
            });
        self.region_routes
            .iter()
            .flat_map(|x| x.follower_peers.iter())
            .for_each(|p| {
                let _ = peers.insert(p.clone());
            });
        let mut peers = peers.into_iter().map(Into::into).collect::<Vec<PbPeer>>();
        peers.sort_by_key(|x| x.id);

        let find_peer = |peer_id: u64| -> u64 {
            peers
                .iter()
                .enumerate()
                .find_map(|(i, x)| {
                    if x.id == peer_id {
                        Some(i as u64)
                    } else {
                        None
                    }
                })
                .unwrap_or_else(|| {
                    panic!("Peer {peer_id} must be present when collecting all peers.")
                })
        };

        let mut region_routes = Vec::with_capacity(self.region_routes.len());
        for region_route in self.region_routes.into_iter() {
            let leader_peer_index = region_route.leader_peer.map(|x| find_peer(x.id)).context(
                error::RouteInfoCorruptedSnafu {
                    err_msg: "'leader_peer' is empty in region route",
                },
            )?;

            let follower_peer_indexes = region_route
                .follower_peers
                .iter()
                .map(|x| find_peer(x.id))
                .collect::<Vec<_>>();

            region_routes.push(PbRegionRoute {
                region: Some(region_route.region.into()),
                leader_peer_index,
                follower_peer_indexes,
            });
        }

        let table_route = PbTableRoute {
            table: Some(self.table.into()),
            region_routes,
        };
        Ok((peers, table_route))
    }

    pub fn find_leaders(&self) -> HashSet<Peer> {
        find_leaders(&self.region_routes)
    }

    pub fn find_leader_regions(&self, datanode: &Peer) -> Vec<RegionNumber> {
        find_leader_regions(&self.region_routes, datanode)
    }

    pub fn find_region_leader(&self, region_number: RegionNumber) -> Option<&Peer> {
        self.region_leaders
            .get(&region_number)
            .and_then(|x| x.as_ref())
    }
}

impl TryFrom<PbTableRouteValue> for TableRoute {
    type Error = error::Error;

    fn try_from(pb: PbTableRouteValue) -> Result<Self> {
        TableRoute::try_from_raw(
            &pb.peers,
            pb.table_route.context(error::InvalidProtoMsgSnafu {
                err_msg: "expected table_route",
            })?,
        )
    }
}

impl TryFrom<TableRoute> for PbTableRouteValue {
    type Error = error::Error;

    fn try_from(table_route: TableRoute) -> Result<Self> {
        let (peers, table_route) = table_route.try_into_raw()?;

        Ok(PbTableRouteValue {
            peers,
            table_route: Some(table_route),
        })
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
    use api::v1::meta::{
        Partition as PbPartition, Peer as PbPeer, Region as PbRegion, RegionRoute as PbRegionRoute,
        RouteRequest as PbRouteRequest, RouteResponse as PbRouteResponse, Table as PbTable,
        TableName as PbTableName, TableRoute as PbTableRoute,
    };

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

    #[test]
    fn test_route_request_trans() {
        let req = RouteRequest {
            table_ids: vec![1, 2],
        };

        let into_req: PbRouteRequest = req.into();

        assert!(into_req.header.is_none());
        assert_eq!(1, into_req.table_ids.get(0).unwrap().id);
        assert_eq!(2, into_req.table_ids.get(1).unwrap().id);
    }

    #[test]
    fn test_route_response_trans() {
        let res = PbRouteResponse {
            header: None,
            peers: vec![
                PbPeer {
                    id: 1,
                    addr: "peer1".to_string(),
                },
                PbPeer {
                    id: 2,
                    addr: "peer2".to_string(),
                },
            ],
            table_routes: vec![PbTableRoute {
                table: Some(PbTable {
                    id: 1,
                    table_name: Some(PbTableName {
                        catalog_name: "c1".to_string(),
                        schema_name: "s1".to_string(),
                        table_name: "t1".to_string(),
                    }),
                    table_schema: b"schema".to_vec(),
                }),
                region_routes: vec![PbRegionRoute {
                    region: Some(PbRegion {
                        id: 1,
                        name: "region1".to_string(),
                        partition: Some(PbPartition {
                            column_list: vec![b"c1".to_vec(), b"c2".to_vec()],
                            value_list: vec![b"v1".to_vec(), b"v2".to_vec()],
                        }),
                        attrs: Default::default(),
                    }),
                    leader_peer_index: 0,
                    follower_peer_indexes: vec![1],
                }],
            }],
        };

        let res: RouteResponse = res.try_into().unwrap();
        let mut table_routes = res.table_routes;
        assert_eq!(1, table_routes.len());
        let table_route = table_routes.remove(0);
        let table = table_route.table;
        assert_eq!(1, table.id);
        assert_eq!("c1", table.table_name.catalog_name);
        assert_eq!("s1", table.table_name.schema_name);
        assert_eq!("t1", table.table_name.table_name);

        let mut region_routes = table_route.region_routes;
        assert_eq!(1, region_routes.len());
        let region_route = region_routes.remove(0);
        let region = region_route.region;
        assert_eq!(1, region.id);
        assert_eq!("region1", region.name);
        let partition = region.partition.unwrap();
        assert_eq!(vec![b"c1".to_vec(), b"c2".to_vec()], partition.column_list);
        assert_eq!(vec![b"v1".to_vec(), b"v2".to_vec()], partition.value_list);

        assert_eq!(1, region_route.leader_peer.as_ref().unwrap().id);
        assert_eq!("peer1", region_route.leader_peer.as_ref().unwrap().addr);

        assert_eq!(1, region_route.follower_peers.len());
        assert_eq!(2, region_route.follower_peers.get(0).unwrap().id);
        assert_eq!("peer2", region_route.follower_peers.get(0).unwrap().addr);
    }

    #[test]
    fn test_table_route_raw_conversion() {
        let raw_peers = vec![
            PbPeer {
                id: 1,
                addr: "a1".to_string(),
            },
            PbPeer {
                id: 2,
                addr: "a2".to_string(),
            },
            PbPeer {
                id: 3,
                addr: "a3".to_string(),
            },
        ];

        // region distribution:
        // region id => leader peer id + [follower peer id]
        // 1 => 2 + [1, 3]
        // 2 => 1 + [2, 3]

        let raw_table_route = PbTableRoute {
            table: Some(PbTable {
                id: 1,
                table_name: Some(PbTableName {
                    catalog_name: "c1".to_string(),
                    schema_name: "s1".to_string(),
                    table_name: "t1".to_string(),
                }),
                table_schema: vec![],
            }),
            region_routes: vec![
                PbRegionRoute {
                    region: Some(PbRegion {
                        id: 1,
                        name: "r1".to_string(),
                        partition: None,
                        attrs: HashMap::new(),
                    }),
                    leader_peer_index: 1,
                    follower_peer_indexes: vec![0, 2],
                },
                PbRegionRoute {
                    region: Some(PbRegion {
                        id: 2,
                        name: "r2".to_string(),
                        partition: None,
                        attrs: HashMap::new(),
                    }),
                    leader_peer_index: 0,
                    follower_peer_indexes: vec![1, 2],
                },
            ],
        };
        let table_route = TableRoute {
            table: Table {
                id: 1,
                table_name: TableName::new("c1", "s1", "t1"),
                table_schema: vec![],
            },
            region_routes: vec![
                RegionRoute {
                    region: Region {
                        id: 1.into(),
                        name: "r1".to_string(),
                        partition: None,
                        attrs: BTreeMap::new(),
                    },
                    leader_peer: Some(Peer::new(2, "a2")),
                    follower_peers: vec![Peer::new(1, "a1"), Peer::new(3, "a3")],
                },
                RegionRoute {
                    region: Region {
                        id: 2.into(),
                        name: "r2".to_string(),
                        partition: None,
                        attrs: BTreeMap::new(),
                    },
                    leader_peer: Some(Peer::new(1, "a1")),
                    follower_peers: vec![Peer::new(2, "a2"), Peer::new(3, "a3")],
                },
            ],
            region_leaders: HashMap::from([
                (2, Some(Peer::new(1, "a1"))),
                (1, Some(Peer::new(2, "a2"))),
            ]),
        };

        let from_raw = TableRoute::try_from_raw(&raw_peers, raw_table_route.clone()).unwrap();
        assert_eq!(from_raw, table_route);

        let into_raw = table_route.try_into_raw().unwrap();
        assert_eq!(into_raw.0, raw_peers);
        assert_eq!(into_raw.1, raw_table_route);
    }
}
