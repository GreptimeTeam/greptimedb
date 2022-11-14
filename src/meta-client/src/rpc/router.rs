use std::collections::HashMap;

use api::v1::meta::CreateRequest as PbCreateRequest;
use api::v1::meta::Partition as PbPartition;
use api::v1::meta::Region as PbRegion;
use api::v1::meta::RouteRequest as PbRouteRequest;
use api::v1::meta::RouteResponse as PbRouteResponse;
use api::v1::meta::Table as PbTable;
use serde::{Deserialize, Serialize, Serializer};
use snafu::OptionExt;

use super::util;
use super::Peer;
use super::TableName;
use crate::error;
use crate::error::Result;

#[derive(Debug, Clone, Default)]
pub struct RouteRequest {
    pub table_names: Vec<TableName>,
}

impl From<RouteRequest> for PbRouteRequest {
    fn from(mut req: RouteRequest) -> Self {
        Self {
            header: None,
            table_names: req.table_names.drain(..).map(Into::into).collect(),
        }
    }
}

impl RouteRequest {
    #[inline]
    pub fn new() -> Self {
        Self {
            table_names: vec![],
        }
    }

    #[inline]
    pub fn add_table_name(mut self, table_name: TableName) -> Self {
        self.table_names.push(table_name);
        self
    }
}

#[derive(Debug, Clone)]
pub struct CreateRequest {
    pub table_name: TableName,
    pub partitions: Vec<Partition>,
}

impl From<CreateRequest> for PbCreateRequest {
    fn from(mut req: CreateRequest) -> Self {
        Self {
            header: None,
            table_name: Some(req.table_name.into()),
            partitions: req.partitions.drain(..).map(Into::into).collect(),
        }
    }
}

impl CreateRequest {
    #[inline]
    pub fn new(table_name: TableName) -> Self {
        Self {
            table_name,
            partitions: vec![],
        }
    }

    #[inline]
    pub fn add_partition(mut self, partition: Partition) -> Self {
        self.partitions.push(partition);
        self
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

        let peers: Vec<Peer> = pb.peers.into_iter().map(Into::into).collect();
        let get_peer = |index: u64| peers.get(index as usize).map(ToOwned::to_owned);
        let mut table_routes = Vec::with_capacity(pb.table_routes.len());
        for table_route in pb.table_routes.into_iter() {
            let table = table_route
                .table
                .context(error::RouteInfoCorruptedSnafu {
                    err_msg: "table required",
                })?
                .try_into()?;

            let mut region_routes = Vec::with_capacity(table_route.region_routes.len());
            for region_route in table_route.region_routes.into_iter() {
                let region = region_route
                    .region
                    .context(error::RouteInfoCorruptedSnafu {
                        err_msg: "'region' not found",
                    })?
                    .into();

                let leader_peer = get_peer(region_route.leader_peer_index);
                let follower_peers = region_route
                    .follower_peer_indexes
                    .into_iter()
                    .filter_map(get_peer)
                    .collect::<Vec<_>>();

                region_routes.push(RegionRoute {
                    region,
                    leader_peer,
                    follower_peers,
                });
            }

            table_routes.push(TableRoute {
                table,
                region_routes,
            });
        }

        Ok(Self { table_routes })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TableRoute {
    pub table: Table,
    pub region_routes: Vec<RegionRoute>,
}

impl TableRoute {
    pub fn find_leaders(&self) -> Vec<Peer> {
        self.region_routes
            .iter()
            .flat_map(|x| &x.leader_peer)
            .cloned()
            .collect::<Vec<Peer>>()
    }

    pub fn find_leader_regions(&self, datanode: &Peer) -> Vec<u32> {
        self.region_routes
            .iter()
            .filter_map(|x| {
                if let Some(peer) = &x.leader_peer {
                    if peer == datanode {
                        return Some(x.region.id as u32);
                    }
                }
                None
            })
            .collect::<Vec<u32>>()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Table {
    pub id: u64,
    pub table_name: TableName,
    #[serde(serialize_with = "as_utf8")]
    pub table_schema: Vec<u8>,
}

impl TryFrom<PbTable> for Table {
    type Error = error::Error;

    fn try_from(t: PbTable) -> Result<Self> {
        let table_name = t
            .table_name
            .context(error::RouteInfoCorruptedSnafu {
                err_msg: "table name requied",
            })?
            .into();
        Ok(Self {
            id: t.id,
            table_name,
            table_schema: t.table_schema,
        })
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct RegionRoute {
    pub region: Region,
    pub leader_peer: Option<Peer>,
    pub follower_peers: Vec<Peer>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Region {
    pub id: u64,
    pub name: String,
    pub partition: Option<Partition>,
    pub attrs: HashMap<String, String>,
}

impl From<PbRegion> for Region {
    fn from(r: PbRegion) -> Self {
        Self {
            id: r.id,
            name: r.name,
            partition: r.partition.map(Into::into),
            attrs: r.attrs,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Partition {
    #[serde(serialize_with = "as_utf8_vec")]
    pub column_list: Vec<Vec<u8>>,
    #[serde(serialize_with = "as_utf8_vec")]
    pub value_list: Vec<Vec<u8>>,
}

fn as_utf8<S: Serializer>(val: &[u8], serializer: S) -> std::result::Result<S::Ok, S::Error> {
    serializer.serialize_str(
        String::from_utf8(val.to_vec())
            .unwrap_or_else(|_| "<unknown-not-UTF8>".to_string())
            .as_str(),
    )
}

fn as_utf8_vec<S: Serializer>(
    val: &[Vec<u8>],
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    serializer.serialize_str(
        val.iter()
            .map(|v| {
                String::from_utf8(v.clone()).unwrap_or_else(|_| "<unknown-not-UTF8>".to_string())
            })
            .collect::<Vec<String>>()
            .join(",")
            .as_str(),
    )
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
    use api::v1::meta::Partition as PbPartition;
    use api::v1::meta::Peer as PbPeer;
    use api::v1::meta::Region as PbRegion;
    use api::v1::meta::RegionRoute as PbRegionRoute;
    use api::v1::meta::RouteRequest as PbRouteRequest;
    use api::v1::meta::RouteResponse as PbRouteResponse;
    use api::v1::meta::Table as PbTable;
    use api::v1::meta::TableName as PbTableName;
    use api::v1::meta::TableRoute as PbTableRoute;

    use super::*;

    #[test]
    fn test_route_request_trans() {
        let req = RouteRequest {
            table_names: vec![
                TableName::new("c1", "s1", "t1"),
                TableName::new("c2", "s2", "t2"),
            ],
        };

        let into_req: PbRouteRequest = req.into();

        assert!(into_req.header.is_none());
        assert_eq!("c1", into_req.table_names.get(0).unwrap().catalog_name);
        assert_eq!("s1", into_req.table_names.get(0).unwrap().schema_name);
        assert_eq!("t1", into_req.table_names.get(0).unwrap().table_name);
        assert_eq!("c2", into_req.table_names.get(1).unwrap().catalog_name);
        assert_eq!("s2", into_req.table_names.get(1).unwrap().schema_name);
        assert_eq!("t2", into_req.table_names.get(1).unwrap().table_name);
    }

    #[test]
    fn test_create_request_trans() {
        let req = CreateRequest {
            table_name: TableName::new("c1", "s1", "t1"),
            partitions: vec![
                Partition {
                    column_list: vec![b"c1".to_vec(), b"c2".to_vec()],
                    value_list: vec![b"v1".to_vec(), b"v2".to_vec()],
                },
                Partition {
                    column_list: vec![b"c1".to_vec(), b"c2".to_vec()],
                    value_list: vec![b"v11".to_vec(), b"v22".to_vec()],
                },
            ],
        };

        let into_req: PbCreateRequest = req.into();

        assert!(into_req.header.is_none());
        let table_name = into_req.table_name;
        assert_eq!("c1", table_name.as_ref().unwrap().catalog_name);
        assert_eq!("s1", table_name.as_ref().unwrap().schema_name);
        assert_eq!("t1", table_name.as_ref().unwrap().table_name);
        assert_eq!(
            vec![b"c1".to_vec(), b"c2".to_vec()],
            into_req.partitions.get(0).unwrap().column_list
        );
        assert_eq!(
            vec![b"v1".to_vec(), b"v2".to_vec()],
            into_req.partitions.get(0).unwrap().value_list
        );
        assert_eq!(
            vec![b"c1".to_vec(), b"c2".to_vec()],
            into_req.partitions.get(1).unwrap().column_list
        );
        assert_eq!(
            vec![b"v11".to_vec(), b"v22".to_vec()],
            into_req.partitions.get(1).unwrap().value_list
        );
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
}
