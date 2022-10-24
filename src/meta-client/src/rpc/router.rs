use std::collections::HashMap;

use api::v1::meta::CreateRequest as PbCreateRequest;
use api::v1::meta::Partition as PbPartition;
use api::v1::meta::Region as PbRegion;
use api::v1::meta::RouteRequest as PbRouteRequest;
use api::v1::meta::RouteResponse as PbRouteResponse;
use api::v1::meta::Table as PbTable;

use super::Peer;
use super::TableName;

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
    pub const fn new() -> Self {
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

#[derive(Debug, Clone, Default)]
pub struct CreateRequest {
    pub table_name: Option<TableName>,
    pub partitions: Vec<Partition>,
}

impl From<CreateRequest> for PbCreateRequest {
    fn from(mut req: CreateRequest) -> Self {
        Self {
            header: None,
            table_name: req.table_name.map(Into::into),
            partitions: req.partitions.drain(..).map(Into::into).collect(),
        }
    }
}

impl CreateRequest {
    #[inline]
    pub const fn new(table_name: TableName) -> Self {
        Self {
            table_name: Some(table_name),
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

impl From<PbRouteResponse> for RouteResponse {
    fn from(res: PbRouteResponse) -> Self {
        let peers: Vec<Peer> = res.peers.into_iter().map(From::from).collect();
        let table_routes = res
            .table_routes
            .into_iter()
            .map(|table_route| {
                let table = table_route.table.map(From::from);
                let region_routes = table_route
                    .region_routes
                    .into_iter()
                    .map(|region_route| {
                        let region = region_route.region.map(From::from);
                        let leader_peer = peers
                            .get(region_route.leader_peer_index as usize)
                            .map(ToOwned::to_owned);
                        let follower_peers = region_route
                            .follower_peer_indexes
                            .into_iter()
                            .filter_map(|i| peers.get(i as usize).map(ToOwned::to_owned))
                            .collect::<Vec<_>>();

                        RegionRoute {
                            region,
                            leader_peer,
                            follower_peers,
                        }
                    })
                    .collect::<Vec<_>>();

                TableRoute {
                    table,
                    region_routes,
                }
            })
            .collect::<Vec<_>>();

        Self { table_routes }
    }
}

#[derive(Debug, Clone, Default)]
pub struct TableRoute {
    pub table: Option<Table>,
    pub region_routes: Vec<RegionRoute>,
}

#[derive(Debug, Clone, Default)]
pub struct Table {
    pub table_name: Option<TableName>,
    pub table_schema: Vec<u8>,
}

impl From<PbTable> for Table {
    fn from(tb: PbTable) -> Self {
        Self {
            table_name: tb.table_name.map(Into::into),
            table_schema: tb.table_schema,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct RegionRoute {
    pub region: Option<Region>,
    pub leader_peer: Option<Peer>,
    pub follower_peers: Vec<Peer>,
}

#[derive(Debug, Clone, Default)]
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

#[derive(Debug, Clone)]
pub struct Partition {
    pub column_list: Vec<Vec<u8>>,
    pub value_list: Vec<Vec<u8>>,
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
            table_name: Some(TableName::new("c1", "s1", "t1")),
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

        let res: RouteResponse = res.into();
        let mut table_routes = res.table_routes;
        assert_eq!(1, table_routes.len());
        let table_route = table_routes.remove(0);
        let table = table_route.table.unwrap();
        assert_eq!("c1", table.table_name.as_ref().unwrap().catalog_name);
        assert_eq!("s1", table.table_name.as_ref().unwrap().schema_name);
        assert_eq!("t1", table.table_name.as_ref().unwrap().table_name);

        let mut region_routes = table_route.region_routes;
        assert_eq!(1, region_routes.len());
        let region_route = region_routes.remove(0);
        let region = region_route.region.unwrap();
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
