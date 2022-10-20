tonic::include_proto!("greptime.v1.meta");

pub const PROTOCOL_VERSION: u64 = 1;

impl Peer {
    pub fn new(id: u64, addr: impl AsRef<str>) -> Self {
        Self {
            id,
            endpoint: Some(addr.as_ref().into()),
        }
    }
}

impl From<&str> for Endpoint {
    fn from(s: &str) -> Self {
        Self {
            addr: s.to_string(),
        }
    }
}

impl RequestHeader {
    pub fn new(cluster_id: u64, member_id: u64) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            cluster_id,
            member_id,
        }
    }

    pub fn with_id(id: (u64, u64)) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            cluster_id: id.0,
            member_id: id.1,
        }
    }
}

impl HeartbeatRequest {
    pub fn new(header: RequestHeader) -> Self {
        Self {
            header: Some(header),
            ..Default::default()
        }
    }
}

impl AskLeaderRequest {
    pub fn new(header: RequestHeader) -> Self {
        Self {
            header: Some(header),
        }
    }
}

impl TableName {
    pub fn new(
        catalog: impl Into<String>,
        schema: impl Into<String>,
        table: impl Into<String>,
    ) -> Self {
        Self {
            catalog_name: catalog.into(),
            schema_name: schema.into(),
            table_name: table.into(),
        }
    }
}

impl RouteRequest {
    pub fn new(header: RequestHeader) -> Self {
        Self {
            header: Some(header),
            ..Default::default()
        }
    }

    pub fn add_table(mut self, table_name: TableName) -> Self {
        self.table_names.push(table_name);
        self
    }
}

impl CreateRequest {
    pub fn new(header: RequestHeader, table_name: TableName) -> Self {
        Self {
            header: Some(header),
            table_name: Some(table_name),
            ..Default::default()
        }
    }

    pub fn add_partition(mut self, partition: Partition) -> Self {
        self.partitions.push(partition);
        self
    }
}

impl Region {
    pub fn new(id: u64, name: impl Into<String>, partition: Partition) -> Self {
        Self {
            id,
            name: name.into(),
            partition: Some(partition),
            ..Default::default()
        }
    }

    pub fn attr(mut self, key: impl Into<String>, val: impl Into<String>) -> Self {
        self.attrs.insert(key.into(), val.into());
        self
    }
}

impl Partition {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn column_list(mut self, column_list: Vec<Vec<u8>>) -> Self {
        self.column_list = column_list;
        self
    }

    pub fn value_list(mut self, value_list: Vec<Vec<u8>>) -> Self {
        self.value_list = value_list;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peer() {
        let peer = Peer::new(1, "test_addr");
        assert_eq!(1, peer.id);
        assert_eq!(
            Endpoint {
                addr: "test_addr".to_string()
            },
            peer.endpoint.unwrap()
        );
    }
}
