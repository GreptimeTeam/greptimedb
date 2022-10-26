tonic::include_proto!("greptime.v1.meta");

pub const PROTOCOL_VERSION: u64 = 1;

impl RequestHeader {
    #[inline]
    pub fn new((cluster_id, member_id): (u64, u64)) -> Option<Self> {
        Some(Self {
            protocol_version: PROTOCOL_VERSION,
            cluster_id,
            member_id,
        })
    }
}

impl ResponseHeader {
    #[inline]
    pub fn success(cluster_id: u64) -> Option<Self> {
        Some(Self {
            protocol_version: PROTOCOL_VERSION,
            cluster_id,
            ..Default::default()
        })
    }

    #[inline]
    pub fn failed(cluster_id: u64, error: Error) -> Option<Self> {
        Some(Self {
            protocol_version: PROTOCOL_VERSION,
            cluster_id,
            error: Some(error),
        })
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
    pub fn add_table(mut self, table_name: TableName) -> Self {
        self.table_names.push(table_name);
        self
    }
}

impl CreateRequest {
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
