tonic::include_proto!("greptime.v1.meta");

pub const PROTOCOL_VERSION: u64 = 1;

impl RequestHeader {
    #[inline]
    pub fn new((cluster_id, member_id): (u64, u64)) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            cluster_id,
            member_id,
        }
    }
}

impl ResponseHeader {
    #[inline]
    pub fn success(cluster_id: u64) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            cluster_id,
            ..Default::default()
        }
    }

    #[inline]
    pub fn failed(cluster_id: u64, error: Error) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            cluster_id,
            error: Some(error),
        }
    }
}

macro_rules! gen_set_header {
    ($req: ty) => {
        impl $req {
            #[inline]
            pub fn set_header(&mut self, (cluster_id, member_id): (u64, u64)) {
                self.header = Some(RequestHeader::new((cluster_id, member_id)));
            }
        }
    };
}

gen_set_header!(HeartbeatRequest);
gen_set_header!(RouteRequest);
gen_set_header!(CreateRequest);
gen_set_header!(RangeRequest);
gen_set_header!(PutRequest);
gen_set_header!(BatchPutRequest);
gen_set_header!(CompareAndPutRequest);
gen_set_header!(DeleteRangeRequest);
