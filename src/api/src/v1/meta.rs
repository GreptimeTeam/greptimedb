tonic::include_proto!("greptime.v1.meta");

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
