#[derive(Clone, Debug, Default)]
pub struct GreptimeOptions {
    pub http_addr: String,
    pub rpc_addr: String,
    pub wal_dir: String,
}
