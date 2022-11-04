use std::collections::HashMap;

#[derive(Default)]
pub struct Context {
    pub exec_info: ExecInfo,
    pub client_info: ClientInfo,
    pub user_info: UserInfo,
}

impl Context {
    pub fn new() -> Self {
        Context::default()
    }
}

#[derive(Default)]
pub struct ExecInfo {
    pub catalog: Option<String>,
    pub schema: Option<String>,
    pub extra_opts: Option<HashMap<String, String>>,
    pub trace_id: Option<String>,
}

#[derive(Default)]
pub struct ClientInfo {
    pub client_addr: Option<String>,
    pub client_ip: Option<String>,
}

#[derive(Default)]
pub struct UserInfo {
    pub username: Option<String>,
    pub from_channel: Option<Channel>,
    pub auth_method: Option<AuthMethod>,
}

pub enum Channel {
    GRPC,
    HTTP,
    MYSQL,
}

pub enum AuthMethod {
    None,
    Password {
        hash_method: AuthHashMethod,
        hashed_value: Vec<u8>,
    },
    Token(String),
}

pub enum AuthHashMethod {
    DoubleSha1,
    Sha256,
}
