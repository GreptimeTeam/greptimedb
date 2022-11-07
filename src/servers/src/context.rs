use std::collections::HashMap;

use crate::context::AuthMethod::Token;
use crate::context::Channel::HTTP;

#[derive(Default, Debug)]
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

#[derive(Default, Debug)]
pub struct ExecInfo {
    pub catalog: Option<String>,
    pub schema: Option<String>,
    pub extra_opts: Option<HashMap<String, String>>,
    pub trace_id: Option<String>,
}

#[derive(Default, Debug)]
pub struct ClientInfo {
    pub client_host: Option<String>,
}

impl ClientInfo {
    pub fn new(host: Option<String>) -> Self {
        ClientInfo { client_host: host }
    }
}

#[derive(Default, Debug)]
pub struct UserInfo {
    pub username: Option<String>,
    pub from_channel: Option<Channel>,
    pub auth_method: Option<AuthMethod>,
}

impl UserInfo {
    pub fn with_http_token(token: String) -> Self {
        UserInfo {
            username: None,
            from_channel: Some(HTTP),
            auth_method: Some(Token(token)),
        }
    }
}

#[derive(Debug)]
pub enum Channel {
    GRPC,
    HTTP,
    MYSQL,
}

#[derive(Debug)]
pub enum AuthMethod {
    None,
    Password {
        hash_method: AuthHashMethod,
        hashed_value: Vec<u8>,
    },
    Token(String),
}

#[derive(Debug)]
pub enum AuthHashMethod {
    DoubleSha1,
    Sha256,
}
