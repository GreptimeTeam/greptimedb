use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::context::AuthMethod::Token;
use crate::context::Channel::HTTP;

type CtxFnRef = Arc<dyn Fn(&Context) -> bool + Send + Sync>;

#[derive(Default, Serialize, Deserialize)]
pub struct Context {
    pub exec_info: ExecInfo,
    pub client_info: ClientInfo,
    pub user_info: UserInfo,
    pub quota: Quota,
    #[serde(skip)]
    pub predicates: Vec<CtxFnRef>,
}

impl Context {
    pub fn new() -> Self {
        Context::default()
    }

    pub fn add_predicate(&mut self, predicate: CtxFnRef) {
        self.predicates.push(predicate);
    }
}

#[derive(Default, Serialize, Deserialize)]
pub struct ExecInfo {
    pub catalog: Option<String>,
    pub schema: Option<String>,
    // should opts to be thread safe?
    pub extra_opts: HashMap<String, String>,
    pub trace_id: Option<String>,
}

#[derive(Default, Serialize, Deserialize)]
pub struct ClientInfo {
    pub client_host: Option<String>,
}

impl ClientInfo {
    pub fn new(host: Option<String>) -> Self {
        ClientInfo { client_host: host }
    }
}

#[derive(Default, Serialize, Deserialize)]
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

#[derive(Serialize, Deserialize)]
pub enum Channel {
    GRPC,
    HTTP,
    MYSQL,
}

#[derive(Serialize, Deserialize)]
pub enum AuthMethod {
    None,
    Password {
        hash_method: AuthHashMethod,
        hashed_value: Vec<u8>,
    },
    Token(String),
}

#[derive(Serialize, Deserialize)]
pub enum AuthHashMethod {
    DoubleSha1,
    Sha256,
}

#[derive(Default, Serialize, Deserialize)]
pub struct Quota {
    pub total: u64,
    pub consumed: u64,
    pub estimated: u64,
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::context::{ClientInfo, Context, ExecInfo, Quota, UserInfo};

    #[test]
    fn test_predicate() {
        let mut ctx = Context::default();
        ctx.add_predicate(Arc::new(|ctx: &Context| {
            ctx.quota.total > ctx.quota.consumed
        }));
        ctx.quota.total = 10;
        ctx.quota.consumed = 5;

        let predicates = ctx.predicates.clone();
        let mut re = true;
        for predicate in predicates {
            re &= predicate(&ctx);
        }
        assert!(re);
    }

    #[test]
    fn test_build() {
        let ctx = Context {
            exec_info: ExecInfo {
                catalog: Some(String::from("greptime")),
                schema: Some(String::from("public")),
                extra_opts: HashMap::new(),
                trace_id: None,
            },
            client_info: ClientInfo::new(Some(String::from("127.0.0.1:4001"))),
            user_info: UserInfo::with_http_token(String::from("HELLO")),
            quota: Quota {
                total: 10,
                consumed: 5,
                estimated: 2,
            },
            predicates: vec![],
        };
        println!("{}", serde_json::to_string(&ctx).unwrap());
    }
}
