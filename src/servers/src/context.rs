use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use snafu::OptionExt;

use crate::error::{BuildingContextSnafu, Result};

type CtxFnRef = Arc<dyn Fn(&Context) -> bool + Send + Sync>;

#[derive(Serialize, Deserialize, Clone)]
pub struct Context {
    pub exec_info: ExecInfo,
    pub client_info: ClientInfo,
    pub user_info: UserInfo,
    pub quota: Quota,
    #[serde(skip)]
    pub predicates: Vec<CtxFnRef>,
}

impl Context {
    pub fn add_predicate(&mut self, predicate: CtxFnRef) {
        self.predicates.push(predicate);
    }
}

#[derive(Default)]
pub struct CtxBuilder {
    client_addr: Option<String>,

    username: Option<String>,
    from_channel: Option<Channel>,
    auth_method: Option<AuthMethod>,
}

impl CtxBuilder {
    pub fn new() -> CtxBuilder {
        CtxBuilder::default()
    }

    pub fn client_addr(mut self, addr: Option<String>) -> CtxBuilder {
        self.client_addr = addr;
        self
    }

    pub fn set_channel(mut self, channel: Option<Channel>) -> CtxBuilder {
        self.from_channel = channel;
        self
    }

    pub fn set_auth_method(mut self, auth_method: Option<AuthMethod>) -> CtxBuilder {
        self.auth_method = auth_method;
        self
    }

    pub fn set_username(mut self, username: Option<String>) -> CtxBuilder {
        self.username = username;
        self
    }

    pub fn build(self) -> Result<Context> {
        Ok(Context {
            client_info: ClientInfo {
                client_host: self.client_addr.context(BuildingContextSnafu {
                    err_msg: "unknown client addr while building ctx",
                })?,
            },
            user_info: UserInfo {
                username: self.username,
                from_channel: self.from_channel.context(BuildingContextSnafu {
                    err_msg: "unknown channel while building ctx",
                })?,
                auth_method: self.auth_method.context(BuildingContextSnafu {
                    err_msg: "unknown auth method while building ctx",
                })?,
            },

            exec_info: ExecInfo::default(),
            quota: Quota::default(),
            predicates: vec![],
        })
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ExecInfo {
    pub catalog: Option<String>,
    pub schema: Option<String>,
    // should opts to be thread safe?
    pub extra_opts: HashMap<String, String>,
    pub trace_id: Option<String>,
}

impl Default for ExecInfo {
    fn default() -> Self {
        ExecInfo {
            catalog: Some("greptime".to_string()),
            schema: Some("public".to_string()),
            extra_opts: HashMap::new(),
            trace_id: None,
        }
    }
}

#[derive(Default, Serialize, Deserialize, Clone)]
pub struct ClientInfo {
    pub client_host: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct UserInfo {
    pub username: Option<String>,
    pub from_channel: Channel,
    pub auth_method: AuthMethod,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub enum Channel {
    GRPC,
    HTTP,
    MYSQL,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub enum AuthMethod {
    None,
    Password {
        hash_method: AuthHashMethod,
        hashed_value: Vec<u8>,
        salt: Vec<u8>,
    },
    Token(String),
}

impl Default for AuthMethod {
    fn default() -> Self {
        AuthMethod::None
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub enum AuthHashMethod {
    DoubleSha1,
    Sha256,
}

#[derive(Default, Serialize, Deserialize, Clone)]
pub struct Quota {
    pub total: u64,
    pub consumed: u64,
    pub estimated: u64,
}

#[cfg(test)]
mod test {

    use std::sync::Arc;

    use crate::context::AuthMethod::Token;
    use crate::context::Channel::HTTP;
    use crate::context::{Channel, Context, CtxBuilder, UserInfo};

    #[test]
    fn test_predicate() {
        let mut ctx = Context {
            exec_info: Default::default(),
            client_info: Default::default(),
            user_info: UserInfo {
                username: None,
                from_channel: Channel::GRPC,
                auth_method: Default::default(),
            },
            quota: Default::default(),
            predicates: vec![],
        };
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
        let ctx = CtxBuilder::new()
            .client_addr(Some("127.0.0.1:4001".to_string()))
            .set_channel(Some(HTTP))
            .set_auth_method(Some(Token("HELLO".to_string())))
            .build()
            .unwrap();

        assert_eq!(ctx.exec_info.catalog.unwrap(), String::from("greptime"));
        assert_eq!(ctx.exec_info.schema.unwrap(), String::from("public"));
        assert_eq!(ctx.exec_info.extra_opts.len(), 0);
        assert_eq!(ctx.exec_info.trace_id, None);

        assert_eq!(ctx.client_info.client_host, String::from("127.0.0.1:4001"));

        assert_eq!(ctx.user_info.username, None);
        assert_eq!(ctx.user_info.from_channel, HTTP);
        assert_eq!(ctx.user_info.auth_method, Token(String::from("HELLO")));

        assert_eq!(ctx.quota.total, 0);
        assert_eq!(ctx.quota.consumed, 0);
        assert_eq!(ctx.quota.estimated, 0);

        assert_eq!(ctx.predicates.capacity(), 0);
    }
}
