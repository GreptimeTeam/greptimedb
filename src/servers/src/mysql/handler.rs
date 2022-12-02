// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use common_query::Output;
use common_telemetry::{debug, error, info};
use opensrv_mysql::{
    AsyncMysqlShim, ErrorKind, InitWriter, ParamParser, QueryResultWriter, StatementMetaWriter,
};
use rand::RngCore;
use session::Session;
use tokio::io::AsyncWrite;
use tokio::sync::RwLock;

use crate::auth::mysql::{auth_mysql, MysqlAuthPlugin};
use crate::auth::{Identity, UserInfo, UserProviderRef};
use crate::context::Channel::MYSQL;
use crate::context::{Context, CtxBuilder};
use crate::error::{self, Result};
use crate::mysql::writer::MysqlResultWriter;
use crate::query_handler::SqlQueryHandlerRef;

// An intermediate shim for executing MySQL queries.
pub struct MysqlInstanceShim {
    query_handler: SqlQueryHandlerRef,
    salt: [u8; 20],
    client_addr: String,
    // TODO(LFC): Break `Context` struct into different fields in `Session`, each with its own purpose.
    ctx: Arc<RwLock<Option<Context>>>,
    session: Arc<Session>,
    user_provider: Option<UserProviderRef>,
}

impl MysqlInstanceShim {
    pub fn create(
        query_handler: SqlQueryHandlerRef,
        client_addr: String,
        user_provider: Option<UserProviderRef>,
    ) -> MysqlInstanceShim {
        // init a random salt
        let mut bs = vec![0u8; 20];
        let mut rng = rand::thread_rng();
        rng.fill_bytes(bs.as_mut());

        let mut scramble: [u8; 20] = [0; 20];
        for i in 0..20 {
            scramble[i] = bs[i] & 0x7fu8;
            if scramble[i] == b'\0' || scramble[i] == b'$' {
                scramble[i] += 1;
            }
        }

        MysqlInstanceShim {
            query_handler,
            salt: scramble,
            client_addr,
            ctx: Arc::new(RwLock::new(None)),
            session: Arc::new(Session::new()),
            user_provider,
        }
    }

    async fn do_query(&self, query: &str) -> Result<Output> {
        debug!("Start executing query: '{}'", query);
        let start = Instant::now();

        // TODO(LFC): Find a better way to deal with these special federated queries:
        // `check` uses regex to filter out unsupported statements emitted by MySQL's federated
        // components, this is quick and dirty, there must be a better way to do it.
        let output =
            if let Some(output) = crate::mysql::federated::check(query, self.session.context()) {
                Ok(output)
            } else {
                self.query_handler
                    .do_query(query, self.session.context())
                    .await
            };

        debug!(
            "Finished executing query: '{}', total time costs in microseconds: {}",
            query,
            start.elapsed().as_micros()
        );
        output
    }
}

#[async_trait]
impl<W: AsyncWrite + Send + Sync + Unpin> AsyncMysqlShim<W> for MysqlInstanceShim {
    type Error = error::Error;

    fn salt(&self) -> [u8; 20] {
        self.salt
    }

    async fn authenticate(
        &self,
        auth_plugin: &str,
        username: &[u8],
        salt: &[u8],
        auth_data: &[u8],
    ) -> bool {
        // if not specified then **root** will be used
        let username = String::from_utf8_lossy(username);
        let client_addr = self.client_addr.clone();

        let auth_plugin = match auth_plugin {
            "mysql_native_password" => MysqlAuthPlugin::MysqlNativePwd,
            _ => return false,
        };

        // TODO(fys): default user_info maybe read from configuration
        let mut user_info = UserInfo::default();

        if let Some(user_provider) = &self.user_provider {
            let user_id = Identity::UserId(username.to_string(), Some(client_addr.clone()));

            match user_provider.get_user_info(&user_id).await {
                Ok(Some(userinfo)) => {
                    let auth_method = if let Some(auth_method) =
                        userinfo.mysql_auth_method(&auth_plugin)
                    {
                        auth_method
                    } else {
                        error!(
                                "Failed to auth, channel: mysql, user_id: {:?}, err: no suitable auth method",
                                user_id
                            );
                        return false;
                    };

                    if auth_mysql(auth_plugin, auth_data, salt, auth_method) {
                        info!("Auth successful, channel: mysql, user_id: {:?}", user_id);
                    } else {
                        info!("Failed to auth, channel: mysql, user_id: {:?}", user_id);
                        return false;
                    }
                    user_info = userinfo;
                }
                Ok(None) => {
                    error!(
                        "Failed to auth, user_id: {:?}, err: can not find user info ",
                        user_id
                    );
                    return false;
                }
                Err(e) => {
                    error!("Failed to auth, user_id: {:?}, err: {:?}", user_id, e);
                    return false;
                }
            }
        }

        return match CtxBuilder::new()
            .client_addr(client_addr)
            .set_channel(MYSQL)
            .set_user_info(user_info)
            .build()
        {
            Ok(ctx) => {
                let mut a = self.ctx.write().await;
                *a = Some(ctx);
                true
            }
            Err(e) => {
                error!(e; "create ctx failed when authing mysql conn");
                false
            }
        };
    }

    async fn on_prepare<'a>(&'a mut self, _: &'a str, w: StatementMetaWriter<'a, W>) -> Result<()> {
        w.error(
            ErrorKind::ER_UNKNOWN_ERROR,
            b"prepare statement is not supported yet",
        )
        .await?;
        Ok(())
    }

    async fn on_execute<'a>(
        &'a mut self,
        _: u32,
        _: ParamParser<'a>,
        w: QueryResultWriter<'a, W>,
    ) -> Result<()> {
        w.error(
            ErrorKind::ER_UNKNOWN_ERROR,
            b"prepare statement is not supported yet",
        )
        .await?;
        Ok(())
    }

    async fn on_close<'a>(&'a mut self, _stmt_id: u32)
    where
        W: 'async_trait,
    {
        // do nothing because we haven't implemented prepare statement
    }

    async fn on_query<'a>(
        &'a mut self,
        query: &'a str,
        writer: QueryResultWriter<'a, W>,
    ) -> Result<()> {
        let output = self.do_query(query).await;
        let mut writer = MysqlResultWriter::new(writer);
        writer.write(query, output).await
    }

    async fn on_init<'a>(&'a mut self, database: &'a str, w: InitWriter<'a, W>) -> Result<()> {
        let query = format!("USE {}", database.trim());
        let output = self.do_query(&query).await;
        if let Err(e) = output {
            w.error(ErrorKind::ER_UNKNOWN_ERROR, e.to_string().as_bytes())
                .await
        } else {
            w.ok().await
        }
        .map_err(|e| e.into())
    }
}
