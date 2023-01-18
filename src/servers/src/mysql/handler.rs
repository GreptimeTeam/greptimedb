// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use common_query::Output;
use common_telemetry::{error, trace};
use opensrv_mysql::{
    AsyncMysqlShim, ErrorKind, InitWriter, ParamParser, QueryResultWriter, StatementMetaWriter,
};
use rand::RngCore;
use session::context::Channel;
use session::Session;
use snafu::ensure;
use tokio::io::AsyncWrite;

use crate::auth::{Identity, Password, UserProviderRef};
use crate::error::{self, Result};
use crate::mysql::writer::MysqlResultWriter;
use crate::query_handler::sql::ServerSqlQueryHandlerRef;

// An intermediate shim for executing MySQL queries.
pub struct MysqlInstanceShim {
    query_handler: ServerSqlQueryHandlerRef,
    salt: [u8; 20],
    session: Arc<Session>,
    user_provider: Option<UserProviderRef>,
}

impl MysqlInstanceShim {
    pub fn create(
        query_handler: ServerSqlQueryHandlerRef,
        user_provider: Option<UserProviderRef>,
        client_addr: SocketAddr,
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
            session: Arc::new(Session::new(client_addr, Channel::Mysql)),
            user_provider,
        }
    }

    async fn do_query(&self, query: &str) -> Vec<Result<Output>> {
        trace!("Start executing query: '{}'", query);
        let start = Instant::now();

        // TODO(LFC): Find a better way to deal with these special federated queries:
        // `check` uses regex to filter out unsupported statements emitted by MySQL's federated
        // components, this is quick and dirty, there must be a better way to do it.
        let output =
            if let Some(output) = crate::mysql::federated::check(query, self.session.context()) {
                vec![Ok(output)]
            } else {
                self.query_handler
                    .do_query(query, self.session.context())
                    .await
            };

        trace!(
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
        // if not specified then **greptime** will be used
        let username = String::from_utf8_lossy(username);

        let mut user_info = None;
        let addr = self.session.conn_info().client_host.to_string();
        if let Some(user_provider) = &self.user_provider {
            let user_id = Identity::UserId(&username, Some(addr.as_str()));

            let password = match auth_plugin {
                "mysql_native_password" => Password::MysqlNativePassword(auth_data, salt),
                other => {
                    error!("Unsupported mysql auth plugin: {}", other);
                    return false;
                }
            };
            match user_provider.authenticate(user_id, password).await {
                Ok(userinfo) => {
                    user_info = Some(userinfo);
                }
                Err(e) => {
                    error!("Failed to auth, err: {:?}", e);
                    return false;
                }
            };
        }
        let user_info = user_info.unwrap_or_default();

        self.session.set_user_info(user_info);

        true
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
        let outputs = self.do_query(query).await;
        let mut writer = MysqlResultWriter::new(writer);
        for output in outputs {
            writer.write(query, output).await?;
        }
        Ok(())
    }

    async fn on_init<'a>(&'a mut self, database: &'a str, w: InitWriter<'a, W>) -> Result<()> {
        let (catalog, schema) = crate::parse_catalog_and_schema_from_client_database_name(database);
        ensure!(
            self.query_handler.is_valid_schema(catalog, schema)?,
            error::DatabaseNotFoundSnafu { catalog, schema }
        );

        let user_info = &self.session.user_info();

        if let Some(schema_validator) = &self.user_provider {
            if let Err(e) = schema_validator.authorize(catalog, schema, user_info).await {
                return w
                    .error(
                        ErrorKind::ER_DBACCESS_DENIED_ERROR,
                        e.to_string().as_bytes(),
                    )
                    .await
                    .map_err(|e| e.into());
            }
        }

        let context = self.session.context();
        context.set_current_catalog(catalog);
        context.set_current_schema(database);

        w.ok().await.map_err(|e| e.into())
    }
}
