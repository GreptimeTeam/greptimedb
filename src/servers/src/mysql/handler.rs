use std::io;
use std::sync::Arc;

use async_trait::async_trait;
use common_telemetry::error;
use opensrv_mysql::{
    AsyncMysqlShim, ErrorKind, ParamParser, QueryResultWriter, StatementMetaWriter,
};
use rand::RngCore;
use tokio::sync::RwLock;

use crate::context::AuthHashMethod::DoubleSha1;
use crate::context::Channel::MYSQL;
use crate::context::{AuthMethod, Context, CtxBuilder};
use crate::error::{self, Result};
use crate::mysql::writer::MysqlResultWriter;
use crate::query_handler::SqlQueryHandlerRef;

// An intermediate shim for executing MySQL queries.
pub struct MysqlInstanceShim {
    query_handler: SqlQueryHandlerRef,
    salt: [u8; 20],
    client_addr: String,
    ctx: Arc<RwLock<Option<Context>>>,
}

impl MysqlInstanceShim {
    pub fn create(query_handler: SqlQueryHandlerRef, client_addr: String) -> MysqlInstanceShim {
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
        }
    }
}

#[async_trait]
impl<W: io::Write + Send + Sync> AsyncMysqlShim<W> for MysqlInstanceShim {
    type Error = error::Error;

    fn salt(&self) -> [u8; 20] {
        self.salt
    }

    async fn authenticate(
        &self,
        _auth_plugin: &str,
        username: &[u8],
        salt: &[u8],
        auth_data: &[u8],
    ) -> bool {
        // if not specified then **root** will be used
        let username = String::from_utf8_lossy(username);
        let client_addr = self.client_addr.clone();
        let auth_method = match auth_data.len() {
            0 => AuthMethod::None,
            _ => AuthMethod::Password {
                hash_method: DoubleSha1,
                hashed_value: auth_data.to_vec(),
                salt: salt.to_vec(),
            },
        };

        return match CtxBuilder::new()
            .client_addr(Some(client_addr))
            .set_channel(Some(MYSQL))
            .set_username(Some(username.to_string()))
            .set_auth_method(Some(auth_method))
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

    async fn on_prepare<'a>(
        &'a mut self,
        _: &'a str,
        writer: StatementMetaWriter<'a, W>,
    ) -> Result<()> {
        writer.error(
            ErrorKind::ER_UNKNOWN_ERROR,
            "prepare statement is not supported yet".as_bytes(),
        )?;
        Ok(())
    }

    async fn on_execute<'a>(
        &'a mut self,
        _: u32,
        _: ParamParser<'a>,
        writer: QueryResultWriter<'a, W>,
    ) -> Result<()> {
        writer.error(
            ErrorKind::ER_UNKNOWN_ERROR,
            "prepare statement is not supported yet".as_bytes(),
        )?;
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
        // TODO(LFC): Find a better way:
        // `check` uses regex to filter out unsupported statements emitted by MySQL's federated
        // components, this is quick and dirty, there must be a better way to do it.
        let output = if let Some(output) = crate::mysql::federated::check(query) {
            Ok(output)
        } else {
            self.query_handler.do_query(query).await
        };

        let mut writer = MysqlResultWriter::new(writer);
        writer.write(output).await
    }
}
