use std::io;

use async_trait::async_trait;
use opensrv_mysql::AsyncMysqlShim;
use opensrv_mysql::ErrorKind;
use opensrv_mysql::ParamParser;
use opensrv_mysql::QueryResultWriter;
use opensrv_mysql::StatementMetaWriter;

use crate::context::Context;
use crate::error::{self, Result};
use crate::mysql::writer::MysqlResultWriter;
use crate::query_handler::SqlQueryHandlerRef;

// An intermediate shim for executing MySQL queries.
pub struct MysqlInstanceShim {
    query_handler: SqlQueryHandlerRef,
}

impl MysqlInstanceShim {
    pub fn create(query_handler: SqlQueryHandlerRef) -> MysqlInstanceShim {
        MysqlInstanceShim { query_handler }
    }
}

#[async_trait]
impl<W: io::Write + Send + Sync> AsyncMysqlShim<W> for MysqlInstanceShim {
    type Error = error::Error;

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
            self.query_handler.do_query(query, &Context::new()).await
        };

        let mut writer = MysqlResultWriter::new(writer);
        writer.write(output).await
    }
}
