use std::io;
use std::sync::Arc;

use async_trait::async_trait;
use opensrv_mysql::AsyncMysqlShim;
use opensrv_mysql::ErrorKind;
use opensrv_mysql::ParamParser;
use opensrv_mysql::QueryResultWriter;
use opensrv_mysql::StatementMetaWriter;
use query::query_engine::Output;

use crate::mysql::error::{self, Result};
use crate::mysql::mysql_writer::MysqlResultWriter;

pub type MysqlInstanceRef = Arc<dyn MysqlInstance + Send + Sync>;

// TODO(LFC): Move to instance layer.
#[async_trait]
pub trait MysqlInstance {
    async fn do_query(&self, query: &str) -> Result<Output>;
}

// An intermediate shim for executing MySQL queries.
pub struct MysqlInstanceShim {
    mysql_instance: MysqlInstanceRef,
}

impl MysqlInstanceShim {
    pub fn create(mysql_instance: MysqlInstanceRef) -> MysqlInstanceShim {
        MysqlInstanceShim { mysql_instance }
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
        let output = self.mysql_instance.do_query(query).await;

        let mut writer = MysqlResultWriter::new(writer);
        writer.write(output).await
    }
}
