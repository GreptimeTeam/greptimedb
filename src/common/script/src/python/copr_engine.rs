//! This is the python coprocessor's engine, which should:
//! 1. store a parsed `Coprocessor` struct
//! 2. store other metadata so other module can call it when needed
//! TODO(discord9): resolve cyclic package dependency of `query` crate

use std::rc::Rc;
use std::sync::Arc;
use std::task::Context;
use std::{pin::Pin, result::Result as StdResult};

use common_recordbatch::{
    error::Error as RbError, RecordBatch, RecordBatchStream, SendableRecordBatchStream,
};
use datafusion_common::record_batch::RecordBatch as DfRecordBatch;
use futures::Stream;
use futures::stream::{self, StreamExt};
use query::QueryEngineRef;

use super::{copr_parse::parse_copr, coprocessor::exec_parsed};
use crate::python::{coprocessor::Coprocessor, error::Result};

pub struct CoprEngine {
    copr: Coprocessor,
    query_engine: QueryEngineRef,
}

pub type CoprocessorStream = Pin<Box<dyn Stream<Item = Result<DfRecordBatch>>>>;

impl CoprEngine {
    pub fn try_new(script: &str, query_engine: QueryEngineRef) -> Result<Self> {
        Ok(Self {
            copr: parse_copr(script)?,
            query_engine,
        })
    }
    pub fn update_script(&mut self, script: &str) -> Result<()> {
        let copr = parse_copr(script)?;
        self.copr = copr;
        Ok(())
    }

    /// this function use sql query to return a async iterator of (a stream) Resulting RecordBatch
    /// computed from input record batch from sql and execute python code
    ///
    /// TODO: return a Stream of Result\<RecordBatch\>
    pub async fn evaluate<'a>(&'a self) -> Result<Vec<Result<DfRecordBatch>>> {
        if let Some(sql) = &self.copr.deco_args.sql {
            let plan = self.query_engine.sql_to_plan(sql)?;
            let res = self.query_engine.execute(&plan).await?;
            let copr = Arc::new((&self.copr).clone());
            match res {
                query::Output::AffectedRows(_) => todo!(),
                query::Output::RecordBatch(stream) => {
                    let run_copr = |rb: StdResult<RecordBatch, RbError>| async{
                        match rb.map(|rb| {
                            let df_rb = rb.df_recordbatch;
                            exec_parsed(&copr.clone(), &df_rb)
                        }) {
                            Ok(Ok(df_rb)) => Ok(df_rb),
                            Ok(Err(py_err)) => Err(py_err),
                            Err(rb_err) => todo!(),
                        }
                    };
                    let stream = stream.then(run_copr);
                    // let ret = Box::pin(stream) as CoprocessorStream;
                    let res = stream.collect::<Vec<Result<DfRecordBatch>>>().await;
                    Ok(res)
                }
            }
        } else {
            // what do you mean when no sql query was given?Then where the record batch come from?
            todo!()
        }
    }
}
