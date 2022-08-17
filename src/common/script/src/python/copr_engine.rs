//! This is the python coprocessor's engine, which should:
//! 1. store a parsed `Coprocessor` struct
//! 2. store other metadata so other module can call it when needed

use std::sync::Arc;
use std::task::{Context, Poll};
use std::{pin::Pin, result::Result as StdResult};

use common_recordbatch::{
    error::Error as RbError, RecordBatch, SendableRecordBatchStream,
};
use datafusion_common::record_batch::RecordBatch as DfRecordBatch;
use futures::Stream;
use futures::stream::StreamExt;
use query::QueryEngineRef;

use super::{copr_parse::parse_copr, coprocessor::exec_parsed};
use crate::python::{coprocessor::Coprocessor, error::Result};

pub struct CoprEngine {
    copr: Coprocessor,
    query_engine: QueryEngineRef,
}

// pub type CoprocessorStream = Pin<Box<dyn Stream<Item = Result<DfRecordBatch>>>>;

pub struct CoprocessorStream {
    copr: Arc<Coprocessor>,
    stream: SendableRecordBatchStream
}

impl Stream for CoprocessorStream {
    type Item = Result<DfRecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        let res = self.stream.as_mut().poll_next(cx);
        match res {
            Poll::Ready(Some(rb)) => {
                match rb{
                    Ok(rb) => {
                        let df_rb = rb.df_recordbatch;
                        let ret = exec_parsed(&self.copr, &df_rb);
                        Poll::Ready(Some(ret))
                        },
                    Err(_err) => todo!(),
                }
            }
            Poll::Ready(None) => Poll::Ready(None),
            _ => Poll::Pending,
        }
    }
}

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
    /// TODO: make a function that return a Stream of Result\<RecordBatch\>
    pub async fn evaluate(&self) -> Result<Vec<Result<DfRecordBatch>>> {
        if let Some(sql) = &self.copr.deco_args.sql {
            let plan = self.query_engine.sql_to_plan(sql)?;
            let res = self.query_engine.execute(&plan).await?;
            let copr = Arc::new((self.copr).clone());
            match res {
                query::Output::AffectedRows(_) => todo!(),
                query::Output::RecordBatch(stream) => {
                    let run_copr = |rb: StdResult<RecordBatch, RbError>| {
                        match rb.map(|rb| {
                            let df_rb = rb.df_recordbatch;
                            exec_parsed(&copr.clone(), &df_rb)
                        }) {
                            Ok(Ok(df_rb)) => Ok(df_rb),
                            Ok(Err(py_err)) => Err(py_err),
                            Err(rb_err) => todo!(),
                        }
                    };
                    let stream = stream.map(run_copr);
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
