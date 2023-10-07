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

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use clap::{Parser, ValueEnum};
use client::{Client, Database};
use common_query::Output;
use common_recordbatch::util::collect;
use common_telemetry::{error, info};
use datatypes::scalars::ScalarVector;
use datatypes::vectors::StringVector;
use snafu::{OptionExt, ResultExt};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::sync::Semaphore;

use crate::cli::{Instance, Tool};
use crate::error::{
    CollectRecordBatchesSnafu, ConnectServerSnafu, EmptyResultSnafu, Error, FileIoSnafu,
    NotDataFromOutputSnafu, RequestDatabaseSnafu, Result,
};

#[derive(Debug, Default, Clone, ValueEnum)]
enum ExportTarget {
    /// Corresponding to `SHOW CREATE TABLE`
    #[default]
    CreateTable,
    /// Corresponding to `EXPORT TABLE`
    TableData,
}

#[derive(Debug, Default, Parser)]
pub struct ExportCommand {
    /// Server address to connect
    #[clap(long)]
    addr: String,

    /// Directory to put the exported data. E.g.: /tmp/greptimedb-export
    #[clap(long)]
    output_dir: String,

    /// The name of the catalog to export. Default to all catalogs.
    #[clap(long, default_value = "")]
    catalog: String,

    /// The name of the database to export. Default to all databases.
    #[clap(long, default_value = "")]
    schema: String,

    /// Parallelism of the export.
    #[clap(long, short = 'j', default_value = "1")]
    export_jobs: usize,

    /// Max retry times for each job.
    #[clap(long, default_value = "3")]
    max_retry: usize,

    /// Things to export
    #[clap(long, short = 't', value_enum)]
    target: ExportTarget,
}

impl ExportCommand {
    pub async fn build(&self) -> Result<Instance> {
        let client = Client::with_urls(&[self.addr.clone()]);
        client
            .health_check()
            .await
            .with_context(|_| ConnectServerSnafu {
                addr: self.addr.clone(),
            })?;
        let database_client = Database::new(self.catalog.clone(), self.schema.clone(), client);

        Ok(Instance::Tool(Box::new(Export {
            client: database_client,
            catalog: self.catalog.clone(),
            schema: self.schema.clone(),
            output_dir: self.output_dir.clone(),
            parallelism: self.export_jobs,
            target: self.target.clone(),
        })))
    }
}

pub struct Export {
    client: Database,
    catalog: String,
    schema: String,
    output_dir: String,
    parallelism: usize,
    target: ExportTarget,
}

impl Export {
    /// Iterate over all db names.
    ///
    /// Newbie: `db_name` is catalog + schema.
    async fn iter_db_names(&self) -> Result<Vec<(String, String)>> {
        if !self.schema.is_empty() {
            Ok(vec![(self.catalog.clone(), self.schema.clone())])
        } else {
            Ok(vec![])
        }
    }

    async fn get_table_list(&self, catalog: &str, schema: &str) -> Result<Vec<String>> {
        let sql = "show tables";
        let mut client = self.client.clone();
        client.set_catalog(catalog);
        client.set_schema(schema);
        let result = client
            .sql(&sql)
            .await
            .with_context(|_| RequestDatabaseSnafu { sql })?;
        let Output::Stream(stream) = result else {
            NotDataFromOutputSnafu.fail()?
        };
        let record_batch = collect(stream)
            .await
            .context(CollectRecordBatchesSnafu)?
            .pop()
            .context(EmptyResultSnafu)?;
        Ok(record_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringVector>()
            .unwrap()
            .iter_data()
            .map(|x| x.unwrap().to_string())
            .collect())
    }

    async fn show_create_table(&self, catalog: &str, schema: &str, table: &str) -> Result<String> {
        let sql = format!("show create table {}.{}.{}", catalog, schema, table);
        let mut client = self.client.clone();
        client.set_catalog(catalog);
        client.set_schema(schema);
        let result = client
            .sql(&sql)
            .await
            .with_context(|_| RequestDatabaseSnafu { sql })?;
        let Output::Stream(stream) = result else {
            NotDataFromOutputSnafu.fail()?
        };
        let record_batch = collect(stream)
            .await
            .context(CollectRecordBatchesSnafu)?
            .pop()
            .context(EmptyResultSnafu)?;
        let create_table = record_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringVector>()
            .unwrap()
            .get_data(0)
            .unwrap();

        Ok(format!("{create_table};\n"))
    }

    async fn export_create_table(&self) -> Result<()> {
        let semaphore = Arc::new(Semaphore::new(self.parallelism));
        let db_names = self.iter_db_names().await?;
        let db_count = db_names.len();
        let mut tasks = Vec::with_capacity(db_names.len());
        for (catalog, schema) in db_names {
            let semaphore_moved = semaphore.clone();
            tasks.push(async move {
                let _permit = semaphore_moved.acquire().await.unwrap();
                let table_list = self.get_table_list(&catalog, &schema).await?;
                tokio::fs::create_dir_all(&self.output_dir)
                    .await
                    .context(FileIoSnafu)?;
                let output_file =
                    Path::new(&self.output_dir).join(format!("{catalog}-{schema}.sql"));
                let mut file = File::create(output_file).await.context(FileIoSnafu)?;
                for table in table_list {
                    match self.show_create_table(&catalog, &schema, &table).await {
                        Err(e) => {
                            error!(e; "Failed to export table {}.{}.{}", catalog, schema, table)
                        }
                        Ok(create_table) => {
                            file.write_all(create_table.as_bytes())
                                .await
                                .context(FileIoSnafu)?;
                        }
                    }
                }
                info!("finished exporting catalog {catalog} schema {schema}");
                Ok::<(), Error>(())
            });
        }

        let success = futures::future::join_all(tasks)
            .await
            .into_iter()
            .filter(|r| match r {
                Ok(_) => true,
                Err(e) => {
                    error!(e; "export job failed");
                    false
                }
            })
            .count();

        info!("success {success}/{db_count} jobs");

        Ok(())
    }
}

#[async_trait]
impl Tool for Export {
    async fn do_work(&self) -> Result<()> {
        match self.target {
            ExportTarget::CreateTable => self.export_create_table().await,
            ExportTarget::TableData => unimplemented!("export table data"),
        }
    }
}
