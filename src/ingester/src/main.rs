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

use clap::Parser;
use common_time::timestamp::TimeUnit;
use datanode::config::StorageConfig;
use meta_client::MetaClientOptions;
use mito2::config::MitoConfig;
use mito2::sst::file::IndexType;
use mito2::sst::parquet::SstInfo;
use serde::{Deserialize, Serialize};
use sst_convert::converter::{InputFile, InputFileType, SstConverter, SstConverterBuilder};

#[derive(Parser, Debug)]
#[command(version, about = "Greptime Ingester", long_about = None)]
struct Args {
    /// Input directory
    #[arg(short, long)]
    input_dir: String,
    /// Directory of input parquet files, relative to input_dir
    #[arg(short, long)]
    parquet_dir: Option<String>,
    /// Directory of input json files, relative to input_dir
    #[arg(short, long)]
    json_dir: Option<String>,
    /// Config file
    #[arg(short, long)]
    cfg: String,
    /// DB HTTP address
    #[arg(short, long)]
    db_http_addr: String,

    /// Output path for the converted SST files.
    /// If it is not None, the converted SST files will be written to the specified path
    /// in the `input_store`.
    /// This is for debugging purposes.
    #[arg(short, long)]
    sst_output_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct IngesterConfig {
    meta_client: MetaClientOptions,
    storage: StorageConfig,
    mito: MitoConfig,
}

#[allow(unreachable_code)]
#[tokio::main]
async fn main() {
    let args = Args::parse();

    let cfg_file = std::fs::read_to_string(&args.cfg).expect("Failed to read config file");
    let cfg: IngesterConfig = toml::from_str(&cfg_file).expect("Failed to parse config");

    let mut sst_converter = {
        let mut builder = SstConverterBuilder::new_fs(args.input_dir)
            .with_meta_options(cfg.meta_client)
            .with_storage_config(cfg.storage)
            .with_config(cfg.mito);

        if let Some(output_path) = args.sst_output_path {
            builder = builder.with_output_path(output_path);
        }

        builder
            .build()
            .await
            .expect("Failed to build sst converter")
    };

    let input_store = sst_converter.input_store.clone();

    if let Some(parquet_dir) = args.parquet_dir {
        // using opendal to read parquet files in given input object store
        let all_parquets = input_store
            .list(&parquet_dir)
            .await
            .expect("Failed to list parquet files");

        let all_parquets = all_parquets
            .iter()
            .filter(|parquet| parquet.name().ends_with(".parquet") && parquet.metadata().is_file())
            .collect::<Vec<_>>();

        let input_files = all_parquets
            .iter()
            .map(|parquet| {
                let full_table_name = parquet.name().split("-").next().unwrap();
                let mut names = full_table_name.split('.').rev();
                let table_name = names.next().unwrap();
                let schema_name = names.next().unwrap();
                let catalog_name = names.next().unwrap_or("greptime");

                InputFile {
                    catalog: catalog_name.to_string(),
                    schema: schema_name.to_string(),
                    table: table_name.to_string(),
                    path: parquet.path().to_string(),
                    file_type: InputFileType::Parquet,
                }
            })
            .collect::<Vec<_>>();

        convert_and_send(&input_files, &mut sst_converter, &args.db_http_addr).await;
    }

    if let Some(json_dir) = args.json_dir {
        // using opendal to read json files in given input object store
        let all_jsons = input_store
            .list(&json_dir)
            .await
            .expect("Failed to list json files");

        let all_jsons = all_jsons
            .iter()
            .filter(|json| json.name().ends_with(".json") && json.metadata().is_file())
            .collect::<Vec<_>>();

        let input_files = all_jsons
            .iter()
            .map(|entry| {
                let full_table_name = entry.name().split("-").next().unwrap();
                let mut names = full_table_name.split('.').rev();
                let table_name = names.next().unwrap();
                let schema_name = names.next().unwrap();
                let catalog_name = names.next().unwrap_or("greptime");

                InputFile {
                    catalog: catalog_name.to_string(),
                    schema: schema_name.to_string(),
                    table: table_name.to_string(),
                    path: entry.path().to_string(),
                    file_type: InputFileType::RemoteWrite,
                }
            })
            .collect::<Vec<_>>();

        convert_and_send(&input_files, &mut sst_converter, &args.db_http_addr).await;
    }
}

async fn convert_and_send(
    input_files: &[InputFile],
    sst_converter: &mut SstConverter,
    db_http_addr: &str,
) {
    let table_names = input_files
        .iter()
        .map(|f| (f.schema.clone(), f.table.clone()))
        .collect::<Vec<_>>();

    let sst_infos = sst_converter
        .convert(input_files)
        .await
        .expect("Failed to convert parquet files");

    let ingest_reqs = table_names
        .iter()
        .zip(sst_infos.iter())
        .flat_map(|(schema_name, sst_info)| {
            sst_info
                .ssts
                .iter()
                .map(|sst| to_ingest_sst_req(&schema_name.0, &schema_name.1, sst))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    // send ingest requests to DB
    send_ingest_requests(db_http_addr, ingest_reqs)
        .await
        .unwrap();
}

async fn send_ingest_requests(
    addr: &str,
    reqs: Vec<ClientIngestSstRequest>,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    for req in reqs {
        client.post(addr).json(&req).send().await?;
    }
    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ClientIngestSstRequest {
    schema: Option<String>,
    table: String,
    pub(crate) file_id: String,
    pub(crate) min_ts: i64,
    pub(crate) max_ts: i64,
    pub(crate) file_size: u64,
    pub(crate) rows: u32,
    pub(crate) row_groups: u32,
    /// Available indexes of the file.
    pub available_indexes: Vec<IndexType>,
    /// Size of the index file.
    pub index_file_size: u64,
}

fn to_ingest_sst_req(
    schema_name: &str,
    table_name: &str,
    sst_info: &SstInfo,
) -> ClientIngestSstRequest {
    let index_file_size = sst_info.index_metadata.file_size;
    let available_indexs = sst_info.index_metadata.build_available_indexes();
    ClientIngestSstRequest {
        schema: Some(schema_name.to_string()),
        table: table_name.to_string(),
        file_id: sst_info.file_id.to_string(),
        min_ts: sst_info
            .time_range
            .0
            .convert_to(TimeUnit::Microsecond)
            .unwrap()
            .value(),
        max_ts: sst_info
            .time_range
            .1
            .convert_to(TimeUnit::Microsecond)
            .unwrap()
            .value(),
        file_size: sst_info.file_size,
        rows: sst_info.num_rows as _,
        row_groups: sst_info.num_row_groups as _,
        available_indexes: available_indexs.to_vec(),
        index_file_size,
    }
}
