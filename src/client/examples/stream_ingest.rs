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

use api::v1::*;
use client::{Client, Database, DEFAULT_SCHEMA_NAME};
use derive_new::new;
use tracing::{error, info};

fn main() {
    tracing::subscriber::set_global_default(tracing_subscriber::FmtSubscriber::builder().finish())
        .unwrap();

    run();
}

#[tokio::main]
async fn run() {
    let greptimedb_endpoint =
        std::env::var("GREPTIMEDB_ENDPOINT").unwrap_or_else(|_| "localhost:4001".to_owned());

    let greptimedb_dbname =
        std::env::var("GREPTIMEDB_DBNAME").unwrap_or_else(|_| DEFAULT_SCHEMA_NAME.to_owned());

    let grpc_client = Client::with_urls(vec![&greptimedb_endpoint]);

    let client = Database::new_with_dbname(greptimedb_dbname, grpc_client);

    let stream_inserter = client.streaming_inserter().unwrap();

    if let Err(e) = stream_inserter
        .insert(vec![to_insert_request(weather_records_1())])
        .await
    {
        error!("Error: {e:?}");
    }

    if let Err(e) = stream_inserter
        .insert(vec![to_insert_request(weather_records_2())])
        .await
    {
        error!("Error: {e:?}");
    }

    let result = stream_inserter.finish().await;

    match result {
        Ok(rows) => {
            info!("Rows written: {rows}");
        }
        Err(e) => {
            error!("Error: {e:?}");
        }
    };
}

#[derive(new)]
struct WeatherRecord {
    timestamp_millis: i64,
    collector: String,
    temperature: f32,
    humidity: i32,
}

fn weather_records_1() -> Vec<WeatherRecord> {
    vec![
        WeatherRecord::new(1686109527000, "c1".to_owned(), 26.4, 15),
        WeatherRecord::new(1686023127000, "c1".to_owned(), 29.3, 20),
        WeatherRecord::new(1685936727000, "c1".to_owned(), 31.8, 13),
        WeatherRecord::new(1686109527000, "c2".to_owned(), 20.4, 67),
        WeatherRecord::new(1686023127000, "c2".to_owned(), 18.0, 74),
        WeatherRecord::new(1685936727000, "c2".to_owned(), 19.2, 81),
    ]
}

fn weather_records_2() -> Vec<WeatherRecord> {
    vec![
        WeatherRecord::new(1686109527001, "c3".to_owned(), 26.4, 15),
        WeatherRecord::new(1686023127002, "c3".to_owned(), 29.3, 20),
        WeatherRecord::new(1685936727003, "c3".to_owned(), 31.8, 13),
        WeatherRecord::new(1686109527004, "c4".to_owned(), 20.4, 67),
        WeatherRecord::new(1686023127005, "c4".to_owned(), 18.0, 74),
        WeatherRecord::new(1685936727006, "c4".to_owned(), 19.2, 81),
    ]
}

/// This function generates some random data and bundle them into a
/// `InsertRequest`.
///
/// Data structure:
///
/// - `ts`: a timestamp column
/// - `collector`: a tag column
/// - `temperature`: a value field of f32
/// - `humidity`: a value field of i32
///
fn to_insert_request(records: Vec<WeatherRecord>) -> InsertRequest {
    // convert records into columns
    let rows = records.len();

    // transpose records into columns
    let (timestamp_millis, collectors, temp, humidity) = records.into_iter().fold(
        (
            Vec::with_capacity(rows),
            Vec::with_capacity(rows),
            Vec::with_capacity(rows),
            Vec::with_capacity(rows),
        ),
        |mut acc, rec| {
            acc.0.push(rec.timestamp_millis);
            acc.1.push(rec.collector);
            acc.2.push(rec.temperature);
            acc.3.push(rec.humidity);

            acc
        },
    );

    let columns = vec![
        // timestamp column: `ts`
        Column {
            column_name: "ts".to_owned(),
            values: Some(column::Values {
                timestamp_millisecond_values: timestamp_millis,
                ..Default::default()
            }),
            semantic_type: SemanticType::Timestamp as i32,
            datatype: ColumnDataType::TimestampMillisecond as i32,
            ..Default::default()
        },
        // tag column: collectors
        Column {
            column_name: "collector".to_owned(),
            values: Some(column::Values {
                string_values: collectors.into_iter().collect(),
                ..Default::default()
            }),
            semantic_type: SemanticType::Tag as i32,
            datatype: ColumnDataType::String as i32,
            ..Default::default()
        },
        // field column: temperature
        Column {
            column_name: "temperature".to_owned(),
            values: Some(column::Values {
                f32_values: temp,
                ..Default::default()
            }),
            semantic_type: SemanticType::Field as i32,
            datatype: ColumnDataType::Float32 as i32,
            ..Default::default()
        },
        // field column: humidity
        Column {
            column_name: "humidity".to_owned(),
            values: Some(column::Values {
                i32_values: humidity,
                ..Default::default()
            }),
            semantic_type: SemanticType::Field as i32,
            datatype: ColumnDataType::Int32 as i32,
            ..Default::default()
        },
    ];

    InsertRequest {
        table_name: "weather_demo".to_owned(),
        columns,
        row_count: rows as u32,
    }
}
