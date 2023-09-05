use std::collections::HashMap;
use std::time::Duration;

use api::v1::Rows;
use common_recordbatch::RecordBatches;
use common_telemetry::init_default_ut_logging;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{
    RegionCloseRequest, RegionFlushRequest, RegionOpenRequest, RegionRequest, RegionTruncateRequest,
};
use store_api::storage::RegionId;

use super::ScanRequest;
use crate::config::MitoConfig;
use crate::test_util::{build_rows, put_rows, rows_schema, CreateRequestBuilder, TestEnv};

fn has_parquet_file(sst_dir: &str) -> bool {
    for entry in std::fs::read_dir(sst_dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if !path.is_dir() {
            assert_eq!("parquet", path.extension().unwrap());
            return true;
        }
    }

    false
}

#[tokio::test]
async fn test_engine_truncate_region_basic() {
    let mut env = TestEnv::with_prefix("truncate-basic");
    let engine = env.create_engine(MitoConfig::default()).await;

    // Create the region.
    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Put data to the region.
    let rows = Rows {
        schema: column_schemas,
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    // Scan the region.
    let request = ScanRequest::default();
    let stream = engine.handle_query(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
+-------+---------+---------------------+";
    assert_eq!(expected, batches.pretty_print().unwrap());

    // Truncate the region.
    engine
        .handle_request(region_id, RegionRequest::Truncate(RegionTruncateRequest {}))
        .await
        .unwrap();

    // Scan the region.
    let request = ScanRequest::default();
    let stream = engine.handle_query(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "++\n++";
    assert_eq!(expected, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_engine_put_data_after_truncate() {
    let mut env = TestEnv::with_prefix("truncate-put");
    let engine = env.create_engine(MitoConfig::default()).await;

    // Create the region.
    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Put data to the region.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    // Scan the region.
    let request = ScanRequest::default();
    let stream = engine.handle_query(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
+-------+---------+---------------------+";
    assert_eq!(expected, batches.pretty_print().unwrap());

    // Truncate the region.
    engine
        .handle_request(region_id, RegionRequest::Truncate(RegionTruncateRequest {}))
        .await
        .unwrap();

    // Put data to the region again.
    let rows = Rows {
        schema: column_schemas,
        rows: build_rows(5, 8),
    };
    put_rows(&engine, region_id, rows).await;

    // Scan the region.
    let request = ScanRequest::default();
    let stream = engine.handle_query(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 5     | 5.0     | 1970-01-01T00:00:05 |
| 6     | 6.0     | 1970-01-01T00:00:06 |
| 7     | 7.0     | 1970-01-01T00:00:07 |
+-------+---------+---------------------+";
    assert_eq!(expected, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_engine_truncate_after_flush() {
    init_default_ut_logging();
    let mut env = TestEnv::with_prefix("truncate-flush");
    let region_dir;
    {
        let engine = env.create_engine(MitoConfig::default()).await;

        // Create the region.
        let region_id = RegionId::new(1, 1);
        let request = CreateRequestBuilder::new().build();
        let column_schemas = rows_schema(&request);
        engine
            .handle_request(region_id, RegionRequest::Create(request))
            .await
            .unwrap();

        // Put data to the region.
        let rows = Rows {
            schema: column_schemas.clone(),
            rows: build_rows(0, 3),
        };
        put_rows(&engine, region_id, rows).await;

        // Flush the region.
        engine
            .handle_request(region_id, RegionRequest::Flush(RegionFlushRequest {}))
            .await
            .unwrap();

        let region = engine.get_region(region_id).unwrap();
        region_dir = env
            .get_data_path()
            .join(region.access_layer.region_dir())
            .display()
            .to_string();
        assert!(has_parquet_file(&region_dir));

        // Truncate the region.
        engine
            .handle_request(region_id, RegionRequest::Truncate(RegionTruncateRequest {}))
            .await
            .unwrap();

        // Put data to the region.
        let rows = Rows {
            schema: column_schemas,
            rows: build_rows(5, 8),
        };
        put_rows(&engine, region_id, rows).await;

        // Scan the region.
        let request = ScanRequest::default();
        let stream = engine.handle_query(region_id, request).await.unwrap();
        let batches = RecordBatches::try_collect(stream).await.unwrap();
        let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 5     | 5.0     | 1970-01-01T00:00:05 |
| 6     | 6.0     | 1970-01-01T00:00:06 |
| 7     | 7.0     | 1970-01-01T00:00:07 |
+-------+---------+---------------------+";
        assert_eq!(expected, batches.pretty_print().unwrap());
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!has_parquet_file(&region_dir));
}

#[tokio::test]
async fn test_engine_truncate_reopen() {
    let mut env = TestEnv::with_prefix("truncate-reopen");
    let engine = env.create_engine(MitoConfig::default()).await;

    // Create the region.
    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let region_dir = request.region_dir.clone();

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Put data to the region.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    // Flush the region.
    engine
        .handle_request(region_id, RegionRequest::Flush(RegionFlushRequest {}))
        .await
        .unwrap();

    // Truncate the region
    engine
        .handle_request(region_id, RegionRequest::Truncate(RegionTruncateRequest {}))
        .await
        .unwrap();

    // Close the region.
    engine
        .handle_request(region_id, RegionRequest::Close(RegionCloseRequest {}))
        .await
        .unwrap();

    // Reopen the region again.
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                region_dir,
                options: HashMap::default(),
            }),
        )
        .await
        .unwrap();

    // Scan the region.
    let request = ScanRequest::default();
    let stream = engine.handle_query(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "++\n++";
    assert_eq!(expected, batches.pretty_print().unwrap());
}
