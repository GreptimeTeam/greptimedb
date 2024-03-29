## Setup tests for multiple storage backend

To run the integration test, please copy `.env.example` to `.env` in the project root folder and change the values on need.

Take `s3` for example. You need to set your S3 bucket, access key id and secret key:

```sh
# Settings for s3 test
GT_S3_BUCKET=S3 bucket
GT_S3_REGION=S3 region
GT_S3_ACCESS_KEY_ID=S3 access key id
GT_S3_ACCESS_KEY=S3 secret access key
```


### Run

Execute the following command in the project root folder:

```
cargo test integration
```

Test s3 storage:

```
cargo test s3
```

Test oss storage:

```
cargo test oss
```

Test azblob storage:

```
cargo test azblob
```

## Setup tests with Kafka wal

To run the integration test, please copy `.env.example` to `.env` in the project root folder and change the values on need.

```sh
GT_KAFKA_ENDPOINTS = localhost:9092
```

### Setup kafka standalone 

```
cd tests-integration/fixtures/kafka 

docker compose -f docker-compose-standalone.yml up
```



