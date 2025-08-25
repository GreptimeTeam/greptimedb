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
cd tests-integration/fixtures

docker compose -f docker-compose.yml up kafka
```

## Setup tests with etcd TLS

This guide explains how to set up and test TLS-enabled etcd connections in GreptimeDB integration tests.

### Quick Start

TLS certificates are already at `tests-integration/fixtures/etcd-tls-certs/`.

1. **Start TLS-enabled etcd**:
   ```bash
   cd tests-integration/fixtures
   docker compose up etcd-tls -d
   ```

2. **Start all services (including etcd-tls)**:
   ```bash
   cd tests-integration/fixtures
   docker compose up -d --wait
   ```

### Certificate Details

The checked-in certificates include:
- `ca.crt` - Certificate Authority certificate
- `server.crt` / `server-key.pem` - Server certificate for etcd-tls service
- `client.crt` / `client-key.pem` - Client certificate for connecting to etcd-tls

The server certificate includes SANs for `localhost`, `etcd-tls`, `127.0.0.1`, and `::1`.

### Regenerating Certificates (Optional)

If you need to regenerate the certificates:
```bash
# Regenerate certificates (overwrites existing ones)
./scripts/generate-etcd-tls-certs.sh

# Or generate in custom location
./scripts/generate-etcd-tls-certs.sh /path/to/cert/directory
```

**Note**: The checked-in certificates are for testing purposes only and should never be used in production.