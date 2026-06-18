# Fuzz Utilities

Utilities in this directory support fuzz tests and related CI/manual workflows.

## Kafka WAL Helper

`kafka_wal_helper` is a small HTTP service used by Remote WAL fuzz tests. It runs near Kafka, usually inside Kubernetes, and provides the minimum Kafka operations needed to simulate external WAL TTL deletion:

- read latest offsets for configured WAL topics;
- delete Kafka records up to previously recorded offsets.

This avoids exposing Kafka outside Kubernetes or relying on broker advertised listeners to be reachable from the fuzz runner.

Source and related files:

- `tests-fuzz/utils/kafka_wal_helper.rs` — HTTP helper binary.
- `docker/ci/ubuntu/Dockerfile.kafka-wal-helper` — lightweight runtime image.
- `.github/actions/setup-greptimedb-cluster/kafka-wal-helper.yaml` — Kubernetes Deployment and Service.

### Build the binary

Build the helper binary with the nightly profile used by fuzz targets:

```bash
cargo build --bin kafka_wal_helper --profile nightly
```

The binary is expected at:

```text
target/nightly/kafka_wal_helper
```

### Build the Docker image

The Dockerfile is runtime-only. Build the Rust binary first, then copy it into an architecture-specific directory expected by the image build.

For `amd64`:

```bash
mkdir -p amd64
cp target/nightly/kafka_wal_helper amd64/kafka_wal_helper

docker build \
  -f docker/ci/ubuntu/Dockerfile.kafka-wal-helper \
  --build-arg TARGETARCH=amd64 \
  -t <registry>/tools/kafka_wal_helper:<tag> \
  .
```

Push the image:

```bash
docker push <registry>/tools/kafka_wal_helper:<tag>
```

### Deploy to Kubernetes

Update `.github/actions/setup-greptimedb-cluster/kafka-wal-helper.yaml` to use the image you pushed:

```yaml
image: <registry>/tools/kafka_wal_helper:<tag>
```

Apply the helper manifest to the namespace where you want the helper to run:

```bash
kubectl -n kafka-cluster apply \
  -f .github/actions/setup-greptimedb-cluster/kafka-wal-helper.yaml
```

Forward the helper service to a local port:

```bash
kubectl -n kafka-cluster port-forward svc/kafka-wal-helper 8080:8080
```

Then call the helper through `127.0.0.1:8080`.

### HTTP API

#### Health check

```bash
curl http://127.0.0.1:8080/health
```

#### Record latest offsets

```bash
curl -s http://127.0.0.1:8080/record-offsets \
  -H 'content-type: application/json' \
  -d '{
    "broker_endpoints":["kafka-broker-0.kafka-broker-headless.test-env-kafka.svc.cluster.local:9092"],
    "topic_prefix":"greptimedb_wal_topic",
    "num_topics":3,
    "partition":0
  }'
```

Example response:

```json
{
  "offsets": [
    {"topic":"greptimedb_wal_topic_0","partition":0,"offset":123},
    {"topic":"greptimedb_wal_topic_1","partition":0,"offset":456},
    {"topic":"greptimedb_wal_topic_2","partition":0,"offset":789}
  ]
}
```

#### Delete records

Use offsets returned by `/record-offsets` as the deletion boundary. Do not delete to the current latest offsets after additional writes, otherwise the test may remove data that should remain replayable.

```bash
curl -s http://127.0.0.1:8080/delete-records \
  -H 'content-type: application/json' \
  -d '{
    "broker_endpoints":["kafka-broker-0.kafka-broker-headless.test-env-kafka.svc.cluster.local:9092"],
    "timeout_ms":5000,
    "offsets":[
      {"topic":"greptimedb_wal_topic_0","partition":0,"offset":123},
      {"topic":"greptimedb_wal_topic_1","partition":0,"offset":456},
      {"topic":"greptimedb_wal_topic_2","partition":0,"offset":789}
    ]
  }'
```

Example response:

```json
{
  "deleted": [
    {"topic":"greptimedb_wal_topic_0","partition":0,"offset":123},
    {"topic":"greptimedb_wal_topic_1","partition":0,"offset":456},
    {"topic":"greptimedb_wal_topic_2","partition":0,"offset":789}
  ]
}
```
