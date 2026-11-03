# Artifact image: binaries run on the query-regression ECS host after extraction.
# Build on the same ECS image as the consumer to preserve the runtime ABI.
FROM scratch
ARG SOURCE_REVISION
LABEL org.opencontainers.image.source="https://github.com/GreptimeTeam/greptimedb"
LABEL org.opencontainers.image.revision=$SOURCE_REVISION
COPY query_perf_fixture query_regression_runner /
