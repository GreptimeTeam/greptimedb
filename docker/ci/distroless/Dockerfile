FROM ubuntu:22.04 AS libs

ARG TARGETARCH

# Copy required library dependencies based on architecture
# TARGETARCH values: amd64, arm64
# Ubuntu library paths: x86_64-linux-gnu, aarch64-linux-gnu
RUN if [ "$TARGETARCH" = "amd64" ]; then \
        mkdir -p /output/x86_64-linux-gnu && \
        cp /lib/x86_64-linux-gnu/libz.so.1.2.11 /output/x86_64-linux-gnu/libz.so.1; \
    elif [ "$TARGETARCH" = "arm64" ]; then \
        mkdir -p /output/aarch64-linux-gnu && \
        cp /lib/aarch64-linux-gnu/libz.so.1.2.11 /output/aarch64-linux-gnu/libz.so.1; \
    else \
        echo "Unsupported architecture: $TARGETARCH" && exit 1; \
    fi

FROM gcr.io/distroless/cc-debian12:latest

# The root path under which contains all the dependencies to build this Dockerfile.
ARG DOCKER_BUILD_ROOT=.
# The binary name of GreptimeDB executable.
# Defaults to "greptime", but sometimes in other projects it might be different.
ARG TARGET_BIN=greptime

ARG TARGETARCH

# Copy required library dependencies
COPY --from=libs /output /lib
COPY --from=busybox:stable /bin/busybox /bin/busybox

ADD $TARGETARCH/$TARGET_BIN /greptime/bin/

ENV PATH=/greptime/bin/:$PATH

ENV TARGET_BIN=$TARGET_BIN

ENV MALLOC_CONF="prof:true,prof_active:false"

ENTRYPOINT ["greptime"]
