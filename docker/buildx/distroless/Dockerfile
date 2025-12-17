FROM ubuntu:22.04 AS builder

ARG CARGO_PROFILE
ARG FEATURES
ARG OUTPUT_DIR

ENV LANG=en_US.utf8
WORKDIR /greptimedb

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y software-properties-common

# Install dependencies.
RUN --mount=type=cache,target=/var/cache/apt \
    apt-get update && apt-get install -y \
    libssl-dev \
    protobuf-compiler \
    curl \
    git \
    build-essential \
    pkg-config

# Install Rust.
SHELL ["/bin/bash", "-c"]
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --no-modify-path --default-toolchain none -y
ENV PATH=/root/.cargo/bin/:$PATH

# Build the project in release mode.
RUN --mount=target=. \
    --mount=type=cache,target=/root/.cargo/registry \
    make build \
    CARGO_PROFILE=${CARGO_PROFILE} \
    FEATURES=${FEATURES} \
    TARGET_DIR=/out/target

FROM ubuntu:22.04 AS libs

ARG TARGETARCH

# Copy required library dependencies based on architecture
RUN if [ "$TARGETARCH" = "amd64" ]; then \
        cp /lib/x86_64-linux-gnu/libz.so.1.2.11 /lib/x86_64-linux-gnu/libz.so.1; \
    elif [ "$TARGETARCH" = "arm64" ]; then \
        cp /lib/aarch64-linux-gnu/libz.so.1.2.11 /lib/aarch64-linux-gnu/libz.so.1; \
    else \
        echo "Unsupported architecture: $TARGETARCH" && exit 1; \
    fi

# Export the binary to the clean distroless image.
FROM gcr.io/distroless/cc-debian12:latest AS base

ARG OUTPUT_DIR
ARG TARGETARCH

# Copy required library dependencies
COPY --from=libs /lib /lib
COPY --from=busybox:stable /bin/busybox /bin/busybox

WORKDIR /greptime
COPY --from=builder /out/target/${OUTPUT_DIR}/greptime /greptime/bin/greptime
ENV PATH=/greptime/bin/:$PATH

ENV MALLOC_CONF="prof:true,prof_active:false"

ENTRYPOINT ["greptime"]
