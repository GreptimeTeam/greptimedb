FROM golang:1.23@sha256:e87b2a5f6df2dff71ea330d55d54f4979eb380ae58a7e3aabc9d53121243e689 AS otelgen

ARG OTELGEN_COMMIT=863a3f395d062c7322cc1de08a38774b7fdaa6c8

RUN git init /tmp/otelgen \
    && git -C /tmp/otelgen remote add origin https://github.com/WenyXu/otelgen.git \
    && git -C /tmp/otelgen fetch --depth=1 origin "${OTELGEN_COMMIT}" \
    && test "$(git -C /tmp/otelgen rev-parse FETCH_HEAD)" = "${OTELGEN_COMMIT}" \
    && git -C /tmp/otelgen checkout --detach FETCH_HEAD \
    && cd /tmp/otelgen \
    && CGO_ENABLED=0 go build -trimpath \
        -ldflags "-s -w -X main.version=${OTELGEN_COMMIT} -X main.commit=${OTELGEN_COMMIT}" \
        -o /usr/local/bin/otelgen ./cmd/otelgen

FROM ghcr.io/astral-sh/uv@sha256:3d868e555f8f1dbc324afa005066cd11e1053fc4743b9808ca8025283e65efa5 AS uv
FROM ghcr.io/actions/actions-runner@sha256:08c30b0a7105f64bddfc485d2487a22aa03932a791402393352fdf674bda2c29

USER root

ARG SCCACHE_VERSION=0.16.0
ARG SCCACHE_SHA256=aec995a83ad3dff3d14b6314e08858b7b73d35ca85a5bcf3d3a9ec07dee35588
ARG RUSTUP_INIT_VERSION=1.29.0
ARG RUSTUP_INIT_TARGET=x86_64-unknown-linux-gnu
ARG RUSTUP_INIT_SHA256=4acc9acc76d5079515b46346a485974457b5a79893cfb01112423c89aeb5aa10
ARG RUST_TOOLCHAIN=nightly-2026-03-21

ENV RUSTUP_HOME=/opt/rustup \
    CARGO_HOME=/opt/cargo \
    RUSTUP_TOOLCHAIN=${RUST_TOOLCHAIN} \
    RUSTUP_AUTO_INSTALL=0 \
    PATH=/opt/cargo/bin:${PATH}

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        clang \
        cmake \
        curl \
        git \
        gzip \
        jq \
        libprotobuf-dev \
        libssl-dev \
        mold \
        openssh-client \
        pkg-config \
        protobuf-compiler \
        python3 \
        sudo \
        tar \
        unzip \
        wget \
        xz-utils \
        zip \
        zstd \
    && rm -rf /var/lib/apt/lists/*

COPY --from=uv /uv /uvx /usr/local/bin/
COPY --from=otelgen /usr/local/bin/otelgen /usr/local/bin/otelgen

RUN curl --fail --location --silent --show-error \
        --output /tmp/sccache.tar.gz \
        "https://github.com/mozilla/sccache/releases/download/v${SCCACHE_VERSION}/sccache-v${SCCACHE_VERSION}-x86_64-unknown-linux-musl.tar.gz" \
    && echo "${SCCACHE_SHA256}  /tmp/sccache.tar.gz" | sha256sum --check --status - \
    && mkdir --parents /tmp/sccache \
    && tar --extract --gzip --file /tmp/sccache.tar.gz \
        --directory /tmp/sccache --strip-components=1 \
        "sccache-v${SCCACHE_VERSION}-x86_64-unknown-linux-musl/sccache" \
    && install --mode=0755 /tmp/sccache/sccache /usr/local/bin/sccache \
    && rm -rf /tmp/sccache.tar.gz /tmp/sccache

RUN curl --fail --location --silent --show-error \
        --output /tmp/rustup-init \
        "https://static.rust-lang.org/rustup/archive/${RUSTUP_INIT_VERSION}/${RUSTUP_INIT_TARGET}/rustup-init" \
    && echo "${RUSTUP_INIT_SHA256}  /tmp/rustup-init" | sha256sum --check --status - \
    && chmod 0755 /tmp/rustup-init \
    && /tmp/rustup-init -y --profile minimal --default-toolchain "${RUST_TOOLCHAIN}" --no-modify-path \
    && rm -f /tmp/rustup-init \
    && chown -R root:root /opt/rustup /opt/cargo \
    && chmod -R go-w /opt/rustup /opt/cargo \
    && find /opt/rustup /opt/cargo -type d -exec chmod 0755 {} + \
    && find /opt/rustup /opt/cargo -type f -exec chmod a+r {} +

USER runner

RUN set -eu \
    && test "$(id -u)" = "1001" \
    && temporary_cargo_home="$(mktemp --directory)" \
    && temporary_proto_dir="" \
    && cleanup() { rm -rf "${temporary_cargo_home}" "${temporary_proto_dir}"; } \
    && trap cleanup 0 \
    && export CARGO_HOME="${temporary_cargo_home}" \
    && test "${RUSTUP_AUTO_INSTALL}" = "0" \
    && rustup --version \
    && cargo --version \
    && rustc --version \
    && active_toolchain="$(rustup show active-toolchain)" \
    && printf 'Active toolchain: %s\n' "${active_toolchain}" \
    && case "${active_toolchain}" in "${RUST_TOOLCHAIN}-x86_64-unknown-linux-gnu"|"${RUST_TOOLCHAIN}-x86_64-unknown-linux-gnu "*) ;; *) exit 1;; esac \
    && test ! -w /opt/rustup \
    && test ! -w /opt/cargo/bin \
    && test -r /usr/include/google/protobuf/any.proto \
    && test -r /usr/include/google/protobuf/empty.proto \
    && temporary_proto_dir="$(mktemp --directory)" \
    && printf '%s\n' \
        'syntax = "proto3";' \
        'package smoke;' \
        'import "google/protobuf/any.proto";' \
        'import "google/protobuf/empty.proto";' \
        'message Smoke { google.protobuf.Any any = 1; google.protobuf.Empty empty = 2; }' \
        > "${temporary_proto_dir}/smoke.proto" \
    && protoc --proto_path="${temporary_proto_dir}" --proto_path=/usr/include \
        --descriptor_set_out="${temporary_proto_dir}/smoke.pb" \
        "${temporary_proto_dir}/smoke.proto" \
    && test -s "${temporary_proto_dir}/smoke.pb"
