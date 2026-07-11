FROM ghcr.io/astral-sh/uv@sha256:3d868e555f8f1dbc324afa005066cd11e1053fc4743b9808ca8025283e65efa5 AS uv
FROM ghcr.io/actions/actions-runner@sha256:08c30b0a7105f64bddfc485d2487a22aa03932a791402393352fdf674bda2c29

USER root

ARG SCCACHE_VERSION=0.16.0
ARG SCCACHE_SHA256=aec995a83ad3dff3d14b6314e08858b7b73d35ca85a5bcf3d3a9ec07dee35588

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

USER runner
