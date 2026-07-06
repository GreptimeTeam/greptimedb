ARG UV_VERSION=0.11.26

FROM ghcr.io/astral-sh/uv:${UV_VERSION} AS uv
FROM ghcr.io/actions/actions-runner:latest

ARG UV_VERSION

USER root

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

USER runner
