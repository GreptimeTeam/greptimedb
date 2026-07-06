FROM ghcr.io/astral-sh/uv@sha256:3d868e555f8f1dbc324afa005066cd11e1053fc4743b9808ca8025283e65efa5 AS uv
FROM ghcr.io/actions/actions-runner@sha256:08c30b0a7105f64bddfc485d2487a22aa03932a791402393352fdf674bda2c29

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
