FROM centos:7 as builder

ARG CARGO_PROFILE
ARG FEATURES
ARG OUTPUT_DIR

ENV LANG en_US.utf8
WORKDIR /greptimedb

# Install dependencies
RUN ulimit -n 1024000 && yum groupinstall -y 'Development Tools'
RUN yum install -y epel-release  \
    openssl \
    openssl-devel  \
    centos-release-scl  \
    rh-python38  \
    rh-python38-python-devel \
    which

# Install protoc
RUN curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v3.15.8/protoc-3.15.8-linux-x86_64.zip
RUN unzip protoc-3.15.8-linux-x86_64.zip -d /usr/local/

# Install Rust
SHELL ["/bin/bash", "-c"]
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --no-modify-path --default-toolchain none -y
ENV PATH /opt/rh/rh-python38/root/usr/bin:/usr/local/bin:/root/.cargo/bin/:$PATH

# Build the project in release mode.
RUN --mount=target=.,rw \
    --mount=type=cache,target=/root/.cargo/registry \
    make build \
    CARGO_PROFILE=${CARGO_PROFILE} \
    FEATURES=${FEATURES} \
    TARGET_DIR=/out/target

# Export the binary to the clean image.
FROM centos:7 as base

ARG OUTPUT_DIR

RUN yum install -y epel-release \
    openssl \
    openssl-devel  \
    centos-release-scl  \
    rh-python38  \
    rh-python38-python-devel \
    which

WORKDIR /greptime
COPY --from=builder /out/target/${OUTPUT_DIR}/greptime /greptime/bin/
ENV PATH /greptime/bin/:$PATH

ENTRYPOINT ["greptime"]
