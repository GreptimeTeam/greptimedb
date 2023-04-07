FROM centos:7 as builder

ENV LANG en_US.utf8
WORKDIR /greptimedb

RUN sed -e 's|^mirrorlist=|#mirrorlist=|g' \
             -e 's|^#baseurl=http://mirror.centos.org/centos|baseurl=http://mirrors.tuna.tsinghua.edu.cn/centos|g' \
             -i.bak \
             /etc/yum.repos.d/CentOS-*.repo

# Install dependencies.
RUN RUN ulimit -n 1024000 && yum install -y epel-release openssl openssl-devel centos-release-scl
RUN yum groupinstall -y 'Development Tools'
RUN yum install -y rh-python38 rh-python38-python-devel

# Install protoc
RUN curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v3.15.8/protoc-3.15.8-linux-x86_64.zip
RUN unzip protoc-3.15.8-linux-x86_64.zip -d /usr/local/

# Install Rust.
SHELL ["/bin/bash", "-c"]
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --no-modify-path --default-toolchain none -y
ENV PATH /opt/rh/rh-python38/root/usr/bin:/usr/local/bin:/root/.cargo/bin/:$PATH

# Build the project in release mode.
COPY . .
RUN cargo build --release

# Export the binary to the clean image.
# TODO(zyy17): Maybe should use the more secure container image.
FROM ubuntu:22.04 as base

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get -y install ca-certificates

WORKDIR /greptime
COPY --from=builder /greptimedb/target/release/greptime /greptime/bin/
ENV PATH /greptime/bin/:$PATH

ENTRYPOINT ["greptime"]
