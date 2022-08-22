FROM ubuntu:22.04 as builder

ENV LANG en_US.utf8
WORKDIR /greptimedb

# Install dependencies.
RUN apt-get update && apt-get install -y \
    libssl-dev \
    protobuf-compiler \
    curl \
    build-essential \
    pkg-config

# Install Rust.
SHELL ["/bin/bash", "-c"]
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --no-modify-path --default-toolchain none -y
ENV PATH /root/.cargo/bin/:$PATH

# Build the project in release mode.
COPY . .
RUN cargo build --release

# Export the binary to the clean image.
# TODO(zyy17): Maybe should use the more secure container image.
FROM ubuntu:22.04 as base

WORKDIR /greptimedb
COPY --from=builder /greptimedb/target/release/greptime /greptimedb/bin/
ENV PATH /greptimedb/bin/:$PATH

ENTRYPOINT [ "greptime" ]
CMD [ "datanode", "start"]
