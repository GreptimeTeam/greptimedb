FROM ubuntu:22.04 as builder

ENV LANG en_US.utf8
WORKDIR /greptimedb

# Install dependencies.
RUN apt update && apt install -y \
    libssl-dev \
    protobuf-compiler \
    curl \
    build-essential \
    pkg-config \
    python3.10 \
    python3.10-dev \
    python3-pip
RUN pip install pyarrow

# Install Rust.
SHELL ["/bin/bash", "-c"]
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --no-modify-path --default-toolchain none -y
ENV PATH /root/.cargo/bin/:$PATH

# Build the project in release mode.
COPY . .
# Install cross platform toolchain
RUN apt -y update && \
    apt -y install libssl-dev pkg-config g++-aarch64-linux-gnu gcc-aarch64-linux-gnu && \
    apt install binutils-aarch64-linux-gnu

# Download and compile Python3.10.10 with shared library support(--enable-shared).
RUN apt -y install wget
RUN wget https://www.python.org/ftp/python/3.10.10/Python-3.10.10.tgz && \
    tar -xvf Python-3.10.10.tgz
RUN cd Python-3.10.10 && \
    ./configure --build=x86_64-linux-gnu --host=aarch64-linux-gnu \
    --prefix=/greptimedb/py_build --enable-optimizations \
    CC=aarch64-linux-gnu-gcc \
    CXX=aarch64-linux-gnu-g++ \
    AR=aarch64-linux-gnu-ar \
    LD=aarch64-linux-gnu-ld \
    RANLIB=aarch64-linux-gnu-ranlib \
    --enable-shared \
    --with-system-ffi \
    ac_cv_pthread_is_default=no ac_cv_pthread=yes ac_cv_cxx_thread=yes \
    ac_cv_have_long_long_format=yes \
    --disable-ipv6 ac_cv_file__dev_ptmx=no ac_cv_file__dev_ptc=no && \
    make -j10 && make altinstall
# Install rustup target for cross compiling.
RUN rustup target add aarch64-unknown-linux-gnu
# Set the environment variable for cross compiling and compile it
RUN export PYO3_CROSS_LIB_DIR=/greptimedb/py_build/lib && \ 
    cargo build --target aarch64-unknown-linux-gnu --release

