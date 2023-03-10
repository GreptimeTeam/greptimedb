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


# Install cross platform toolchain
RUN apt-get -y update && \
    apt-get -y install libssl-dev pkg-config g++-aarch64-linux-gnu gcc-aarch64-linux-gnu && \
    apt-get install binutils-aarch64-linux-gnu



# Download and compile Python3.10.10 with shared library support(--enable-shared).
RUN apt-get -y install wget
RUN wget https://www.python.org/ftp/python/3.10.10/Python-3.10.10.tgz && \
    tar -xvf Python-3.10.10.tgz
# explain Python compile options here a bit:s
# --enable-shared: enable building a shared Python library (default is no) but we do need it for calling from rust
# CC, CXX, AR, LD, RANLIB: set the compiler, archiver, linker, and ranlib programs to use
# build: the machine you are building on, host: the machine you will run the compiled program on
# --with-system-ffi: build _ctypes module using an installed ffi library, see Doc/library/ctypes.rst, not used in here TODO: could remove
# ac_cv_pthread_is_default=no ac_cv_pthread=yes ac_cv_cxx_thread=yes:
# allow cross-compiled python to have -pthread set for CXX, see https://github.com/python/cpython/pull/22525
# ac_cv_have_long_long_format=yes: target platform supports long long type
# disable-ipv6: disable ipv6 support, we don't need it in here
# ac_cv_file__dev_ptmx=no ac_cv_file__dev_ptc=no: disable pty support, we don't need it in here

# Build local python first, then build cross-compiled python.
ENV PY_INSTALL_PATH=${PWD}/python_arm64_build
RUN cd Python-3.10.10 && \
    ./configure \
    --enable-shared \
    ac_cv_pthread_is_default=no ac_cv_pthread=yes ac_cv_cxx_thread=yes \
    ac_cv_have_long_long_format=yes \
    --disable-ipv6 ac_cv_file__dev_ptmx=no ac_cv_file__dev_ptc=no && \
    make && make install
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib/
# Build cross-compiled python.
RUN cd Python-3.10.10 && \
    make clean && \
    make distclean && \
    alias python=python3 && \
    ./configure --build=x86_64-linux-gnu --host=aarch64-linux-gnu \
    --prefix=$PY_INSTALL_PATH/py_cross --enable-optimizations \
    CC=aarch64-linux-gnu-gcc \
    CXX=aarch64-linux-gnu-g++ \
    AR=aarch64-linux-gnu-ar \
    LD=aarch64-linux-gnu-ld \
    RANLIB=aarch64-linux-gnu-ranlib \
    --enable-shared \
    ac_cv_pthread_is_default=no ac_cv_pthread=yes ac_cv_cxx_thread=yes \
    ac_cv_have_long_long_format=yes \
    --disable-ipv6 ac_cv_file__dev_ptmx=no ac_cv_file__dev_ptc=no && \
    make && make altinstall && \
    cd ..

COPY . .
# Install rustup target for cross compiling.
RUN rustup target add aarch64-unknown-linux-gnu
# Set the environment variable for cross compiling and compile it
# Build the project in release mode.
RUN export PYO3_CROSS_LIB_DIR=$PY_INSTALL_PATH/py_cross/lib && \ 
    alias python=python3 && \
    cargo build --target aarch64-unknown-linux-gnu --release

