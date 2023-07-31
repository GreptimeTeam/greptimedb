#!/usr/bin/env bash

set -e

# this script will download Python source code, compile it, and install it to /usr/local/lib
# then use this python to compile cross-compiled python for aarch64
ARCH=$1
PYTHON_VERSION=3.10.10
PYTHON_SOURCE_DIR=Python-${PYTHON_VERSION}
PYTHON_INSTALL_PATH_AMD64=${PWD}/python-${PYTHON_VERSION}/amd64
PYTHON_INSTALL_PATH_AARCH64=${PWD}/python-${PYTHON_VERSION}/aarch64

function download_python_source_code() {
  wget https://www.python.org/ftp/python/$PYTHON_VERSION/Python-$PYTHON_VERSION.tgz
  tar -xvf Python-$PYTHON_VERSION.tgz
}

function compile_for_amd64_platform() {
  mkdir -p "$PYTHON_INSTALL_PATH_AMD64"

  echo "Compiling for amd64 platform..."

  ./configure \
    --prefix="$PYTHON_INSTALL_PATH_AMD64" \
    --enable-shared \
    ac_cv_pthread_is_default=no ac_cv_pthread=yes ac_cv_cxx_thread=yes \
    ac_cv_have_long_long_format=yes \
    --disable-ipv6 ac_cv_file__dev_ptmx=no ac_cv_file__dev_ptc=no

  make
  make install
}

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
function compile_for_aarch64_platform() {
  export LD_LIBRARY_PATH=$PYTHON_INSTALL_PATH_AMD64/lib:$LD_LIBRARY_PATH
  export LIBRARY_PATH=$PYTHON_INSTALL_PATH_AMD64/lib:$LIBRARY_PATH
  export PATH=$PYTHON_INSTALL_PATH_AMD64/bin:$PATH

  mkdir -p "$PYTHON_INSTALL_PATH_AARCH64"

  echo "Compiling for aarch64 platform..."
  echo "LD_LIBRARY_PATH: $LD_LIBRARY_PATH"
  echo "LIBRARY_PATH: $LIBRARY_PATH"
  echo "PATH: $PATH"

  ./configure --build=x86_64-linux-gnu --host=aarch64-linux-gnu \
    --prefix="$PYTHON_INSTALL_PATH_AARCH64" --enable-optimizations \
    CC=aarch64-linux-gnu-gcc \
    CXX=aarch64-linux-gnu-g++ \
    AR=aarch64-linux-gnu-ar \
    LD=aarch64-linux-gnu-ld \
    RANLIB=aarch64-linux-gnu-ranlib \
    --enable-shared \
    ac_cv_pthread_is_default=no ac_cv_pthread=yes ac_cv_cxx_thread=yes \
    ac_cv_have_long_long_format=yes \
    --disable-ipv6 ac_cv_file__dev_ptmx=no ac_cv_file__dev_ptc=no

  make
  make altinstall
}

# Main script starts here.
download_python_source_code

# Enter the python source code directory.
cd $PYTHON_SOURCE_DIR || exit 1

# Build local python first, then build cross-compiled python.
compile_for_amd64_platform

# Clean the build directory.
make clean && make distclean

# Cross compile python for aarch64.
if [ "$ARCH" = "aarch64-unknown-linux-gnu" ]; then
  compile_for_aarch64_platform
fi
