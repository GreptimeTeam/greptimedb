FROM centos:7

ENV TARGET_BIN=greptime

RUN yum install -y epel-release \
    openssl \
    openssl-devel  \
    centos-release-scl  \
    rh-python38  \
    rh-python38-python-devel

ARG TARGETARCH

ADD $TARGETARCH/$TARGET_BIN /greptime/bin/

ENV PATH /greptime/bin/:$PATH

ENTRYPOINT ["$TARGET_BIN"]
