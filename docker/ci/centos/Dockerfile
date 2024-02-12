FROM centos:7

RUN yum install -y epel-release \
    openssl \
    openssl-devel  \
    centos-release-scl  \
    rh-python38  \
    rh-python38-python-devel

ARG TARGETARCH

ADD $TARGETARCH/greptime /greptime/bin/

ENV PATH /greptime/bin/:$PATH

ENTRYPOINT ["greptime"]
