FROM centos:7

# Note: CentOS 7 has reached EOL since 2024-07-01 thus `mirror.centos.org` is no longer available and we need to use `vault.centos.org` instead.
RUN sed -i s/mirror.centos.org/vault.centos.org/g /etc/yum.repos.d/*.repo
RUN sed -i s/^#.*baseurl=http/baseurl=http/g /etc/yum.repos.d/*.repo

RUN yum install -y epel-release \
    openssl \
    openssl-devel  \
    centos-release-scl

ARG TARGETARCH

ADD $TARGETARCH/greptime /greptime/bin/

ENV PATH=/greptime/bin/:$PATH

ENV MALLOC_CONF="prof:true,prof_active:false"

ENTRYPOINT ["greptime"]
