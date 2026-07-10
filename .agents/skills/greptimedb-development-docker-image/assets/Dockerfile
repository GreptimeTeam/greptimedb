FROM ubuntu:24.04

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    curl \
    unzip \
    mysql-client \
    wget

ADD ./greptime /greptime/bin/

ENV PATH=/greptime/bin/:$PATH

ENTRYPOINT ["greptime"]
