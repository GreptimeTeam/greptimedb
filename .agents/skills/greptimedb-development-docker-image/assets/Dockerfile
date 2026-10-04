FROM ubuntu:24.04

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    unzip \
    mysql-client \
    wget \
    && rm -rf /var/lib/apt/lists/*

COPY ./greptime /greptime/bin/

ENV PATH=/greptime/bin/:$PATH

ENTRYPOINT ["greptime"]
