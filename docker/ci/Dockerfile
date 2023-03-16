FROM ubuntu:22.04

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    ca-certificates \
    python3 \
    python3-dev

ARG TARGETARCH

ADD $TARGETARCH/greptime /greptime/bin/

ENV PATH /greptime/bin/:$PATH

ENTRYPOINT ["greptime"]
