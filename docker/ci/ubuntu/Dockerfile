FROM ubuntu:22.04

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    ca-certificates \
    python3.10 \
    python3.10-dev \
    python3-pip \
    curl

COPY ./docker/python/requirements.txt /etc/greptime/requirements.txt

RUN python3 -m pip install -r /etc/greptime/requirements.txt

ARG TARGETARCH

ADD $TARGETARCH/greptime /greptime/bin/

ENV PATH /greptime/bin/:$PATH

ENTRYPOINT ["greptime"]
