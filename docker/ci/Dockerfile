FROM ubuntu:22.04

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get -y install ca-certificates

ARG TARGETARCH

ADD $TARGETARCH/greptime /greptime/bin/

ENV PATH /greptime/bin/:$PATH

ENTRYPOINT ["greptime"]
