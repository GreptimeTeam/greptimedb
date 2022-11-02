FROM ubuntu:22.04

ARG TARGETARCH

ADD $TARGETARCH/greptime /greptime/bin/

ENV PATH /greptime/bin/:$PATH

ENTRYPOINT ["greptime"]
