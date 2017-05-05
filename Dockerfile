FROM golang:1.6


ADD . /go/src/github.com/segment-sources/postgres

RUN go get "github.com/tools/godep"
RUN cd /go/src/github.com/segment-sources/postgres \
    && godep go install ./cmd/postgres

ENTRYPOINT ["postgres"]
