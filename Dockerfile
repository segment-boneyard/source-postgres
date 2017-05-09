FROM golang:1.6

ADD . /go/src/github.com/segment-sources/source-postgres

RUN go get "github.com/tools/godep"
RUN cd /go/src/github.com/segment-sources/source-postgres \
    && godep go install ./cmd/source-postgres

# Additionally add a cron-like runner to run on an
# interval.
RUN go get -u github.com/segmentio/go-every

ENTRYPOINT ["source-postgres"]
