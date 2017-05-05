FROM golang:1.6

ADD . $GOPATH/src/github.com/segment-sources/postgres

RUN go get "github.com/tools/godep"
RUN cd $GOPATH/src/github.com/segment-sources/postgres \
    && godep go install ./cmd/postgres

ENTRYPOINT ["postgres"]
