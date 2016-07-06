FROM golang:1.6

RUN go get -u github.com/segment-sources/postgres

ENTRYPOINT ["postgres"]