FROM golang:1.13-alpine as builder
ENV APP_NAME="candled" \
    APP_PATH="/go/src/github.com/jsteenb2/candle"
ENV APP_BUILD_NAME="${APP_NAME}"
COPY . ${APP_PATH}
WORKDIR ${APP_PATH}
ENV GO111MODULE="on" \
    CGO_ENABLED=0 \
    GOOS=linux \
    GOFLAGS="-mod=vendor"

RUN go mod vendor
RUN go build -mod vendor -o /go/bin/candled ./cmd/candled
CMD [""]

FROM alpine
RUN apk --no-cache add curl
COPY --from=builder /go/bin/candled /candled
ENTRYPOINT ["/candled"]
